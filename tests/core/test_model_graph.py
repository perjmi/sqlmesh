import sqlite3
from pathlib import Path

import pytest
from sqlglot import parse_one

from sqlmesh.core.model import create_sql_model
from sqlmesh.core.model.graph import ProjectGraphIndex
from sqlmesh.core.model.registry import IndexedModelRegistry, ModelMetadata, ModelPayloadStore
from sqlmesh.core.selector import MetadataSelector
from sqlmesh.core.snapshot.definition import fingerprint_from_node
from sqlmesh.core.snapshot.streaming import StreamingFingerprinter
from sqlmesh.core.context_diff_streaming import CompactContextDiff
from sqlmesh.core.snapshot import Snapshot, SnapshotChangeCategory
from sqlmesh.utils.errors import SQLMeshError


def _metadata(
    fqn: str,
    *dependencies: str,
    tags: tuple[str, ...] = (),
) -> ModelMetadata:
    return ModelMetadata(
        fqn=fqn,
        name=fqn,
        source_path=Path("models") / f"{fqn}.sql",
        project="analytics",
        dialect="duckdb",
        gateway=None,
        enabled=True,
        kind_name="VIEW",
        dependencies=tuple(dependencies),
        tags=tags,
        dbt_fqn=None,
    )


def test_project_graph_index_is_persistent_deterministic_and_replaceable(tmp_path):
    path = tmp_path / "graph.sqlite"
    index = ProjectGraphIndex(path)
    records = [
        _metadata("c", "a", "b", tags=("daily",)),
        _metadata("a"),
        _metadata("b", "a"),
    ]

    index.replace(records)

    reopened = ProjectGraphIndex(path)
    assert list(reopened.iter_metadata()) == [records[1], records[2], records[0]]
    assert reopened.metadata("c") == records[0]
    assert reopened.upstream({"c"}) == {"a", "b"}
    assert reopened.downstream({"a"}) == {"b", "c"}
    assert list(reopened.iter_metadata({"c", "a"})) == [records[1], records[0]]
    assert [
        tuple(record.fqn for record in batch)
        for batch in reopened.iter_metadata_batches(batch_size=2)
    ] == [("a", "b"), ("c",)]

    reopened.replace([_metadata("replacement")])
    assert [record.fqn for record in reopened.iter_metadata()] == ["replacement"]


def test_project_graph_index_replace_is_atomic(tmp_path):
    index = ProjectGraphIndex(tmp_path / "graph.sqlite")
    original = _metadata("original")
    duplicate = _metadata("duplicate")
    index.replace([original])

    with pytest.raises(sqlite3.IntegrityError):
        index.replace([duplicate, duplicate])

    assert list(index.iter_metadata()) == [original]


def test_project_graph_index_incremental_replacement_is_atomic(tmp_path):
    index = ProjectGraphIndex(tmp_path / "graph.sqlite")
    original = _metadata("original")
    index.replace([original])

    with pytest.raises(RuntimeError, match="discovery failed"):
        with index.replacing() as writer:
            writer.add(_metadata("a"))
            writer.add(_metadata("b", "a"))
            raise RuntimeError("discovery failed")

    assert list(index.iter_metadata()) == [original]

    with index.replacing() as writer:
        writer.add(_metadata("a"))
        writer.add(_metadata("b", "a"))

    assert [record.fqn for record in index.iter_metadata()] == ["a", "b"]


def test_indexed_registry_hydrates_batches_with_a_bounded_lru(tmp_path):
    index = ProjectGraphIndex(tmp_path / "graph.sqlite")
    records = [_metadata(name) for name in ("a", "b", "c", "d", "e")]
    index.replace(records)
    hydrated = []

    def load(name: str):
        hydrated.append(name)
        return create_sql_model(name, parse_one("SELECT 1 AS id"))

    registry = IndexedModelRegistry(index, load, max_entries=2)
    batches = list(registry.hydrate_batches({"e", "d", "c", "b", "a"}, batch_size=2))

    assert [[model.name for model in batch] for batch in batches] == [
        ["a", "b"],
        ["c", "d"],
        ["e"],
    ]
    assert hydrated == ["a", "b", "c", "d", "e"]
    assert registry.cache_size == 2
    assert registry.max_cache_size_seen == 2

    registry.hydrate("e")
    assert hydrated == ["a", "b", "c", "d", "e"]
    registry.hydrate("a")
    assert hydrated == ["a", "b", "c", "d", "e", "a"]
    registry.evict({"a", "e"})
    assert registry.cache_size == 0


def test_model_payload_store_round_trips_models_by_metadata_digest(tmp_path):
    model = create_sql_model("db.model", parse_one("SELECT 1 AS id"))
    model._data_hash = "authoritative_data_hash"
    model._metadata_hash = "authoritative_metadata_hash"
    metadata = ModelMetadata.from_model(model)
    store = ModelPayloadStore(tmp_path / "payloads")

    store.put(model, metadata)

    hydrated = store.get(metadata)
    assert hydrated is not None
    assert hydrated.dict() == model.dict()
    assert hydrated is not model
    assert hydrated.data_hash == "authoritative_data_hash"
    assert hydrated.metadata_hash == "authoritative_metadata_hash"
    assert store.get(_metadata("missing")) is None


def test_indexed_metadata_selection_does_not_enumerate_model_records(tmp_path, monkeypatch):
    index = ProjectGraphIndex(tmp_path / "graph.sqlite")
    index.replace([_metadata("a"), _metadata("b", tags=("daily",)), _metadata("c")])
    registry = IndexedModelRegistry(
        index,
        lambda name: create_sql_model(name, parse_one("SELECT 1")),
        max_entries=1,
    )

    def fail_if_enumerated(*args, **kwargs):
        raise AssertionError("full metadata enumeration is not bounded selection")

    monkeypatch.setattr(index, "iter_metadata", fail_if_enumerated)

    assert MetadataSelector(registry).expand_model_selections(["tag:daily"]) == {"b"}


def test_streaming_fingerprints_match_eager_with_bounded_hydration(tmp_path):
    model_a = create_sql_model("a", parse_one("SELECT 1 AS id"))
    model_b = create_sql_model("b", parse_one("SELECT * FROM a"), depends_on={model_a.fqn})
    model_c = create_sql_model("c", parse_one("SELECT * FROM b"), depends_on={model_b.fqn})
    models = {model.fqn: model for model in (model_a, model_b, model_c)}
    index = ProjectGraphIndex(tmp_path / "graph.sqlite")
    metadata = [ModelMetadata.from_model(model) for model in models.values()]
    index.replace(metadata)
    store = ModelPayloadStore(tmp_path / "payloads")
    for record in metadata:
        store.put(models[record.fqn], record)
    registry = IndexedModelRegistry(
        index,
        lambda name: store.get(index.metadata(name)),
        max_entries=1,
    )

    streamed = dict(StreamingFingerprinter(index, registry, batch_size=1).fingerprint())
    expected_cache = {}
    expected = {
        name: fingerprint_from_node(model, nodes=models, cache=expected_cache)
        for name, model in models.items()
    }

    assert streamed == expected
    assert registry.max_cache_size_seen == 1
    assert registry.cache_size == 0
    assert index.fingerprint(model_c.fqn) == expected[model_c.fqn]


def test_topological_batches_reject_cycles(tmp_path):
    index = ProjectGraphIndex(tmp_path / "graph.sqlite")
    index.replace([_metadata("a", "b"), _metadata("b", "a")])

    with pytest.raises(SQLMeshError, match="cycle"):
        list(index.iter_topological_batches(batch_size=1))


def test_compact_context_diff_matches_fingerprints_without_snapshot_payloads(tmp_path):
    local_models = {
        name: create_sql_model(name, parse_one(query))
        for name, query in {
            "a": "SELECT 1 AS id",
            "b": "SELECT 2 AS id",
            "c": "SELECT 3 AS id",
        }.items()
    }
    index = ProjectGraphIndex(tmp_path / "graph.sqlite")
    index.replace(ModelMetadata.from_model(model) for model in local_models.values())
    index.put_fingerprints(
        (
            model.fqn,
            fingerprint_from_node(model, nodes=local_models),
        )
        for name, model in local_models.items()
    )

    remote_a = Snapshot.from_node(local_models["a"], nodes=local_models)
    remote_b_model = create_sql_model("b", parse_one("SELECT 200 AS id"))
    remote_b = Snapshot.from_node(remote_b_model, nodes={"b": remote_b_model})
    remote_d_model = create_sql_model("d", parse_one("SELECT 4 AS id"))
    remote_d = Snapshot.from_node(remote_d_model, nodes={"d": remote_d_model})
    for snapshot in (remote_a, remote_b, remote_d):
        snapshot.categorize_as(SnapshotChangeCategory.BREAKING)

    compact = CompactContextDiff.create(
        index, [remote_a.table_info, remote_b.table_info, remote_d.table_info]
    )

    assert {snapshot_id.name for snapshot_id in compact.added} == {'"c"'}
    assert {snapshot_id.name for snapshot_id in compact.removed} == {'"d"'}
    assert set(compact.modified) == {'"b"'}
    assert set(compact.unchanged) == {'"a"'}

    selected = CompactContextDiff.create(
        index,
        [],
        local_snapshot_ids={remote_a.name: remote_a.snapshot_id},
    )
    assert selected.added == {remote_a.snapshot_id}
