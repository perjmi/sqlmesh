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
    metadata = ModelMetadata.from_model(model)
    store = ModelPayloadStore(tmp_path / "payloads")

    store.put(model, metadata)

    hydrated = store.get(metadata)
    assert hydrated is not None
    assert hydrated.dict() == model.dict()
    assert hydrated is not model
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
    model_b = create_sql_model(
        "b", parse_one("SELECT * FROM a"), depends_on={model_a.fqn}
    )
    model_c = create_sql_model(
        "c", parse_one("SELECT * FROM b"), depends_on={model_b.fqn}
    )
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
