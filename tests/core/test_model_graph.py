import sqlite3
from concurrent.futures import Future
from pathlib import Path

import pytest
from sqlglot import parse_one

from sqlmesh.core.config import TableNamingConvention
from sqlmesh.core.model import create_sql_model
from sqlmesh.core.model.graph import IndexedDAG, ProjectGraphIndex
from sqlmesh.core.model.registry import IndexedModelRegistry, ModelMetadata, ModelPayloadStore
from sqlmesh.core.model.schema import update_model_schemas_streaming
from sqlmesh.core.selector import MetadataSelector
from sqlmesh.core.snapshot.definition import fingerprint_from_node
from sqlmesh.core.snapshot.streaming import (
    StreamingFingerprinter,
    StreamingSnapshotTask,
    build_serialized_streaming_snapshot,
    init_streaming_snapshot_worker,
)
from sqlmesh.core.context_diff_streaming import CompactContextDiff
from sqlmesh.core.snapshot import Snapshot, SnapshotChangeCategory
from sqlmesh.core.plan.store import deserialize_snapshot
from sqlmesh.utils.errors import SQLMeshError
from sqlmesh.utils.dag import DAG


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


def test_streaming_schema_propagation_hydrates_only_child_and_one_parent(tmp_path, monkeypatch):
    model_a = create_sql_model("db.a", parse_one("SELECT 1 AS id"))
    model_b = create_sql_model("db.b", parse_one("SELECT * FROM db.a"), depends_on={model_a.fqn})
    index = ProjectGraphIndex(tmp_path / "graph.sqlite")
    store = ModelPayloadStore(tmp_path / "payloads")
    discovered = [
        ModelMetadata.from_model(model, include_payload_digest=False)
        for model in (model_a, model_b)
    ]
    index.replace(discovered)
    for model, metadata in zip((model_a, model_b), discovered):
        store.put_discovered(model, metadata)

    def fail_if_global_schema_is_updated(*args, **kwargs):
        raise AssertionError("streaming propagation must not accumulate a project-wide schema")

    monkeypatch.setattr(
        "sqlmesh.core.model.schema._update_schema_with_model", fail_if_global_schema_is_updated
    )
    max_hydrated = update_model_schemas_streaming(
        index,
        store,
        cache_dir=tmp_path,
        batch_size=1,
        max_workers=1,
    )

    assert max_hydrated == 2
    finalized_b_metadata = index.metadata(model_b.fqn)
    assert finalized_b_metadata.payload_digest
    finalized_b = store.get(finalized_b_metadata)
    assert finalized_b is not None
    assert finalized_b.mapping_schema
    assert finalized_b.columns_to_types == model_a.columns_to_types


def test_streaming_schema_propagation_uses_bounded_recycled_worker_pools(
    tmp_path, monkeypatch
):
    models = [create_sql_model(f"db.model_{i}", parse_one(f"SELECT {i} AS id")) for i in range(5)]
    index = ProjectGraphIndex(tmp_path / "graph.sqlite")
    store = ModelPayloadStore(tmp_path / "payloads")
    discovered = [
        ModelMetadata.from_model(model, include_payload_digest=False) for model in models
    ]
    index.replace(discovered)
    for model, metadata in zip(models, discovered):
        store.put_discovered(model, metadata)

    pools = []
    events = []

    class RecordingExecutor:
        def __init__(self, initializer, initargs):
            initializer(*initargs)
            self.submitted = []
            self.shutdown_called = False

        def submit(self, function, name):
            self.submitted.append(name)
            events.append(("submit", name))
            future = Future()
            try:
                future.set_result(function(name))
            except BaseException as ex:
                future.set_exception(ex)
            return future

        def shutdown(self, wait=True, cancel_futures=False):
            self.shutdown_called = True

    def create_recording_executor(*, initializer, initargs, max_workers):
        assert max_workers == 2
        executor = RecordingExecutor(initializer, initargs)
        pools.append(executor)
        return executor

    original_update = index.update_payload_references

    def record_update(records):
        records = tuple(records)
        events.append(("commit", tuple(sorted(record.fqn for record in records))))
        original_update(records)

    monkeypatch.setattr(
        "sqlmesh.core.model.schema.create_process_pool_executor", create_recording_executor
    )
    monkeypatch.setattr(index, "update_payload_references", record_update)

    max_hydrated = update_model_schemas_streaming(
        index,
        store,
        cache_dir=tmp_path,
        batch_size=5,
        max_workers=2,
        worker_max_tasks=1,
    )

    assert max_hydrated == 2
    names = [model.fqn for model in models]
    assert [pool.submitted for pool in pools] == [
        names[0:2],
        names[2:4],
        names[4:5],
    ]
    assert all(pool.shutdown_called for pool in pools)
    assert events == [
        ("submit", names[0]),
        ("submit", names[1]),
        ("commit", tuple(names[0:2])),
        ("submit", names[2]),
        ("submit", names[3]),
        ("commit", tuple(names[2:4])),
        ("submit", names[4]),
        ("commit", tuple(names[4:5])),
    ]


def test_streaming_schema_propagation_parallel_workers_respect_dependency_barriers(tmp_path):
    model_a = create_sql_model("db.a", parse_one("SELECT 1 AS id"))
    model_b = create_sql_model("db.b", parse_one("SELECT 2 AS id"))
    model_c = create_sql_model(
        "db.c", parse_one("SELECT * FROM db.a"), depends_on={model_a.fqn}
    )
    model_d = create_sql_model(
        "db.d", parse_one("SELECT * FROM db.b"), depends_on={model_b.fqn}
    )
    models = (model_a, model_b, model_c, model_d)
    index = ProjectGraphIndex(tmp_path / "graph.sqlite")
    store = ModelPayloadStore(tmp_path / "payloads")
    discovered = [
        ModelMetadata.from_model(model, include_payload_digest=False) for model in models
    ]
    index.replace(discovered)
    for model, metadata in zip(models, discovered):
        store.put_discovered(model, metadata)

    max_hydrated = update_model_schemas_streaming(
        index,
        store,
        cache_dir=tmp_path,
        batch_size=4,
        max_workers=2,
        worker_max_tasks=10,
    )

    assert max_hydrated == 4
    finalized_c = store.get(index.metadata(model_c.fqn))
    finalized_d = store.get(index.metadata(model_d.fqn))
    assert finalized_c is not None
    assert finalized_d is not None
    assert finalized_c.columns_to_types == model_a.columns_to_types
    assert finalized_d.columns_to_types == model_b.columns_to_types


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


def test_streaming_fingerprints_match_eager_without_model_hydration(tmp_path):
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
        lambda name: (_ for _ in ()).throw(
            AssertionError(f"fingerprinting hydrated model '{name}'")
        ),
        max_entries=1,
    )

    streamed = dict(StreamingFingerprinter(index, registry, batch_size=1).fingerprint())
    expected_cache = {}
    expected = {
        name: fingerprint_from_node(model, nodes=models, cache=expected_cache)
        for name, model in models.items()
    }

    assert streamed == expected
    assert registry.max_cache_size_seen == 0
    assert registry.cache_size == 0
    assert index.fingerprint(model_c.fqn) == expected[model_c.fqn]


def test_streaming_snapshot_includes_physical_parents_of_embedded_models(tmp_path):
    seed = create_sql_model("db.seed", parse_one("SELECT 1 AS id"), kind="FULL")
    embedded = create_sql_model(
        "db.embedded",
        parse_one("SELECT * FROM db.seed"),
        kind="EMBEDDED",
        depends_on={seed.fqn},
    )
    downstream = create_sql_model(
        "db.downstream",
        parse_one("SELECT * FROM db.embedded"),
        kind="FULL",
        depends_on={embedded.fqn},
    )
    models = {model.fqn: model for model in (seed, embedded, downstream)}
    index = ProjectGraphIndex(tmp_path / "graph.sqlite")
    metadata = [ModelMetadata.from_model(model) for model in models.values()]
    index.replace(metadata)
    payloads = ModelPayloadStore(tmp_path / "payloads")
    for record in metadata:
        payloads.put(models[record.fqn], record)
    list(
        StreamingFingerprinter(
            index,
            IndexedModelRegistry(index, lambda name: models[name], max_entries=1),
            batch_size=1,
        ).fingerprint()
    )
    init_streaming_snapshot_worker(str(index.path), str(tmp_path / "payloads"))

    serialized = build_serialized_streaming_snapshot(
        StreamingSnapshotTask(
            name=downstream.fqn,
            created_ts=1,
            ttl="in 1 week",
            table_naming_convention=TableNamingConvention.default,
        )
    )
    streamed = deserialize_snapshot(serialized.payload)
    eager = Snapshot.from_node(downstream, nodes=models)

    assert streamed.parents == eager.parents
    assert {parent.name for parent in streamed.parents} == {seed.fqn, embedded.fqn}


def test_topological_batches_reject_cycles(tmp_path):
    index = ProjectGraphIndex(tmp_path / "graph.sqlite")
    index.replace([_metadata("a", "b"), _metadata("b", "a")])

    with pytest.raises(SQLMeshError, match="cycle"):
        list(index.iter_topological_batches(batch_size=1))


def test_topological_batches_do_not_enumerate_graph_into_python(tmp_path, monkeypatch):
    index = ProjectGraphIndex(tmp_path / "graph.sqlite")
    index.replace([_metadata("c", "a", "b"), _metadata("a"), _metadata("b", "a")])

    def fail_if_names_are_enumerated():
        raise AssertionError("topological traversal must keep graph-wide state in SQLite")

    monkeypatch.setattr(index, "iter_names", fail_if_names_are_enumerated)

    assert list(index.iter_topological_batches(batch_size=1)) == [("a",), ("b",), ("c",)]


def test_indexed_dag_matches_read_only_dag_api(tmp_path):
    records = [
        _metadata("c", "a", "b", "external"),
        _metadata("a"),
        _metadata("b"),
    ]
    index = ProjectGraphIndex(tmp_path / "graph.sqlite")
    index.replace(records)
    indexed = IndexedDAG(index, batch_size=1)
    eager = DAG({record.fqn: set(record.dependencies) for record in records})

    assert indexed.graph == eager.graph
    assert indexed.sorted == eager.sorted
    assert list(indexed) == eager.sorted
    assert indexed.roots == eager.roots
    assert indexed.upstream("c") == eager.upstream("c")
    assert indexed.downstream("external") == eager.downstream("external")
    assert indexed.prune("c", "external").graph == eager.prune("c", "external").graph
    assert indexed.subdag("c").graph == eager.subdag("c").graph
    assert indexed.lineage("a").graph == eager.lineage("a").graph
    assert indexed.reversed.graph == eager.reversed.graph
    assert "external" in indexed
    assert "missing" not in indexed

    with pytest.raises(SQLMeshError, match="project reload"):
        indexed.add("new")


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
