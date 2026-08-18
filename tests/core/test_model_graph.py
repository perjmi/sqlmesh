import sqlite3
from pathlib import Path

import pytest
from sqlglot import parse_one

from sqlmesh.core.model import create_sql_model
from sqlmesh.core.model.graph import ProjectGraphIndex
from sqlmesh.core.model.registry import IndexedModelRegistry, ModelMetadata


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
