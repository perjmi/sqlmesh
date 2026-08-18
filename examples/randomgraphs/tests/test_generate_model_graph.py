# SPDX-License-Identifier: Apache-2.0

import sys
from pathlib import Path

import pytest

from sqlmesh.core.dialect import parse

sys.path.insert(0, str(Path(__file__).parents[1]))

from generate_model_graph import (  # noqa: E402
    GRAPH_DEPTH,
    chunk_model_graph,
    generate_model_graph,
)


def test_generate_model_graph(tmp_path: Path) -> None:
    graph = generate_model_graph(5, output_dir=tmp_path, seed=7)

    assert len(graph.input_models) == 5
    assert len(graph.layers) == GRAPH_DEPTH
    assert all(len(layer) == 5 for layer in graph.layers)
    assert len(graph.model_paths) == 5 + GRAPH_DEPTH * 5
    assert set(graph.input_row_counts) == set(graph.input_models)
    assert all(10 <= count <= 1000 for count in graph.input_row_counts.values())

    previous_layer = set(graph.input_models)
    for layer in graph.layers:
        for model_name in layer:
            upstreams = graph.dependencies[model_name]
            assert 2 <= len(upstreams) <= 4
            assert len(upstreams) == len(set(upstreams))
            assert set(upstreams) <= previous_layer
        previous_layer = set(layer)

    for path in graph.model_paths:
        assert parse(path.read_text())

    materialized_model = (tmp_path / "mv_d01_0000.sql").read_text()
    assert "materialization 'postgres_materialized_view'" in materialized_model
    assert "\nJOIN " in materialized_model
    assert "GROUP BY" in materialized_model

    for index, input_model in enumerate(graph.input_models):
        input_sql = (tmp_path / f"input_{index:04d}.sql").read_text()
        assert f"GENERATE_SERIES(1, {graph.input_row_counts[input_model]})" in input_sql


def test_generation_is_deterministic_and_removes_stale_models(tmp_path: Path) -> None:
    first = generate_model_graph(3, output_dir=tmp_path, seed=11)
    first_contents = {path.name: path.read_text() for path in first.model_paths}

    second = generate_model_graph(3, output_dir=tmp_path, seed=11)
    second_contents = {path.name: path.read_text() for path in second.model_paths}
    assert second_contents == first_contents

    smaller = generate_model_graph(1, output_dir=tmp_path, seed=11)
    assert len(smaller.model_paths) == GRAPH_DEPTH + 1
    assert not (tmp_path / "input_0001.sql").exists()
    assert not (tmp_path / "mv_d06_0002.sql").exists()


def test_row_count_bounds_are_configurable(tmp_path: Path) -> None:
    graph = generate_model_graph(
        4,
        output_dir=tmp_path,
        seed=3,
        min_rows_per_input=23,
        max_rows_per_input=29,
    )

    assert all(23 <= count <= 29 for count in graph.input_row_counts.values())


def test_chunk_model_graph_returns_balanced_dag_ordered_chunks(tmp_path: Path) -> None:
    graph = generate_model_graph(100, output_dir=tmp_path, seed=42)

    chunks = chunk_model_graph(graph)

    assert len(chunks) == 10
    assert all(len(chunk) == 70 for chunk in chunks)

    all_models = (*graph.input_models, *(model for layer in graph.layers for model in layer))
    assert tuple(model for chunk in chunks for model in chunk) == all_models

    chunk_by_model = {
        model_name: chunk_index for chunk_index, chunk in enumerate(chunks) for model_name in chunk
    }
    for model_name, upstreams in graph.dependencies.items():
        assert all(chunk_by_model[upstream] <= chunk_by_model[model_name] for upstream in upstreams)


def test_chunk_model_graph_distributes_remainder(tmp_path: Path) -> None:
    graph = generate_model_graph(3, output_dir=tmp_path, seed=1)

    chunks = chunk_model_graph(graph, chunk_count=10)

    assert [len(chunk) for chunk in chunks] == [3, 2, 2, 2, 2, 2, 2, 2, 2, 2]


@pytest.mark.parametrize("chunk_count", [0, -1, 22])
def test_chunk_model_graph_rejects_invalid_chunk_count(tmp_path: Path, chunk_count: int) -> None:
    graph = generate_model_graph(3, output_dir=tmp_path, seed=1)

    with pytest.raises(ValueError, match="chunk_count"):
        chunk_model_graph(graph, chunk_count=chunk_count)


@pytest.mark.parametrize("n", [0, -1])
def test_generate_model_graph_rejects_non_positive_width(tmp_path: Path, n: int) -> None:
    with pytest.raises(ValueError, match="positive integer"):
        generate_model_graph(n, output_dir=tmp_path)


@pytest.mark.parametrize("bounds", [(0, 10), (20, 10)])
def test_generate_model_graph_rejects_invalid_row_bounds(
    tmp_path: Path, bounds: tuple[int, int]
) -> None:
    with pytest.raises(ValueError, match="row bounds"):
        generate_model_graph(
            1,
            output_dir=tmp_path,
            min_rows_per_input=bounds[0],
            max_rows_per_input=bounds[1],
        )
