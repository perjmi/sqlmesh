# SPDX-License-Identifier: Apache-2.0

from __future__ import annotations

import subprocess
import sys
from collections.abc import Iterator
from pathlib import Path

import pytest

PROJECT_PATH = Path(__file__).parents[1]
sys.path.insert(0, str(PROJECT_PATH))
sys.path.insert(0, str(Path(__file__).parent))

from accuracy_tiers import AccuracyScenario, selected_accuracy_scenarios  # noqa: E402
from generate_model_graph import chunk_model_graph, generate_model_graph  # noqa: E402

COMPOSE = (
    "docker",
    "compose",
    "-p",
    "randomgraphs-accuracy",
    "-f",
    str(PROJECT_PATH / "compose.accuracy.yaml"),
)
DATABASE_SERVICE = {"reference": "reference_db", "candidate": "candidate_db"}


def _run(*args: str, capture_output: bool = True) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        args,
        cwd=PROJECT_PATH,
        check=True,
        capture_output=capture_output,
        text=True,
    )


@pytest.fixture(scope="session", autouse=True)
def accuracy_stack() -> Iterator[None]:
    _run(*COMPOSE, "up", "-d", "--wait", "reference_db", "candidate_db")
    try:
        yield
    finally:
        generate_model_graph(10, output_dir=PROJECT_PATH / "models" / "generated", seed=42)
        _run(*COMPOSE, "down", "--volumes", "--remove-orphans", capture_output=False)


def _reset_database(target: str) -> None:
    service = DATABASE_SERVICE[target]
    _run(
        *COMPOSE,
        "exec",
        "-T",
        service,
        "dropdb",
        "--if-exists",
        "--force",
        "-U",
        "postgres",
        "sqlmesh",
    )
    _run(*COMPOSE, "exec", "-T", service, "createdb", "-U", "postgres", "sqlmesh")


def _plan(target: str, selected_models: tuple[str, ...] = ()) -> str:
    command = [
        *COMPOSE,
        "run",
        "--rm",
        "--no-deps",
        target,
        "sqlmesh",
        "--log-file-dir",
        "/tmp/sqlmesh-logs",
        "plan",
        "--auto-apply",
        "--no-prompts",
    ]
    for model_name in selected_models:
        command.extend(("--select-model", model_name))
    return _run(*command).stdout


def _query(target: str, sql: str) -> tuple[str, ...]:
    result = _run(
        *COMPOSE,
        "exec",
        "-T",
        DATABASE_SERVICE[target],
        "psql",
        "-U",
        "postgres",
        "-d",
        "sqlmesh",
        "-At",
        "-F",
        "|",
        "-c",
        sql,
    )
    return tuple(line for line in result.stdout.splitlines() if line)


def _database_signature(target: str, model_names: tuple[str, ...]) -> dict[str, object]:
    schemas = _query(
        target,
        """
        SELECT table_name, ordinal_position, column_name, data_type,
               COALESCE(numeric_precision::TEXT, ''), COALESCE(numeric_scale::TEXT, '')
        FROM information_schema.columns
        WHERE table_schema = 'generated'
        ORDER BY table_name, ordinal_position
        """,
    )
    digest_queries = [
        f"""
        SELECT '{model_name}', COUNT(*)::TEXT,
               COALESCE(SUM(entity_id), 0)::TEXT,
               COALESCE(SUM(bucket_id), 0)::TEXT,
               COALESCE(SUM(total_value), 0)::TEXT,
               COALESCE(SUM(row_count), 0)::TEXT,
               COALESCE(MIN(entity_id), 0)::TEXT,
               COALESCE(MAX(entity_id), 0)::TEXT
        FROM {model_name}
        """
        for model_name in model_names
    ]
    digests = tuple(
        row
        for start in range(0, len(digest_queries), 100)
        for row in _query(target, " UNION ALL ".join(digest_queries[start : start + 100]))
    )
    materialized_view_count = _query(
        target,
        "SELECT COUNT(*) FROM pg_matviews WHERE schemaname = 'sqlmesh__generated'",
    )
    return {
        "schemas": schemas,
        "digests": digests,
        "materialized_view_count": materialized_view_count,
    }


@pytest.mark.parametrize(
    "scenario",
    selected_accuracy_scenarios(),
    ids=lambda scenario: scenario.test_id,
)
def test_reference_and_candidate_are_equivalent(scenario: AccuracyScenario) -> None:
    graph = generate_model_graph(
        scenario.width,
        output_dir=PROJECT_PATH / "models" / "generated",
        seed=scenario.seed,
    )
    model_names = (*graph.input_models, *(model for layer in graph.layers for model in layer))

    _reset_database("reference")
    _reset_database("candidate")
    _plan("reference")
    if scenario.candidate_mode == "full":
        _plan("candidate")
    else:
        for chunk in chunk_model_graph(graph):
            _plan("candidate", chunk)

    reference_signature = _database_signature("reference", model_names)
    candidate_signature = _database_signature("candidate", model_names)
    assert candidate_signature == reference_signature

    assert "No changes to plan" in _plan("reference")
    assert "No changes to plan" in _plan("candidate")
