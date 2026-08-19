# SPDX-License-Identifier: Apache-2.0

from __future__ import annotations

import re
import subprocess
from dataclasses import dataclass
from pathlib import Path

PROJECT_PATH = Path(__file__).parents[1]
COMPOSE = (
    "docker",
    "compose",
    "-p",
    "randomgraphs-accuracy",
    "-f",
    str(PROJECT_PATH / "compose.accuracy.yaml"),
)
DATABASE_SERVICE = {"reference": "reference_db", "candidate": "candidate_db"}
ANSI_ESCAPE = re.compile(r"\x1b\[[0-?]*[ -/]*[@-~]")


def run(*args: str, capture_output: bool = True) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        args,
        cwd=PROJECT_PATH,
        check=True,
        capture_output=capture_output,
        text=True,
    )


def reset_database(target: str) -> None:
    service = DATABASE_SERVICE[target]
    run(
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
    run(*COMPOSE, "exec", "-T", service, "createdb", "-U", "postgres", "sqlmesh")


def plan(target: str, selected_models: tuple[str, ...] = ()) -> str:
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
    return run(*command).stdout


def query(target: str, sql: str) -> tuple[str, ...]:
    result = run(
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


def database_signature(target: str, model_names: tuple[str, ...]) -> dict[str, object]:
    schemas = query(
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
        for row in query(target, " UNION ALL ".join(digest_queries[start : start + 100]))
    )
    materialized_view_count = query(
        target,
        "SELECT COUNT(*) FROM pg_matviews WHERE schemaname = 'sqlmesh__generated'",
    )
    return {
        "schemas": schemas,
        "digests": digests,
        "materialized_view_count": materialized_view_count,
    }


def assert_model_rows_equal(model_names: tuple[str, ...]) -> None:
    """Naively compares every ordered row instead of relying only on aggregate digests."""
    for model_name in model_names:
        sql = f"""
            SELECT entity_id::TEXT, bucket_id::TEXT, total_value::TEXT, row_count::TEXT
            FROM {model_name}
            ORDER BY entity_id, bucket_id, total_value, row_count
        """
        reference_rows = query("reference", sql)
        candidate_rows = query("candidate", sql)
        assert candidate_rows == reference_rows, f"Row mismatch for {model_name}"


@dataclass(frozen=True)
class AuditOutcome:
    returncode: int
    found: int
    passed: int
    failed: int
    errors: int
    result_counts: tuple[int, ...]


def audit(target: str, model_name: str) -> AuditOutcome:
    command = (
        *COMPOSE,
        "run",
        "--rm",
        "--no-deps",
        target,
        "sqlmesh",
        "--log-file-dir",
        "/tmp/sqlmesh-logs",
        "audit",
        "--model",
        model_name,
    )
    result = subprocess.run(
        command,
        cwd=PROJECT_PATH,
        check=False,
        capture_output=True,
        text=True,
    )
    output = ANSI_ESCAPE.sub("", f"{result.stdout}\n{result.stderr}")
    found_match = re.search(r"Found (\d+) audit", output)
    errors_match = re.search(r"Finished with (\d+) audit error", output)
    return AuditOutcome(
        returncode=result.returncode,
        found=int(found_match.group(1)) if found_match else 0,
        passed=len(re.findall(r"\bPASS\b", output)),
        failed=len(re.findall(r"\bFAIL\b", output)),
        errors=int(errors_match.group(1)) if errors_match else 0,
        result_counts=tuple(int(count) for count in re.findall(r"Got (\d+) results", output)),
    )
