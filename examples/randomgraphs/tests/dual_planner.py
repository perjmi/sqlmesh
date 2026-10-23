# SPDX-License-Identifier: Apache-2.0

from __future__ import annotations

import json
import random
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


def run(
    *args: str, capture_output: bool = True, check: bool = True
) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        args,
        cwd=PROJECT_PATH,
        check=check,
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


def randomized_plan_options(
    option_groups: tuple[tuple[str, ...], ...], *, seed: int
) -> tuple[str, ...]:
    """Return a reproducibly shuffled CLI argument list while keeping option values together."""
    shuffled = list(option_groups)
    random.Random(seed).shuffle(shuffled)
    return tuple(value for group in shuffled for value in group)


def plan(
    target: str,
    selected_models: tuple[str, ...] = (),
    *,
    environment: str | None = None,
    option_groups: tuple[tuple[str, ...], ...] = (),
    permutation_seed: int = 0,
) -> str:
    result = plan_result(
        target,
        selected_models,
        environment=environment,
        option_groups=option_groups,
        permutation_seed=permutation_seed,
    )
    result.check_returncode()
    return result.stdout


def plan_result(
    target: str,
    selected_models: tuple[str, ...] = (),
    *,
    environment: str | None = None,
    option_groups: tuple[tuple[str, ...], ...] = (),
    permutation_seed: int = 0,
) -> subprocess.CompletedProcess[str]:
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
    ]
    if environment is not None:
        command.append(environment)
    groups = [
        ("--auto-apply",),
        ("--no-prompts",),
        *(("--select-model", model_name) for model_name in selected_models),
        *option_groups,
    ]
    command.extend(randomized_plan_options(tuple(groups), seed=permutation_seed))
    return run(*command, check=False)


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


def database_signature(
    target: str, model_names: tuple[str, ...], *, schema: str = "generated"
) -> dict[str, object]:
    schemas = query(
        target,
        f"""
        SELECT table_name, ordinal_position, column_name, data_type,
               COALESCE(numeric_precision::TEXT, ''), COALESCE(numeric_scale::TEXT, '')
        FROM information_schema.columns
        WHERE table_schema = '{schema}'
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


def model_names_for_environment(model_names: tuple[str, ...], environment: str) -> tuple[str, ...]:
    return tuple(
        model_name.replace("generated.", f"generated__{environment}.", 1)
        for model_name in model_names
    )


def state_signature(target: str) -> dict[str, tuple[str, ...]]:
    """Return deterministic planner state while excluding timestamps and generated row IDs."""
    snapshots = _normalize_json_rows(
        query(
            target,
            """
            SELECT JSONB_BUILD_OBJECT(
              'name', name,
              'identifier', identifier,
              'version', version,
              'dev_version', dev_version,
              'kind_name', kind_name,
              'unrestorable', unrestorable,
              'forward_only', forward_only,
              'fingerprint', fingerprint::JSONB,
              'change_category', snapshot::JSONB->'change_category',
              'parents', COALESCE(snapshot::JSONB->'parents', '[]'::JSONB),
              'previous_versions', COALESCE(
                snapshot::JSONB->'previous_versions', '[]'::JSONB
              ),
              'dev_table_suffix', snapshot::JSONB->'dev_table_suffix',
              'table_naming_convention', snapshot::JSONB->'table_naming_convention'
            )::TEXT
            FROM sqlmesh._snapshots
            ORDER BY name, identifier
            """,
        )
    )
    intervals = query(
        target,
        """
        SELECT name,
               identifier,
               version,
               start_ts::TEXT,
               end_ts::TEXT,
               is_dev::TEXT,
               is_removed::TEXT,
               is_compacted::TEXT
        FROM sqlmesh._intervals
        ORDER BY name, identifier, version, start_ts, end_ts, is_dev, is_removed, is_compacted
        """,
    )
    environments = _normalize_json_rows(
        query(
            target,
            """
            SELECT JSONB_BUILD_OBJECT(
              'name', name,
              'start_at', start_at,
              'end_at', end_at,
              'snapshots', snapshots::JSONB,
              'promoted_snapshot_ids', promoted_snapshot_ids::JSONB,
              'suffix_target', suffix_target,
              'catalog_name_override', catalog_name_override,
              'previous_finalized_snapshots', previous_finalized_snapshots::JSONB,
              'normalize_name', normalize_name,
              'requirements', requirements::JSONB
            )::TEXT
            FROM sqlmesh._environments
            ORDER BY name
            """,
        )
    )
    return {
        "snapshots": snapshots,
        "intervals": intervals,
        "environments": environments,
    }


def _normalize_json_rows(rows: tuple[str, ...]) -> tuple[str, ...]:
    def _normalize(value: object) -> object:
        if isinstance(value, dict):
            return {key: _normalize(item) for key, item in value.items()}
        if isinstance(value, list):
            normalized = [_normalize(item) for item in value]
            if all(isinstance(item, dict) and "name" in item for item in normalized):
                normalized.sort(
                    key=lambda item: json.dumps(item, sort_keys=True, separators=(",", ":"))
                )
            return normalized
        return value

    return tuple(
        json.dumps(_normalize(json.loads(row)), sort_keys=True, separators=(",", ":"))
        for row in rows
    )


def assert_state_equal() -> None:
    """Compare normalized state with a compact first-difference diagnostic."""
    reference = state_signature("reference")
    candidate = state_signature("candidate")
    assert candidate.keys() == reference.keys()
    for section in reference:
        candidate_rows = candidate[section]
        reference_rows = reference[section]
        assert len(candidate_rows) == len(reference_rows), (
            f"{section} row count differs: "
            f"candidate={len(candidate_rows)}, reference={len(reference_rows)}"
        )
        for row_index, (candidate_row, reference_row) in enumerate(
            zip(candidate_rows, reference_rows)
        ):
            if candidate_row == reference_row:
                continue
            character_index = next(
                (
                    index
                    for index, values in enumerate(zip(candidate_row, reference_row))
                    if values[0] != values[1]
                ),
                min(len(candidate_row), len(reference_row)),
            )
            start = max(character_index - 100, 0)
            end = character_index + 200
            raise AssertionError(
                f"{section}[{row_index}] differs at character {character_index}: "
                f"candidate={candidate_row[start:end]!r}, "
                f"reference={reference_row[start:end]!r}"
            )


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
