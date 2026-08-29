# SPDX-License-Identifier: Apache-2.0

from __future__ import annotations

from pathlib import Path

from generate_model_graph import GENERATED_HEADER

PROJECT_PATH = Path(__file__).parents[1]
GENERATED_MODELS_PATH = PROJECT_PATH / "models" / "generated"
INCREMENTAL_MODEL_NAME = "generated.incremental_events"


def write_incremental_model(
    *,
    name: str = INCREMENTAL_MODEL_NAME,
    include_extra: bool = False,
    include_legacy: bool = True,
    row_count: int = 1,
    with_audit: bool = False,
) -> Path:
    """Write a deterministic daily incremental model used by planner-option scenarios."""
    table_name = name.rsplit(".", 1)[-1]
    path = GENERATED_MODELS_PATH / f"{table_name}.sql"
    columns = [
        "    event_date DATE",
        "    entity_id BIGINT",
        "    bucket_id INTEGER",
        "    total_value DECIMAL(38, 6)",
        "    row_count BIGINT",
    ]
    projections = [
        "  event_date::DATE AS event_date",
        "  entity_id::BIGINT AS entity_id",
        "  (entity_id % 16)::INTEGER AS bucket_id",
        "  (entity_id * 10)::DECIMAL(38, 6) AS total_value",
        f"  {row_count}::BIGINT AS row_count",
    ]
    if include_legacy:
        columns.append("    legacy_metric BIGINT")
        projections.append("  7::BIGINT AS legacy_metric")
    if include_extra:
        columns.append("    extra_metric BIGINT")
        projections.append("  11::BIGINT AS extra_metric")

    audit = "  audits (randomgraph_invariants),\n" if with_audit else ""
    columns_sql = ",\n".join(columns)
    projections_sql = ",\n".join(projections)
    path.write_text(
        f"""{GENERATED_HEADER}
MODEL (
  name {name},
  kind INCREMENTAL_BY_TIME_RANGE (
    time_column event_date,
    on_destructive_change error,
    on_additive_change error
  ),
  start '2026-08-25',
  cron '@daily',
  grain (event_date, entity_id),
{audit}  columns (
{columns_sql}
  )
);

SELECT
{projections_sql}
FROM GENERATE_SERIES(
  CAST(@start_date AS DATE),
  CAST(@end_date AS DATE),
  INTERVAL '1 day'
) AS dates(event_date)
CROSS JOIN GENERATE_SERIES(1, 3) AS entities(entity_id);
""",
        encoding="utf-8",
    )
    return path


def write_passthrough_view(name: str, upstream: str) -> Path:
    table_name = name.rsplit(".", 1)[-1]
    path = GENERATED_MODELS_PATH / f"{table_name}.sql"
    path.write_text(
        f"""{GENERATED_HEADER}
MODEL (
  name {name},
  kind VIEW,
  grain (entity_id, bucket_id)
);

SELECT
  entity_id,
  bucket_id,
  total_value,
  row_count
FROM {upstream};
""",
        encoding="utf-8",
    )
    return path


def write_full_model(name: str, *, row_count: int = 5, with_audit: bool = False) -> Path:
    table_name = name.rsplit(".", 1)[-1]
    path = GENERATED_MODELS_PATH / f"{table_name}.sql"
    audit = "  audits (randomgraph_invariants),\n" if with_audit else ""
    path.write_text(
        f"""{GENERATED_HEADER}
MODEL (
  name {name},
  kind FULL,
  grain (entity_id, bucket_id),
{audit}  columns (
    entity_id BIGINT,
    bucket_id INTEGER,
    total_value DECIMAL(38, 6),
    row_count BIGINT
  )
);

SELECT
  entity_id::BIGINT AS entity_id,
  (entity_id % 16)::INTEGER AS bucket_id,
  (entity_id * 5)::DECIMAL(38, 6) AS total_value,
  {row_count}::BIGINT AS row_count
FROM GENERATE_SERIES(1, 5) AS source(entity_id);
""",
        encoding="utf-8",
    )
    return path


def write_fan_in_view(name: str, upstreams: tuple[str, ...]) -> Path:
    if len(upstreams) < 2:
        raise ValueError("fan-in views require at least two upstream models")
    table_name = name.rsplit(".", 1)[-1]
    path = GENERATED_MODELS_PATH / f"{table_name}.sql"
    aliases = tuple(f"u{index}" for index in range(len(upstreams)))
    joins = "\n".join(
        f"JOIN {upstream} AS {alias}\n"
        f"  ON u0.entity_id = {alias}.entity_id\n"
        f"  AND u0.bucket_id = {alias}.bucket_id"
        for upstream, alias in zip(upstreams[1:], aliases[1:])
    )
    total_value = " + ".join(f"{alias}.total_value" for alias in aliases)
    row_count = " + ".join(f"{alias}.row_count" for alias in aliases)
    path.write_text(
        f"""{GENERATED_HEADER}
MODEL (
  name {name},
  kind VIEW,
  grain (entity_id, bucket_id)
);

SELECT
  u0.entity_id::BIGINT AS entity_id,
  u0.bucket_id::INTEGER AS bucket_id,
  ({total_value})::DECIMAL(38, 6) AS total_value,
  ({row_count})::BIGINT AS row_count
FROM {upstreams[0]} AS u0
{joins};
""",
        encoding="utf-8",
    )
    return path
