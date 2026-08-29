# SPDX-License-Identifier: Apache-2.0

from __future__ import annotations

import datetime as dt
import decimal
import json
import os
import sys
from collections.abc import Iterable
from typing import Any

MODEL_NAMES = (
    "matrix.seed_model",
    "matrix.full_model",
    "matrix.view_model",
    "matrix.incremental_time_model",
    "matrix.incremental_unique_model",
    "matrix.scd_time_model",
    "matrix.scd_column_model",
)


def _connect() -> Any:
    engine = os.environ["MATRIX_ENGINE"]
    if engine == "postgres":
        import psycopg2

        return psycopg2.connect(
            host=os.environ["POSTGRES_HOST"],
            port=int(os.environ.get("POSTGRES_PORT", "5432")),
            user=os.environ["POSTGRES_USER"],
            password=os.environ["POSTGRES_PASSWORD"],
            dbname=os.environ["POSTGRES_DB"],
        )
    if engine == "mysql":
        import pymysql

        return pymysql.connect(
            host=os.environ["MYSQL_HOST"],
            port=int(os.environ.get("MYSQL_PORT", "3306")),
            user=os.environ["MYSQL_USER"],
            password=os.environ["MYSQL_PASSWORD"],
            database=os.environ["MYSQL_DB"],
        )
    if engine == "duckdb":
        import duckdb

        return duckdb.connect(os.environ["DUCKDB_PATH"], read_only=True)
    raise ValueError(f"Unsupported matrix engine: {engine}")


def _json_value(value: Any) -> Any:
    if isinstance(value, bytes):
        value = value.decode()
    if isinstance(value, (dt.date, dt.datetime, dt.time, decimal.Decimal)):
        return str(value)
    if isinstance(value, dict):
        return {key: _json_value(item) for key, item in sorted(value.items())}
    if isinstance(value, (list, tuple)):
        normalized = [_json_value(item) for item in value]
        if all(isinstance(item, dict) and "name" in item for item in normalized):
            normalized.sort(
                key=lambda item: json.dumps(item, sort_keys=True, separators=(",", ":"))
            )
        return normalized
    return value


def _json_blob(value: Any) -> Any:
    if value is None:
        return None
    if isinstance(value, bytes):
        value = value.decode()
    return json.loads(value) if isinstance(value, str) else value


def _rows(connection: Any, sql: str) -> list[dict[str, Any]]:
    cursor = connection.cursor()
    try:
        cursor.execute(sql)
        names = [column[0] for column in cursor.description]
        return [dict(zip(names, row)) for row in cursor.fetchall()]
    finally:
        cursor.close()


def _canonical_rows(rows: Iterable[Any]) -> list[str]:
    return sorted(
        json.dumps(_json_value(row), sort_keys=True, separators=(",", ":")) for row in rows
    )


def _state_signature(connection: Any) -> dict[str, list[str]]:
    snapshots = []
    for row in _rows(
        connection,
        """
        SELECT name, identifier, version, dev_version, kind_name, unrestorable,
               forward_only, fingerprint, snapshot
        FROM sqlmesh._snapshots
        """,
    ):
        payload = _json_blob(row.pop("snapshot"))
        row["fingerprint"] = _json_blob(row["fingerprint"])
        row.update(
            {
                key: payload.get(key)
                for key in (
                    "change_category",
                    "parents",
                    "previous_versions",
                    "dev_table_suffix",
                    "table_naming_convention",
                )
            }
        )
        snapshots.append(row)

    intervals = _rows(
        connection,
        """
        SELECT name, identifier, version, dev_version, start_ts, end_ts, is_dev,
               is_removed, is_compacted, is_pending_restatement
        FROM sqlmesh._intervals
        """,
    )

    environments = []
    for row in _rows(
        connection,
        """
        SELECT name, start_at, end_at, snapshots, promoted_snapshot_ids, suffix_target,
               catalog_name_override, previous_finalized_snapshots, normalize_name,
               gateway_managed, requirements
        FROM sqlmesh._environments
        """,
    ):
        for key in (
            "snapshots",
            "promoted_snapshot_ids",
            "previous_finalized_snapshots",
            "requirements",
        ):
            row[key] = _json_blob(row[key])
        environments.append(row)

    return {
        "snapshots": _canonical_rows(snapshots),
        "intervals": _canonical_rows(intervals),
        "environments": _canonical_rows(environments),
    }


def _data_signature(connection: Any, environment: str) -> dict[str, list[str]]:
    schema = "matrix" if environment == "prod" else f"matrix__{environment}"
    schema = f"matrix.{schema}" if os.environ["MATRIX_ENGINE"] == "duckdb" else schema
    return {
        model_name: _canonical_rows(
            tuple(row.values())
            for row in _rows(
                connection,
                f"SELECT * FROM {schema}.{model_name.split('.', 1)[1]}",
            )
        )
        for model_name in MODEL_NAMES
    }


def main() -> None:
    environment = sys.argv[1] if len(sys.argv) > 1 else "prod"
    connection = _connect()
    try:
        print(
            json.dumps(
                {
                    "state": _state_signature(connection),
                    "data": _data_signature(connection, environment),
                },
                sort_keys=True,
                separators=(",", ":"),
            )
        )
    finally:
        connection.close()


if __name__ == "__main__":
    main()
