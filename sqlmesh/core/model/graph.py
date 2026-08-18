from __future__ import annotations

import json
import sqlite3
import typing as t
from pathlib import Path

from sqlmesh.core.model.registry import ModelMetadata


class ProjectGraphIndex:
    """A disposable, transactional SQLite index of lightweight model metadata."""

    SCHEMA_VERSION = 1

    def __init__(self, path: Path) -> None:
        self.path = path
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._initialize()

    def _connect(self) -> sqlite3.Connection:
        connection = sqlite3.connect(self.path)
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA foreign_keys = ON")
        return connection

    def _initialize(self) -> None:
        with self._connect() as connection:
            connection.execute(
                "CREATE TABLE IF NOT EXISTS graph_meta (key TEXT PRIMARY KEY, value TEXT NOT NULL)"
            )
            row = connection.execute(
                "SELECT value FROM graph_meta WHERE key = 'schema_version'"
            ).fetchone()
            if row is not None and int(row["value"]) != self.SCHEMA_VERSION:
                connection.execute("DROP TABLE IF EXISTS model_dependencies")
                connection.execute("DROP TABLE IF EXISTS models")
                connection.execute("DELETE FROM graph_meta")

            connection.execute(
                """
                CREATE TABLE IF NOT EXISTS models (
                    fqn TEXT PRIMARY KEY,
                    name TEXT NOT NULL,
                    source_path TEXT,
                    project TEXT NOT NULL,
                    dialect TEXT NOT NULL,
                    gateway TEXT,
                    enabled INTEGER NOT NULL,
                    kind_name TEXT NOT NULL,
                    tags TEXT NOT NULL,
                    dbt_fqn TEXT
                )
                """
            )
            connection.execute(
                """
                CREATE TABLE IF NOT EXISTS model_dependencies (
                    model_fqn TEXT NOT NULL REFERENCES models(fqn) ON DELETE CASCADE,
                    dependency_fqn TEXT NOT NULL,
                    PRIMARY KEY (model_fqn, dependency_fqn)
                )
                """
            )
            connection.execute(
                "CREATE INDEX IF NOT EXISTS model_dependencies_reverse_idx "
                "ON model_dependencies(dependency_fqn, model_fqn)"
            )
            connection.execute(
                "INSERT OR REPLACE INTO graph_meta(key, value) VALUES ('schema_version', ?)",
                (str(self.SCHEMA_VERSION),),
            )

    def replace(self, records: t.Iterable[ModelMetadata]) -> None:
        """Atomically replaces the complete indexed graph."""
        with self._connect() as connection:
            connection.execute("DELETE FROM model_dependencies")
            connection.execute("DELETE FROM models")
            for record in records:
                connection.execute(
                    """
                    INSERT INTO models(
                        fqn, name, source_path, project, dialect, gateway, enabled,
                        kind_name, tags, dbt_fqn
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        record.fqn,
                        record.name,
                        str(record.source_path) if record.source_path is not None else None,
                        record.project,
                        record.dialect,
                        record.gateway,
                        record.enabled,
                        record.kind_name,
                        json.dumps(record.tags),
                        record.dbt_fqn,
                    ),
                )
                connection.executemany(
                    "INSERT INTO model_dependencies(model_fqn, dependency_fqn) VALUES (?, ?)",
                    ((record.fqn, dependency) for dependency in record.dependencies),
                )

    def metadata(self, name: str) -> ModelMetadata:
        with self._connect() as connection:
            row = connection.execute(
                """
                SELECT m.*, GROUP_CONCAT(d.dependency_fqn, char(31)) AS dependencies
                FROM models AS m
                LEFT JOIN model_dependencies AS d ON d.model_fqn = m.fqn
                WHERE m.fqn = ?
                GROUP BY m.fqn
                """,
                (name,),
            ).fetchone()
        if row is None:
            raise KeyError(name)
        return self._row_to_metadata(row)

    def iter_metadata(
        self, names: t.Optional[t.Iterable[str]] = None
    ) -> t.Iterator[ModelMetadata]:
        selected_names = None if names is None else set(names)
        with self._connect() as connection:
            rows = connection.execute(
                """
                SELECT m.*, GROUP_CONCAT(d.dependency_fqn, char(31)) AS dependencies
                FROM models AS m
                LEFT JOIN model_dependencies AS d ON d.model_fqn = m.fqn
                GROUP BY m.fqn
                ORDER BY m.fqn
                """
            )
            for row in rows:
                if selected_names is None or row["fqn"] in selected_names:
                    yield self._row_to_metadata(row)

    def upstream(self, names: t.Iterable[str]) -> t.Set[str]:
        result: t.Set[str] = set()
        with self._connect() as connection:
            for name in set(names):
                rows = connection.execute(
                    """
                    WITH RECURSIVE upstream(fqn) AS (
                        SELECT dependency_fqn FROM model_dependencies WHERE model_fqn = ?
                        UNION
                        SELECT d.dependency_fqn
                        FROM model_dependencies AS d
                        JOIN upstream AS u ON d.model_fqn = u.fqn
                    )
                    SELECT fqn FROM upstream
                    """,
                    (name,),
                )
                result.update(row["fqn"] for row in rows)
        return result

    def downstream(self, names: t.Iterable[str]) -> t.Set[str]:
        result: t.Set[str] = set()
        with self._connect() as connection:
            for name in set(names):
                rows = connection.execute(
                    """
                    WITH RECURSIVE downstream(fqn) AS (
                        SELECT model_fqn FROM model_dependencies WHERE dependency_fqn = ?
                        UNION
                        SELECT d.model_fqn
                        FROM model_dependencies AS d
                        JOIN downstream AS downstream_model ON d.dependency_fqn = downstream_model.fqn
                    )
                    SELECT fqn FROM downstream
                    """,
                    (name,),
                )
                result.update(row["fqn"] for row in rows)
        return result

    @staticmethod
    def _row_to_metadata(row: sqlite3.Row) -> ModelMetadata:
        dependencies = row["dependencies"]
        return ModelMetadata(
            fqn=row["fqn"],
            name=row["name"],
            source_path=Path(row["source_path"]) if row["source_path"] is not None else None,
            project=row["project"],
            dialect=row["dialect"],
            gateway=row["gateway"],
            enabled=bool(row["enabled"]),
            kind_name=row["kind_name"],
            dependencies=tuple(sorted(dependencies.split(chr(31)))) if dependencies else (),
            tags=tuple(json.loads(row["tags"])),
            dbt_fqn=row["dbt_fqn"],
        )
