from __future__ import annotations

import json
import sqlite3
import typing as t
import fnmatch
from contextlib import contextmanager
from pathlib import Path

from sqlmesh.core.model.registry import ModelMetadata
from sqlmesh.core.snapshot.definition import SnapshotFingerprint
from sqlmesh.utils.dag import DAG
from sqlmesh.utils.errors import SQLMeshError


class ProjectGraphWriter:
    """Transaction-scoped writer for an incrementally discovered project graph."""

    def __init__(self, connection: sqlite3.Connection) -> None:
        self._connection = connection

    def add(self, record: ModelMetadata) -> None:
        self._connection.execute(
            """
            INSERT INTO models(
                fqn, name, source_path, project, dialect, gateway, enabled,
                kind_name, tags, dbt_fqn, payload_key, payload_digest
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
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
                record.payload_key,
                record.payload_digest,
            ),
        )
        self._connection.executemany(
            "INSERT INTO model_dependencies(model_fqn, dependency_fqn) VALUES (?, ?)",
            ((record.fqn, dependency) for dependency in record.dependencies),
        )

    def contains(self, name: str) -> bool:
        return (
            self._connection.execute("SELECT 1 FROM models WHERE fqn = ?", (name,)).fetchone()
            is not None
        )


class ProjectGraphIndex:
    """A disposable, transactional SQLite index of lightweight model metadata."""

    SCHEMA_VERSION = 3

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
                connection.execute("DROP TABLE IF EXISTS model_fingerprints")
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
                    dbt_fqn TEXT,
                    payload_key TEXT NOT NULL,
                    payload_digest TEXT NOT NULL
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
                """
                CREATE TABLE IF NOT EXISTS model_fingerprints (
                    fqn TEXT PRIMARY KEY REFERENCES models(fqn) ON DELETE CASCADE,
                    data_hash TEXT NOT NULL,
                    metadata_hash TEXT NOT NULL,
                    parent_data_hash TEXT NOT NULL,
                    parent_metadata_hash TEXT NOT NULL
                )
                """
            )
            connection.execute(
                "INSERT OR REPLACE INTO graph_meta(key, value) VALUES ('schema_version', ?)",
                (str(self.SCHEMA_VERSION),),
            )

    def replace(self, records: t.Iterable[ModelMetadata]) -> None:
        """Atomically replaces the complete indexed graph."""
        with self.replacing() as writer:
            for record in records:
                writer.add(record)

    @contextmanager
    def replacing(self) -> t.Iterator[ProjectGraphWriter]:
        """Opens an atomic replacement that can be populated one record at a time."""
        connection = self._connect()
        try:
            connection.execute("DELETE FROM model_dependencies")
            connection.execute("DELETE FROM models")
            yield ProjectGraphWriter(connection)
            connection.commit()
        except Exception:
            connection.rollback()
            raise
        finally:
            connection.close()

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

    def contains(self, name: str) -> bool:
        with self._connect() as connection:
            return (
                connection.execute("SELECT 1 FROM models WHERE fqn = ?", (name,)).fetchone()
                is not None
            )

    def contains_node(self, name: str) -> bool:
        """Returns whether a model or dependency-only node exists in the indexed graph."""
        with self._connect() as connection:
            return (
                connection.execute(
                    """
                    SELECT 1 FROM models WHERE fqn = ?
                    UNION ALL
                    SELECT 1 FROM model_dependencies WHERE dependency_fqn = ?
                    LIMIT 1
                    """,
                    (name, name),
                ).fetchone()
                is not None
            )

    def model_count(self) -> int:
        with self._connect() as connection:
            row = connection.execute("SELECT COUNT(*) AS count FROM models").fetchone()
        return int(row["count"])

    def update_payload_reference(self, record: ModelMetadata) -> None:
        """Updates a payload after schema propagation without rebuilding graph metadata."""
        self.update_payload_references((record,))

    def update_payload_references(self, records: t.Iterable[ModelMetadata]) -> None:
        """Updates a bounded batch of post-schema payload references."""
        with self._connect() as connection:
            updates = tuple(
                (record.payload_key, record.payload_digest, record.fqn) for record in records
            )
            connection.executemany(
                "UPDATE models SET payload_key = ?, payload_digest = ? WHERE fqn = ?", updates
            )
            if connection.total_changes != len(updates):
                raise KeyError("One or more model payload references do not exist")

    def iter_names(self) -> t.Iterator[str]:
        with self._connect() as connection:
            for row in connection.execute("SELECT fqn FROM models ORDER BY fqn"):
                yield row["fqn"]

    def match_names(self, pattern: str) -> t.Set[str]:
        with self._connect() as connection:
            return {
                row["fqn"]
                for row in connection.execute("SELECT fqn, name FROM models")
                if fnmatch.fnmatchcase(row["name"], pattern)
            }

    def match_tags(self, pattern: str) -> t.Set[str]:
        with self._connect() as connection:
            return {
                row["fqn"]
                for row in connection.execute("SELECT fqn, tags FROM models")
                if any(
                    fnmatch.fnmatchcase(tag.lower(), pattern.lower())
                    for tag in json.loads(row["tags"])
                )
            }

    def match_kinds(self, kind_names: t.Collection[str]) -> t.Set[str]:
        if not kind_names:
            return set()
        placeholders = ", ".join("?" for _ in kind_names)
        with self._connect() as connection:
            return {
                row["fqn"]
                for row in connection.execute(
                    f"SELECT fqn FROM models WHERE kind_name IN ({placeholders})",  # noqa: S608
                    tuple(kind_names),
                )
            }

    def match_source_paths(self, paths: t.Collection[Path]) -> t.Set[str]:
        if not paths:
            return set()
        path_strings = {str(path) for path in paths}
        with self._connect() as connection:
            return {
                row["fqn"]
                for row in connection.execute(
                    "SELECT fqn, source_path FROM models WHERE source_path IS NOT NULL"
                )
                if row["source_path"] in path_strings
            }

    def iter_metadata(self, names: t.Optional[t.Iterable[str]] = None) -> t.Iterator[ModelMetadata]:
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

    def iter_metadata_batches(self, batch_size: int) -> t.Iterator[t.Tuple[ModelMetadata, ...]]:
        """Yields all metadata using keyset pagination and bounded SQLite result sets."""
        if batch_size <= 0:
            raise ValueError("batch_size must be greater than 0")

        last_fqn = ""
        while True:
            with self._connect() as connection:
                rows = connection.execute(
                    """
                    SELECT m.*, GROUP_CONCAT(d.dependency_fqn, char(31)) AS dependencies
                    FROM models AS m
                    LEFT JOIN model_dependencies AS d ON d.model_fqn = m.fqn
                    WHERE m.fqn > ?
                    GROUP BY m.fqn
                    ORDER BY m.fqn
                    LIMIT ?
                    """,
                    (last_fqn, batch_size),
                ).fetchall()
            if not rows:
                return
            batch = tuple(self._row_to_metadata(row) for row in rows)
            yield batch
            last_fqn = batch[-1].fqn

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

    def iter_topological_batches(
        self, batch_size: int, include_external: bool = False
    ) -> t.Iterator[t.Tuple[str, ...]]:
        """Yields deterministic batches with traversal state spilled to SQLite."""
        if batch_size <= 0:
            raise ValueError("batch_size must be greater than 0")

        connection = self._connect()
        try:
            connection.execute("PRAGMA temp_store = FILE")
            connection.execute(
                """
                CREATE TEMP TABLE topological_work (
                    fqn TEXT PRIMARY KEY,
                    remaining_dependencies INTEGER NOT NULL,
                    processed INTEGER NOT NULL DEFAULT 0
                )
                """
            )
            if include_external:
                connection.execute(
                    """
                    WITH graph_nodes(fqn) AS (
                        SELECT fqn FROM models
                        UNION
                        SELECT dependency_fqn FROM model_dependencies
                    )
                    INSERT INTO topological_work(fqn, remaining_dependencies)
                    SELECT node.fqn, COUNT(parent.fqn)
                    FROM graph_nodes AS node
                    LEFT JOIN model_dependencies AS d ON d.model_fqn = node.fqn
                    LEFT JOIN graph_nodes AS parent ON parent.fqn = d.dependency_fqn
                    GROUP BY node.fqn
                    """
                )
            else:
                connection.execute(
                    """
                    INSERT INTO topological_work(fqn, remaining_dependencies)
                    SELECT m.fqn, COUNT(parent.fqn)
                    FROM models AS m
                    LEFT JOIN model_dependencies AS d ON d.model_fqn = m.fqn
                    LEFT JOIN models AS parent ON parent.fqn = d.dependency_fqn
                    GROUP BY m.fqn
                    """
                )
            connection.execute(
                "CREATE INDEX topological_ready_idx ON "
                "topological_work(processed, remaining_dependencies, fqn)"
            )
            connection.execute("CREATE TEMP TABLE topological_batch (fqn TEXT PRIMARY KEY)")
            connection.commit()

            while True:
                rows = connection.execute(
                    """
                    SELECT fqn
                    FROM topological_work
                    WHERE processed = 0 AND remaining_dependencies = 0
                    ORDER BY fqn
                    LIMIT ?
                    """,
                    (batch_size,),
                ).fetchall()
                if not rows:
                    break

                batch = tuple(row["fqn"] for row in rows)
                yield batch
                connection.execute("DELETE FROM topological_batch")
                connection.executemany(
                    "INSERT INTO topological_batch(fqn) VALUES (?)",
                    ((name,) for name in batch),
                )
                connection.execute(
                    """
                    UPDATE topological_work
                    SET processed = 1
                    WHERE EXISTS (
                        SELECT 1 FROM topological_batch AS batch
                        WHERE batch.fqn = topological_work.fqn
                    )
                    """
                )
                connection.execute(
                    """
                    UPDATE topological_work
                    SET remaining_dependencies = remaining_dependencies - (
                        SELECT COUNT(*)
                        FROM model_dependencies AS d
                        JOIN topological_batch AS batch ON batch.fqn = d.dependency_fqn
                        WHERE d.model_fqn = topological_work.fqn
                    )
                    WHERE processed = 0
                      AND EXISTS (
                          SELECT 1
                          FROM model_dependencies AS d
                          JOIN topological_batch AS batch ON batch.fqn = d.dependency_fqn
                          WHERE d.model_fqn = topological_work.fqn
                      )
                    """
                )
                connection.commit()

            remaining = connection.execute(
                "SELECT COUNT(*) AS count FROM topological_work WHERE processed = 0"
            ).fetchone()["count"]
            if remaining:
                sample = tuple(
                    row["fqn"]
                    for row in connection.execute(
                        "SELECT fqn FROM topological_work WHERE processed = 0 ORDER BY fqn LIMIT 20"
                    )
                )
                raise SQLMeshError(
                    f"Detected a cycle in the indexed model graph ({remaining} affected models; "
                    f"sample: {sample})"
                )
        finally:
            connection.close()

    def clear_fingerprints(self) -> None:
        with self._connect() as connection:
            connection.execute("DELETE FROM model_fingerprints")

    def put_fingerprints(self, fingerprints: t.Iterable[t.Tuple[str, SnapshotFingerprint]]) -> None:
        with self._connect() as connection:
            connection.executemany(
                """
                INSERT OR REPLACE INTO model_fingerprints(
                    fqn, data_hash, metadata_hash, parent_data_hash, parent_metadata_hash
                ) VALUES (?, ?, ?, ?, ?)
                """,
                (
                    (
                        name,
                        fingerprint.data_hash,
                        fingerprint.metadata_hash,
                        fingerprint.parent_data_hash,
                        fingerprint.parent_metadata_hash,
                    )
                    for name, fingerprint in fingerprints
                ),
            )

    def fingerprint(self, name: str) -> SnapshotFingerprint:
        with self._connect() as connection:
            row = connection.execute(
                "SELECT * FROM model_fingerprints WHERE fqn = ?", (name,)
            ).fetchone()
        if row is None:
            raise KeyError(name)
        return SnapshotFingerprint(
            data_hash=row["data_hash"],
            metadata_hash=row["metadata_hash"],
            parent_data_hash=row["parent_data_hash"],
            parent_metadata_hash=row["parent_metadata_hash"],
        )

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
            payload_key=row["payload_key"],
            payload_digest=row["payload_digest"],
        )


class IndexedDAG(DAG[str]):
    """DAG compatibility view whose complete edge set remains in SQLite."""

    def __init__(self, index: ProjectGraphIndex, batch_size: int) -> None:
        super().__init__()
        self._index = index
        self._batch_size = batch_size

    def add(self, node: str, dependencies: t.Optional[t.Iterable[str]] = None) -> None:
        raise SQLMeshError("Indexed DAG mutations require a project reload")

    @property
    def graph(self) -> t.Dict[str, t.Set[str]]:
        graph: t.Dict[str, t.Set[str]] = {}
        for metadata in self._index.iter_metadata():
            dependencies = set(metadata.dependencies)
            graph[metadata.fqn] = dependencies
            for dependency in dependencies:
                graph.setdefault(dependency, set())
        return graph

    @property
    def sorted(self) -> t.List[str]:
        return [
            name
            for batch in self._index.iter_topological_batches(
                self._batch_size, include_external=True
            )
            for name in batch
        ]

    @property
    def roots(self) -> t.Set[str]:
        with self._index._connect() as connection:
            return {
                row["fqn"]
                for row in connection.execute(
                    """
                    WITH graph_nodes(fqn) AS (
                        SELECT fqn FROM models
                        UNION
                        SELECT dependency_fqn FROM model_dependencies
                    )
                    SELECT node.fqn
                    FROM graph_nodes AS node
                    WHERE NOT EXISTS (
                        SELECT 1
                        FROM model_dependencies AS d
                        WHERE d.model_fqn = node.fqn
                    )
                    """
                )
            }

    def upstream(self, node: str) -> t.Set[str]:
        return self._index.upstream((node,))

    def downstream(self, node: str) -> t.List[str]:
        selected = self._index.downstream((node,))
        return [name for name in self if name in selected]

    def prune(self, *nodes: str) -> DAG[str]:
        selected = set(nodes)
        dag = DAG({node: set() for node in selected if self._index.contains_node(node)})
        for metadata in self._index.iter_metadata(selected):
            dag.add(
                metadata.fqn,
                (dependency for dependency in metadata.dependencies if dependency in selected),
            )
        return dag

    def subdag(self, *nodes: str) -> DAG[str]:
        selected = set(nodes) | self._index.upstream(nodes)
        return self.prune(*selected)

    def lineage(self, node: str) -> DAG[str]:
        return self.subdag(node, *self.downstream(node))

    @property
    def reversed(self) -> DAG[str]:
        return DAG(self.graph).reversed

    def __contains__(self, item: str) -> bool:
        return self._index.contains_node(item)

    def __iter__(self) -> t.Iterator[str]:
        for batch in self._index.iter_topological_batches(self._batch_size, include_external=True):
            yield from batch
