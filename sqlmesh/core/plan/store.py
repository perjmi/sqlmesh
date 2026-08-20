from __future__ import annotations

import pickle
import sqlite3
import typing as t
from collections import OrderedDict
from collections.abc import Iterator, Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path

from sqlmesh.core.snapshot import Snapshot, SnapshotId
from sqlmesh.utils.dag import DAG
from sqlmesh.utils.errors import SQLMeshError


@dataclass(frozen=True)
class SerializedSnapshot:
    """A worker-produced snapshot payload that can be inserted without coordinator hydration."""

    snapshot_id: SnapshotId
    parents: t.Tuple[SnapshotId, ...]
    fingerprint: bytes
    payload: bytes
    is_new: bool

    @classmethod
    def from_snapshot(cls, snapshot: Snapshot, *, is_new: bool) -> SerializedSnapshot:
        return cls(
            snapshot_id=snapshot.snapshot_id,
            parents=snapshot.parents,
            fingerprint=pickle.dumps(snapshot.fingerprint, protocol=pickle.HIGHEST_PROTOCOL),
            payload=pickle.dumps(snapshot, protocol=pickle.HIGHEST_PROTOCOL),
            is_new=is_new,
        )


@dataclass(frozen=True)
class SerializedSnapshotUpdate:
    """A worker-produced snapshot payload update that preserves graph metadata."""

    snapshot_id: SnapshotId
    payload: bytes

    @classmethod
    def from_snapshot(cls, snapshot: Snapshot) -> SerializedSnapshotUpdate:
        return cls(
            snapshot_id=snapshot.snapshot_id,
            payload=pickle.dumps(snapshot, protocol=pickle.HIGHEST_PROTOCOL),
        )


class IndexedSnapshotMapping(Mapping[SnapshotId, Snapshot]):
    """A mapping view over snapshots whose payloads live in a plan store."""

    def __init__(self, store: SnapshotPlanStore, *, new_only: bool = False) -> None:
        self._store = store
        self._new_only = new_only

    @property
    def store(self) -> SnapshotPlanStore:
        return self._store

    def __getitem__(self, snapshot_id: SnapshotId) -> Snapshot:
        return self._store.get_snapshot(snapshot_id, new_only=self._new_only)

    def __iter__(self) -> Iterator[SnapshotId]:
        yield from self._store.iter_snapshot_ids(new_only=self._new_only)

    def __len__(self) -> int:
        return self._store.snapshot_count(new_only=self._new_only)

    def __contains__(self, value: object) -> bool:
        return isinstance(value, SnapshotId) and self._store.contains_snapshot(
            value, new_only=self._new_only
        )


class IndexedSnapshotNameMapping(Mapping[str, Snapshot]):
    """A name-keyed mapping view over snapshots in a plan store."""

    def __init__(self, store: SnapshotPlanStore) -> None:
        self._store = store

    @property
    def store(self) -> SnapshotPlanStore:
        return self._store

    def __getitem__(self, name: str) -> Snapshot:
        return self._store.get_snapshot_by_name(name)

    def __iter__(self) -> Iterator[str]:
        for snapshot_id in self._store.iter_snapshot_ids():
            yield snapshot_id.name

    def __len__(self) -> int:
        return self._store.snapshot_count()

    def __contains__(self, value: object) -> bool:
        return isinstance(value, str) and self._store.contains_snapshot_name(value)


class IndexedSnapshotSequence(Sequence[Snapshot]):
    """A list-compatible lazy sequence over new snapshots in a plan store."""

    def __init__(self, store: SnapshotPlanStore) -> None:
        self._store = store

    def __len__(self) -> int:
        return self._store.snapshot_count(new_only=True)

    def __iter__(self) -> Iterator[Snapshot]:
        yield from self._store.new_snapshots.values()

    @t.overload
    def __getitem__(self, index: int) -> Snapshot: ...

    @t.overload
    def __getitem__(self, index: slice) -> t.Sequence[Snapshot]: ...

    def __getitem__(self, index: t.Union[int, slice]) -> t.Union[Snapshot, t.Sequence[Snapshot]]:
        if isinstance(index, slice):
            start, stop, step = index.indices(len(self))
            return [self[offset] for offset in range(start, stop, step)]

        if index < 0:
            index += len(self)
        if index < 0 or index >= len(self):
            raise IndexError(index)
        return self._store.get_snapshot(self._store.snapshot_id_at_offset(index, new_only=True))


class IndexedSnapshotDAG(DAG[SnapshotId]):
    """DAG compatibility view backed by the plan store's edge table."""

    def __init__(self, store: SnapshotPlanStore, batch_size: int) -> None:
        super().__init__()
        self._store = store
        self._batch_size = batch_size

    def add(
        self,
        node: SnapshotId,
        dependencies: t.Optional[t.Iterable[SnapshotId]] = None,
    ) -> None:
        raise SQLMeshError("Indexed snapshot DAG mutations require rebuilding the plan store")

    def __iter__(self) -> Iterator[SnapshotId]:
        for batch in self._store.iter_topological_batches(self._batch_size):
            yield from batch

    def upstream(self, node: SnapshotId) -> t.Set[SnapshotId]:
        return self._store.upstream(node)

    def downstream(self, node: SnapshotId) -> t.List[SnapshotId]:
        downstream = self._store.downstream(node)
        return [snapshot_id for snapshot_id in self if snapshot_id in downstream]

    def subdag(self, *nodes: SnapshotId) -> DAG[SnapshotId]:
        selected = set(nodes)
        for node in nodes:
            selected.update(self._store.upstream(node))
        dag: DAG[SnapshotId] = DAG()
        for snapshot_id in selected:
            dag.add(
                snapshot_id,
                (parent for parent in self._store.parents(snapshot_id) if parent in selected),
            )
        return dag


class SnapshotPlanStore:
    """SQLite-backed snapshot payloads and edges for a single planning run.

    Snapshot objects are hydrated through an entry-bounded LRU. Planner mutations are queued with
    :meth:`save_snapshot` and persisted in bounded transactions by :meth:`flush`.
    """

    SCHEMA_VERSION = 2

    def __init__(
        self,
        path: Path,
        *,
        max_cached_snapshots: int,
        write_batch_size: int = 100,
    ) -> None:
        if max_cached_snapshots < 0:
            raise ValueError("max_cached_snapshots must be greater than or equal to 0")
        if write_batch_size <= 0:
            raise ValueError("write_batch_size must be greater than 0")
        self.path = path
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._max_cached_snapshots = max_cached_snapshots
        self._write_batch_size = write_batch_size
        self._cache: OrderedDict[SnapshotId, Snapshot] = OrderedDict()
        self._pending_payloads: OrderedDict[SnapshotId, bytes] = OrderedDict()
        self.max_cache_size_seen = 0
        self._initialize()
        self.snapshots = IndexedSnapshotMapping(self)
        self.new_snapshots = IndexedSnapshotMapping(self, new_only=True)
        self.snapshots_by_name = IndexedSnapshotNameMapping(self)
        self.new_snapshot_sequence = IndexedSnapshotSequence(self)

    @property
    def cache_size(self) -> int:
        return len(self._cache)

    @property
    def max_cached_snapshots(self) -> int:
        return self._max_cached_snapshots

    @property
    def write_batch_size(self) -> int:
        return self._write_batch_size

    def _connect(self) -> sqlite3.Connection:
        connection = sqlite3.connect(self.path)
        connection.row_factory = sqlite3.Row
        return connection

    def _initialize(self) -> None:
        with self._connect() as connection:
            connection.execute("PRAGMA journal_mode = WAL")
            connection.execute(
                "CREATE TABLE IF NOT EXISTS plan_meta (key TEXT PRIMARY KEY, value TEXT NOT NULL)"
            )
            row = connection.execute(
                "SELECT value FROM plan_meta WHERE key = 'schema_version'"
            ).fetchone()
            if row is not None and int(row["value"]) != self.SCHEMA_VERSION:
                connection.execute("DROP TABLE IF EXISTS snapshot_edges")
                connection.execute("DROP TABLE IF EXISTS snapshots")
                connection.execute("DELETE FROM plan_meta")

            connection.execute(
                """
                CREATE TABLE IF NOT EXISTS snapshots (
                    name TEXT NOT NULL,
                    identifier TEXT NOT NULL,
                    fingerprint BLOB NOT NULL,
                    payload BLOB NOT NULL,
                    is_new INTEGER NOT NULL,
                    PRIMARY KEY (name, identifier)
                )
                """
            )
            connection.execute(
                "CREATE INDEX IF NOT EXISTS snapshots_new_idx "
                "ON snapshots(is_new, name, identifier)"
            )
            connection.execute(
                """
                CREATE TABLE IF NOT EXISTS snapshot_edges (
                    child_name TEXT NOT NULL,
                    child_identifier TEXT NOT NULL,
                    parent_name TEXT NOT NULL,
                    parent_identifier TEXT NOT NULL,
                    PRIMARY KEY (
                        child_name, child_identifier, parent_name, parent_identifier
                    )
                )
                """
            )
            connection.execute(
                "CREATE INDEX IF NOT EXISTS snapshot_edges_parent_idx "
                "ON snapshot_edges(parent_name, parent_identifier, child_name, child_identifier)"
            )
            connection.execute(
                "INSERT OR REPLACE INTO plan_meta(key, value) VALUES ('schema_version', ?)",
                (str(self.SCHEMA_VERSION),),
            )

    def put_snapshot(self, snapshot: Snapshot, *, is_new: bool) -> None:
        with self._connect() as connection:
            self._write_snapshot(connection, snapshot, is_new=is_new)
        self._cache.pop(snapshot.snapshot_id, None)
        self._pending_payloads.pop(snapshot.snapshot_id, None)

    def put_snapshots(self, snapshots: t.Iterable[t.Tuple[Snapshot, bool]]) -> None:
        """Writes a bounded batch of snapshots in one transaction."""
        records = tuple(snapshots)
        with self._connect() as connection:
            for snapshot, is_new in records:
                self._write_snapshot(connection, snapshot, is_new=is_new)
        for snapshot, _ in records:
            self._cache.pop(snapshot.snapshot_id, None)
            self._pending_payloads.pop(snapshot.snapshot_id, None)

    def put_serialized_snapshots(self, snapshots: t.Iterable[SerializedSnapshot]) -> None:
        """Writes worker-produced payloads without hydrating models in the coordinator."""
        records = tuple(snapshots)
        with self._connect() as connection:
            for snapshot in records:
                self._write_serialized_snapshot(connection, snapshot)
        for snapshot in records:
            self._cache.pop(snapshot.snapshot_id, None)
            self._pending_payloads.pop(snapshot.snapshot_id, None)

    @staticmethod
    def _write_snapshot(
        connection: sqlite3.Connection, snapshot: Snapshot, *, is_new: bool
    ) -> None:
        SnapshotPlanStore._write_serialized_snapshot(
            connection,
            SerializedSnapshot.from_snapshot(snapshot, is_new=is_new),
        )

    @staticmethod
    def _write_serialized_snapshot(
        connection: sqlite3.Connection, snapshot: SerializedSnapshot
    ) -> None:
        snapshot_id = snapshot.snapshot_id
        connection.execute(
            """
            INSERT OR REPLACE INTO snapshots(name, identifier, fingerprint, payload, is_new)
            VALUES (?, ?, ?, ?, ?)
            """,
            (
                snapshot_id.name,
                snapshot_id.identifier,
                sqlite3.Binary(snapshot.fingerprint),
                sqlite3.Binary(snapshot.payload),
                snapshot.is_new,
            ),
        )
        connection.execute(
            "DELETE FROM snapshot_edges WHERE child_name = ? AND child_identifier = ?",
            (snapshot_id.name, snapshot_id.identifier),
        )
        connection.executemany(
            """
            INSERT INTO snapshot_edges(
                child_name, child_identifier, parent_name, parent_identifier
            ) VALUES (?, ?, ?, ?)
            """,
            (
                (
                    snapshot_id.name,
                    snapshot_id.identifier,
                    parent.name,
                    parent.identifier,
                )
                for parent in snapshot.parents
            ),
        )

    def get_fingerprint(self, snapshot_id: SnapshotId) -> t.Any:
        with self._connect() as connection:
            row = connection.execute(
                "SELECT fingerprint FROM snapshots WHERE name = ? AND identifier = ?",
                (snapshot_id.name, snapshot_id.identifier),
            ).fetchone()
        if row is None:
            raise KeyError(snapshot_id)
        return pickle.loads(row["fingerprint"])

    def mark_new(self, snapshot_ids: t.Iterable[SnapshotId]) -> None:
        records = tuple(snapshot_ids)
        with self._connect() as connection:
            connection.executemany(
                "UPDATE snapshots SET is_new = 1 WHERE name = ? AND identifier = ?",
                ((snapshot_id.name, snapshot_id.identifier) for snapshot_id in records),
            )

    def get_snapshot(self, snapshot_id: SnapshotId, *, new_only: bool = False) -> Snapshot:
        cached = self._cache.get(snapshot_id)
        if cached is not None and (
            not new_only or self.contains_snapshot(snapshot_id, new_only=True)
        ):
            self._cache.move_to_end(snapshot_id)
            return cached

        pending_payload = self._pending_payloads.get(snapshot_id)
        if pending_payload is not None and (
            not new_only or self.contains_snapshot(snapshot_id, new_only=True)
        ):
            snapshot = t.cast(Snapshot, pickle.loads(pending_payload))
            self._cache_snapshot(snapshot)
            return snapshot

        new_predicate = " AND is_new = 1" if new_only else ""
        with self._connect() as connection:
            row = connection.execute(
                "SELECT payload FROM snapshots WHERE name = ? AND identifier = ?" + new_predicate,
                (snapshot_id.name, snapshot_id.identifier),
            ).fetchone()
        if row is None:
            raise KeyError(snapshot_id)

        snapshot = t.cast(Snapshot, pickle.loads(row["payload"]))
        self._cache_snapshot(snapshot)
        return snapshot

    def get_snapshot_by_name(self, name: str) -> Snapshot:
        return self.get_snapshot(self.get_snapshot_id_by_name(name))

    def get_snapshot_id_by_name(self, name: str) -> SnapshotId:
        with self._connect() as connection:
            row = connection.execute(
                "SELECT identifier FROM snapshots WHERE name = ?", (name,)
            ).fetchone()
        if row is None:
            raise KeyError(name)
        return SnapshotId(name=name, identifier=row["identifier"])

    def contains_snapshot_name(self, name: str) -> bool:
        with self._connect() as connection:
            return (
                connection.execute("SELECT 1 FROM snapshots WHERE name = ?", (name,)).fetchone()
                is not None
            )

    def contains_snapshot(self, snapshot_id: SnapshotId, *, new_only: bool = False) -> bool:
        new_predicate = " AND is_new = 1" if new_only else ""
        with self._connect() as connection:
            return (
                connection.execute(
                    "SELECT 1 FROM snapshots WHERE name = ? AND identifier = ?" + new_predicate,
                    (snapshot_id.name, snapshot_id.identifier),
                ).fetchone()
                is not None
            )

    def iter_snapshot_ids(self, *, new_only: bool = False) -> Iterator[SnapshotId]:
        last_name = ""
        last_identifier = ""
        while True:
            new_predicate = "is_new = 1 AND " if new_only else ""
            with self._connect() as connection:
                rows = connection.execute(
                    "SELECT name, identifier FROM snapshots WHERE "
                    + new_predicate
                    + "(name > ? OR (name = ? AND identifier > ?)) "
                    "ORDER BY name, identifier LIMIT 100",
                    (last_name, last_name, last_identifier),
                ).fetchall()
            if not rows:
                return
            for row in rows:
                yield SnapshotId(name=row["name"], identifier=row["identifier"])
            last_name = rows[-1]["name"]
            last_identifier = rows[-1]["identifier"]

    def snapshot_count(self, *, new_only: bool = False) -> int:
        new_predicate = " WHERE is_new = 1" if new_only else ""
        with self._connect() as connection:
            row = connection.execute(
                "SELECT COUNT(*) AS count FROM snapshots" + new_predicate
            ).fetchone()
        return int(row["count"])

    def snapshot_id_at_offset(self, offset: int, *, new_only: bool = False) -> SnapshotId:
        new_predicate = " WHERE is_new = 1" if new_only else ""
        with self._connect() as connection:
            row = connection.execute(
                "SELECT name, identifier FROM snapshots"
                + new_predicate
                + " ORDER BY name, identifier LIMIT 1 OFFSET ?",
                (offset,),
            ).fetchone()
        if row is None:
            raise IndexError(offset)
        return SnapshotId(name=row["name"], identifier=row["identifier"])

    def flush(self) -> None:
        self._flush_pending_payloads()

    def save_snapshot(self, snapshot: Snapshot) -> None:
        """Queues a mutated snapshot payload for bounded write-back."""
        self._persist_payload(snapshot)

    def save_snapshots(self, snapshots: t.Iterable[Snapshot]) -> None:
        """Immediately persists a bounded batch without changing graph metadata."""
        self.save_serialized_snapshot_updates(
            SerializedSnapshotUpdate.from_snapshot(snapshot) for snapshot in snapshots
        )

    def save_serialized_snapshot_updates(
        self,
        snapshots: t.Iterable[SerializedSnapshotUpdate],
        *,
        connection: t.Optional[sqlite3.Connection] = None,
    ) -> None:
        """Persists bounded worker-produced payload updates through the coordinator writer."""
        records = tuple(snapshots)
        if connection is not None:
            self._save_serialized_snapshot_updates(connection, records)
        else:
            with self._connect() as owned_connection:
                self._save_serialized_snapshot_updates(owned_connection, records)

        for snapshot in records:
            self._cache.pop(snapshot.snapshot_id, None)
            self._pending_payloads.pop(snapshot.snapshot_id, None)

    @staticmethod
    def _save_serialized_snapshot_updates(
        connection: sqlite3.Connection, snapshots: t.Iterable[SerializedSnapshotUpdate]
    ) -> None:
        connection.executemany(
            "UPDATE snapshots SET payload = ? WHERE name = ? AND identifier = ?",
            (
                (
                    sqlite3.Binary(snapshot.payload),
                    snapshot.snapshot_id.name,
                    snapshot.snapshot_id.identifier,
                )
                for snapshot in snapshots
            ),
        )

    def clear_cache(self) -> None:
        self.flush()
        self._cache.clear()

    def _cache_snapshot(self, snapshot: Snapshot) -> None:
        snapshot_id = snapshot.snapshot_id
        self._cache.pop(snapshot_id, None)

        if not self._max_cached_snapshots:
            return
        self._cache[snapshot_id] = snapshot
        while len(self._cache) > self._max_cached_snapshots:
            self._cache.popitem(last=False)
        self.max_cache_size_seen = max(self.max_cache_size_seen, len(self._cache))

    def _persist_payload(self, snapshot: Snapshot) -> None:
        snapshot_id = snapshot.snapshot_id
        self._pending_payloads[snapshot_id] = pickle.dumps(
            snapshot, protocol=pickle.HIGHEST_PROTOCOL
        )
        self._pending_payloads.move_to_end(snapshot_id)
        if len(self._pending_payloads) >= self._write_batch_size:
            self._flush_pending_payloads()

    def _flush_pending_payloads(self) -> None:
        if not self._pending_payloads:
            return
        with self._connect() as connection:
            connection.executemany(
                """
                UPDATE snapshots SET payload = ? WHERE name = ? AND identifier = ?
                """,
                (
                    (sqlite3.Binary(payload), snapshot_id.name, snapshot_id.identifier)
                    for snapshot_id, payload in self._pending_payloads.items()
                ),
            )
        self._pending_payloads.clear()

    def iter_topological_batches(self, batch_size: int) -> Iterator[t.Tuple[SnapshotId, ...]]:
        if batch_size <= 0:
            raise ValueError("batch_size must be greater than 0")

        connection = self._connect()
        try:
            connection.execute("PRAGMA temp_store = FILE")
            connection.execute(
                """
                CREATE TEMP TABLE topological_work (
                    name TEXT NOT NULL,
                    identifier TEXT NOT NULL,
                    remaining_dependencies INTEGER NOT NULL,
                    processed INTEGER NOT NULL DEFAULT 0,
                    PRIMARY KEY(name, identifier)
                )
                """
            )
            connection.execute(
                """
                INSERT INTO topological_work(name, identifier, remaining_dependencies)
                SELECT s.name, s.identifier, COUNT(parent.name)
                FROM snapshots AS s
                LEFT JOIN snapshot_edges AS edge
                    ON edge.child_name = s.name AND edge.child_identifier = s.identifier
                LEFT JOIN snapshots AS parent
                    ON parent.name = edge.parent_name
                    AND parent.identifier = edge.parent_identifier
                GROUP BY s.name, s.identifier
                """
            )
            connection.execute(
                "CREATE INDEX topological_ready_idx ON "
                "topological_work(processed, remaining_dependencies, name, identifier)"
            )
            connection.execute(
                "CREATE TEMP TABLE topological_wave "
                "(name TEXT NOT NULL, identifier TEXT NOT NULL, PRIMARY KEY(name, identifier))"
            )

            while True:
                connection.execute("DELETE FROM topological_wave")
                connection.execute(
                    """
                    INSERT INTO topological_wave(name, identifier)
                    SELECT name, identifier FROM topological_work
                    WHERE processed = 0 AND remaining_dependencies = 0
                    """
                )
                wave_size = connection.execute(
                    "SELECT COUNT(*) AS count FROM topological_wave"
                ).fetchone()["count"]
                if not wave_size:
                    break

                last_name: t.Optional[str] = None
                last_identifier: t.Optional[str] = None
                while True:
                    if last_name is None:
                        rows = connection.execute(
                            """
                            SELECT name, identifier FROM topological_wave
                            ORDER BY name, identifier LIMIT ?
                            """,
                            (batch_size,),
                        ).fetchall()
                    else:
                        rows = connection.execute(
                            """
                            SELECT name, identifier FROM topological_wave
                            WHERE name > ? OR (name = ? AND identifier > ?)
                            ORDER BY name, identifier LIMIT ?
                            """,
                            (last_name, last_name, last_identifier, batch_size),
                        ).fetchall()
                    if not rows:
                        break
                    last_name = rows[-1]["name"]
                    last_identifier = rows[-1]["identifier"]
                    yield tuple(
                        SnapshotId(name=row["name"], identifier=row["identifier"]) for row in rows
                    )

                connection.execute(
                    """
                    UPDATE topological_work SET processed = 1
                    WHERE EXISTS (
                        SELECT 1 FROM topological_wave AS wave
                        WHERE wave.name = topological_work.name
                        AND wave.identifier = topological_work.identifier
                    )
                    """
                )
                connection.execute(
                    """
                    UPDATE topological_work
                    SET remaining_dependencies = remaining_dependencies - (
                        SELECT COUNT(*) FROM snapshot_edges AS edge
                        JOIN topological_wave AS wave
                            ON wave.name = edge.parent_name
                            AND wave.identifier = edge.parent_identifier
                        WHERE edge.child_name = topological_work.name
                        AND edge.child_identifier = topological_work.identifier
                    )
                    WHERE processed = 0 AND EXISTS (
                        SELECT 1 FROM snapshot_edges AS edge
                        JOIN topological_wave AS wave
                            ON wave.name = edge.parent_name
                            AND wave.identifier = edge.parent_identifier
                        WHERE edge.child_name = topological_work.name
                        AND edge.child_identifier = topological_work.identifier
                    )
                    """
                )

            remaining = connection.execute(
                "SELECT COUNT(*) AS count FROM topological_work WHERE processed = 0"
            ).fetchone()["count"]
            if remaining:
                raise SQLMeshError(
                    f"Detected a cycle in the snapshot graph ({remaining} snapshots)"
                )
        finally:
            connection.close()

    def upstream(self, snapshot_id: SnapshotId) -> t.Set[SnapshotId]:
        return self._traverse(snapshot_id, upstream=True)

    def downstream(self, snapshot_id: SnapshotId) -> t.Set[SnapshotId]:
        return self._traverse(snapshot_id, upstream=False)

    def snapshots_with_upstream(self, names: t.Iterable[str]) -> t.Set[SnapshotId]:
        """Returns named snapshots and their ancestors using one SQLite traversal."""
        selected_names = tuple(set(names))
        if not selected_names:
            return set()

        with self._connect() as connection:
            connection.execute(
                "CREATE TEMP TABLE selected_snapshot_names "
                "(name TEXT NOT NULL PRIMARY KEY) WITHOUT ROWID"
            )
            connection.executemany(
                "INSERT INTO selected_snapshot_names(name) VALUES (?)",
                ((name,) for name in selected_names),
            )
            rows = connection.execute(
                """
                WITH RECURSIVE selected(name, identifier) AS (
                    SELECT snapshot.name, snapshot.identifier
                    FROM snapshots AS snapshot
                    JOIN selected_snapshot_names AS requested
                      ON requested.name = snapshot.name
                    UNION
                    SELECT edge.parent_name, edge.parent_identifier
                    FROM snapshot_edges AS edge
                    JOIN selected
                      ON selected.name = edge.child_name
                     AND selected.identifier = edge.child_identifier
                )
                SELECT name, identifier FROM selected
                """
            )
            return {SnapshotId(name=row["name"], identifier=row["identifier"]) for row in rows}

    def parents(self, snapshot_id: SnapshotId) -> t.Tuple[SnapshotId, ...]:
        with self._connect() as connection:
            rows = connection.execute(
                """
                SELECT parent_name, parent_identifier FROM snapshot_edges
                WHERE child_name = ? AND child_identifier = ?
                ORDER BY parent_name, parent_identifier
                """,
                (snapshot_id.name, snapshot_id.identifier),
            )
            return tuple(
                SnapshotId(name=row["parent_name"], identifier=row["parent_identifier"])
                for row in rows
            )

    def _traverse(self, snapshot_id: SnapshotId, *, upstream: bool) -> t.Set[SnapshotId]:
        if upstream:
            seed_name, seed_identifier = "parent_name", "parent_identifier"
            join_left_name, join_left_identifier = "child_name", "child_identifier"
            join_right_name, join_right_identifier = "parent_name", "parent_identifier"
        else:
            seed_name, seed_identifier = "child_name", "child_identifier"
            join_left_name, join_left_identifier = "parent_name", "parent_identifier"
            join_right_name, join_right_identifier = "child_name", "child_identifier"

        query = f"""
            WITH RECURSIVE related(name, identifier) AS (
                SELECT {seed_name}, {seed_identifier} FROM snapshot_edges
                WHERE {join_left_name} = ? AND {join_left_identifier} = ?
                UNION
                SELECT edge.{join_right_name}, edge.{join_right_identifier}
                FROM snapshot_edges AS edge
                JOIN related
                    ON edge.{join_left_name} = related.name
                    AND edge.{join_left_identifier} = related.identifier
            )
            SELECT name, identifier FROM related
        """
        with self._connect() as connection:
            rows = connection.execute(query, (snapshot_id.name, snapshot_id.identifier))
            return {SnapshotId(name=row["name"], identifier=row["identifier"]) for row in rows}
