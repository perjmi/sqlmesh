from __future__ import annotations

import typing as t
from dataclasses import dataclass
from pathlib import Path

from sqlmesh.core.config import TableNamingConvention
from sqlmesh.core.model.graph import ProjectGraphIndex
from sqlmesh.core.model.registry import ModelPayloadStore, ModelRegistry
from sqlmesh.core.plan.store import SerializedSnapshot
from sqlmesh.core.snapshot.definition import Snapshot, SnapshotFingerprint, SnapshotId
from sqlmesh.utils.hashing import hash_data
from sqlmesh.utils.errors import SQLMeshError


@dataclass(frozen=True)
class StreamingSnapshotTask:
    name: str
    created_ts: int
    ttl: str
    table_naming_convention: TableNamingConvention


_streaming_snapshot_index: t.Optional[ProjectGraphIndex] = None
_streaming_snapshot_payload_store: t.Optional[ModelPayloadStore] = None


def init_streaming_snapshot_worker(index_path: str, payload_store_path: str) -> None:
    global _streaming_snapshot_index, _streaming_snapshot_payload_store
    _streaming_snapshot_index = ProjectGraphIndex(Path(index_path))
    _streaming_snapshot_payload_store = ModelPayloadStore(
        Path(payload_store_path), cleanup=False
    )


def build_serialized_streaming_snapshot(task: StreamingSnapshotTask) -> SerializedSnapshot:
    if _streaming_snapshot_index is None or _streaming_snapshot_payload_store is None:
        raise SQLMeshError("Streaming snapshot worker was not initialized")

    index = _streaming_snapshot_index
    metadata = index.metadata(task.name)
    node = _streaming_snapshot_payload_store.get(metadata)
    if node is None:
        raise SQLMeshError(f"Missing finalized model payload for '{task.name}'")
    snapshot = Snapshot(
        name=node.fqn,
        fingerprint=index.fingerprint(task.name),
        node=node,
        parents=tuple(
            SnapshotId(
                name=parent,
                identifier=index.fingerprint(parent).to_identifier(),
            )
            for parent in metadata.dependencies
            if index.contains(parent)
        ),
        intervals=[],
        dev_intervals=[],
        created_ts=task.created_ts,
        updated_ts=task.created_ts,
        ttl=task.ttl,
        table_naming_convention=task.table_naming_convention,
    )
    return SerializedSnapshot.from_snapshot(snapshot, is_new=False)


class StreamingFingerprinter:
    """Computes and persists fingerprints with a bounded hydrated-model working set."""

    def __init__(
        self,
        index: ProjectGraphIndex,
        registry: ModelRegistry,
        batch_size: int,
    ) -> None:
        if batch_size <= 0:
            raise ValueError("batch_size must be greater than 0")
        self._index = index
        self._registry = registry
        self._batch_size = batch_size

    def fingerprint(self) -> t.Iterator[t.Tuple[str, SnapshotFingerprint]]:
        self._index.clear_fingerprints()
        for names in self._index.iter_topological_batches(self._batch_size):
            completed: t.List[t.Tuple[str, SnapshotFingerprint]] = []
            for name in names:
                metadata = self._index.metadata(name)
                if metadata.data_hash is None or metadata.metadata_hash is None:
                    raise ValueError(f"Finalized model hashes are missing for '{name}'")
                parents = [
                    self._index.fingerprint(parent)
                    for parent in metadata.dependencies
                    if self._index.contains(parent)
                ]
                fingerprint = SnapshotFingerprint(
                    data_hash=metadata.data_hash,
                    metadata_hash=metadata.metadata_hash,
                    parent_data_hash=hash_data(sorted(parent.to_version() for parent in parents)),
                    parent_metadata_hash=hash_data(
                        sorted(
                            value
                            for parent in parents
                            for value in (
                                parent.metadata_hash,
                                parent.parent_metadata_hash,
                            )
                        )
                    ),
                )
                completed.append((name, fingerprint))
            self._index.put_fingerprints(completed)
            yield from completed
