from __future__ import annotations

import typing as t
from dataclasses import dataclass

from sqlmesh.core.model.graph import ProjectGraphIndex
from sqlmesh.core.snapshot.definition import SnapshotId, SnapshotTableInfo


@dataclass(frozen=True)
class CompactContextDiff:
    """The payload-free first pass of an environment context diff."""

    added: t.Set[SnapshotId]
    removed: t.Dict[SnapshotId, SnapshotTableInfo]
    modified: t.Dict[str, t.Tuple[SnapshotId, SnapshotTableInfo]]
    unchanged: t.Dict[str, SnapshotTableInfo]

    @classmethod
    def create(
        cls,
        index: ProjectGraphIndex,
        environment_snapshots: t.Iterable[SnapshotTableInfo],
        local_snapshot_ids: t.Optional[t.Mapping[str, SnapshotId]] = None,
    ) -> CompactContextDiff:
        remote_by_name = {snapshot.name: snapshot for snapshot in environment_snapshots}
        local_names = (
            set(local_snapshot_ids) if local_snapshot_ids is not None else set(index.iter_names())
        )
        added: t.Set[SnapshotId] = set()
        removed: t.Dict[SnapshotId, SnapshotTableInfo] = {}
        modified: t.Dict[str, t.Tuple[SnapshotId, SnapshotTableInfo]] = {}
        unchanged: t.Dict[str, SnapshotTableInfo] = {}

        for name in local_names:
            if local_snapshot_ids is not None:
                snapshot_id = local_snapshot_ids[name]
            else:
                fingerprint = index.fingerprint(name)
                snapshot_id = SnapshotId(name=name, identifier=fingerprint.to_identifier())
            remote = remote_by_name.get(name)
            if remote is None:
                added.add(snapshot_id)
            elif not remote.is_model:
                added.add(snapshot_id)
                removed[remote.snapshot_id] = remote
            elif snapshot_id != remote.snapshot_id:
                modified[name] = (snapshot_id, remote)
            else:
                unchanged[name] = remote

        for name, remote in remote_by_name.items():
            if name not in local_names and remote.is_model:
                removed[remote.snapshot_id] = remote

        return cls(
            added=added,
            removed=removed,
            modified=modified,
            unchanged=unchanged,
        )
