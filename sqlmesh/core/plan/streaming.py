from __future__ import annotations

import pickle
import sqlite3
import typing as t
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path

from sqlmesh.core.plan.store import SerializedSnapshotUpdate
from sqlmesh.core.snapshot import Snapshot, SnapshotChangeCategory, SnapshotId
from sqlmesh.utils.date import TimeLike, to_datetime, to_timestamp
from sqlmesh.utils.errors import SQLMeshError


_streaming_plan_connection: t.Optional[sqlite3.Connection] = None


@dataclass(frozen=True)
class StreamingCategorizationTask:
    snapshot_id: SnapshotId
    category: SnapshotChangeCategory
    forward_only: bool
    effective_from: t.Optional[TimeLike]


@dataclass(frozen=True)
class StreamingDeployabilityTask:
    snapshot_id: SnapshotId
    plan_start: t.Optional[TimeLike]
    start_override: t.Optional[TimeLike]
    parent_start_ts: t.Optional[int]
    parent_children_deployable: t.Optional[bool]
    evaluation_time: datetime


@dataclass(frozen=True)
class StreamingDeployabilityResult:
    snapshot_id: SnapshotId
    start_ts: int
    deployable: bool
    children_deployable: bool
    representative: bool


def init_streaming_plan_worker(store_path: str) -> None:
    """Initializes one read-only snapshot connection per disposable worker."""
    global _streaming_plan_connection
    close_streaming_plan_worker()
    store_uri = f"{Path(store_path).resolve().as_uri()}?mode=ro"
    _streaming_plan_connection = sqlite3.connect(store_uri, uri=True)
    _streaming_plan_connection.row_factory = sqlite3.Row


def close_streaming_plan_worker() -> None:
    """Closes the in-process connection used by the synchronous executor fallback."""
    global _streaming_plan_connection
    if _streaming_plan_connection is not None:
        _streaming_plan_connection.close()
        _streaming_plan_connection = None


def categorize_streaming_snapshot(task: StreamingCategorizationTask) -> SerializedSnapshotUpdate:
    """Categorizes one snapshot without hydrating it in the coordinator."""
    snapshot = _load_snapshot(task.snapshot_id)
    if (
        snapshot.evaluatable
        and not snapshot.disable_restatement
        and (not snapshot.full_history_restatement_only or not snapshot.is_incremental)
    ):
        snapshot.effective_from = task.effective_from
    snapshot.categorize_as(task.category, task.forward_only)
    return SerializedSnapshotUpdate.from_snapshot(snapshot)


def compute_streaming_deployability(
    task: StreamingDeployabilityTask,
) -> StreamingDeployabilityResult:
    """Computes one node's deployability from its already-finalized parent state."""
    snapshot = _load_snapshot(task.snapshot_id)

    if task.start_override is not None:
        snapshot_start = task.start_override
    elif snapshot.node.start:
        snapshot_start = to_datetime(snapshot.node.start)
    elif task.parent_start_ts is not None:
        snapshot_start = to_datetime(task.parent_start_ts)
    else:
        snapshot_start = snapshot.node.cron_prev(snapshot.node.cron_floor(task.evaluation_time))

    this_deployable = snapshot.virtual_environment_mode.is_full and (
        task.parent_children_deployable is None or task.parent_children_deployable
    )
    representative = False
    if this_deployable:
        is_forward_only_model = (
            snapshot.is_model and snapshot.model.forward_only and not snapshot.is_metadata
        )
        has_auto_restatement = (
            snapshot.is_model and snapshot.model.auto_restatement_cron is not None
        )
        is_valid_start = (
            snapshot.is_valid_start(task.plan_start, snapshot_start)
            if task.plan_start is not None
            else True
        )
        children_deployable = is_valid_start and not has_auto_restatement
        if (
            snapshot.is_forward_only
            or snapshot.is_indirect_non_breaking
            or is_forward_only_model
            or has_auto_restatement
            or not is_valid_start
        ):
            this_deployable = False
            if not snapshot.is_paused or (snapshot.is_indirect_non_breaking and snapshot.intervals):
                representative = True
            else:
                children_deployable = False
    else:
        children_deployable = False
        representative = not snapshot.is_paused

    return StreamingDeployabilityResult(
        snapshot_id=task.snapshot_id,
        start_ts=to_timestamp(snapshot_start),
        deployable=this_deployable,
        children_deployable=children_deployable,
        representative=representative,
    )


def _load_snapshot(snapshot_id: SnapshotId) -> Snapshot:
    if _streaming_plan_connection is None:
        raise SQLMeshError("Streaming plan worker was not initialized")

    row = _streaming_plan_connection.execute(
        "SELECT payload FROM snapshots WHERE name = ? AND identifier = ?",
        (snapshot_id.name, snapshot_id.identifier),
    ).fetchone()
    if row is None:
        raise SQLMeshError(f"Snapshot '{snapshot_id}' was not found in the plan store")
    return t.cast(Snapshot, pickle.loads(row["payload"]))
