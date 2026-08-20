from __future__ import annotations

import typing as t
from enum import Enum

from pydantic import Field

from sqlmesh.core.config.base import BaseConfig


class PlannerMode(str, Enum):
    """The implementation used to build a plan."""

    EAGER = "eager"
    SHADOW = "shadow"
    STREAMING = "streaming"


class PlannerConfig(BaseConfig):
    """Controls planner working-set and rollout behavior.

    Args:
        mode: The planner implementation. Eager remains the compatibility default.
        model_batch_size: Maximum number of local models hydrated in one streaming batch.
        snapshot_batch_size: Maximum number of state snapshots hydrated in one streaming batch.
        hydrated_model_cache_size: Maximum number of hydrated models retained by a streaming registry.
        hydrated_snapshot_cache_size: Maximum number of snapshots retained by the state cache. ``None``
            preserves the eager planner's legacy unbounded cache.
        streaming_workers: Maximum number of discovery, schema, snapshot, and finalization workers
            used by the streaming planner. This is additionally capped by ``MAX_FORK_WORKERS``.
        streaming_worker_max_tasks: Target number of models or snapshots processed per worker
            before the pool is recycled and its allocator state is released.
    """

    mode: PlannerMode = PlannerMode.EAGER
    model_batch_size: int = Field(default=100, gt=0)
    snapshot_batch_size: int = Field(default=10, gt=0)
    hydrated_model_cache_size: int = Field(default=250, ge=0)
    hydrated_snapshot_cache_size: t.Optional[int] = Field(default=None, ge=0)
    streaming_workers: int = Field(default=2, gt=0)
    streaming_worker_max_tasks: int = Field(default=25, gt=0)
