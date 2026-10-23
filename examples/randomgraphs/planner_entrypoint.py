# SPDX-License-Identifier: Apache-2.0

"""Apply optional planner settings before running a command in either test image."""

from __future__ import annotations

import os
import sys

PLANNER_SETTINGS = (
    "MODEL_BATCH_SIZE",
    "SNAPSHOT_BATCH_SIZE",
    "HYDRATED_MODEL_CACHE_SIZE",
    "HYDRATED_SNAPSHOT_CACHE_SIZE",
    "STREAMING_WORKERS",
    "STREAMING_WORKER_MAX_TASKS",
)


def main() -> None:
    mode = os.environ.get("RANDOMGRAPHS_PLANNER_MODE", "default")
    if mode not in {"default", "eager", "shadow", "streaming"}:
        raise SystemExit(f"Unsupported RANDOMGRAPHS_PLANNER_MODE: {mode!r}")
    if len(sys.argv) < 2:
        raise SystemExit("Usage: planner_entrypoint.py COMMAND [ARGS...]")

    # Ordinary upstream builds have no planner config. Only opt-in modes may emit it.
    if mode != "default":
        os.environ["SQLMESH__PLANNER__MODE"] = mode
        for setting in PLANNER_SETTINGS:
            value = os.environ.get(f"RANDOMGRAPHS_{setting}")
            if value is not None:
                os.environ[f"SQLMESH__PLANNER__{setting}"] = value

    os.execvp(sys.argv[1], sys.argv[1:])


if __name__ == "__main__":
    main()
