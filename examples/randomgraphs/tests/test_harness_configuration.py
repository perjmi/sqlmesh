# SPDX-License-Identifier: Apache-2.0

from __future__ import annotations

import json
import os
import subprocess
import sys
from pathlib import Path

import pytest

ENTRYPOINT = Path(__file__).parents[1] / "planner_entrypoint.py"


def entrypoint_environment(mode: str) -> dict[str, str]:
    environment = {
        key: value
        for key, value in os.environ.items()
        if not key.startswith(("SQLMESH__", "RANDOMGRAPHS_"))
    }
    environment.update(
        RANDOMGRAPHS_PLANNER_MODE=mode,
        RANDOMGRAPHS_MODEL_BATCH_SIZE="3",
        RANDOMGRAPHS_SNAPSHOT_BATCH_SIZE="2",
        RANDOMGRAPHS_HYDRATED_MODEL_CACHE_SIZE="1",
        RANDOMGRAPHS_HYDRATED_SNAPSHOT_CACHE_SIZE="1",
        RANDOMGRAPHS_STREAMING_WORKERS="2",
        RANDOMGRAPHS_STREAMING_WORKER_MAX_TASKS="1",
    )
    return environment


def test_default_entrypoint_loads_upstream_config_without_planner_settings() -> None:
    result = subprocess.run(
        [
            sys.executable,
            str(ENTRYPOINT),
            sys.executable,
            "-c",
            "from sqlmesh.core.config import Config; "
            "from sqlmesh.core.config.loader import load_config_from_env; "
            "config = load_config_from_env(); "
            "assert 'planner' not in config; "
            "Config.parse_obj(config)",
        ],
        env=entrypoint_environment("default"),
        capture_output=True,
        text=True,
    )
    assert result.returncode == 0, result.stderr


@pytest.mark.parametrize("mode", ("eager", "shadow", "streaming"))
def test_explicit_planner_mode_reaches_command_with_batch_and_worker_settings(mode: str) -> None:
    result = subprocess.run(
        [
            sys.executable,
            str(ENTRYPOINT),
            sys.executable,
            "-c",
            "import json; from sqlmesh.core.config.loader import load_config_from_env; "
            "print(json.dumps(load_config_from_env()))",
        ],
        env=entrypoint_environment(mode),
        capture_output=True,
        text=True,
    )
    assert result.returncode == 0, result.stderr
    assert json.loads(result.stdout)["planner"] == {
        "mode": mode,
        "model_batch_size": "3",
        "snapshot_batch_size": "2",
        "hydrated_model_cache_size": "1",
        "hydrated_snapshot_cache_size": "1",
        "streaming_workers": "2",
        "streaming_worker_max_tasks": "1",
    }


def test_invalid_planner_mode_fails_before_running_command() -> None:
    result = subprocess.run(
        [sys.executable, str(ENTRYPOINT), sys.executable, "-c", "print('command ran')"],
        env=entrypoint_environment("straming"),
        capture_output=True,
        text=True,
    )
    assert result.returncode != 0
    assert "Unsupported RANDOMGRAPHS_PLANNER_MODE" in result.stderr
    assert "command ran" not in result.stdout
