# SPDX-License-Identifier: Apache-2.0

from __future__ import annotations

import sys
from collections.abc import Iterator
from pathlib import Path

import pytest

PROJECT_PATH = Path(__file__).parents[1]
sys.path.insert(0, str(PROJECT_PATH))
sys.path.insert(0, str(Path(__file__).parent))

from dual_planner import COMPOSE, run  # noqa: E402
from generate_model_graph import generate_model_graph  # noqa: E402
from randomgraph_mutations import write_randomgraph_audit  # noqa: E402


@pytest.fixture(scope="session")
def accuracy_stack() -> Iterator[None]:
    run(*COMPOSE, "up", "-d", "--wait", "reference_db", "candidate_db")
    try:
        yield
    finally:
        write_randomgraph_audit()
        generate_model_graph(10, output_dir=PROJECT_PATH / "models" / "generated", seed=42)
        run(*COMPOSE, "down", "--volumes", "--remove-orphans", capture_output=False)
