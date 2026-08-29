# SPDX-License-Identifier: Apache-2.0

from __future__ import annotations

import hashlib
import json
import os
import subprocess
from collections.abc import Iterator

import pytest

from dual_planner import PROJECT_PATH, run

COMPOSE = (
    "docker",
    "compose",
    "-p",
    "randomgraphs-model-kinds",
    "-f",
    str(PROJECT_PATH / "compose.model-kinds.yaml"),
)
DATABASE_SERVICES = {
    "postgres": ("reference_postgres", "candidate_postgres"),
    "mysql": ("reference_mysql", "candidate_mysql"),
    "duckdb": (),
}


@pytest.fixture
def model_kind_stack(engine: str) -> Iterator[None]:
    run(*COMPOSE, "down", "--volumes", "--remove-orphans", check=False)
    original_images: dict[str, str | None] = {}
    if engine == "mysql":
        for target in ("reference", "candidate"):
            variable = f"{target.upper()}_SQLMESH_IMAGE"
            original_images[variable] = os.environ.get(variable)
            image = os.environ.get(variable, "randomgraphs-sqlmesh")
            os.environ[variable] = _image_with_mysql_driver(image)
    try:
        if services := DATABASE_SERVICES[engine]:
            run(*COMPOSE, "up", "-d", "--wait", *services)
        yield
    finally:
        run(*COMPOSE, "down", "--volumes", "--remove-orphans", capture_output=False)
        for variable, value in original_images.items():
            if value is None:
                os.environ.pop(variable, None)
            else:
                os.environ[variable] = value


def _image_with_mysql_driver(image: str) -> str:
    probe = run("docker", "run", "--rm", image, "python", "-c", "import pymysql", check=False)
    if probe.returncode == 0:
        return image
    image_digest = hashlib.sha256(image.encode()).hexdigest()[:12]
    derived_image = f"randomgraphs-model-kinds-mysql:{image_digest}"
    run(
        "docker",
        "build",
        "--build-arg",
        f"BASE_IMAGE={image}",
        "--tag",
        derived_image,
        "--file",
        str(PROJECT_PATH / "model_kind_matrix" / "Dockerfile.mysql"),
        ".",
    )
    return derived_image


def _plan(
    target: str,
    engine: str,
    *,
    value: int,
    environment: str = "prod",
    options: tuple[str, ...] = (),
) -> subprocess.CompletedProcess[str]:
    command = [
        *COMPOSE,
        "run",
        "--rm",
        "--no-deps",
        "-e",
        f"MATRIX_ENGINE={engine}",
        "-e",
        f"MATRIX_VALUE={value}",
        target,
        "sqlmesh",
        "--log-file-dir",
        "/tmp/sqlmesh-logs",
        "plan",
    ]
    if environment != "prod":
        command.append(environment)
    command.extend(("--auto-apply", "--no-prompts", *options))
    return run(*command, check=False)


def _oracle(target: str, engine: str, environment: str = "prod") -> dict[str, object]:
    result = run(
        *COMPOSE,
        "run",
        "--rm",
        "--no-deps",
        "-e",
        f"MATRIX_ENGINE={engine}",
        target,
        "python",
        "oracle.py",
        environment,
    )
    return json.loads(result.stdout)


def _assert_parity(engine: str, environment: str = "prod") -> None:
    reference = _oracle("reference", engine, environment)
    candidate = _oracle("candidate", engine, environment)
    for category in ("data", "state"):
        reference_sections = reference[category]
        candidate_sections = candidate[category]
        assert isinstance(reference_sections, dict)
        assert isinstance(candidate_sections, dict)
        assert candidate_sections.keys() == reference_sections.keys()
        for section in reference_sections:
            reference_rows = reference_sections[section]
            candidate_rows = candidate_sections[section]
            assert candidate_rows == reference_rows, (
                f"{engine} {environment} {category}.{section} differs:\n"
                f"reference={reference_rows}\n"
                f"candidate={candidate_rows}"
            )


def _assert_plans_succeed(
    engine: str,
    *,
    value: int,
    environment: str = "prod",
    options: tuple[str, ...] = (),
) -> tuple[subprocess.CompletedProcess[str], subprocess.CompletedProcess[str]]:
    reference = _plan("reference", engine, value=value, environment=environment, options=options)
    candidate = _plan("candidate", engine, value=value, environment=environment, options=options)
    assert reference.returncode == 0, reference.stdout + reference.stderr
    assert candidate.returncode == 0, candidate.stdout + candidate.stderr
    return reference, candidate


@pytest.mark.parametrize("engine", ("postgres", "mysql", "duckdb"))
def test_model_kind_lifecycle_matrix_preserves_state_and_data_parity(
    engine: str, model_kind_stack: None
) -> None:
    baseline_options = ("--execution-time", "2024-01-04T00:00:00+00:00")
    _assert_plans_succeed(engine, value=0, options=baseline_options)
    _assert_parity(engine)

    reference, candidate = _assert_plans_succeed(engine, value=0, options=baseline_options)
    assert "No changes to plan" in reference.stdout
    assert "No changes to plan" in candidate.stdout
    _assert_parity(engine)

    mutation_options = (
        "--allow-destructive-model",
        "matrix.scd_column_model",
        "--allow-destructive-model",
        "matrix.scd_time_model",
        "--execution-time",
        "2024-01-05T00:00:00+00:00",
    )
    _assert_plans_succeed(engine, value=7, options=mutation_options)
    _assert_parity(engine)

    development_options = (
        "--create-from",
        "prod",
        "--include-unmodified",
        "--execution-time",
        "2024-01-05T00:00:00+00:00",
    )
    _assert_plans_succeed(
        engine,
        value=7,
        environment="model_matrix_dev",
        options=development_options,
    )
    _assert_parity(engine, "model_matrix_dev")

    restatement_options = (
        "--restate-model",
        "matrix.incremental_time_model",
        "--backfill-model",
        "matrix.incremental_time_model",
        "--start",
        "2024-01-02",
        "--end",
        "2024-01-03",
        "--execution-time",
        "2024-01-05T00:00:00+00:00",
    )
    _assert_plans_succeed(engine, value=7, options=restatement_options)
    _assert_parity(engine)
