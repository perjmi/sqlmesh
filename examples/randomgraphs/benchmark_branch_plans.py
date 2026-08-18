# SPDX-License-Identifier: Apache-2.0

from __future__ import annotations

import argparse
import csv
import json
import os
import statistics
import subprocess
import sys
import threading
import time
from collections import defaultdict
from pathlib import Path
from typing import Any, Iterable

from generate_model_graph import generate_model_graph

PROJECT_PATH = Path(__file__).parent
RESULT_MARKER = "RANDOMGRAPH_BENCHMARK_RESULT="
DEFAULT_WIDTHS = (10, 100, 200, 500)
COMPOSE = (
    "docker",
    "compose",
    "-p",
    "randomgraphs-branch-benchmark",
    "-f",
    str(PROJECT_PATH / "compose.yaml"),
)
NETWORK = "randomgraphs-branch-benchmark_default"
COMPOSE_ENV = {**os.environ, "POSTGRES_HOST_PORT": "55439"}
RAW_FIELDS = (
    "branch",
    "commit",
    "planner_mode",
    "width",
    "iteration",
    "seed",
    "models",
    "elapsed_seconds",
    "peak_rss_bytes",
    "peak_rss_mib",
    "cgroup_peak_bytes",
    "cgroup_peak_mib",
)


def _run(
    command: Iterable[str],
    *,
    env: dict[str, str] | None = None,
    capture_output: bool = True,
) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        tuple(command),
        cwd=PROJECT_PATH,
        env=env,
        check=True,
        capture_output=capture_output,
        text=True,
    )


def _aggregate_rss_bytes() -> int:
    """Return aggregate RSS for every process in this container's PID namespace."""
    total_kib = 0
    for status_path in Path("/proc").glob("[0-9]*/status"):
        try:
            for line in status_path.read_text(encoding="utf-8").splitlines():
                if line.startswith("VmRSS:"):
                    total_kib += int(line.split()[1])
                    break
        except (FileNotFoundError, PermissionError, ProcessLookupError):
            continue
    return total_kib * 1024


def _read_cgroup_peak_bytes() -> int:
    for path in (
        Path("/sys/fs/cgroup/memory.peak"),
        Path("/sys/fs/cgroup/memory/memory.max_usage_in_bytes"),
    ):
        try:
            value = path.read_text(encoding="utf-8").strip()
            if value and value != "max":
                return int(value)
        except (FileNotFoundError, PermissionError, ValueError):
            continue
    return 0


def _measure_plan() -> int:
    stop = threading.Event()
    peak_rss_bytes = _aggregate_rss_bytes()

    def sample_memory() -> None:
        nonlocal peak_rss_bytes
        while not stop.wait(0.02):
            peak_rss_bytes = max(peak_rss_bytes, _aggregate_rss_bytes())

    sampler = threading.Thread(target=sample_memory, name="rss-sampler", daemon=True)
    sampler.start()
    started = time.perf_counter()
    try:
        from sqlmesh import Context

        context = Context(paths=PROJECT_PATH)
        plan = context.plan_builder(
            "benchmark",
            skip_tests=True,
            skip_backfill=True,
        ).build()
        elapsed_seconds = time.perf_counter() - started
        peak_rss_bytes = max(peak_rss_bytes, _aggregate_rss_bytes())

        manifest = json.loads(
            (PROJECT_PATH / "models" / "generated" / "graph.json").read_text(encoding="utf-8")
        )
        expected_models = int(manifest["width"]) * 7
        if len(plan.new_snapshots) != expected_models:
            raise RuntimeError(
                f"Expected {expected_models} new snapshots, got {len(plan.new_snapshots)}"
            )
    finally:
        stop.set()
        sampler.join()

    result = {
        "models": expected_models,
        "elapsed_seconds": elapsed_seconds,
        "peak_rss_bytes": peak_rss_bytes,
        "cgroup_peak_bytes": _read_cgroup_peak_bytes(),
    }
    print(f"{RESULT_MARKER}{json.dumps(result, sort_keys=True)}")
    return 0


def _reset_database() -> None:
    commands = (
        (
            *COMPOSE,
            "exec",
            "-T",
            "postgres",
            "dropdb",
            "--if-exists",
            "--force",
            "-U",
            "postgres",
            "sqlmesh",
        ),
        (
            *COMPOSE,
            "exec",
            "-T",
            "postgres",
            "createdb",
            "-U",
            "postgres",
            "sqlmesh",
        ),
    )
    failures = []
    for attempt in range(1, 4):
        failures.clear()
        for command in commands:
            result = subprocess.run(
                command,
                cwd=PROJECT_PATH,
                env=COMPOSE_ENV,
                capture_output=True,
                text=True,
            )
            if result.returncode:
                failures.append(
                    f"{' '.join(command)}\nstdout:\n{result.stdout}\nstderr:\n{result.stderr}"
                )
                break
        if not failures:
            return
        time.sleep(attempt)
    raise RuntimeError("Database reset failed after three attempts:\n" + "\n".join(failures))


def _run_sample(image: str, planner_mode: str) -> dict[str, Any]:
    _reset_database()
    command = [
        "docker",
        "run",
        "--rm",
        "--network",
        NETWORK,
        "-e",
        "POSTGRES_HOST=postgres",
        "-e",
        "POSTGRES_PORT=5432",
        "-e",
        "POSTGRES_DB=sqlmesh",
        "-e",
        "POSTGRES_USER=postgres",
        "-e",
        "POSTGRES_PASSWORD=postgres",
        "-e",
        "SQLMESH_CACHE_DIR=/tmp/sqlmesh-cache",
        "-e",
        "MAX_FORK_WORKERS=1",
        "-e",
        "PYTHONDONTWRITEBYTECODE=1",
    ]
    if planner_mode:
        command.extend(("-e", f"SQLMESH__PLANNER__MODE={planner_mode}"))
    command.extend(
        (
            "-v",
            f"{PROJECT_PATH}:/workspace/examples/randomgraphs:ro",
            image,
            "python",
            "/workspace/examples/randomgraphs/benchmark_branch_plans.py",
            "--measure-plan",
        )
    )
    result = _run(command)
    for line in reversed(result.stdout.splitlines()):
        if line.startswith(RESULT_MARKER):
            return json.loads(line.removeprefix(RESULT_MARKER))
    raise RuntimeError(
        f"Benchmark result marker missing. Output:\n{result.stdout}\n{result.stderr}"
    )


def _load_existing(path: Path) -> tuple[list[dict[str, str]], set[tuple[str, int, int]]]:
    if not path.exists():
        return [], set()
    with path.open(newline="", encoding="utf-8") as file:
        rows = list(csv.DictReader(file))
    completed = {(row["branch"], int(row["width"]), int(row["iteration"])) for row in rows}
    return rows, completed


def _write_rows(path: Path, rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as file:
        writer = csv.DictWriter(file, fieldnames=RAW_FIELDS, lineterminator="\n")
        writer.writeheader()
        writer.writerows(rows)


def _percentile(values: list[float], percentile: float) -> float:
    ordered = sorted(values)
    position = (len(ordered) - 1) * percentile
    lower = int(position)
    upper = min(lower + 1, len(ordered) - 1)
    fraction = position - lower
    return ordered[lower] + (ordered[upper] - ordered[lower]) * fraction


def _write_summary(raw_path: Path, summary_path: Path) -> None:
    with raw_path.open(newline="", encoding="utf-8") as file:
        rows = list(csv.DictReader(file))
    grouped: dict[tuple[str, int], list[dict[str, str]]] = defaultdict(list)
    for row in rows:
        grouped[(row["branch"], int(row["width"]))].append(row)

    fields = (
        "branch",
        "width",
        "samples",
        "time_mean_seconds",
        "time_median_seconds",
        "time_stdev_seconds",
        "time_p95_seconds",
        "rss_mean_mib",
        "rss_median_mib",
        "rss_stdev_mib",
        "rss_p95_mib",
    )
    output = []
    for (branch, width), group in sorted(
        grouped.items(), key=lambda item: (item[0][1], item[0][0])
    ):
        times = [float(row["elapsed_seconds"]) for row in group]
        rss = [float(row["peak_rss_mib"]) for row in group]
        time_stdev = statistics.stdev(times) if len(times) > 1 else 0.0
        rss_stdev = statistics.stdev(rss) if len(rss) > 1 else 0.0
        output.append(
            {
                "branch": branch,
                "width": width,
                "samples": len(group),
                "time_mean_seconds": f"{statistics.mean(times):.6f}",
                "time_median_seconds": f"{statistics.median(times):.6f}",
                "time_stdev_seconds": f"{time_stdev:.6f}",
                "time_p95_seconds": f"{_percentile(times, 0.95):.6f}",
                "rss_mean_mib": f"{statistics.mean(rss):.6f}",
                "rss_median_mib": f"{statistics.median(rss):.6f}",
                "rss_stdev_mib": f"{rss_stdev:.6f}",
                "rss_p95_mib": f"{_percentile(rss, 0.95):.6f}",
            }
        )
    summary_path.parent.mkdir(parents=True, exist_ok=True)
    with summary_path.open("w", newline="", encoding="utf-8") as file:
        writer = csv.DictWriter(file, fieldnames=fields, lineterminator="\n")
        writer.writeheader()
        writer.writerows(output)


def _benchmark(args: argparse.Namespace) -> int:
    revisions = (
        ("main", args.main_image, args.main_commit, ""),
        ("streaming", args.streaming_image, args.streaming_commit, "streaming"),
    )
    rows, completed = _load_existing(args.output)
    _run((*COMPOSE, "up", "-d", "--wait", "postgres"), env=COMPOSE_ENV)
    try:
        for width in args.widths:
            for iteration in range(1, args.samples + 1):
                seed = iteration - 1
                generate_model_graph(width, seed=seed)
                ordered_revisions = revisions if iteration % 2 else tuple(reversed(revisions))
                for branch, image, commit, planner_mode in ordered_revisions:
                    key = (branch, width, iteration)
                    if key in completed:
                        continue
                    print(
                        f"Running branch={branch} width={width} iteration={iteration} seed={seed}",
                        flush=True,
                    )
                    measurement = _run_sample(image, planner_mode)
                    peak_rss_bytes = int(measurement["peak_rss_bytes"])
                    cgroup_peak_bytes = int(measurement["cgroup_peak_bytes"])
                    row: dict[str, Any] = {
                        "branch": branch,
                        "commit": commit,
                        "planner_mode": planner_mode or "eager",
                        "width": width,
                        "iteration": iteration,
                        "seed": seed,
                        "models": int(measurement["models"]),
                        "elapsed_seconds": f"{float(measurement['elapsed_seconds']):.6f}",
                        "peak_rss_bytes": peak_rss_bytes,
                        "peak_rss_mib": f"{peak_rss_bytes / 1024**2:.6f}",
                        "cgroup_peak_bytes": cgroup_peak_bytes,
                        "cgroup_peak_mib": f"{cgroup_peak_bytes / 1024**2:.6f}",
                    }
                    rows.append(row)
                    completed.add(key)
                    _write_rows(args.output, rows)
                    print(
                        f"Completed branch={branch} width={width} iteration={iteration}: "
                        f"{row['elapsed_seconds']}s, {row['peak_rss_mib']} MiB RSS",
                        flush=True,
                    )
    finally:
        generate_model_graph(10, seed=42)
        _run(
            (*COMPOSE, "down", "--volumes", "--remove-orphans"),
            env=COMPOSE_ENV,
            capture_output=False,
        )

    _write_summary(args.output, args.summary_output)
    return 0


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--measure-plan", action="store_true")
    parser.add_argument("--main-image", default="randomgraphs-sqlmesh-main")
    parser.add_argument("--streaming-image", default="randomgraphs-sqlmesh-streaming")
    parser.add_argument("--main-commit", default="")
    parser.add_argument("--streaming-commit", default="")
    parser.add_argument("--widths", type=int, nargs="+", default=DEFAULT_WIDTHS)
    parser.add_argument("--samples", type=int, default=10)
    parser.add_argument(
        "--output",
        type=Path,
        default=PROJECT_PATH / "benchmark_results" / "branch_plan_benchmark_raw.csv",
    )
    parser.add_argument(
        "--summary-output",
        type=Path,
        default=PROJECT_PATH / "benchmark_results" / "branch_plan_benchmark_summary.csv",
    )
    return parser.parse_args()


if __name__ == "__main__":
    arguments = _parse_args()
    sys.exit(_measure_plan() if arguments.measure_plan else _benchmark(arguments))
