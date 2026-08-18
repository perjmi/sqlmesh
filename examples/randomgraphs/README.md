<!-- SPDX-License-Identifier: Apache-2.0 -->

# Random model graphs

This example generates wide, six-level SQLMesh DAGs for experiments against PostgreSQL.

Given a width `n`, `generate_model_graph` creates:

- `n` FULL input-table models
- Six layers of `n` native PostgreSQL materialized-view models
- `7n` models in total
- A `graph.json` manifest describing every dependency

Each materialized view randomly selects up to four distinct models from the immediately
preceding layer. When at least two upstreams exist, it uses at least two so the query performs
a join. All generated models share a stable unique grain and use projections, joins, and grouped
aggregations.

## Run with Docker

Build the SQLMesh image and start PostgreSQL:

```bash
docker compose build
docker compose up -d postgres
```

Generate a graph and apply it:

```bash
docker compose run --rm sqlmesh python generate_model_graph.py 10 --seed 42
docker compose run --rm sqlmesh sqlmesh plan --auto-apply --no-prompts
```

The first command creates 70 models under `models/generated/`. The project directory is mounted
into the SQLMesh container, so the files and `graph.json` manifest remain available on the host.
Generated artifacts are ignored by Git.

PostgreSQL is exposed on host port `5432`. If that port is occupied, set another host port when
starting the service:

```bash
POSTGRES_HOST_PORT=5433 docker compose up -d postgres
```

## Call from Python

```python
from generate_model_graph import chunk_model_graph, generate_model_graph

graph = generate_model_graph(10, seed=42)
print(len(graph.model_paths))  # 70

chunks = chunk_model_graph(graph)
print([len(chunk) for chunk in chunks])  # Ten DAG-ordered chunks of seven
```

`chunk_model_graph` orders inputs before each successive depth, then divides that order into ten
balanced chunks by default. Every dependency therefore occurs in the same or an earlier chunk.

## Accuracy tiers

The dual-planner accuracy suite applies the same generated graph to isolated reference and
candidate PostgreSQL containers. The reference always receives a full plan; the candidate receives
either a full plan or ten DAG-ordered plans. It compares schemas, native materialized-view counts,
and data digests for every model, then verifies that repeated plans are idempotent.

Both services use the current `randomgraphs-sqlmesh` image by default. Build it and run the smoke
tier with:

```bash
docker compose build sqlmesh
pytest -q -o addopts='' -p no:cacheprovider tests/test_dual_planner_accuracy.py
```

The reference service explicitly uses `planner.mode: eager`; the candidate uses
`planner.mode: shadow`. Shadow mode still applies the eager plan, but fails the run if indexed
metadata selection, streamed fingerprints, or the compact context diff disagree with it.

Select a larger tier with `RANDOMGRAPHS_ACCURACY_TIER=pr`, `nightly`, or `stress`. To compare two
implementations, set `REFERENCE_SQLMESH_IMAGE` and `CANDIDATE_SQLMESH_IMAGE` to their respective
image tags. The candidate defaults to shadow mode; exercise the actual indexed-model cutover with:

```bash
CANDIDATE_PLANNER_MODE=streaming pytest -q -o addopts='' -p no:cacheprovider \
  tests/test_dual_planner_accuracy.py
```

Native streaming plans use two bounded workers by default in this suite. Override
`STREAMING_WORKERS` to control parallelism and `STREAMING_WORKER_MAX_TASKS` to control how often
worker processes are recycled. The coordinator remains the sole SQLite writer; workers persist
model payloads independently and return compact metadata or serialized snapshots.

For multi-process memory comparisons, use `cgroup_peak_mib` from `benchmark_branch_plans.py` as the
primary whole-container measurement. Aggregate RSS remains in the output for continuity but counts
fork-shared pages once per process.

By default, each input table receives a deterministic random count of 10 to 1,000 rows. All
tables use the same key sequence starting at `1`, so every join has matching records and every
leaf model contains data. Override the bounds with `--min-rows-per-input` and
`--max-rows-per-input`. Calling the generator again replaces only artifacts previously created
by the generator.

## Clean up

Stop the containers while retaining experiment data with `docker compose down`. Delete the
PostgreSQL volume too with `docker compose down --volumes`.
