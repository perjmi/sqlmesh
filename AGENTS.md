# Repository guidance for coding agents

This file applies to the whole repository. More specific `AGENTS.md` files, if added below this
directory, override it for their subtree.

## Start here

SQLMesh is a Python data-transformation framework built around versioned models, plans, virtual
environments, state synchronization, and multi-engine execution. Read the files relevant to the task
before editing:

- `CONTRIBUTING.md` for the normal development workflow.
- `sqlmesh/core/context.py` for project loading and orchestration.
- `sqlmesh/core/model/` and `sqlmesh/core/snapshot/` for model and snapshot semantics.
- `sqlmesh/core/plan/` for plan construction.
- `docs/concepts/architecture/streaming_planner.md` for the streaming branch's architecture,
  guarantees, rollout status, and known eager-retention points.
- `examples/randomgraphs/README.md` for the generated-DAG accuracy and performance harness.
- `examples/randomgraphs/model_kind_matrix/` for portable PostgreSQL, MySQL, and DuckDB planner
  parity fixtures.

Python 3.9 or newer is supported. Use an isolated environment and install development dependencies
with `make install-dev` when they are not already available.

## Development expectations

- Write or identify a failing test before changing behavior. Keep eager behavior as the compatibility
  oracle for streaming-planner changes.
- Preserve snapshot identity, categorization, deployability, interval, audit, promotion, and physical
  naming semantics unless the task explicitly changes them.
- Keep edits scoped. The worktree may contain user changes; do not reformat, stage, revert, or delete
  unrelated files.
- Use `rg` and `rg --files` for repository searches. Use `apply_patch` for hand-written edits.
- Run the narrowest relevant tests while iterating, then broaden validation in proportion to risk.
- Run `make style` before a final commit when the full development environment is available. For
  focused work, at minimum run Ruff on touched Python files and `git diff --check`.
- Do not force-push, rewrite published history, or change external repositories without explicit
  authorization.

Common commands:

```bash
make style
make fast-test
pytest -q -n 0 tests/core/test_plan.py
pytest -q -n 0 tests/core/test_context.py
```

Some full suites require optional engine packages and credentials. A missing BigQuery, Spark, dbt, or
other engine extra is an environment limitation, not evidence that a focused change failed; report it
clearly and still run all available relevant tests.

## Streaming-planner invariants

The `streaming` planner is opt-in and must remain semantically equivalent to the eager planner for the
same project, state, configuration, execution time, and user flags.

- Do not reconstruct all model payloads, snapshot payloads, columns, or edges in coordinator memory.
- SQLite-backed indexes and catalogs own graph and payload state. The coordinator is the sole SQLite
  writer; workers use read-only connections and return compact metadata or bounded serialized results.
- Preserve topological barriers. A worker may process a node only after the state needed from its
  parents is finalized and visible.
- Bound Python working sets by configured workers, bounded queues/batches, hydration caches, and the
  largest active model or direct-parent schema boundary—not total DAG size.
- Drain payload futures continuously. Do not accumulate a worker-pool-sized history of completed
  hydrated models or serialized snapshots.
- Recycle worker pools according to `streaming_worker_max_tasks` so allocator high-water marks are
  released.
- Keep SQLite writes deterministic and batched. Avoid a durable commit for every individual result.
- APIs that explicitly request a complete graph or complete model mapping may materialize it, but the
  ordinary native streaming plan path must not call those compatibility APIs.
- If increasing `streaming_workers` above the default, raise `snapshot_batch_size` enough to expose
  useful parallelism; otherwise some finalization phases are batch-width limited.

Correctness work must compare eager and streaming outputs, not merely assert that streaming completes.
At minimum cover fingerprints, versions, categories, deployability, selected changes, audits, repeated
no-change plans, and resulting data where the change can affect execution.

## Random-graph validation and benchmarking

The random-graph project generates `7 * inputs` models: input tables plus six materialized-view layers.
Use it for wide-DAG regression, audit, data-parity, timing, and memory tests.

Focused commands:

```bash
pytest -q -o addopts='' -p no:cacheprovider \
  examples/randomgraphs/tests/test_generate_model_graph.py

CANDIDATE_PLANNER_MODE=streaming \
pytest -q -o addopts='' -p no:cacheprovider \
  examples/randomgraphs/tests/test_dual_planner_accuracy.py

REFERENCE_SQLMESH_IMAGE=randomgraphs-sqlmesh-main:latest \
CANDIDATE_SQLMESH_IMAGE=randomgraphs-sqlmesh-streaming-iterator:latest \
CANDIDATE_PLANNER_MODE=streaming \
pytest -q -o addopts='' -p no:cacheprovider \
  examples/randomgraphs/tests/test_dual_planner_mutations.py

REFERENCE_SQLMESH_IMAGE=randomgraphs-sqlmesh-main:latest \
CANDIDATE_SQLMESH_IMAGE=randomgraphs-sqlmesh-streaming-iterator:latest \
CANDIDATE_PLANNER_MODE=streaming \
pytest -q -o addopts='' -p no:cacheprovider \
  examples/randomgraphs/tests/test_dual_planner_model_kind_matrix.py
```

The model-kind matrix must run the same lifecycle and assertions on PostgreSQL, MySQL, and DuckDB.
Do not silently skip an engine or kind. Add an explicit support note when a kind is inherently
engine-specific or requires externally provisioned data. Keep normalized state comparisons and full
ordered-row comparisons alongside successful-plan assertions. An older reference image without the
MySQL driver may use the suite's thin driver-only derivative; do not replace its SQLMesh source.

Benchmark rules:

- Compare the same graph seed, source revision, planner flags, worker count, and database reset.
- Run timed samples sequentially; concurrent samples invalidate timing and memory comparisons.
- Treat Docker cgroup `memory.peak` as the primary whole-container memory metric. Aggregate RSS
  double-counts copy-on-write pages shared by forked processes.
- Never start an in-process RSS sampler thread before creating fork-based worker pools. Forking a
  multithreaded Python process can deadlock. Sample externally or use a process-safe measurement design.
- Report graph generation separately from plan construction unless the experiment explicitly measures
  both.
- Label single samples as smoke measurements rather than distributions.
- After a benchmark, regenerate the normal 10-input graph with seed 42 and remove only the temporary
  benchmark containers, networks, and volumes created by that run.

## Git and handoff

Before committing, inspect `git status` and stage explicit paths. Summarize tests, benchmark conditions,
known environment limitations, and any uncommitted user files in the handoff. Keep the upstream
`SQLMesh/sqlmesh` remote intact when pushing experimental branches to a personal fork or repository.
