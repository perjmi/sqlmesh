# Streaming planner implementation plan

This document describes an incremental implementation of a bounded-working-set SQLMesh planner.
The implementation lives on the `streaming` branch until it reaches semantic parity with the eager
planner.

## Implementation status

The branch currently implements the shadow-safe foundation and keeps eager application as the oracle:

- `planner.mode` configuration and eager-compatible cache defaults.
- An entry-bounded snapshot LRU and deterministic batched state hydration APIs.
- A dict-compatible eager registry plus a bounded indexed registry backed by per-model payload files.
- A transactional, versioned SQLite graph index with indexed selection and dependency queries.
- Source-file-bounded native SQL discovery in a bounded, recyclable worker pool. Workers persist model
  payloads directly and return only compact metadata to the single SQLite writer in the coordinator.
- Versioned model payloads that preserve authoritative post-schema data and metadata hashes across
  serialization.
- An indexed compatibility mapping in `streaming` mode that releases the eager project dictionary
  after load and hydrates subsequent model access through the configured bounded LRU.
- Parallel streaming schema propagation with topological barriers. Each worker hydrates one child and
  at most one parent at a time, then validates and persists the finalized model before returning compact
  metadata to the coordinator.
- SQLite-spilled topological traversal and an indexed `Context.dag` view, so Python never reconstructs
  the complete edge map during native streaming load, schema propagation, or fingerprinting.
- Topological, batch-bounded fingerprinting over metadata hashes without hydrating model payloads.
- A compact first-pass context diff based on snapshot headers.
- A per-plan SQLite snapshot catalog with bounded, write-back hydration, lazy ID/name/sequence views,
  persisted snapshot edges, and disk-backed topological traversal.
- Worker-built snapshot payloads and streaming context-diff merging that write serialized snapshots
  directly to the plan catalog instead of constructing all-snapshot dictionaries in the coordinator.
- Streaming preview eligibility checks that repeatedly scan the SQLite-backed snapshot mapping without
  caching hydrated changed snapshots in `PlanBuilder`.
- SQLite-backed snapshot DAG traversal, deployability propagation, and earliest-start inference.
- Shadow checks for graph round-trips, selection, fingerprints, and context-diff change sets.
- A two-Postgres random-graph oracle that runs eager reference plans against shadow candidates.

The production streaming cutover is deliberately not claimed yet. In `streaming` mode native SQL
discovery emits and spills source-file batches without constructing a project-wide model dictionary.
Schema propagation walks the indexed DAG and finalizes each model from its direct-parent column
mappings. Later model access uses the indexed compatibility mapping. Snapshot payloads in `ContextDiff`
and `Plan` are now lazy views over the per-plan SQLite catalog, including the public indexable
`Plan.new_snapshots` sequence. APIs that explicitly materialize model values remain outside the bound.
Change-ID sets, modified old/new snapshot pairs, environment construction, and application stages still
have eager retention points. Streaming application/checkpoints remain delivery milestones D and E below.
This boundary prevents an opt-in mode from silently applying mixed semantics before the remaining
accuracy and failure-injection gates pass.

To preserve the bounded-memory contract, this cutover currently rejects Python and external-model
projects in `streaming` mode before loading their model payloads; those projects must use `shadow` until
their source formats have incremental readers. Eager and shadow behavior is unchanged.

The native local-project memory guarantee is independent of total DAG width and depth. Its working set is
bounded by the configured number of active workers, each worker's current model and direct-parent column
schemas, bounded coordinator result batches, and configured hydration caches. Worker pools are recycled
after a configured task count so allocator high-water marks are released. A single model with unusually
many parents or columns can therefore increase the boundary, but unrelated models, columns, edges, and
completed snapshots remain on disk. Commands that explicitly request the complete graph or complete
model mapping are materializing compatibility APIs and are outside this bound.

## Goals

- Preserve the complete-project semantics of model selection, change propagation, fingerprinting,
  environment promotion, and backfill ordering.
- Keep the peak planning working set proportional to the selected models and their semantic boundary,
  rather than the complete local project and target environment.
- Stream local model hydration, state snapshot hydration, plan-stage construction, and application in
  deterministic batches.
- Retain the eager planner as a compatibility path and differential oracle during rollout.
- Make interrupted streaming plans retryable without publishing a partially promoted environment.

## Non-goals

- Avoiding all work proportional to project size. New and changed files still need discovery and
  dependency indexing.
- Changing SQLMesh snapshot identity, change categorization, interval semantics, or physical naming.
- Requiring a remote state backend for local commands such as formatting or unit tests.
- Loading arbitrary Python models without executing their registration code at least once after a
  relevant source or dependency change.

## Correctness invariants

For identical project files, state, configuration, execution time, and user flags, eager and streaming
planning must produce equivalent:

1. Selected, added, removed, directly modified, and indirectly modified model sets.
2. Snapshot fingerprints, versions, change categories, deployability, and parent identifiers.
3. Missing intervals, restatements, backfill order, and audit scope.
4. Promoted and demoted environment snapshot sets.
5. Physical object types and schemas.
6. Model data digests after application.
7. No-change results for an immediately repeated plan.

The random-graph dual-runner tiers are the primary integration oracle. Existing context, selector,
snapshot, state-sync, plan-builder, evaluator, dbt, and engine integration suites remain required.

## Current eager retention points

### Local project

`Loader.load` exposes native SQL results one source file at a time and the graph index consumes those
batches transactionally. Eager and shadow modes still return a `LoadedProject` containing the same fully
hydrated `Model` instances. Native `streaming` mode instead spills each discovered payload and returns no
local model dictionary. `Context.load` installs a SQLite-backed DAG view and propagates mapping schemas
one model at a time before installing the indexed, bounded-hydration compatibility mapping.

### Remote state

`Context.load`, `Selector.select_models`, `Context._snapshots`, and `ContextDiff.create` hydrate remote
snapshots. `CachingStateSync` retains requested snapshots in an unbounded process-local dictionary.

### Plan

The streaming `ContextDiff` and `Plan` snapshot collections are backed by a per-plan SQLite catalog and
an entry-bounded hydration cache. Preview eligibility and categorization traverse those mappings without
retaining a project-wide hydrated snapshot list. The current change-ID sets and modified old/new pairs
are still Python collections, while `PlanStagesBuilder`, evaluatable-plan conversion, environment
construction, and several application stages can still materialize complete compact snapshot metadata
collections.

## Target architecture

### 1. Project graph index

Introduce a versioned index under `cache_dir` containing lightweight records:

```text
ModelMetadata
  fqn
  name
  source path and source digest
  project and gateway
  enabled flag
  model kind summary
  dependency names
  tags
  column contract digest
  macro / Python dependency digests
  cached-model payload key
```

The index must support deterministic iteration, duplicate detection, reverse-edge queries, wildcard and
tag selection, and transactional replacement after successful discovery. SQLite is the initial storage
format because it provides bounded cursors, indexes, transactions, and a single portable cache file.
The format has its own schema version and is disposable when incompatible.

SQL files are parsed independently and written to the index as their workers finish. Fully hydrated model
payloads remain in the existing model-definition cache and are not accumulated by the parent loader.
Python models are indexed per source module after registration. Changes to macros, variables, gateways,
audits, signals, or loader versions invalidate affected records conservatively.

### 2. Model registry

All context and selector consumers move behind a registry protocol:

```text
metadata(name) -> ModelMetadata
iter_metadata(selection) -> Iterator[ModelMetadata]
upstream(names) -> Iterator[str]
downstream(names) -> Iterator[str]
hydrate(names, batch_size) -> Iterator[Model]
evict(names)
```

The first implementation wraps the current eager dictionary. The indexed implementation loads cached
model payloads on demand and uses a size-bounded LRU for hydrated models. Operations that explicitly
request `context.models` may materialize all models for backward compatibility and emit a diagnostic.

### 3. Metadata selection

Selector parsing and expansion operate on the graph index rather than hydrated models. Selection computes
three sets:

- Models whose local definitions participate in the plan.
- Upstream models required for rendering, schema propagation, and fingerprints.
- Downstream metadata required for indirect-change and removal analysis.

Only the first two sets are hydrated. Downstream models remain metadata-only unless fingerprint or schema
propagation proves that their semantic payload is required.

### 4. Bounded state hydration

Add state-reader APIs that yield snapshot headers and full snapshots in stable batches. Snapshot headers
contain identifiers, versions, fingerprints, kinds, parent identifiers, and environment-facing table
information without deserializing model payloads.

`CachingStateSync` becomes an entry- and byte-bounded LRU. Negative results are bounded separately.
Callers explicitly pin snapshots while a stage needs them and release them afterward. Existing unbounded
behavior remains available in eager mode during rollout.

### 5. Streaming context diff

Context diff is split into two passes:

1. Compare local metadata/fingerprints with environment snapshot headers and compute change sets.
2. Hydrate only added, modified, removed-boundary, and categorization-boundary snapshots.

The resulting diff stores snapshot identifiers and compact headers by default. Full `Snapshot` objects are
accessed through the registry and bounded state reader. Compatibility properties materialize collections
only for eager callers.

### 6. Streaming fingerprints and schemas

Fingerprint computation follows topological batches. Each completed fingerprint is persisted to the graph
index before its model is eligible for eviction. Mapping schemas use compact column contracts from the
index; full query rendering is performed only where contracts are absent or invalidated.

Cycles and unresolved dependencies fail before plan application. Batch boundaries never alter fingerprint
inputs or downstream categorization.

### 7. Streaming plan stages

Replace the eagerly built stage list with a replayable stage iterator. Each stage contains identifiers and
a batch loader instead of an all-snapshot dictionary. The evaluator requests a batch, performs the stage,
persists its checkpoint, and releases hydrated objects.

Stage order remains:

1. Push snapshot records.
2. Create physical schemas and objects.
3. Backfill missing intervals in DAG order.
4. Run audits.
5. Promote the complete environment atomically.
6. Perform post-promotion work and finalize.

Promotion is never streamed as partial environment updates. The final environment record is assembled from
compact snapshot table information and written atomically after all required pre-promotion stages succeed.

### 8. Checkpoints and retries

Each streaming application has a plan identifier and persisted stage/batch checkpoints. Retrying a failed
plan revalidates its project and environment fingerprints, skips completed idempotent batches, and resumes
before promotion. Checkpoints expire with the plan and are removed after finalization or cleanup.

The first implementation may persist checkpoints in the existing plan DAG/state facilities. A state schema
migration is introduced only if existing records cannot represent batch completion safely.

## Configuration and rollout

The planner mode is explicit during development:

```yaml
planner:
  mode: eager       # eager | streaming
  model_batch_size: 100
  snapshot_batch_size: 10
  hydrated_model_cache_size: 250
  hydrated_snapshot_cache_size: 250
  streaming_workers: 2
  streaming_worker_max_tasks: 25
```

`streaming_workers` is also capped by `MAX_FORK_WORKERS`. The coordinator is the only SQLite writer;
workers read indexed dependencies, persist immutable payload files, and return compact metadata or
serialized snapshots. Set `streaming_workers: 1` for the smallest process footprint, or increase it for
parallel discovery, schema propagation, and snapshot construction. Lowering
`streaming_worker_max_tasks` recycles processes more often and can reduce allocator retention at the cost
of additional process startup time.

Rollout stages:

1. Eager-only behavior through new registry interfaces.
2. Streaming shadow mode: build both plans, apply eager, and compare normalized plans.
3. Opt-in streaming apply with eager fallback before physical mutation.
4. Streaming default for supported native SQL projects.
5. dbt and dynamic Python parity, followed by removal of the fallback only in a future major release.

Every fallback records the unsupported feature and hydration counts. Silent mixed semantics are not
allowed.

## Delivery milestones

### Milestone A: contracts and observability

- Dual-runner accuracy tiers and deterministic mutation sequences.
- Hydrated-model, hydrated-snapshot, cache-hit, batch-size, and stage-release metrics.
- Memory profiles for 700, 1,400, and 2,100 model projects.

### Milestone B: compatibility abstractions

- Eager model-registry implementation.
- Bounded state cache with eager-compatible defaults.
- Snapshot-header and batch-reader APIs.
- No semantic or performance regression in eager mode.

### Milestone C: indexed loading and selection

- Versioned project graph index.
- Incremental SQL discovery and payload caching.
- Metadata selector and boundary calculation.
- Selected-plan peak memory scales with the selected boundary.

### Milestone D: streaming diff

- Header-based environment comparison.
- Batched model/snapshot hydration.
- Streaming fingerprint and schema propagation.
- Full mutation-tier parity.

### Milestone E: streaming apply

- Replayable stage iterator and bounded evaluator batches.
- Atomic promotion and retry checkpoints.
- Failure-injection parity and cleanup tests.

### Milestone F: rollout

- Native SQL, Python, dbt, multi-project, multi-gateway, and engine test matrices.
- Configuration reference, migration notes, diagnostics, and operational runbook.
- Stress-tier memory and execution-time acceptance report.

## Acceptance gates

- Zero normalized-plan, environment, schema, or data-digest mismatches across the accuracy tiers.
- Selected native-SQL plans hydrate no unrelated downstream model payloads.
- Peak memory grows with the selected semantic boundary, with a documented fixed overhead, rather than
  total project width.
- Full-plan streaming peak memory remains within the configured model and snapshot cache bounds plus the
  largest active execution batch.
- A failure before promotion leaves the previous environment active; retry produces the eager-equivalent
  final state.
- Eager mode remains unchanged until all supported-project gates pass.
