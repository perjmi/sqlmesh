# Main versus streaming full-plan benchmark

This benchmark compares the repository's `main` branch at `909ee91a` in eager mode with the
`streaming` branch at `872e0649` in native streaming mode. Each result contains ten samples using
random-graph seeds 0 through 9.

## Results

Values are the sample mean ± sample standard deviation. Peak memory is aggregate resident set size
(RSS), sampled every 20 ms across every process in the benchmark container.

| Inputs | Models | Main time (s) | Streaming time (s) | Time change | Main RSS (MiB) | Streaming RSS (MiB) | RSS reduction |
|---:|---:|---:|---:|---:|---:|---:|---:|
| 10 | 70 | 3.77 ± 0.10 | 5.12 ± 0.19 | +35.8% | 179.1 ± 0.7 | 169.6 ± 0.3 | 5.3% |
| 100 | 700 | 18.17 ± 0.56 | 34.70 ± 1.38 | +91.0% | 423.5 ± 1.9 | 299.0 ± 1.0 | 29.4% |
| 200 | 1,400 | 34.08 ± 0.49 | 66.98 ± 1.08 | +96.6% | 701.1 ± 2.5 | 444.7 ± 1.2 | 36.6% |
| 500 | 3,500 | 92.61 ± 3.35 | 173.34 ± 6.82 | +87.2% | 1,519.3 ± 4.5 | 879.5 ± 2.3 | 42.1% |

Distribution details:

| Inputs | Branch | Time median (s) | Time p95 (s) | RSS median (MiB) | RSS p95 (MiB) |
|---:|:---|---:|---:|---:|---:|
| 10 | main | 3.77 | 3.90 | 179.2 | 180.0 |
| 10 | streaming | 5.06 | 5.44 | 169.5 | 169.9 |
| 100 | main | 18.14 | 18.93 | 422.7 | 426.3 |
| 100 | streaming | 34.75 | 36.43 | 298.8 | 300.5 |
| 200 | main | 33.91 | 34.75 | 701.1 | 704.6 |
| 200 | streaming | 66.71 | 68.76 | 444.7 | 446.3 |
| 500 | main | 91.62 | 98.47 | 1,520.3 | 1,523.7 |
| 500 | streaming | 170.88 | 184.49 | 880.1 | 881.8 |

## Method

- A sample starts with a fresh SQLMesh process, empty cache, and freshly recreated PostgreSQL
  database. Container startup and database reset are outside the timer.
- The timer includes SQLMesh import, `Context` construction/project loading, and construction of a
  complete plan against empty state with `skip_tests=True` and `skip_backfill=True`.
- Applying the plan and executing/backfilling models are excluded, so PostgreSQL query execution does
  not obscure planner performance.
- Every sample asserts that the plan contains exactly `7 * inputs` new snapshots.
- Both revisions use Python 3.11, the same dependency build, `MAX_FORK_WORKERS=1`, identical generated
  files for each paired seed, and alternating branch order between seeds.

## Interpretation

The streaming implementation trades planning time for memory. At 3,500 models it reduces aggregate
peak RSS by 42.1% (about 640 MiB), while taking 87.2% longer.

Total full-plan memory is not yet bounded independently of project size. This branch bounds native
local-project model hydration, edge traversal, and schema-column propagation, but the completed plan
still contains project-sized snapshot and plan structures. Their retained memory explains why the
streaming measurements continue to increase with model count even though graph edges and the global
column catalog no longer reside in Python memory.
