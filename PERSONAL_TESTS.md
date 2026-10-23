<!-- SPDX-License-Identifier: Apache-2.0 -->

# Personal regression tests

`personal-tests` is a permanent test branch in `perjmi/sqlmesh`. It starts from
`main` and carries the reusable `examples/randomgraphs/` harness copied from
`streaming` at `bf0b604eb354a40da48dead1768173e55a323e52`.
Tests that import streaming-only internals remain on `streaming` alongside that
implementation. This branch changes no SQLMesh production code.

## Branch workflow

Keep `main` aligned with `upstream/main`. Extend the reusable harness on
`personal-tests`, commit it, and push to `origin/personal-tests`.
Bring upstream changes into this branch with a merge:

```bash
git switch personal-tests
git fetch upstream
git merge upstream/main
# Run the relevant checks below before committing a conflict resolution.
git push origin personal-tests
```

The initial branch is based on `origin/main`; the first upstream refresh is a
separate merge so it can be reviewed and tested deliberately.

For everyday development including these tests:

```bash
git switch -c work/my-change personal-tests
```

For a contribution intended for upstream, start its branch from `upstream/main`
and cherry-pick only the production fix and appropriate upstream tests from your
work branch. Keep the harness commits on the personal branch. Avoid rebasing or
force-pushing this shared, permanent branch.

## Fast local checks

Use Python 3.10 or newer for the harness and an isolated environment:

```bash
python -m venv .venv
. .venv/bin/activate
python -m pip install -e . pytest
python -m pytest -c examples/randomgraphs/pytest.ini \
  examples/randomgraphs/tests/test_generate_model_graph.py \
  examples/randomgraphs/tests/test_harness_configuration.py \
  examples/randomgraphs/tests/test_dual_planner_options.py::test_randomized_plan_options_preserve_groups_and_are_reproducible
```

These checks need no Docker. The dedicated pytest configuration avoids the full
repository's optional test-plugin requirements.

## Docker smoke checks

Docker with the Compose plugin is required for the parity suites. From the
repository root, build the candidate and compare it with itself as an initial
harness check:

```bash
candidate_image="randomgraphs-sqlmesh:$(git rev-parse HEAD)"
docker build -f examples/randomgraphs/Dockerfile -t "$candidate_image" .
export REFERENCE_SQLMESH_IMAGE="$candidate_image"
export CANDIDATE_SQLMESH_IMAGE="$candidate_image"
python -m pytest -c examples/randomgraphs/pytest.ini \
  examples/randomgraphs/tests/test_dual_planner_accuracy.py \
  examples/randomgraphs/tests/test_dual_planner_mutations.py
```

Both sides use their build's default planner unless explicitly configured.
Comparing the same build checks the harness; compare against a trusted build to
check a change or upgrade. Commit candidate code before building so the SHA tag
identifies the source accurately; rebuild after changes.

Build a reference image from a pinned SQLMesh revision without copying the harness
into that checkout. The harness is mounted into each image at runtime:

```bash
reference_sha=$(git rev-parse origin/main)
reference_path="../sqlmesh-reference-${reference_sha:0:12}"
git worktree add --detach "$reference_path" "$reference_sha"
reference_image="randomgraphs-sqlmesh:$reference_sha"
docker build -f "$PWD/examples/randomgraphs/Dockerfile" \
  -t "$reference_image" "$reference_path"
git worktree remove "$reference_path"
export REFERENCE_SQLMESH_IMAGE="$reference_image"
```

Record the chosen reference SHA and advance it deliberately after validation.
Dependencies are currently resolved at build time; retain the built image or pin
its digest when you need to reproduce the complete environment.

To compare eager and streaming in the same streaming-enabled image:

```bash
export REFERENCE_SQLMESH_IMAGE="randomgraphs-sqlmesh:<streaming-commit-sha>"
export CANDIDATE_SQLMESH_IMAGE="$REFERENCE_SQLMESH_IMAGE"
export REFERENCE_PLANNER_MODE=eager
export CANDIDATE_PLANNER_MODE=streaming
python -m pytest -c examples/randomgraphs/pytest.ini \
  examples/randomgraphs/tests/test_dual_planner_accuracy.py \
  examples/randomgraphs/tests/test_dual_planner_batch_matrix.py
unset REFERENCE_PLANNER_MODE CANDIDATE_PLANNER_MODE
```

Replace the placeholder image tag with an actual built image. Explicit eager,
shadow, and streaming modes require an image containing the streaming changes.

## Broader checks and extensions

Run all suites with:

```bash
python -m pytest -c examples/randomgraphs/pytest.ini examples/randomgraphs/tests
```

Use `RANDOMGRAPHS_ACCURACY_TIER=pr`, `nightly`, or `stress` for wider graphs.
Use `RANDOMGRAPHS_MUTATION_TIER=sweep10` for ten fixed mutation seeds, or `pr`,
`nightly`, or `stress` for larger graphs. The model-kind matrix covers PostgreSQL,
MySQL, and DuckDB; use `-k duckdb` on that module for a focused engine run.

Add each regression as a deterministic scenario with a fixed graph/mutation seed
and execution time. Compare normalized state and ordered model rows, and add
explicit expected behavior where parity alone could hide a shared bug. State
comparisons read internal SQLMesh tables, so assess schema and intentional
semantic changes when comparing releases.

Run Docker suites sequentially in one dedicated worktree. They currently share
fixed Compose project names, reset their test databases, modify generated models
and the audit fixture, and restore the normal seed-42 graph during cleanup.
Do not run two copies concurrently. Benchmarks and their historical results are
preserved in the harness but are separate from the everyday regression checks.
