<!-- SPDX-License-Identifier: Apache-2.0 -->

# Merge tests

The root `.gitlab-ci.yml` runs the full `examples/randomgraphs/tests` suite for
GitLab merge requests, pushes to `personal-tests`, and manual or scheduled
pipelines. Merge requests should target `personal-tests`, and their source
branches should include that branch's CI configuration and harness. GitHub
upstream contributions continue to use the separate GitHub workflow.

## Upstream PR checks

`.gitlab/upstream-pr-tests.yml` adds the Linux checks from upstream's
[PR workflow](https://github.com/SQLMesh/sqlmesh/blob/main/.github/workflows/pr.yaml)
and [DB2 workflow](https://github.com/SQLMesh/sqlmesh/blob/main/.github/workflows/db2.yaml).
It reuses the repository's Makefile, migration script, fixtures, and Compose
files rather than maintaining a separate copy of the tests.

| Jobs | Checks |
| --- | --- |
| `upstream-python` (Python 3.9–3.13) | `make py-style`, benchmarks except on 3.9, and all `make cicd-test` phases |
| `upstream-documentation` (Python 3.10) | Development/doc dependencies and `make doc-test` |
| `upstream-migrations` (Python 3.10) | Previous-release-to-candidate migrations for sushi and sushi_dbt |
| `upstream-engines` (Python 3.12) | DuckDB, PostgreSQL, MySQL, MSSQL, Trino, Spark, ClickHouse, RisingWave, StarRocks |
| `upstream-dbt` (Python 3.10, dbt 1.3–1.12) | Upstream dependency overrides, fast dbt tests, and `sqlmesh info` compatibility |
| `upstream-db2` (Python 3.12) | DB2 adapter and integration tests |
| `upstream-ui-style`, `upstream-ui` | Frontend lint, build, unit tests, and Chromium/Firefox/WebKit end-to-end tests |
| `upstream-vscode`, `upstream-vscode-e2e` | Workspace CI and extension end-to-end tests with pinned code-server 4.137.0 |
| `upstream-dco` | DCO sign-offs on contribution commits, excluding merge commits |

Python checks run for framework, test, example, backend, dependency, or CI changes.
Frontend and DB2 jobs use affected-path rules matching upstream, with additional
coverage for GitLab configuration and shared frontend dependency files. Changes
to GitLab CI run every portable suite. Manual and scheduled pipelines run all
available suites; `UPSTREAM_TEST_ALL=1` also forces them for an individual pipeline.
All automatic jobs are required and run alongside `personal-regressions`.
`UPSTREAM_TEST_ALL=1` forces all 33 merge-request checks, including Spark, Trino,
and DB2.
New commits cancel superseded interruptible pipelines, as upstream does.

A complete merge-request pipeline has 33 automatic required jobs.
Windows checks need a Windows
runner and are outside this Linux cluster. Upstream's cloud-engine checks run
only on `main`; they require separate test accounts and are not PR merge tests.
The cluster blocks the upstream fixtures' `cgr.dev` and `icr.io` image sources.
Spark and Trino instead use MinIO server `RELEASE.2025-09-07T16-13-09Z` and client
`RELEASE.2025-08-13T08-35-41Z` from official GitHub releases, verified against the
publisher's SHA-256 checksums and cached in the project registry. DB2 uses IBM's
official `ibmcom/db2:11.5.8.0` Docker Hub image pinned by digest, so its version
differs from upstream's floating community image. `.gitlab/prepare-engine-compose.py`
resolves the original Compose files and substitutes only these images in an
artifact copy. The Docker wrapper keeps existing Makefile and readiness commands
working with that copy; upstream fixtures and tests remain unchanged.
Infrastructure failures fail the pipeline just like test failures.

The upstream controller uses the job's Docker socket and network so database
ports are available on localhost, as on GitHub's runners. Dependency images are
cached in this project's GitLab container registry, keyed by the image recipe,
Python version, and `pyproject.toml`. Each job installs its exact candidate
checkout before testing. Registry login uses short-lived job credentials that
are not passed to the controller. The build context contains only the image
recipe and dependency manifest, so credentials and candidate source are not
baked into these shared images.

Jobs publish JUnit files and candidate/image revisions under `test-results/`.
The pytest wrapper also gives each isolated test process its own JUnit report;
the main upstream reports retain their original filenames. It excludes the
personal harness from upstream's bare pytest commands; that harness runs in its
own required job with separate images and fixtures. Browser jobs retain
Playwright reports and failure traces, and engine jobs retain Compose logs.
The code-server installer comes from its official GitHub release tag, since
the cluster blocks `code-server.dev`; the installed version remains 4.137.0.
VS Code end-to-end jobs use the repository's `.venv`, matching upstream fixtures
that invoke `.venv/bin/sqlmesh` directly.
Migration jobs restore the candidate checkout on failure as well as success.
Python, DuckDB, and dbt jobs pin DuckDB 1.5.5 (1.4.4 on Python 3.9) and cache its matching `httpfs`,
`tpch`, and `ducklake` binaries from PyPI wheels, because the cluster blocks
`extensions.duckdb.org`. DuckDB verifies publisher signatures when loading them;
the wheels' Python modules are not imported. Framework and test sources remain
the candidate's originals. Pytest stops at the first failed test to expose
diagnostics promptly; successful runs still execute every selected test.
The mypy wrapper likewise limits upstream's strict type checks to its original
tree; Ruff and formatting still cover the personal harness. Its standalone
imports and historical benchmark scripts use a separate testing environment.

The project uses merged-result pipelines: GitLab tests a temporary merge of the
source and target branches. A successful pipeline is required before merging.
The `personal-regressions` job publishes its JUnit report, pinned reference and
candidate revisions, and Compose logs under `test-results/personal/`. Both
random graph suites use the `pr` tier by default. Override
`RANDOMGRAPHS_ACCURACY_TIER` or `RANDOMGRAPHS_MUTATION_TIER` for a manual or
scheduled run. `REFERENCE_SQLMESH_SHA` pins a chosen upstream reference; otherwise
the job fetches upstream `main` once and records the resolved SHA.

The job streams each test result as it completes, runs CLI option checks first,
and stops at the first failure. A successful run executes the full suite. The
harness sets a historical model start date so its fixed execution-time scenarios
remain valid on later CI runs. Aggregate comparisons sort complete records to account for PostgreSQL's
unordered `UNION ALL` output while still comparing every value.
The custom PostgreSQL materialization converts empty backfill placeholders in a
transaction while preserving dependent development views.

## Kubernetes runner

The project-specific runner has tag `sqlmesh-personal-k8s` and runs in namespace
`ws-tryg360-next-development`. Provision it from a workspace authenticated to
that namespace and to GitLab:

```bash
python .gitlab/provision-runner.py
kubectl -n ws-tryg360-next-development rollout status deployment/sqlmesh-personal-runner
```

The provisioner reads the existing `gitlabtoken` environment variable, creates a
project runner, stores its authentication token in a Kubernetes Secret, applies
`kubernetes/runner.yaml`, and enables the GitLab merge-result and pipeline merge
requirements. It reuses an existing runner and Secret on subsequent runs. Do
not commit runner tokens or print Secret contents.

The runner uses GitLab's Docker executor inside a Kubernetes pod. Its Docker
sidecar hosts job containers and each job's separate Docker-in-Docker service.
The pod uses `sqlmesh-personal-tests` with no mounted Kubernetes API token and
needs no pod-attach or exec permissions. Only the Docker daemons run privileged;
build and helper containers remain unprivileged. No Kubernetes node Docker
socket is mounted. The outer Docker sidecar receives up to 8 CPUs and 16 GiB
of memory for image builds and all test databases. There are three runner pods,
each executing one job at a time. Each pod requests 2 CPUs and 4 GiB for Docker;
the three Docker sidecars together have limits of 24 CPUs and 48 GiB. Job Docker
storage is removed with its service container. Fixed Compose names are safe
across jobs because each job has its own daemon and checkout.

The Docker executor shares the `/builds` volume across job containers. The job builds
reference and candidate SQLMesh images, then runs pytest in a controller image
with the candidate installed. Pytest runs from the harness directory so the raw
checkout does not shadow the installed package and its generated version module.
The controller mounts the job's Docker socket and
checkout at the same absolute path, which makes Compose bind mounts work.
PostgreSQL, MySQL, and DuckDB test resources are isolated within the job's daemon
and cleaned up after testing.

The job's Docker daemon explicitly uses the cluster DNS server `10.96.5.5`.
Docker's embedded resolver in the service container does not propagate to
nested build containers; the explicit server keeps image builds and test
containers able to resolve package indexes and other external hosts.

## Testing a GitHub contribution after rebase

Fetch the pull request branch from `github` and rebase it onto a freshly fetched
`upstream/main` in a separate worktree. Create a GitLab `work/<name>` branch from
`personal-tests` and apply only the rebased contribution commits. Record the
GitHub PR URL, original head, upstream base, and rebased head in the GitLab commit
message. Verify that `sqlmesh/`, `sqlmesh_dbt/`, and upstream `tests/` match the
rebased contribution, then push the branch to `origin` and open a merge request
targeting `personal-tests`. This runs the contribution against the personal
harness while preserving the GitHub branch and its contribution identity.
