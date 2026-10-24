<!-- SPDX-License-Identifier: Apache-2.0 -->

# Personal regression merge tests

The root `.gitlab-ci.yml` runs the full `examples/randomgraphs/tests` suite for
GitLab merge requests, pushes to `personal-tests`, and manual or scheduled
pipelines. Merge requests should target `personal-tests`, and their source
branches should include that branch's CI configuration and harness. GitHub
upstream contributions continue to use the separate GitHub workflow.

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
harness sets a historical model start date so its fixed execution-time scenarios remain valid on later CI
runs. Aggregate comparisons sort complete records to account for PostgreSQL's
unordered `UNION ALL` output while still comparing every value.

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
of memory for image builds and all test databases. The runner executes one job
at a time, and job Docker storage is removed with its service container.

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
