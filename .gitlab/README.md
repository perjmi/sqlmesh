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

The manager uses the existing workspace service account, which can manage pods
within this namespace. Job pods use `sqlmesh-personal-tests` with no mounted
Kubernetes API token. Each job receives its own Docker-in-Docker sidecar and
ephemeral Docker storage; only that sidecar runs privileged. No Kubernetes node
Docker socket is mounted. The Docker sidecar receives up to 8 CPUs and 16 GiB
of memory for image builds and all test databases. The runner executes one job
at a time.

The runner shares the `/builds` volume across its containers. The job builds
reference and candidate SQLMesh images, then runs pytest in a controller image
with the candidate installed. The controller mounts the job's Docker socket and
checkout at the same absolute path, which makes Compose bind mounts work.
PostgreSQL, MySQL, and DuckDB test resources are isolated within the job's daemon
and cleaned up after testing.

## Testing a GitHub contribution after rebase

Fetch the pull request branch from `github` and rebase it onto a freshly fetched
`upstream/main` in a separate worktree. Create a GitLab `work/<name>` branch from
`personal-tests` and apply only the rebased contribution commits. Record the
GitHub PR URL, original head, upstream base, and rebased head in the GitLab commit
message. Verify that `sqlmesh/`, `sqlmesh_dbt/`, and upstream `tests/` match the
rebased contribution, then push the branch to `origin` and open a merge request
targeting `personal-tests`. This runs the contribution against the personal
harness while preserving the GitHub branch and its contribution identity.
