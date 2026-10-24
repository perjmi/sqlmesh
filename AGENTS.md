<!-- SPDX-License-Identifier: Apache-2.0 -->

# Repository instructions

This repository contains the SQLMesh data transformation framework, its web UI,
VS Code extension, documentation, and tests. These instructions apply throughout
the checkout. Read [PERSONAL_TESTS.md](PERSONAL_TESTS.md) for the personal branch
workflow and regression harness, and [CONTRIBUTING.md](CONTRIBUTING.md) and
[docs/development.md](docs/development.md) for upstream contribution conventions.

## Repository map

| Path | Purpose |
| --- | --- |
| `sqlmesh/core/` | Models, plans, snapshots, state synchronization, engine adapters, audits, and scheduling |
| `sqlmesh/cli/`, `sqlmesh/cicd/` | CLI and CI/CD bot entry points |
| `sqlmesh/dbt/`, `sqlmesh_dbt/` | dbt integration and compatibility CLI |
| `sqlmesh/lsp/` | Language server |
| `sqlmesh/migrations/` | Persistent SQLMesh state migrations |
| `web/client/`, `web/common/`, `web/server/` | React UI, shared frontend code, and FastAPI backend |
| `vscode/extension/`, `vscode/react/`, `vscode/bus/` | VS Code extension, React components, and communication package |
| `tests/` | Framework, engine, CLI, web, dbt, and integration tests |
| `examples/`, `benchmarks/`, `docs/`, `tooling/` | Example projects, performance checks, documentation, and development tools |
| `examples/randomgraphs/` | Personal regression and planner comparison harness |
| `.gitlab/`, `.gitlab-ci.yml` | Kubernetes runner provisioning, upstream PR checks, and personal regression merge tests |

## GitHub and GitLab

The checkout has three remotes. The user's GitHub account is **`perjmi`**; the
internal GitLab account is **`statpej`**. Both accounts belong to Per Jensen.

| Remote | Repository | Purpose |
| --- | --- | --- |
| `origin` | `https://gitlab.devops.prdroot.net/statpej/sqlmesh.git` | Private GitLab repository for everyday development and personal tests |
| `github` | `https://github.com/perjmi/sqlmesh` | Public GitHub fork for upstream pull requests |
| `upstream` | `https://github.com/SQLMesh/sqlmesh.git` | Upstream SQLMesh source |

The default push remote is `origin`. Use an explicit remote when pushing;
contribution branches intended for GitHub go to `github`. There is no automatic
mirroring between GitLab and GitHub.

GitHub configuration and integration code live in these locations:

- `.github/workflows/`: PR checks, engine tests, DCO checks, and releases.
- `.github/scripts/`: CI helper scripts; `.github/` also holds Dependabot
  configuration and the pull request template.
- `sqlmesh/integrations/github/cicd/`: GitHub bot commands, configuration, and
  controller. The bot currently supports GitHub Actions.
- `tests/integrations/github/` and `tests/fixtures/github/`: Integration tests
  and GitHub event payloads.
- `docs/integrations/github.md` and `docs/integrations/github/`: Integration
  guide and its images.

GitLab merge tests are configured in `.gitlab-ci.yml`; `.gitlab/` contains the
runner manifest, provisioner, controller Dockerfiles, and test scripts. Read
[.gitlab/README.md](.gitlab/README.md) for cluster operation. The runner uses tag
`sqlmesh-personal-k8s` in namespace `ws-tryg360-next-development`, with three runner
pods, one job per pod, and a separate Docker daemon for each job. Merge requests target
`personal-tests` and use merged-result pipelines; a successful pipeline is
required to merge. GitLab branch conventions remain in `PERSONAL_TESTS.md`.

`.gitlab/upstream-pr-tests.yml` translates upstream's Linux Python, engine, dbt,
migration, documentation, frontend, VS Code, and DCO checks. Keep the Makefile
and existing fixtures as the test sources. Changes to GitLab CI run all portable
suites; normal merge requests use affected-path rules, and manual/scheduled
pipelines run everything. `UPSTREAM_TEST_ALL=1` forces every portable suite.
Spark, Trino, and DB2 are manual while the cluster blocks their `cgr.dev` and
`icr.io` image sources; `UPSTREAM_TEST_NETWORK_ENGINES=1` enables them as required checks.
Dependency images are cached in the project registry; each job installs the
exact checkout before testing. Do not forward registry credentials to test
controllers. The Docker network shared with each upstream controller belongs to
that job's isolated daemon, so Compose localhost ports cannot collide across jobs.
Upstream test controllers must not set `SQLMESH__` configuration variables,
because configuration tests expect a clean environment. DuckDB extension wheels
use a matching supported engine version and retain publisher signature checks.

Authenticated Git and API access to both hosts has been verified in this
workspace. GitHub uses the `gh` CLI; `gh api user` checks the active account and
`gh repo view perjmi/sqlmesh` reads repository metadata. For the internal GitLab,
the API base is `https://gitlab.devops.prdroot.net/api/v4`; the project is
`statpej/sqlmesh` (project ID `15354`). The current workspace provides its API
token in the lowercase environment variable `gitlabtoken`. Read credentials
programmatically and never print or commit them. Authentication is specific to
each workspace and host, so verify the active account when needed.

## Branches and contribution identity

- Keep `main` aligned with `upstream/main`.
- `personal-tests` is a permanent branch carrying the reusable random graph
  harness. Refresh it by merging upstream changes; avoid rebasing or
  force-pushing this shared branch.
- Start everyday work that needs the harness on `work/<name>` from
  `personal-tests`. Harness maintenance belongs on the personal branch.
- Start upstream contributions on `contribution/<name>` from `upstream/main`.
  Bring over only the production fix and appropriate upstream tests, keeping
  personal harness commits on the personal branch. Open GitHub pull requests
  from `perjmi/sqlmesh` to `SQLMesh/sqlmesh`.
- GitLab's push rule requires an author email registered to a project member.
  The local `includeIf` configuration selects
  `Per Jensen <18235555+perjmi@users.noreply.github.com>` on `contribution/*`
  branches. Check the selected identity before committing. These settings live
  in `.git/config` and `.git/github-contribution-identity.gitconfig` and must be
  configured again in a new checkout.
- Upstream contributions require DCO sign-off (`git commit -s`). When moving
  your GitLab-authored fix to a contribution branch, use the cherry-pick and
  `--reset-author` procedure documented in `PERSONAL_TESTS.md`.

## Development and checks

Use a virtual environment. The Python package supports Python 3.9 or newer; the
random graph harness requires Python 3.10 or newer. Frontend packages require
Node.js 22 or newer and pnpm 10 or newer.

| Task | Command |
| --- | --- |
| Install framework development dependencies | `make install-dev` |
| Install frontend dependencies | `pnpm install` |
| Full style checks before submitting a contribution | `make style` |
| Python style checks | `make py-style` |
| Fast framework tests | `make fast-test` |
| GitHub bot tests | `make github-test` |
| Backend web tests | `make web-test` |
| Frontend lint | `pnpm run lint` |
| Frontend unit tests | `pnpm --prefix web/client run test:unit` |
| Documentation doctests | `make doc-test` |

Run focused tests for the affected behavior first. The framework pytest
configuration is in `pyproject.toml`, with optional plugins required by the
development dependencies. Reuse existing fixtures and test markers, including
the isolation markers for tests that cannot share a process. Documentation-only
edits should be checked for valid references and whitespace.

For fast harness checks without Docker or the full framework test plugins:

```bash
python -m pytest -c examples/randomgraphs/pytest.ini \
  examples/randomgraphs/tests/test_generate_model_graph.py \
  examples/randomgraphs/tests/test_harness_configuration.py \
  examples/randomgraphs/tests/test_dual_planner_options.py::test_randomized_plan_options_preserve_groups_and_are_reproducible
```

Docker parity suites require Docker Compose and must run sequentially in one
dedicated worktree. They share fixed Compose project names, reset test databases,
and modify generated models and an audit fixture. Follow `PERSONAL_TESTS.md` for
image builds, reference SHA selection, and planner mode settings. Explicit eager,
shadow, and streaming modes require an image containing the streaming changes.
Add deterministic regressions with fixed seeds and execution times; include
expected behavior because parity alone can miss a bug shared by both builds.

## Implementation conventions

Follow existing module patterns, preserve compatibility, and assess persistent
state migration needs when changing snapshots or state schemas. Add SPDX license
headers to new files as described in `CONTRIBUTING.md`.

Python uses Ruff linting and formatting with a 100-character line length, and
mypy for type checking. `pyproject.toml` and `.pre-commit-config.yaml` are the
tooling configuration sources. Avoid module-level imports of `duckdb`, `numpy`,
and `pandas`; the Ruff configuration bans them. TypeScript and React packages
use ESLint and Prettier.

`.claude/agents/` contains developer, reviewer, QA, and technical writer role
definitions. Consult them when relevant, while following the current repository
tooling configuration and the user's instructions.
