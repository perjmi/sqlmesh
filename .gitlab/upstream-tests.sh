#!/usr/bin/env bash
# SPDX-License-Identifier: Apache-2.0
set -euo pipefail

project_dir=$(pwd)
git config --global --add safe.directory "$project_dir"
candidate_sha=$(git rev-parse HEAD)
report_dir="$project_dir/test-results/upstream"
export PATH="$project_dir/.gitlab/bin:$PATH"
export PYTEST_REPORT_DIR="$report_dir"

prepare_duckdb() {
    # The cluster blocks extensions.duckdb.org. Matching PyPI wheels contain
    # DuckDB's signed binaries; pin the supported engine version for repeatability.
    duckdb_version=1.5.5
    if [[ "$PYTHON_VERSION" == 3.9 ]]; then
        duckdb_version=1.4.4
    fi
    uv pip install "duckdb==$duckdb_version" "duckdb-extension-httpfs==$duckdb_version" \
        "duckdb-extension-tpch==$duckdb_version" "duckdb-extension-ducklake==$duckdb_version"
    python .gitlab/prepare-duckdb-extensions.py
}

if [[ "$UPSTREAM_TEST_SUITE" == migrations ]]; then
    # The upstream migration script temporarily checks out a release tag. Restore
    # the candidate even on failure so artifacts retain the pipeline's checkout.
    trap 'git checkout --detach "$candidate_sha"' EXIT
fi

case "$UPSTREAM_TEST_SUITE" in
    python)
        make install-dev
        prepare_duckdb
        make py-style
        if [[ "$PYTHON_VERSION" != 3.9 ]]; then
            make benchmark-ci
        fi
        make cicd-test
        ;;
    documentation)
        make install-dev install-doc
        PYTEST_ADDOPTS="--junitxml=$report_dir/junit-documentation.xml" make doc-test
        ;;
    migrations)
        bash .github/scripts/test_migration.sh sushi '--gateway duckdb_persistent'
        bash .github/scripts/test_migration.sh sushi_dbt '--config migration_test_config'
        ;;
    engine)
        make install-dev
        if [[ "$ENGINE" == duckdb ]]; then
            prepare_duckdb
        fi
        if [[ "$ENGINE" == db2 ]]; then
            apt-get update && apt-get install -y --no-install-recommends libxml2-dev
        fi
        make "$ENGINE-test"
        ;;
    dbt)
        # Old dbt releases have conflicting web/LSP requirements; preserve the
        # upstream Makefile's separate installation and compatibility overrides.
        uv venv .venv
        source .venv/bin/activate
        export PATH="$project_dir/.gitlab/bin:$PATH"
        make "install-dev-dbt-$DBT_VERSION"
        prepare_duckdb
        case "$DBT_VERSION" in
            1.3|1.4|1.5)
                schema_file=tests/fixtures/dbt/sushi_test/models/schema.yml
                awk '
                    /^semantic_models:/ { in_semantic=1; next }
                    /^metrics:/ { in_metrics=1; next }
                    /^[^ ]/ && (in_semantic || in_metrics) { in_semantic=0; in_metrics=0 }
                    !in_semantic && !in_metrics { print }
                ' "$schema_file" > "$schema_file.tmp"
                mv "$schema_file.tmp" "$schema_file"
                ;;
        esac
        if [[ "$DBT_VERSION" == 1.6 ]]; then
            pytest -n auto -m 'dbt and fast' --reruns 3 --ignore=tests/lsp --ignore=tests/web
        else
            make dbt-fast-test
        fi
        cd examples/sushi_dbt
        sed -i 's/target: in_memory/target: postgres/g' profiles.yml
        case "$DBT_VERSION" in
            1.3|1.4)
                sed -i -e 's/, version=1) }}/) }}/g' -e 's/, v=1) }}/) }}/g' models/top_waiters.sql
                ;;
        esac
        sqlmesh info --skip-connection
        ;;
    ui-style|ui|vscode|vscode-e2e)
        export PLAYWRIGHT_SKIP_BROWSER_DOWNLOAD=1
        pnpm install --frozen-lockfile
        case "$UPSTREAM_TEST_SUITE" in
            ui-style) pnpm run lint ;;
            ui)
                pnpm --prefix web/client run build
                pnpm --prefix web/client run test:unit --reporter=default --reporter=junit \
                    --outputFile="$report_dir/junit-ui-unit.xml"
                PLAYWRIGHT_SKIP_BUILD=1 PLAYWRIGHT_JUNIT_OUTPUT_FILE="$report_dir/junit-ui-e2e.xml" \
                    pnpm --prefix web/client run test:e2e --reporter=line,junit,html
                ;;
            vscode) pnpm run ci ;;
            vscode-e2e)
                make install-dev
                curl -fsSL --retry 3 \
                    https://raw.githubusercontent.com/coder/code-server/v4.137.0/install.sh \
                    | sh -s -- --version 4.137.0
                cd vscode/extension
                PLAYWRIGHT_JUNIT_OUTPUT_FILE="$report_dir/junit-vscode-e2e.xml" \
                    pnpm run test:e2e --reporter=line,junit,html
                ;;
        esac
        ;;
esac
