#!/bin/sh
# SPDX-License-Identifier: Apache-2.0
set -eu

project_dir=$(git rev-parse --show-toplevel)
cd "$project_dir"
report_dir="$project_dir/test-results/upstream"
mkdir -p "$report_dir"
controller_name="sqlmesh-upstream-tests-${CI_JOB_ID:-local}"
build_context=$(mktemp -d)
export DOCKER_CONFIG
DOCKER_CONFIG=$(mktemp -d)

cleanup() {
    result=$?
    trap - EXIT INT TERM
    docker rm -f "$controller_name" >/dev/null 2>&1 || true
    if [ "${UPSTREAM_TEST_SUITE:-}" = engine ] && [ "${ENGINE:-duckdb}" != duckdb ]; then
        compose="tests/core/engine_adapter/integration/docker/compose.$ENGINE.yaml"
        docker compose -f "$compose" logs --no-color > "$report_dir/compose.log" 2>&1 || true
        docker compose -f "$compose" down --volumes --remove-orphans >/dev/null 2>&1 || true
    fi
    rm -rf "$build_context" "$DOCKER_CONFIG"
    exit "$result"
}
trap cleanup EXIT INT TERM

attempt=0
until docker info >/dev/null 2>&1; do
    attempt=$((attempt + 1))
    [ "$attempt" -lt 60 ] || { echo 'Docker daemon did not become ready' >&2; exit 1; }
    sleep 2
done

case "$UPSTREAM_TEST_SUITE" in
    ui|ui-style|vscode|vscode-e2e)
        dockerfile=.gitlab/Dockerfile.upstream-frontend
        image_family=browser
        ;;
    python|documentation|migrations|engine|dbt)
        dockerfile=.gitlab/Dockerfile.upstream-tests
        image_family="python-${PYTHON_VERSION:-3.12}"
        ;;
    *) echo "Unknown upstream test suite: $UPSTREAM_TEST_SUITE" >&2; exit 1 ;;
esac

# Reuse dependency images across the isolated job daemons and runner pods. The
# key changes whenever dependencies or the image recipe change. Registry access
# uses the job's short-lived credentials, which are not passed to test containers.
image_key=$(cat "$dockerfile" pyproject.toml | sha256sum | cut -c 1-20)
image="${CI_REGISTRY_IMAGE:-sqlmesh}/ci-$image_family:$image_key"
if [ -n "${CI_REGISTRY:-}" ]; then
    printf '%s' "$CI_REGISTRY_PASSWORD" | docker login "$CI_REGISTRY" \
        --username "$CI_REGISTRY_USER" --password-stdin
fi
if ! docker pull "$image"; then
    cp pyproject.toml "$build_context/pyproject.toml"
    cp "$dockerfile" "$build_context/Dockerfile"
    docker build --build-arg "PYTHON_VERSION=${PYTHON_VERSION:-3.12}" \
        -t "$image" "$build_context"
    if [ -n "${CI_REGISTRY:-}" ]; then
        docker push "$image"
    fi
fi

printf 'candidate_sha=%s\nmerge_request_event_type=%s\nsuite=%s\npython=%s\nengine=%s\ndbt=%s\nimage=%s\n' \
    "$(git rev-parse HEAD)" "${CI_MERGE_REQUEST_EVENT_TYPE:-branch}" \
    "$UPSTREAM_TEST_SUITE" "${PYTHON_VERSION:-3.12}" "${ENGINE:-}" "${DBT_VERSION:-}" \
    "$(docker image inspect --format '{{.Id}}' "$image")" \
    > "$report_dir/revisions.txt"
cat "$report_dir/revisions.txt"

# Compose publishes database ports on the per-job daemon's localhost. Sharing
# that network gives upstream's localhost-based fixtures exactly the same layout
# as a GitHub runner, while keeping every job isolated from the Kubernetes node.
docker run --rm --name "$controller_name" --network host --shm-size=1g \
    -v /var/run/docker.sock:/var/run/docker.sock \
    -v "$project_dir:$project_dir" -w "$project_dir" \
    -e CI=true -e UPSTREAM_TEST_SUITE -e PYTHON_VERSION -e ENGINE -e DBT_VERSION \
    -e DBT_SEND_ANONYMOUS_USAGE_STATS=false \
    -e PYTEST_XDIST_AUTO_NUM_WORKERS=2 -e SPARK_LOCAL_IP=127.0.0.1 \
    -e PYTHONDONTWRITEBYTECODE=1 \
    "$image" bash .gitlab/upstream-tests.sh
