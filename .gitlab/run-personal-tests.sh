#!/bin/sh
# SPDX-License-Identifier: Apache-2.0
set -eu

project_dir=$(git rev-parse --show-toplevel)
cd "$project_dir"
report_dir="$project_dir/test-results/personal"
mkdir -p "$report_dir"
reference_dir=$(mktemp -d)
controller_name="sqlmesh-personal-tests-${CI_JOB_ID:-local}"

cleanup() {
    result=$?
    trap - EXIT INT TERM
    docker rm -f "$controller_name" >/dev/null 2>&1 || true
    docker compose -p randomgraphs-accuracy -f examples/randomgraphs/compose.accuracy.yaml \
        logs --no-color > "$report_dir/compose.log" 2>&1 || true
    docker compose -p randomgraphs-accuracy -f examples/randomgraphs/compose.accuracy.yaml \
        down --volumes --remove-orphans >/dev/null 2>&1 || true
    docker compose -p randomgraphs-model-kinds -f examples/randomgraphs/compose.model-kinds.yaml \
        down --volumes --remove-orphans >/dev/null 2>&1 || true
    rm -rf "$reference_dir"
    exit "$result"
}
trap cleanup EXIT INT TERM

attempt=0
until docker info >/dev/null 2>&1; do
    attempt=$((attempt + 1))
    if [ "$attempt" -ge 60 ]; then
        echo "Docker daemon did not become ready" >&2
        exit 1
    fi
    sleep 2
done
docker version
docker compose version

# Fetch the reference once and record the exact commit used for the comparison.
git -C "$reference_dir" init -q
git -C "$reference_dir" fetch --depth 1 https://github.com/SQLMesh/sqlmesh.git \
    "${REFERENCE_SQLMESH_SHA:-main}"
git -C "$reference_dir" checkout -q --detach FETCH_HEAD
reference_sha=$(git -C "$reference_dir" rev-parse HEAD)
candidate_sha=$(git rev-parse HEAD)
export REFERENCE_SQLMESH_IMAGE="sqlmesh-reference:$reference_sha"
export CANDIDATE_SQLMESH_IMAGE="sqlmesh-candidate:$candidate_sha"
controller_image="sqlmesh-personal-controller:$candidate_sha"
printf 'reference_sha=%s\ncandidate_sha=%s\nmerge_request_event_type=%s\naccuracy_tier=%s\nmutation_tier=%s\n' \
    "$reference_sha" "$candidate_sha" "${CI_MERGE_REQUEST_EVENT_TYPE:-branch}" \
    "${RANDOMGRAPHS_ACCURACY_TIER:-pr}" "${RANDOMGRAPHS_MUTATION_TIER:-pr}" \
    > "$report_dir/revisions.txt"
cat "$report_dir/revisions.txt"

docker build --build-arg PYTHON_IMAGE=mirror.gcr.io/library/python:3.11-slim \
    -f "$project_dir/examples/randomgraphs/Dockerfile" \
    -t "$REFERENCE_SQLMESH_IMAGE" "$reference_dir"
docker build --build-arg PYTHON_IMAGE=mirror.gcr.io/library/python:3.11-slim \
    -f examples/randomgraphs/Dockerfile -t "$CANDIDATE_SQLMESH_IMAGE" .
docker build --build-arg "CANDIDATE_SQLMESH_IMAGE=$CANDIDATE_SQLMESH_IMAGE" \
    -f .gitlab/Dockerfile.personal-tests -t "$controller_image" .

# The Docker executor mounts /builds into the build and DinD containers. Keep the same
# absolute path in the controller so Compose bind mounts see the harness files.
# Only the test controller receives the per-job Docker socket, not cluster API
# credentials or a socket from a Kubernetes node.
docker run --rm --name "$controller_name" \
    -v /var/run/docker.sock:/var/run/docker.sock \
    -v "$project_dir:$project_dir" -w "$project_dir" \
    -e REFERENCE_SQLMESH_IMAGE -e CANDIDATE_SQLMESH_IMAGE \
    -e "RANDOMGRAPHS_ACCURACY_TIER=${RANDOMGRAPHS_ACCURACY_TIER:-pr}" \
    -e "RANDOMGRAPHS_MUTATION_TIER=${RANDOMGRAPHS_MUTATION_TIER:-pr}" \
    -e PYTHONDONTWRITEBYTECODE=1 \
    "$controller_image" python -m pytest -c examples/randomgraphs/pytest.ini \
    examples/randomgraphs/tests --junitxml="$report_dir/junit.xml"
