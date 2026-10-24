#!/bin/sh
# SPDX-License-Identifier: Apache-2.0
set -eu

base=${CI_MERGE_REQUEST_DIFF_BASE_SHA:?Merge-request diff base is required}
git rev-list --no-merges "$base..$CI_COMMIT_SHA" | while IFS= read -r commit; do
    if ! git show -s --format=%B "$commit" \
        | grep -Eq '^Signed-off-by: .+ <[^<>[:space:]]+@[^<>[:space:]]+>$'; then
        echo "Missing DCO sign-off: $commit" >&2
        exit 1
    fi
    git show -s --format='DCO OK: %h %s' "$commit"
done
