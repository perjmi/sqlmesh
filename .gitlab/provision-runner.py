#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
"""Provision the SQLMesh project runner and merge-result CI in this workspace."""

from __future__ import annotations

import base64
import json
import os
import subprocess
import urllib.error
import urllib.request
from pathlib import Path

API = "https://gitlab.devops.prdroot.net/api/v4"
PROJECT_ID = 15354
NAMESPACE = "ws-tryg360-next-development"
NAME = "sqlmesh-personal-runner"
TAG = "sqlmesh-personal-k8s"
MANIFEST = Path(__file__).parent / "kubernetes" / "runner.yaml"


def api(method: str, path: str, body: dict | None = None) -> dict | list:
    token = os.environ.get("gitlabtoken")
    if not token:
        raise SystemExit("Set the gitlabtoken environment variable before provisioning.")
    request = urllib.request.Request(
        API + path,
        method=method,
        headers={"PRIVATE-TOKEN": token, "Content-Type": "application/json"},
        data=json.dumps(body).encode() if body is not None else None,
    )
    try:
        with urllib.request.urlopen(request, timeout=30) as response:
            return json.load(response)
    except urllib.error.HTTPError as error:
        raise SystemExit(f"GitLab {method} {path} failed: HTTP {error.code}") from None


def provision() -> None:
    subprocess.run(["kubectl", "apply", "--dry-run=server", "-f", str(MANIFEST)], check=True)
    existing = subprocess.run(
        ["kubectl", "-n", NAMESPACE, "get", "secret", NAME, "--ignore-not-found", "-o", "json"],
        text=True,
        capture_output=True,
        check=True,
    ).stdout
    runners = api("GET", f"/projects/{PROJECT_ID}/runners?type=project_type&per_page=100")
    matches = [runner for runner in runners if runner["description"] == TAG]
    if existing:
        secret = json.loads(existing)
        runner_id = int(secret["metadata"]["annotations"]["gitlab-runner-id"])
        if not any(runner["id"] == runner_id for runner in matches):
            raise SystemExit("Existing runner secret does not match an active project runner.")
    else:
        if matches:
            raise SystemExit("Runner already exists without its secret; recover its token first.")
        runner = api(
            "POST",
            "/user/runners",
            {
                "runner_type": "project_type",
                "project_id": PROJECT_ID,
                "description": TAG,
                "tag_list": [TAG],
                "run_untagged": False,
                "locked": True,
                "access_level": "not_protected",
                "maximum_timeout": 7200,
            },
        )
        runner_id = runner["id"]
        secret = {
            "apiVersion": "v1",
            "kind": "Secret",
            "metadata": {
                "name": NAME,
                "namespace": NAMESPACE,
                "annotations": {"gitlab-runner-id": str(runner_id)},
            },
            "type": "Opaque",
            "data": {"token": base64.b64encode(runner["token"].encode()).decode()},
        }
        # Keep the token out of shell arguments, files, and tool output.
        subprocess.run(
            ["kubectl", "apply", "-f", "-"], input=json.dumps(secret), text=True, check=True
        )
    subprocess.run(["kubectl", "apply", "-f", str(MANIFEST)], check=True)
    project = api(
        "PUT",
        f"/projects/{PROJECT_ID}",
        {
            "merge_pipelines_enabled": True,
            "only_allow_merge_if_pipeline_succeeds": True,
            "build_timeout": 7200,
        },
    )
    if not project.get("merge_pipelines_enabled"):
        raise SystemExit("GitLab did not enable merged-result pipelines.")
    print(f"Configured project runner {runner_id} in {NAMESPACE}; merged-result CI enabled.")


if __name__ == "__main__":
    provision()
