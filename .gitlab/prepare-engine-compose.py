#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
"""Resolve upstream Compose fixtures and replace blocked image distributions."""

from __future__ import annotations

import json
import os
import subprocess
from pathlib import Path


def main() -> None:
    engine = os.environ["ENGINE"]
    original = Path(f"tests/core/engine_adapter/integration/docker/compose.{engine}.yaml")
    config = json.loads(
        subprocess.check_output(
            ["/usr/local/bin/docker", "compose", "-f", str(original), "config", "--format", "json"],
            text=True,
        )
    )
    if engine in {"spark", "trino"}:
        image = os.environ["UPSTREAM_MINIO_IMAGE"]
        config["services"]["minio"]["image"] = image
        config["services"]["mc-job"]["image"] = image
    elif engine == "db2":
        config["services"]["db2"]["image"] = (
            "ibmcom/db2:11.5.8.0@sha256:"
            "d678ec116f66992366bf8e7f23df24fe16ccc3f70710243d2c88e3c687670e4c"
        )
    else:
        raise ValueError(f"No image substitution configured for {engine}")
    output = Path(os.environ["UPSTREAM_COMPOSE_FILE"])
    output.write_text(json.dumps(config, indent=2) + "\n")
    print(f"Resolved {engine} infrastructure: {output}")
    for name, service in config["services"].items():
        if "image" in service:
            print(f"{name}: {service['image']}")


if __name__ == "__main__":
    main()
