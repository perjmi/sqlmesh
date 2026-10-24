#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
"""Resolve upstream Compose fixtures with reachable dependency sources."""

from __future__ import annotations

import json
import os
import shutil
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
    output = Path(os.environ["UPSTREAM_COMPOSE_FILE"])
    if engine in {"spark", "trino"}:
        image = os.environ["UPSTREAM_MINIO_IMAGE"]
        config["services"]["minio"]["image"] = image
        config["services"]["mc-job"]["image"] = image
        if engine == "trino":
            # Nested Docker must not size Trino's 80% initial heap from the
            # Kubernetes node's RAM. Leave room for four Hive metastores and CI.
            trino = config["services"]["trino"]
            trino["mem_limit"] = "8g"
            trino["cpus"] = 4.0
            trino.setdefault("environment", {})["JDK_JAVA_OPTIONS"] = (
                "-Xms512m -Xmx3g -XX:MaxDirectMemorySize=1g -XX:ActiveProcessorCount=4"
            )
            config["services"]["nessie"]["mem_limit"] = "1g"
        if engine == "spark":
            # Keep upstream's Spark base and JAR versions, using Maven Central's
            # reachable official endpoint for both image builds and Ivy resolution.
            spark = config["services"]["spark"]
            recipe = (Path(spark["build"]["context"]) / "Dockerfile").read_text()
            recipe = recipe.replace(
                "https://jdbc.postgresql.org/download/postgresql-42.5.0.jar",
                "https://repo.maven.apache.org/maven2/org/postgresql/postgresql/42.5.0/"
                "postgresql-42.5.0.jar",
            ).replace("https://repo1.maven.org/maven2/", "https://repo.maven.apache.org/maven2/")
            build_dir = output.parent / "spark-build"
            build_dir.mkdir(exist_ok=True)
            (build_dir / "Dockerfile").write_text(
                recipe + "\nCOPY ivysettings.xml /opt/bitnami/spark/conf/ivysettings.xml\n"
            )
            shutil.copyfile(Path(".gitlab/spark-ivysettings.xml"), build_dir / "ivysettings.xml")
            spark["build"]["context"] = str(build_dir.resolve())
            spark["command"].extend(
                ["--conf", "spark.jars.ivySettings=/opt/bitnami/spark/conf/ivysettings.xml"]
            )
    elif engine == "db2":
        config["services"]["db2"]["image"] = (
            "ibmcom/db2:11.5.8.0@sha256:"
            "d678ec116f66992366bf8e7f23df24fe16ccc3f70710243d2c88e3c687670e4c"
        )
    else:
        raise ValueError(f"No image substitution configured for {engine}")
    output.write_text(json.dumps(config, indent=2) + "\n")
    print(f"Resolved {engine} infrastructure: {output}")
    for name, service in config["services"].items():
        if "image" in service:
            print(f"{name}: {service['image']}")


if __name__ == "__main__":
    main()
