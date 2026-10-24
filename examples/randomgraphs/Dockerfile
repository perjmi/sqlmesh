# SPDX-License-Identifier: Apache-2.0

ARG PYTHON_IMAGE=python:3.11-slim
FROM ${PYTHON_IMAGE}

ENV PIP_DISABLE_PIP_VERSION_CHECK=1 \
    PIP_NO_CACHE_DIR=1 \
    PYTHONUNBUFFERED=1

WORKDIR /workspace

COPY pyproject.toml README.md LICENSE ./
COPY sqlmesh ./sqlmesh
COPY sqlmesh_dbt ./sqlmesh_dbt

RUN pip install . psycopg2-binary pymysql

# Compose mounts the harness separately so this Dockerfile also builds revisions
# that do not contain examples/randomgraphs.
WORKDIR /workspace/examples/randomgraphs

CMD ["sqlmesh", "plan", "--auto-apply", "--no-prompts"]
