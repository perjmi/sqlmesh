# SPDX-License-Identifier: Apache-2.0

FROM python:3.11-slim

ENV PIP_DISABLE_PIP_VERSION_CHECK=1 \
    PIP_NO_CACHE_DIR=1 \
    PYTHONUNBUFFERED=1

WORKDIR /workspace

COPY pyproject.toml README.md LICENSE ./
COPY sqlmesh ./sqlmesh
COPY sqlmesh_dbt ./sqlmesh_dbt

RUN pip install . psycopg2-binary pymysql

COPY examples/randomgraphs ./examples/randomgraphs

WORKDIR /workspace/examples/randomgraphs

CMD ["sqlmesh", "plan", "--auto-apply", "--no-prompts"]
