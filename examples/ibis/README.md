<!-- SPDX-License-Identifier: Apache-2.0 -->

# Ibis example with PostgreSQL

The default configuration still runs against DuckDB. The included container setup selects
the PostgreSQL gateway and configures Ibis to use the same database.

From this directory, build the SQLMesh image, start PostgreSQL, and apply the example plan:

```bash
docker compose up --build --abort-on-container-exit
```

PostgreSQL is exposed on host port `5432`. If that port is already in use, set a
different one with `POSTGRES_HOST_PORT=5433 docker compose up --build --abort-on-container-exit`.

For an interactive experimentation shell instead, start PostgreSQL in the background and
open a shell in a one-off SQLMesh container:

```bash
docker compose up -d postgres
docker compose run --rm sqlmesh bash
```

Inside the container, useful commands include:

```bash
sqlmesh plan --auto-apply
sqlmesh fetchdf "SELECT * FROM ibis.ibis_full_model_python"
sqlmesh fetchdf "SELECT * FROM ibis.ibis_full_model_sql"
```

Stop the services while retaining the PostgreSQL data volume:

```bash
docker compose down
```

To also delete the experiment data, run `docker compose down --volumes`.
