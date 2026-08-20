import os
import typing as t

import ibis  # type: ignore

DB_PATH = os.path.join(os.path.dirname(__file__), "data/local.duckdb")


def create_ibis_connection() -> t.Any:
    """Create an Ibis connection for the configured example backend."""
    if database_url := os.getenv("IBIS_DATABASE_URL"):
        return ibis.connect(database_url)
    return ibis.duckdb.connect(DB_PATH)
