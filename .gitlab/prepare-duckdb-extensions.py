#!/usr/bin/env python
# SPDX-License-Identifier: Apache-2.0
"""Cache publisher-signed extensions distributed as matching PyPI wheels."""

from importlib.metadata import distribution
from pathlib import Path
import shutil


def main() -> None:
    import duckdb

    with duckdb.connect() as connection:
        platform = connection.execute("PRAGMA platform").fetchone()[0]
        version = duckdb.__version__
        cache = Path.home() / ".duckdb" / "extensions" / f"v{version}" / platform
        cache.mkdir(parents=True, exist_ok=True)
        for extension in ("httpfs", "tpch", "ducklake"):
            package = distribution(f"duckdb-extension-{extension}")
            if package.version != version:
                raise RuntimeError(f"{extension} {package.version} does not match DuckDB {version}")
            binary = package.locate_file(
                f"duckdb_extension_{extension}/extensions/v{version}/{extension}.duckdb_extension"
            )
            shutil.copyfile(binary, cache / f"{extension}.duckdb_extension")
            # Keep DuckDB's signature verification enabled. Only binary data is
            # copied from the wheels; their Python modules are never imported.
            connection.install_extension(extension)
            connection.load_extension(extension)
            print(f"Verified and cached {extension} for DuckDB {version} ({platform})")


if __name__ == "__main__":
    main()
