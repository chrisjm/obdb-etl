from __future__ import annotations

import duckdb
from pandas import DataFrame
from pathlib import Path


def write_df_to_duckdb(
    df: DataFrame,
    table_name: str,
    db_path: str | Path,
    load_spatial: bool = False,
) -> int:
    """
    Write a DataFrame to DuckDB, returning the row count written.
    Optionally loads the spatial extension.
    """
    with duckdb.connect(database=str(db_path), read_only=False) as con:
        if load_spatial:
            con.sql("INSTALL spatial;")
            con.sql("LOAD spatial;")

        # duckdb.connect in tests may be monkeypatched with a fake object; prefer a method if present
        if hasattr(con, "register"):
            con.register("df", df)
        elif hasattr(con, "from_df"):
            con.from_df(df, "df")  # type: ignore[attr-defined]
        else:
            raise AttributeError(
                "Connection object does not support registering DataFrames"
            )
        con.sql(f"CREATE OR REPLACE TABLE {table_name} AS SELECT * FROM df")
        result = con.sql(f"SELECT COUNT(*) FROM {table_name}").fetchone()
        row_count = result[0] if result is not None else 0
    return row_count


__all__ = ["write_df_to_duckdb"]
