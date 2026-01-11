from __future__ import annotations

import duckdb
from pandas import DataFrame
from pathlib import Path
from typing import Any


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

        if not hasattr(con, "register"):
            raise AttributeError(
                "Connection object does not support registering DataFrames"
            )

        con.register("df", df)
        con.sql(f"CREATE OR REPLACE TABLE {table_name} AS SELECT * FROM df")
        result = con.sql(f"SELECT COUNT(*) FROM {table_name}").fetchone()
        row_count = result[0] if result is not None else 0
    return row_count


def fetch_ingest_runs(db_path: str | Path, limit: int = 20) -> list[dict[str, Any]]:
    """
    Return recent ingest_runs records with metrics JSON (if present).
    """
    with duckdb.connect(database=str(db_path), read_only=True) as con:
        exists = con.sql(
            """
            SELECT table_name
            FROM information_schema.tables
            WHERE table_name = 'ingest_runs'
            """
        ).fetchone()
        if exists is None:
            return []

        safe_limit = max(1, int(limit))
        # Handle legacy ingest_runs without metrics_json/duration_seconds
        columns = con.sql("PRAGMA table_info('ingest_runs')").to_df()["name"].tolist()
        select_cols = [
            "ts",
            "source",
            "table_name",
            "row_count",
            "status",
            "note",
        ]
        if "metrics_json" in columns:
            select_cols.append("metrics_json")
        if "duration_seconds" in columns:
            select_cols.append("duration_seconds")

        select_sql = ", ".join(select_cols)
        df = con.sql(
            f"""
            SELECT {select_sql}
            FROM ingest_runs
            ORDER BY ts DESC
            LIMIT {safe_limit}
            """
        ).to_df()
        return df.to_dict("records")


__all__ = ["write_df_to_duckdb", "fetch_ingest_runs"]
