import time
import urllib.error
import urllib.request
from datetime import datetime, timezone
from io import BytesIO
from typing import Iterable, Sequence

import pandas as pd

DEFAULT_TIMEOUT = 15


def fetch_bytes(
    url: str, retries: int = 3, backoff: float = 2.0, timeout: int = DEFAULT_TIMEOUT
) -> bytes:
    """
    Fetch content from a URL with simple retry + backoff.
    Avoids adding extra dependencies (uses urllib).
    """
    attempt = 0
    while True:
        try:
            req = urllib.request.Request(url, headers={"User-Agent": "obdb-etl/1.0"})
            with urllib.request.urlopen(req, timeout=timeout) as resp:  # type: ignore[arg-type]
                return resp.read()
        except (urllib.error.URLError, TimeoutError):
            attempt += 1
            if attempt > retries:
                raise
            sleep_for = backoff**attempt
            time.sleep(sleep_for)
            continue


def load_csv_from_url(url: str) -> pd.DataFrame:
    raw = fetch_bytes(url)
    return pd.read_csv(BytesIO(raw))


def load_json_from_url(url: str) -> pd.DataFrame:
    raw = fetch_bytes(url)
    return pd.read_json(BytesIO(raw))


def ensure_non_empty(df: pd.DataFrame, context: str) -> pd.DataFrame:
    if df.empty:
        raise ValueError(f"{context}: no rows returned")
    return df


def ensure_required_columns(
    df: pd.DataFrame, columns: Sequence[str], context: str
) -> pd.DataFrame:
    missing = [c for c in columns if c not in df.columns]
    if missing:
        raise ValueError(f"{context}: missing required columns {missing}")
    return df


def ensure_not_all_null(
    df: pd.DataFrame, columns: Iterable[str], context: str
) -> pd.DataFrame:
    cols = list(columns)
    null_only = [c for c in cols if c in df.columns and df[c].isna().all()]
    if null_only:
        raise ValueError(f"{context}: columns entirely null {null_only}")
    return df


def profile_df(df: pd.DataFrame) -> None:
    print(f"DataFrame shape: {df.shape}")
    print("Column data types and non-null counts:")
    df.info()


def log_ingest_run(
    con,
    source: str,
    table_name: str,
    row_count: int,
    status: str,
    note: str | None = None,
    duration_seconds: float | None = None,
) -> None:
    con.execute(
        """
        CREATE TABLE IF NOT EXISTS ingest_runs (
            ts TIMESTAMP WITH TIME ZONE,
            source STRING,
            table_name STRING,
            row_count BIGINT,
            status STRING,
            note STRING,
            duration_seconds DOUBLE
        )
        """
    )
    con.execute(
        "INSERT INTO ingest_runs VALUES (?, ?, ?, ?, ?, ?, ?)",
        (
            datetime.now(timezone.utc),
            source,
            table_name,
            row_count,
            status,
            note,
            duration_seconds,
        ),
    )
