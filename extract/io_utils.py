import time
import urllib.error
import urllib.request
from io import BytesIO

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


def profile_df(df: pd.DataFrame) -> None:
    print(f"DataFrame shape: {df.shape}")
    print("Column data types and non-null counts:")
    df.info()
