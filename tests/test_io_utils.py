import duckdb
import pandas as pd
import pytest

from extract import io_utils


def test_load_csv_from_url_with_retry(monkeypatch):
    calls = {"n": 0}

    def fake_fetch(url: str) -> bytes:
        calls["n"] += 1
        return b"col1,col2\n1,2\n3,4\n"

    monkeypatch.setattr(io_utils, "fetch_bytes", fake_fetch)
    df = io_utils.load_csv_from_url("http://example.com/data.csv")
    assert calls["n"] == 1
    assert list(df.columns) == ["col1", "col2"]
    assert len(df) == 2


def test_ensure_non_empty_raises():
    with pytest.raises(ValueError):
        io_utils.ensure_non_empty(pd.DataFrame(), "ctx")


def test_ensure_required_columns_and_not_all_null():
    df = pd.DataFrame({"a": [1, None], "b": [None, None], "c": [3, 4]})
    # columns present passes
    io_utils.ensure_required_columns(df, ["a", "b"], "ctx")
    # all-null column raises
    with pytest.raises(ValueError):
        io_utils.ensure_not_all_null(df, ["b"], "ctx")


def test_log_ingest_run(tmp_path):
    db_path = tmp_path / "test.duckdb"
    with duckdb.connect(str(db_path), read_only=False) as con:
        io_utils.log_ingest_run(con, "src", "tbl", 5, "success", "note")
        out = con.sql(
            "SELECT source, table_name, row_count, status, note FROM ingest_runs"
        ).fetchone()
    assert out == ("src", "tbl", 5, "success", "note")
