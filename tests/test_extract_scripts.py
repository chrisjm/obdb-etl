import duckdb
import pandas as pd
from extract import load_ba_json_data, load_obdb_csv_data


def test_load_obdb_csv_data_smoke(monkeypatch, tmp_path):
    db_path = tmp_path / "obdb.duckdb"
    monkeypatch.setenv("OBDB_DUCKDB_PATH", str(db_path))

    sample = pd.DataFrame(
        {
            "id": [1, 2],
            "name": ["a", "b"],
            "brewery_type": ["micro", "regional"],
            "address_1": ["addr1", "addr2"],
            "address_2": [None, None],
            "address_3": [None, None],
            "city": ["x", "y"],
            "state_province": ["ca", "or"],
            "postal_code": ["11111", "22222"],
            "country": ["us", "us"],
            "phone": [None, None],
            "website_url": [None, None],
            "longitude": [None, -120.0],
            "latitude": [1.0, None],
        }
    )
    monkeypatch.setattr(load_obdb_csv_data, "load_csv_from_url", lambda url: sample)

    load_obdb_csv_data.main()

    with duckdb.connect(str(db_path), read_only=True) as con:
        count = con.sql("SELECT COUNT(*) FROM raw_obdb_breweries").fetchone()[0]
        assert count == 2
        ingest = con.sql(
            "SELECT status FROM ingest_runs WHERE source='obdb_csv'"
        ).fetchone()
        assert ingest[0] == "success"


def test_load_ba_json_data_smoke(monkeypatch, tmp_path):
    # Avoid installing spatial and real DuckDB by faking IO + writer + connection
    sample = pd.DataFrame({"id": [1], "name": ["ba"], "city": ["x"], "state": ["y"]})
    monkeypatch.setattr(load_ba_json_data, "load_json_from_url", lambda url: sample)
    monkeypatch.setenv("OBDB_DUCKDB_PATH", str(tmp_path / "obdb.duckdb"))
    monkeypatch.setenv("BA_JSON_LOCAL_PATH", str(tmp_path / "ba.json"))
    monkeypatch.setenv("OBDB_ENABLE_SPATIAL", "0")
    sample.to_json(tmp_path / "ba.json", orient="records", indent=2)

    class FakeResult:
        def __init__(self, n):
            self.n = n

        def fetchone(self):
            return (self.n,)

    class FakeCon:
        def __init__(self):
            self.ingest_records = []

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

        def execute(self, query, params=None):
            # log_ingest_run uses execute
            self.ingest_records.append((query, params))
            return self

    fake_con = FakeCon()
    holder = {}

    def fake_write_df(df, table_name, db_path, load_spatial=False):
        holder["write_called"] = True
        holder["load_spatial"] = load_spatial
        holder["table_name"] = table_name
        holder["row_count"] = len(df)
        return len(df)

    monkeypatch.setattr(load_ba_json_data.duckdb, "connect", lambda *a, **k: fake_con)
    monkeypatch.setattr(load_ba_json_data, "write_df_to_duckdb", fake_write_df)

    load_ba_json_data.main()

    assert holder.get("write_called") is True
    assert holder.get("load_spatial") is False
    assert holder.get("table_name") == "raw_ba_json_data"
    assert holder.get("row_count") == 1
    assert fake_con.ingest_records  # ingest logging was attempted
