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
            "city": ["x", "y"],
            "state": ["ca", "or"],
            "latitude": [1.0, None],
            "longitude": [None, -120.0],
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
    # Avoid installing spatial by faking DuckDB connection
    sample = pd.DataFrame({"id": [1], "name": ["ba"], "city": ["x"], "state": ["y"]})
    monkeypatch.setattr(load_ba_json_data, "load_json_from_url", lambda url: sample)

    holder = {}

    class FakeResult:
        def __init__(self, n):
            self.n = n

        def fetchone(self):
            return (self.n,)

    class FakeCon:
        def __init__(self):
            holder["con"] = self
            self.last_sql = []
            self.ingest_records = []

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

        def sql(self, query):
            self.last_sql.append(query)
            if query.lower().startswith("select count(*)"):
                return FakeResult(len(sample))
            return self

        def execute(self, query, params=None):
            # log_ingest_run uses execute
            self.ingest_records.append((query, params))
            return self

    monkeypatch.setattr(load_ba_json_data.duckdb, "connect", lambda *a, **k: FakeCon())

    load_ba_json_data.main()

    fake_con = holder["con"]
    assert any("INSTALL spatial" in q for q in fake_con.last_sql)
    assert any("LOAD spatial" in q for q in fake_con.last_sql)
    assert any("CREATE OR REPLACE TABLE" in q for q in fake_con.last_sql)
    assert any("ingest_runs" in (rec[0] or "") for rec in fake_con.ingest_records)
