import duckdb
from extract.config import load_settings
from extract.io_utils import ensure_non_empty, load_csv_from_url


def main():
    """
    Extracts data from a URL, loads it into a Pandas DataFrame,
    and then loads that data into a DuckDB database.
    """
    settings = load_settings()
    db_path = settings.db_path
    data_url = settings.obdb_csv_url
    table_name = settings.obdb_table

    print("---  ETL process started ---")

    # Ensure the target directory exists
    db_path.parent.mkdir(parents=True, exist_ok=True)

    # EXTRACT: Read the data from the URL into a Pandas DataFrame with retry
    print(f"📥 Extracting data from {data_url}...")
    df = load_csv_from_url(data_url)
    df = ensure_non_empty(df, "Open Brewery DB CSV")
    print(f"✅ Extracted {len(df)} rows.")

    # LOAD: Connect to DuckDB and load the data
    print(f"🦆 Connecting to DuckDB at {db_path}...")
    with duckdb.connect(database=str(db_path), read_only=False) as con:
        print(f"Writing {len(df)} rows to table '{table_name}'...")
        con.sql(f"CREATE OR REPLACE TABLE {table_name} AS SELECT * FROM df")

        # Verify the data was loaded
        result = con.sql(f"SELECT COUNT(*) FROM {table_name}").fetchone()
        row_count = result[0] if result is not None else 0
        print(f"✅ Successfully loaded {row_count} rows into '{table_name}'.")
    print("--- ETL process finished ---")


if __name__ == "__main__":
    main()
