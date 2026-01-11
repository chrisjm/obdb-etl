import duckdb
import pandas as pd
from extract.config import load_settings


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

    # EXTRACT: Read the data from the URL into a Pandas DataFrame
    print(f"📥 Extracting data from {data_url}...")
    df = pd.read_csv(data_url)
    print(f"✅ Extracted {len(df)} rows.")

    # LOAD: Connect to DuckDB and load the data
    print(f"🦆 Connecting to DuckDB at {db_path}...")
    con = duckdb.connect(database=str(db_path), read_only=False)

    print(f"Writing {len(df)} rows to table '{table_name}'...")
    con.sql(f"CREATE OR REPLACE TABLE {table_name} AS SELECT * FROM df")

    # Verify the data was loaded
    result = con.sql(f"SELECT COUNT(*) FROM {table_name}").fetchone()
    row_count = result[0] if result is not None else 0
    print(f"✅ Successfully loaded {row_count} rows into '{table_name}'.")

    con.close()
    print("--- ETL process finished ---")


if __name__ == "__main__":
    main()
