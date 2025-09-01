import duckdb
import pandas as pd
import os
from pathlib import Path

# --- Configuration ---
# Get the project root directory (the parent of the 'src' directory)
PROJECT_ROOT = Path(__file__).resolve().parent.parent

# The local path to our DuckDB database file, relative to the project root
DB_PATH = PROJECT_ROOT / "data" / "obdb.duckdb"

# The raw data source (a CSV file on the web)
DATA_URL = (
    "https://raw.githubusercontent.com/openbrewerydb/openbrewerydb/master/breweries.csv"
)

# The name of the table we'll create in DuckDB
TABLE_NAME = "raw_obdb_breweries"


def main():
    """
    Extracts data from a URL, loads it into a Pandas DataFrame,
    and then loads that data into a DuckDB database.
    """
    print("---  ETL process started ---")

    # Ensure the target directory exists
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)

    # EXTRACT: Read the data from the URL into a Pandas DataFrame
    print(f"📥 Extracting data from {DATA_URL}...")
    df = pd.read_csv(DATA_URL)
    print(f"✅ Extracted {len(df)} rows.")

    # LOAD: Connect to DuckDB and load the data
    print(f"🦆 Connecting to DuckDB at {DB_PATH}...")
    con = duckdb.connect(database=str(DB_PATH), read_only=False)

    print(f"Writing {len(df)} rows to table '{TABLE_NAME}'...")
    con.sql(f"CREATE OR REPLACE TABLE {TABLE_NAME} AS SELECT * FROM df")

    # Verify the data was loaded
    result = con.sql(f"SELECT COUNT(*) FROM {TABLE_NAME}").fetchone()
    row_count = result[0] if result is not None else 0
    print(f"✅ Successfully loaded {row_count} rows into '{TABLE_NAME}'.")

    con.close()
    print("--- ETL process finished ---")


if __name__ == "__main__":
    main()
