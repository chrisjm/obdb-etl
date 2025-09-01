import duckdb
import pandas as pd
import os
from pathlib import Path

# --- Configuration ---
# Get the project root directory
PROJECT_ROOT = Path(__file__).resolve().parent.parent

# The local path to our DuckDB database file
DB_PATH = PROJECT_ROOT / "data" / "obdb.duckdb"

# The raw data source (a temporary JSON file on the web)
# https://www.brewersassociation.org/wp-content/themes/ba2019/json-store/breweries/breweries.json?nocache=1756663355733
DATA_URL = "https://www.example.com/wp-files/large-file.json"

# The local cache path for the downloaded JSON data
LOCAL_JSON_PATH = PROJECT_ROOT / "data" / "breweries.json"

# The name of the table we'll create in DuckDB
TABLE_NAME = "raw_ba_json_data"


def main():
    """
    Extracts data from a JSON source. It first checks for a local file,
    if not found, it downloads, saves it locally, and then proceeds with analysis.
    """
    print("--- JSON ETL process started ---")

    # EXTRACT: Check for a local file first, otherwise download and save.
    df = None
    if LOCAL_JSON_PATH.exists():
        try:
            print(f"📄 Local file found. Loading data from {LOCAL_JSON_PATH}...")
            df = pd.read_json(LOCAL_JSON_PATH)
            print(f"✅ Extracted {len(df)} rows from local file.")
        except Exception as e:
            print(f"❌ Failed to read or parse local JSON file: {e}")
            return
    else:
        try:
            print(f"📥 Local file not found. Downloading from {DATA_URL}...")
            df = pd.read_json(DATA_URL)
            print(f"✅ Extracted {len(df)} rows from URL.")

            # SAVE: Save the downloaded data to the local path for future runs
            print(f"💾 Saving data to {LOCAL_JSON_PATH}...")
            LOCAL_JSON_PATH.parent.mkdir(parents=True, exist_ok=True)
            df.to_json(LOCAL_JSON_PATH, orient="records", indent=4)
            print("✅ Data saved locally for future use.")

        except Exception as e:
            print(f"❌ Failed to download or parse JSON data from URL: {e}")
            return

    # Halt if the DataFrame could not be created for any reason
    if df is None:
        print("❌ DataFrame could not be loaded. Halting execution.")
        return

    # --- ANALYSIS & DEBUGGING ---
    print("\n--- 🕵️ Data Analysis ---")
    if not df.empty:
        # Print the first 5 rows to see the structure (the "head")
        print("\nFirst 5 records:")
        print(df.head())

        # Print the DataFrame's dimensions (rows, columns)
        print(f"\nDataFrame shape: {df.shape}")

        # Print a summary of the columns and their data types
        print("\nColumn data types and non-null counts:")
        df.info()
    else:
        print("DataFrame is empty. No data to analyze.")
    print("--- End of Analysis ---\n")

    # --- LOAD (Optional) ---
    # The following code will load the data into DuckDB.
    # It is commented out by default so you can analyze first.
    # To enable it, simply remove the triple quotes (""") before and after.
    print(f"🦆 Connecting to DuckDB at {DB_PATH}...")
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect(database=str(DB_PATH), read_only=False)

    print("📦 Installing and loading SPATIAL extension...")
    con.sql("INSTALL spatial;")
    con.sql("LOAD spatial;")
    print("✅ SPATIAL extension loaded.")

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
