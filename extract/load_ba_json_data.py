import duckdb
import pandas as pd
from extract.config import load_settings


def main():
    """
    Extracts data from a JSON source. It first checks for a local file,
    if not found, it downloads, saves it locally, and then proceeds with analysis.
    """
    settings = load_settings()
    db_path = settings.db_path
    data_url = settings.ba_json_url
    local_json_path = settings.ba_local_json_path
    table_name = settings.ba_table

    print("--- JSON ETL process started ---")

    # EXTRACT: Check for a local file first, otherwise download and save.
    df = None
    if local_json_path.exists():
        try:
            print(f"📄 Local file found. Loading data from {local_json_path}...")
            df = pd.read_json(local_json_path)
            print(f"✅ Extracted {len(df)} rows from local file.")
        except Exception as e:
            print(f"❌ Failed to read or parse local JSON file: {e}")
            return
    else:
        try:
            print(f"📥 Local file not found. Downloading from {data_url}...")
            df = pd.read_json(data_url)
            print(f"✅ Extracted {len(df)} rows from URL.")

            # SAVE: Save the downloaded data to the local path for future runs
            print(f"💾 Saving data to {local_json_path}...")
            local_json_path.parent.mkdir(parents=True, exist_ok=True)
            df.to_json(local_json_path, orient="records", indent=4)
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
    print(f"🦆 Connecting to DuckDB at {db_path}...")
    db_path.parent.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect(database=str(db_path), read_only=False)

    print("📦 Installing and loading SPATIAL extension...")
    con.sql("INSTALL spatial;")
    con.sql("LOAD spatial;")
    print("✅ SPATIAL extension loaded.")

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
