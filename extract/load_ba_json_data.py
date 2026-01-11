import os
import time
import duckdb
import pandas as pd
from extract.config import load_settings
from extract.duckdb_utils import write_df_to_duckdb
from extract.io_utils import (
    ensure_non_empty,
    load_json_from_url,
    log_ingest_run,
)


def main():
    """
    Extracts data from a JSON source. It first checks for a local file,
    if not found, it downloads, saves it locally, and then proceeds with analysis.
    """
    settings = load_settings()
    started = time.monotonic()
    db_path = settings.db_path
    data_url = settings.ba_json_url
    local_json_path = settings.ba_local_json_path
    table_name = settings.ba_table

    print("--- JSON ETL process started ---")

    row_count = 0
    try:
        # EXTRACT: Check for a local file first, otherwise download and save.
        df = None
        if local_json_path.exists():
            try:
                print(f"📄 Local file found. Loading data from {local_json_path}...")
                df = pd.read_json(local_json_path)
                print(f"✅ Extracted {len(df)} rows from local file.")
            except Exception as e:
                raise RuntimeError(
                    f"Failed to read or parse local JSON file: {e}"
                ) from e
        else:
            print(f"📥 Local file not found. Downloading from {data_url}...")
            df = load_json_from_url(data_url)
            print(f"✅ Extracted {len(df)} rows from URL.")

            # SAVE: Save the downloaded data to the local path for future runs
            print(f"💾 Saving data to {local_json_path}...")
            local_json_path.parent.mkdir(parents=True, exist_ok=True)
            df.to_json(local_json_path, orient="records", indent=4)
            print("✅ Data saved locally for future use.")

        # Halt if the DataFrame could not be created for any reason
        if df is None:
            raise RuntimeError("DataFrame could not be loaded.")
        df = ensure_non_empty(df, "Brewers Association JSON")

        # --- ANALYSIS & DEBUGGING ---
        print("\n--- 🕵️ Data Analysis ---")
        if not df.empty:
            print("\nFirst 5 records:")
            print(df.head())

            print(f"\nDataFrame shape: {df.shape}")
            print("\nColumn data types and non-null counts:")
            df.info()
        else:
            print("DataFrame is empty. No data to analyze.")
        print("--- End of Analysis ---\n")

        print(f"🦆 Connecting to DuckDB at {db_path}...")
        db_path.parent.mkdir(parents=True, exist_ok=True)

        enable_spatial = os.getenv("OBDB_ENABLE_SPATIAL", "1") != "0"
        try:
            row_count = write_df_to_duckdb(
                df, table_name, db_path, load_spatial=enable_spatial
            )
        except Exception as exc:
            if enable_spatial:
                print(
                    f"⚠️ Spatial extension failed ({exc}); retrying without spatial support."
                )
                row_count = write_df_to_duckdb(
                    df, table_name, db_path, load_spatial=False
                )
            else:
                raise

        duration = time.monotonic() - started
        print(f"✅ Successfully loaded {row_count} rows into '{table_name}'.")
        with duckdb.connect(database=str(db_path), read_only=False) as con:
            log_ingest_run(
                con,
                "ba_json",
                table_name,
                row_count,
                "success",
                None,
                duration_seconds=duration,
            )
        print("--- ETL process finished ---")
    except Exception as exc:
        print(f"❌ ETL failed: {exc}")
        try:
            duration = time.monotonic() - started
            with duckdb.connect(database=str(db_path), read_only=False) as con:
                log_ingest_run(
                    con,
                    "ba_json",
                    table_name,
                    row_count,
                    "failed",
                    note=str(exc),
                    duration_seconds=duration,
                )
        finally:
            raise


if __name__ == "__main__":
    main()
