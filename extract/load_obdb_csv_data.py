import duckdb
from extract.config import load_settings
from extract.io_utils import (
    ensure_non_empty,
    ensure_not_all_null,
    ensure_required_columns,
    load_csv_from_url,
    log_ingest_run,
)
from extract.duckdb_utils import write_df_to_duckdb


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

    row_count = 0
    try:
        # EXTRACT: Read the data from the URL into a Pandas DataFrame with retry
        print(f"📥 Extracting data from {data_url}...")
        df = load_csv_from_url(data_url)
        df = ensure_non_empty(df, "Open Brewery DB CSV")
        df = ensure_required_columns(
            df,
            [
                "id",
                "name",
                "brewery_type",
                "address_1",
                "address_2",
                "address_3",
                "city",
                "state_province",
                "postal_code",
                "country",
                "phone",
                "website_url",
                "longitude",
                "latitude",
            ],
            "Open Brewery DB CSV",
        )
        df = ensure_not_all_null(df, ["latitude", "longitude"], "Open Brewery DB CSV")
        print(f"✅ Extracted {len(df)} rows.")

        # LOAD: Connect to DuckDB and load the data
        print(f"🦆 Connecting to DuckDB at {db_path}...")
        row_count = write_df_to_duckdb(df, table_name, db_path, load_spatial=False)
        print(f"✅ Successfully loaded {row_count} rows into '{table_name}'.")
        with duckdb.connect(database=str(db_path), read_only=False) as con:
            log_ingest_run(con, "obdb_csv", table_name, row_count, "success", None)
        print("--- ETL process finished ---")
    except Exception as exc:
        print(f"❌ ETL failed: {exc}")
        try:
            with duckdb.connect(database=str(db_path), read_only=False) as con:
                log_ingest_run(
                    con,
                    "obdb_csv",
                    table_name,
                    row_count,
                    "failed",
                    note=str(exc),
                )
        finally:
            raise


if __name__ == "__main__":
    main()
