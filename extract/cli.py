import argparse
import json
from typing import Any

from extract import load_ba_json_data, load_obdb_csv_data
from extract.config import load_settings
from extract.duckdb_utils import fetch_ingest_runs


def run(action: str, limit: int = 10) -> None:
    if action == "obdb":
        load_obdb_csv_data.main()
    elif action == "ba":
        load_ba_json_data.main()
    elif action == "all":
        load_obdb_csv_data.main()
        load_ba_json_data.main()
    elif action == "ingest-runs":
        settings = load_settings()
        rows: list[dict[str, Any]] = fetch_ingest_runs(settings.db_path, limit=limit)
        if not rows:
            print("No ingest_runs records found.")
            return
        print(json.dumps(rows, indent=2, default=str))
    else:
        raise ValueError(f"Unknown action: {action}")


def main() -> None:
    parser = argparse.ArgumentParser(description="Run OBDB ETL loaders")
    parser.add_argument(
        "action",
        choices=["obdb", "ba", "all", "ingest-runs"],
        help="Which loader to run or inspect ingest history",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=10,
        help="Number of ingest_runs records to show (ingest-runs only)",
    )
    args = parser.parse_args()
    run(args.action, limit=args.limit)


if __name__ == "__main__":
    main()
