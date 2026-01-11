import argparse

from extract import load_ba_json_data, load_obdb_csv_data


def run(action: str) -> None:
    if action == "obdb":
        load_obdb_csv_data.main()
    elif action == "ba":
        load_ba_json_data.main()
    elif action == "all":
        load_obdb_csv_data.main()
        load_ba_json_data.main()
    else:
        raise ValueError(f"Unknown action: {action}")


def main() -> None:
    parser = argparse.ArgumentParser(description="Run OBDB ETL loaders")
    parser.add_argument(
        "action",
        choices=["obdb", "ba", "all"],
        help="Which loader to run (obdb CSV, ba JSON, or all)",
    )
    args = parser.parse_args()
    run(args.action)


if __name__ == "__main__":
    main()
