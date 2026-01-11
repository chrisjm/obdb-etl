import os
from dataclasses import dataclass
from pathlib import Path

# Root of the repository (two levels up from this file)
PROJECT_ROOT = Path(__file__).resolve().parent.parent


def _path_env(name: str, default: Path) -> Path:
    value = os.getenv(name)
    return Path(value).expanduser() if value else default


@dataclass(frozen=True)
class Settings:
    db_path: Path
    obdb_csv_url: str
    ba_json_url: str
    ba_local_json_path: Path
    obdb_table: str
    ba_table: str


def load_settings() -> Settings:
    """
    Centralized settings with environment overrides.

    Environment variables:
      - OBDB_DUCKDB_PATH: absolute path to DuckDB file (default: data/obdb.duckdb)
      - OBDB_CSV_URL: override Open Brewery DB CSV source URL
      - BA_JSON_URL: override Brewers Association JSON source URL
      - BA_JSON_LOCAL_PATH: override local cache path for BA JSON
      - OBDB_TABLE: override table name for OBDB CSV
      - BA_TABLE: override table name for BA JSON
    """
    db_path_default = PROJECT_ROOT / "data" / "obdb.duckdb"
    return Settings(
        db_path=_path_env("OBDB_DUCKDB_PATH", db_path_default),
        obdb_csv_url=os.getenv(
            "OBDB_CSV_URL",
            "https://raw.githubusercontent.com/openbrewerydb/openbrewerydb/master/breweries.csv",
        ),
        ba_json_url=os.getenv(
            "BA_JSON_URL",
            "https://www.brewersassociation.org/wp-content/themes/ba2019/json-store/breweries/breweries.json?nocache=1756663355733",
        ),
        ba_local_json_path=_path_env(
            "BA_JSON_LOCAL_PATH", PROJECT_ROOT / "data" / "breweries.json"
        ),
        obdb_table=os.getenv("OBDB_TABLE", "raw_obdb_breweries"),
        ba_table=os.getenv("BA_TABLE", "raw_ba_json_data"),
    )


__all__ = ["Settings", "load_settings", "PROJECT_ROOT"]
