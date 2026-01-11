# obdb-etl

ETL pipeline for Open Brewery DB data, using DuckDB, dbt, and Apache Airflow.

## Overview

This project extracts brewery data from the [Open Brewery DB](https://www.openbrewerydb.org/), loads it into a local DuckDB database, and transforms it using dbt. The pipeline is designed for analytics and data warehousing use cases.

## Features

- **Extract**: Downloads public CSV + JSON brewery datasets with retry and validation.
- **Load**: Loads raw data into DuckDB (`data/obdb.duckdb`) with ingest metadata logging.
- **Transform**: Uses dbt models to clean and structure the data.
- **Orchestrate**: Airflow DAG (`brewery_data_pipeline`) with env overrides for schedule/paths.
- **CLI**: Run loaders via `python -m extract.cli {obdb|ba|all}`.

## Project Structure (high level)

```
extract/
  config.py              # env-aware settings (paths, URLs, table names)
  io_utils.py            # retrying fetch, validations, ingest logging
  duckdb_utils.py        # shared DuckDB writer
  load_obdb_csv_data.py  # Open Brewery DB CSV loader
  load_ba_json_data.py   # Brewers Association JSON loader
  cli.py                 # thin CLI to run loaders
dbt_project/brewery_models/  # dbt models & sources
dags/brewery_pipeline_dag.py # Airflow DAG wiring extracts -> dbt
tests/                   # pytest smoke tests
```

## Setup

### Prerequisites

- Python 3.13+
- [DuckDB](https://duckdb.org/)
- [dbt-duckdb](https://docs.getdbt.com/docs/core/connect-data-platform/duckdb)
- [pandas](https://pandas.pydata.org/)
- [Apache Airflow](https://airflow.apache.org/) (optional, for orchestration)

Install dependencies with [uv](https://github.com/astral-sh/uv) (preferred):

```bash
# install uv if you don't have it
curl -LsSf https://astral.sh/uv/install.sh | sh

# create + activate virtualenv
uv venv
source .venv/bin/activate

# install from pyproject + uv.lock
uv sync
```

All Python tools (including `dbt-duckdb`) are installed via `uv sync`; no separate OS-level dbt install is needed.

## Usage

### 1. Extract and Load (direct scripts)

```bash
uv run python extract/load_obdb_csv_data.py
uv run python extract/load_ba_json_data.py
```

### 1b. Extract and Load via CLI (recommended)

```bash
uv run python -m extract.cli obdb   # CSV only
uv run python -m extract.cli ba     # BA JSON only
uv run python -m extract.cli all    # both
```

Environment overrides (optional): `OBDB_DUCKDB_PATH`, `OBDB_CSV_URL`, `BA_JSON_URL`, `BA_JSON_LOCAL_PATH`, `OBDB_TABLE`, `BA_TABLE`.

### 2. Transform with dbt

```bash
uv run dbt run --project-dir dbt_project/brewery_models/
uv run dbt test --project-dir dbt_project/brewery_models/
```

### 3. (Optional) Run via Airflow DAG

DAG: `dags/brewery_pipeline_dag.py`

```bash
uv run airflow standalone
# UI: http://localhost:8080
```

Env overrides: `OBDB_DAG_SCHEDULE` (default hourly), `OBDB_PROJECT_DIR`, `OBDB_DBT_PROJECT_DIR`, `OBDB_VENV_PYTHON`.

Enable/trigger `brewery_data_pipeline` in the UI or:

```bash
uv run airflow dags trigger brewery_data_pipeline
```

## Testing

```bash
uv run pytest
```

Smoke tests cover helper utilities and both extract loaders (with mocks).

## dbt Models

- `stg_breweries`: Stages raw data from DuckDB.
- `dim_breweries`: Cleans, standardizes, and enriches brewery data for analytics.

## Development

- Add new dbt models in `dbt_project/brewery_models/models/`.
- Update ETL scripts in `extract/` and extend validations in `io_utils.py`.
- Add Airflow DAGs for scheduling in `dags/`.

## License

MIT
