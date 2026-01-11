# obdb-etl

ETL pipeline for Open Brewery DB data, using DuckDB, dbt, and Apache Airflow.

## Overview

This project extracts brewery data from the [Open Brewery DB](https://www.openbrewerydb.org/), loads it into a local DuckDB database, and transforms it using dbt. The pipeline is designed for analytics and data warehousing use cases.

## Features

- **Extract**: Downloads brewery data from a public CSV.
- **Load**: Loads raw data into DuckDB (`data/obdb.duckdb`).
- **Transform**: Uses dbt models to clean and structure the data.
- **Orchestrate**: (Planned) Airflow DAGs for automation.

## Project Structure

```
main.py                  # Entry point (demo/placeholder)
extract/load_obdb_csv_data.py # Extracts and loads raw data into DuckDB
data/obdb.duckdb          # Local DuckDB database
dbt_project/
	brewery_models/
		dbt_project.yml      # dbt project config
		models/              # dbt models (SQL transformations)
		...
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

### 1. Extract and Load Raw Data

```bash
uv run python extract/load_obdb_csv_data.py
```

This downloads the latest breweries data and loads it into `data/obdb.duckdb` as the `raw_obdb_breweries` table.

### 2. Transform with dbt

Navigate to the dbt project directory:

```bash
uv run dbt run --project-dir dbt_project/brewery_models/
```

This will create/refresh models like `stg_breweries` and `dim_breweries` in DuckDB.

### 3. (Optional) Run via Airflow DAG

The DAG lives at `dags/brewery_pipeline_dag.py`.

Start Airflow locally with uv (Airflow 3+):

```bash
# migrate metadata DB (once)
uv run airflow db migrate

# easiest: all-in-one local stack (webserver + scheduler + default user)
uv run airflow standalone

# UI: http://localhost:8080 (default creds printed in stdout; usually admin/admin)
```

Then enable and trigger the `brewery_data_pipeline` DAG in the Airflow UI (http://localhost:8080). It will:

- Run `extract/load_obdb_csv_data.py`
- Run `extract/load_ba_json_data.py`
- Run `dbt run` and `dbt test` against `dbt_project/brewery_models/`

Optional CLI trigger (instead of using the UI):

```bash
uv run airflow dags trigger brewery_data_pipeline
```

## Testing

```bash
uv run dbt test --project-dir dbt_project/brewery_models/
```

## dbt Models

- `stg_breweries`: Stages raw data from DuckDB.
- `dim_breweries`: Cleans, standardizes, and enriches brewery data for analytics.

## Development

- Add new dbt models in `dbt_project/brewery_models/models/`.
- Update or add ETL scripts in `extract/`.
- Add Airflow DAGs for scheduling.

## License

MIT
