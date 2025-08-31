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
extract/load_raw_data.py # Extracts and loads raw data into DuckDB
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

Install dependencies:

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

## Usage

### 1. Extract and Load Raw Data

```bash
python extract/load_raw_data.py
```

This downloads the latest breweries data and loads it into `data/obdb.duckdb` as the `raw_breweries` table.

### 2. Transform with dbt

Navigate to the dbt project directory:

```bash
dbt run --project-dir dbt_project/brewery_models/
```

This will create/refresh models like `stg_breweries` and `dim_breweries` in DuckDB.

## Testing

```bash
dbt test --project-dir dbt_project/brewery_models/
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
