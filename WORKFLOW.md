# obdb-etl: Workflow & Environment Reference

## Overview

- **Goal**: Extract brewery data (Open Brewery DB CSV + Brewers Association JSON), land in DuckDB, then transform/testing via dbt, orchestrated by an Airflow DAG.
- **Core components**: `extract/` Python loaders, DuckDB at `data/obdb.duckdb`, dbt project at `dbt_project/brewery_models/`, Airflow DAG `dags/brewery_pipeline_dag.py`.

## Orchestration (Airflow DAG)

- DAG: `brewery_data_pipeline` (`dags/brewery_pipeline_dag.py`).
- Schedule: every 5 minutes; catchup disabled; retries: 2 with 1-minute delay.
- Tasks (bash operators, executed with project `.venv/bin/python`):
  1. `load_obdb_data`: runs `extract/load_obdb_csv_data.py`.
  2. `load_ba_data`: runs `extract/load_ba_json_data.py`.
  3. `dbt_run`: `dbt run` in `dbt_project/brewery_models`.
  4. `dbt_test`: `dbt test` in `dbt_project/brewery_models`.
- Dependencies: both extracts → dbt run → dbt test.

## Extract & Load

- `extract/load_obdb_csv_data.py`
  - Source: `https://raw.githubusercontent.com/openbrewerydb/openbrewerydb/master/breweries.csv`
  - Target DB: `data/obdb.duckdb`
  - Table: `raw_obdb_breweries`
  - Behavior: downloads CSV with pandas, writes/replaces DuckDB table.
- `extract/load_ba_json_data.py`
  - Source: BA JSON URL (cached locally at `data/breweries.json` if absent).
  - Target DB: `data/obdb.duckdb`
  - Table: `raw_ba_json_data`
  - Behavior: load JSON (local cache preferred), prints quick profile, installs/loads DuckDB `spatial` extension, writes/replaces table.

## Transform & Test (dbt)

- Project: `dbt_project/brewery_models` (`dbt_project.yml` profile name: `brewery_models`).
- Sources (`models/sources.yml`): `raw_obdb_breweries`, `raw_ba_json_data` (schema `main`).
- Models (non-exhaustive):
  - `stg_breweries.sql`: cleans OBDB CSV data (bounds latitude/longitude).
  - `stg_ba_breweries.sql`: filters BA JSON to craft breweries, normalizes fields.
  - `map_brewery_ids.sql`: joins OBDB to BA using fuzzy logic (see file for logic).
  - `dim_breweries.sql` and `dim_breweries_combined.sql`: final dimensionalized outputs.
- Tests (`models/dims.yml`): uniqueness/not-null and accepted values on key columns.

## Data Storage

- DuckDB file: `data/obdb.duckdb` (created automatically by loaders).
- Tables created:
  - `raw_obdb_breweries` (CSV)
  - `raw_ba_json_data` (JSON)
  - dbt-generated staging/dim tables under schema `main`.

## Environment Variables & Config

- **Repository code does not read any environment variables directly.**
- Airflow: uses its own env-driven config; no project-specific env keys set here.
- dbt profile: `profile: brewery_models` requires a DuckDB profile in `~/.dbt/profiles.yml` (not committed). Example:
  ```yaml
  brewery_models:
    target: dev
    outputs:
      dev:
        type: duckdb
        path: /absolute/path/to/data/obdb.duckdb
  ```
- Virtualenv: DAG expects `.venv/bin/python` at project root (created via `uv venv` + `uv sync`).

## How to Run Locally (happy path)

1. Create venv + install deps: `uv venv && source .venv/bin/activate && uv sync`.
2. Load raw data (optional outside Airflow):
   - `uv run python extract/load_obdb_csv_data.py`
   - `uv run python extract/load_ba_json_data.py`
3. dbt: `uv run dbt run --project-dir dbt_project/brewery_models && uv run dbt test --project-dir dbt_project/brewery_models`.
4. Airflow (optional orchestration): `uv run airflow standalone`, then enable/trigger `brewery_data_pipeline`.

## Gaps / Observations

- No committed `profiles.yml`; users must supply their DuckDB path.
- No secrets or API keys required; data sources are public.
- DAG assumes presence of `.venv` and project path resolution using `dag.folder`.
