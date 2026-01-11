import os

import pendulum
from airflow.decorators import dag, task

default_args = {
    "owner": "chris@openbrewerydb.org",
    "retries": 2,
    "retry_delay": pendulum.duration(minutes=1),
}


@dag(
    dag_id="brewery_data_pipeline",
    start_date=pendulum.datetime(2025, 8, 29, tz="America/Los_Angeles"),
    schedule=os.getenv(
        "OBDB_DAG_SCHEDULE", "0 * * * *"
    ),  # default hourly; override via env
    catchup=False,
    doc_md="""
  ### Brewery Data Pipeline (v2)
  This DAG extracts brewery data, transforms it with dbt, and runs data quality tests.
  """,
)
def brewery_pipeline():
    """Defines the full brewery data pipeline."""

    project_dir = os.getenv("OBDB_PROJECT_DIR", "{{ dag.folder }}/..")
    dbt_project_dir = os.getenv(
        "OBDB_DBT_PROJECT_DIR", f"{project_dir}/dbt_project/brewery_models"
    )
    venv_python = os.getenv("OBDB_VENV_PYTHON", f"{project_dir}/.venv/bin/python")

    bash_opts = "set -euo pipefail"

    @task.bash(cwd=project_dir)
    def load_obdb_data() -> str:
        """Runs the Python script to load raw data."""
        return f"{bash_opts}\n{venv_python} ./extract/load_obdb_csv_data.py"

    @task.bash(cwd=project_dir)
    def load_ba_data() -> str:
        """Runs the Python script to load raw JSON data."""
        return f"{bash_opts}\n{venv_python} ./extract/load_ba_json_data.py"

    @task.bash(cwd=dbt_project_dir)
    def dbt_run() -> str:
        """Runs the dbt models."""
        return f"{bash_opts}\ndbt run"

    @task.bash(cwd=dbt_project_dir)
    def dbt_test() -> str:
        """Runs the dbt tests after the models are built."""
        return f"{bash_opts}\ndbt test"

    load_obdb_task = load_obdb_data()
    load_ba_task = load_ba_data()
    run_task = dbt_run()
    test_task = dbt_test()

    [load_obdb_task, load_ba_task] >> run_task >> test_task


brewery_pipeline()
