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
    schedule="*/5 * * * *",
    catchup=False,
    doc_md="""
  ### Brewery Data Pipeline (v2)
  This DAG extracts brewery data, transforms it with dbt, and runs data quality tests.
  """,
)
def brewery_pipeline():
    """Defines the full brewery data pipeline."""

    project_dir = "{{ dag.folder }}/.."
    dbt_project_dir = f"{project_dir}/dbt_project/brewery_models"
    venv_python = f"{project_dir}/.venv/bin/python"

    @task.bash(cwd=project_dir)
    def load_raw_data() -> str:
        """Runs the Python script to load raw data."""
        return f"{venv_python} ./extract/load_raw_data.py"

    @task.bash(cwd=project_dir)
    def load_raw_json_data() -> str:
        """Runs the Python script to load raw JSON data."""
        return f"{venv_python} ./extract/load_json_data.py"

    @task.bash(cwd=dbt_project_dir)
    def dbt_run() -> str:
        """Runs the dbt models."""
        return f"dbt run"

    @task.bash(cwd=dbt_project_dir)
    def dbt_test() -> str:
        """Runs the dbt tests after the models are built."""
        return f"dbt test"

    load_task = load_raw_data()
    load_json_task = load_raw_json_data()
    run_task = dbt_run()
    test_task = dbt_test()

    [load_csv_task, load_json_task] >> run_task >> test_task


brewery_pipeline()
