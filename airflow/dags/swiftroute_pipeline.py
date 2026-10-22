
from datetime import datetime

from airflow.sdk import DAG
from airflow.providers.databricks.operators.databricks import (
    DatabricksRunNowOperator,
)
from airflow.providers.standard.operators.bash import BashOperator


with DAG(
    dag_id="swiftroute_pipeline",
    description="Orchestrate SwiftRoute Bronze ingestion and dbt transformations.",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    tags=["swiftroute", "databricks", "dbt"],
) as dag:

    # Step 1: Run the existing Databricks job to ingest data into Bronze.
    ingest_bronze = DatabricksRunNowOperator(
        task_id="ingest_bronze",
        databricks_conn_id="databricks_default",
        job_id=276784196523287,
        wait_for_termination=True,
        polling_period_seconds=30,
        deferrable=True,
    )

    # Step 2: Build and test the dbt project after Bronze ingestion succeeds.
    dbt_build = BashOperator(
        task_id="dbt_build",
        bash_command=(
            "dbt build "
            "--project-dir /opt/airflow/swiftroute_project "
            "--profiles-dir /opt/airflow/swiftroute_project "
            "--target dev"
        ),
        cwd="/opt/airflow/swiftroute_project",
    )

    # Enforce the execution order.
    ingest_bronze >> dbt_build