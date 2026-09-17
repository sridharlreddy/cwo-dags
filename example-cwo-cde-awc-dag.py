from datetime import datetime
from airflow import DAG
from cloudera.airflow.providers.operators.cde import CdeRunJobOperator

with DAG(
    dag_id="spark_cde",
    schedule=None,
    start_date=datetime(2026, 1, 1),
    catchup=False
) as dag:
    CdeRunJobOperator(
        task_id=f"submit_job",
        job_name="example-cwo-cde-awc",
        connection_id="awc-cde"
    )
