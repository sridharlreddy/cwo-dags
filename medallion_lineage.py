# ~/airflow/dags/medallion_sql_lineage.py
from datetime import datetime
from airflow import DAG
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.operators.python import PythonOperator

def run_python_transform(track, stage):
    print(f"Executing actual transformation logic for {track} ({stage})...")

with DAG(
    dag_id="medallion_sql_lineage_pipeline",
    start_date=datetime(2026, 1, 1),
    schedule="@daily",
    catchup=False,
    tags=["medallion", "openmetadata", "autolineage"],
) as dag:

    for track in ["district", "card", "disp"]:
        # 1. Actual Python processing task
        py_b2s = PythonOperator(
            task_id=f"python_transform_{track}_bronze_to_silver",
            python_callable=run_python_transform,
            op_args=[track, "Bronze to Silver"],
        )

        # 2. Dummy SQL task for OpenMetadata automatic lineage detection
        # (Uses built-in Airflow 'sqlite_default' or any default connection)
        sql_b2s = SQLExecuteQueryOperator(
            task_id=f"transform_{track}_bronze_to_silver",
            conn_id="sqlite_default",
            sql=f"""
                CREATE TABLE medallion_db.default."medallion-demo"."silver/{track}/{track}_silver.parquet" AS
                SELECT * FROM medallion_db.default."medallion-demo"."bronze/{track}/{track}_bronze.parquet";
            """,
        )

        py_b2s >> sql_b2s
