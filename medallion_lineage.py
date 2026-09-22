# ~/airflow/dags/medallion_lineage_pipeline.py
from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator

# Exact OpenMetadata Hierarchy
SERVICE  = "medallion_db"
DATABASE = "default"
SCHEMA   = "medallion-demo"

# Base FQN prefix
FQN_PREFIX = f"{SERVICE}.{DATABASE}.{SCHEMA}"

def execute_transform(track, stage):
    print(f"Executing transformation for {track} track: {stage}")

with DAG(
    dag_id="medallion_full_lineage_pipeline",
    start_date=datetime(2026, 1, 1),
    schedule_interval="@daily",
    catchup=False,
    tags=["medallion", "openmetadata", "lineage"],
) as dag:

    for track in ["district", "card", "disp"]:
        # Match exact S3 key paths extracted by OpenMetadata DataLake ingestion
        bronze_fqn = f'{FQN_PREFIX}."bronze/{track}/{track}_bronze.parquet"'
        silver_fqn = f'{FQN_PREFIX}."silver/{track}/{track}_silver.parquet"'
        gold_fqn   = f'{FQN_PREFIX}."gold/{track}/{track}_gold.parquet"'

        # Task 1: Bronze -> Silver
        b2s_task = PythonOperator(
            task_id=f"transform_{track}_bronze_to_silver",
            python_callable=execute_transform,
            op_args=[track, "Bronze to Silver"],
            inlets=[bronze_fqn],
            outlets=[silver_fqn],
        )

        # Task 2: Silver -> Gold
        s2g_task = PythonOperator(
            task_id=f"transform_{track}_silver_to_gold",
            python_callable=execute_transform,
            op_args=[track, "Silver to Gold"],
            inlets=[silver_fqn],
            outlets=[gold_fqn],
        )

        b2s_task >> s2g_task
