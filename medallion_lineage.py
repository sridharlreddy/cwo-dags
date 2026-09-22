# ~/airflow/dags/medallion_lineage.py
from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator

SERVICE  = "medallion_db"
DATABASE = "default"
SCHEMA   = "medallion-demo"

FQN_PREFIX = f"{SERVICE}.{DATABASE}.{SCHEMA}"

def execute_transform(track, stage):
    print(f"Executing transformation for {track} track: {stage}")

with DAG(
    dag_id="medallion_full_lineage_pipeline",
    start_date=datetime(2026, 1, 1),
    schedule="@daily",
    catchup=False,
    tags=["medallion", "openmetadata", "lineage"],
) as dag:

    for track in ["district", "card", "disp"]:
        bronze_fqn = f'{FQN_PREFIX}."bronze/{track}/{track}_bronze.parquet"'
        silver_fqn = f'{FQN_PREFIX}."silver/{track}/{track}_silver.parquet"'
        gold_fqn   = f'{FQN_PREFIX}."gold/{track}/{track}_gold.parquet"'

        # Note: inlets and outlets MUST be a list [...]
        b2s_task = PythonOperator(
            task_id=f"transform_{track}_bronze_to_silver",
            python_callable=execute_transform,
            op_args=[track, "Bronze to Silver"],
            inlets=[{"tables": [bronze_fqn]}],
            outlets=[{"tables": [silver_fqn]}],
        )

        s2g_task = PythonOperator(
            task_id=f"transform_{track}_silver_to_gold",
            python_callable=execute_transform,
            op_args=[track, "Silver to Gold"],
            inlets=[{"tables": [silver_fqn]}],
            outlets=[{"tables": [gold_fqn]}],
        )

        b2s_task >> s2g_task
