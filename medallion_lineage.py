# medallion_sql_lineage.py
from datetime import datetime
from airflow import DAG
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator

with DAG(
    dag_id="medallion_sql_lineage_pipeline",
    start_date=datetime(2026, 1, 1),
    schedule="@daily",
    catchup=False,
    tags=["medallion", "openmetadata", "autolineage"],
) as dag:

    for track in ["district", "card", "disp"]:
        bronze_tbl = f'medallion_db.default."medallion-demo"."bronze/{track}/{track}_bronze.parquet"'
        silver_tbl = f'medallion_db.default."medallion-demo"."silver/{track}/{track}_silver.parquet"'
        gold_tbl   = f'medallion_db.default."medallion-demo"."gold/{track}/{track}_gold.parquet"'

        # 1. Initialize dummy tables in SQLite
        init_sqlite = SQLExecuteQueryOperator(
            task_id=f"init_sqlite_{track}",
            conn_id="sqlite_default",
            sql=f"""
                CREATE TABLE IF NOT EXISTS {bronze_tbl} (id INT);
                CREATE TABLE IF NOT EXISTS {silver_tbl} (id INT);
                CREATE TABLE IF NOT EXISTS {gold_tbl} (id INT);
            """,
        )

        # 2. Bronze -> Silver (Parsed automatically by OpenMetadata sqlglot)
        b2s_task = SQLExecuteQueryOperator(
            task_id=f"transform_{track}_bronze_to_silver",
            conn_id="sqlite_default",
            sql=f"""
                INSERT INTO {silver_tbl} 
                SELECT * FROM {bronze_tbl};
            """,
        )

        # 3. Silver -> Gold (Parsed automatically by OpenMetadata sqlglot)
        s2g_task = SQLExecuteQueryOperator(
            task_id=f"transform_{track}_silver_to_gold",
            conn_id="sqlite_default",
            sql=f"""
                INSERT INTO {gold_tbl} 
                SELECT * FROM {silver_tbl};
            """,
        )

        init_sqlite >> b2s_task >> s2g_task
