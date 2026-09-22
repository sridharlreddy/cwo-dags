from datetime import datetime
from airflow import DAG
from airflow.models.baseoperator import BaseOperator

# Custom operator that holds a SQL string for OpenMetadata lineage parsing
# without making any database connections in Airflow.
class DummySQLOperator(BaseOperator):
    template_fields = ('sql',)

    def __init__(self, sql: str, **kwargs):
        super().__init__(**kwargs)
        self.sql = sql

    def execute(self, context):
        self.log.info("Executing Dummy SQL operator for OpenMetadata lineage extraction...")
        return None

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

        # Bronze -> Silver
        b2s_task = DummySQLOperator(
            task_id=f"transform_{track}_bronze_to_silver",
            sql=f"""
                CREATE TABLE {silver_tbl} AS 
                SELECT * FROM {bronze_tbl};
            """,
        )

        # Silver -> Gold
        s2g_task = DummySQLOperator(
            task_id=f"transform_{track}_silver_to_gold",
            sql=f"""
                CREATE TABLE {gold_tbl} AS 
                SELECT * FROM {silver_tbl};
            """,
        )

        b2s_task >> s2g_task
