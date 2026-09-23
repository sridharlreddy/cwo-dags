from datetime import datetime
from airflow import DAG
from airflow.models.baseoperator import BaseOperator


class SQLExecuteQueryOperator(BaseOperator):
    """No-op. SQL is illustrative only; lineage comes from inlets/outlets."""
    template_fields = ("sql",)

    def __init__(self, sql: str, conn_id: str = "default", **kwargs):
        super().__init__(**kwargs)
        self.sql = sql
        self.conn_id = conn_id

    def execute(self, context):
        self.log.info("No-op; lineage declared via inlets/outlets")
        return None


# ⚠️ Copy these from the OpenMetadata UI — see the FQN note below.
SERVICE, DB, SCHEMA = "medallion_db", "default", "medallion-demo"

def tbl(layer: str, track: str) -> str:
    # Name parts containing dots must be quoted in an OM FQN.
    return f'{SERVICE}.{DB}.{SCHEMA}."{layer}/{track}/{track}_{layer}.parquet"'

def om(fqn: str, key: str) -> dict:
    return {"entity": "table", "fqn": fqn, "key": key}


with DAG(
    dag_id="medallion_sql_lineage_pipeline",
    description="Medallion bronze → silver → gold demo with declared lineage",
    start_date=datetime(2026, 1, 1),
    schedule="@daily",
    catchup=False,
    tags=["medallion", "openmetadata", "autolineage"],
) as dag:

    for track in ["district", "card", "disp"]:
        bronze, silver, gold = tbl("bronze", track), tbl("silver", track), tbl("gold", track)

        b2s_key = f"{track}_bronze_to_silver"   # unique key per edge
        s2g_key = f"{track}_silver_to_gold"

        b2s = SQLExecuteQueryOperator(
            task_id=f"transform_{track}_bronze_to_silver",
            sql=f"CREATE TABLE {silver} AS SELECT * FROM {bronze};",
            inlets=[om(bronze, b2s_key)],
            outlets=[om(silver, b2s_key)],
            doc_md=f"Promotes `{track}` from bronze to silver.",
            owner="data-eng",
        )

        s2g = SQLExecuteQueryOperator(
            task_id=f"transform_{track}_silver_to_gold",
            sql=f"CREATE TABLE {gold} AS SELECT * FROM {silver};",
            inlets=[om(silver, s2g_key)],
            outlets=[om(gold, s2g_key)],
            doc_md=f"Promotes `{track}` from silver to gold.",
            owner="data-eng",
        )

        b2s >> s2g
