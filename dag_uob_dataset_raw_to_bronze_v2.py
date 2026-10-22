"""Airflow DAG: raw CSVs on Ozone -> uob.uob_dataset_bronze, with OpenMetadata lineage.

Task 1 (load_bronze) runs uob_dataset_raw_to_bronze.py through spark-submit, with the
OpenLineage listener off. Task 2 (lineage_op) publishes the DAG and its lineage to
OpenMetadata: each Ozone container (inlet) -> its bronze table (outlet), under the
Airflow pipeline service ETL_airflow.

Same pattern as omd_scripts/dag_raw_storage_to_database.py, but secrets come from
Airflow Variables instead of the file:
    openmetadata_jwt_token        OpenMetadata JWT (bot or personal access token)
    polaris_credential_secret     Polaris client_id:client_secret (principal manish)
Names contain "token" / "secret", so Airflow masks them in task logs.

    airflow variables set openmetadata_jwt_token '<jwt>'
    airflow variables set polaris_credential_secret '<client_id>:<client_secret>'

Both are read only when a task runs, never while the DAG file is parsed: on this
Airflow 3 setup Variable.get() at parse time fails with VARIABLE_NOT_FOUND even
though the Variable exists.

The bronze tables must exist in OpenMetadata (Trino ingestion) for the outlets to
resolve: first run creates them, then ingest Trino, then re-run (or clear lineage_op).
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.standard.operators.bash import BashOperator

try:
    from airflow.sdk import Variable
except ImportError:          # Airflow 2
    from airflow.models import Variable

from airflow_provider_openmetadata.lineage.operator import OpenMetadataLineageOperator
from metadata.generated.schema.entity.data.container import Container
from metadata.generated.schema.entity.data.table import Table
from metadata.generated.schema.entity.services.connections.metadata.openMetadataConnection import (
    OpenMetadataConnection,
)
from metadata.generated.schema.security.client.openMetadataJWTClientConfig import (
    OpenMetadataJWTClientConfig,
)
from metadata.ingestion.source.pipeline.airflow.lineage_parser import OMEntity

OPENMETADATA_HOST = "http://54.87.133.180:8585/api"
PIPELINE_SERVICE = "ETL_airflow"

# Where the uob_dataset folder lives on the Spark/Airflow host.
JOBS_DIR = "/home/ubuntu/spark_jobs/uob_dataset"
SPARK_SUBMIT = "/opt/spark/bin/spark-submit"

STORAGE_SERVICE = "uob-dataset-raw"     # OpenMetadata storage service == bucket container
DATABASE_SERVICE = "uob-dataset-datalake"   # OpenMetadata Trino service (catalog uob)
BRONZE_FQN = f"{DATABASE_SERVICE}.uob.uob_dataset_bronze"

# (domain, entity): container <service>.<bucket>.<domain>/<entity> -> table <bronze>.<entity>
ENTITIES = [
    ("retail", "customer"),
    ("retail", "deposit_account_monthly"),
    ("retail", "card_transaction"),
    ("wholesale", "corporate_customer"),
    ("wholesale", "corporate_relationship"),
    ("wholesale", "trade_finance_transaction"),
    ("risk", "credit_rating_monthly"),
    ("risk", "aml_alert"),
]

def openmetadata_connection(jwt):
    return OpenMetadataConnection(
        hostPort=OPENMETADATA_HOST,
        authProvider="openmetadata",
        securityConfig=OpenMetadataJWTClientConfig(jwtToken=jwt),
    )


class RuntimeTokenLineageOperator(OpenMetadataLineageOperator):
    """OpenMetadataLineageOperator that reads the JWT when the task runs.

    The stock operator needs a full connection (token included) when the DAG is
    parsed; here it gets a placeholder and the real token is swapped in at execute().
    """

    def execute(self, context):
        self.server_config = openmetadata_connection(Variable.get("openmetadata_jwt_token_v2"))
        return super().execute(context)

default_args = {
    "owner": "ubuntu",
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
}

with DAG(
    "uob_dataset_raw_to_bronze_v2",
    default_args=default_args,
    description="Load the raw UOB dataset CSVs from Ozone into Iceberg bronze tables",
    schedule=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["uob_dataset", "bronze"],
) as dag:

    # The command must not end in ".sh", or Airflow tries to load it as a Jinja template file.
    load_bronze = BashOperator(
        task_id="load_bronze",
        bash_command=(f"cd {JOBS_DIR} && "
                      f"bash submit_uob_dataset.sh uob_dataset_raw_to_bronze.py uob_dataset_raw_to_bronze "
                      "--run-id {{ ds_nodash }}"),
        env={
            "POLARIS_OAUTH_CREDENTIAL": "{{ var.value.polaris_credential_secret }}",
            "OL_TRANSPORT": "none",
            # Without this, the job's print() output is buffered and can go missing
            # from the task log when spark-submit fails.
            "PYTHONUNBUFFERED": "1",
            "SPARK_SUBMIT": SPARK_SUBMIT,
        },
        append_env=True,
        inlets=[OMEntity(entity=Container, fqn=f"{STORAGE_SERVICE}.{STORAGE_SERVICE}.{domain}/{entity}", key=entity)
                for domain, entity in ENTITIES],
        outlets=[OMEntity(entity=Table, fqn=f"{BRONZE_FQN}.{entity}", key=entity)
                 for _, entity in ENTITIES],
    )

    lineage_op = RuntimeTokenLineageOperator(
        task_id="lineage_op",
        depends_on_past=False,
        server_config=openmetadata_connection("set-at-runtime"),
        service_name=PIPELINE_SERVICE,
        only_keep_dag_lineage=False,
    )

    load_bronze >> lineage_op
