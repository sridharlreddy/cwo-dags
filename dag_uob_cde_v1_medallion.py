"""Airflow DAG (Cloudera Data Engineering): mockup_data.xlsx -> bronze -> silver -> gold.

Runs the three existing CDE jobs in order, through the CDE connection `cde_connection`:

    uob-cde-v1-load-bronze  >>  uob-cde-v1-bronze-to-silver  >>  uob-cde-v1-silver-to-gold

The jobs keep their own configuration (resources, Polaris credential, OpenLineage listener);
this DAG only overrides spark.uob.dry_run, from the DAG parameter `dry_run`:

    trigger with {"dry_run": true}    definition reports only, nothing written (default)
    trigger with {"dry_run": false}   real run: rebuilds the uob_cde_v1_* tables, lineage to OpenMetadata

Each task waits for its CDE job run to finish; a failed run fails the task and stops the chain.
Lineage comes from the Spark jobs themselves (OpenLineage listener), not from this DAG.
"""

from datetime import timedelta

import pendulum
from airflow import DAG
from airflow.models.param import Param

from cloudera.cdp.airflow.operators.cde_operator import CDEJobRunOperator

CDE_CONNECTION = "awc-cde-spark35"

JOBS = [
    ("load_bronze", "spark-ol-omd-v1-bronze"),
    ("bronze_to_silver", "spark-ol-omd-v1-silver"),
    ("silver_to_gold", "spark-ol-omd-v1-gold"),
]

DRY_RUN = "{{ 'true' if params.dry_run else 'false' }}"

default_args = {
    "owner": "UOB Data Products",
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}

with DAG(
    dag_id="uob_cde_v1_medallion",
    description="UOB mock-up: workbook -> bronze -> silver -> gold (CDE jobs, lineage to OpenMetadata)",
    start_date=pendulum.datetime(2026, 11, 1, tz="Asia/Singapore"),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    params={"dry_run": Param(True, type="boolean", description="true = definition report only, nothing written")},
    tags=["uob", "uob_cde_v1", "medallion", "openlineage"],
) as dag:
    previous = None
    for task_id, job_name in JOBS:
        task = CDEJobRunOperator(
            task_id=task_id,
            connection_id=CDE_CONNECTION,
            job_name=job_name,
            overrides={"spark": {"conf": {"spark.uob.dry_run": DRY_RUN}}},
            wait=True,
        )
        if previous is not None:
            previous >> task
        previous = task
