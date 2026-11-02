"""dpp: monthly UOB medallion on CDE. Check the NiFi landing, then raw -> bronze -> silver -> gold.

Trigger conf (all optional):
  batch_month  YYYY-MM NiFi dt= partition to process (default: previous UTC month)
  mode         "month" (default), or "backfill" to rebuild every layer from all landed months
  raw_root     NiFi landing root (default: params.raw_root)

NiFi owns landing_zone/raw; this DAG only lists it. The CDE jobs are created by cde/deploy.sh.
"""

from __future__ import annotations

import logging
import re
from collections.abc import Mapping
from datetime import datetime, timedelta, timezone
from typing import Any, List, Optional, Tuple

import pendulum
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import DAG, Param
from airflow.sdk.exceptions import AirflowException
from cloudera.airflow.providers.operators.cde import CdeRunJobOperator

from dpp_pipeline_settings import (
    AWS_CONNECTION_ID,
    AWS_REGION,
    CDE_CONNECTION_ID,
    CDE_JOB_BRONZE_TO_SILVER,
    CDE_JOB_RAW_TO_BRONZE,
    CDE_JOB_SILVER_TO_GOLD,
    DEFAULT_DOMAINS,
    RAW_ROOT,
)

LOGGER = logging.getLogger(__name__)

# Phase 1 retail feeds under the raw root; same as raw_path in spark/entities.py.
RETAIL_FEEDS = {
    "customer": "retail/core_banking/customer",
    "card_transaction": "retail/cards/card_transaction",
    "deposit_account_monthly": "retail/core_banking/deposit_account_monthly",
}
MODES = ("month", "backfill")
MONTH_PATTERN = re.compile(r"^\d{4}-(0[1-9]|1[0-2])$")

# The CDE jobs take one argument string (split by uob_common.parse_args).
LANDING = "ti.xcom_pull(task_ids='check_raw_landing')"
SCOPE_ARGS = "{{ " + LANDING + "['scope_args'] }} --domains {{ params.domains }}"
RAW_TO_BRONZE_ARGS = SCOPE_ARGS + " --raw-root {{ " + LANDING + "['raw_root'] }}"
BRONZE_TO_SILVER_ARGS = SCOPE_ARGS + " --max-reject-rate {{ params.max_reject_rate }}"
SILVER_TO_GOLD_ARGS = SCOPE_ARGS


def previous_month(now: Optional[datetime] = None) -> str:
    today = (now or datetime.now(timezone.utc)).date()
    year, month = (today.year, today.month - 1) if today.month > 1 else (today.year - 1, 12)
    return f"{year:04d}-{month:02d}"


def resolve_run(conf: Mapping[str, Any], default_raw_root: str, now: Optional[datetime] = None) -> dict:
    """Validate trigger conf into the run scope passed to every CDE job."""
    mode = conf.get("mode") or "month"
    if mode not in MODES:
        raise AirflowException(f"mode must be one of {MODES}")
    batch_month = conf.get("batch_month")
    if mode == "backfill":
        if batch_month:
            raise AirflowException("batch_month cannot be combined with mode=backfill")
    else:
        batch_month = batch_month or previous_month(now)
        if not MONTH_PATTERN.match(str(batch_month)):
            raise AirflowException("batch_month must be YYYY-MM")
    raw_root = str(conf.get("raw_root") or default_raw_root).rstrip("/")
    if not re.match(r"^s3a?://[^/]+/.+", raw_root):
        raise AirflowException("raw_root must be an s3a:// or s3:// URI with a prefix")
    return {
        "mode": mode,
        "batch_month": batch_month if mode == "month" else None,
        "raw_root": raw_root,
        "scope_args": "--all" if mode == "backfill" else f"--batch-month {batch_month}",
    }


def split_s3_uri(uri: str) -> Tuple[str, str]:
    bucket, _, prefix = uri.split("://", 1)[1].partition("/")
    return bucket, prefix.strip("/")


def landed_feeds(client: Any, raw_root: str, batch_month: Optional[str]) -> List[str]:
    """Feeds with at least one object for the month (or for any month when batch_month is None)."""
    bucket, prefix = split_s3_uri(raw_root)
    landed = []
    for entity, path in RETAIL_FEEDS.items():
        key_prefix = f"{prefix}/{path}/dt=" + (f"{batch_month}/" if batch_month else "")
        response = client.list_objects_v2(Bucket=bucket, Prefix=key_prefix, MaxKeys=1)
        if response.get("KeyCount", 0) > 0:
            landed.append(entity)
    return landed


def _s3_client() -> Any:
    from airflow.providers.amazon.aws.hooks.s3 import S3Hook

    return S3Hook(aws_conn_id=AWS_CONNECTION_ID, region_name=AWS_REGION).get_conn()


def check_raw_landing(**context: Any) -> dict:
    dag_run = context.get("dag_run")
    conf = (dag_run.conf if dag_run is not None else None) or {}
    run = resolve_run(conf, context["params"]["raw_root"])
    try:
        landed = landed_feeds(_s3_client(), run["raw_root"], run["batch_month"])
    except AirflowException:
        raise
    except Exception:
        # Do not chain the boto error: it can carry request details.
        raise AirflowException("Unable to list the NiFi landing zone") from None
    scope = run["batch_month"] or "all months"
    missing = sorted(set(RETAIL_FEEDS) - set(landed))
    if not landed:
        raise AirflowException(f"No NiFi raw feed has landed for {scope} under {run['raw_root']}")
    if missing:
        # raw_to_bronze records feed_present=0 for these in dq_results; the run continues.
        LOGGER.warning("Feeds not landed for %s: %s", scope, ", ".join(missing))
    LOGGER.info("Landed for %s: %s", scope, ", ".join(landed))
    return {**run, "landed": landed}


def cde_job(task_id: str, job_name: str, args: str) -> CdeRunJobOperator:
    return CdeRunJobOperator(
        task_id=task_id,
        job_name=job_name,
        connection_id=CDE_CONNECTION_ID,
        wait=True,
        # CdeHook sends this as the run's "overrides"; job args live under "spark".
        overrides={"spark": {"args": [args]}},
    )


with DAG(
    dag_id="dpp_medallion",
    description="NiFi raw Avro -> CDE Spark bronze/silver/gold in Polaris catalog uob",
    start_date=pendulum.datetime(2026, 10, 1, tz="UTC"),
    # NiFi lands each month during the following month; run on the 3rd for the previous month.
    schedule="0 6 3 * *",
    catchup=False,
    max_active_runs=1,
    dagrun_timeout=timedelta(hours=3),
    is_paused_upon_creation=True,
    params={
        "raw_root": Param(default=RAW_ROOT, type="string"),
        "domains": Param(default=",".join(DEFAULT_DOMAINS), type="string"),
        "max_reject_rate": Param(default=0.05, type="number", minimum=0, maximum=1),
    },
    tags=["dpp", "uob", "cde", "spark", "iceberg", "polaris", "trino"],
) as dag:
    check_landing = PythonOperator(
        task_id="check_raw_landing",
        python_callable=check_raw_landing,
        retries=2,
        retry_delay=timedelta(minutes=2),
    )
    raw_to_bronze = cde_job("raw_to_bronze", CDE_JOB_RAW_TO_BRONZE, RAW_TO_BRONZE_ARGS)
    bronze_to_silver = cde_job("bronze_to_silver", CDE_JOB_BRONZE_TO_SILVER, BRONZE_TO_SILVER_ARGS)
    silver_to_gold = cde_job("silver_to_gold", CDE_JOB_SILVER_TO_GOLD, SILVER_TO_GOLD_ARGS)

    check_landing >> raw_to_bronze >> bronze_to_silver >> silver_to_gold
