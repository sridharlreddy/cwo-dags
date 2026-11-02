"""Shared UOB NiFi medallion settings used by the Spark jobs and the Airflow DAG."""

from __future__ import annotations

S3_BUCKET = "uob-polaris-bucket"
AWS_REGION = "us-east-1"

# NiFi lands Avro here: <domain>/<system>/<entity>/dt=YYYY-MM/*.avro
# NiFi-rejected records land under <RAW_ROOT>/quarantine/<same layout>.
RAW_ROOT = f"s3a://{S3_BUCKET}/landing_zone/raw"
QUARANTINE_DIR = "quarantine"

# Polaris warehouse uob_aws_catalog, exposed as catalog "uob" in CDE Spark and Trino (cle01).
CATALOG = "uob"

# Demo namespaces, same convention as uob_dataset_{bronze,silver,gold}.
NAMESPACE_PREFIX = "dpp"
BRONZE_NS = f"{NAMESPACE_PREFIX}_bronze"
SILVER_NS = f"{NAMESPACE_PREFIX}_silver"
GOLD_NS = f"{NAMESPACE_PREFIX}_gold"

# Polaris only accepts a namespace at its default location, <catalog base>/<namespace>/.
# CDE otherwise sends s3://<bucket>/iceberg/<namespace>/, which Polaris rejects.
NAMESPACE_ROOT = f"s3://{S3_BUCKET}"

# Reference tables have no raw feed; setup copies them from the existing pipeline.
REFERENCE_SOURCE_NS = "uob_dataset_silver"

# CDE jobs created by cde/deploy.sh and triggered by the DAG.
CDE_CONNECTION_ID = "awc-cde"
# Airflow AWS connection used by the DAG's landing check (list-only access to the raw bucket).
AWS_CONNECTION_ID = "s3_connection"
CDE_JOB_SETUP = "dpp-setup"
CDE_JOB_RAW_TO_BRONZE = "dpp-raw-to-bronze"
CDE_JOB_BRONZE_TO_SILVER = "dpp-bronze-to-silver"
CDE_JOB_SILVER_TO_GOLD = "dpp-silver-to-gold"

# Phase 1 scope. Phase 2 adds "risk" and "wholesale".
DEFAULT_DOMAINS = ["retail"]

PRODUCT_VERSION = "1.0.0"
