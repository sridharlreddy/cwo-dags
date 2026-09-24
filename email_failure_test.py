import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator

default_args = {
    "owner": "airflow",
    "start_date": datetime.datetime(2026, 9, 25),
    "email": [
        "alert_team@cldr.com",
        "admin@cldr.com",
    ],
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 2,
}

with DAG(
    dag_id="automated_email_alerts_dag",
    default_args=default_args,
    schedule=None,
    catchup=False,
) as dag:

    critical_process = BashOperator(
        task_id="critical_process",
        bash_command="exit 1",
    )
