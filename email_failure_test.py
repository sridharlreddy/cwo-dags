import datetime
from airflow import DAG
from airflow.operators.bash import BashOperator

# Airflow automatically reads these arguments to trigger built-in SMTP alerts
default_args = {
    "owner": "airflow",
    "start_date": datetime.datetime(2026, 09, 25),
    "email": ["alert_team@cldr.com", "admin@cldr.com"], 
    "email_on_failure": True,   # Send email if a task fails
    "email_on_retry": False,    # Do not send email on every retry attempt
    "retries": 2,
}

with DAG(
    dag_id="automated_email_alerts_dag",
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
) as dag:

    # If this task fails after its retries, Airflow automatically sends a system alert
    critical_process = BashOperator(
        task_id="critical_process",
        bash_command="exit 1",  # Intentionally simulating a failure
    )
