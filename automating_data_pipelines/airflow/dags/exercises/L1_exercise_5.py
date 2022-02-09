import datetime
import logging

from airflow import DAG
from airflow.operators.python_operator import PythonOperator

def log_details(*args, **kwargs):

    # Look here for context variables passed in on kwargs:
    # https://airflow.apache.org/docs/apache-airflow/stable/macros-ref.html

    ds = kwargs['ds']
    run_id = kwargs['run_id']
    previous_ds = kwargs['prev_ds']
    next_ds = kwargs['next_ds']

    logging.info(f"Execution date is {ds}")
    logging.info(f"My run id is {run_id}")
    if previous_ds:
        logging.info(f"My previous run was on {previous_ds}")
    if next_ds:
        logging.info(f"My next run will be {next_ds}")

dag = DAG(
    'lesson1.exercise5',
    schedule_interval="@daily",
    start_date=datetime.datetime.now() - datetime.timedelta(days=2)
)

list_task = PythonOperator(
    task_id="log_details",
    python_callable=log_details,
    provide_context=True,
    dag=dag
)