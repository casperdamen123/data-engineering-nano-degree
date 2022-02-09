import datetime
import logging

from airflow import DAG
from airflow.operators.python_operator import PythonOperator

def hello_world():
    logging.info("Hello World")


dag = DAG(
    'lesson1.exercise2',
    start_date=datetime.datetime.now() - datetime.timedelta(days=1),
    schedule_interval="@daily"
)

greet_task = PythonOperator(
    task_id="greet_world",
    python_callable=hello_world,
    dag=dag
)

greet_task