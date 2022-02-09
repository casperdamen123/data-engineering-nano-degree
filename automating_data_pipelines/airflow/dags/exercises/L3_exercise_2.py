import datetime
import logging

from airflow import DAG
from airflow.hooks.postgres_hook import PostgresHook

from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.python_operator import PythonOperator


def log_youngest():

    redshift_hook = PostgresHook("redshift")
    records = redshift_hook.get_records("SELECT birthyear FROM younger_riders ORDER BY birthyear DESC LIMIT 1")

    if len(records) > 0 and len(records[0]) > 0:
        logging.info(f"Youngest rider was born in {records[0][0]}")


def log_oldest():
    redshift_hook = PostgresHook("redshift")
    records = redshift_hook.get_records("""
        SELECT birthyear FROM older_riders ORDER BY birthyear ASC LIMIT 1
    """)
    if len(records) > 0 and len(records[0]) > 0:
        logging.info(f"Oldest rider was born in {records[0][0]}")


dag = DAG(
    "lesson3.exercise2",
    start_date=datetime.datetime.utcnow()
)


create_oldest_task = PostgresOperator(
    task_id="create_oldest",
    dag=dag,
    sql="""
        BEGIN;
        DROP TABLE IF EXISTS older_riders;
        CREATE TABLE older_riders AS (
            SELECT * FROM trips WHERE birthyear > 0 AND birthyear <= 1945
        );
        COMMIT;
    """,
    postgres_conn_id="redshift"
)


log_oldest_task = PythonOperator(
    task_id="log_oldest",
    dag=dag,
    python_callable=log_oldest
)


create_young_riders_task = PostgresOperator(
    task_id="create_young_riders",
    dag=dag,
    sql="""
        BEGIN;
        DROP TABLE IF EXISTS younger_riders;
        CREATE TABLE younger_riders AS (SELECT * FROM trips WHERE birthyear > 2000);
        COMMIT;
        """,
    postgres_conn_id="redshift"
)

log_youngest_task = PythonOperator(
    task_id="log_youngest",
    dag=dag,
    python_callable=log_youngest
)

create_bike_rides_task = PostgresOperator(
    task_id="create_bike_rides",
    dag=dag,
    sql="""
        BEGIN;
        DROP TABLE IF EXISTS lifetime_rides;
        CREATE TABLE lifetime_rides AS (
          SELECT bikeid, COUNT(bikeid)
          FROM trips
          GROUP BY bikeid
        );
        COMMIT;
        """,
    postgres_conn_id="redshift"
)

create_city_stations_task = PostgresOperator(
    task_id="create_city_stations",
    dag=dag,
    sql="""
        BEGIN;
        DROP TABLE IF EXISTS city_station_counts;
        CREATE TABLE city_station_counts AS(
          SELECT city, COUNT(city)
          FROM stations
          GROUP BY city
        );
        COMMIT;
        """,
    postgres_conn_id="redshift"
)


create_oldest_task >> log_oldest_task
create_young_riders_task >> log_youngest_task
create_bike_rides_task
create_city_stations_task