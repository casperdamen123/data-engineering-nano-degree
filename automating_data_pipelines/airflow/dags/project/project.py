from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator

from operators import StageToRedshiftOperator, LoadFactOperator, LoadDimensionOperator, DataQualityOperator
from helpers.sql_statements_project import SqlQueries

default_args = {
    'owner': 'cdamen',
    'depends_on_past': False,
    'start_date': datetime(2018, 11, 1),
    'end_date': datetime(2018, 11, 30),
    'retries': 3,
    "retry_delay": timedelta(minutes=5),
    'email_on_retry': False,
    'catchup': False
}

dag = DAG('end_project_airlf',
          default_args=default_args,
          description='Load and transform data in Redshift with Airflow',
          schedule_interval='@hourly')

start_operator = DummyOperator(task_id='begin_execution', dag=dag)

stage_events_to_redshift = StageToRedshiftOperator(
    task_id='stage_events',
    redshift_conn_id="redshift",
    s3_bucket="udacity-dend",
    s3_key="log_data/{execution_date.year}/{execution_date.month}/{ds}-events.json",
    table="staging_events",
    region="us-west-2",
    json_path="s3://udacity-dend/log_json_path.json",
    aws_credentials_id="aws_credentials",
    dag=dag
)

stage_songs_to_redshift = StageToRedshiftOperator(
    task_id='stage_songs',
    redshift_conn_id="redshift",
    s3_bucket="udacity-dend",
    s3_key="song_data/A/A/A/TRAAAAK128F9318786.json",   #"song_data",
    table="staging_songs",
    region="us-west-2",
    json_path="auto",
    aws_credentials_id="aws_credentials",
    dag=dag
)

load_songplays_table = LoadFactOperator(
    task_id='load_songplays_fact_table',
    redshift_conn_id="redshift",
    target_table="fct_songplays",
    creation_statement=SqlQueries.songplay_table_create,
    insertion_statement=SqlQueries.songplay_table_insert,
    dag=dag
)

load_user_dimension_table = LoadDimensionOperator(
    task_id='load_user_dim_table',
    redshift_conn_id="redshift",
    target_table="dim_users",
    append_mode=False,
    creation_statement=SqlQueries.user_table_create,
    insertion_statement=SqlQueries.user_table_insert,
    dag=dag
)

load_song_dimension_table = LoadDimensionOperator(
    task_id='load_song_dim_table',
    redshift_conn_id="redshift",
    target_table="dim_songs",
    append_mode=False,
    creation_statement=SqlQueries.song_table_create,
    insertion_statement=SqlQueries.song_table_insert,
    dag=dag
)

load_artist_dimension_table = LoadDimensionOperator(
    task_id='load_artist_dim_table',
    redshift_conn_id="redshift",
    target_table="dim_artists",
    append_mode=False,
    creation_statement=SqlQueries.artist_table_create,
    insertion_statement=SqlQueries.artist_table_insert,
    dag=dag
)

load_time_dimension_table = LoadDimensionOperator(
    task_id='load_time_dim_table',
    redshift_conn_id="redshift",
    target_table="dim_time",
    append_mode=False,
    creation_statement=SqlQueries.time_table_create,
    insertion_statement=SqlQueries.time_table_insert,
    dag=dag
)

run_quality_checks = DataQualityOperator(
    task_id='run_data_quality_checks',
    redshift_conn_id="redshift",
    tables=('fct_songplays', 'dim_users', 'dim_songs', 'dim_artists', 'dim_time'),
    dag=dag
)

end_operator = DummyOperator(task_id='stop_execution',  dag=dag)

start_operator >> stage_events_to_redshift
start_operator >> stage_songs_to_redshift
stage_events_to_redshift >> load_songplays_table
stage_songs_to_redshift >> load_songplays_table
load_songplays_table >> load_song_dimension_table >> run_quality_checks >> end_operator
load_songplays_table >> load_user_dimension_table >> run_quality_checks >> end_operator
load_songplays_table >> load_artist_dimension_table >> run_quality_checks >> end_operator
load_songplays_table >> load_time_dimension_table >> run_quality_checks >> end_operator
