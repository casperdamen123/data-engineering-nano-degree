import configparser
from datetime import datetime, timedelta
from airflow.operators.dummy_operator import DummyOperator
from airflow import DAG
from helpers.create_redshift_table_query import create_imdb_table
from airflow.providers.amazon.aws.transfers.s3_to_redshift import S3ToRedshiftOperator
from operators import (
    CreateRedshiftClusterConnectionOperator,
    CreateRedshiftTableOperator,
    DataValidationOperator
)

# Set configuration variables
config = configparser.ConfigParser(allow_no_value=True)
config.read_file(open('airflow.cfg'))

# AWS
AWS_CREDENTIAL_ID = config.get('AWS', 'CREDENTIALS_ID')

# Redshift
REDSHIFT_CONN_ID = config.get('REDSHIFT', 'CONNECTION_ID')

# S3
S3_BUCKET = config.get("S3", "BUCKET")
S3_PROJECT = config.get("S3", "PROJECT")
S3_LOG_KEY = config.get("S3", "LOG_KEY")
S3_INPUT_KEY = config.get("S3", "INPUT_KEY")
S3_OUTPUT_KEY = config.get("S3", "OUTPUT_KEY")

default_args = {
    "owner": "cdamen",
    "depends_on_past": True,
    "wait_for_downstream": True,
    "start_date": datetime(2021, 2, 21),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
}

dag = DAG(
    "imdb_to_redshift",
    default_args=default_args,
    max_active_runs=1
)

start_data_pipeline = DummyOperator(task_id="start_data_pipeline", dag=dag)

create_redshift_cluster = CreateRedshiftClusterConnectionOperator(
    task_id='create_redshift_cluster',
    conn_id=REDSHIFT_CONN_ID,
    conn_type='postgres',
    dag=dag,
    aws_credentials_id=AWS_CREDENTIAL_ID,
    region='us-east-1',
    role_name='udacity-capstone-redshift',
    role_policies=['AmazonS3FullAccess', 'AmazonRedshiftFullAccess'],
    cluster_type='multi-node',
    cluster_name='capstone-nano-degree-de-project-y',
    node_type='dc2.large',
    num_nodes=4,
    db_name='capstone',
    db_user='admin',
    db_password='UdacityEndProject12',
    port=5439,
    ip='0.0.0.0/0'
)

create_redshift_table = CreateRedshiftTableOperator(
    task_id='create_redshift_table',
    dag=dag,
    redshift_conn_id='redshift',
    aws_credentials_id=AWS_CREDENTIAL_ID,
    table='imdb_titles',
    creation_query=create_imdb_table
)

copy_from_s3_to_redshift = S3ToRedshiftOperator(
    task_id='transfer_s3_to_redshift',
    dag=dag,
    redshift_conn_id=REDSHIFT_CONN_ID,
    aws_conn_id=AWS_CREDENTIAL_ID,
    s3_bucket=S3_BUCKET,
    s3_key=f"{S3_PROJECT}/{S3_OUTPUT_KEY}/imdb_movie_details/",
    table='imdb_titles',
    schema='PUBLIC',
    copy_options=['FORMAT AS PARQUET']
)

run_data_validation = DataValidationOperator(
    task_id='run_data_validation',
    dag=dag,
    redshift_conn_id=REDSHIFT_CONN_ID,
    table='imdb_titles',
    null_columns=('title_key', 'primary_title', 'original_title', 'title_type'),
    pos_columns=('average_rating', 'num_votes')
)

end_data_pipeline = DummyOperator(task_id="end_data_pipeline", dag=dag)

start_data_pipeline >> create_redshift_cluster >> create_redshift_table >> \
copy_from_s3_to_redshift >> run_data_validation >> end_data_pipeline
