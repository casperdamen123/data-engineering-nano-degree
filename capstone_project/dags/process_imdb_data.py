import configparser
from pathlib import Path
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.contrib.operators.emr_create_job_flow_operator import EmrCreateJobFlowOperator
from airflow.contrib.operators.emr_add_steps_operator import EmrAddStepsOperator
from airflow.contrib.sensors.emr_step_sensor import EmrStepSensor
from airflow.contrib.operators.emr_terminate_job_flow_operator import EmrTerminateJobFlowOperator
from operators.upload_to_s3 import UploadFilesToS3Operator


# Set configurations
config = configparser.ConfigParser(allow_no_value=True)
config.read_file(open('airflow.cfg'))

S3_BUCKET = config.get("S3", "BUCKET")
S3_PROJECT = config.get("S3", "PROJECT")
S3_LOG_KEY = config.get("S3", "LOG_KEY")
S3_INPUT_KEY = config.get("S3", "INPUT_KEY")
S3_OUTPUT_KEY = config.get("S3", "OUTPUT_KEY")

# Spark EMR cluster configurations
JOB_FLOW_OVERRIDES = {
    'Name': 'capstone-nano-degree-de-project',
    'LogUri': "s3://{}/{}/{}".format(S3_BUCKET, S3_PROJECT, S3_LOG_KEY),
    'ReleaseLabel': 'emr-5.29.0',
    'Applications': [{'Name': 'Spark'}, {'Name': 'Hadoop'}],
    'Configurations': [
        {
            'Classification': 'spark-env',
            'Properties': {},
            'Configurations': [
                {
                    'Classification': 'export',
                    'Properties': {
                        'PYSPARK_PYTHON': '/usr/bin/python3',
                        'S3_BUCKET': '{}'.format(S3_BUCKET),
                        'S3_PROJECT': '{}'.format(S3_PROJECT),
                        'S3_INPUT_KEY': '{}'.format(S3_INPUT_KEY),
                        'S3_OUTPUT_KEY': '{}'.format(S3_OUTPUT_KEY)
                    }
                }
            ]
        },
    ],
    'Instances': {
        'InstanceGroups': [
            {
                'Name': 'Master node',
                'Market': 'ON_DEMAND',
                'InstanceRole': 'MASTER',
                'InstanceType': 'm5.xlarge',
                'InstanceCount': 1,
            },
            {
                "Name": "Slave nodes",
                "Market": "ON_DEMAND",
                "InstanceRole": "CORE",
                "InstanceType": "m5.xlarge",
                "InstanceCount": 2
            }
        ],
        'KeepJobFlowAliveWhenNoSteps': False,
        'TerminationProtected': False,
    },
    'JobFlowRole': 'EMR_EC2_DefaultRole',
    'ServiceRole': 'EMR_DefaultRole',
}

SPARK_STEPS = [
    {
        "Name": "Process and merge data files",
        "ActionOnFailure": "CANCEL_AND_WAIT",
        "HadoopJarStep": {
            "Jar": "command-runner.jar",
            "Args": [
                "spark-submit",
                "--deploy-mode",
                "client",
                "s3://{{ params.S3_BUCKET }}/{{ params.S3_PROJECT }}/{{ params.S3_SCRIPT_KEY }}/{{ params.FILE }}",
            ],
        },
    }
]

default_args = {
    "owner": "cdamen",
    "depends_on_past": True,
    "wait_for_downstream": True,
    "start_date": datetime(2021, 2, 21),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

dag = DAG(
    "imdb_process_data",
    default_args=default_args,
    max_active_runs=1
)

start_data_pipeline = DummyOperator(task_id="start_data_pipeline", dag=dag)

# Upload spark pre_processing script to S3
spark_script_to_s3 = UploadFilesToS3Operator(
    task_id="upload_spark_script_to_s3",
    file_path=Path(__file__).parent / 'scripts/imdb_processor.py',
    s3_bucket=config.get('S3', 'BUCKET'),
    s3_project=config.get('S3', 'PROJECT'),
    s3_key='{}/imdb_processor.py'.format(config.get('S3', 'SCRIPT_KEY')),
    aws_credentials_id=config.get('AWS', 'CREDENTIALS_ID'),
    dag=dag
)

# Create an EMR cluster
create_emr_cluster = EmrCreateJobFlowOperator(
    task_id="create_emr_cluster",
    job_flow_overrides=JOB_FLOW_OVERRIDES,
    aws_conn_id=config.get('AWS', 'CREDENTIALS_ID'),
    emr_conn_id="emr_default",
    dag=dag
)

# Add steps to the EMR cluster
add_emr_steps = EmrAddStepsOperator(
    task_id="add_spark_steps",
    job_flow_id="{{ task_instance.xcom_pull(task_ids='create_emr_cluster', key='return_value') }}",
    aws_conn_id=config.get('AWS', 'CREDENTIALS_ID'),
    steps=SPARK_STEPS,
    params={
        "S3_BUCKET": config.get('S3', 'BUCKET'),
        "S3_PROJECT": config.get('S3', 'PROJECT'),
        "S3_INPUT_KEY": config.get('S3', 'INPUT_KEY'),
        "S3_SCRIPT_KEY": config.get('S3', 'SCRIPT_KEY'),
        "FILE": 'imdb_processor.py',
    },
    dag=dag
)

# Wait for steps to complete
check_emr_steps = EmrStepSensor(
    task_id="watch_spark_steps",
    job_flow_id="{{ task_instance.xcom_pull('create_emr_cluster', key='return_value') }}",
    step_id="{{ task_instance.xcom_pull(task_ids='add_spark_steps', key='return_value')[0] }}",
    aws_conn_id=config.get('AWS', 'CREDENTIALS_ID'),
    dag=dag
)

# Terminate the EMR cluster
terminate_emr_cluster = EmrTerminateJobFlowOperator(
    task_id="terminate_emr_cluster",
    job_flow_id="{{ task_instance.xcom_pull(task_ids='create_emr_cluster', key='return_value') }}",
    aws_conn_id=config.get('AWS', 'CREDENTIALS_ID'),
    dag=dag
)

end_data_pipeline = DummyOperator(task_id="end_data_pipeline", dag=dag)

start_data_pipeline >> spark_script_to_s3 >> create_emr_cluster
create_emr_cluster >> add_emr_steps >> check_emr_steps >> terminate_emr_cluster
terminate_emr_cluster >> end_data_pipeline
