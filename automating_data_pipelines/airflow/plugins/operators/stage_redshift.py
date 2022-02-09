from airflow.hooks.postgres_hook import PostgresHook
from airflow.providers.amazon.aws.hooks.base_aws import AwsBaseHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from helpers.sql_statements_project import SqlQueries

class StageToRedshiftOperator(BaseOperator):
    """
    This operator loads staging data from an S3 bucket to a Redshift cluster, currently designed for song/log data
    """
    ui_color = '#358140'
    template_fields = ["s3_key"]
    copy_sql = SqlQueries.copy_json_table

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 s3_bucket="",
                 s3_key="",
                 table="",
                 region="",
                 json_path="",
                 aws_credentials_id="",
                 *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.table = table
        self.region = region
        self.json_path = json_path
        self.aws_credentials_id = aws_credentials_id

    def execute(self, context):

        aws_hook = AwsBaseHook(self.aws_credentials_id, client_type='iam')
        credentials = aws_hook.get_credentials()
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        if self.table == "staging_events":
            self.log.info(f"Create {self.table} table if not exists")
            redshift.run(SqlQueries.staging_events_table_create)

            # Dynamic s3_key for log files based on execution time
            self.s3_key = self.s3_key.format(**context)

        elif self.table == "staging_songs":
            self.log.info(f"Create {self.table} table if not exists")
            redshift.run(SqlQueries.staging_songs_table_create)

        s3_path = f"s3://{self.s3_bucket}/{self.s3_key}"

        self.log.info("Copying data from S3 to Redshift")

        formatted_sql = StageToRedshiftOperator.copy_sql.format(
            table=self.table,
            s3_data_path=s3_path,
            aws_access_key=credentials.access_key,
            aws_secret_key=credentials.secret_key,
            region=self.region,
            json_path=self.json_path
        )

        redshift.run(formatted_sql)
