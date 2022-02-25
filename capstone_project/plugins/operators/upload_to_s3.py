from airflow.hooks.S3_hook import S3Hook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class UploadFilesToS3Operator(BaseOperator):
    """
    Upload a local file to specified S3 bucket
    """

    ui_color = '#74FF33'

    @apply_defaults
    def __init__(self,
                 file_path="",
                 s3_bucket="",
                 s3_project="",
                 s3_key="",
                 aws_credentials_id="",
                 *args, **kwargs):

        super(UploadFilesToS3Operator, self).__init__(*args, **kwargs)
        self.s3_bucket = s3_bucket
        self.s3_project = s3_project
        self.s3_key = s3_key
        self.aws_credentials_id = aws_credentials_id
        self.file_path = file_path

    def execute(self, context):
        s3 = S3Hook(aws_conn_id=self.aws_credentials_id, )
        s3_path = "s3://{}/{}/{}".format(self.s3_bucket, self.s3_project, self.s3_key)
        s3_combined_key = "{}/{}".format(self.s3_project, self.s3_key)
        self.log.info(f"Copying file {self.file_path} to {s3_path}")
        s3.load_file(filename=self.file_path, bucket_name=self.s3_bucket, replace=True, key=s3_combined_key)
