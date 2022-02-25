from airflow import settings
from airflow.models import Connection
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class SetupRedshiftConnectionOperator(BaseOperator):

    """
    Setup Airflow connection programmatically
    """

    ui_color = '#F3FF33'

    @apply_defaults
    def __init__(self,
                 conn_id: str,
                 conn_type: str,
                 host: str,
                 login: str,
                 port: str,
                 schema: str,
                 password: str,
                 *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.conn_id = conn_id
        self.conn_type = conn_type
        self.host = host,
        self.login = login,
        self.port = port
        self.schema = schema
        self.password = password

    def execute(self, context):

        self.log.info(f"Using {self.host} as Redshift DB host")

