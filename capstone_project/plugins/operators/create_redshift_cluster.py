from airflow.providers.amazon.aws.hooks.base_aws import AwsBaseHook
from airflow.models import BaseOperator
from airflow import settings
from airflow.models import Connection
from helpers.aws_redshift import ManageAwsRedshift
from airflow.utils.decorators import apply_defaults
from typing import List

class CreateRedshiftClusterConnectionOperator(BaseOperator):

    """
    Create redshift cluster with database
    """

    ui_color = '#33FFE9'

    @apply_defaults
    def __init__(self,
                 aws_credentials_id: str,
                 conn_id: str,
                 conn_type: str,
                 region: str,
                 role_name: str,
                 role_policies: List[str],
                 cluster_type: str,
                 cluster_name: str,
                 node_type: str,
                 num_nodes: int,
                 db_name: str,
                 db_user: str,
                 db_password: str,
                 port: int,
                 ip: str,
                 *args, **kwargs):
        super(CreateRedshiftClusterConnectionOperator, self).__init__(*args, **kwargs)
        self.aws_credentials_id = aws_credentials_id
        self.conn_id = conn_id
        self.conn_type = conn_type
        self.region = region
        self.role_name = role_name
        self.role_policies = role_policies
        self.cluster_type = cluster_type
        self.cluster_name = cluster_name
        self.num_nodes = num_nodes
        self.node_type = node_type
        self.db_name = db_name
        self.db_user = db_user
        self.db_password = db_password
        self.port = port
        self.ip = ip

    def execute(self, context):
        aws_hook = AwsBaseHook(self.aws_credentials_id, client_type='iam')
        credentials = aws_hook.get_credentials()

        redshift = ManageAwsRedshift(credentials, self.region)

        self.log.info("Creating IAM role named: {}".format(self.role_name))
        redshift.create_role(self.role_name, self.role_policies)

        self.log.info("Creating cluster named: {}".format(self.cluster_name))
        host = redshift.create_cluster(self.cluster_type, self.cluster_name, self.node_type,
                                       self.num_nodes, self.db_name, self.db_user,
                                       self.db_password, self.role_name, self.port)

        self.log.info("Adding ingress role to VPC")
        redshift.add_ingress_role_vpc(self.cluster_name, self.port, self.ip)

        self.log.info(f"Setting up Airflow connection for {host}")
        try:
            conn = Connection(
                conn_id=self.conn_id,
                conn_type=self.conn_type,
                host=host,
                login=self.db_user,
                port=self.port,
                schema=self.db_name,
                password=self.db_password
            )

            # Add connection to Airflow
            session = settings.Session()
            session.add(conn)
            session.commit()
            self.log.info("Created Airflow connection {}".format(self.conn_id))
        except Exception as e:
            print(e)
            self.log.info("Airflow connection {} already exists".format(self.conn_id))
