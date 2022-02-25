from typing import List, Dict
import logging
import boto3
import json
import time
import os


class ManageAwsRedshift:

    def __init__(self, credentials, region):

        # Set credentials and region
        self.role_arn = None
        self.credentials = credentials
        self.region = region

        # Resource clients
        self.iam = boto3.client(
            'iam',
            region_name=self.region,
            aws_access_key_id=self.credentials.access_key,
            aws_secret_access_key=self.credentials.secret_key
        )

        self.redshift = boto3.client(
            'redshift',
            region_name=self.region,
            aws_access_key_id=self.credentials.access_key,
            aws_secret_access_key=self.credentials.secret_key
        )

        self.ec2 = boto3.resource(
            'ec2',
            region_name=self.region,
            aws_access_key_id=self.credentials.access_key,
            aws_secret_access_key=self.credentials.secret_key
        )

    def create_role(self, role_name: str, role_policies: List[str]) -> str:
        """Creates IAM role and attached specified policies.
        Args:
            role_name (str): Name of the AWS role
            role_policies: Names of policies to attach to AWS role

        Returns:
            role_arn (str): Role ARN
        """

        assume_role_policy_doc = dict(
            Statement=[
                dict(
                    Action='sts:AssumeRole',
                    Effect='Allow',
                    Principal=dict(Service='redshift.amazonaws.com')
                )
            ],
            Version='2012-10-17'
        )

        # Create role
        try:
            self.iam.create_role(
                Path='/',
                RoleName=role_name,
                Description='Allows Redshift clusters to call AWS services on your behalf',
                AssumeRolePolicyDocument=json.dumps(assume_role_policy_doc)
            )
        except Exception as e:
            print(e)

        # Attach policies to role
        try:
            for role in role_policies:
                self.iam.attach_role_policy(
                    RoleName=role_name,
                    PolicyArn=f"arn:aws:iam::aws:policy/{role}"
                )
                logging.info(f"Attached policy {role} to {role_name}")
        except Exception as e:
            print(e)

        # Store role arn
        try:
            get_role_response = self.iam.get_role(RoleName=role_name)
            logging.info(f"IAM role ARN equals to {get_role_response['Role']['Arn']}")
            self.role_arn = get_role_response['Role']['Arn']
            return self.role_arn
        except Exception as e:
            print(e)

    def delete_role(self, role_name: str, role_policies: List[str]) -> str:
        """
        Deletes IAM role after removing specified policies.
        Args:
            role_name (str): Name of the AWS role
            role_policies: Names of all policies to remove before able to delete AWS role

        Returns:
            The deleted role name
        """

        # Detach policy from role
        for policy in role_policies:
            policy_arn = f"arn:aws:iam::aws:policy/{policy}"
            try:
                self.iam.detach_role_policy(
                    RoleName=role_name,
                    PolicyArn=policy_arn
                )
                logging.info(f"Detached policy {policy}")
            except Exception as e:
                print(e)

        # Delete role
        try:
            self.iam.delete_role(RoleName=role_name)
            logging.info(f"Deleted role {role_name}")
            return role_name
        except Exception as e:
            print(e)

    def create_cluster(self, cluster_type: str, cluster_name: str, node_type: str,
                       num_nodes: str, db_name: str, db_user: str, db_password: str,
                       role_name: str, port: str) -> str:

        """Create Redshift cluster if not already exists, use cluster to set up Airflow connection.
        Args:
            - cluster_type (str): Type of the cluster to be create
            - cluster_name (str): Name of Redshift cluster to create
            - node_type (str):

        Returns:
            - cluster_endpoint (str): Endpoint of cluster
        """

        try:
            self.redshift.create_cluster(
                ClusterType=cluster_type,
                NodeType=node_type,
                NumberOfNodes=num_nodes,
                DBName=db_name,
                ClusterIdentifier=cluster_name,
                MasterUsername=db_user,
                MasterUserPassword=db_password,
                IamRoles=[self._get_iam_role_arn(role_name)],
                Port=port
            )
            logging.info(f"Cluster {cluster_name} created")

        except Exception as e:
            print(e)

        # Wait 10 seconds if cluster status is not available, otherwise return cluster endpoint
        while self._get_cluster_properties(cluster_name)['ClusterStatus'] != 'available':
            logging.info(f"Cluster status is {self._get_cluster_properties(cluster_name)['ClusterStatus']}")
            logging.info(f"Waiting for {cluster_name} to become available")
            time.sleep(10)
        else:
            cluster_endpoint = self._get_cluster_properties(cluster_name)['Endpoint']['Address']
            return cluster_endpoint

    def delete_cluster(self, cluster_name: str) -> str:
        """Deletes a redshift cluster
        Args:
            cluster_name (str): Name of Redshift cluster to create

        Returns:
            Name of the deleted cluster
        """

        try:
            delete_cluster_response = self.redshift.delete_cluster(ClusterIdentifier=cluster_name,
                                                                   SkipFinalClusterSnapshot=True)
            logging.info(f"Deleted {delete_cluster_response['Cluster']['ClusterIdentifier']}")
            return delete_cluster_response['Cluster']['ClusterIdentifier']
        except Exception as e:
            print(e)

    def add_ingress_role_vpc(self, cluster_name: str, port: int, ip: str) -> None:
        """Setting ingress role to default security group for cluster
        Args:
            cluster_name (str): The identifier for the Redshift cluster
            port (int): Port for cluster endpoint
            ip (int): IP to set ingress rule for

        Returns:
            None
        """
        try:
            default_sg = self._get_default_security_group(cluster_name)
            logging.info(f"Adding ingress role for {ip} to {default_sg}")

            default_sg.authorize_ingress(
                GroupName=default_sg.group_name,
                CidrIp=ip,
                IpProtocol='TCP',
                FromPort=port,
                ToPort=port
            )

        except Exception as e:
            print(e)

    def _get_iam_role_arn(self, name: str) -> str:
        """Get IAM role ARN
        Args:
            - name (str): Name of the role

        Returns:
            - role_arn (str): Role arn for role name
        """
        try:
            role = self.iam.get_role(RoleName=name)
            return role['Role']['Arn']
        except Exception as e:
            print(e)

    def _get_cluster_properties(self, cluster_name: str) -> Dict:
        """Get properties on cluster
        Args:
            cluster_name (str): The identifier for the Redshift cluster

        Returns:
            cluster_properties (dict): Dictionary with cluster properties
        """
        return self.redshift.describe_clusters(ClusterIdentifier=cluster_name)['Clusters'][0]

    def _get_default_security_group(self, cluster_name: str) -> str:
        """Get default security group for cluster
        Args:
            cluster_name (str): The identifier for the Redshift cluster

        Returns:
            security_group (str): Security group ID
        """
        cluster_properties = self._get_cluster_properties(cluster_name)
        vpc_sg = cluster_properties['VpcSecurityGroups'][0]
        return self.ec2.SecurityGroup(id=vpc_sg['VpcSecurityGroupId'])
