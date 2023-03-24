import json

from aws_cdk import (
    Stack,
    aws_ec2 as ec2,
    aws_glue as glue,
)
from aws_cdk.aws_glue import CfnCrawler
from constructs import Construct
import boto3


class CdkStack(Stack):

    def __init__(self, scope: Construct, construct_id: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        secret_name = "rds-creds"
        region_name = "eu-north-1"

        # Create a Secrets Manager client
        session = boto3.session.Session()
        client = session.client(
            service_name='secretsmanager',
            region_name=region_name
        )

        get_secret_value_response = client.get_secret_value(SecretId=secret_name)

        # Decrypts secret values from the SecretString.
        secrets = json.loads(get_secret_value_response['SecretString'])
        secret_username = secrets['username']
        secret_password = secrets['password']

        security_group = ec2.SecurityGroup.from_security_group_id(
            self,
            "MySecurityGroup",
            security_group_id="sg-03dd0099c4253b670"
        )
        subnet = ec2.Subnet.from_subnet_attributes(
            self,
            "MySubnet",
            subnet_id="subnet-04c0c3337404ca35a",
            availability_zone="eu-north-1a"
        )

        # Create the Glue connection for RDS
        cfn_connection = glue.CfnConnection(self, "MyCfnConnection",
                                            catalog_id="789733903478",
                                            connection_input=glue.CfnConnection.ConnectionInputProperty(
                                                connection_type="JDBC",

                                                connection_properties={
                                                    "JDBC_CONNECTION_URL": "jdbc:postgresql://database-1.cr4vhy3almjs.eu-north-1.rds.amazonaws.com:5432/postgres",
                                                    "PASSWORD": secret_password,
                                                    "USERNAME": secret_username,
                                                },
                                                name="MyCfnConnection",
                                                physical_connection_requirements=glue.CfnConnection.PhysicalConnectionRequirementsProperty(
                                                    security_group_id_list=[security_group.security_group_id],
                                                    subnet_id=subnet.subnet_id,
                                                    availability_zone=subnet.availability_zone
                                                )
                                            )
                                            )

        db_props = {
            "catalog_id": "789733903478",
            "database_input": {
                "name": "my-glue-database"
            }
        }
        db = glue.CfnDatabase(self, "MyGlueDatabase", **db_props)

        # create a Glue Crawler that uses the JDBC connection
        crawler = CfnCrawler(
            self,
            "MyGlueCrawler",
            role="arn:aws:iam::789733903478:role/service-role/AWSGlueServiceRole",
            database_name="my-glue-database",
            targets={
                "jdbcTargets": [
                    {
                        "connectionName": cfn_connection.ref,
                        "path": "postgres/public/users"
                    }
                ]
            }
        )

        # Create the Glue job
        glue_job = glue.CfnJob(self, 'MyGlueJob',
                               command=glue.CfnJob.JobCommandProperty(
                                   name="glueetl",
                                   python_version="3",
                                   script_location="s3://sample-rds-glue-bucket/scripts/sample-rds-s3.py"
                               ),
                               connections=glue.CfnJob.ConnectionsListProperty(
                                   connections=["MyCfnConnection"]
                               ),
                               role="arn:aws:iam::789733903478:role/service-role/AWSGlueServiceRole",
                               name="MyGlueJob",
                               timeout=30,
                               glue_version="3.0"
                               )
