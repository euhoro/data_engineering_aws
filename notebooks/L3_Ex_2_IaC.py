#!/usr/bin/env python
# coding: utf-8

from time import sleep
import pandas as pd
import boto3
import json
import configparser
import redshift_connector
from botocore.exceptions import ClientError
from sqlalchemy import create_engine

def load_config():
    """Load configuration from 'dwh.cfg' file.

    Returns:
        configparser.ConfigParser: Configuration object with AWS and Redshift settings.
    """
    config = configparser.ConfigParser()
    config.read_file(open('dwh.cfg'))
    return config

def create_clients(config):
    """Create AWS clients for EC2, S3, IAM, and Redshift.

    Args:
        config (configparser.ConfigParser): Configuration object with AWS settings.

    Returns:
        tuple: AWS clients for EC2, S3, IAM, and Redshift.
    """
    ec2 = boto3.resource('ec2',
                           region_name=config.get('AWS','region'),
                           aws_access_key_id=config.get('AWS','key'),
                           aws_secret_access_key=config.get('AWS','secret')
                        )

    s3 = boto3.resource('s3',
                           region_name=config.get('AWS','region'),
                           aws_access_key_id=config.get('AWS','key'),
                           aws_secret_access_key=config.get('AWS','secret')
                       )

    iam = boto3.client('iam',aws_access_key_id=config.get('AWS','key'),
                         aws_secret_access_key=config.get('AWS','secret'),
                         region_name=config.get('AWS','region')
                      )

    redshift = boto3.client('redshift',
                           region_name=config.get('AWS','region'),
                           aws_access_key_id=config.get('AWS','key'),
                           aws_secret_access_key=config.get('AWS','secret')
                           )
    return ec2, s3, iam, redshift

def create_iam_role(iam, role_name):
    """Create an IAM role for Redshift to access S3.

    Args:
        iam (boto3.client): IAM client.
        role_name (str): Name of the IAM role.

    Returns:
        str: ARN of the created IAM role.
    """
    try:
        dwhRole = iam.create_role(
            Path='/',
            RoleName=role_name,
            Description = "Allows Redshift clusters to call AWS services on your behalf.",
            AssumeRolePolicyDocument=json.dumps(
                {'Statement': [{'Action': 'sts:AssumeRole',
                   'Effect': 'Allow',
                   'Principal': {'Service': 'redshift.amazonaws.com'}}],
                 'Version': '2012-10-17'})
        )    
    except Exception as e:
        print(e)
        
    iam.attach_role_policy(RoleName=role_name,
                           PolicyArn="arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess"
                          )['ResponseMetadata']['HTTPStatusCode']

    roleArn = iam.get_role(RoleName=role_name)['Role']['Arn']
    return roleArn

def create_redshift_cluster(redshift, config, roleArn):
    """Create a Redshift cluster.

    Args:
        redshift (boto3.client): Redshift client.
        config (configparser.ConfigParser): Configuration object with Redshift settings.
        roleArn (str): ARN of the IAM role.
    """
    try:
        response = redshift.create_cluster(        
            ClusterType=config.get("DWH","DWH_CLUSTER_TYPE"),
            NodeType=config.get("DWH","DWH_NODE_TYPE"),
            NumberOfNodes=int(config.get("DWH","DWH_NUM_NODES")),
            DBName=config.get("DWH","DWH_DB"),
            ClusterIdentifier=config.get("DWH","DWH_CLUSTER_IDENTIFIER"),
            MasterUsername=config.get("DWH","DWH_DB_USER"),
            MasterUserPassword=config.get("DWH","DWH_DB_PASSWORD"),
            IamRoles=[roleArn]  
        )
    except Exception as e:
        print(e)

def open_tcp_port(ec2, myClusterProps, DWH_PORT):
    """Open an incoming TCP port to access the Redshift cluster endpoint.

    Args:
        ec2 (boto3.resource): EC2 resource.
        myClusterProps (dict): Properties of the Redshift cluster.
        DWH_PORT (str): Port number for the Redshift cluster.
    """
    try:
        vpc = ec2.Vpc(id=myClusterProps['VpcId'])
        defaultSg = list(vpc.security_groups.all())[0]
        defaultSg.authorize_ingress(
            GroupName=defaultSg.group_name,
            CidrIp='0.0.0.0/0',
            IpProtocol='TCP',
            FromPort=int(DWH_PORT),
            ToPort=int(DWH_PORT)
        )
    except Exception as e:
        print(e)

def connect_redshift(DWH_ENDPOINT, DWH_DB, DWH_PORT, DWH_DB_USER, DWH_DB_PASSWORD):
    """Connect to the Redshift cluster and verify the connection.

    Args:
        DWH_ENDPOINT (str): Endpoint address of the Redshift cluster.
        DWH_DB (str): Database name.
        DWH_PORT (str): Port number.
        DWH_DB_USER (str): Database user.
        DWH_DB_PASSWORD (str): Database password.
    """
    conn_string="postgresql://{}:{}@{}:{}/{}".format(DWH_DB_USER, DWH_DB_PASSWORD, DWH_ENDPOINT, DWH_PORT, DWH_DB)
    conn = redshift_connector.connect(
         host=DWH_ENDPOINT,
         database=DWH_DB,
         port=int(DWH_PORT),
         user=DWH_DB_USER,
         password=DWH_DB_PASSWORD
    )

    engine = create_engine(conn_string)

    with engine.connect() as connection:
        result = connection.execute("SELECT current_date;")
        print(result.fetchone())

def copy_s3_data(s3, config):
    """Copy data from S3 source locations to S3 destination locations.

    Args:
        s3 (boto3.resource): S3 resource.
        config (configparser.ConfigParser): Configuration object with S3 settings.
    """
    s3_sections = ['log_data', 'log_json_path', 'song_data']
    
    for section in s3_sections:
        src = config.get('S3', section).replace("s3://", "").split("/", 1)
        dest = config.get('S3Output', section).replace("s3://", "").split("/", 1)

        src_bucket = src[0]
        src_prefix = src[1] if len(src) > 1 else ""

        dest_bucket = dest[0]
        dest_prefix = dest[1] if len(dest) > 1 else ""

        src_bucket_obj = s3.Bucket(src_bucket)
        for obj in src_bucket_obj.objects.filter(Prefix=src_prefix):
            src_key = obj.key
            dest_key = f"{dest_prefix}/{src_key[len(src_prefix):]}" if src_prefix else src_key
            copy_source = {'Bucket': src_bucket, 'Key': src_key}
            s3.Object(dest_bucket, dest_key).copy(copy_source)
            print(f"Copied {src_key} from {src_bucket} to {dest_key} in {dest_bucket}")

def clean_s3_output_bucket(s3, config):
    """Clean the S3 output bucket by deleting all objects in it.

    Args:
        s3 (boto3.resource): S3 resource.
        config (configparser.ConfigParser): Configuration object with S3 settings.
    """
    s3_sections = ['log_data', 'log_json_path', 'song_data']
    
    for section in s3_sections:
        dest = config.get('S3Output', section).replace("s3://", "").split("/", 1)
        dest_bucket = dest[0]
        dest_prefix = dest[1] if len(dest) > 1 else ""
        
        dest_bucket_obj = s3.Bucket(dest_bucket)
        for obj in dest_bucket_obj.objects.filter(Prefix=dest_prefix):
            obj.delete()
            print(f"Deleted {obj.key} from {dest_bucket}")

def cluster_up():
    # """Start the Redshift cluster and set up the necessary IAM roles."""
    # config = load_config()
    # ec2, s3, iam, redshift = create_clients(config)

    # roleArn = create_iam_role(iam, config.get("DWH", "DWH_IAM_ROLE_NAME"))
    # create_redshift_cluster(redshift, config, roleArn)
    # #'Endpoint' in myClusterProps and 'Address' in myClusterProps['Endpoint']
    # myClusterProps ={}
    # while 'Endpoint' not in myClusterProps or 'Address' not in myClusterProps['Endpoint']:
    #     myClusterProps = redshift.describe_clusters(ClusterIdentifier=config.get("DWH","DWH_CLUSTER_IDENTIFIER"))['Clusters'][0]
    #     sleep(35);print("cluster in progress...")
    # open_tcp_port(ec2, myClusterProps, config.get("DWH","DWH_PORT"))

    # DWH_ENDPOINT = myClusterProps['Endpoint']['Address']
    # DWH_ROLE_ARN = myClusterProps['IamRoles'][0]['IamRoleArn']
    # print("DWH_ENDPOINT :: ", DWH_ENDPOINT)
    # print("DWH_ROLE_ARN :: ", DWH_ROLE_ARN)
    #copy_s3_data(s3, config)
    #connect_redshift(DWH_ENDPOINT, config.get("DWH","DWH_DB"), config.get("DWH","DWH_PORT"), config.get("DWH","DWH_DB_USER"), config.get("DWH","DWH_DB_PASSWORD"))
    pass

def cluster_down():
    """Shut down the Redshift cluster and delete the IAM role."""
    config = load_config()
    _, _, iam, redshift = create_clients(config)

    redshift.delete_cluster(ClusterIdentifier=config.get("DWH","DWH_CLUSTER_IDENTIFIER"), SkipFinalClusterSnapshot=True)

    iam.detach_role_policy(RoleName=config.get("DWH","DWH_IAM_ROLE_NAME"), PolicyArn="arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess")
    iam.delete_role(RoleName=config.get("DWH","DWH_IAM_ROLE_NAME"))
    #clean_s3_output_bucket()

# Run the functions
if __name__ == "__main__":
    cluster_up()
    cluster_down()  # Uncomment to bring down the cluster
