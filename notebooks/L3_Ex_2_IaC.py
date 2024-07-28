#!/usr/bin/env python
# coding: utf-8

import pandas as pd
import boto3
import json
import configparser
import redshift_connector
from botocore.exceptions import ClientError
from sqlalchemy import create_engine

def load_config():
    config = configparser.ConfigParser()
    config.read_file(open('dwh.cfg'))
    return config

def create_clients(config):
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

def cluster_up():
    config = load_config()
    ec2, s3, iam, redshift = create_clients(config)

    roleArn = create_iam_role(iam, config.get("DWH", "DWH_IAM_ROLE_NAME"))
    create_redshift_cluster(redshift, config, roleArn)

    myClusterProps = redshift.describe_clusters(ClusterIdentifier=config.get("DWH","DWH_CLUSTER_IDENTIFIER"))['Clusters'][0]
    open_tcp_port(ec2, myClusterProps, config.get("DWH","DWH_PORT"))

    DWH_ENDPOINT = myClusterProps['Endpoint']['Address']
    DWH_ROLE_ARN = myClusterProps['IamRoles'][0]['IamRoleArn']
    print("DWH_ENDPOINT :: ", DWH_ENDPOINT)
    print("DWH_ROLE_ARN :: ", DWH_ROLE_ARN)

    connect_redshift(DWH_ENDPOINT, config.get("DWH","DWH_DB"), config.get("DWH","DWH_PORT"), config.get("DWH","DWH_DB_USER"), config.get("DWH","DWH_DB_PASSWORD"))

def cluster_down():
    config = load_config()
    _, _, iam, redshift = create_clients(config)

    redshift.delete_cluster(ClusterIdentifier=config.get("DWH","DWH_CLUSTER_IDENTIFIER"), SkipFinalClusterSnapshot=True)

    iam.detach_role_policy(RoleName=config.get("DWH","DWH_IAM_ROLE_NAME"), PolicyArn="arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess")
    iam.delete_role(RoleName=config.get("DWH","DWH_IAM_ROLE_NAME"))

# Run the functions
if __name__ == "__main__":
    cluster_up()
    cluster_down()  # Uncomment to bring down the cluster
