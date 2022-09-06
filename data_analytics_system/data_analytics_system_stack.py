#!/usr/bin/env python3
# -*- encoding: utf-8 -*-
# vim: tabstop=2 shiftwidth=2 softtabstop=2 expandtab

import os
import random

import aws_cdk as cdk

from aws_cdk import (
  Stack,
  aws_ec2,
  aws_iam,
  aws_s3 as s3,
  aws_lambda as _lambda,
  aws_kinesis as kinesis,
  aws_kinesisfirehose,
  aws_logs,
  aws_elasticsearch,
  aws_events,
  aws_events_targets
)
from constructs import Construct

from aws_cdk.aws_lambda_event_sources import (
  KinesisEventSource
)

random.seed(47)

S3_BUCKET_LAMBDA_LAYER_LIB = os.getenv('S3_BUCKET_LAMBDA_LAYER_LIB', 'deali-ad-data-lambda-layer-packages')
S3_BUCKET_CRYPTO_LAMBDA_LAYER_LIB = os.getenv('S3_BUCKET_CRYPTO_LAMBDA_LAYER_LIB', 'deali-ad-crypto-lambda-layer')

class DataAnalyticsSystemStack(Stack):

  def __init__(self, scope: Construct, construct_id: str, **kwargs) -> None:
    super().__init__(scope, construct_id, **kwargs)

    deploy_env = self.node.try_get_context("deploy_env")

    vpc_name = {"dev" : "deali-sandbox-vpc", "qa" : "deali-sandbox-vpc", "prod" : "deali-ai-vpc"}.get(deploy_env, "unknown")
    vpc = aws_ec2.Vpc.from_lookup(self, vpc_name, is_default=False)

    sg_use_es = aws_ec2.SecurityGroup(self, "ElasticSearchClientSG",
      vpc=vpc,
      allow_all_outbound=True,
      description='security group for ad elasticsearch client',
      security_group_name='ad-data-es-client'
    )
    cdk.Tags.of(sg_use_es).add('Name', 'use-es-cluster-sg')

    sg_es = aws_ec2.SecurityGroup(self, "ElasticSearchSG",
      vpc=vpc,
      allow_all_outbound=True,
      description='security group for ad elasticsearch cluster',
      security_group_name='ad-data-es',
    )
    cdk.Tags.of(sg_es).add('Name', 'es-cluster-sg')

    sg_es.add_ingress_rule(peer=sg_es, connection=aws_ec2.Port.all_tcp(), description='es-cluster-sg')
    sg_es.add_ingress_rule(peer=sg_use_es, connection=aws_ec2.Port.all_tcp(), description='use-es-cluster-sg')

    s3_bucket = s3.Bucket(self, "s3bucketBelugaAdAction",
      bucket_name="ad-data-beluga-ad-action-{deploy_env}".format(deploy_env=deploy_env))

    ad_action_kinesis_stream = kinesis.Stream(self, "BelugaAdActionKinesisStreams", stream_name='ad-data-beluga-ad-action-{deploy_env}'.format(deploy_env=deploy_env))

    firehose_role_policy_doc = aws_iam.PolicyDocument()
    firehose_role_policy_doc.add_statements(aws_iam.PolicyStatement(**{
      "effect": aws_iam.Effect.ALLOW,
      "resources": [s3_bucket.bucket_arn, "{}/*".format(s3_bucket.bucket_arn)],
      "actions": ["s3:AbortMultipartUpload",
        "s3:GetBucketLocation",
        "s3:GetObject",
        "s3:ListBucket",
        "s3:ListBucketMultipartUploads",
        "s3:PutObject"]
    }))

    firehose_role_policy_doc.add_statements(aws_iam.PolicyStatement(
      effect=aws_iam.Effect.ALLOW,
      resources=["*"],
      actions=["glue:GetTable",
        "glue:GetTableVersion",
        "glue:GetTableVersions"]
    ))

    firehose_role_policy_doc.add_statements(aws_iam.PolicyStatement(
      effect=aws_iam.Effect.ALLOW,
      resources=[ad_action_kinesis_stream.stream_arn],
      actions=["kinesis:DescribeStream",
        "kinesis:GetShardIterator",
        "kinesis:GetRecords"]
    ))

    firehose_log_group_name = "/aws/kinesisfirehose/ad-data-beluga-ad-action-{deploy_env}".format(deploy_env=deploy_env)
    firehose_role_policy_doc.add_statements(aws_iam.PolicyStatement(
      effect=aws_iam.Effect.ALLOW,
      #XXX: The ARN will be formatted as follows:
      # arn:{partition}:{service}:{region}:{account}:{resource}{sep}}{resource-name}
      resources=[self.format_arn(service="logs", resource="log-group",
        resource_name="{}:log-stream:*".format(firehose_log_group_name),
        arn_format=cdk.ArnFormat.COLON_RESOURCE_NAME)],
      actions=["logs:PutLogEvents"]
    ))

    firehose_role = aws_iam.Role(self, "FirehoseDeliveryRole",
      role_name="FirehoseDeliveryRole",
      assumed_by=aws_iam.ServicePrincipal("firehose.amazonaws.com"),
      #XXX: use inline_policies to work around https://github.com/aws/aws-cdk/issues/5221
      inline_policies={
        "firehose_role_policy": firehose_role_policy_doc
      }
    )

    s3_crypto_lib_bucket = s3.Bucket.from_bucket_name(self, construct_id + 'crypto-lib', S3_BUCKET_CRYPTO_LAMBDA_LAYER_LIB)
    crypto_lib_layer = _lambda.LayerVersion(self, "CryptoLib",
      layer_version_name="crypto-lib",
      compatible_runtimes=[_lambda.Runtime.PYTHON_3_7],
      code=_lambda.Code.from_bucket(s3_crypto_lib_bucket, "var/crypto-lib.zip")
    )

    etl_beluga_ad_action_lambda_fn = _lambda.Function(self, "etl-beluga-ad-action",
      runtime=_lambda.Runtime.PYTHON_3_7,
      function_name="etl-beluga-ad-action-{deploy_env}".format(deploy_env=deploy_env),
      handler="etl_beluga_ad_action.lambda_handler",
      description="ETL beluga-ad-action data",
      code=_lambda.Code.from_asset("./src/main/python/ETL"),
      layers=[crypto_lib_layer],
      timeout=cdk.Duration.minutes(5)
    )

    ad_action_to_s3_delivery_stream = aws_kinesisfirehose.CfnDeliveryStream(self, "KinesisFirehoseToS3",
      delivery_stream_name="ad-data-beluga-ad-action-{deploy_env}".format(deploy_env=deploy_env),
      delivery_stream_type="KinesisStreamAsSource",
      kinesis_stream_source_configuration={
        "kinesisStreamArn": ad_action_kinesis_stream.stream_arn,
        "roleArn": firehose_role.role_arn
      },
      extended_s3_destination_configuration={
        "bucketArn": s3_bucket.bucket_arn,
        "bufferingHints": {
          "intervalInSeconds": 60,
          "sizeInMBs": 1
        },
        "cloudWatchLoggingOptions": {
          "enabled": True,
          "logGroupName": firehose_log_group_name,
          "logStreamName": "S3Delivery"
        },
        "compressionFormat": "UNCOMPRESSED", # [GZIP | HADOOP_SNAPPY | Snappy | UNCOMPRESSED | ZIP]
        "prefix": "json-data/year=!{timestamp:yyyy}/month=!{timestamp:MM}/day=!{timestamp:dd}/hour=!{timestamp:HH}/",
        "errorOutputPrefix": "error-json/year=!{timestamp:yyyy}/month=!{timestamp:MM}/day=!{timestamp:dd}/hour=!{timestamp:HH}/!{firehose:error-output-type}",
        "roleArn": firehose_role.role_arn, 
        "processingConfiguration": aws_kinesisfirehose.CfnDeliveryStream.ProcessingConfigurationProperty(
          enabled=True,
          processors=[aws_kinesisfirehose.CfnDeliveryStream.ProcessorProperty(
              type="Lambda",

              parameters=[aws_kinesisfirehose.CfnDeliveryStream.ProcessorParameterProperty(
                  parameter_name="LambdaArn",
                  parameter_value=etl_beluga_ad_action_lambda_fn.function_arn
              )]
          )]
        )
      }
    )

    # ElasticSearch
    #XXX: aws cdk elastsearch example - https://github.com/aws/aws-cdk/issues/2873
    es_domain_name = 'ad-data-es-{deploy_env}'.format(deploy_env=deploy_env)
    subnet_ids = {
      "dev" : ["subnet-0b079ac6535fdc2ce", "subnet-0c49d8d7355a17c41"], 
      "qa" : ["subnet-0b079ac6535fdc2ce", "subnet-0c49d8d7355a17c41"], 
      "prod" : ["subnet-0dfb095b182b3a664", "subnet-0c12e4fe4b6010d7c"]
      }.get(deploy_env, "unknown")
    es_cfn_domain = aws_elasticsearch.CfnDomain(self, "ElasticSearch",
      elasticsearch_cluster_config={
        "dedicatedMasterCount": 3,
        "dedicatedMasterEnabled": True,
        "dedicatedMasterType": "t3.small.elasticsearch",
        "instanceCount": 2,
        "instanceType": "t3.small.elasticsearch",
        "zoneAwarenessEnabled": True
      },
      ebs_options={
        "ebsEnabled": True,
        "volumeSize": 10,
        "volumeType": "gp2"
      },
      domain_name=es_domain_name,
      elasticsearch_version="7.8",
      encryption_at_rest_options={
        "enabled": False
      },
      access_policies={
        "Version": "2012-10-17",
        "Statement": [{
            "Effect": "Allow",
            "Principal": {
              "AWS": "*"
            },
            "Action": [
              "es:Describe*",
              "es:List*",
              "es:Get*",
              "es:ESHttp*"
            ],
            "Resource": self.format_arn(service="es", resource="domain", resource_name="{}/*".format(es_domain_name))
          }
        ]
      },
      snapshot_options={
        "automatedSnapshotStartHour": 17
      },
      vpc_options={
        "securityGroupIds": [sg_es.security_group_id],
        "subnetIds": subnet_ids
      }
    )
    cdk.Tags.of(es_cfn_domain).add('Name', 'ad-data-es-{deploy_env}'.format(deploy_env=deploy_env))

    #XXX: https://github.com/aws/aws-cdk/issues/1342
    s3_lib_bucket = s3.Bucket.from_bucket_name(self, construct_id + 'es-lib', S3_BUCKET_LAMBDA_LAYER_LIB)
    es_lib_layer = _lambda.LayerVersion(self, "ESLib",
      layer_version_name="es-lib",
      compatible_runtimes=[_lambda.Runtime.PYTHON_3_7],
      code=_lambda.Code.from_bucket(s3_lib_bucket, "var/es-lib.zip")
    )

    #XXX: add more than 2 security groups
    # https://github.com/aws/aws-cdk/blob/ea10f0d141a48819ec0000cd7905feda993870a9/packages/%40aws-cdk/aws-lambda/lib/function.ts#L387
    # https://github.com/aws/aws-cdk/issues/1555
    # https://github.com/aws/aws-cdk/pull/5049
    #XXX: Deploy lambda in VPC - https://github.com/aws/aws-cdk/issues/1342
    upsert_to_es_lambda_fn = _lambda.Function(self, "UpsertToES",
      runtime=_lambda.Runtime.PYTHON_3_7,
      function_name="UpsertToES-{deploy_env}".format(deploy_env=deploy_env),
      handler="upsert_to_es.lambda_handler",
      description="Upsert records into elasticsearch",
      code=_lambda.Code.from_asset("./src/main/python/UpsertToES"),
      environment={
        'ES_HOST': es_cfn_domain.attr_domain_endpoint,
        #TODO: MUST set appropriate environment variables for your workloads.
        'ES_INDEX': 'beluga-ad-action',
        'ES_TYPE': 'trans',
        'REQUIRED_FIELDS': 'meta,content',
        'REGION_NAME': kwargs['env'].region,
        'DATE_TYPE_FIELDS': 'datetime'
      },
      timeout=cdk.Duration.minutes(5),
      layers=[es_lib_layer],
      security_groups=[sg_use_es],
      vpc=vpc
    )

    ad_action_kinesis_event_source = KinesisEventSource(ad_action_kinesis_stream, batch_size=1000, starting_position=_lambda.StartingPosition.LATEST)
    upsert_to_es_lambda_fn.add_event_source(ad_action_kinesis_event_source)

    log_group = aws_logs.LogGroup(self, "UpsertToESLogGroup",
      log_group_name="/aws/lambda/UpsertToES-{deploy_env}".format(deploy_env=deploy_env),
      retention=aws_logs.RetentionDays.THREE_DAYS)
    log_group.grant_write(upsert_to_es_lambda_fn)

    # CTAS    
    merge_small_files_lambda_fn = _lambda.Function(self, "MergeSmallFiles-{deploy_env}".format(deploy_env=deploy_env),
      runtime=_lambda.Runtime.PYTHON_3_7,
      function_name="MergeSmallFiles-{deploy_env}".format(deploy_env=deploy_env),
      handler="athena_ctas.lambda_handler",
      description="Merge small files in S3",
      code=_lambda.Code.from_asset("./src/main/python/MergeSmallFiles"),
      environment={
        #TODO: MUST set appropriate environment variables for your workloads.
        'OLD_DATABASE': 'beluga_ad_action_database_{deploy_env}'.format(deploy_env=deploy_env),
        'OLD_TABLE_NAME': 'beluga_ad_action_raw',
        'NEW_DATABASE': 'beluga_ad_action_database_{deploy_env}'.format(deploy_env=deploy_env),
        'NEW_TABLE_NAME': 'ctas_beluga_ad_action_parquet',
        'WORK_GROUP': 'primary',
        'OLD_TABLE_LOCATION_PREFIX': 's3://{}'.format(os.path.join(s3_bucket.bucket_name, 'json-data')),
        'OUTPUT_PREFIX': 's3://{}'.format(os.path.join(s3_bucket.bucket_name, 'parquet-data')),
        'STAGING_OUTPUT_PREFIX': 's3://{}'.format(os.path.join(s3_bucket.bucket_name, 'tmp')),
        'COLUMN_NAMES': '*'
      },
      timeout=cdk.Duration.minutes(5)
    )

    merge_small_files_lambda_fn.add_to_role_policy(aws_iam.PolicyStatement(
      effect=aws_iam.Effect.ALLOW,
      resources=["*"],
      actions=["athena:*"]))
    merge_small_files_lambda_fn.add_to_role_policy(aws_iam.PolicyStatement(
      effect=aws_iam.Effect.ALLOW,
      resources=["*"],
      actions=["s3:Get*",
        "s3:List*",
        "s3:AbortMultipartUpload",
        "s3:PutObject",
      ]))
    merge_small_files_lambda_fn.add_to_role_policy(aws_iam.PolicyStatement(
      effect=aws_iam.Effect.ALLOW,
      resources=["*"],
      actions=["glue:CreateDatabase",
        "glue:DeleteDatabase",
        "glue:GetDatabase",
        "glue:GetDatabases",
        "glue:UpdateDatabase",
        "glue:CreateTable",
        "glue:DeleteTable",
        "glue:BatchDeleteTable",
        "glue:UpdateTable",
        "glue:GetTable",
        "glue:GetTables",
        "glue:BatchCreatePartition",
        "glue:CreatePartition",
        "glue:DeletePartition",
        "glue:BatchDeletePartition",
        "glue:UpdatePartition",
        "glue:GetPartition",
        "glue:GetPartitions",
        "glue:BatchGetPartition"
      ]))
    merge_small_files_lambda_fn.add_to_role_policy(aws_iam.PolicyStatement(
      effect=aws_iam.Effect.ALLOW,
      resources=["*"],
      actions=["lakeformation:GetDataAccess"]))

    lambda_fn_target = aws_events_targets.LambdaFunction(merge_small_files_lambda_fn)
    aws_events.Rule(self, "ScheduleRule",
      schedule=aws_events.Schedule.cron(minute="5"),
      targets=[lambda_fn_target]
    )

    log_group = aws_logs.LogGroup(self, "MergeSmallFilesLogGroup-{deploy_env}".format(deploy_env=deploy_env),
      log_group_name="/aws/lambda/MergeSmallFiles-{deploy_env}".format(deploy_env=deploy_env),
      retention=aws_logs.RetentionDays.THREE_DAYS)
    log_group.grant_write(merge_small_files_lambda_fn)

    cdk.CfnOutput(self, 'ESDomainEndpoint', value=es_cfn_domain.attr_domain_endpoint, export_name='ESDomainEndpoint')
    cdk.CfnOutput(self, 'ESDashboardsURL', value=f"{es_cfn_domain.attr_domain_endpoint}/_dashboards/", export_name='ESDashboardsURL')
