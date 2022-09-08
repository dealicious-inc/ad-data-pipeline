# es에 ad action 인덱스 생성

import argparse
import json
import sys

import boto3
from elasticsearch import Elasticsearch
from elasticsearch import RequestsHttpConnection
from requests_aws4auth import AWS4Auth


def create_ad_data_index(es_client):
  es_client.indices.create(
    index='ad_action',
    body={
      "mappings": {
        "properties": {
          "userid": {"type": "text"},
          "platform": {"type": "text"},
          "osVersion": {"type": "text"},
          "appVersion": {"type": "text"},
          "screenName": {"type": "text"},
          "screenLabel": {"type": "text"},
          "referrer": {"type": "text"},
          "referrerLabel": {"type": "text"},
          "accountId": {"type": "text"},
          "bidPrice": {"type": "double"},
          "campaignIdx": {"type": "long"},
          "chargingType": {"type": "text"},
          "event": {"type": "text"},
          "groupIdx": {"type": "long"},
          "productIdx": {"type": "long"},
          "selectionGroupId": {"type": "text"},
          "selectionId": {"type": "text"},
          "wsIdx": {"type": "long"},
          "unitIdx": {"type": "long"},
          "creativeIdx": {"type": "long"},
          "pageIdx": {"type": "long"},
          "query": {"type": "text"},
          "keywordIdx": {"type": "long"},
          "keyword": {"type": "text"},
          "userType": {"type": "text"},
          "rsIdx": {"type": "long"},
          "uuid": {"type": "text"},
          "storeId": {"type": "long"},
          "cdIdx": {"type": "long"},
          "selectionTime": {"type": "date"},
          "exposureTime": {"type": "date"},
          "timestamp": {"type": "date"},
          "datetime": {"type": "date"}
        }
      }
    }
  )


def main():
  parser = argparse.ArgumentParser()

  parser.add_argument(
    '--region-name',
    action='store',
    default='ap-northeast-2',
    help='aws region name (default: ap-northeast-2)')
  parser.add_argument(
    '--es-host',
    help='The host uri of es ex) vpc-xxx.ap-northeast-2.es.amazonaws.com'
  )
  parser.add_argument('--profile', default='deali-sandbox')

  options = parser.parse_args()

  session = boto3.Session(profile_name=options.profile)
  credentials = session.get_credentials()
  credentials = credentials.get_frozen_credentials()
  access_key, secret_key, token = (credentials.access_key, credentials.secret_key, credentials.token)

  aws_auth = AWS4Auth(
    access_key,
    secret_key,
    options.region_name,
    'es',
    session_token=token
  )

  es_client = Elasticsearch(
    hosts=[{'host': options.es_host, 'port': 443}],
    http_auth=aws_auth,
    use_ssl=True,
    verify_certs=True,
    connection_class=RequestsHttpConnection
  )
  print('[INFO] ElasticSearch Service', json.dumps(es_client.info(), indent=2), file=sys.stderr)

  create_ad_data_index(es_client)


if __name__ == '__main__':
  main()
