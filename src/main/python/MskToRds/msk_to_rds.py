import base64
import itertools
import json
import os
import traceback

import awswrangler as wr
import pandas as pd

CONFIG_FILE = os.getenv("CONFIG_FILE")  # 파일경로/파일명.json


def lambda_handler(event, context):
  """
  kafka topic -> rds
  ad_payment_by_creative -> ad_payment_by_creative_rt

  MSK 권한 추가 필요
  https://docs.aws.amazon.com/ko_kr/lambda/latest/dg/lambda-intro-execution-role.html#permissions-executionrole-features
  https://docs.aws.amazon.com/ko_kr/lambda/latest/dg/with-msk.html#msk-permissions-iam-policy#msk-permissions-iam-policy

  Secret Manager 추가 필요
  https://docs.aws.amazon.com/mediaconnect/latest/ug/iam-policy-examples-asm-secrets.html

  RDS 권한 필요
  """
  try:
    config = json.load(open(CONFIG_FILE, "r"))

    """
    현재 카프카만 고려하여 데이터프레임 추출 중
    다른 이벤트 소스 고려해야할 경우 event.eventSource 사용해서 분기 가능
    """
    records = list(event["records"].values())
    messages = list(
      map(
        lambda record: json.loads(base64.b64decode(
          record["value"]).decode("utf-8")),
        itertools.chain.from_iterable(records)
      )
    )
    print(f"number of messages: {len(messages)}")
    df = pd.DataFrame(
      messages
    )

    for datetime_column in config["datetimeColumns"]:
      df[datetime_column] = pd.to_datetime(
        arg=df[datetime_column],
        utc=True  # KST 원할 경우 False
      )
      df[datetime_column] = df[datetime_column].dt.strftime(config["datetimeFormat"])

    for numeric_column in config["numericColumns"]:
      df[numeric_column] = pd.to_numeric(df[numeric_column])

    df = df.groupby(
      by=config["groupBy"],
      group_keys=True,
      as_index=False
    )
    df = df[config["columnToSum"]].sum()
    df.rename(
      columns=config["renameColumn"],
      inplace=True
    )

    secret_id = config["secretId"]
    con = wr.mysql.connect(secret_id=secret_id)
    wr.mysql.to_sql(
      df=df,
      table=config["tableName"],
      schema=con.db.decode("utf-8"),
      con=con,
      use_column_names=True,
      mode="upsert_duplicate_key"  # 단순 insert는 append 사용
    )
    con.close()

  except Exception as ex:
    print(event)
    print(context)
    traceback.print_exc()
