# <a name="top"></a>광고 데이터 파이프라인 

이 코드의 목적은 광고 데이터 파이프라인을 AWS cdk 를 활용하여 구축하는 것입니다. AWS sample 코드를 기반으로 딜리셔스 환경에 맞게 수정하여 구축하였습니다. 

## Prerequisites
1. AWS CDK Toolkit 을 설치합니다.
    ```sh
    npm install -g aws-cdk
    ```
2. cdk가 정상적으로 설치되었는지, 다음 명령어를 실행해서 확인합니다.
    ```sh
    cdk --version
    ```
    예)
    ```shell script
    $ cdk --version
    1.71.0 (build 953bc25)
    ```
3. ElasticSearch 를 위한 Lambda Layer 를 추가합니다.
    
    :warning: **Python 패키지를 생성할 때는 AWS Lambda의 실행환경과 동일한 환경에서 생성해야하므로, Amazon Linux에서 Python 패키지를 생성하는 것을 추천 드립니다.**
      <pre>
      [ec2-user@ip-172-31-6-207 ~] $ python3 -m venv es-lib # virtual environments을 생성함
      [ec2-user@ip-172-31-6-207 ~] $ cd es-lib
      [ec2-user@ip-172-31-6-207 ~] $ source bin/activate
      (es-lib) $ mkdir -p python_modules # 필요한 패키지를 저장할 디렉터리 생성
      (es-lib) $ pip install 'elasticsearch>=7.0.0,<7.11' requests requests-aws4auth -t python_modules # 필요한 패키지를 사용자가 지정한 패키지 디렉터리에 저장함
      (es-lib) $ mv python_modules python # 사용자가 지정한 패키지 디렉터리 이름을 python으로 변경함 (python 디렉터리에 패키지를 설치할 경우 에러가 나기 때문에 다른 이름의 디렉터리에 패키지를 설치 후, 디렉터리 이름을 변경함)
      (es-lib) $ zip -r es-lib.zip python/ # 필요한 패키지가 설치된 디렉터리를 압축함
      (es-lib) $ aws s3 mb s3://deali-ad-data-lambda-layer-packages --region ap-northeast-2 # 압축한 패키지를 업로드할 s3 bucket을 생성함
      (es-lib) $ aws s3 cp es-lib.zip s3://deali-ad-data-lambda-layer-packages/var/ --region ap-northeast-2 # 압축한 패키지를 s3에 업로드 한 후, lambda layer에 패키지를 등록할 때, s3 위치를 등록하면 됨
      (es-lib) $ deactivate
      </pre>
    + [How to create a Lambda layer using a simulated Lambda environment with Docker](https://aws.amazon.com/premiumsupport/knowledge-center/lambda-layer-simulated-docker/)
      ```
      $ cat <<EOF > requirements.txt
      > elasticsearch>=7.0.0,<7.11
      > requests==2.23.0
      > requests-aws4auth==0.9
      > EOF
      $ docker run -v "$PWD":/var/task "public.ecr.aws/sam/build-python3.7" /bin/sh -c "pip install -r requirements.txt -t python/lib/python3.7/site-packages/; exit"
      $ zip -r es-lib.zip python > /dev/null
      $ aws s3 mb s3://deali-ad-data-lambda-layer-packages --region ap-northeast-2
      $ aws s3 cp es-lib.zip s3://deali-ad-data-lambda-layer-packages/var/ --region ap-northeast-2
      ```

4. bidPrice 필드를 decrypt 하기 위해 crypto 라이브러리를 위한 Lambda Layer 를 추가합니다. 방법은 위와 동일합니다. 
    <pre>
    [ec2-user@ip-172-31-6-207 ~] $ python3 -m venv crypto-lib # virtual environments을 생성함
    [ec2-user@ip-172-31-6-207 ~] $ cd crypto-lib
    [ec2-user@ip-172-31-6-207 ~] $ source bin/activate
    (crypto-lib) $ mkdir -p python_modules # 필요한 패키지를 저장할 디렉터리 생성
    (crypto-lib) $ pip install pycryptodome -t python_modules/ # 필요한 패키지를 사용자가 지정한 패키지 디렉터리에 저장함
    (crypto-lib) $ mv python_modules python # 사용자가 지정한 패키지 디렉터리 이름을 python으로 변경함 (python 디렉터리에 패키지를 설치할 경우 에러가 나기 때문에 다른 이름의 디렉터리에 패키지를 설치 후, 디렉터리 이름을 변경함)
    (crypto-lib) $ zip -r crypto-lib.zip python/ # 필요한 패키지가 설치된 디렉터리를 압축함
    (crypto-lib) $ aws s3 mb s3://deali-ad-crypto-lambda-layer --region ap-northeast-2 # 압축한 패키지를 업로드할 s3 bucket을 생성함
    (crypto-lib) $ aws s3 cp crypto-lib.zip s3://deali-ad-crypto-lambda-layer/var/ --region ap-northeast-2 # 압축한 패키지를 s3에 업로드 한 후, lambda layer에 패키지를 등록할 때, s3 위치를 등록하면 됨
    (crypto-lib) $ deactivate
    </pre>

5. AWS Lambda Function을 이용해서 S3에 저장된 작은 파일들을 큰 파일로 합치기
    
    실시간으로 들어오는 데이터를 Kinesis Data Firehose를 이용해서 S3에 저장할 경우, 데이터 사이즈가 작은 파일들이 생성됩니다. Amazon Athena의 쿼리 성능 향상을 위해서 작은 파일들을 하나의 큰 파일로 합쳐주는 것이 좋습니다. 이러한 작업을 주기적으로 실행하기 위해서 Athena의 CTAS(Create Table As Select) 쿼리를 실행하는 AWS Lambda function 함수를 생성하고자 합니다.

    이러한 작업을 위해 먼저 Athena 콘솔에서 데이터베이스 및 테이블 생성을 해주어야 합니다. 아래의 명령어들을 Athena 콘솔 - 쿼리 편집기에서 실행하면 됩니다. 

    :warning: **{env} 자리에 환경 이름(dev, qa, prod 등)을 꼭 넣어줄 것!** 

    먼저 데이터베이스를 생성합니다. 환경별로 데이터베이스 이름을 구분하여 만들어줍니다. 
    ```sh
    CREATE DATABASE IF NOT EXISTS beluga_ad_action_database_{env};
    ```

    변환하기 전 원본 데이터가 있는 S3 위치를 참조하는 테이블을 생성합니다. S3 위치 이름과 데이터베이스 이름을 환경에 맞게 조정해줍니다. 
    ```sh
    CREATE EXTERNAL TABLE IF NOT EXISTS `beluga_ad_action_database_{env}.beluga_ad_action_raw` (
        `timestamp` string,
        `datetime` string,
        `userType` string,
        `userid` string,
        `storeId` string,
        `platform` string,
        `osVersion` string,
        `appVersion` string,
        `uuid` string,
        `screenName` string,
        `screenLabel` string,
        `referrer` string,
        `referrerLabel` string,
        `event` string,
        `groupIdx` string,
        `campaignIdx` string,
        `pageIdx` string,
        `selectionId` string,
        `selectionGroupId` string,
        `chargingType` string,
        `selectionTime` string,
        `bidPrice` string,
        `creativeIdx` string,
        `productIdx` string,
        `accountId` string,
        `wsIdx` string,
        `cdIdx` string,
        `unitIdx` string,
        `query` string,
        `rsIdx` string,
        `keywordIdx` string,
        `keyword` string,
        `exposureTime` string
    )
    PARTITIONED BY (
        `year` int,
        `month` int,
        `day` int,
        `hour` int
    )
    ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
    STORED AS INPUTFORMAT 'org.apache.hadoop.mapred.TextInputFormat' OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.IgnoreKeyTextOutputFormat'
    LOCATION 's3://ad-data-beluga-ad-action-{env}/json-data'
    ```

    CTAS 쿼리 결과를 저장하는 테이블을 생성합니다. 
    ```sh
    CREATE EXTERNAL TABLE IF NOT EXISTS `beluga_ad_action_database_{env}.ctas_beluga_ad_action_parquet`(
        `timestamp` string COMMENT '이벤트 발생 시간(GMT 기준) + 9 시간 계산',
        `datetime` string COMMENT '이벤트 발생 시간',
        `userType` string COMMENT '도/소매 여부 도매 : “W” 소매 : “R”',
        `userid` string COMMENT '유저 ID',
        `storeId` string COMMENT '',
        `platform` string COMMENT '접속 기기 구분',
        `osVersion` string COMMENT 'OS 버젼 (pc web은 빈 값으로 전송)',
        `appVersion` string COMMENT '앱 버젼 (pc web은 빈 값으로 전송)',
        `uuid` string COMMENT '(pc web은 빈 값으로 전송)',
        `screenName` string COMMENT '이벤트가 발생된 화면의 이름',
        `screenLabel` string COMMENT '이벤트가 발생된 화면의 코드 - 이제 사용하지 않기로 함, 이전에 있는 건 유지',
        `referrer` string COMMENT '이벤트가 발생 직전의 화면 이름',
        `referrerLabel` string COMMENT '이벤트가 발생 직전의 화면 코드',
        `event` string COMMENT '동작 분류 코드',
        `groupIdx` string COMMENT '광고 그룹 고유번호',
        `campaignIdx` string COMMENT '광고 캠페인 고유번호',
        `pageIdx` string COMMENT '지면 고유번호',
        `selectionId` string COMMENT '낙찰 고유 번호',
        `selectionGroupId` string COMMENT '낙찰그룹 ID (guid)',
        `chargingType` string COMMENT '과금 방식 (CPM/CPC)',
        `selectionTime` string COMMENT '낙찰 고유 번호',
        `bidPrice` string COMMENT '입찰가',
        `creativeIdx` string COMMENT '광고 소재 고유번호',
        `productIdx` string COMMENT '광고 상품 고유번호',
        `accountId` string COMMENT '광고주 계정키 (guid)',
        `wsIdx` string COMMENT '도매 ID',
        `cdIdx` string COMMENT '카테고리 고유번호',
        `unitIdx` string COMMENT '광고 유닛 고유번호',
        `query` string COMMENT '검색어',
        `rsIdx` string COMMENT '소매 고유번호',
        `keywordIdx` string COMMENT '키워드 고유 아이디',
        `keyword` string COMMENT '키워드',
        `exposureTime` string COMMENT '노출시간'
    )
    PARTITIONED BY (
        `year` int,
        `month` int,
        `day` int,
        `hour` int
    )
    ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
    STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat' OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
    LOCATION 's3://ad-data-beluga-ad-action-{env}/parquet-data'
    TBLPROPERTIES (
        'has_encrypted_data' = 'false',
        'parquet.compression' = 'SNAPPY'
    );
    ```

athena_ctas.py 로 만든 lambda 함수에서 위의 테이블들을 이용하여 작은 파일들을 큰 파일로 변환, parquet 포맷으로 변환 등의 작업을 하게 됩니다. 이 lambad 함수는 CDK 를 통해 자동으로 배포됩니다. 

### Useful Commands
- ```cdk ls``` list all stacks in the app
- ```cdk synth``` emits the synthesized CloudFormation template
- ```cdk deploy``` deploy this stack to your default AWS account/region
- ```cdk diff``` compare deployed stack with current state
- ```cdk docs``` open CDK documentation

## Deployment
CDK로 배포할 경우, 아래 아키텍처 그림의 1(a), 1(b), 1(c), 1(f), 2(b), 2(a)가 자동으로 생성됩니다.

![aws-analytics-system-build-steps](./assets/aws-analytics-system-build-steps.svg)

:white_check_mark: 아래 설명은 `sandbox` 계정 기준으로 되어 있습니다. 다른 계정에 배포할 때는 
`--profile {profile_name}` 을 수정하여 적용하면 됩니다. 또한 deploy 할 환경을 dev, qa, prod 중에 같이 선택하여 `-c deploy_env={env}` 와 같이
넣어주어야 합니다. 

1. [Getting Started With the AWS CDK](https://docs.aws.amazon.com/cdk/latest/guide/getting_started.html)를 참고해서 cdk를 설치하고,
aicd.sh 등의 스크립트를 실행하여 유저 정보를 `~/.aws/config`에 등록합니다.

    ```shell script
    $ ~/aicd.sh
    ...
    $ cat ~/.aws/config
    [profile deali-sandbox]
    aws_access_key_id=AKIAIOSFODNN7EXAMPLE
    aws_secret_access_key=wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY
    region=ap-northeast-2
    ```

2. Lambda Layer에 등록할 Python 패키지를 생성해서 s3 bucket에 저장한다.
에를 들어, elasticsearch 패키지를 Lambda Layer에 등록 할 수 있도록 `deali-ad-data-lambda-layer-packages`라는 이름의 s3 bucket을 생성 후, 아래와 같이 저장된 것을 확인합니다.

    ```shell script
    $ aws s3 ls s3://deali-ad-data-lambda-layer-packages/var/ --profile deali-sandbox
    2022-08-17 08:55:35    1449115 es-lib.zip
    ```

3. 소스 코드를 git에서 다운로드 받은 후, `S3_BUCKET_LAMBDA_LAYER_LIB` 라는 환경 변수에 lambda layer에 등록할 패키지가 저장된 s3 bucket 이름을
설정 한 후, `cdk deploy` 명령어를 이용해서 배포합니다.

    ```shell script
    $ git clone https://github.com/dealicious-inc/ad-data-pipeline.git
    $ cd ad-data-pipeline
    $ python3 -m venv .env
    $ source .env/bin/activate
    (.env) $ pip install -r requirements.txt
    (.env) $ cdk bootstrap -c deploy_env=dev --profile deali-sandbox
    (.env) $ export S3_BUCKET_LAMBDA_LAYER_LIB=deali-ad-data-lambda-layer-packages       # 저장 위치가 deali-ad-data-lambda-layer-packages 라면 생략가능
    (.env) $ cdk synth --profile deali-sandbox -c deploy_env=dev                         # 생략 가능
    (.env) $ cdk deploy --profile deali-sandbox -c deploy_env=dev
    ```

   :white_check_mark: `cdk bootstrap ...` 명령어는 CDK toolkit stack 배포를 위해 최초 한번만 실행 하고, 이후에 배포할 때는 CDK toolkit stack 배포 없이 `cdk deploy` 명령어만 수행하면 됩니다.

    ```shell script
    (.env) $ export S3_BUCKET_LAMBDA_LAYER_LIB=deali-ad-data-lambda-layer-packages
    (.env) $ cdk deploy --profile deali-sandbox -c deploy_env=dev
    ```

4. 엘라스틱 서치 index를 생성합니다.

   ```shell
   python3 src/main/python/utils/create_ad_data_index.py \
   --es-host 'vpc-ad-data-es-dev-6roglhwk4hzx2is4lo7zno5wme.ap-northeast-2.es.amazonaws.com' \
   --profile 'deali-sandbox'
   ```   
   > vpn 연결 필요

   `GET /_cat/indices` 로 인덱스 생성 확인

5. 배포한 애플리케이션을 삭제하려면, `cdk destroy` 명령어를 아래와 같이 실행 합니다.
    ```shell script
    (.env) $ cdk destroy --profile deali-sandbox -c deploy_env=dev
    ```
    실행 후에도 S3 버킷과 CloudWatch LogGroup 은 남아 있으므로, 관련된 모든 리소스를 제거하려면 수동으로 지우거나 아래 예제처럼 같이 제거되도록 설정해주어야 합니다. 
    ```python
    bucket = s3.Bucket(self, "MyFirstBucket",
    versioned=True,
    removal_policy=cdk.RemovalPolicy.DESTROY,
    auto_delete_objects=True)
    ```

## Test
Kinesis Streams 에 데이터를 흘려 보내서 테스트하려면 다음과 같이 스크립트를 실행합니다. 
    <pre>
    $ python3 ./src/main/python/ETL/etl_beluga_ad_action.py    # deali-sandbox 계정, dev 환경
    </pre>

[[Top](#top)]
