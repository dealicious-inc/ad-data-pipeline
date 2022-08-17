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
      (es-lib) $ aws s3 mb s3://my-bucket-for-lambda-layer-packages # 압축한 패키지를 업로드할 s3 bucket을 생성함
      (es-lib) $ aws s3 cp es-lib.zip s3://my-bucket-for-lambda-layer-packages/var/ # 압축한 패키지를 s3에 업로드 한 후, lambda layer에 패키지를 등록할 때, s3 위치를 등록하면 됨
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
      $ aws s3 mb s3://my-bucket-for-lambda-layer-packages
      $ aws s3 cp es-lib.zip s3://my-bucket-for-lambda-layer-packages/var/
      ```

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
에를 들어, elasticsearch 패키지를 Lambda Layer에 등록 할 수 있도록 `deali-ad-data-lambda-layer-packages`라는 이름의 s3 bucket을 생성 후, 아래와 같이 저장합니다.

    ```shell script
    $ aws s3 ls s3://deali-ad-data-lambda-layer-packages/var/ --profile deali-sandbox
    2022-08-17 08:55:35    1449115 es-lib.zip
    ```

3. 소스 코드를 git에서 다운로드 받은 후, `S3_BUCKET_LAMBDA_LAYER_LIB` 라는 환경 변수에 lambda layer에 등록할 패키지가 저장된 s3 bucket 이름을
설정 한 후, `cdk deploy` 명령어를 이용해서 배포합니다.

    ```shell script
    $ git clone https://github.com/aws-samples/aws-analytics-immersion-day.git
    $ cd aws-analytics-immersion-day
    $ python3 -m venv .env
    $ source .env/bin/activate
    (.env) $ pip install -r requirements.txt
    (.env) $ cdk bootstrap --profile deali-sandbox
    (.env) $ export S3_BUCKET_LAMBDA_LAYER_LIB=deali-ad-data-lambda-layer-packages
    (.env) $ cdk --profile deali-sandbox deploy -c deploy_env=dev
    ```

   :white_check_mark: `cdk bootstrap ...` 명령어는 CDK toolkit stack 배포를 위해 최초 한번만 실행 하고, 이후에 배포할 때는 CDK toolkit stack 배포 없이 `cdk deploy` 명령어만 수행하면 됩니다.

    ```shell script
    (.env) $ export S3_BUCKET_LAMBDA_LAYER_LIB=deali-ad-data-lambda-layer-packages
    (.env) $ cdk --profile deali-sandbox deploy -c deploy_env=dev
    ```

4. 배포한 애플리케이션을 삭제하려면, `cdk destroy` 명령어를 아래와 같이 실행 합니다.
    ```shell script
    (.env) $ cdk --profile deali-sandbox destroy -c deploy_env=dev
    ```

\[[Top](#top)\]
