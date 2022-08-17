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
### Useful Commands
- ```cdk ls``` list all stacks in the app
- ```cdk synth``` emits the synthesized CloudFormation template
- ```cdk deploy``` deploy this stack to your default AWS account/region
- ```cdk diff``` compare deployed stack with current state
- ```cdk docs``` open CDK documentation

## Deployment
CDK로 배포할 경우, 아래 아키텍처 그림의 1(a), 1(b), 1(c), 1(f), 2(b), 2(a)가 자동으로 생성됩니다.

![aws-analytics-system-build-steps](./assets/aws-analytics-system-build-steps.svg)

