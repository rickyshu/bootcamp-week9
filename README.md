https://github.com/rickyshu/bootcamp-week9

# 데엔8기 이호인 9주차

현재 범위는 **Q1 실행 환경 구성**입니다. 이니셜은 `lhi`입니다. 이 저장소의 구성 파일은 EC2에 컨테이너를 만드는 입력 파일이며, 파일이 존재하는 것과 실제 컨테이너가 실행되는 것은 다릅니다. AWS에서 실행하고 화면을 확인하기 전까지 Q1 완료로 간주하지 않습니다.

## 구성과 실행 흐름

Ubuntu 22.04 EC2 한 대(`t3.large`, EBS 30GiB)에 `part1-ev/docker-compose.yaml`을 실행합니다. Compose 파일은 Kafka 브로커 3개, Kafka UI, Spark master·worker, Airflow 컨테이너를 정의합니다. 이름이 다른 컨테이너끼리는 Compose 내부 네트워크로 통신하며 Kafka 포트 9092와 Spark 제출 포트 7077은 외부에 공개하지 않습니다.

`part2-price/airflow/Dockerfile`은 실행 프로그램을 넣은 이미지의 제작법입니다. Airflow 공식 이미지에 Java 17, PySpark 3.5.3, SparkSubmitOperator를 추가하고 Spark master·worker도 같은 이미지를 사용합니다. 이렇게 Python 3.11과 Spark 버전을 통일합니다. Airflow는 한 번에 한 작업을 실행하는 SequentialExecutor와 SQLite를 사용하는 학습용 구성입니다. Q9의 세 작업은 순서대로 실행할 수 있고, 운영 환경의 다중 작업 처리 구성은 아닙니다.

Kafka는 ZooKeeper 대신 브로커에 포함된 KRaft controller를 사용합니다. 브로커마다 JVM 힙을 384MiB로 제한하고 Spark worker는 작업에 1536MiB·1코어를 제공합니다. `mem_limit`은 컨테이너 전체 메모리 상한이며 JVM 힙과 같은 값이 아닙니다. Q4 스트리밍과 Q9 배치를 동시에 실행하지 않습니다.

Kafka 컨테이너의 주기적인 상태 검사는 새 JVM을 띄우지 않고 내부 9092 포트 연결을 확인합니다. 실제 Kafka 프로토콜과 파티션·복제 상태는 시작 스크립트 마지막의 토픽 생성·describe 명령으로 별도 검증합니다.

S3·Athena 접근 권한은 EC2에 연결한 IAM 역할 `de9-ec2-lhi`에서 받습니다. 역할을 컨테이너까지 전달할 수 있도록 EC2의 IMDSv2를 활성화하고 응답 hop limit을 2로 설정합니다. `.env`는 리전·버킷명 같은 설정값을 담고 AWS 액세스 키는 담지 않습니다. Kafka 데이터와 Airflow DB·실행 로그는 Docker named volume에 저장되어 컨테이너 재시작 후에도 유지됩니다. `docker compose down -v`는 이 데이터를 지우므로 사용하지 않습니다.

## 준비

1. 관리자가 `../week9-iam/README.md`에 따라 `data_eng` 권한과 EC2 역할·인스턴스 프로파일을 준비합니다. 해당 문서는 로컬 작업 폴더의 관리용 자료이며 이 공개 저장소에는 포함하지 않습니다.
2. 과제 버킷은 `de9-lhi-947554757698`입니다. 생성 여부는 AWS에서 별도로 확인해야 합니다.
3. 서울 기본 VPC의 퍼블릭 서브넷에 EC2를 생성합니다. Ubuntu AMI, `t3.large`, 암호화된 gp3 EBS 30GiB, 인스턴스 프로파일 `de9-ec2-lhi`, IMDSv2 필수·hop limit 2를 적용합니다. 인스턴스·보안그룹·키 페어에 `Project=bootcamp-week9` 태그를 붙입니다.
4. 보안그룹은 TCP 22·4040·8080·8081·8082·8083을 **현재 본인 공인 IP의 /32**에만 허용합니다. 4040은 Airflow 컨테이너에서 Spark 작업의 driver가 실행되는 동안 열리는 Spark UI입니다.

브라우저로 보는 서비스는 Kafka UI `http://EC2_PUBLIC_IP:8080`, Spark master `:8081`, Spark worker `:8082`, Airflow `:8083`입니다. EC2를 중지 후 시작하면 공인 IP가 바뀔 수 있습니다. 웹 UI에서 입력하는 Airflow 사용자 이름은 `admin`이며 비밀번호는 첫 실행 시 컨테이너 안에 생성됩니다. 비밀번호를 Git에 저장하지 않습니다.

## EC2에서 설치·실행

아래는 EC2 Ubuntu 호스트에서 실행할 명령입니다. 로컬 실행 결과를 제출 증빙으로 사용하지 않습니다.

```bash
git clone https://github.com/rickyshu/bootcamp-week9.git
cd bootcamp-week9
bash scripts/prepare-ec2.sh
```

Docker 그룹 적용을 위해 SSH를 재접속하고 저장소로 이동합니다.

```bash
cd ~/bootcamp-week9
git pull --ff-only
bash scripts/start-q1.sh
```

`start-q1.sh`는 `.env.example`을 `.env`로 복사하고 이미지를 빌드한 뒤 컨테이너 상태가 정상화될 때까지 기다립니다. 마지막으로 `ev-charging-sessions` 토픽을 파티션 3개·복제 2로 생성하고 describe를 출력합니다. 이미 존재하는 토픽은 삭제하거나 초기화하지 않습니다. 기존 토픽의 파티션·복제 수가 다르면 출력을 확인하고 원인을 수정해야 합니다.

Airflow 비밀번호는 캡처 화면 밖에서만 확인합니다.

```bash
docker compose -f part1-ev/docker-compose.yaml exec airflow \
  cat /opt/airflow/standalone_admin_password.txt
```

Spark 제출 기능은 EC2에서 다음 소규모 실제 분산 작업으로 확인할 수 있습니다. 결과 `10`은 이 확인 프로그램의 예상값이며, Q2 이후 제공 데이터의 처리 결과를 대신하지 않습니다.

```bash
docker compose -f part1-ev/docker-compose.yaml exec -T airflow \
  python /opt/de9/scripts/verify-spark-submit.py
```

이 명령은 `SparkSubmitOperator`를 직접 호출하여 실제 Spark 워커에서 계산하고 EC2 역할로 S3 버킷을 조회합니다. Q9의 DAG 스케줄 실행을 대신하는 증빙은 아닙니다.

## Q1 캡처

코드를 문항 단위로 커밋·push한 뒤 EC2에서 pull하고 아래 명령을 실행합니다. `date`, `hostname`, `git rev-parse HEAD`는 **EC2 호스트에서** 실행합니다. 터미널 캡처의 커밋 해시는 GitHub에 올라간 Q1 코드 커밋이어야 합니다.

```bash
date && hostname && git rev-parse HEAD && aws sts get-caller-identity
date && hostname && git rev-parse HEAD && docker ps
date && hostname && git rev-parse HEAD && bash scripts/create-ev-topic.sh
```

캡처 위치는 `part1-ev/capture/Q1/`입니다. 캡처 이미지와 입력 CSV는 공개 저장소에 올리지 않고 최종 ZIP에 별도로 포함합니다. 아래 이름은 필요한 파일 목록이며 아직 이미지가 생성되었다는 뜻이 아닙니다.

| 파일 | 실제 화면에서 확인할 내용 |
|---|---|
| `ec2_instance.png` | 서울 리전 EC2 → 인스턴스 → de9-lhi 선택 → 인스턴스 요약/세부 정보. Running, 인스턴스 ID, t3.large, 프라이빗 IP, IAM 역할이 보이게 촬영 |
| `security_group.png` | EC2 → 보안 그룹 → de9-lhi → 인바운드 규칙. TCP 22·4040·8080·8081·8082·8083과 각 소스의 본인 IP /32가 보이게 촬영 |
| `iam_role.png` | IAM → 역할 → de9-ec2-lhi → 권한. Week9RuntimeLhi 정책을 펼쳐 S3·Athena·Glue 권한과 역할 이름이 보이게 촬영. 정책 본문 조회가 제한되면 같은 계정의 관리자 콘솔에서 촬영 |
| `iam_role_check.png` | 실행 지문과 STS 결과: 역할 이름·세션 인스턴스 ID |
| `docker_ps.png` | 실행 지문과 Kafka 3개·UI·Spark master/worker·Airflow 실행 상태 |
| `topic_describe.png` | 실행 지문과 PartitionCount 3·ReplicationFactor 2, 각 파티션의 ISR |

`scripts/verify-q1.sh`의 출력은 진단 보조용이며 위 화면 캡처를 대체하지 않습니다. EC2·IAM 콘솔 화면은 CLI 출력으로 대체할 수 없습니다. 미실행 상태에서 성공 캡처나 예시 결과를 만들지 않습니다.

터미널 캡처는 로컬 터미널에서 SSH 접속한 **EC2 셸**에서 위 세 명령을 각각 실행한 화면입니다. 호스트명 `ip-프라이빗-IP`와 EC2 콘솔에 나온 프라이빗 IP가 대응해야 합니다. 날짜·호스트명·Git 커밋 해시·실행 명령·결과가 한 화면에 들어가도록 창을 넓힙니다. `docker ps`가 너무 넓으면 `docker ps --format 'table {{.Names}}\t{{.Image}}\t{{.Status}}'`를 사용해도 실제 컨테이너 이름·이미지·실행 상태를 확인할 수 있습니다.

## 실제 실행 확인 기록

2026-11-23 EC2에서 Q1 구성 코드 커밋 `1a9afcf`를 실행했습니다. 아래는 CLI·SSH로 실제 확인한 내용이고, 제출용 화면 캡처는 별도로 필요합니다.

- Kafka 3개, Kafka UI, Spark master·worker, Airflow가 실행되고 상태 검사가 통과했습니다. Kafka UI는 별도 Docker healthcheck가 없는 서비스이며, 기동 후 HTTP health API를 추가 확인합니다.
- `ev-charging-sessions`: 파티션 3개, 복제 2, 각 파티션 ISR 2개를 확인했습니다. Q2 데이터를 전송하지 않은 빈 토픽입니다.
- `SparkSubmitOperator`가 Spark standalone 워커에 작업을 제출했습니다. 실제 분산 계산 결과는 `10`, Python 작업의 실행 호스트는 `spark-worker`, 워커 Python은 `3.11`이었습니다.
- 같은 작업에서 S3A로 과제 버킷의 목록을 조회했고 EC2 역할 인증으로 성공했습니다. Q4·Q5의 데이터 적재를 실행한 것은 아닙니다.
- 첫 Airflow 웹서버 초기화가 완료되지 않아 재시작으로 정상화했고, 초기화 여유를 위해 master·worker 시작 제한 시간을 300초로 설정했습니다.

과제 중단 시 EC2를 **Stop**합니다. EBS 저장 요금은 중지 상태에도 남습니다. 다시 시작할 때 공인 IP와 보안그룹의 본인 IP를 확인합니다.

## 참고

- 과제 원문: [9주차 문제지](https://drive.google.com/file/d/1PpJEN2VyGTrbmo4B6qU34ImX7Gcb168P/view), 1~8쪽. 문제지 자체는 공개 저장소에 복제하지 않습니다.
- [Kafka 공식 Docker 예제](https://github.com/apache/kafka/blob/3.9.1/docker/examples/docker-compose-files/cluster/combined/plaintext/docker-compose.yml)
- [Airflow 이미지 확장](https://airflow.apache.org/docs/docker-stack/build.html)
- [EC2 역할 사용](https://docs.aws.amazon.com/IAM/latest/UserGuide/id_roles_use_switch-role-ec2.html)
- [컨테이너의 IMDS 설정](https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/configuring-instance-metadata-options.html)
