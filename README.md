https://github.com/rickyshu/bootcamp-week9

# 데엔8기 이호인 9주차

현재 **Q1~Q7 구현·실제 실행·검증 및 캡처를 완료**했고, **Q8 구현·실제 실행·검증을 완료**했습니다. Q8 제출용 화면 캡처는 별도로 저장해야 합니다. 이니셜은 `lhi`입니다. 이 저장소의 구성 파일은 EC2에 컨테이너를 만드는 입력 파일이며, 파일이 존재하는 것과 실제 컨테이너가 실행되는 것은 다릅니다. AWS에서 실행하고 화면을 확인하기 전까지 문항 완료로 간주하지 않습니다.

## 구성과 실행 흐름

Ubuntu 22.04 EC2 한 대(`t3.large`, EBS 30GiB)에 `part1-ev/docker-compose.yaml`을 실행합니다. Compose 파일은 Kafka 브로커 3개, Kafka UI, Spark master·worker, Airflow 컨테이너를 정의합니다. 이름이 다른 컨테이너끼리는 Compose 내부 네트워크로 통신하며 Kafka 포트 9092와 Spark 제출 포트 7077은 외부에 공개하지 않습니다.

`part2-price/airflow/Dockerfile`은 실행 프로그램을 넣은 이미지의 제작법입니다. Airflow 공식 이미지에 Java 17, PySpark 3.5.3, SparkSubmitOperator를 추가하고 Spark master·worker도 같은 이미지를 사용합니다. 이렇게 Python 3.11과 Spark 버전을 통일합니다. Airflow는 한 번에 한 작업을 실행하는 SequentialExecutor와 SQLite를 사용하는 학습용 구성입니다. Q9의 세 작업은 순서대로 실행할 수 있고, 운영 환경의 다중 작업 처리 구성은 아닙니다.

Kafka는 ZooKeeper 대신 브로커에 포함된 KRaft controller를 사용합니다. 브로커마다 JVM 힙을 384MiB로 제한하고 Spark worker는 작업에 1536MiB·1코어를 제공합니다. `mem_limit`은 컨테이너 전체 메모리 상한이며 JVM 힙과 같은 값이 아닙니다. Q4 스트리밍과 Q9 배치를 동시에 실행하지 않습니다.

Airflow에는 웹서버·스케줄러뿐 아니라 Spark 제출 시 driver JVM도 뜹니다. 실제 제출 점검에서 기존 1792MiB 상한에 걸려 웹서버가 종료되어 Airflow 상한을 2560MiB로 늘리고 Spark worker 컨테이너 상한을 2048MiB로 조정했습니다. Spark executor 메모리는 1GiB로 유지합니다. 이 상한은 예약 메모리가 아니며, 같은 8GiB 호스트에서 모든 컨테이너가 동시에 상한까지 쓸 수 있다는 뜻은 아닙니다.

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

2026-11-23 EC2에서 최종 Q1 구성 코드 커밋 `c7294f5`를 실행했습니다. 아래는 CLI·SSH로 실제 확인한 내용이고, 제출용 화면 캡처는 별도로 필요합니다.

- Kafka 3개, Kafka UI, Spark master·worker, Airflow가 실행되고 상태 검사가 통과했습니다. Kafka UI는 별도 Docker healthcheck가 없는 서비스이며, HTTP health API의 `UP` 응답을 확인했습니다.
- `ev-charging-sessions`: 파티션 3개, 복제 2, 각 파티션 ISR 2개를 확인했습니다. Q2 데이터를 전송하지 않은 빈 토픽입니다.
- `SparkSubmitOperator`가 Spark standalone 워커에 작업을 제출했습니다. 실제 분산 계산 결과는 `10`, Python 작업의 실행 호스트는 `spark-worker`, 워커 Python은 `3.11`이었습니다.
- 같은 작업에서 S3A로 과제 버킷의 목록을 조회했고 EC2 역할 인증으로 성공했습니다. Q4·Q5의 데이터 적재를 실행한 것은 아닙니다.
- EC2 역할로 Athena 워크그룹 `de9-lhi`의 설정 조회도 성공했습니다. 실제 분석 SQL 실행은 Q5·Q9에서 진행합니다.
- 최종 분산 작업 직후 Airflow DB·스케줄러·triggerer health API가 모두 healthy였고, 메모리 종료 카운터는 0이었습니다. Airflow 컨테이너의 관찰된 메모리 최고 사용량은 약 2.12GiB로 조정된 2.5GiB 상한 안에 들어왔습니다.
- 첫 Airflow 웹서버 초기화가 완료되지 않아 재시작으로 정상화했고, 초기화 여유를 위해 master·worker 시작 제한 시간을 300초로 설정했습니다.

과제 중단 시 EC2를 **Stop**합니다. EBS 저장 요금은 중지 상태에도 남습니다. 다시 시작할 때 공인 IP와 보안그룹의 본인 IP를 확인합니다.

## Q2: CSV를 Kafka로 한 번 전송

Producer는 Kafka 라이브러리의 Python 객체입니다. `part1-ev/charger_producer.py`를 실행한 프로세스 안에서 생성되고, 원본 CSV를 읽어 Kafka 브로커에 네트워크 요청을 보냅니다. CSV 파일이 Kafka 토픽 자체가 되는 것은 아닙니다. 전송 후에는 JSON 메시지가 브로커의 로그와 Docker 데이터 볼륨에 남습니다.

원본 `data/charger_sessions.csv`는 공유 Drive의 제공 파일을 수정하지 않고 EC2에 별도로 복사합니다. 175행, 21,583바이트이며 SHA256은 `6ffb4fafd1fcc4e42ac21d2fa02869e309eb48800ac06719eafaf5622a4a32f8`입니다. 원본 CSV는 공개 Git에 포함하지 않고 최종 제출 ZIP의 data 폴더에 포함합니다.

코드는 `startTime`을 날짜·시각 값으로 읽어 정렬하고 행 사이에 0.1초를 기다립니다. 각 메시지의 key는 `stationName`의 UTF-8 바이트이고 value는 요구된 10개 필드를 가진 JSON입니다. `portNumber`는 정수, `energyKwh`는 실수, 날짜·기간은 원본 문자열을 사용합니다. 같은 key는 기본 파티셔너의 해시로 같은 파티션에 들어갑니다. 서로 다른 파티션 사이에는 Kafka가 전체 시간순 소비를 보장하지 않습니다.

`acks=all`은 현재 동기화된 복제본의 확인을 기다리고, `enable_idempotence=True`는 같은 Producer 세션의 내부 재시도로 생길 중복을 방지합니다. 이 라이브러리 버전에서는 `max_in_flight_requests_per_connection=1`을 함께 사용합니다. 애플리케이션을 다시 실행해서 같은 175행을 다시 send하는 것까지 중복 제거하지는 않습니다. 이 때문에 기존 토픽의 오프셋이 모두 0인 새 빈 토픽에서만 전송하도록 보호합니다. `try...finally`에서 flush와 close를 보장하고, 로그의 ACK 건수는 실제 브로커 확인을 받은 건수입니다.

EC2에서 CSV만 먼저 확인하려면 다음 명령을 사용합니다. 이 모드에서는 Kafka 연결과 send를 수행하지 않습니다.

```bash
docker compose -f part1-ev/docker-compose.yaml exec -T airflow \
  python /opt/de9/part1-ev/charger_producer.py --validate-only
```

실제 전송은 **한 번만** 실행합니다. 코드를 커밋·push하고 EC2에서 pull한 뒤 EC2 호스트에서 실행 지문과 함께 실행합니다.

```bash
date && hostname && git rev-parse HEAD && bash scripts/run-q2.sh
```

이미 한 번 성공한 명령은 캡처를 위해 다시 실행하지 않습니다. 실제 실행 터미널 세션을 유지하면 나중에 같은 화면을 캡처할 수 있습니다. 전송 기록은 EC2의 `.runtime/q2-producer.log`에도 보관하지만, 이 파일은 실제 실행 화면 캡처와 Kafka UI 캡처를 대신하지 않습니다. 실패 후 일부만 전송된 경우에도 자동으로 재전송하거나 토픽을 삭제하지 않습니다. 후속 단계·체크포인트와 함께 원인을 확인하고 문제지의 초기화 절차를 따릅니다.

읽기 전용 검증은 다음 명령으로 수행합니다. 검증용 Consumer는 그룹에 가입하거나 오프셋을 커밋하지 않아 Q3 그룹 실험에 영향을 주지 않습니다. 원본 전체 행과 Kafka JSON의 값·중복 횟수, key, 파티션별 시간순서, station별 파티션 일치를 대조합니다.

```bash
docker compose -f part1-ev/docker-compose.yaml exec -T airflow \
  python /opt/de9/scripts/verify-q2.py
```

필수 캡처는 `part1-ev/capture/Q2/producer_run.png`(실행 지문과 실제 전송 결과), `kafka_ui_topic.png`(Kafka UI의 ev-charging-sessions 토픽과 메시지 수 175)입니다. Kafka UI에서 메시지를 새로 보내거나 삭제하지 않습니다.

### Q2 실제 실행과 캡처

2026-11-23 12:12:54 UTC(한국 21:12:54)에 공개 커밋 `9f52fb8`로 EC2에서 Producer를 한 번 실행했습니다. ACK 175건, 파티션별 0=76·1=35·2=64건이었습니다. 원본 CSV 전체와 Kafka 메시지 전체의 값·중복 횟수가 일치했고 23개 station의 key·파티션 일치, 파티션별 시간순서 검증도 통과했습니다. Kafka UI의 읽기 API에서도 각 파티션의 시작 오프셋 0, 끝 오프셋 76·35·64와 ISR 2개를 확인했습니다. 브로커를 실제로 중단하는 장애 주입 실험은 수행하지 않았습니다.

실제 실행 터미널은 EC2의 `de9-q2` tmux 세션에 남겨 두었습니다. tmux는 서버에서 셸 화면을 유지하는 터미널 프로그램입니다. SSH 접속 후 아래 명령으로 기존 실행 화면에 연결합니다. Producer를 재실행하거나 저장 로그를 실행 결과처럼 꾸미는 명령이 아닙니다.

```bash
tmux attach -t de9-q2
```

창을 넓혀 실행 명령·날짜·호스트명·커밋 해시·`SUCCESS: 175 messages acknowledged`까지 보이게 `producer_run.png`를 찍습니다. 캡처 후 Ctrl+B를 누르고 손을 뗀 다음 D를 누르면 화면에서 분리됩니다. 세션을 종료하거나 전송 명령을 다시 입력하지 않습니다.

`kafka_ui_topic.png`는 브라우저에서 EC2 공인 IP의 8080 포트 → 클러스터 `de9-lhi` → Topics → `ev-charging-sessions`를 열고 토픽 이름·메시지 수 175·파티션 3·복제 2가 보이게 촬영합니다. 이 저장소에 원본 CSV·키·캡처·실행 로그를 공개하지 않습니다.

공식 동작 근거: [kafka-python 2.3.2 KafkaProducer](https://kafka-python.readthedocs.io/en/2.3.2/apidoc/KafkaProducer.html).

## Q3: 두 Consumer와 수동 오프셋 커밋

Consumer는 Kafka를 읽는 Python 객체이며, 이 실험에서는 같은 EC2의 Airflow 컨테이너 안에서 별도의 Python 프로세스 두 개를 실행합니다. 컨테이너나 EC2를 두 대 만드는 것이 아닙니다. 두 프로세스의 client ID는 다르고 group ID는 `ev-consumer-lhi`로 같습니다. 그룹의 멤버와 파티션 할당·커밋 오프셋은 Kafka 브로커가 관리합니다.

Q2의 기존 메시지 175건을 그대로 사용합니다. 그룹은 `subscribe()`로 동적으로 참여합니다. `assign()`으로 파티션을 수동 고정하지 않습니다. A가 먼저 참여할 때의 할당과 B 합류 후의 revoked/assigned 콜백을 실제 로그에 출력해 리밸런싱을 관찰합니다.

첫 Consumer가 모든 데이터를 먼저 읽지 않도록 할당 직후 `pause()`하고, `poll()`로 그룹 참여를 유지합니다. 두 멤버의 파티션 분담이 안정화된 것을 확인한 뒤 호스트의 `.runtime/q3/start` 파일을 생성하면 소비를 시작합니다. 이 파일은 레코드를 담지 않는 실행 제어용 신호입니다.

`enable_auto_commit=False`로 자동 커밋을 끕니다. 각 JSON의 endedBy 집계가 끝난 다음 **해당 레코드 offset+1**만 동기 커밋합니다. 예를 들어 offset 7 처리 후 커밋 8은 다음 읽을 위치를 뜻하며, 8번 레코드를 이미 처리했다는 뜻은 아닙니다. 미리 읽어 둔 다른 레코드의 poll 위치 전체를 커밋하지 않습니다.

호스트 터미널의 두 창에서 실행할 명령은 다음과 같습니다. 기존 그룹을 초기화하거나 Producer를 재실행하지 않습니다.

```bash
date && hostname && git rev-parse HEAD && bash scripts/run-q3-consumer.sh a
date && hostname && git rev-parse HEAD && bash scripts/run-q3-consumer.sh b
```

A가 먼저 그룹에 가입한 상태에서 B를 시작하고 두 멤버가 안정화된 뒤, 별도 EC2 호스트 셸에서 시작 신호를 생성합니다.

```bash
touch .runtime/q3/start
```

각 Consumer는 자신의 파티션을 모두 처리하면 3종 endedBy 건수·총합·커밋 위치를 출력합니다. 이후에도 pause/poll을 유지하므로 Kafka UI 캡처 시 member 2개가 남습니다. 실제 offset별 처리 기록은 컨테이너 `/tmp/de9-q3-a.json`, `/tmp/de9-q3-b.json`에 저장합니다. 읽기 전용 `scripts/verify-q3.py`가 두 보고서를 원본 CSV 및 토픽의 실제 offset 범위와 대조해 중복·누락과 집계 불일치를 확인합니다.

필수 캡처는 `part1-ev/capture/Q3/endedby_counts.png`(두 실제 실행 화면의 지문·각 endedBy 건수), `consumer_group_lag.png`(Kafka UI의 `ev-consumer-lhi`, member 2개, 각 파티션 current-offset·LAG)입니다. 집계 후 lag=0인 화면도 정상 결과입니다. 캡처 전에는 Consumer를 종료하거나 오프셋을 리셋하지 않습니다.

캡처가 끝난 뒤 두 Consumer를 정상 종료하려면 별도 호스트 셸에서 `touch .runtime/q3/stop`을 실행합니다. 종료하면 멤버 수가 줄어드므로 member 2개 캡처를 먼저 완료해야 합니다. 과제의 오프셋 리셋은 Q10에서 별도로 수행합니다.

### Q3 실제 실행과 캡처

2026-11-23 EC2에서 공개 코드 커밋 `356742d`로 A를 먼저 참여시키고 B를 합류시켰습니다. A의 파티션 `[0,1,2]`가 revoked되고 `[0,1]`이 재할당되는 콜백을 관찰했습니다. B는 `[2]`를 할당받았습니다. 실제 그룹이 STABLE·member 2개인 것을 확인한 뒤 start 신호를 생성했습니다.

| 실제 처리 결과 | Consumer A | Consumer B | 합계 |
|---|---:|---:|---:|
| CPS Server | 1 | 1 | 2 |
| Customer | 12 | 6 | 18 |
| Plug Out at Vehicle | 98 | 57 | 155 |
| 전체 | 111 | 64 | 175 |

두 보고서를 검증해 175개 `(partition,offset)`이 중복·누락 없이 처리됐고 원본 CSV의 endedBy 건수와 일치함을 확인했습니다. Kafka UI의 실제 읽기 API에서는 member 2개·STABLE·전체 lag 0, 파티션 0·1·2의 current-offset=76·35·64와 각각 lag 0을 확인했습니다.

실제 실행 화면은 EC2의 `de9-q3` tmux 세션에 두 창을 나란히 배치해 보존하고 있습니다. SSH 접속한 터미널에서 아래 명령으로 연결하고, 창을 넓혀 양쪽의 실행 지문·리밸런싱·각 endedBy 집계를 함께 `endedby_counts.png`로 촬영합니다. 프로그램은 재실행하지 않습니다.

```bash
tmux attach -t de9-q3
```

Ctrl+B를 누르고 손을 뗀 뒤 D를 눌러 화면에서만 분리합니다. Consumer는 종료하지 않습니다. Kafka UI에서 Consumers → `ev-consumer-lhi` 상세 화면을 열고 그룹명·member 2개·각 파티션의 current-offset·LAG가 보이게 `consumer_group_lag.png`를 찍습니다. 캡처가 끝나기 전에는 stop 파일을 생성하거나 EC2를 중지하지 않습니다. 실제 보고서·리밸런싱 로그·API 응답도 호스트 `.runtime/q3/`에 보관했고 공개 Git에서는 제외합니다.

공식 동작 근거: [kafka-python 2.3.2 KafkaConsumer](https://kafka-python.readthedocs.io/en/2.3.2/apidoc/KafkaConsumer.html).

## Q4: Kafka 스트림을 S3 Bronze/Silver에 저장

`part1-ev/charger_stream.py`는 Spark driver가 실행하는 Python 프로그램입니다. `spark.readStream`으로 만든 DataFrame은 Kafka에서 읽을 방법을 정의한 실행 계획이며, `writeStream.start()`를 호출하면 실제 스트리밍 쿼리가 시작됩니다. Spark worker의 executor가 데이터를 읽고 변환·저장합니다. Q3의 Python Consumer와 별개의 실행이며, `ev-consumer-lhi` 그룹을 재사용하거나 리셋하지 않습니다.

이 프로그램은 명시적인 10개 필드 JSON 스키마로 Q2의 기존 메시지를 해석합니다. 같은 입력 계획에서 세 쿼리를 시작합니다. Bronze와 Silver는 각각 `foreachBatch`로 매 트리거의 DataFrame을 Parquet 파일로 저장하고, 세 번째 쿼리는 `locationGroup`별 누적 건수를 complete 모드의 Spark console sink로 출력합니다. 충전소 이름은 `truncate=false`로 잘리지 않게 출력합니다. CSV를 직접 읽어 스트리밍 입력을 대신하지 않습니다.

Bronze의 원본 10개 값과 자료형은 그대로 유지합니다. Silver에서는 예를 들어 `PALO ALTO CA / CAMBRIDGE #2`의 끝 ` #2`만 제거해 `locationGroup`을 만들고, `1:46:26`을 6386초로 바꿔 `totalDuration`에 저장합니다. `chargingDuration`도 초 단위 정수로 바꾸며 두 값의 차이를 `idleSec`에 저장합니다. 날짜·시간 문자열과 `stationName` 원본은 유지합니다. 기간은 시계가 아니므로 24시간 이상도 계산할 수 있습니다.

S3 경로의 `dt`는 처리한 오늘 날짜가 아니라 이벤트의 `startTime` 날짜입니다. 이 CSV의 예상 파티션은 `dt=2017-06-06`입니다. 각 마이크로배치는 그 아래 `batch_id=00000000/` 같은 별도 디렉터리에 저장합니다. 동일 배치 재시도는 자기 배치 경로만 덮어써 중복 append를 피합니다. 다른 배치의 결과를 덮어쓰지 않습니다. 이 방식은 Spark의 원자적 파일 sink나 트랜잭션 저장소와 같지 않아 장애 중에는 배치 파일이 일시적으로 불완전할 수 있으며, 성공한 재시도와 검증이 필요합니다.

세 쿼리는 각각 `s3://버킷/checkpoints/q4/{bronze,silver,counts}/`에 처리 위치와 집계 상태를 남깁니다. **체크포인트와 출력 경로를 함께 보존**해야 합니다. 출력만 남기고 체크포인트를 지우거나 실행 옵션·변환 계획을 바꿔 재생하면 잘못된 결과가 생길 수 있습니다. 첫 실행의 `startingOffsets=earliest`는 이미 입력된 175건을 처음부터 읽기 위한 설정이고, 재개 시에는 체크포인트의 위치를 따릅니다.

### 실행과 읽기 전용 검증

코드를 커밋·push한 뒤 EC2에서 pull합니다. EC2 재시작 후 기존 Docker 볼륨을 유지한 상태에서 서비스를 시작하고 Kafka에 기존 175건이 남았는지 먼저 확인합니다. Q2 Producer는 재실행하지 않습니다. Kafka 커넥터는 설치된 Spark와 맞춘 `spark-sql-kafka-0-10_2.12:3.5.3`을 spark-submit의 `--packages`로 추가합니다.

EC2 호스트에서 실행 지문과 함께 아래 명령을 실행합니다.

```bash
date && hostname && git rev-parse HEAD && bash scripts/run-q4.sh
```

기본 20초 트리거와 트리거당 약 60건의 상한으로 기존 입력을 여러 번에 나눠 처리합니다. 세 쿼리는 독립적으로 진행하므로 같은 시점에 출력되는 Bronze/Silver 배치 번호와 console 번호가 반드시 일치하지는 않습니다. `--drain` 모드는 실제 스트리밍 쿼리에 `processAllAvailable()`을 호출해 현재 입력을 모두 처리한 뒤 정상 종료합니다. Kafka에 가짜 이벤트를 추가하지 않습니다. 일반적인 계속 실행은 `charger_stream.py`에서 `--drain`을 생략하면 되며, Q9 배치와 동시에 실행하지 않습니다.

S3에 쓰인 결과 전체의 검증은 EC2에서 별도 Spark 작업으로 수행합니다. 이 검증은 Kafka 그룹이나 S3 데이터를 변경하지 않습니다.

```bash
docker compose -f part1-ev/docker-compose.yaml exec -T airflow \
  spark-submit --conf spark.driver.host=airflow /opt/de9/scripts/verify-q4.py
```

검증 코드는 실제 S3 Parquet를 다시 읽어 원본 CSV 175행 전체의 값·중복 횟수, Silver의 초 변환·idleSec·locationGroup, 날짜 파티션과 설치구역별 합계를 대조합니다. CSV에서 계산한 예상 설치구역 수는 10개이며, 실제 적재 검증을 수행하기 전에는 성공 결과로 간주하지 않습니다.

### Q4 캡처

`part1-ev/capture/Q4/`에 아래 세 이미지를 보관합니다. 실제 집계 출력은 `.runtime/q4/stream.log`, 시작·진단 로그는 `.runtime/q4/spark-stderr.log`에 따로 저장됩니다. 화면에는 실제 stdout 집계가 그대로 나오고 실패하면 진단 로그 끝부분도 표시합니다. 로그 파일은 캡처를 대체하지 않습니다.

| 파일 | 촬영할 실제 화면 |
|---|---|
| `spark_streaming.png` | EC2 호스트의 실행 지문과 Spark console sink의 연속 2~3개 Batch 누적 집계. 전체 설치구역 이름이 보이게 촬영 |
| `s3_bronze.png` | AWS S3 웹 콘솔 → 본인 버킷 → `bronze/ev-charging-sessions/dt=2017-06-06/` 아래 배치별 Parquet 객체 목록. 필요하면 객체 검색으로 하위 경로까지 표시 |
| `s3_silver.png` | 같은 버킷의 `silver/ev-charging-sessions/dt=2017-06-06/` 아래 배치별 Parquet 객체 목록 |

S3 캡처는 사용자가 실제 웹 콘솔에서 촬영합니다. CLI의 객체 목록이나 검증 출력으로 대신하지 않습니다. 코드 공개 저장소에는 CSV·키·캡처·실행 로그를 포함하지 않습니다.

### Q4 실제 실행 확인 기록

2026-11-24 07:57:39 한국 시각(2026-11-23 22:57:39 UTC)에 공개 코드 커밋 `6550109`로 동일 EC2에서 실행했습니다. 실행 전 Kafka 175건을 원본과 대조했고, 실제 Spark standalone worker에서 세 스트리밍 쿼리를 수행했습니다. Batch 0·1·2가 각각 59·58·58건을 읽었으며 Bronze/Silver 각각 Parquet 3개가 `dt=2017-06-06/batch_id=.../` 아래 생성됐습니다. 현재 입력 처리 후 세 쿼리는 정상 종료했습니다.

별도 Spark 검증 작업으로 실제 S3 Parquet를 다시 읽어 원본 175행의 모든 값과 중복 횟수, Silver의 초 단위 정수 자료형·idleSec·locationGroup, 날짜 파티션과 집계를 검증했습니다. 모두 통과했습니다. 최종 설치구역별 건수는 아래와 같습니다.

| locationGroup | 실제 건수 |
|---|---:|
| PALO ALTO CA / BRYANT | 18 |
| PALO ALTO CA / CAMBRIDGE | 21 |
| PALO ALTO CA / HAMILTON | 16 |
| PALO ALTO CA / HIGH | 44 |
| PALO ALTO CA / MPL | 22 |
| PALO ALTO CA / RINCONADA LIB 1 | 4 |
| PALO ALTO CA / RINCONADA LIB 2 | 4 |
| PALO ALTO CA / RINCONADA LIB 3 | 5 |
| PALO ALTO CA / TED THOMPSON | 11 |
| PALO ALTO CA / WEBSTER | 30 |

Q3 그룹은 Consumer 종료 상태(member 0·EMPTY)이며 커밋 오프셋 0=76·1=35·2=64가 그대로 유지됐습니다. Q4는 Q3 그룹을 리셋하지 않았습니다. 실행 stdout·진단 로그·검증 결과·S3 객체 목록·Q3 그룹 확인 JSON을 EC2 및 로컬 `.runtime/q4/`에 보관합니다. 실제 실행 터미널은 EC2의 `de9-q4` tmux 세션에 남겨 두었으며 아래 명령으로 연결합니다. **스트리밍을 재실행하지 않습니다.**

```bash
tmux attach -t de9-q4
```

SSH 재접속 후 창을 크게 열고 글자 크기를 줄여 실행 지문과 3개 Batch 표를 함께 촬영합니다. 필요하면 Ctrl+B를 누르고 손을 뗀 뒤 `[`로 스크롤 모드에 들어가 PageUp으로 실제 실행 화면 위쪽을 봅니다. `q`로 스크롤 모드를 끝내고 Ctrl+B를 누른 뒤 D로 화면에서 분리합니다. tmux는 EC2 중지 후에는 유지되지 않으므로 캡처를 먼저 완료합니다. 제출용 세 이미지의 촬영·저장은 별도 단계이며 위 검증 기록만으로 캡처 완료를 의미하지 않습니다.

공식 동작 참고: [Spark 3.5 계열 Structured Streaming의 foreachBatch·체크포인트](https://spark.apache.org/docs/3.5.7/structured-streaming-programming-guide.html), [Kafka 입력·커넥터 배포](https://spark.apache.org/docs/3.5.7/structured-streaming-kafka-integration.html). 이 과제의 실제 설치 버전과 커넥터는 3.5.3으로 고정합니다.

## Q5: 유휴 시간 이상 감지와 Slack·Gold·Athena 연결

Q5는 Q4가 실제 S3에 저장한 Silver Parquet 파일을 입력으로 사용합니다. `part1-ev/charger_alerts.py`의 `spark.readStream`이 새 파일을 읽고, Spark executor가 `idleSec > 5400`인 행을 필터링합니다. 원본 CSV를 직접 읽어 감지 결과를 대신하지 않습니다. 이미 존재하는 Silver 3개 파일도 첫 실행의 입력에 포함됩니다. `availableNow=True` 트리거는 현재 파일을 파일당 한 마이크로배치로 처리하고 정상 종료하며, 다시 실행할 때는 Q5의 체크포인트 다음부터 읽습니다. Q4 체크포인트나 Kafka·Q3 그룹은 바꾸지 않습니다.

같은 이상 행에서 두 쿼리를 실행합니다. 첫 쿼리의 `foreachBatch` 콜백은 driver 프로세스에서 Slack Incoming Webhook URL로 JSON HTTP POST를 보냅니다. 이는 Python 라이브러리 내부 호출만으로 끝나는 작업이 아니라 Slack 서버에 실제 네트워크 요청을 보내는 동작입니다. 메시지는 배치별로 묶되, 각 세션의 `stationName`, `locationGroup`, `startTime`, `idleSec`를 모두 포함합니다. 제공된 2017년 데이터 처리 결과임을 메시지에 표시합니다.

Webhook URL은 해당 Slack 채널로 메시지를 보낼 수 있는 비밀 주소입니다. 로컬과 EC2의 `.private/slack-webhook`에 권한 600으로 별도 보관하고 공개 Git·최종 ZIP·캡처에서 제외합니다. 실행 스크립트는 표준 입력으로 URL을 컨테이너의 driver 전용 파일에 복사하고 종료 시 그 임시 파일을 지웁니다. URL을 명령 인수나 로그에 출력하지 않습니다. Slack 앱은 `Week9 EV Alerts`, 권한은 특정 채널에 메시지를 보내는 `incoming-webhook`입니다. 앱 공개 배포·채널 대화 읽기 권한은 필요하지 않습니다.

Slack HTTP 응답 `200`과 본문 `ok`를 확인한 후 `checkpoints/q5/alerts-sent/batch-....json`에 보낸 4개 필드와 ACK를 기록합니다. 같은 배치의 기록이 있으면 재전송을 건너뜁니다. **Slack 전송과 S3 기록은 하나의 원자적 트랜잭션이 아닙니다.** ACK 직후 기록 전에 장애가 나면 재실행으로 중복이 생길 수 있습니다. 429의 Retry-After만 제한적으로 재시도하며 응답 불명확·기타 실패는 중단합니다. 이때는 Slack 수신 상태를 확인한 뒤 복구해야 합니다. 체크포인트·ACK 기록을 지우고 다시 실행하지 않습니다.

두 번째 쿼리는 설치구역별 이상 건수를 상태로 누적합니다. complete 모드의 `foreachBatch`에서 현재 전체 집계를 `s3://버킷/gold/ev_charging_agg/`의 Parquet로 덮어씁니다. Gold에는 `location_group` 문자열과 `anomaly_count` BIGINT 두 열이 있으며, 이상 건수가 있는 설치구역만 포함합니다. 실행 중의 덮어쓰기는 원자적 테이블 교체가 아니므로 스트림 종료 후 Gold 검증·Athena 조회를 수행합니다.

### EC2 실행과 실제 검증

Q5 코드를 커밋·push한 뒤 EC2에서 pull하고, Q4 작업이 종료됐는지 확인합니다. 원본 CSV·Silver·Q4 출력은 그대로 둡니다. EC2에 비공개 Webhook 설정이 준비된 후 아래를 실행합니다.

```bash
date && hostname && git rev-parse HEAD && bash scripts/run-q5.sh
docker compose -f part1-ev/docker-compose.yaml exec -T airflow \
  spark-submit --conf spark.driver.host=airflow /opt/de9/scripts/verify-q5.py
python3 scripts/register-q5-athena.py --bucket de9-lhi-947554757698
```

`verify-q5.py`는 실제 Gold Parquet와 S3의 Slack ACK 기록을 읽어 CSV에서 독립 계산한 이상 세션·설치구역별 건수와 대조합니다. ACK는 Slack 서버가 요청을 받아들였다는 기록이며, 과제에서 요구하는 Slack 수신 화면 캡처를 대신하지 않습니다.

Athena 등록 스크립트는 EC2 호스트의 AWS CLI를 호출하므로 EC2 IAM 역할로 인증합니다. `de9_lhi` 데이터베이스와 실제 Gold 경로를 가리키는 외부 테이블 `ev_charging_agg`를 등록한 다음 워크그룹 `de9-lhi`에서 SELECT를 실행합니다. 쿼리 실행 ID와 결과·조회 SQL은 `.runtime/q5/athena/`에 남고, 결과를 원본 CSV의 예상 집계와 대조합니다. 테이블 등록은 데이터 복사가 아니라 S3 파일의 위치·열 구조를 Glue Data Catalog에 기록하는 작업입니다. Athena가 SELECT 시 실제 Parquet를 읽습니다.

원본 CSV에서 사전 계산한 예상치는 **이상 세션 18건·설치구역 8개**입니다. 아래 실제 실행에서 별도 검증 코드로 같은 결과임을 확인했습니다.

### Q5 캡처

`part1-ev/capture/Q5/`에 다음 3개 파일을 보관합니다. 캡처는 사용자가 실제 화면에서 촬영하고, Webhook 설정·토큰 화면은 찍지 않습니다.

| 파일 | 필요한 실제 화면 |
|---|---|
| `slack_alert.png` | 본인 워크스페이스의 채널명·Week9 EV Alerts 앱·실제 수신 메시지. 네 필드와 `idleSec > 5400`이 보이게 촬영 |
| `s3_gold.png` | AWS S3 웹 콘솔의 `gold/ev_charging_agg/` 경로와 Parquet 객체 목록 |
| `athena_query_ev.png` | Athena 웹 콘솔 Query Editor, 워크그룹 `de9-lhi`·데이터베이스 `de9_lhi`, 아래 SELECT와 실제 결과 |

```sql
SELECT location_group, anomaly_count
FROM de9_lhi.ev_charging_agg
ORDER BY location_group;
```

### Q5 실제 실행 확인 기록

2026-11-24 18:35:41 한국 시각(09:35:41 UTC)에 공개 커밋 `da72abb`로 동일 EC2에서 실행했습니다. Q4의 실제 Silver 파일을 스트리밍 입력으로 읽어 3개 배치에서 7·3·8건의 이상 세션을 감지했습니다. `Week9 EV Alerts` 앱을 통해 본인 워크스페이스 `bootcamp-week9-lhi`의 `#bootcamp-week9-lhi-전체` 채널로 3개 메시지를 전송했고, 모두 Slack의 `200 / ok` 응답과 S3 ACK 기록을 확인했습니다. 실제 Slack 웹 채널에도 앱이 보낸 세션 정보가 표시되는 것을 확인했습니다. 테스트 문구나 수동 입력 메시지로 증빙을 대신하지 않았습니다.

Gold의 실제 Parquet를 다시 읽고 Slack ACK 기록에 담긴 18개 세션의 네 필드·중복 횟수까지 원본 CSV의 독립 계산과 대조했습니다. 누락·중복 없이 통과했으며 최종 Gold 결과는 다음과 같습니다. 이 수치는 Q4의 전체 충전 건수가 아니라 `idleSec > 5400`인 이상 세션 건수입니다.

| location_group | anomaly_count |
|---|---:|
| PALO ALTO CA / BRYANT | 1 |
| PALO ALTO CA / CAMBRIDGE | 4 |
| PALO ALTO CA / HAMILTON | 1 |
| PALO ALTO CA / HIGH | 2 |
| PALO ALTO CA / MPL | 4 |
| PALO ALTO CA / RINCONADA LIB 1 | 1 |
| PALO ALTO CA / TED THOMPSON | 1 |
| PALO ALTO CA / WEBSTER | 4 |

Gold 경로에는 8개 행을 담은 Parquet 1개와 `_SUCCESS`가 생성됐습니다. Athena에서 `de9_lhi.ev_charging_agg`를 실제 등록하고 SELECT를 수행해 8개 행·합계 18건이 원본 계산과 일치함을 확인했습니다.

- 데이터베이스 등록 QueryExecutionId: `c256edf3-2795-4b33-9dee-c56a93d671de`
- 외부 테이블 등록 QueryExecutionId: `39a8fdaf-4827-4d01-a350-813188befc41`
- 검증 SELECT QueryExecutionId: `63b21fa8-2131-4d1c-9094-fb109f370719`

현재 입력을 모두 처리한 Q5 스트리밍 쿼리는 정상 종료했습니다. Airflow·Spark worker의 OOMKilled 값은 false였습니다. 실제 실행 화면은 EC2의 `de9-q5` tmux 세션에, 로그·검증 결과·Gold 객체 목록·Slack ACK 기록·Athena SQL 및 결과 JSON은 EC2와 로컬 `.runtime/q5/`에 보존합니다. 이 기록과 비공개 Webhook 설정은 공개 Git에서 제외합니다. 캡처를 위해 알림 전송을 재실행하지 않습니다.

Slack 연결 자체는 무료 플랜의 앱 연결 범위에서 사용하며 EC2·S3·Athena 사용료는 별도입니다. 공식 참고: [Slack Incoming Webhooks](https://docs.slack.dev/messaging/sending-messages-using-incoming-webhooks/), [Athena Parquet 외부 테이블](https://docs.aws.amazon.com/athena/latest/ug/parquet-serde.html).

## Q6 — 식료품 가격 Producer

`part2-price/price_producer.py`는 EC2의 기존 Airflow 컨테이너 안에서 실행하는 Python 프로그램입니다. 입력 파일 `data/india_grocery_prices.csv`의 원본 200행을 읽어 `product-prices` 토픽에 JSON 메시지로 보냅니다. CSV는 수정하지 않고 공개 Git에서도 제외합니다. `year`·`month`는 JSON 정수, `price`는 JSON 실수이며 나머지 네 필드는 문자열입니다. CSV 행 순서를 유지합니다.

`RegionPartitioner`는 Python 클래스이며, 프로그램 시작 시 만든 객체를 `KafkaProducer(partitioner=...)`에 전달합니다. kafka-python이 도시명을 UTF-8 바이트로 직렬화한 뒤 이 객체를 호출합니다. 객체가 반환한 파티션 번호를 Producer가 사용하므로 `send()`에서 파티션 번호를 직접 지정하지 않습니다. 등록되지 않은 도시는 kafka-python의 `DefaultPartitioner`로 넘깁니다. 알려진 도시는 leader가 일시적으로 없어도 다른 권역으로 보내지 않습니다.

| 파티션 | 도시 | 원본에서 계산한 예상 건수 |
|---|---|---:|
| 0 | Delhi, Jaipur, Lucknow, Kanpur | 80 |
| 1 | Mumbai, Ahmedabad, Bhopal | 60 |
| 2 | Hyderabad, Chennai, Patna | 60 |

### Q6 실행과 검증

로컬 파티셔너 검사는 Kafka에 연결하지 않습니다. 실제 라이브러리의 기본 파티셔너와 미등록 도시의 선택 결과를 비교하고, 10개 도시의 권역·leader 부재 시 동작·`send(partition=...)` 미사용과 CSV 예상 건수를 검사합니다.

```bash
python scripts/check-q6-local.py
python part2-price/price_producer.py --validate-only
```

코드를 커밋·push하고 EC2에서 pull한 뒤, 원본 CSV를 EC2의 같은 `data/` 경로에 준비하여 호스트 셸에서 실행합니다. 기존 EV 토픽·컨슈머 그룹·S3 출력은 유지합니다.

```bash
date && hostname && git rev-parse HEAD && bash scripts/run-q6.sh
docker compose -f part1-ev/docker-compose.yaml exec -T airflow \
  python /opt/de9/scripts/verify-q6.py
```

실행 스크립트가 토픽을 파티션 3개·복제 2로 생성하고 실제 설정을 출력합니다. Producer는 설정과 시작·끝 오프셋을 검사한 뒤 새 빈 토픽일 때만 전송합니다. ACK를 받은 건수만 파티션별로 집계하며 `try...finally`에서 flush와 close를 수행합니다. 멱등 전송은 같은 Producer 세션의 내부 재시도 중복을 막으며, 프로그램 재실행 자체를 중복 제거하지는 않습니다. 재실행이 필요하더라도 토픽을 자동 삭제하지 않습니다. 먼저 원인 및 Q7 이후 출력과의 관계를 확인해야 합니다.

`verify-q6.py`는 그룹 가입·오프셋 커밋 없이 실제 토픽을 읽습니다. 원본 200행의 모든 필드 값과 중복 횟수, 숫자 타입, 도시 키, 권역별 파티션과 80·60·60건을 독립 대조합니다. 실행 로그는 EC2 `.runtime/q6/producer.log`에 남습니다. 공개 코드만으로는 실제 실행 증빙을 대신하지 않습니다.

### Q6 제출 캡처

`part2-price/capture/Q6/`에 실제 화면을 저장합니다.

| 파일 | 필요한 화면 |
|---|---|
| `producer_run.png` | EC2 호스트 실행 지문(date·hostname·공개 커밋 SHA), 토픽 생성·describe(파티션 3·복제 2), Producer 파티션별 ACK와 총 200건 |
| `kafka_ui_partitions.png` | Kafka UI의 `product-prices` 토픽명, 총 200건 및 파티션 0=80·1=60·2=60이 함께 보이는 화면 |

### Q6 실제 실행 확인 기록

2026-11-24 19:11:01 한국 시각(10:11:01 UTC)에 공개 커밋 `e426d5743da8fab9bef4ef47677394e13c056dcc`로 기존 EC2 `ip-172-31-11-232`에서 실행했습니다. `product-prices`를 새로 생성해 파티션 3개·복제 2 및 모든 ISR을 확인했고, 시작·끝 오프셋이 모두 0인 상태에서 원본 CSV 200행을 한 번 전송했습니다. 원본 SHA256은 `b6f6faa8c513b1b4721b0bc421f8b31ec00340fad96b84031c8dc6356d6649ca`입니다.

브로커 ACK를 받은 실제 건수는 파티션 0=80, 1=60, 2=60이며, Producer는 flush·close 후 정상 종료했습니다. 별도 검증 프로그램이 실제 Kafka 메시지 전체를 읽어 원본 CSV의 모든 값·중복 횟수, JSON 숫자 타입, market 키와 권역별 파티션을 독립 대조하여 통과했습니다. 검증 시 컨슈머 그룹 가입과 오프셋 커밋은 하지 않았습니다.

실제 실행 화면은 EC2 `de9-q6` tmux 세션에 남아 있습니다. 토픽 생성·전송 로그 `producer.log`, 검증 결과 `verify.log`, 실행 지문을 포함한 터미널 기록 `terminal.txt`는 EC2와 로컬 `.runtime/q6/`에 보존하며 공개 Git에서 제외합니다. 캡처를 위해 Producer를 다시 실행하지 않습니다. Q6의 두 제출 PNG는 실제 화면에서 촬영해 저장했습니다. `producer_run.png`에는 실행 지문·토픽 생성·파티션별 ACK·SUCCESS가, `kafka_ui_partitions.png`에는 실제 토픽의 200건 및 80·60·60건이 보입니다.

공식 참고: [kafka-python KafkaProducer의 partitioner 인자](https://kafka-python.readthedocs.io/en/2.3.2/apidoc/KafkaProducer.html).

## Q7 — 가격 데이터 Bronze/Silver 배치

`part2-price/price_batch.py`는 기존 Spark master·worker에서 실행하는 유한한 배치 프로그램입니다. `spark.read.format("kafka")`로 `product-prices`의 처음부터 실행 시점의 끝 오프셋까지 읽습니다. 새 메시지를 기다리는 스트리밍 쿼리를 만들지 않으며 처리가 끝나면 Spark를 종료합니다. Q6 Producer를 재실행할 필요가 없습니다.

처음 읽은 Kafka DataFrame은 처리 중 메모리에 캐시합니다. Bronze는 S3 `bronze/product-prices/`의 Parquet 파일에 메시지의 원본 `key`·`value` 바이트를 그대로 저장하며, `topic`·`partition`·`offset`·`timestamp`로 실제 Kafka 레코드를 식별할 수 있습니다. Bronze는 파싱된 Silver와 달리 JSON의 공백·키 순서도 보존합니다.

Silver는 명시한 JSON 스키마로 메시지를 파싱하고 `price`를 `double` 실수형으로 바꿉니다. 가격이 없거나 0 이하이면 제외하며, 숫자로 변환할 수 없는 값·NaN·무한대도 제외합니다. 유효한 7개 필드를 S3 `silver/product-prices/`의 Parquet 파일에 저장합니다. 제공 원본에는 제외 대상이 없어 **예상 Bronze·Silver 행 수는 각각 200행**입니다. 실제 파일 검증 결과도 아래에 기록했습니다.

저장 후 Silver의 `item_name`·`month`별 평균·최고·최저 가격을 계산하여 품목명·월 순으로 20개 그룹을 모두 콘솔에 출력합니다. 저장 모드는 각 가격 경로의 전체 스냅샷을 교체하는 `overwrite`이므로, Q9에서 전체 토픽을 다시 읽어도 단순 append 중복을 만들지 않습니다. S3 두 경로의 교체는 하나의 원자적 트랜잭션이 아니므로 작업 중간에 실패하면 완료 상태와 양쪽 출력을 확인한 뒤 재실행해야 합니다. 전기차 경로와 Kafka 원본은 변경하지 않습니다.

### Q7 실행과 검증

코드를 커밋·push한 뒤 EC2에서 pull하고 호스트 셸에서 실행합니다. 기존 컨테이너의 S3 접근은 EC2 IAM 역할을 사용합니다.

```bash
date && hostname && git rev-parse HEAD && bash scripts/run-q7.sh
docker compose -f part1-ev/docker-compose.yaml exec -T airflow \
  spark-submit --conf spark.driver.host=airflow /opt/de9/scripts/verify-q7.py
```

`scripts/check-q7-transform.py`는 메모리에서만 만든 입력으로 정제 제외 규칙과 집계 계산을 검사합니다. Kafka·S3에는 테스트 데이터를 쓰지 않습니다. `scripts/verify-q7.py`는 실제 S3 파일을 읽고 원본 CSV의 모든 값·중복 횟수, 실제 Kafka의 원본 key/value 바이트와 오프셋, Silver 실수형 스키마 및 20개 그룹의 평균·최고·최저 가격을 독립 계산과 대조합니다. 검증은 읽기 전용이며 Kafka 그룹 가입·커밋·S3 쓰기를 하지 않습니다.

사전 검사에서는 실제 EC2의 Spark 3.5.3으로 메모리 입력만 사용하여 정제·집계 함수 검사를 수행했고 통과했습니다. 이 검사는 실제 Kafka→S3 적재 완료를 의미하지 않습니다.

실행 출력은 EC2 `.runtime/q7/batch.log`, Spark 진단은 `.runtime/q7/spark-stderr.log`에 남습니다. 이 기록과 원본 CSV는 공개 Git에서 제외합니다.

### Q7 제출 캡처

`part2-price/capture/Q7/`에 다음 3개 파일을 저장합니다. S3 캡처는 사용자가 실제 AWS 웹 콘솔에서 촬영합니다.

| 파일 | 필요한 화면 |
|---|---|
| `batch_console.png` | EC2 호스트 실행 지문과 spark-submit 실행 결과, 20개 품목·월별 평균·최고·최저 가격 집계 |
| `s3_bronze_price.png` | S3 웹 콘솔의 본인 버킷 `bronze/product-prices/` 경로와 실제 객체 목록 |
| `s3_silver_price.png` | S3 웹 콘솔의 본인 버킷 `silver/product-prices/` 경로와 실제 Parquet 객체 목록 |

### Q7 실제 실행 확인 기록

2026-11-24 19:21:41 한국 시각(10:21:41 UTC)에 공개 커밋 `67777408d8a7cd908c625e75ac0bf091d7d6f54f`로 기존 EC2 `ip-172-31-11-232`에서 실행했습니다. Spark standalone 실행 ID는 `app-20261124102150-0004`입니다. Q6 토픽의 200건을 배치로 읽어 Bronze 200행·Silver 200행을 저장했고, 제외 행은 0행이었습니다. 20개 품목·월별 집계를 모두 콘솔에 출력한 뒤 `Q7 SUCCESS`와 함께 정상 종료했습니다.

실제 S3 출력은 다음과 같습니다. 두 경로에 각각 Parquet 1개와 `_SUCCESS`가 생성됐습니다.

| 경로 | Parquet 크기 | 실제 행 수 |
|---|---:|---:|
| `bronze/product-prices/` | 7,331 bytes | 200 |
| `silver/product-prices/` | 3,320 bytes | 200 |

별도의 검증 프로그램이 S3 Bronze를 Kafka 원본 key/value 바이트·topic/partition/offset과 대조해 완전히 일치함을 확인했습니다. Silver의 `price=double`, 200개 행의 모든 CSV 값·중복 횟수, 20개 그룹 평균·최고·최저 가격을 독립 Decimal 계산과 대조하여 통과했습니다. 결측·0·음수·숫자 변환 실패·NaN·무한대 가격의 제외 검사도 통과했습니다. Airflow와 Spark worker의 OOMKilled 값은 false였습니다.

실제 배치 실행 화면은 EC2 `de9-q7` tmux 세션에 보존합니다. 실행·진단·검증 로그와 실행 지문을 포함한 터미널 기록, Bronze·Silver 객체 목록은 EC2와 로컬 `.runtime/q7/`에 백업했습니다. 이 기록과 원본 CSV는 공개 Git에서 제외합니다. 제출 PNG 3개는 실제 터미널 및 AWS S3 웹 콘솔에서 촬영해 지정 경로에 저장했습니다. 실행 지문·20행 집계·정상 종료와 Bronze/Silver의 실제 객체 목록을 확인했습니다.

공식 참고: [Spark 3.5 계열 Kafka 배치 읽기와 원본 열 구조](https://spark.apache.org/docs/3.5.6/structured-streaming-kafka-integration.html#creating-a-kafka-source-for-batch-queries). 실행 라이브러리는 기존 환경과 동일한 Spark 3.5.3입니다.

## Q8 — 도시별 가격 변동률과 Gold

`part2-price/price_change.py`는 Q7이 S3에 저장한 `silver/product-prices/` Parquet를 읽는 Spark 배치 프로그램입니다. Kafka Producer나 Q7을 다시 실행하지 않고 현재 Silver를 사용합니다. 2016년 11·12월을 선택하고 도시·품목마다 각 달의 가격이 정확히 한 행씩 존재하는지 검사합니다. 누락 월·중복 월·유효하지 않은 가격은 오류로 처리해 잘못된 Gold 저장을 막습니다.

윈도우는 S3 폴더가 아니라 Spark가 같은 도시·품목의 행들을 묶고 월 순서로 계산하도록 지정한 코드 객체입니다. `Window.partitionBy("market", "item_name").orderBy("year", "month")`와 `lag()`로 12월 행에서 11월 가격을 참조합니다. 12월 필터는 반드시 lag 계산 뒤에 적용합니다. 두 달을 따로 읽어 조인하지 않습니다.

변동률은 `(12월 가격 - 11월 가격) / 11월 가격 × 100`입니다. 계산 중 가격을 십진수로 다루고 Spark `round(..., 2)`로 HALF_UP 반올림합니다. 예를 들어 100에서 60으로 하락하면 -40.00%이며, 100에서 110으로 상승한 +10.00%보다 변동 폭이 큽니다. 도시별 두 번째 윈도우에서 반올림한 변동률의 절대값 내림차순으로 `rank()`를 계산합니다. 동률은 같은 순위이며 다음 순위는 건너뜁니다(1·2·2·4). 제공 원본에는 도시별 1위 동률이 없습니다.

Gold는 `gold/grocery_price_change/`에 Parquet 스냅샷으로 저장합니다. 1위만 저장하지 않고 **도시 10곳 × 품목 10종의 전체 100행**을 저장하며, 콘솔에는 도시명 순으로 1위 10행을 출력합니다. 열은 다음 6개입니다.

| 열 | 실제 저장 타입·값 |
|---|---|
| `market` | 도시명 문자열 |
| `item_name` | 품목명 문자열 |
| `price_nov` | 2016년 11월 가격, double |
| `price_dec` | 2016년 12월 가격, double |
| `change_rate` | 부호를 유지한 백분율, decimal(18,2) |
| `change_rank` | 도시 내 절대 변동률 순위, int |

출력은 이 Gold 경로만 `overwrite`하므로 반복 실행 시 전체 스냅샷을 교체합니다. Silver·Bronze·전기차 Gold는 변경하지 않습니다. 덮어쓰기는 원자적 트랜잭션이 아니며, 실패하면 출력 상태를 확인한 뒤 재실행합니다.

### Q8 사전 계산과 실행

아래는 원본 CSV를 Python Decimal로 독립 계산한 **실행 전 예상 1위**입니다. 실제 Spark 출력으로 가장하지 않으며, 저장된 Gold는 별도 검증 프로그램으로 전체 100행을 대조합니다.

| market | item_name | change_rate (%) |
|---|---|---:|
| Ahmedabad | Tomatoes | -2.43 |
| Bhopal | Potatoes | -31.81 |
| Chennai | Potatoes | -14.51 |
| Delhi | Potatoes | -38.33 |
| Hyderabad | Tomatoes | -30.18 |
| Jaipur | Potatoes | -38.03 |
| Kanpur | Potatoes | -47.57 |
| Lucknow | Potatoes | -42.72 |
| Mumbai | Tomatoes | -24.75 |
| Patna | Onions | 32.95 |

코드를 커밋·push한 뒤 EC2에서 pull하고 호스트 셸에서 실행합니다.

```bash
date && hostname && git rev-parse HEAD && bash scripts/run-q8.sh
docker compose -f part1-ev/docker-compose.yaml exec -T airflow \
  spark-submit --conf spark.driver.host=airflow /opt/de9/scripts/verify-q8.py
```

`scripts/check-q8-transform.py`는 메모리 입력만 사용하여 뒤섞인 월 순서에서의 전월 참조, 상승·하락 절대값 순위, 동률, 양수·음수 반올림 경계와 누락·중복 월을 검사합니다. Kafka/S3 접근이나 쓰기는 하지 않습니다. `scripts/verify-q8.py`는 실제 S3 Gold를 읽어 원본 CSV의 Decimal 독립 계산과 100행 전체의 전월·당월 가격, 반올림한 변동률, 동률 순위 및 6개 열을 대조합니다. 실제 1위가 도시별 하나씩 10행인지도 확인합니다.

실제 EC2의 Spark 3.5.3에서 위 메모리 입력 검사를 수행하여 전월 참조·절대값 순위·동률·양수/음수 반올림 경계·누락/중복 월·0원 제외 검사가 통과했습니다. 이는 실제 Silver 읽기와 Gold 적재 완료를 의미하지 않습니다.

실행 로그는 `.runtime/q8/change.log`, Spark 진단은 `.runtime/q8/spark-stderr.log`에 남습니다. 원본 CSV와 실행 기록은 공개 Git에서 제외합니다.

### Q8 제출 캡처

`part2-price/capture/Q8/`에 다음 두 파일을 저장합니다.

| 파일 | 필요한 실제 화면 |
|---|---|
| `change_rank_top1.png` | EC2 호스트 실행 지문과 도시명 순 1위 10행, 두 달 가격·변동률·순위, Gold 전체 100행 저장 결과 |
| `s3_gold_price.png` | S3 웹 콘솔의 본인 버킷 `gold/grocery_price_change/` 경로와 실제 Parquet 객체 목록 |

### Q8 실제 실행 확인 기록

2026-11-24 19:35:09 한국 시각(10:35:09 UTC)에 공개 커밋 `b3e4cb691796c190e5b151c578d246298686b436`로 기존 EC2 `ip-172-31-11-232`에서 실행했습니다. Spark standalone 실행 ID는 `app-20261124103515-0006`입니다. 실제 Q7 Silver를 읽어 윈도우 함수로 계산하고, Gold에 도시×품목 전체 100행·6개 열을 저장한 뒤 도시명 순으로 1위 10행을 출력하고 정상 종료했습니다.

실제 `gold/grocery_price_change/`에는 Parquet 1개(3,722 bytes)와 `_SUCCESS`가 생성됐습니다. 별도의 읽기 전용 검증 프로그램이 저장 파일을 다시 읽어 원본 CSV의 Decimal 독립 계산과 100행 전체의 두 달 가격·소수 둘째 자리 반올림 변동률·동률 순위를 대조하여 통과했습니다. 도시별 1위 10행은 위 사전 계산 표와 모두 일치했습니다. `change_rate`의 실제 물리 타입은 `decimal(18,2)`이며 Airflow와 Spark worker의 OOMKilled 값은 false였습니다.

실제 실행 화면은 EC2 `de9-q8` tmux 세션에 보존합니다. 실행·진단·검증 로그와 실행 지문을 포함한 터미널 기록, Gold 객체 목록은 EC2와 로컬 `.runtime/q8/`에 백업했습니다. 이 기록과 원본 CSV는 공개 Git에서 제외합니다. 제출용 `change_rank_top1.png`와 `s3_gold_price.png`는 실제 터미널 및 AWS S3 웹 콘솔에서 별도로 촬영해야 합니다.

공식 참고: Spark 3.5 계열의 [lag](https://spark.apache.org/docs/3.5.6/api/python/reference/pyspark.sql/api/pyspark.sql.functions.lag.html), [rank](https://spark.apache.org/docs/3.5.6/api/python/reference/pyspark.sql/api/pyspark.sql.functions.rank.html), [round](https://spark.apache.org/docs/3.5.6/api/python/reference/pyspark.sql/api/pyspark.sql.functions.round.html). 실제 실행 환경은 Spark 3.5.3입니다.

## 참고

- 과제 원문: [9주차 문제지](https://drive.google.com/file/d/1PpJEN2VyGTrbmo4B6qU34ImX7Gcb168P/view), 1~8쪽. 문제지 자체는 공개 저장소에 복제하지 않습니다.
- [Kafka 공식 Docker 예제](https://github.com/apache/kafka/blob/3.9.1/docker/examples/docker-compose-files/cluster/combined/plaintext/docker-compose.yml)
- [Airflow 이미지 확장](https://airflow.apache.org/docs/docker-stack/build.html)
- [EC2 역할 사용](https://docs.aws.amazon.com/IAM/latest/UserGuide/id_roles_use_switch-role-ec2.html)
- [컨테이너의 IMDS 설정](https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/configuring-instance-metadata-options.html)
