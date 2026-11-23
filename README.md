https://github.com/rickyshu/bootcamp-week9

# 데엔8기 이호인 9주차

현재 범위는 **Q1 실행 환경 구성, Q2·Q3 전기차 Producer/Consumer와 Q4 Spark 스트리밍 코드**입니다. 이니셜은 `lhi`입니다. 이 저장소의 구성 파일은 EC2에 컨테이너를 만드는 입력 파일이며, 파일이 존재하는 것과 실제 컨테이너가 실행되는 것은 다릅니다. AWS에서 실행하고 화면을 확인하기 전까지 문항 완료로 간주하지 않습니다.

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

## 참고

- 과제 원문: [9주차 문제지](https://drive.google.com/file/d/1PpJEN2VyGTrbmo4B6qU34ImX7Gcb168P/view), 1~8쪽. 문제지 자체는 공개 저장소에 복제하지 않습니다.
- [Kafka 공식 Docker 예제](https://github.com/apache/kafka/blob/3.9.1/docker/examples/docker-compose-files/cluster/combined/plaintext/docker-compose.yml)
- [Airflow 이미지 확장](https://airflow.apache.org/docs/docker-stack/build.html)
- [EC2 역할 사용](https://docs.aws.amazon.com/IAM/latest/UserGuide/id_roles_use_switch-role-ec2.html)
- [컨테이너의 IMDS 설정](https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/configuring-instance-metadata-options.html)
