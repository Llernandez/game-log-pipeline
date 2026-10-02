# Changelog

## Unreleased

- 게임 진단 로그 어댑터(`game_log_pipeline/device.py`, CLI `adapt`): 게임이 내보내는 진단 JSONL(스키마 1~3)의 `battle_result`를 `stage_attempt`로 변환. 탑 층은 무한 도전 트랙, 스토리 단계는 1부터, `duration_ms = ticks × 100`. `event_id`를 유지하므로 겹쳐서 다시 내보낸 기록은 재전송으로 처리.
- 제외 규칙: 개발 보조(`assisted`·`session_assisted`) 기록, 전투 외 기록(서버 거래 ID가 없는 재화 기록 포함), 깨진 JSON·계약 위반을 사유별로 집계.
- 검증 경계: `device_diagnostic` 출처는 `stage_attempt`에만 허용하고 재화 이벤트로 들어오면 격리.
- 형식만 같은 합성 샘플 `examples/device_diagnostic_sample.jsonl`, 검사 34개 → 38개.
- 문서: README 실행 화면(구조도, Argo CD, Airflow, Grafana, Snowflake), [트러블슈팅](docs/troubleshooting.md), [게임 로그 연결](docs/device-logs.md).

## 0.5.0 - 2026-09-29

- 두 번째 입력 `stage_attempt`: 스토리·무한 도전 트랙의 단계 시도 한 건(트랙, 단계, 성공/실패, 소요 시간). 재화 이벤트와 같은 RAW·Kafka·재계산 경로를 타고 같은 규칙(허용 필드 목록, event_id 충돌 격리, 스키마 v1~v3 정규화)을 따른다. 재화 거래 집계에는 섞이지 않는다.
- marts: `clean_attempts` 테이블, `stage_funnel`(단계별 도전 이용자·통과 이용자·시도·통과 수), `difficulty_walls`(3번 이상 실패하고 아직 못 넘은 이용자가 2명 이상인 단계). 밸런스를 고칠 구간을 찾는 신호이지 이용자 판정이 아니다. PostgreSQL·Snowflake·SQLite에서 같은 SQL을 쓴다.
- 증분 재계산: 시도 이벤트는 event_id로만 충돌하므로 거래 키 없이 event 키 폐포로 닫힌다. 무작위 순서 대조 검사에도 시도 이벤트를 포함.
- 합성 데이터: 무한 도전 8단계의 벽(4명이 막히고 1명만 두 번 실패 뒤 통과), 재전송·충돌·범위 밖 단계. 검사 29개 → 34개.
- 이미지·차트 0.5.0.

## 0.4.0 - 2026-09-29

- 증분 재계산: 대상(PostgreSQL·Snowflake)마다 RAW 워터마크(`pipeline_state`)를 두고 워터마크 이후 RAW만 읽는다. 영향 범위는 시간이 아니라 **키**로 정한다. 새 RAW와 같은 `event_id`의 모든 전송, 같은 이용자·통화·거래 ID의 모든 이벤트로 닫힐 때까지 범위를 넓힌다. 그 뒤 기존 규칙으로 그 범위만 다시 계산해 한 트랜잭션으로 교체한다. 지연 도착은 새 RAW일 뿐이라 별도 처리가 필요 없다(일별 집계는 뷰).
- 정확성: 무작위 도착 순서·재전송·늦게 온 충돌·거래 충돌·스키마 v3 재전송·잘못된 JSON을 섞은 40가지 순서에서, 묶음마다 증분 결과가 전체 재계산과 같은지 대조. 영향 범위 확장을 끄면 이 검사가 실패함을 확인(변이 검사). 검사 25개 → 29개.
- 적재: 로더가 RAW를 저장할 때 `event_key`·`txn_key`를 함께 기록(인덱스). 이전 행은 첫 재계산에서 한 번 채운다(`keyed`).
- 실행 방식: 기본 증분, 워터마크가 없는 대상은 전체. Airflow는 `{"mode": "full"}` conf로 전체 재처리(규칙 변경 후 backfill). CLI `rebuild --mode incremental|full`.
- 한계: 워터마크는 RAW id가 id 순서로 보인다는 가정(한 번에 한 묶음씩 커밋하는 단일 로더)에 기댄다. 병렬 적재에는 커밋 순서 로그나 안전 지연이 필요하다.

## 0.3.0 - 2026-09-29

- 스키마 진화: 생산자가 점진적으로 업그레이드되는 상황을 합성 이벤트로 재현. `schema_version` 1~3을 모두 받아 v1 업무 형태로 정규화(upcasting)한다. v2는 `client_version`(필수, 계보용), v3는 `currency`→`currency_code` 이름 변경. 지원하지 않는 버전·계약 위반은 사유와 함께 격리. 업그레이드된 클라이언트가 같은 이벤트를 새 버전으로 다시 보내면 충돌이 아니라 재전송으로 처리. 보고서에 수신 버전 분포(`schema_versions`). 검사 19개 → 23개.
- 모니터링: kube-prometheus-stack(Prometheus·Alertmanager·Grafana)을 Argo CD 앱으로 추가. 수집 API `/metrics`(`glp_ingest_events_total{result}`), 로더 지표(`glp_loader_rows_total`, `glp_loader_last_commit_unixtime`), Strimzi Kafka Exporter의 컨슈머 지연(`kafka_consumergroup_lag`). 경보 5개(지연 과다, 로더 정체, 로더 없음, 미확인 전송, 재계산 파드 실패)와 Grafana 대시보드를 Git에서 관리.
- Airflow 차트의 `create-user`/`migrate` Job을 Argo CD Sync 훅으로 실행. 차트 TTL이 완료 Job을 지우면서 앱이 OutOfSync로 보이던 문제 해결.
- 이미지·차트 0.3.0. kind 설정에 Grafana NodePort 30083.
- Snowflake 체험 계정에서 Airflow 재계산으로 marts 적재 실행. 결과는 PostgreSQL과 일치(clean 6, 격리 9, 이상 후보 2).
- Snowflake 키 페어 인증(PEM `private_key`, 없으면 password). 체험 계정용 `infra/snowflake/setup.sql`(전용 역할·XSMALL 자동정지 창고·서비스 사용자·월 1크레딧 리소스 모니터)과 Secret 생성 스크립트, [실행 절차](docs/snowflake.md).

## Unreleased (0.2.x 운영 수정)

- 로컬 GitOps 수정: 파드 DNS가 호스트 search 도메인 때문에 외부 이름을 잘못 해석하던 문제(kubelet `resolvConf`), kind 네트워크 MTU를 호스트에 맞춤, strimzi 앱 `ServerSideDiff`로 CRD OutOfSync 반복 제거.
- 클러스터 재구성부터 Argo CD 7개 앱 Synced/Healthy, Airflow `glp_rebuild` DAG 성공까지 확인. [docs/kubernetes.md](docs/kubernetes.md).

## 0.2.0 - 2026-09-28

- FastAPI 수집 API(원문 보존, 요청 크기 제한, idempotent producer), Kafka loader(DB 커밋 후 오프셋 커밋, `(topic, partition, offset)` 고유 키), 재계산 작업(PostgreSQL landing → PostgreSQL/Snowflake marts). v0.1.0 규칙은 변경 없음.
- 하나의 컨테이너 이미지(api / load / rebuild / send), 비루트·읽기 전용 루트 파일시스템.
- Helm 차트(values-local / values-eks), Strimzi Kafka와 CloudNativePG의 Kustomize 오버레이(local / eks), Airflow 3 DAG(KubernetesPodOperator, git-sync), Argo CD app-of-apps와 sync wave.
- WSL2 kind 로컬 설치·부트스트랩 스크립트, EKS 설정 참고 문서(생성하지 않음).
- CI: 매니페스트 검사(helm lint / template, kustomize build)와 이미지 빌드. 태그 시 GHCR 게시.
- kind에서 end-to-end 확인: 10건 수신 → CLEAN 6 / 격리 3 / 이상 후보 2, 같은 묶음 재전송 시 업무 결과 불변.
- 검사 12개 → 19개.

## 0.1.0 - 2026-09-25

- 독립 합성 이벤트 fixture와 로컬 RAW/CLEAN/격리 구조.
- 이벤트/거래 중복과 보상 중복 지급 후보 분리.
- 공개 정책 상한, UTC 집계, 지연 도착 전체 재계산.
- 재시도·충돌·실패 롤백 검사, CLI와 CI 정의.
- Kafka/Snowflake/Airflow/API/Kubernetes 미구현. README 후속 범위 참조.
