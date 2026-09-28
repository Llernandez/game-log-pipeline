# Changelog

## 0.3.0 — 2026-09-29

- 스키마 진화: 생산자가 점진적으로 업그레이드되는 상황을 합성 이벤트로 재현. `schema_version` 1~3을 모두 받아 v1 업무 형태로 정규화(upcasting)한다. v2는 `client_version`(필수, 계보용), v3는 `currency`→`currency_code` 이름 변경. 지원하지 않는 버전·계약 위반은 사유와 함께 격리. 업그레이드된 클라이언트가 같은 이벤트를 새 버전으로 다시 보내면 충돌이 아니라 재전송으로 처리. 보고서에 수신 버전 분포(`schema_versions`). 검사 19개 → 23개.
- 모니터링: kube-prometheus-stack(Prometheus·Alertmanager·Grafana)을 Argo CD 앱으로 추가. 수집 API `/metrics`(`glp_ingest_events_total{result}`), 로더 지표(`glp_loader_rows_total`, `glp_loader_last_commit_unixtime`), Strimzi Kafka Exporter의 컨슈머 지연(`kafka_consumergroup_lag`). 경보 5개(지연 과다, 로더 정체, 로더 없음, 미확인 전송, 재계산 파드 실패)와 Grafana 대시보드를 Git에서 관리.
- Airflow 차트의 `create-user`/`migrate` Job을 Argo CD Sync 훅으로 실행. 차트 TTL이 완료 Job을 지우면서 앱이 OutOfSync로 보이던 문제 해결.
- 이미지·차트 0.3.0. kind 설정에 Grafana NodePort 30083.
- Snowflake 체험 계정에서 Airflow 재계산으로 marts 적재 실행, PostgreSQL과 결과 일치(clean 6, 격리 9, 이상 후보 2).
- Snowflake 키 페어 인증(PEM `private_key`, 없으면 password). 체험 계정용 `infra/snowflake/setup.sql`(전용 역할·XSMALL 자동정지 창고·서비스 사용자·월 1크레딧 리소스 모니터)과 Secret 생성 스크립트, [실행 절차](docs/snowflake.md).

## Unreleased (0.2.x 운영 수정)

- 로컬 GitOps 수정: 파드 DNS가 호스트 search 도메인 때문에 외부 이름을 잘못 해석하던 문제(kubelet `resolvConf`), kind 네트워크 MTU를 호스트에 맞춤, strimzi 앱 `ServerSideDiff`로 CRD OutOfSync 반복 제거.
- 클러스터 재구성부터 Argo CD 7개 앱 Synced/Healthy, Airflow `glp_rebuild` DAG 성공까지 확인. [docs/kubernetes.md](docs/kubernetes.md).

## 0.2.0 — 2026-09-28

- FastAPI 수집 API(원문 보존, 요청 크기 제한, idempotent producer), Kafka loader(DB 커밋 후 오프셋 커밋, `(topic, partition, offset)` 고유 키), 재계산 작업(PostgreSQL landing → PostgreSQL/Snowflake marts). v0.1.0 규칙은 변경 없음.
- 하나의 컨테이너 이미지(api / load / rebuild / send), 비루트·읽기 전용 루트 파일시스템.
- Helm 차트(values-local / values-eks), Strimzi Kafka와 CloudNativePG의 Kustomize 오버레이(local / eks), Airflow 3 DAG(KubernetesPodOperator, git-sync), Argo CD app-of-apps와 sync wave.
- WSL2 kind 로컬 설치·부트스트랩 스크립트, EKS 설정 참고 문서(생성하지 않음).
- CI: 매니페스트 검사(helm lint / template, kustomize build)와 이미지 빌드. 태그 시 GHCR 게시.
- kind에서 end-to-end 확인: 10건 수신 → CLEAN 6 / 격리 3 / 이상 후보 2, 같은 묶음 재전송 시 업무 결과 불변.
- 검사 12개 → 19개.

## 0.1.0 — 2026-09-25

- 독립 합성 이벤트 fixture와 로컬 RAW/CLEAN/격리 구조.
- 이벤트/거래 중복과 보상 중복 지급 후보 분리.
- 공개 정책 상한, UTC 집계, 지연 도착 전체 재계산.
- 재시도·충돌·실패 롤백 검사, CLI와 CI 정의.
- Kafka/Snowflake/Airflow/API/Kubernetes 미구현; README 후속 범위 참조.
