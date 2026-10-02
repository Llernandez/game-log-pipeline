# Kubernetes 배포 (v0.2.0)

v0.1.0의 검증·중복 제거 규칙(`game_log_pipeline/pipeline.py`)은 그대로 두고 앞뒤를 쿠버네티스 서비스로 감쌌습니다. 로컬 kind 클러스터에서 end-to-end로 실행해 확인했습니다. EKS는 같은 차트·오버레이의 설정만 준비했습니다(생성하지 않음).

## 구조

```mermaid
flowchart LR
  subgraph glp[namespace glp]
    API[ingest API<br/>FastAPI Deployment] -->|idempotent producer<br/>acks=all, key=event_id| K
    L[loader Deployment<br/>consumer group] -->|INSERT ... ON CONFLICT<br/>(topic, partition, offset)| PG[(CloudNativePG<br/>raw_events)]
    AF[Airflow 3<br/>scheduler] -->|KubernetesPodOperator| R[rebuild pod]
    R -->|read RAW| PG
    R -->|marts| PG
    R -.->|optional marts| SF[(Snowflake)]
  end
  subgraph kafka[namespace kafka]
    K[[Strimzi Kafka 4.2<br/>topic game.events.raw]] --> L
  end
  G[demo traffic Job<br/>/ game client] -->|POST /v1/events JSONL| API
  subgraph argocd[namespace argocd]
    A[Argo CD app-of-apps] -. sync from Git .-> glp & kafka
  end
```

| 구성 | 방식 | 버전 |
|---|---|---|
| 클러스터 | kind(로컬, WSL2) / EKS 설정만 | Kubernetes 1.37 |
| 배포 | Argo CD app-of-apps, sync wave(-2 운영자 → -1 플랫폼 → 0 앱 → 1 Airflow) | Argo CD 3.5.3 (chart 10.9.2) |
| 앱 | Helm 차트 `deploy/charts/game-log-pipeline` (values-local / values-eks) | 0.2.0 |
| 플랫폼 CR | Kustomize base + overlays(local / eks) | - |
| Kafka | Strimzi 운영자, KRaft, v1 API | Strimzi 1.2.0, Kafka 4.2.0 |
| PostgreSQL | CloudNativePG 운영자 | 1.30.1 (chart 0.29.1) |
| 오케스트레이션 | Airflow 공식 차트, LocalExecutor + KubernetesPodOperator, DAG는 git-sync | Airflow 3.2.2 (chart 1.22.0) |

## 전달 보장과 멱등성

| 구간 | 보장 | 중복 처리 |
|---|---|---|
| 클라이언트 → API | 요청 전체가 acks=all로 확인되면 202, 아니면 503 → 클라이언트가 묶음 전체 재전송 | 재전송으로 생긴 중복은 아래 event_id 규칙이 흡수 |
| API → Kafka | idempotent producer | 브로커가 producer 재시도 중복 제거 |
| Kafka → PostgreSQL | at-least-once(DB 커밋 후 오프셋 커밋) | `(topic, partition, offset)` 고유 키로 재전달 무시 |
| RAW → marts | 전체 재계산(작은 데이터 기준) | v0.1.0의 event_id / transaction_id / reward_claim_id 규칙 그대로 |

API는 이벤트를 검증하지 않고 원문을 보존합니다. 스키마 오류와 ID 충돌은 재계산에서 격리됩니다(v0.1.0 설계 유지). 요청 크기만 제한합니다(1MiB, 1,000줄, 줄당 16KiB).

## 로컬 실행(WSL2)

```sh
# 1회: WSL 배포판에 Docker, kind, kubectl, Helm, argocd CLI (체크섬 검증)
sudo bash infra/local/wsl-setup.sh
# 클러스터 → Argo CD → root Application. 나머지는 Argo CD가 Git에서 생성
bash infra/local/bootstrap.sh
```

| 주소 | 내용 |
|---|---|
| http://localhost:30080/readyz | 수집 API(Kafka 연결 확인) |
| http://localhost:30081 | Argo CD |
| http://localhost:30082 | Airflow |
| http://localhost:30083 | Grafana(대시보드 `game-log-pipeline`). 0.3.0 이전에 만든 클러스터는 포트 매핑이 없으므로 `kubectl -n monitoring port-forward svc/kps-grafana 3000:80` |

## 확인한 결과(2026-09-28, kind)

- 데모 트래픽 Job이 합성 fixture 10건 전송 → `{"accepted":10}` → loader `{"loaded": 10}` → rebuild `{"raw": 10, "clean": 6, "quarantine": 3}`.
- PostgreSQL marts의 이상 후보는 `offline_policy_exceeded:p_excess`, `duplicate_reward_claim:p_repeat`로 v0.1.0 SQLite 기준 구현과 같습니다.
- 같은 묶음을 한 번 더 보내면 RAW 20, CLEAN 6, 이상 후보 2 유지(격리는 수신 단위라 3→6).
- eks 오버레이와 values-eks 렌더링은 서버 dry-run 통과.
- GitOps 전체 재구성(클러스터 삭제 → `bootstrap.sh`): Argo CD 앱 7개(root, strimzi, cloudnative-pg, kafka, postgres, game-log-pipeline, airflow)가 모두 Synced/Healthy. 데모 트래픽 → loader → PostgreSQL 결과는 위와 같습니다.
- Airflow `glp_rebuild` DAG: 예약 실행과 수동 실행 모두 success. KubernetesPodOperator가 `rebuild_marts` 파드를 띄워 재계산합니다.
- 0.3.0 모니터링(2026-09-29, kind): Argo CD 앱 9개(monitoring, monitoring-config 추가) 모두 Synced/Healthy. Prometheus 대상 `glp-ingest`·`glp-loader`·`glp-kafka-exporter` 모두 up. 데모 묶음 전송 뒤 `glp_ingest_events_total{result="accepted"}`와 `glp_loader_rows_total`이 같은 값(20)으로 증가, `kafka_consumergroup_lag` 파티션 0~2 모두 0. 경보 5개 로드(inactive), 대시보드 ConfigMap 적용. Airflow OutOfSync 해소.
- 시작 순서: loader가 PostgreSQL보다 먼저 뜨면 연결 거부로 재시작하고 DB가 준비되면 복구됩니다(재시작 정책에 맡김).

## 로컬 환경 문제와 해결

| 증상 | 원인 | 해결 |
|---|---|---|
| Argo CD가 GitHub에서 `context deadline exceeded` | 파드는 호스트의 DNS search 도메인과 `ndots:5`를 물려받는다. 일부 ISP DNS는 존재하지 않는 이름에도 응답하므로 `github.com.<search 도메인>`이 ISP 주소로 "해석"된다. 노드는 `ndots:0`이라 정상이어서 파드에서만 재현된다 | `bootstrap.sh`가 search 줄을 뺀 resolv.conf를 kubelet `resolvConf`로 지정 |
| VPN 사용 시 WSL MTU 1280 | kind 기본 네트워크는 1500 | kind 네트워크를 호스트 MTU로 생성 |
| airflow 앱이 OutOfSync(`Job/airflow-create-user`) | 차트가 완료 Job을 TTL로 지우면 Argo CD는 리소스가 사라졌다고 본다 | 두 Job을 Argo CD Sync 훅으로 실행(`jobAnnotations`) |
| strimzi 앱이 계속 OutOfSync, 몇 분마다 CRD 재적용 | API 서버가 큰 Kafka CRD를 정규화해서 클라이언트 측 diff가 끝나지 않음 | 해당 Application에 `ServerSideDiff=true` |

## 모니터링 (0.3.0)

| 신호 | 출처 | 쓰임 |
|---|---|---|
| `glp_ingest_events_total{result}` | 수집 API `/metrics` | 수신·거부·미확인(Kafka ack 실패) 이벤트 수 |
| `glp_loader_rows_total`, `glp_loader_last_commit_unixtime` | 로더 `:9100/metrics` | DB 커밋 후 오프셋 커밋까지 끝난 행 수와 마지막 시각 |
| `kafka_consumergroup_lag` | Strimzi Kafka Exporter | `glp-loader` 그룹의 파티션별 지연 |
| `kube_pod_status_phase` | kube-state-metrics | Airflow가 띄운 `glp-rebuild` 파드 실패 |

경보(`deploy/platform/monitoring/base/rules.yaml`): 지연 100건 초과 5분, 지연이 있는데 10분간 커밋 없음(로더 정체), 로더 대상 없음, Kafka 미확인 전송, 재계산 파드 실패. 임계치는 데모 값입니다. 스크레이프 대상(PodMonitor)·경보·대시보드가 모두 Git에 있고 Argo CD가 동기화합니다.

## 스키마 진화 (0.3.0)

생산자(게임 클라이언트)는 한꺼번에 업그레이드되지 않으므로 여러 스키마 버전이 동시에 들어옵니다. 재계산은 각 버전을 v1 업무 형태로 정규화한 뒤 같은 규칙을 적용합니다. 지원하지 않는 버전과 계약 위반은 사유를 남겨 격리합니다. 같은 이벤트가 다른 버전으로 재전송되면 충돌로 보지 않습니다. 버전 분포는 보고서의 `schema_versions`로 확인합니다.

## 증분 재계산 (0.4.0)

1. 로더가 RAW를 저장할 때 `event_key`·`txn_key`를 함께 기록합니다.
2. 재계산은 대상마다 `pipeline_state.raw_watermark` 이후 RAW만 읽습니다.
3. 새 행과 event_id가 같은 모든 전송, 거래 키가 같은 모든 이벤트를 더 늘어나지 않을 때까지 모읍니다(폐포).
4. 그 범위만 기존 규칙으로 계산해 해당 이벤트·전송 행을 교체하고 워터마크를 옮깁니다(한 트랜잭션).

워터마크가 없는 대상은 전체 재계산으로 시작합니다.

확인(2026-09-29, kind + Snowflake 체험 계정, Airflow `glp_rebuild`):

| 실행 | PostgreSQL | Snowflake |
|---|---|---|
| 1. 워터마크 없음 | full, RAW 40(이전 30행 키 채움), clean 6 | full, clean 6 |
| 2. 같은 fixture 재전송 10건 뒤 | incremental, 새 RAW 10 → 재계산 50(모두 같은 키), clean 6, 격리 15 | 같음 |
| 3. 새 이벤트 1건 뒤 | incremental, 새 RAW 1 → 재계산 1, clean 7 | 같음 |

재전송은 기존 전송과 키가 같아서 폐포가 커집니다. 새 이벤트는 그 한 행만 다시 계산합니다. 규칙을 바꾼 뒤에는 Airflow에서 conf `{"mode": "full"}`로 실행합니다.

## 단계 시도 입력 (0.5.0)

게임이 커지면 재화 외에 진행 로그가 필요합니다. `stage_attempt`는 같은 토픽·RAW 테이블로 들어오고 재계산에서 이벤트 유형별로 나뉩니다.

- 계약: `track`(story/endless), `stage`(1~10000), `outcome`(clear/fail), `duration_ms`. 허용 필드 밖의 값, 범위 밖 단계, 같은 event_id의 다른 내용은 격리합니다.
- `stage_funnel`: 트랙·단계별 도전 이용자, 통과 이용자, 시도, 통과.
- `difficulty_walls`: 3번 이상 실패했고 아직 통과하지 못한 이용자가 2명 이상인 단계. 합성 데이터에서는 무한 도전 8단계(막힌 3명, 실패 10회).
- 증분: 시도는 거래 키가 없어 event_id 폐포만으로 닫힙니다.

확인(2026-09-29, kind + Snowflake 체험 계정): Argo CD가 0.5.0으로 동기화한 뒤 데모 트래픽 Job이 재화 fixture와 시도 46건을 보냈고 로더가 56행을 적재했습니다. Airflow `glp_rebuild`(증분)로 두 대상 모두 워터마크 107, `clean_attempts` 42, `difficulty_walls` = 무한 도전 8단계(막힌 3명, 실패 10회), `stage_funnel` 8단계 도전 4명·통과 1명·시도 13회로 같았습니다. 기존 테이블에 새 테이블·뷰를 더하는 변경이라 전체 재계산 없이 증분으로 반영됐습니다.

## 한계

- 증분 워터마크는 단일 로더의 커밋 순서에 기댑니다. 병렬 적재에는 커밋 순서 로그나 안전 지연이 필요합니다.
- 로컬은 브로커·DB 모두 단일 인스턴스입니다. 복제·장애 조치는 eks 오버레이에서 설정만 했습니다.
- 2026-09-28 첫 실행은 Snowflake 자격 증명 없이 PostgreSQL marts만 확인했습니다. Snowflake는 0.4.0부터 체험 계정에서 같은 결과를 확인했습니다.
- 인증·TLS가 없는 로컬 데모입니다. 실제 수집 API는 인증과 요청 제한이 필요합니다.
- ingress-nginx가 2026년 3월 지원 종료되어 로컬에서는 NodePort를 씁니다. EKS에서는 AWS Load Balancer Controller를 가정합니다.
