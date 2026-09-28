# Kubernetes 배포 (v0.2.0)

v0.1.0의 검증·중복 제거 규칙(`game_log_pipeline/pipeline.py`)은 그대로 두고, 앞뒤를 쿠버네티스 서비스로 감쌌습니다. 로컬 kind 클러스터에서 end-to-end로 실행해 확인했고, EKS는 같은 차트·오버레이의 설정만 준비했습니다(생성하지 않음).

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
| 플랫폼 CR | Kustomize base + overlays(local / eks) | — |
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

## 확인한 결과(2026-09-28, kind)

- 데모 트래픽 Job이 합성 fixture 10건 전송 → `{"accepted":10}` → loader `{"loaded": 10}` → rebuild `{"raw": 10, "clean": 6, "quarantine": 3}`.
- PostgreSQL marts의 이상 후보: `offline_policy_exceeded:p_excess`, `duplicate_reward_claim:p_repeat` — v0.1.0 SQLite 기준 구현과 같다.
- 같은 묶음을 한 번 더 보내면 RAW 20, CLEAN 6, 이상 후보 2 유지(격리는 수신 단위라 3→6).
- eks 오버레이와 values-eks 렌더링은 서버 dry-run 통과.

## 한계

- 재계산은 전체 재계산입니다. 증분 워터마크·late window는 후속입니다.
- 로컬은 브로커·DB 모두 단일 인스턴스입니다. 복제·장애 조치는 eks 오버레이에서 설정만 했습니다.
- Snowflake 적재 경로는 코드와 설정이 있으나, 자격 증명이 없어 이번 실행에서는 PostgreSQL marts만 확인했습니다.
- 인증·TLS가 없는 로컬 데모입니다. 실제 수집 API는 인증과 요청 제한이 필요합니다.
- ingress-nginx가 2026년 3월 지원 종료되어 로컬에서는 NodePort를 씁니다. EKS에서는 AWS Load Balancer Controller를 가정합니다.
