# EKS 설정 (생성하지 않음)

같은 Helm 차트와 Kustomize 오버레이를 EKS에 올릴 때의 차이만 정리했습니다. 비용 때문에 이 포트폴리오에서는 클러스터를 만들지 않았습니다. `values-eks.yaml`과 `overlays/eks`는 로컬 클러스터에서 서버 dry-run으로 검증했습니다.

## 배포 전 미해결 사항

현재 `values-eks.yaml`은 로더 3개를 지정하지만 증분 재계산은 단일 로더의 커밋 순서를 전제로 합니다. 큰 RAW ID가 먼저 커밋돼 처리 위치가 이동하면 나중에 커밋된 작은 ID가 빠질 수 있습니다. 렌더링·dry-run 통과로 이 문제가 해결되지는 않습니다.

먼저 로더를 하나로 제한하고 배포 중 중첩 실행도 방지하거나, 병렬 커밋에 맞게 처리 위치를 설계하고 검증해야 합니다. 이번 문서 수정에서는 배포 설정을 바꾸지 않았습니다.

## 환경별 구성

| 영역 | 로컬(kind) | EKS |
|---|---|---|
| 클러스터 | `infra/local/kind-config.yaml` | `cluster.yaml` (eksctl) |
| 스토리지 | local-path | EBS CSI 드라이버 + `gp3` StorageClass |
| Kafka | 1 노드, RF 1 | 3 노드, RF 3, min ISR 2 (`deploy/platform/kafka/overlays/eks`) |
| PostgreSQL | 1 인스턴스 | 3 인스턴스 (`deploy/platform/postgres/overlays/eks`) |
| 앱 이미지 | `kind load` | GHCR (`ghcr.io/llernandez/game-log-pipeline`) |
| 수집 API 노출 | NodePort 30080 | AWS Load Balancer Controller + Ingress(ALB) |
| 수집 API 확장 | 1 replica | HPA 2–6 (metrics-server 필요) |
| 비밀 값 | 운영자가 생성한 Secret 참조, Airflow 메타데이터는 bootstrap이 파생 | External Secrets Operator + AWS Secrets Manager, Pod 권한은 IRSA/Pod Identity |
| Snowflake | 선택 | `glp-snowflake` Secret을 ESO로 동기화 |

Argo CD 애플리케이션은 별도 app 세트를 두는 방식(환경별 폴더)을 권장합니다. 이 세트에서는 `path: .../overlays/eks`, `valueFiles: [values-eks.yaml]`로 바꿉니다.
