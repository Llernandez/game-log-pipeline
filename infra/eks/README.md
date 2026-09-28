# EKS 설정 (생성하지 않음)

같은 Helm 차트와 Kustomize 오버레이를 EKS에 올릴 때의 차이만 정리했습니다. 비용 때문에 이 포트폴리오에서는 클러스터를 만들지 않았고, `values-eks.yaml`과 `overlays/eks`는 로컬 클러스터에서 서버 dry-run으로 검증했습니다.

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

Argo CD 애플리케이션은 `path: .../overlays/eks`, `valueFiles: [values-eks.yaml]`로 바꾼 별도 app 세트를 두는 방식을 권장합니다(환경별 폴더).
