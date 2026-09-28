# Changelog

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
