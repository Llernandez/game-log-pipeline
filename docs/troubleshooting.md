# 트러블슈팅 기록

로컬 kind 클러스터에서 GitOps 배포를 구성하며 만난 문제와 해결입니다. 설정 변경은 모두 Git에 반영해 클러스터를 지우고 다시 만들어도 재발하지 않게 했습니다.

## 파드에서만 GitHub에 접속되지 않는 문제

**증상.** Argo CD가 Git 저장소를 가져오지 못하고 `context deadline exceeded`로 실패했습니다. 같은 노드에서 직접 접속하면 정상이었습니다.

**분석.**

1. 노드와 파드의 차이를 좁혀 보니 DNS 설정만 달랐습니다. 파드는 호스트의 DNS search 도메인과 `ndots:5`를 물려받습니다.
2. `ndots:5`이면 점이 5개 미만인 `github.com`을 먼저 `github.com.<search 도메인>`으로 조회합니다.
3. 일부 ISP DNS는 존재하지 않는 이름에도 주소를 돌려줍니다. 그래서 파드는 GitHub가 아닌 엉뚱한 주소로 연결하고 있었습니다. 노드는 `ndots:0`이라 재현되지 않았습니다.

**해결.** search 줄을 뺀 resolv.conf를 kubelet `resolvConf`로 지정하고 `infra/local/bootstrap.sh`에 넣었습니다.

**배운 점.** "노드에선 되는데 파드에선 안 된다"는 증상은 네트워크 경로보다 이름 해석 단계부터 의심해야 합니다.

## 그 밖에 해결한 문제

| 증상 | 원인 | 해결 |
|---|---|---|
| Kafka 앱이 몇 분마다 OutOfSync | API 서버가 큰 CRD를 정규화해 클라이언트 측 비교가 끝나지 않음 | 해당 Application에 `ServerSideDiff=true` |
| Airflow 앱이 계속 OutOfSync | 차트의 완료 Job을 TTL이 지우면서 Git과 달라짐 | 사용자 생성·마이그레이션 Job을 Argo CD Sync 훅으로 실행 |
| VPN 사용 시 통신 불안정 | WSL MTU 1280, kind 네트워크 1500 | kind 네트워크를 호스트 MTU로 생성 |
| ACCOUNTADMIN으로 Snowflake marts 조회 불가 | marts는 서비스 역할 `GLP_LOADER` 소유인데 상위 역할에 연결되지 않음 | `GRANT ROLE GLP_LOADER TO ROLE SYSADMIN` (Snowflake 권장 역할 계층) |
| loader가 PostgreSQL보다 먼저 떠서 재시작 | 시작 순서 | 재시작 정책에 맡기고 DB 준비 후 자동 복구됨을 확인 |
