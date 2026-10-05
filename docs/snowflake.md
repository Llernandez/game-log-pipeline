# Snowflake 적재 실행 (무료 체험 계정)

재계산 작업은 PostgreSQL marts를 만들고, Snowflake 접속 설정이 있으면 같은 집계 SQL로 Snowflake marts도 만든다. 아래 실행 기록은 체험 크레딧을 사용했다. 리소스 모니터는 월 1크레딧을 기준으로 웨어하우스를 중지하도록 설정했다. 실행 중인 쿼리와 중지 지연 때문에 이 값을 초과할 수 있으며, 전체 계정 비용의 정확한 상한은 아니다. [리소스 모니터의 제한](https://docs.snowflake.com/en/user-guide/resource-monitors)

## 1. 계정 (직접)

[가입 페이지](https://signup.snowflake.com)에서 체험 기간과 크레딧 조건을 확인하고 계정을 만든다. 계정 식별자(`<orgname>-<accountname>`)를 적어 둔다.

## 2. 키 페어 (WSL)

서비스 사용자는 비밀번호 대신 키 페어로 접속한다. 개인 키는 이 PC와 Kubernetes Secret에만 둔다.

```sh
mkdir -p ~/.glp && cd ~/.glp
openssl genrsa 2048 | openssl pkcs8 -topk8 -inform PEM -out glp_key.p8 -nocrypt
openssl rsa -in glp_key.p8 -pubout -out glp_key.pub
chmod 600 glp_key.p8
```

## 3. 객체 생성 (Snowsight 워크시트)

[`infra/snowflake/setup.sql`](../infra/snowflake/setup.sql)의 공개 키 자리에 `glp_key.pub`의 BEGIN/END 사이 본문을 붙여 실행한다. 역할·XSMALL 창고(60초 후 자동 정지)·DB·스키마·서비스 사용자·리소스 모니터를 만든다.

## 4. Secret과 실행

```sh
SNOWFLAKE_ACCOUNT=<orgname-accountname> bash infra/snowflake/create-secret.sh
```

Airflow에서 `glp_rebuild` DAG를 실행한다. 재계산 파드가 Secret을 읽어 Snowflake에도 marts를 만든다.

## 5. 확인

```sql
SELECT COUNT(*) FROM GLP.MARTS.CLEAN_EVENTS;   -- 합성 fixture 기준 6
SELECT * FROM GLP.MARTS.ANOMALY_CANDIDATES;     -- offline_policy_exceeded, duplicate_reward_claim
```

두 대상은 `game_log_pipeline/marts.sql`의 집계 정의를 사용한다. 같은 입력에 대해 행 수뿐 아니라 이상 후보의 키와 금액도 비교한다. 아래 표는 해당 합성 입력에서 결과가 일치한 기록이며, 모든 데이터와 SQL 동작의 동등성을 증명하지는 않는다.

## 실행 결과 (2026-09-29, v0.3.0 재화 전용 입력, 체험 계정, AWS)

- 키 페어로 `GLP_SERVICE` 접속: 역할 `GLP_LOADER`, 창고 `GLP_WH`, `GLP.MARTS` 확인.
- Secret 생성 후 Airflow `glp_rebuild` 수동 실행 → `rebuild_marts` success.
- 같은 RAW(kind 클러스터에 누적된 데모 전송 3회분)에서 두 대상의 결과가 일치한다.

| 항목 | PostgreSQL | Snowflake |
|---|---:|---:|
| clean_events | 6 | 6 |
| quarantine | 9 | 9 |
| duplicate_reward_claim:p_repeat | 2 | 2 |
| offline_policy_exceeded:p_excess | 1 | 1 |

격리 9건은 당시 재화 전용 fixture를 세 번 보낸 결과(회당 3건)다. 업무 집계와 이상 후보는 재전송에도 변하지 않는다. 현재 `demo`에는 전투 시도가 추가됐으므로 [현재 기대값](../README.md#로컬-실행)과 구분한다.
