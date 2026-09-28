# Snowflake 적재 실행 (무료 체험 계정)

재계산 작업은 PostgreSQL marts를 항상 만들고, Secret `glp-snowflake`가 있으면 같은 SQL로 Snowflake marts도 만든다. 체험 계정만 있으면 비용 없이 확인할 수 있다(체험 크레딧 사용, 아래 설정은 월 1크레딧에서 창고를 멈춘다).

## 1. 계정 (직접)

https://signup.snowflake.com 에서 30일 체험 계정을 만든다. 계정 식별자(`<orgname>-<accountname>`)를 적어 둔다.

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

PostgreSQL marts와 같은 값이면 두 대상이 같은 SQL을 받는다는 것이 확인된다. 결과 화면을 캡처해 두면 면접 자료로 쓸 수 있다.
