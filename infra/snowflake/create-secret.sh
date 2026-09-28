#!/usr/bin/env bash
# Creates the Kubernetes Secret the rebuild job reads. Nothing here is written into Git.
# Usage (inside WSL, repo root): SNOWFLAKE_ACCOUNT=<orgname-accountname> bash infra/snowflake/create-secret.sh
set -euo pipefail
: "${SNOWFLAKE_ACCOUNT:?set SNOWFLAKE_ACCOUNT, e.g. myorg-myaccount}"
KEY=${KEY:-$HOME/.glp/glp_key.p8}
kubectl -n glp create secret generic glp-snowflake \
  --from-literal=account="$SNOWFLAKE_ACCOUNT" \
  --from-literal=user=GLP_SERVICE \
  --from-literal=warehouse=GLP_WH \
  --from-literal=database=GLP \
  --from-literal=schema=MARTS \
  --from-file=private_key="$KEY" \
  --dry-run=client -o yaml | kubectl apply -f -
echo "Secret glp-snowflake updated. Trigger the glp_rebuild DAG in Airflow (http://localhost:30082)."
