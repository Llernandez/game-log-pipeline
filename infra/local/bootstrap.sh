#!/usr/bin/env bash
# Local GitOps bootstrap: kind cluster -> Argo CD -> root Application. After this, everything else
# (operators, Kafka, PostgreSQL, the pipeline chart, Airflow) is created by Argo CD from Git.
# Run from the repo root inside WSL: bash infra/local/bootstrap.sh
set -euo pipefail
cd "$(dirname "$0")/../.."
IMAGE=game-log-pipeline:0.3.0

# VPNs can lower the WSL MTU (1280 here); keep kind's network at the host MTU instead of 1500.
host_if=$(ip route show default | awk '{print $5; exit}')
host_mtu=$(cat "/sys/class/net/${host_if}/mtu" 2>/dev/null || echo 1500)
if ! kind get clusters | grep -qx glp; then
  docker network inspect kind >/dev/null 2>&1 || \
    docker network create kind --opt com.docker.network.driver.mtu="${host_mtu}" >/dev/null
  kind create cluster --config infra/local/kind-config.yaml
fi
# WSL can stop the distro when idle; bring the node back with Docker instead of leaving it exited.
docker update --restart=unless-stopped glp-control-plane >/dev/null
# Pods inherit the host's DNS search domains with ndots:5. Some ISP resolvers answer every unknown
# name, so github.com.<isp-suffix> "resolves" and Argo CD can't reach GitHub. Kubelet gets the
# node's nameservers without search domains.
docker exec glp-control-plane sh -c '
  grep "^nameserver" /etc/resolv.conf > /etc/kubelet-resolv.conf
  grep -q "^resolvConf:" /var/lib/kubelet/config.yaml || {
    echo "resolvConf: /etc/kubelet-resolv.conf" >> /var/lib/kubelet/config.yaml
    systemctl restart kubelet; }'
kubectl wait --for=condition=Ready node --all --timeout=180s
kubectl -n kube-system rollout restart deployment/coredns >/dev/null

# The local overlay pulls nothing for the app: build and side-load the image (GHCR on EKS).
docker build -q -t "$IMAGE" . >/dev/null
kind load docker-image "$IMAGE" --name glp >/dev/null

helm repo add argo https://argoproj.github.io/argo-helm >/dev/null 2>&1 || true
helm repo update argo >/dev/null
helm upgrade --install argocd argo/argo-cd --version 10.9.2 -n argocd --create-namespace \
  -f infra/local/argocd-values.yaml --wait --timeout 10m >/dev/null
kubectl apply -f deploy/argocd/root.yaml

# Airflow's metadata connection is derived from the operator-generated credentials; the password
# never enters Git. (On EKS: External Secrets Operator + AWS Secrets Manager instead.)
until kubectl -n glp get secret glp-db-app >/dev/null 2>&1; do sleep 10; done
user=$(kubectl -n glp get secret glp-db-app -o jsonpath='{.data.username}' | base64 -d)
pass=$(kubectl -n glp get secret glp-db-app -o jsonpath='{.data.password}' | base64 -d)
enc=$(python3 -c 'import sys, urllib.parse; print(urllib.parse.quote(sys.argv[1], safe=""))' "$pass")
kubectl -n glp create secret generic airflow-metadata \
  --from-literal=connection="postgresql+psycopg2://${user}:${enc}@glp-db-rw.glp:5432/airflow" \
  --dry-run=client -o yaml | kubectl apply -f - >/dev/null

echo "Argo CD  http://localhost:30081  user admin, password:"
echo "  kubectl -n argocd get secret argocd-initial-admin-secret -o jsonpath='{.data.password}' | base64 -d"
echo "Airflow  http://localhost:30082  (chart default admin user, local only)"
echo "Ingest   http://localhost:30080/readyz"
