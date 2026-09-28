#!/usr/bin/env bash
# One-time setup of the dedicated WSL distro (for example "glp-k8s") that hosts the local kind cluster.
# Run as root inside the distro: wsl -d <distro> -u root -- bash /mnt/e/Portfolio/game-log-pipeline/infra/local/wsl-setup.sh
# Everything (Docker images, cluster volumes) lives in the distro's disk, which was imported on E:.
set -euo pipefail

if ! grep -q "systemd=true" /etc/wsl.conf 2>/dev/null; then
  printf '[boot]\nsystemd=true\n' > /etc/wsl.conf
  echo "systemd enabled; run 'wsl --terminate <distro>' and re-run this script."
  exit 0
fi

export DEBIAN_FRONTEND=noninteractive
apt-get update -qq
apt-get install -y -qq docker.io curl ca-certificates git jq python3-venv >/dev/null
systemctl enable --now docker

arch=amd64
bin=/usr/local/bin
latest() { curl -fsSL "https://api.github.com/repos/$1/releases/latest" | jq -r .tag_name; }

kubectl_v=$(curl -fsSL https://dl.k8s.io/release/stable.txt)
curl -fsSLo "$bin/kubectl" "https://dl.k8s.io/release/${kubectl_v}/bin/linux/${arch}/kubectl"
echo "$(curl -fsSL "https://dl.k8s.io/release/${kubectl_v}/bin/linux/${arch}/kubectl.sha256")  $bin/kubectl" | sha256sum -c -

kind_v=$(latest kubernetes-sigs/kind)
curl -fsSLo "$bin/kind" "https://github.com/kubernetes-sigs/kind/releases/download/${kind_v}/kind-linux-${arch}"
echo "$(curl -fsSL "https://github.com/kubernetes-sigs/kind/releases/download/${kind_v}/kind-linux-${arch}.sha256sum" | cut -d' ' -f1)  $bin/kind" | sha256sum -c -

helm_v=$(latest helm/helm)
tmp=$(mktemp -d)
curl -fsSLo "$tmp/helm.tgz" "https://get.helm.sh/helm-${helm_v}-linux-${arch}.tar.gz"
echo "$(curl -fsSL "https://get.helm.sh/helm-${helm_v}-linux-${arch}.tar.gz.sha256sum" | cut -d' ' -f1)  $tmp/helm.tgz" | sha256sum -c -
tar -xzf "$tmp/helm.tgz" -C "$tmp" && install "$tmp/linux-${arch}/helm" "$bin/helm"
rm -rf "$tmp"

argocd_v=$(latest argoproj/argo-cd)
curl -fsSLo "$bin/argocd" "https://github.com/argoproj/argo-cd/releases/download/${argocd_v}/argocd-linux-${arch}"
echo "$(curl -fsSL "https://github.com/argoproj/argo-cd/releases/download/${argocd_v}/cli_checksums.txt" | grep " argocd-linux-${arch}$" | cut -d' ' -f1)  $bin/argocd" | sha256sum -c -

chmod +x "$bin/kubectl" "$bin/kind" "$bin/argocd"
cat <<EOF
docker  $(docker version --format '{{.Server.Version}}')
kubectl ${kubectl_v}
kind    ${kind_v}
helm    ${helm_v}
argocd  ${argocd_v}
EOF
