#!/usr/bin/env bash
set -euo pipefail

if [[ $EUID -ne 0 ]]; then
  echo "Execute este script como root ou com sudo." >&2
  exit 1
fi

if ! grep -qi '^ID=ubuntu' /etc/os-release; then
  echo "Este script foi preparado para Ubuntu." >&2
  exit 1
fi

export DEBIAN_FRONTEND=noninteractive

echo "Instalando Docker no Ubuntu..."

apt-get update
apt-get install -y ca-certificates curl gnupg lsb-release ufw

install -m 0755 -d /etc/apt/keyrings
curl -fsSL https://download.docker.com/linux/ubuntu/gpg | gpg --dearmor --yes -o /etc/apt/keyrings/docker.gpg
chmod a+r /etc/apt/keyrings/docker.gpg

echo \
  "deb [arch=$(dpkg --print-architecture) signed-by=/etc/apt/keyrings/docker.gpg] https://download.docker.com/linux/ubuntu \
  $(. /etc/os-release && echo "$VERSION_CODENAME") stable" | tee /etc/apt/sources.list.d/docker.list > /dev/null

apt-get update
apt-get install -y docker-ce docker-ce-cli containerd.io docker-buildx-plugin docker-compose-plugin

systemctl enable docker
systemctl start docker

if [[ -n "${SUDO_USER:-}" ]] && id -u "$SUDO_USER" >/dev/null 2>&1; then
  usermod -aG docker "$SUDO_USER"
  echo "Usuario $SUDO_USER adicionado ao grupo docker."
elif [[ "${USER:-}" != "root" ]] && id -u "$USER" >/dev/null 2>&1; then
  usermod -aG docker "$USER"
  echo "Usuario $USER adicionado ao grupo docker."
else
  echo "Executando como root. Nenhum usuario comum foi adicionado ao grupo docker."
fi

if command -v ufw >/dev/null 2>&1; then
  ufw allow 22/tcp >/dev/null 2>&1 || true
  ufw --force enable >/dev/null 2>&1 || true
fi

echo "Instalacao concluida."
echo "Verifique com: docker --version"
echo "Verifique o compose com: docker compose version"
echo "Para usar o docker sem sudo, faca logout/login ou rode: newgrp docker"
