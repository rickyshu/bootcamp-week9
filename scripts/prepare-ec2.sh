#!/usr/bin/env bash
# EC2 Ubuntu 22.04에서 실행. AWS 자격 증명은 설치하거나 복사하지 않습니다.
set -euo pipefail
if ! grep -q 'VERSION_ID="22.04"' /etc/os-release; then
  echo 'Ubuntu 22.04에서 실행하세요.' >&2
  exit 1
fi
printf '%s\n' \
  'Acquire::ForceIPv4 "true";' \
  'Acquire::http::Timeout "20";' \
  'Acquire::https::Timeout "20";' \
  'Acquire::Retries "2";' \
  'Acquire::IndexTargets::deb::CNF::DefaultEnabled "false";' \
  | sudo tee /etc/apt/apt.conf.d/99-de9-network >/dev/null
sudo sed -i \
  's|http://security.ubuntu.com/ubuntu|http://ap-northeast-2.ec2.archive.ubuntu.com/ubuntu|g' \
  /etc/apt/sources.list
sudo apt-get update
sudo apt-get install -y ca-certificates curl git unzip awscli
sudo install -m 0755 -d /etc/apt/keyrings
curl -fsSL https://download.docker.com/linux/ubuntu/gpg | sudo tee /etc/apt/keyrings/docker.asc >/dev/null
sudo chmod a+r /etc/apt/keyrings/docker.asc
printf '%s\n' \
  "deb [arch=$(dpkg --print-architecture) signed-by=/etc/apt/keyrings/docker.asc] https://download.docker.com/linux/ubuntu jammy stable" \
  | sudo tee /etc/apt/sources.list.d/docker.list >/dev/null
sudo apt-get update
sudo apt-get install -y docker-ce docker-ce-cli containerd.io docker-buildx-plugin docker-compose-plugin
sudo usermod -aG docker "$USER"
echo '설치 완료. SSH 재접속 후 docker compose version을 확인하세요.'
