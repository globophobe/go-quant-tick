#!/bin/bash
set -euo pipefail

metadata() {
  curl -fsSL -H "Metadata-Flavor: Google" \
    "http://metadata.google.internal/computeMetadata/v1/instance/attributes/$1"
}

optional_metadata() {
  metadata "$1" 2>/dev/null || true
}

ARTIFACT_PROJECT_ID="$(metadata ARTIFACT_PROJECT_ID)"
ARTIFACT_LOCATION="$(metadata ARTIFACT_LOCATION)"
ARTIFACT_REPOSITORY="$(metadata ARTIFACT_REPOSITORY)"
ARTIFACT_PACKAGE="$(metadata ARTIFACT_PACKAGE)"
ARTIFACT_VERSION="$(metadata ARTIFACT_VERSION)"
ARTIFACT_NAME="$(metadata ARTIFACT_NAME)"
SERVICE_ENV="$(optional_metadata SERVICE_ENV)"

for value in ARTIFACT_PROJECT_ID ARTIFACT_LOCATION ARTIFACT_REPOSITORY ARTIFACT_PACKAGE ARTIFACT_VERSION ARTIFACT_NAME; do
  if [ -z "${!value}" ]; then
    echo "${value} metadata is required" >&2
    exit 1
  fi
done

systemctl disable --now ssh.service ssh.socket sshd.service sshd.socket >/dev/null 2>&1 || true
systemctl mask ssh.service ssh.socket sshd.service sshd.socket >/dev/null 2>&1 || true
systemctl disable --now exim4.service exim4-base.timer >/dev/null 2>&1 || true
systemctl mask exim4.service >/dev/null 2>&1 || true

systemctl disable --now go-quant-tick.service >/dev/null 2>&1 || true
if command -v docker >/dev/null 2>&1; then
  docker rm -f go-quant-tick >/dev/null 2>&1 || true
fi
systemctl disable --now docker.service docker.socket containerd.service >/dev/null 2>&1 || true

apt-get update
apt-get install -y ca-certificates curl google-cloud-cli

install -d -m 0755 /opt/go-quant-tick /usr/local/bin /etc/go-quant-tick
download_dir="$(mktemp -d)"
trap 'rm -rf "${download_dir}"' EXIT

echo "Downloading ${ARTIFACT_PACKAGE}/${ARTIFACT_VERSION}/${ARTIFACT_NAME}"
gcloud artifacts generic download \
  --project "${ARTIFACT_PROJECT_ID}" \
  --location "${ARTIFACT_LOCATION}" \
  --repository "${ARTIFACT_REPOSITORY}" \
  --package "${ARTIFACT_PACKAGE}" \
  --version "${ARTIFACT_VERSION}" \
  --destination "${download_dir}"

artifact_path="$(find "${download_dir}" -type f -name "${ARTIFACT_NAME}" -print -quit)"
if [ -z "${artifact_path}" ]; then
  echo "Downloaded artifact ${ARTIFACT_NAME} was not found under ${download_dir}" >&2
  find "${download_dir}" -maxdepth 4 -type f -print >&2
  exit 1
fi

install -m 0755 "${artifact_path}" /usr/local/bin/quanttick
printf '%s\n' "${SERVICE_ENV}" > /etc/go-quant-tick/env
chmod 0640 /etc/go-quant-tick/env

cat >/etc/systemd/system/go-quant-tick.service <<SERVICE
[Unit]
Description=go-quant-tick websocket collector
After=network-online.target
Wants=network-online.target

[Service]
Restart=always
RestartSec=5
EnvironmentFile=-/etc/go-quant-tick/env
ExecStart=/usr/local/bin/quanttick
TimeoutStopSec=30

[Install]
WantedBy=multi-user.target
SERVICE

systemctl daemon-reload
systemctl enable go-quant-tick.service
systemctl restart go-quant-tick.service
