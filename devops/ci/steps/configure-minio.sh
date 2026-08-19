#!/bin/bash
# Step: install MinIO client (mc) and configure the MinIO service alias + buckets.
#
# Replaces the duplicated "Install MinIO Client (mc)" and "Configure MinIO
# service" steps across all engine jobs.
set -eo pipefail
source "$(dirname "$0")/../env.sh"

if ! command -v mc >/dev/null 2>&1; then
  ci_log "Installing mc"
  curl -fsSL -o /tmp/mc https://dl.min.io/client/mc/release/linux-amd64/mc
  chmod +x /tmp/mc
  if command -v sudo >/dev/null 2>&1; then
    sudo mv /tmp/mc /usr/local/bin/mc
  else
    mv /tmp/mc /usr/local/bin/mc
  fi
fi

ci_log "Configuring MinIO alias '${MINIO_ALIAS}' -> ${MINIO_ENDPOINT}"
mc alias set "${MINIO_ALIAS}" "${MINIO_ENDPOINT}" "${MINIO_ROOT_USER}" "${MINIO_ROOT_PASSWORD}"
mc admin info "${MINIO_ALIAS}"

for bucket in ${MINIO_BUCKETS}; do
  ci_log "Creating bucket '${bucket}'"
  mc mb "${MINIO_ALIAS}/${bucket}" || ci_log "bucket ${bucket} already exists"
done

ci_log "MinIO ready"
