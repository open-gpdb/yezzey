#!/bin/bash
# Step: initialize the build container environment.
#
# Mirrors the "Cloudberry Environment Initialization" / "GPDB Environment
# Initialization" steps from the original workflow. Runs the container's
# /tmp/init_system.sh as gpadmin, prepares build-logs and environment dump.
set -eo pipefail
source "$(dirname "$0")/env.sh"

if [[ -x /tmp/init_system.sh ]]; then
  if ! su - "${GPADMIN_USER}" -c "/tmp/init_system.sh"; then
    ci_err "Container initialization failed"
  fi
else
  ci_log "No /tmp/init_system.sh found, skipping container init"
fi

mkdir -p "${DB_SRC_DIR}/build-logs"
chown -R "${GPADMIN_USER}:${GPADMIN_USER}" "${DB_SRC_DIR}/build-logs" 2>/dev/null || true
mkdir -p "${LOGS_DIR}/details"
chown -R "${GPADMIN_USER}:${GPADMIN_USER}" "${WORKSPACE}" 2>/dev/null || true
chmod -R 755 "${WORKSPACE}" 2>/dev/null || true
chmod 777 "${LOGS_DIR}" 2>/dev/null || true

{
  echo "=== Environment Information ==="
  uname -a
  df -h
  free -h
  env | sort
} | tee -a "${LOGS_DIR}/details/environment.log"

df -h | tee -a "${LOGS_DIR}/details/disk-usage.log"
free -h | tee -a "${LOGS_DIR}/details/memory-usage.log"

ci_log "Environment initialized for ${ENGINE}"
