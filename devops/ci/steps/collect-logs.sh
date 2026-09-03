#!/bin/bash
# Step: collect logs / artifacts after a test run.
#
# Bundles regression outputs, yproxy.log and the gpdemo datadir logs into a
# single artifact directory so the uploader step (CI or local) can simply
# `cp` it. Idempotent: safe to call on success or failure.
set -eo pipefail
source "$(dirname "$0")/../env.sh"

artifacts="${WORKSPACE}/artifacts"
mkdir -p "${artifacts}"

# regression outputs from the gpcontrib copy.
yezzey_dst="${WORKSPACE}/${DB_GPCONTRIB_DIR}/yezzey"
if [[ -d "${yezzey_dst}" ]]; then
  cp -r "${yezzey_dst}"/regression.* "${artifacts}/" 2>/dev/null || true
  cp -r "${yezzey_dst}/results" "${artifacts}/" 2>/dev/null || true
fi

# yproxy log
cp "${WORKSPACE}/yproxy.log" "${artifacts}/" 2>/dev/null || true

# gpdemo datadir logs
datadirs_root="${DB_SRC_DIR}/gpAux/gpdemo/datadirs"
if [[ -d "${datadirs_root}" ]]; then
  for d in "${datadirs_root}"/*/log; do
    rel="${d#${datadirs_root}/}"
    mkdir -p "${artifacts}/datadirs/${rel}"
    cp -r "${d}/." "${artifacts}/datadirs/${rel}/" 2>/dev/null || true
  done
fi

# build-logs
cp -r "${LOGS_DIR}" "${artifacts}/" 2>/dev/null || true

ci_log "Artifacts collected to ${artifacts}"
