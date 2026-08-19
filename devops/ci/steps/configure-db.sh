#!/bin/bash
# Step: configure the database engine (Cloudberry / GPDB).
#
# Calls the upstream configure script shipped inside the DB source tree
# (Cloudberry) or inside the gpdb-devops repo (GPDB).
set -eo pipefail
source "$(dirname "$0")/../env.sh"

if [[ "${ENGINE}" == "gpdb" ]]; then
  # Debian-style build layout for GPDB.
  rm -rf /opt/greenplum-db-6 2>/dev/null || true
  mkdir -p "${BUILD_DESTINATION}"
  chown -R "${GPADMIN_USER}:${GPADMIN_USER}" "${BUILD_DESTINATION}" 2>/dev/null || true
  configure_script="${DEVOPS_DIR}/build_automation/gpdb/scripts/configure-gpdb.sh"
  chmod +x "${configure_script}"
  run_as_gpadmin "${DB_SRC_DIR}" -- \
    SRC_DIR="${DB_SRC_DIR}" \
    ENABLE_DEBUG="${ENABLE_DEBUG}" \
    BUILD_DESTINATION="${BUILD_DESTINATION}" \
    "${configure_script}"
else
  configure_script="${DB_SRC_DIR}/devops/build/automation/cloudberry/scripts/configure-cloudberry.sh"
  chmod +x "${configure_script}"
  run_as_gpadmin "${DB_SRC_DIR}" -- \
    SRC_DIR="${DB_SRC_DIR}" \
    ENABLE_DEBUG="${ENABLE_DEBUG}" \
    CONFIGURE_EXTRA_OPTS="${CONFIGURE_EXTRA_OPTS}" \
    "${configure_script}"
fi

ci_log "Configure ${ENGINE} done"
