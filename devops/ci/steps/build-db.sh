#!/bin/bash
# Step: build the database engine (Cloudberry / GPDB) from source.
set -eo pipefail
source "$(dirname "$0")/../env.sh"

if [[ "${ENGINE}" == "gpdb" ]]; then
  build_script="${DEVOPS_DIR}/build_automation/gpdb/scripts/build-gpdb.sh"
  chmod +x "${build_script}"
  run_as_gpadmin "${DB_SRC_DIR}" -- \
    SRC_DIR="${DB_SRC_DIR}" \
    BUILD_DESTINATION="${BUILD_DESTINATION}" \
    "${build_script}"

  # Copy runtime shared libraries into the install prefix.
  cp /usr/local/lib/libsigar.so "${BUILD_DESTINATION}/lib"
  cp /usr/local/lib/libxerces* "${BUILD_DESTINATION}/lib"
else
  build_script="${DB_SRC_DIR}/devops/build/automation/cloudberry/scripts/build-cloudberry.sh"
  chmod +x "${build_script}"
  run_as_gpadmin "${DB_SRC_DIR}" -- \
    SRC_DIR="${DB_SRC_DIR}" \
    "${build_script}"
fi

ci_log "Build ${ENGINE} done"
