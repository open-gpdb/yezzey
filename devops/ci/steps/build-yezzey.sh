#!/bin/bash
# Step: build yezzey inside the DB gpcontrib tree and install it.
#
# Copies the yezzey source into <db>/gpcontrib/yezzey, chowns it to gpadmin
# and runs `make && make install`.
set -eo pipefail
source "$(dirname "$0")/../env.sh"

gpcontrib_dir="${WORKSPACE}/${DB_GPCONTRIB_DIR}"
yezzey_dst="${gpcontrib_dir}/yezzey"

ci_log "Moving yezzey -> ${yezzey_dst}"
rm -rf "${yezzey_dst}"
cp -r "${YEZZEY_SRC_DIR}" "${yezzey_dst}"
chown -R "${GPADMIN_USER}:${GPADMIN_USER}" "${yezzey_dst}" 2>/dev/null || true

run_as_gpadmin "${yezzey_dst}" -- make
run_as_gpadmin "${yezzey_dst}" -- make install

ci_log "Build yezzey done"
