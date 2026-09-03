#!/bin/bash
# Step: create the demo cluster with yezzey preloaded.
#
# Delegates to the existing per-engine script
# (create_demo_yezzey_cloudberry.sh / create_demo_yezzey_gpdb.sh) shipped in
# the yezzey tree.
set -eo pipefail
source "$(dirname "$0")/../env.sh"

create_script="${YEZZEY_SRC_DIR}/devops/scripts/${DB_CREATE_DEMO_SCRIPT}"
chmod +x "${create_script}"

run_as_gpadmin "${DB_SRC_DIR}" -- "${create_script}"
ci_log "Demo cluster created"
