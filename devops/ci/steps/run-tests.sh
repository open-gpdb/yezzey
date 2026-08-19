#!/bin/bash
# Step: run yezzey regression tests (make installcheck).
#
# Launches yproxy in the background, sources the DB + gpdemo environment
# scripts and runs `make installcheck` inside the yezzey gpcontrib copy.
#
# Engine-specific regress target:
#   - cloudberry3 uses REGRESS=simple_cbdb
#   - others leave default REGRESS unset
set -eo pipefail
source "$(dirname "$0")/../env.sh"

launch_yproxy="${YEZZEY_SRC_DIR}/devops/scripts/launch_yproxy.sh"
chmod +x "${launch_yproxy}"

yezzey_dst="${WORKSPACE}/${DB_GPCONTRIB_DIR}/yezzey"

regress_env=()
if [[ -n "${REGRESS_TARGET}" ]]; then
  regress_env+=(REGRESS="${REGRESS_TARGET}")
fi
if [[ "${ENGINE}" == cloudberry* ]]; then
  regress_env+=(IS_CLOUDBERRY=true)
fi

run_as_gpadmin "${DB_SRC_DIR}" -- \
  "${launch_yproxy}"

# Installcheck needs to run from the gpcontrib/yezzey copy.
# We build the full command as a string because su - gpadmin -c expects a shell.
cmd="cd '${yezzey_dst}'"
cmd+=" && source '${DB_ENV_SCRIPT}'"
cmd+=" && source '${DB_GPDEMO_SCRIPT}'"
for kv in "${regress_env[@]}"; do
  cmd+=" && export ${kv}"
done
cmd+=" && make installcheck"

if ! su - "${GPADMIN_USER}" -c "${cmd}"; then
  echo "::error::Test yezzey failed"
  cat "${yezzey_dst}/regression.diffs" 2>/dev/null || true
  exit 1
fi

ci_log "Tests passed"
