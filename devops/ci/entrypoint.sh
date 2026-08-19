#!/bin/bash
# devops/ci/entrypoint.sh
#
# Single entrypoint for the CI builder containers (both local docker and
# GitHub Actions `container:` jobs). Reads PIPELINE and ENGINE env vars and
# dispatches to the appropriate sequence of step scripts.
#
# Modes (PIPELINE):
#   fast         -> clang-tidy + unit-tests (no DB cluster)
#   integration  -> full end-to-end: init-env -> minio -> configure-db ->
#                   build-db -> build-yezzey -> deploy-config -> yproxy ->
#                   create-cluster -> run-tests -> collect-logs
#
# The same script is invoked by:
#   - docker compose -f devops/build/docker/docker-compose.ci.yaml --profile <engine> up
#   - .github/workflows/yezzey-ci.yaml  (container step: devops/ci/entrypoint.sh)
set -eo pipefail

# Locate devops/ci dir relative to this script.
CI_DIR="$(cd "$(dirname "$0")" && pwd)"
source "${CI_DIR}/env.sh"

STEPS_DIR="${CI_DIR}/steps"

run_step() {
  local name="$1"; shift
  local script="${STEPS_DIR}/${name}.sh"
  echo "::group::step:${name}"
  if [[ ! -x "${script}" ]]; then
    chmod +x "${script}" 2>/dev/null || true
  fi
  if ! time bash "${script}" "$@"; then
    echo "::error::step ${name} failed"
    exit 1
  fi
  echo "::endgroup::"
}

# ---------------------------------------------------------------------------
# Helper: clone the DB engine repo + yproxy repo into /workspace if missing
# (local docker runs don't have actions/checkout to do it for us).
# In CI the actions/checkout step already populated these directories.
# ---------------------------------------------------------------------------
clone_sources() {
  if [[ "${SKIP_CLONE:-0}" == "1" ]]; then
    ci_log "SKIP_CLONE=1 -> assuming sources already present"
    return
  fi

  if [[ ! -d "${DB_SRC_DIR}/.git" ]]; then
    ci_log "Cloning ${DB_REPO}@${DB_REF} -> ${DB_SRC_DIR}"
    git clone --depth 1 --branch "${DB_REF}" --recurse-submodules \
      "https://github.com/${DB_REPO}.git" "${DB_SRC_DIR}"
    chown -R "${GPADMIN_USER}:${GPADMIN_USER}" "${DB_SRC_DIR}" 2>/dev/null || true
  fi

  if [[ ! -d "${YPROXY_SRC_DIR}/.git" ]]; then
    ci_log "Cloning yproxy -> ${YPROXY_SRC_DIR}"
    git clone --depth 1 https://github.com/open-gpdb/yproxy.git "${YPROXY_SRC_DIR}"
    chown -R "${GPADMIN_USER}:${GPADMIN_USER}" "${YPROXY_SRC_DIR}" 2>/dev/null || true
  fi

  # GPDB build automation lives in a separate repo.
  if [[ "${ENGINE}" == "gpdb" && ! -d "${DEVOPS_DIR}/.git" ]]; then
    ci_log "Cloning gpdb-devops -> ${DEVOPS_DIR}"
    git clone --depth 1 https://github.com/open-gpdb/gpdb-devops.git "${DEVOPS_DIR}"
  fi
}

# ---------------------------------------------------------------------------
# Pipelines
# ---------------------------------------------------------------------------
pipeline_fast() {
  ci_log "Running fast pipeline (no DB cluster)"
  run_step clang-tidy
  run_step unit-tests
}

pipeline_integration() {
  ci_log "Running integration pipeline (${ENGINE})"

  clone_sources
  run_step init-env
  run_step configure-minio
  run_step configure-db
  run_step build-db
  run_step build-yezzey
  run_step deploy-yezzey-config
  run_step install-yproxy
  run_step create-cluster
  run_step run-tests
  run_step collect-logs
}

# ---------------------------------------------------------------------------
# Dispatch
# ---------------------------------------------------------------------------
case "${PIPELINE:-integration}" in
  fast)         pipeline_fast ;;
  integration)  pipeline_integration ;;
  *)
    ci_err "Unknown PIPELINE='${PIPELINE}'. Supported: fast, integration"
    ;;
esac

ci_log "Pipeline ${PIPELINE} (${ENGINE}) completed successfully"
