#!/bin/bash
# devops/ci/run-ci.sh
#
# Thin wrapper around docker compose that drives the Yezzey CI pipeline
# locally. Used by the Makefile `ci-*` targets; can also be invoked directly:
#
#   devops/ci/run-ci.sh fast                 # format/tidy/unit only
#   devops/ci/run-ci.sh integration          # full cluster, default engine
#   ENGINE=gpdb devops/ci/run-ci.sh integration
#   ENGINE=cloudberry3 devops/ci/run-ci.sh integration
#   devops/ci/run-ci.sh shell cloudberry2    # drop into builder bash
#   devops/ci/run-ci.sh clean                # tear down compose stack
#
# Env knobs:
#   ENGINE      cloudberry2 | cloudberry3 | gpdb   (default: cloudberry2)
#   BUILD       --no-build to skip image build     (default: use cache)
#   COMPOSE     path to docker compose binary      (default: docker compose)
set -eo pipefail

CI_DIR="$(cd "$(dirname "$0")" && pwd)"
COMPOSE_FILE="${CI_DIR}/../build/docker/docker-compose.ci.yaml"
COMPOSE="${COMPOSE:-docker compose}"

action="${1:-integration}"
shift || true

ENGINE="${ENGINE:-cloudberry2}"
export ENGINE

run_compose() {
  ${COMPOSE} -f "${COMPOSE_FILE}" "$@"
}

case "${action}" in
  fast)
    run_compose --profile fast run --rm \
      -e PIPELINE=fast builder-fast
    ;;
  integration)
    case "${ENGINE}" in
      cloudberry2|cloudberry3|gpdb) ;;
      *) echo "ENGINE must be cloudberry2|cloudberry3|gpdb, got '${ENGINE}'" >&2; exit 2 ;;
    esac
    # Bring up minio + setup-minio first so the builder sees a healthy S3.
    run_compose --profile "${ENGINE}" up -d --wait minio setup-minio
    run_compose --profile "${ENGINE}" run --rm \
      -e PIPELINE=integration \
      -e ENGINE="${ENGINE}" \
      "builder-${ENGINE}"
    ;;
  shell)
    case "${ENGINE}" in
      cloudberry2|cloudberry3|gpdb) ;;
      *) echo "ENGINE must be cloudberry2|cloudberry3|gpdb, got '${ENGINE}'" >&2; exit 2 ;;
    esac
    run_compose --profile "${ENGINE}" up -d --wait minio setup-minio
    run_compose --profile "${ENGINE}" run --rm \
      --entrypoint /bin/bash \
      "builder-${ENGINE}"
    ;;
  clean|down)
    run_compose down -v --remove-orphans
    ;;
  *)
    echo "usage: run-ci.sh <fast|integration|shell|clean> [ENGINE=cloudberry2|cloudberry3|gpdb]" >&2
    exit 2
    ;;
esac
