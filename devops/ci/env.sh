#!/bin/bash
# Common environment defaults for Yezzey CI.
#
# All variables can be overridden via the environment. The same file is
# sourced by every step script so that CI and local docker runs share the
# exact same configuration.
#
# Engine selection (ENGINE):
#   cloudberry2  -> Apache Cloudberry 2.x   (image apache/incubator-cloudberry:cbdb-build-ubuntu22.04-latest)
#   cloudberry3  -> Apache Cloudberry 3.x   (image apache/incubator-cloudberry:cbdb-build-ubuntu22.04-latest, branch main)
#   gpdb         -> OpenGPDB               (image ghcr.io/open-gpdb/gpdb-env:jammy-latest)
#
# The engine choice drives DB_REPO, DB_REF, DB_GPCONTRIB_DIR, DB_ENV_SCRIPT
# and DB_GPDEMO_SCRIPT.

set -eo pipefail

# ---------------------------------------------------------------------------
# Engine selection
# ---------------------------------------------------------------------------
: "${ENGINE:=cloudberry2}"

case "${ENGINE}" in
  cloudberry2)
    DB_REPO="${DB_REPO:-open-gpdb/cloudberry}"
    DB_REF="${DB_REF:-REL_2_STABLE}"
    DB_GPCONTRIB_DIR="${DB_GPCONTRIB_DIR:-cloudberry/gpcontrib}"
    DB_ENV_SCRIPT="${DB_ENV_SCRIPT:-/usr/local/cloudberry-db/cloudberry-env.sh}"
    DB_GPDEMO_SCRIPT="${DB_GPDEMO_SCRIPT:-../../gpAux/gpdemo/gpdemo-env.sh}"
    DB_SRC_DIR_NAME="${DB_SRC_DIR_NAME:-cloudberry}"
    DB_CREATE_DEMO_SCRIPT="${DB_CREATE_DEMO_SCRIPT:-create_demo_yezzey_cloudberry.sh}"
    REGRESS_TARGET="${REGRESS_TARGET:-}"
    ;;
  cloudberry3)
    DB_REPO="${DB_REPO:-open-gpdb/cloudberry}"
    DB_REF="${DB_REF:-main}"
    DB_GPCONTRIB_DIR="${DB_GPCONTRIB_DIR:-cloudberry/gpcontrib}"
    DB_ENV_SCRIPT="${DB_ENV_SCRIPT:-/usr/local/cloudberry-db/cloudberry-env.sh}"
    DB_GPDEMO_SCRIPT="${DB_GPDEMO_SCRIPT:-../../gpAux/gpdemo/gpdemo-env.sh}"
    DB_SRC_DIR_NAME="${DB_SRC_DIR_NAME:-cloudberry}"
    DB_CREATE_DEMO_SCRIPT="${DB_CREATE_DEMO_SCRIPT:-create_demo_yezzey_cloudberry.sh}"
    REGRESS_TARGET="${REGRESS_TARGET:-simple_cbdb}"
    ;;
  gpdb)
    DB_REPO="${DB_REPO:-open-gpdb/gpdb}"
    DB_REF="${DB_REF:-OPENGPDB_STABLE}"
    DB_GPCONTRIB_DIR="${DB_GPCONTRIB_DIR:-gpdb/gpcontrib}"
    DB_ENV_SCRIPT="${DB_ENV_SCRIPT:-/opt/greenplum-db-6/greenplum_path.sh}"
    DB_GPDEMO_SCRIPT="${DB_GPDEMO_SCRIPT:-../../gpAux/gpdemo/gpdemo-env.sh}"
    DB_SRC_DIR_NAME="${DB_SRC_DIR_NAME:-gpdb}"
    DB_CREATE_DEMO_SCRIPT="${DB_CREATE_DEMO_SCRIPT:-create_demo_yezzey_gpdb.sh}"
    REGRESS_TARGET="${REGRESS_TARGET:-}"
    ;;
  *)
    echo "::error::Unknown ENGINE='${ENGINE}'. Supported: cloudberry2, cloudberry3, gpdb" >&2
    exit 2
    ;;
esac

export ENGINE DB_REPO DB_REF DB_GPCONTRIB_DIR DB_ENV_SCRIPT DB_GPDEMO_SCRIPT
export DB_SRC_DIR_NAME DB_CREATE_DEMO_SCRIPT REGRESS_TARGET

# ---------------------------------------------------------------------------
# Source layout (paths inside the CI workspace / container)
# ---------------------------------------------------------------------------
: "${WORKSPACE:=/workspace}"
: "${YEZZEY_SRC_DIR:=${WORKSPACE}/yezzey}"
: "${DB_SRC_DIR:=${WORKSPACE}/${DB_SRC_DIR_NAME}}"
: "${YPROXY_SRC_DIR:=${WORKSPACE}/yproxy}"
: "${DEVOPS_DIR:=${WORKSPACE}/gpdb-devops}"
: "${LOGS_DIR:=${WORKSPACE}/build-logs}"

export WORKSPACE YEZZEY_SRC_DIR DB_SRC_DIR YPROXY_SRC_DIR DEVOPS_DIR LOGS_DIR

# ---------------------------------------------------------------------------
# Build options
# ---------------------------------------------------------------------------
: "${ENABLE_DEBUG:=1}"
: "${CONFIGURE_EXTRA_OPTS:=}"
: "${BUILD_DESTINATION:=}"
export ENABLE_DEBUG CONFIGURE_EXTRA_OPTS BUILD_DESTINATION

# For GPDB the build destination is /opt/greenplum-db-6 (debian layout).
if [[ "${ENGINE}" == "gpdb" && -z "${BUILD_DESTINATION}" ]]; then
  BUILD_DESTINATION="/opt/greenplum-db-6"
  export BUILD_DESTINATION
fi

# ---------------------------------------------------------------------------
# MinIO / S3 configuration
# ---------------------------------------------------------------------------
: "${MINIO_ENDPOINT:=http://minio:9000}"
: "${MINIO_ROOT_USER:=some_key}"
: "${MINIO_ROOT_PASSWORD:=some_key}"
: "${MINIO_ALIAS:=minio-ci}"
: "${MINIO_BUCKETS:=gpyezzey gpyezzey2 gpyezzey3}"
export MINIO_ENDPOINT MINIO_ROOT_USER MINIO_ROOT_PASSWORD MINIO_ALIAS MINIO_BUCKETS

# yezzey test credentials (used by prepare_test_yezzey.sh)
: "${ACCESS_KEY_ID:=${MINIO_ROOT_USER}}"
: "${SECRET_ACCESS_KEY:=${MINIO_ROOT_PASSWORD}}"
: "${BUCKET_NAME:=gpyezzey}"
export ACCESS_KEY_ID SECRET_ACCESS_KEY BUCKET_NAME

# ---------------------------------------------------------------------------
# Cluster user
# ---------------------------------------------------------------------------
: "${GPADMIN_USER:=gpadmin}"
export GPADMIN_USER

# ---------------------------------------------------------------------------
# Logging helpers
# ---------------------------------------------------------------------------
mkdir -p "${LOGS_DIR}/details"

ci_log() { echo "[ci:${ENGINE}] $*" | tee -a "${LOGS_DIR}/ci.log"; }
ci_err() { echo "::error::$*" >&2; exit 1; }
