#!/bin/bash
# Step: deploy yezzey test config (GPG keys + yproxy config templating).
#
# Replaces "Deploy yezzey config" job step. Calls the existing
# prepare_test_yezzey.sh from the yezzey tree.
set -eo pipefail
source "$(dirname "$0")/../env.sh"

# Export S3/yproxy credentials so prepare_test_yezzey.sh picks them up.
export accessKeyId="${ACCESS_KEY_ID}"
export secretAccessKey="${SECRET_ACCESS_KEY}"
export bucketName="${BUCKET_NAME}"
export s3endpoint="http:\\/\\/minio:9000"

chmod +x "${YEZZEY_SRC_DIR}/devops/scripts/prepare_test_yezzey.sh"
run_as_gpadmin "${YEZZEY_SRC_DIR}" -- \
  devops/scripts/prepare_test_yezzey.sh

ci_log "Deploy yezzey config done"
