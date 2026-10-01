#!/bin/bash
set -ex

export accessKeyId=some_key
export secretAccessKey=some_key
export bucketName=gpyezzey
export s3endpoint="http:\\/\\/minio:9000"

script_dir="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"

# The test keypair is generated on the fly instead of being stored in the
# repository, see generate_test_gpg_keys.sh.
"${script_dir}/generate_test_gpg_keys.sh" /home/gpadmin/yezzey_test

cp -f "${script_dir}/../config/yproxy.conf" /tmp/yproxy.yaml
sed -i "s/\$AWS_ACCESS_KEY_ID/${accessKeyId}/g" /tmp/yproxy.yaml
sed -i "s/\$AWS_SECRET_ACCESS_KEY/${secretAccessKey}/g" /tmp/yproxy.yaml
sed -i "s/\$AWS_ENDPOINT/${s3endpoint}/g" /tmp/yproxy.yaml
sed -i "s/\$WALG_S3_PREFIX/${bucketName}\/yezzey-test-files/g" /tmp/yproxy.yaml

