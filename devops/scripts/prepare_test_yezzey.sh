#!/bin/bash
set -ex

export accessKeyId="${ACCESS_KEY_ID:-some_key}"
export secretAccessKey="${SECRET_ACCESS_KEY:-some_key}"
export bucketName="${BUCKET_NAME:-gpyezzey}"
export s3endpoint="${S3_ENDPOINT:-http:\\/\\/minio:9000}"

mkdir -p /home/gpadmin/yezzey_test

cp devops/config/priv.gpg /home/gpadmin/yezzey_test/priv.gpg
cp devops/config/pub.gpg /home/gpadmin/yezzey_test/pub.gpg

gpg --import /home/gpadmin/yezzey_test/pub.gpg
gpg --import /home/gpadmin/yezzey_test/priv.gpg

cp -f devops/config/yproxy.conf /tmp/yproxy.yaml
sed -i "s/\$AWS_ACCESS_KEY_ID/${accessKeyId}/g" /tmp/yproxy.yaml
sed -i "s/\$AWS_SECRET_ACCESS_KEY/${secretAccessKey}/g" /tmp/yproxy.yaml
sed -i "s/\$AWS_ENDPOINT/${s3endpoint}/g" /tmp/yproxy.yaml
sed -i "s/\$WALG_S3_PREFIX/${bucketName}\/yezzey-test-files/g" /tmp/yproxy.yaml

