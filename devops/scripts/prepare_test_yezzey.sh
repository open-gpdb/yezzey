#!/bin/bash
set -ex

export accessKeyId=some_key
export secretAccessKey=some_key
export bucketName=gpyezzey
export s3endpoint="http:\\/\\/minio:9000"

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

