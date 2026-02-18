#!/bin/bash

set -ex

          export GPHOME=/usr/local/cloudberry-db
          source $GPHOME/cloudberry-env.sh
          ulimit -n 65536
          make destroy-demo-cluster && make create-demo-cluster
          export USER=gpadmin
          source gpAux/gpdemo/gpdemo-env.sh

          gpconfig -c shared_preload_libraries -v yezzey

          gpstop -a -i && gpstart -a

          createdb $USER

          gpconfig -c yezzey.yproxy_socket -v "/tmp/yproxy.sock"
          psql -c "ALTER SYSTEM SET yezzey.use_gpg_crypto TO false"
          gpconfig -c yezzey.use_otm_feature -v "true"
          gpconfig -c yezzey.use_gpg_crypto -v "false"

          gpstop -a -i && gpstart -a
