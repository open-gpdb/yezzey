#!/bin/bash

          set -eo pipefail
          set -x

          rm -rf /tmp/yproxy.sock
          #run yproxy in daemon mode
          mkdir  /tmp/data
          /usr/bin/yproxy -c /tmp/yproxy.yaml -ldebug > yproxy.log 2>&1 &

          i=0
          while (! [ -S /tmp/yproxy.sock ]) && [ $i -lt 20 ]; do sleep 1; i=$(($i+1)) ; done

          if (! [ -S /tmp/yproxy.sock ]); then
             echo "::error::Config yproxy failed"
             exit 1
          fi

          chmod 777 /tmp/yproxy.sock
