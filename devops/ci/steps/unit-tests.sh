#!/bin/bash
# Step: unit tests (googletest), optionally under ASan + UBSan.
#
# Usage:
#   unit-tests.sh            -> plain
#   SANITIZE=1 unit-tests.sh -> ASan + UBSan
#
# Works both on the GitHub runner (apt-get needs sudo) and inside the
# builder containers (running as root, no sudo).
set -eo pipefail
source "$(dirname "$0")/../env.sh"

if command -v sudo >/dev/null 2>&1 && [[ "$(id -u)" -ne 0 ]]; then
  APT="sudo apt-get"
else
  APT="apt-get"
fi

$APT update
$APT install -y --no-install-recommends \
  g++ make libssl-dev libxml2-dev libcurl4-openssl-dev zlib1g-dev

# Fetch googletest (release-1.11.0 is the last one that builds with -std=c++11).
if [[ ! -d "${YEZZEY_SRC_DIR}/test/googletest" ]]; then
  git clone --depth 1 --branch release-1.11.0 \
    https://github.com/google/googletest.git "${YEZZEY_SRC_DIR}/test/googletest"
fi

if [[ "${SANITIZE:-0}" == "1" ]]; then
  export ASAN_OPTIONS=detect_leaks=1:abort_on_error=1:color=always
  export UBSAN_OPTIONS=print_stacktrace=1:abort_on_error=1:color=always
  make -C "${YEZZEY_SRC_DIR}/test" sanitize
else
  make -C "${YEZZEY_SRC_DIR}/test" test
fi

ci_log "Unit tests OK (sanitize=${SANITIZE:-0})"
