#!/bin/bash
# Step: clang-tidy const-correctness check.
#
# Uses the silkeh/clang:16 image semantics (or whatever CLANG_TIDY_IMAGE is
# set to). Runs `make -f Makefile.tidy tidy-check` inside the yezzey tree.
set -eo pipefail
source "$(dirname "$0")/../env.sh"

make -C "${YEZZEY_SRC_DIR}" -f Makefile.tidy tidy-check
ci_log "clang-tidy OK"
