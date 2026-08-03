#!/bin/bash

set -euo pipefail

CLANG_FORMAT_BIN="${CLANG_FORMAT_BIN:-clang-format-18}"

# Sarcasm/run-clang-format needs the path to the clang-format binary.
exec /run-clang-format/run-clang-format.py \
    -r \
    --clang-format-executable "${CLANG_FORMAT_BIN}" \
    "$@"
