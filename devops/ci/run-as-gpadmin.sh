#!/bin/bash
# Run a command as the gpadmin user when inside the build container.
# Falls back to current user (local dev / CI runner) if gpadmin is absent.
#
# Usage: run_as_gpadmin <workdir> -- <command...>
#
# The function is also defined inline in env.sh so that step scripts which
# source env.sh get it automatically. This file is kept for direct CLI use.
set -eo pipefail
source "$(dirname "$0")/env.sh"

workdir="$1"
shift
[[ "$1" == "--" ]] && shift

run_as_gpadmin "${workdir}" -- "$@"
