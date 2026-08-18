#!/bin/bash
# Run a command as the gpadmin user when inside the build container.
# Falls back to current user (local dev / CI runner) if gpadmin is absent.
#
# Usage: run_as_gpadmin <workdir> -- <command...>
set -eo pipefail

workdir="$1"
shift
[[ "$1" == "--" ]] && shift

if id gpadmin >/dev/null 2>&1; then
  su - "${GPADMIN_USER:-gpadmin}" -c "cd '${workdir}' && $*"
else
  ( cd "${workdir}" && "$@" )
fi
