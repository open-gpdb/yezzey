#!/bin/bash
# Build yproxy from a source tree and install the requested binaries.
#
# Usage: install_yproxy.sh [binary ...]   (default: yproxy)
#        SRC_DIR points at the yproxy checkout; it defaults to $PWD.
#
# The CI runs this inside the Apache Cloudberry build images, which cover
# Rocky Linux 8/9/10 and Ubuntu 22.04/24.04, so nothing here may assume a
# package manager.  In practice there is very little left to install: every
# one of those images already ships Go under /usr/local/go, and yproxy is
# plain Go apart from one cgo file that includes <time.h>, so the C toolchain
# the images carry for building Cloudberry is all it needs.  The package
# manager is only reached when an image turns up without a Go compiler.

set -eo pipefail

SRC_DIR=${SRC_DIR:-$(pwd)}

BINARIES=("$@")
if [[ ${#BINARIES[@]} -eq 0 ]]; then
    BINARIES=(yproxy)
fi

# /etc/profile.d/go.sh only runs for login shells, so pick the image's Go up
# by hand rather than relying on the inherited PATH.
if [[ -d /usr/local/go/bin ]]; then
    export PATH="${PATH}:/usr/local/go/bin"
fi

if ! command -v go > /dev/null; then
    echo "No Go compiler found in the image, installing one"

    # shellcheck disable=SC1091
    . /etc/os-release

    case "${ID}" in
        ubuntu | debian)
            sudo apt-get update
            sudo apt-get install -y --no-install-recommends software-properties-common
            sudo add-apt-repository -y ppa:longsleep/golang-backports
            sudo apt-get update
            sudo apt-get install -y --no-install-recommends golang-go
            ;;
        rocky | rhel | centos | almalinux)
            sudo dnf install -y golang
            ;;
        *)
            echo "::error::Cannot install Go on unsupported distribution '${ID}'"
            exit 1
            ;;
    esac
fi

go version

git config --global --add safe.directory "${SRC_DIR}"
cd "${SRC_DIR}"
make build

for binary in "${BINARIES[@]}"; do
    sudo install -m 0755 "devbin/${binary}" "/usr/bin/${binary}"
    # Check the installation
    "${binary}" --version
done
