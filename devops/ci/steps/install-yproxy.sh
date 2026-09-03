#!/bin/bash
# Step: build and install yproxy.
#
# Replaces the "Install yproxy" step. Installs Go toolchain + deps, builds
# yproxy and moves the binary to /usr/bin/yproxy.
#
# Uses sudo when available and not running as root (GitHub runner); falls
# back to direct invocation inside the builder containers (root).
set -eo pipefail
source "$(dirname "$0")/../env.sh"

if command -v sudo >/dev/null 2>&1 && [[ "$(id -u)" -ne 0 ]]; then
  APT="sudo apt-get"
  SUDO="sudo"
else
  APT="apt-get"
  SUDO=""
fi

# Install Go toolchain if missing.
if ! command -v go >/dev/null 2>&1; then
  ci_log "Installing Go toolchain"
  $APT update
  $APT install -y software-properties-common
  $SUDO add-apt-repository -y ppa:longsleep/golang-backports
  $APT update
  $APT install -y golang-go
fi

$APT install -y libbrotli-dev liblzo2-dev libsodium-dev curl cmake

git config --global --add safe.directory "${YPROXY_SRC_DIR}"
( cd "${YPROXY_SRC_DIR}" && make build )
$SUDO mv "${YPROXY_SRC_DIR}/devbin/yproxy" /usr/bin/yproxy

yproxy --version
ci_log "yproxy installed"
