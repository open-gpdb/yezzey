#!/bin/bash
#
# Generate a throwaway OpenPGP keypair for Yezzey integration tests.
#
# The keys used to live in devops/config, but an Apache source release must not
# ship key material (https://www.apache.org/legal/release-policy), so CI
# generates a fresh keypair instead.
#
# Yezzey is vendored as a submodule by several repositories, so this script is
# the single reusable entry point for all of them: it only needs gpg and works
# from any current directory.
#
#   <path-to-yezzey>/devops/scripts/generate_test_gpg_keys.sh [key-dir]
#
# It writes an ASCII-armored keypair to <key-dir>/priv.gpg and
# <key-dir>/pub.gpg, imports it into the caller's keyring, marks it ultimately
# trusted and stores the fingerprint in <key-dir>/key_id (handy for the
# gpg_key_id/gpg_key_path pair in a yproxy config).  Re-running the script
# keeps an existing keypair and only repeats the import, so it is safe to call
# from several CI steps.

set -eo pipefail

KEY_DIR="${1:-${YEZZEY_TEST_KEY_DIR:-${HOME}/yezzey_test}}"
KEY_NAME="${YEZZEY_TEST_KEY_NAME:-Yezzey CI}"
KEY_EMAIL="${YEZZEY_TEST_KEY_EMAIL:-yezzey-ci@localhost}"

PRIV_KEY="${KEY_DIR}/priv.gpg"
PUB_KEY="${KEY_DIR}/pub.gpg"
KEY_ID_FILE="${KEY_DIR}/key_id"

mkdir -p "${KEY_DIR}"
chmod 700 "${KEY_DIR}"

if [ -s "${PRIV_KEY}" ] && [ -s "${PUB_KEY}" ]; then
  echo "generate_test_gpg_keys.sh: reusing keypair in ${KEY_DIR}"
else
  # gpg only generates a passphrase-less key unattended, and only from a batch
  # parameter file -- yproxy and the tests read the key non-interactively, so
  # %no-protection is what we want here.
  params="$(mktemp)"
  trap 'rm -f "${params}"' EXIT

  cat > "${params}" <<EOF
Key-Type: RSA
Key-Length: 3072
Subkey-Type: RSA
Subkey-Length: 3072
Name-Real: ${KEY_NAME}
Name-Email: ${KEY_EMAIL}
Expire-Date: 0
%no-protection
%commit
EOF

  # --status-fd gives us the fingerprint of the key we just created, which is
  # more reliable than looking the uid up in a keyring that may already hold
  # other test keys.
  fingerprint="$(gpg --batch --status-fd 1 --gen-key "${params}" |
    awk '/^\[GNUPG:\] KEY_CREATED/ { print $4; exit }')"

  if [ -z "${fingerprint}" ]; then
    echo "generate_test_gpg_keys.sh: gpg reported no generated key" >&2
    exit 1
  fi

  (
    umask 077
    gpg --batch --yes --armor --export "${fingerprint}" > "${PUB_KEY}"
    gpg --batch --yes --armor --export-secret-keys "${fingerprint}" > "${PRIV_KEY}"
  )

  echo "generate_test_gpg_keys.sh: generated ${KEY_NAME} <${KEY_EMAIL}> in ${KEY_DIR}"
fi

gpg --batch --import "${PUB_KEY}"
gpg --batch --import "${PRIV_KEY}"

fingerprint="$(gpg --batch --with-colons --show-keys "${PUB_KEY}" |
  awk -F: '/^fpr:/ { print $10; exit }')"

# An imported key is not trusted by default, which makes "gpg --encrypt" fail
# without a confirmation prompt.  These keys exist only for tests.
printf '%s:6:\n' "${fingerprint}" | gpg --batch --import-ownertrust

printf '%s\n' "${fingerprint}" > "${KEY_ID_FILE}"
echo "generate_test_gpg_keys.sh: key id ${fingerprint}"
