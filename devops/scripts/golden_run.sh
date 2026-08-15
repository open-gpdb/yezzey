#!/bin/bash
#
# golden_run.sh — runs the full yezzey regress test suite inside a
# Cloudberry / GPDB build container (the same way CI does) and writes the
# freshly generated ``results/*.out`` files to ``/work/results/`` so that
# the host Makefile can promote them into ``expected/``.
#
# Everything happens inside the container.  The yezzey source tree is
# COPYed into the image at build time (``/yezzey``).  The host only
# bind-mounts a scratch directory at ``/work`` for the results.
#
# Environment variables
# ---------------------
#   DB_FLAVOR       "cloudberry" (default) or "gpdb"
#   CBDB_REF        cloudberry git ref to checkout    (default: REL_2_STABLE)
#   GPDB_REF        gpdb git ref to checkout          (default: OPENGPDB_STABLE)
#   YPROXY_REF      yproxy git ref to checkout        (default: master)
#   GOLDEN_TESTS    space-separated list of test names to write; defaults to
#                   *all* files present in results/ after installcheck.
#   EXTRA_REGRESS   extra REGRESS overrides passed to ``make installcheck``
#                   (e.g. "REGRESS=simple_cbdb").
#

set -eo pipefail
set -x

DB_FLAVOR="${DB_FLAVOR:-cloudberry}"
CBDB_REF="${CBDB_REF:-REL_2_STABLE}"
GPDB_REF="${GPDB_REF:-OPENGPDB_STABLE}"
YPROXY_REF="${YPROXY_REF:-master}"

WORK=/work
YEZZEY_SRC=/yezzey
GOLDEN_RESULTS="$WORK/results"

case "$DB_FLAVOR" in
  cloudberry)
    DB_SRC="$WORK/cloudberry"
    DB_ENV="/usr/local/cloudberry-db/cloudberry-env.sh"
    ;;
  gpdb)
    DB_SRC="$WORK/gpdb"
    DB_ENV="/opt/greenplum-db-6/greenplum_path.sh"
    ;;
  *)
    echo "::error::Unknown DB_FLAVOR='$DB_FLAVOR' (expected cloudberry|gpdb)"
    exit 1
    ;;
esac

mkdir -p "$GOLDEN_RESULTS"

# ---------------------------------------------------------------------------
# 0. Container init (same as CI /tmp/init_system.sh does)
# ---------------------------------------------------------------------------
if ! su - gpadmin -c "/tmp/init_system.sh"; then
  echo "::error::Container initialization failed"
  exit 1
fi

# Mark /work as a safe git directory for both root and gpadmin — we run git
# as root (clone/checkout) and as gpadmin (yproxy Makefile) so both need it.
git config --global --add safe.directory '*'
su - gpadmin -c "git config --global --add safe.directory '*'"

chown -R gpadmin:gpadmin "$WORK"
chmod -R 755 "$WORK"

# ---------------------------------------------------------------------------
# 1. Checkout DB source
# ---------------------------------------------------------------------------
if [ ! -d "$DB_SRC/.git" ]; then
  case "$DB_FLAVOR" in
    cloudberry)
      git clone --depth 1 --branch "$CBDB_REF" \
        https://github.com/open-gpdb/cloudberry.git "$DB_SRC"
      cd "$DB_SRC"
      git submodule update --init --recursive || true
      ;;
    gpdb)
      git clone --depth 1 --branch "$GPDB_REF" \
        https://github.com/open-gpdb/gpdb.git "$DB_SRC"
      cd "$DB_SRC"
      git submodule update --init --recursive || true
      ;;
  esac
fi
chown -R gpadmin:gpadmin "$DB_SRC"

# ---------------------------------------------------------------------------
# 2. Checkout yproxy
# ---------------------------------------------------------------------------
YPROXY_SRC="$WORK/yproxy"
if [ ! -d "$YPROXY_SRC/.git" ]; then
  # Full clone (not --depth 1) so that `git describe --tags` works in the
  # yproxy Makefile ldflags.
  git clone https://github.com/open-gpdb/yproxy.git "$YPROXY_SRC"
else
  git -C "$YPROXY_SRC" fetch --tags origin
fi
chown -R gpadmin:gpadmin "$YPROXY_SRC"

# ---------------------------------------------------------------------------
# 3. Prepare filesystem storage for yproxy (no MinIO needed)
# ---------------------------------------------------------------------------
mkdir -p /tmp/yezzey-fs-storage /tmp/yezzey-fs-backup
chown -R gpadmin:gpadmin /tmp/yezzey-fs-storage /tmp/yezzey-fs-backup

# ---------------------------------------------------------------------------
# 4. Configure & build the DB
# ---------------------------------------------------------------------------
case "$DB_FLAVOR" in
  cloudberry)
    CONFIGURE_SCRIPT="$DB_SRC/devops/build/automation/cloudberry/scripts/configure-cloudberry.sh"
    BUILD_SCRIPT="$DB_SRC/devops/build/automation/cloudberry/scripts/build-cloudberry.sh"
    ;;
  gpdb)
    CONFIGURE_SCRIPT="$DB_SRC/../gpdb-devops/build_automation/gpdb/scripts/configure-gpdb.sh"
    BUILD_SCRIPT="$DB_SRC/../gpdb-devops/build_automation/gpdb/scripts/build-gpdb.sh"
    ;;
esac

if [ -x "$CONFIGURE_SCRIPT" ]; then
  chmod +x "$CONFIGURE_SCRIPT" "$BUILD_SCRIPT"
  # The configure/build scripts write logs to ${SRC_DIR}/build-logs; create
  # it ahead of time (CI does this in a separate step).
  mkdir -p "$DB_SRC/build-logs"
  chown -R gpadmin:gpadmin "$DB_SRC/build-logs"
  case "$DB_FLAVOR" in
    cloudberry)
      su - gpadmin -c "cd $DB_SRC && SRC_DIR=$DB_SRC $CONFIGURE_SCRIPT"
      su - gpadmin -c "cd $DB_SRC && SRC_DIR=$DB_SRC $BUILD_SCRIPT"
      ;;
    gpdb)
      rm -rf /opt/greenplum-db-6
      mkdir -p /opt/greenplum-db-6
      chown gpadmin:gpadmin /opt/greenplum-db-6
      su - gpadmin -c "cd $DB_SRC && SRC_DIR=$DB_SRC \
        BUILD_DESTINATION=/opt/greenplum-db-6 $CONFIGURE_SCRIPT"
      su - gpadmin -c "cd $DB_SRC && SRC_DIR=$DB_SRC \
        BUILD_DESTINATION=/opt/greenplum-db-6 $BUILD_SCRIPT"
      # copy shared libs (same as CI)
      cp /usr/local/lib/libsigar.so /opt/greenplum-db-6/lib 2>/dev/null || true
      cp /usr/local/lib/libxerces* /opt/greenplum-db-6/lib 2>/dev/null || true
      ;;
  esac
else
  # Fallback: plain in-tree configure+make
  case "$DB_FLAVOR" in
    gpdb)
      BUILD_PREFIX=/opt/greenplum-db-6
      rm -rf "$BUILD_PREFIX"
      mkdir -p "$BUILD_PREFIX"
      chown gpadmin:gpadmin "$BUILD_PREFIX"
      ;;
    *)
      BUILD_PREFIX="$DB_SRC/install"
      ;;
  esac
  su - gpadmin -c "cd $DB_SRC && ./configure --prefix=$BUILD_PREFIX \
    --enable-debug --disable-spinlocks --with-perl --with-python \
    --with-libxml --with-openssl --with-pgport=5432 --without-mdblocales" || true
  su - gpadmin -c "cd $DB_SRC && make -j\$(nproc) && make install"
fi

# ---------------------------------------------------------------------------
# 5. Build & install yezzey into the DB gpcontrib tree
# ---------------------------------------------------------------------------
YEZZEY_IN_DB="$DB_SRC/gpcontrib/yezzey"
rm -rf "$YEZZEY_IN_DB"
cp -a "$YEZZEY_SRC" "$YEZZEY_IN_DB"
chown -R gpadmin:gpadmin "$YEZZEY_IN_DB"

su - gpadmin -c "cd $YEZZEY_IN_DB && make && make install"

# ---------------------------------------------------------------------------
# 6. Deploy yezzey config (gpg keys + yproxy.conf)
# ---------------------------------------------------------------------------
chmod +x "$YEZZEY_IN_DB/devops/scripts/prepare_test_yezzey.sh"
# Use the filesystem storage config (no MinIO / S3 needed).
cp -f "$YEZZEY_IN_DB/devops/config/yproxy-fs.conf" /tmp/yproxy.yaml
# gpg keys still needed by yezzey
mkdir -p /home/gpadmin/yezzey_test
cp "$YEZZEY_IN_DB/devops/config/priv.gpg" /home/gpadmin/yezzey_test/priv.gpg
cp "$YEZZEY_IN_DB/devops/config/pub.gpg" /home/gpadmin/yezzey_test/pub.gpg
gpg --import /home/gpadmin/yezzey_test/pub.gpg
gpg --import /home/gpadmin/yezzey_test/priv.gpg

# ---------------------------------------------------------------------------
# 7. Build & install yproxy
# ---------------------------------------------------------------------------
apt-get update -y
apt-get install -y --no-install-recommends \
  software-properties-common libbrotli-dev liblzo2-dev libsodium-dev cmake
# yproxy needs Go >= 1.20 (uses -pgo flag); install from golang-backports PPA
add-apt-repository -y ppa:longsleep/golang-backports
apt-get update -y
apt-get install -y --no-install-recommends golang-go
# Build yproxy directly (without the Makefile's `git describe` ldflags,
# which fails when the current commit has no tag).
mkdir -p "$YPROXY_SRC/devbin"
su - gpadmin -c "cd $YPROXY_SRC && go build -o devbin/yproxy ./cmd/yproxy"
cp "$YPROXY_SRC/devbin/yproxy" /usr/bin/yproxy

# ---------------------------------------------------------------------------
# 8. Create demo cluster
# ---------------------------------------------------------------------------
case "$DB_FLAVOR" in
  cloudberry)
    su - gpadmin -c "cd $DB_SRC && $YEZZEY_IN_DB/devops/scripts/create_demo_yezzey_cloudberry.sh"
    ;;
  gpdb)
    su - gpadmin -c "cd $DB_SRC && BUILD_DESTINATION=/opt/greenplum-db-6 \
      $YEZZEY_IN_DB/devops/scripts/create_demo_yezzey_gpdb.sh"
    ;;
esac

# ---------------------------------------------------------------------------
# 9. Launch yproxy, then run installcheck
# ---------------------------------------------------------------------------
chmod +x "$YEZZEY_IN_DB/devops/scripts/launch_yproxy.sh"

case "$DB_FLAVOR" in
  cloudberry) IS_CBDB=true  ;;
  gpdb)       IS_CBDB=      ;;
esac

INSTALLCHECK_ENV=""
if [ -n "$EXTRA_REGRESS" ]; then
  INSTALLCHECK_ENV="$INSTALLCHECK_ENV $EXTRA_REGRESS"
fi
# If GOLDEN_TESTS is set, restrict which tests pg_regress runs by
# overriding REGRESS on the make command line.
if [ -n "$GOLDEN_TESTS" ]; then
  INSTALLCHECK_ENV="$INSTALLCHECK_ENV REGRESS=$GOLDEN_TESTS"
fi

# Run installcheck. We intentionally do NOT fail on regress diffs here —
# we *want* the fresh results/ even if they differ from the old expected/.
set +e
su - gpadmin -c "cd $DB_SRC && \
  $YEZZEY_IN_DB/devops/scripts/launch_yproxy.sh && \
  cd $YEZZEY_IN_DB && \
  source $DB_ENV && \
  source ../../gpAux/gpdemo/gpdemo-env.sh && \
  ${IS_CBDB:+IS_CLOUDBERRY=true} $INSTALLCHECK_ENV make installcheck"
RC=$?
set -e

if [ $RC -ne 0 ]; then
  echo "::warning::installcheck returned non-zero (rc=$RC) — this is expected when golden output differs from old expected."
  if [ -f "$YEZZEY_IN_DB/regression.diffs" ]; then
    echo "::warning::regression.diffs follows:"
    cat "$YEZZEY_IN_DB/regression.diffs" || true
  fi
fi

# ---------------------------------------------------------------------------
# 10. Copy results/*.out to the scratch dir for the host to pick up.
# ---------------------------------------------------------------------------
RESULTS_DIR="$YEZZEY_IN_DB/results"
if [ ! -d "$RESULTS_DIR" ]; then
  echo "::error::No results/ directory found at $RESULTS_DIR — installcheck did not produce output"
  exit 1
fi

# Build the list of tests that actually produced a result file.
RAN_TESTS=""
for f in "$RESULTS_DIR"/*.out; do
  [ -e "$f" ] || continue
  RAN_TESTS="$RAN_TESTS $(basename "$f" .out)"
done

# Filter by GOLDEN_TESTS if the user provided an explicit allow-list.
if [ -n "$GOLDEN_TESTS" ]; then
  FILTERED=""
  for t in $RAN_TESTS; do
    case " $GOLDEN_TESTS " in
      *" $t "*) FILTERED="$FILTERED $t" ;;
    esac
  done
  RAN_TESTS="$FILTERED"
fi

if [ -z "$RAN_TESTS" ]; then
  echo "::error::No matching result files to promote"
  exit 1
fi

# Clear previous results so stale files don't linger.
rm -f "$GOLDEN_RESULTS/"*.out

for t in $RAN_TESTS; do
  cp "$RESULTS_DIR/$t.out" "$GOLDEN_RESULTS/$t.out"
  echo "::wrote::$t.out"
done

echo "::done::Wrote $(echo $RAN_TESTS | wc -w) result file(s) to $GOLDEN_RESULTS/"
