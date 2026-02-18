#!/bin/bash
set -euo pipefail

# Cloudberry DEB Package Build Script for Ubuntu 22.04
CLOUDBERRY_VERSION="${CLOUDBERRY_VERSION:-99.0.0}"
CLOUDBERRY_BUILD="${CLOUDBERRY_BUILD:-1}"
INSTALL_PREFIX="${INSTALL_PREFIX:-/usr/local/cloudberry-db}"
WORKSPACE="${WORKSPACE:-$HOME/workspace}"
CLOUDBERRY_SRC="${WORKSPACE}/cloudberry"

echo "=== Cloudberry DEB Package Build ==="
echo "Version: ${CLOUDBERRY_VERSION}"
echo "Build: ${CLOUDBERRY_BUILD}"
echo "Install Prefix: ${INSTALL_PREFIX}"
echo "Source: ${CLOUDBERRY_SRC}"

# Clean previous installation
rm -rf "${INSTALL_PREFIX}"
mkdir -p "${INSTALL_PREFIX}"

# Configure Cloudberry
cd "${CLOUDBERRY_SRC}"
./configure --prefix="${INSTALL_PREFIX}" \
            --disable-external-fts \
            --enable-gpcloud \
            --enable-ic-proxy \
            --enable-mapreduce \
            --enable-orafce \
            --enable-orca \
            --disable-pax \
            --disable-pxf \
            --enable-tap-tests \
            --with-gssapi \
            --with-ldap \
            --with-libxml \
            --with-lz4 \
            --with-pam \
            --with-perl \
            --with-pgport=5432 \
            --with-python \
            --with-pythonsrc-ext \
            --with-ssl=openssl \
            --with-uuid=e2fs \
            --with-includes=/usr/include/xercesc

# Build and install
make -j$(nproc)
make -j$(nproc) -C contrib
make install
make install -C contrib

# Copy LICENSE
cp LICENSE "${INSTALL_PREFIX}/"

# Create deb package structure
DEB_BUILD_DIR="${WORKSPACE}/cloudberry-deb"
DEB_PKG_DIR="${DEB_BUILD_DIR}/apache-cloudberry-db_${CLOUDBERRY_VERSION}-${CLOUDBERRY_BUILD}_amd64"
mkdir -p "${DEB_PKG_DIR}/DEBIAN"
mkdir -p "${DEB_PKG_DIR}${INSTALL_PREFIX}"

# Copy installed files
cp -a "${INSTALL_PREFIX}"/* "${DEB_PKG_DIR}${INSTALL_PREFIX}/"

# Create control file
cat > "${DEB_PKG_DIR}/DEBIAN/control" << EOF
Package: apache-cloudberry-db
Version: ${CLOUDBERRY_VERSION}-${CLOUDBERRY_BUILD}
Section: database
Priority: optional
Architecture: amd64
Maintainer: Apache Cloudberry <dev@cloudberry.apache.org>
Description: Apache Cloudberry Database
 Apache Cloudberry is a massively parallel processing (MPP) database
 built on PostgreSQL for analytics and data warehousing.
Depends: libc6, libssl3, libreadline8, libxml2, libxerces-c3.2, liblz4-1, libzstd1, libapr1, libcurl4, libevent-2.1-7, libkrb5-3, libldap-2.5-0, libpam0g, libuv1, libyaml-0-2
EOF

# Create postinst script
cat > "${DEB_PKG_DIR}/DEBIAN/postinst" << 'EOF'
#!/bin/bash
set -e
if ! id -u gpadmin >/dev/null 2>&1; then
    useradd -m -s /bin/bash gpadmin
fi
chown -R gpadmin:gpadmin /usr/local/cloudberry-db
echo "Apache Cloudberry Database installed successfully"
EOF

chmod 755 "${DEB_PKG_DIR}/DEBIAN/postinst"

# Build deb package
cd "${DEB_BUILD_DIR}"
dpkg-deb --build "$(basename ${DEB_PKG_DIR})"

DEB_FILE="${DEB_BUILD_DIR}/apache-cloudberry-db_${CLOUDBERRY_VERSION}-${CLOUDBERRY_BUILD}_amd64.deb"
echo "=== DEB Package Created ==="
ls -lh "${DEB_FILE}"
dpkg-deb -I "${DEB_FILE}"
echo "=== Build Complete ==="
