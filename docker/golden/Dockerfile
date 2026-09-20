# Dockerfile.golden — builds a golden-run image on top of the Cloudberry /
# GPDB build image.  The current yezzey source tree is COPYed into the image
# so there is no need to git-clone it inside the container.
#
# Usage (from the repo root):
#   docker build -f docker/golden/Dockerfile --build-arg BASE_IMAGE=<image> -t yezzey/golden .
#

ARG BASE_IMAGE=apache/incubator-cloudberry:cbdb-build-ubuntu22.04-latest
FROM ${BASE_IMAGE}

# Copy the current yezzey source tree (as-is from the host working directory).
COPY . /yezzey

# The golden_run.sh script is the entrypoint.
RUN chmod +x /yezzey/devops/scripts/golden_run.sh

ENTRYPOINT ["bash", "/yezzey/devops/scripts/golden_run.sh"]
