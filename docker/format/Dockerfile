# Run clang-format against the sources in a pinned environment.

FROM ubuntu:noble

RUN apt-get update && apt-get install -y --no-install-recommends \
        ca-certificates \
        clang-format-18 \
        git \
        python3 \
        python-is-python3

RUN git clone --depth 1 https://github.com/Sarcasm/run-clang-format.git /run-clang-format

COPY docker/format/run-format.sh /run.sh
RUN chmod +x /run.sh

WORKDIR /yezzey

ENTRYPOINT [ "/run.sh" ]
