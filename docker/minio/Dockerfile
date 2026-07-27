FROM golang:1.24.9-bookworm@sha256:737b40b61ce956d738bed59f18ba854d8d67e7a4c4fa63f64437f4c70247ac5b AS builder

ARG MINIO_RELEASE=RELEASE.2025-10-15T17-29-55Z
ARG MINIO_COMMIT=9e49d5e7a648f00e26f2246f4dc28e6b07f8c84a
WORKDIR /src
RUN git clone --filter=blob:none --no-checkout https://github.com/minio/minio.git . \
    && git fetch --depth 1 origin "refs/tags/${MINIO_RELEASE}:refs/tags/${MINIO_RELEASE}" \
    && test "$(git rev-list -n 1 "${MINIO_RELEASE}")" = "${MINIO_COMMIT}" \
    && git checkout --detach "${MINIO_COMMIT}" \
    && make build \
    && ./minio --version

FROM python:3.11.9-slim-bookworm@sha256:8fb099199b9f2d70342674bd9dbccd3ed03a258f26bbd1d556822c6dfc60c317

ARG MINIO_RELEASE=RELEASE.2025-10-15T17-29-55Z
ARG MINIO_COMMIT=9e49d5e7a648f00e26f2246f4dc28e6b07f8c84a
LABEL org.opencontainers.image.title="MinIO source build for tiktok_SE_intern" \
      org.opencontainers.image.version="${MINIO_RELEASE}" \
      org.opencontainers.image.revision="${MINIO_COMMIT}" \
      org.opencontainers.image.source="https://github.com/minio/minio"

COPY --from=builder /src/minio /usr/local/bin/minio
RUN mkdir -p /data \
    && chown 1000:1000 /data

ENV HOME=/tmp
USER 1000:1000
EXPOSE 9000 9001
ENTRYPOINT ["/usr/local/bin/minio"]
CMD ["server", "/data", "--address", ":9000", "--console-address", ":9001"]

