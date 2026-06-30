# Воспроизводимая сборка DataFlow OS под RedOS 8 / RHEL 9-совместимые.
#
# По умолчанию rockylinux:9 — тот же класс, что RedOS 8.02 (glibc 2.34, OpenSSL 3,
# SONAME libssl.so.3 / libcrypto.so.3 / libsqlite3.so.0 / libcurl.so.4).
# Для гарантированного ABI под вашу RedOS подставьте её образ:
#   docker build --build-arg BASE_IMAGE=<redos-image> -f packaging/redos/Dockerfile ...
ARG BASE_IMAGE=rockylinux:9
FROM ${BASE_IMAGE} AS build
ARG VERSION=redos

# gateway + csv/parquet/json_http/siebel/xml/soap + pg/gp.
# Kafka → добавьте librdkafka-devel (EPEL); Oracle → ODPI-C (вручную).
RUN dnf -y install gcc make sqlite-devel libcurl-devel openssl-devel zlib-devel libpq-devel \
 && dnf clean all

WORKDIR /src
COPY . .
RUN make clean >/dev/null 2>&1 || true
RUN make BUILD=release dist VERSION="${VERSION}"

# Готовый дистрибутив: /src/build/release/dist/dataflow-os-*-linux-*.tar.gz
