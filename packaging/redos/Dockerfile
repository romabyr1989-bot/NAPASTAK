# Воспроизводимая сборка DataFlow OS под RedOS 8 / RHEL 9-совместимые.
#
# По умолчанию rockylinux:9 — тот же класс, что RedOS 8.02 (glibc 2.34, OpenSSL 3,
# SONAME libssl.so.3 / libcrypto.so.3 / libsqlite3.so.0 / libcurl.so.4).
# Для гарантированного ABI под вашу RedOS подставьте её образ:
#   docker build --build-arg BASE_IMAGE=<redos-image> -f packaging/redos/Dockerfile ...
ARG BASE_IMAGE=rockylinux:9
FROM ${BASE_IMAGE} AS build
ARG VERSION=redos
# ODPI-C — open-source (UPL/Apache) C-обёртка Oracle. Свободно распространяется;
# проприетарный Oracle Instant Client она dlopen()-ит только в момент подключения.
ARG ODPI_VERSION=4.6.1

# Тулчейн + dev-библиотеки для ВСЕХ 10 коннекторов:
#   base (gcc/make/sqlite/curl/openssl/zlib) → csv, parquet, json_http, siebel, xml, soap
#   libpq-devel                              → pg, greenplum
#   librdkafka-devel (EPEL + CRB)            → kafka
#   ODPI-C (сборка из исходников ниже)       → oracle (в рантайме нужен Instant Client)
RUN dnf -y install epel-release \
 && ( dnf config-manager --set-enabled crb 2>/dev/null \
      || dnf config-manager --set-enabled powertools 2>/dev/null || true ) \
 && dnf -y install gcc make git tar gzip \
      sqlite-devel libcurl-devel openssl-devel zlib-devel libpq-devel librdkafka-devel \
 && dnf clean all

# ODPI-C: собрать libodpic.so и положить в /usr/local (Makefile ищет dpi.h там).
# oracle_connector.so слинкуется с libodpic и будет ЗАГРУЖАТЬСЯ; реальное
# подключение к Oracle требует Instant Client в рантайме (см. README → Oracle).
RUN curl -fsSL "https://github.com/oracle/odpi/archive/refs/tags/v${ODPI_VERSION}.tar.gz" | tar xz \
 && make -C "odpi-${ODPI_VERSION}" \
 && install -m 0755 odpi-${ODPI_VERSION}/lib/libodpic.so* /usr/local/lib/ \
 && ( cd /usr/local/lib && for f in libodpic.so.*; do ln -sf "$f" libodpic.so; done ) \
 && install -m 0644 odpi-${ODPI_VERSION}/include/dpi.h /usr/local/include/ \
 && echo /usr/local/lib > /etc/ld.so.conf.d/odpi.conf && ldconfig \
 && rm -rf "odpi-${ODPI_VERSION}"

WORKDIR /src
COPY . .
RUN make clean >/dev/null 2>&1 || true
RUN make BUILD=release dist VERSION="${VERSION}"

# Гарантия «из коробки»: падаем, если хоть один из 10 коннекторов не попал в бандл,
# и печатаем вложённые рантайм-.so (lib/deps) для наглядности.
RUN set -e; cd /src/build/release/dist/*/lib; \
    for c in csv pg gp parquet json_http siebel xml soap kafka oracle; do \
      test -f "${c}_connector.so" || { echo "!! MISSING ${c}_connector.so"; exit 1; }; \
    done; \
    echo "OK: собраны все 10 коннекторов:"; ls -1 *_connector.so | sed 's/^/  /'; \
    echo "вложенные рантайм-.so (lib/deps):"; ls -1 deps/ 2>/dev/null | sed 's/^/  /'

# Готовый дистрибутив: /src/build/release/dist/dataflow-os-*-linux-*.tar.gz
