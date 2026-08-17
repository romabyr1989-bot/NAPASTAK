# ── Builder ──────────────────────────────────────────────
FROM ubuntu:24.04 AS builder

RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc make \
    libsqlite3-dev \
    libssl-dev \
    libcurl4-openssl-dev \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /src
COPY . .

RUN make BUILD=release 2>&1 | tail -30

# ── Runtime ──────────────────────────────────────────────
FROM debian:12-slim

RUN apt-get update && apt-get install -y --no-install-recommends \
    libsqlite3-0 \
    libssl3 \
    ca-certificates \
    curl \
    gettext-base \
    && rm -rf /var/lib/apt/lists/*

RUN useradd -r -s /bin/false -u 1001 napastak

WORKDIR /app

# Binary + connector plugins
COPY --from=builder /src/build/release/bin/napastak_gateway ./napastak_gateway
COPY --from=builder /src/build/release/lib/*.so        ./lib/

# UI static files
COPY ui/ ./ui/

# Default self-signed cert (override via volume mount)
COPY certs/ ./certs/

# Config template — env vars substituted at startup
COPY config.template.json ./config.template.json
COPY scripts/entrypoint.sh ./entrypoint.sh
RUN chmod +x /app/entrypoint.sh

RUN mkdir -p /data && chown -R napastak:napastak /data /app

USER napastak

# /data is persisted via named volume; /app is read-only after chown
VOLUME ["/data"]

EXPOSE 8080 8443

HEALTHCHECK --interval=15s --timeout=5s --start-period=20s --retries=3 \
    CMD curl -fsk https://localhost:8443/ || exit 1

ENTRYPOINT ["/app/entrypoint.sh"]
