#!/bin/sh
set -e

: "${DATA_DIR:=/data}"
: "${PLUGINS_DIR:=/app/lib}"
: "${PORT:=8080}"
: "${JWT_SECRET:=insecure-default-please-set-in-production}"
: "${ADMIN_PASSWORD:=admin}"
: "${TLS_CERT:=/app/certs/cert.pem}"
: "${TLS_KEY:=/app/certs/key.pem}"

export DATA_DIR PLUGINS_DIR PORT JWT_SECRET ADMIN_PASSWORD TLS_CERT TLS_KEY

envsubst < /app/config.template.json > /tmp/config.json

exec /app/napastak_gateway -c /tmp/config.json
