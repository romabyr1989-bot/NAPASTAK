#!/bin/bash
# Генерирует самоподписанный сертификат для dev/staging
mkdir -p certs
openssl req -x509 -newkey rsa:4096 -keyout certs/key.pem \
  -out certs/cert.pem -days 365 -nodes \
  -subj "/CN=localhost/O=NAPASTAK/C=RU" \
  -addext "subjectAltName=DNS:localhost,IP:127.0.0.1"
echo "Certificates generated in ./certs/"
echo "Add to config.json: \"tls_cert\": \"./certs/cert.pem\", \"tls_key\": \"./certs/key.pem\""
