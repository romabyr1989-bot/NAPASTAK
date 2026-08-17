# NAPASTAK — TLS/HTTPS Setup

## Overview

NAPASTAK now supports TLS/HTTPS encryption for secure client-server communication. TLS is implemented as a transparent wrapper over the existing TCP layer in `lib/net/http.c`, without modifying the core HTTP logic.

## Quick Start

### 1. Generate Self-Signed Certificate

```bash
bash scripts/gen_cert.sh
```

This creates:
- `certs/cert.pem` — Self-signed certificate
- `certs/key.pem` — Private key

Valid for 365 days with SANs: `localhost`, `127.0.0.1`

### 2. Enable TLS in config.json

```json
{
  "port": 8080,
  "tls_cert": "./certs/cert.pem",
  "tls_key": "./certs/key.pem",
  "auth_enabled": true,
  "jwt_secret": "...",
  "admin_password": "..."
}
```

### 3. Start Server

```bash
./build/debug/bin/napastak_gateway -c config.json
```

Expected output:
```
HTTPS server listening on :8080 (with TLS)
```

## Testing

### Get JWT Token

```bash
TOKEN=$(curl -s -k https://localhost:8080/api/auth/token \
  -H "Content-Type: application/json" \
  -d '{"username":"admin","password":"admin"}' \
  | python3 -c "import sys, json; print(json.load(sys.stdin)['token'])" | tr -d '\n')
```

### Access API with Token

```bash
curl -k https://localhost:8080/api/tables \
  -H "Authorization: Bearer $TOKEN"
```

Response: `200 OK` with table list

### Verify Without Token

```bash
curl -k https://localhost:8080/api/tables
```

Response: `401 Unauthorized`

### Inspect Certificate

```bash
openssl s_client -connect localhost:8080
```

Should show:
- Protocol: `TLSv1.3` or higher
- Cipher: `TLS_AES_256_GCM_SHA384` (or similar strong cipher)
- Subject: `CN=localhost, O=NAPASTAK, C=RU`

## Architecture

### New Module: `lib/net/tls.h` / `lib/net/tls.c`

**Public API:**
- `TlsCtx *tls_server_ctx_create(cert_pem, key_pem)` — Create server context
- `TlsConn *tls_conn_accept(ctx, fd)` — TLS handshake on accepted fd
- `ssize_t tls_read(conn, buf, n)` — Read encrypted data
- `ssize_t tls_write(conn, buf, n)` — Write encrypted data

**Implementation:**
- Uses OpenSSL: `SSL_CTX`, `SSL`
- Minimum TLS 1.2 enforced
- Cipher suite: `HIGH:!aNULL:!eNULL:!EXPORT:!RC4`
- Self-signed certificate support (dev/staging only)

### Modified: `lib/net/http.c`

**Key Changes:**
1. `struct HttpServer` now has `TlsCtx *tls_ctx` field
2. `http_server_create()` accepts `tls_ctx` parameter
3. `handle_conn()` performs TLS handshake if `tls_ctx` is set
4. Socket read/write replaced with `tls_read()` / `tls_write()` when TLS enabled
5. Response building identical for both HTTP and HTTPS

### Modified: `src/gateway/app.h` / `main.c`

**New App Fields:**
```c
char tls_cert_path[512];
char tls_key_path[512];
bool tls_enabled;
```

**Config Parsing:**
- Reads `"tls_cert"` and `"tls_key"` from config.json
- Automatically enables TLS if both paths are non-empty

**TLS Context Creation:**
```c
TlsCtx *tls_ctx = NULL;
if (app->tls_enabled) {
    tls_ctx = tls_server_ctx_create(app->tls_cert_path, app->tls_key_path);
}
app->server = http_server_create(&app->router, app->port, 128, tls_ctx);
```

## Makefile Updates

```makefile
LDFLAGS += -lssl -lcrypto
NET_SRCS = lib/net/http.c lib/net/tls.c
```

## Production Considerations

1. **Certificate Management:** 
   - Use proper CA-signed certificates in production
   - Implement certificate rotation
   - Securely store private keys

2. **TLS Version:** 
   - Minimum TLS 1.2 enforced
   - TLS 1.3 recommended

3. **Cipher Suite:** 
   - Strong ciphers only (no RC4, aNULL, eNULL, EXPORT)
   - Consider PFS (Perfect Forward Secrecy) ciphers

4. **WebSocket over TLS:** 
   - Client must use `wss://` protocol
   - TLS handshake happens before WebSocket upgrade
   - Automatic upgrade for secure connections

## Known Limitations

- No SNI (Server Name Indication) support yet
- No OCSP stapling
- No certificate pinning
- Self-signed certificates require `-k` flag in curl (unsafe in production)

## Future Enhancements

- HTTP → HTTPS redirect on separate port
- Dynamic certificate reloading
- Multiple domain/certificate support
- HSTS header support
- WebSocket secure (WSS) with proper frame handling
