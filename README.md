# NAPASTAK

A lightweight data platform for secure local deployment and containerized development.

## Docker Quickstart

1. Generate self-signed certificates:

   ```bash
   mkdir -p certs
   ./scripts/gen_cert.sh certs/server.crt certs/server.key
   ```

2. Build and start the service:

   ```bash
   ./scripts/quickstart.sh
   ```

3. Open the UI in your browser:

   ```text
   https://localhost:8443
   ```

## Docker Support

- `Dockerfile` builds the release binary and runs it in a minimal Ubuntu runtime.
- `docker-compose.yml` exposes HTTP on `8080` and HTTPS on `8443`.
- `config.docker.json` supports environment-variable expansion for runtime settings.
- HTTP requests to port `8080` are redirected to HTTPS on port `8443` when TLS is enabled.

## Runtime configuration

The container configuration reads values from `config.json`. The Docker runtime supports environment expansion using `${VAR}` syntax for:

- `DATA_DIR`
- `JWT_SECRET`
- `ADMIN_PASSWORD`
- `TLS_CERT`
- `TLS_KEY`

Place TLS certificates into `./certs` and mount them into `/app/certs` in the container.

## Files added

- `Dockerfile`
- `docker-compose.yml`
- `.dockerignore`
- `config.docker.json`
- `scripts/quickstart.sh`
