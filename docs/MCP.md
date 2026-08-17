# NAPASTAK — MCP Server

NAPASTAK ships with `napastak_mcp_server`, a [Model Context Protocol](https://modelcontextprotocol.io)
server that exposes the platform's tables, queries, and pipelines to AI agents
(Claude Desktop, Cursor, Continue, Cline, LangChain, …).

## What it does

The MCP server is a small stdio binary that translates JSON-RPC tool calls
into HTTP requests against a running `napastak_gateway`. The agent gets ten
high-level tools instead of raw HTTP endpoints, all rate-limited by the
gateway's RBAC.

## Quick start — Claude Desktop

1. Build the binaries:

   ```sh
   make BUILD=release all
   ```

2. Start the gateway with a fixed JWT secret (so tokens survive restarts):

   ```sh
   ./build/release/bin/napastak_gateway -c config.local.json
   ```

3. Get an API token:

   ```sh
   curl -s -X POST http://localhost:8080/api/auth/token \
     -H 'Content-Type: application/json' \
     -d '{"username":"admin","password":"admin"}' | jq -r .token
   ```

4. Add to `~/Library/Application Support/Claude/claude_desktop_config.json`
   (macOS) or `%APPDATA%\Claude\claude_desktop_config.json` (Windows):

   ```json
   {
     "mcpServers": {
       "napastak": {
         "command": "/absolute/path/to/build/release/bin/napastak_mcp_server",
         "args": [
           "--gateway", "http://localhost:8080",
           "--api-key", "PASTE_JWT_HERE"
         ]
       }
     }
   }
   ```

5. Restart Claude Desktop. Ask: *"What tables do I have in NAPASTAK?"* —
   Claude should call `list_tables` and render a markdown table.

## Tools

| Tool              | Purpose                                                         |
|-------------------|-----------------------------------------------------------------|
| `query`           | Execute SQL (SELECT/DML/transactions, JOINs, aggregates)        |
| `list_tables`     | Enumerate tables with row counts and source                     |
| `describe_table`  | Schema + sample rows for one table                              |
| `ingest_csv`      | Upload CSV text into a table                                    |
| `ingest_url`      | Fetch a JSON/CSV URL via the pipeline runner and ingest         |
| `list_pipelines`  | List configured pipelines                                       |
| `run_pipeline`    | Trigger a pipeline immediately by id                            |
| `create_pipeline` | Create a new pipeline definition                                |
| `get_metrics`     | Process metrics (uptime, query/pipeline latencies, totals)      |
| `analyze`         | Quick column profile: distinct, min, max                        |

All responses are returned as markdown text inside the MCP `content` field —
agents handle that more reliably than raw JSON.

## Security — read this before granting access

The MCP server inherits all permissions from its API token. **Do not reuse
the admin token.** Mint a dedicated key:

```sh
curl -X POST http://localhost:8080/api/auth/apikeys \
  -H 'Authorization: Bearer ADMIN_TOKEN' \
  -H 'Content-Type: application/json' \
  -d '{"user_id":"claude-desktop","role":"analyst"}'
```

Then constrain it with RBAC policies (with `rbac_enabled: true` in the
gateway config):

```sh
curl -X POST http://localhost:8080/api/rbac/policies \
  -H 'Authorization: Bearer ADMIN_TOKEN' \
  -H 'Content-Type: application/json' \
  -d '{"role":1,"table_pattern":"public_*","allowed_actions":1,"row_filter":""}'
```

`role:1` is `analyst`; `allowed_actions:1` is read-only. Patterns are
glob-matched (`public_*`, `*`, exact name). Never grant `role:0` (admin) to
an agent — it can drop tables.

## Modes

`--gateway URL --api-key TOKEN` is the only mode currently shipped. The
binary accepts `--data-dir DIR` for a future embedded mode that links the
storage layer directly; that mode is not yet wired and exits with an error
today.

## Troubleshooting

- **`Method not found` on every call** — the agent is calling a tool name
  that does not match. `tools/list` is the source of truth.
- **`HTTP 401` from every call** — the gateway's JWT secret rotated. Mint a
  new token and update `claude_desktop_config.json`. Use a fixed
  `jwt_secret` in your gateway config to avoid this.
- **Logs missing** — the binary writes logs to **stderr**. The MCP client
  captures stdout only; check the client's debug logs for stderr capture.
- **Hung after `initialize`** — the agent expects `notifications/initialized`
  to be delivered next. The server treats that as a notification and does
  not respond, which is correct per spec.

## Implementing with `mcptools` for ad-hoc testing

Install [`mcptools`](https://github.com/f/mcptools) and run:

```sh
mcp tools "stdio:./build/release/bin/napastak_mcp_server --gateway http://localhost:8080 --api-key $TOKEN"
mcp call query --params '{"sql":"SELECT count(*) FROM my_table"}' \
  "stdio:./build/release/bin/napastak_mcp_server --gateway http://localhost:8080 --api-key $TOKEN"
```

This is also what the smoke test script does without external tooling — see
`tests/integration/test_mcp.sh`.
