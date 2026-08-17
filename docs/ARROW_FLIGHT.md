# Apache Arrow Flight bridge

`napastak_flight_server` is a small C++ binary that exposes NAPASTAK over the
[Apache Arrow Flight](https://arrow.apache.org/docs/format/Flight.html)
gRPC protocol. Arrow Flight clients (PyArrow, pandas, polars, Spark,
DuckDB, BigQuery, Snowflake, R Arrow) can then read NAPASTAK tables
**without JSON serialization overhead** — the bridge converts query
results to Arrow record batches and streams them as native Arrow IPC.

The Flight server is a separate process from the gateway; it talks to the
gateway over plain HTTP and inherits its RBAC.

```
┌───────────────┐  Flight (gRPC)   ┌─────────────────────┐  HTTP   ┌──────────────┐
│ pyarrow /     │ ───────────────▶ │  napastak_flight_server  │ ──────▶ │ napastak_gateway  │
│ pandas / dbt  │ ◀─── Arrow IPC ── │  (port 8815)        │ ◀────── │ (port 8080)  │
└───────────────┘                  └─────────────────────┘          └──────────────┘
```

Heavy C++ deps (Arrow C++, gRPC, nlohmann_json) are isolated to
`src/flight_server/` and built **separately** — the rest of the project
remains pure C with no new toolchain requirements.

## Install dependencies

| Platform | Command |
|----------|---------|
| macOS 14+ (Apple Silicon)   | `brew install apache-arrow grpc nlohmann-json` |
| Debian / Ubuntu             | `apt install libarrow-flight-dev libgrpc++-dev nlohmann-json3-dev libcurl4-openssl-dev cmake` |
| Fedora                      | `dnf install arrow-flight-devel grpc-devel json-devel libcurl-devel cmake` |
| Nix                         | `nix-shell -p arrow-cpp grpc nlohmann_json` |
| Conda (any platform)        | `conda install -c conda-forge libarrow-all libprotobuf grpc-cpp nlohmann_json` |

Disk footprint after Homebrew install is ~600 MB (Arrow C++ alone is
~250 MB). If you can't install Arrow C++, fall back to the existing
JSON `/api/tables/query` endpoint — it's slower but identical in
functionality.

### Platform note: macOS 13 (Ventura) on Intel

Homebrew dropped macOS 13/Intel from its [Tier 1 support list](
https://docs.brew.sh/Support-Tiers) in late 2024. `brew install
apache-arrow` on that combination tries to compile from source and
typically fails partway through aws-sdk-cpp / arrow build (multiple
hours and several gigabytes of disk). We verified this in our own dev
environment.

If you're on macOS 13 Intel:
- Easiest path: install via **conda-forge** (pre-built bottles,
  ~2 minutes): `conda install -c conda-forge libarrow-all grpc-cpp nlohmann_json`,
  then point CMake at the conda prefix:
  `cmake -DCMAKE_PREFIX_PATH=$(conda info --base)/envs/<env>`.
- Alternative: build inside a Linux container (Debian / Ubuntu image),
  bind-mount the source, run `make flight` there.
- Or upgrade to macOS 14+ on Apple Silicon — Homebrew bottles work
  out of the box.

## Build & run

```sh
make BUILD=release           # base project (no Arrow needed)
make flight                  # builds src/flight_server via cmake
DYLD_LIBRARY_PATH=$HOME/miniconda3/lib ./build/release/bin/napastak_flight_server \
    --gateway http://localhost:8080 \
    --api-key "$JWT_TOKEN" \
    --port 8815
```

`make flight` auto-detects `~/miniconda3` or `~/anaconda3` and threads
its prefix into cmake. Override with `make flight FLIGHT_PREFIX=/opt/X`
for a custom install location, or unset it on Linux/Apple-Silicon if
the system `pkg-config arrow` already finds your packages. If Arrow /
gRPC / nlohmann_json aren't found, the target stops with a clear
error message.

Arrow ≥ 24.x needs C++20 (`std::log2p1`); Apple Clang 14 (Xcode 14)
doesn't ship that yet, so the Makefile transparently uses
`~/miniconda3/bin/clang++` (clang 19) when the conda prefix is active.

**Runtime:** the binary needs `libarrow*.dylib` and friends on
`DYLD_LIBRARY_PATH` (or rpath), which is why the example shows the
`DYLD_LIBRARY_PATH=` prefix. The Makefile prints a HINT line after
build with the right value.

Get the JWT token like any NAPASTAK client:

```sh
TOKEN=$(curl -s -X POST http://localhost:8080/api/auth/token \
  -H 'Content-Type: application/json' \
  -d '{"username":"admin","password":"admin"}' | jq -r .token)
```

## Client usage

### Python (PyArrow + pandas)

```python
import pyarrow.flight as flight

client = flight.FlightClient("grpc://localhost:8815")

# List available tables (each is a Flight)
for fi in client.list_flights():
    print(fi.descriptor.path, fi.schema)

# Pull a SQL result as an Arrow Table → pandas DataFrame
ticket = flight.Ticket(b"sql:SELECT * FROM users WHERE active = true")
df = client.do_get(ticket).read_pandas()
print(df.head())

# Or by table name (full scan)
df = client.do_get(flight.Ticket(b"table:users")).read_pandas()
```

### Python (Polars)

```python
import polars as pl, pyarrow.flight as flight

reader = flight.FlightClient("grpc://localhost:8815") \
              .do_get(flight.Ticket(b"sql:SELECT * FROM events LIMIT 1000"))
df = pl.from_arrow(reader.read_all())
```

### dbt-postgres-style configuration with adbc

```python
from adbc_driver_flightsql.dbapi import connect
conn = connect(uri="grpc://localhost:8815")
cursor = conn.cursor()
cursor.execute("SELECT count(*) FROM events")
print(cursor.fetchone())
```

## Ticket format

NAPASTAK Flight accepts these `Ticket` payload formats:

| Ticket bytes              | Meaning                              |
|---------------------------|--------------------------------------|
| `sql:SELECT … FROM …`     | execute arbitrary SQL                |
| `table:my_table`          | shortcut for `SELECT * FROM my_table`|
| anything else             | treated as raw SQL                   |

Use `client.get_flight_info(FlightDescriptor.for_path([table_name]))` or
`for_command(b"sql:SELECT …")` to discover schema before calling `do_get`.

## What's implemented

| Method        | Status           | Notes                                                     |
|---------------|------------------|-----------------------------------------------------------|
| `ListFlights` | ✓                | each table → one `FlightInfo`; schema discovered via `LIMIT 0` |
| `GetFlightInfo` | ✓              | accepts `PATH`-typed and `CMD`-typed (`sql:…`) descriptors |
| `DoGet`       | ✓                | builds the full result in memory, streams batches            |
| `DoPut`       | ✗ (returns NotImplemented) | use `POST /api/ingest/csv` on the gateway as a workaround |
| `DoExchange`  | ✗                | not implemented                                              |
| `DoAction`    | ✗                | not implemented                                              |

## Type mapping

JSON → Arrow type inference is done from the first non-null value in
each column (since `/api/tables/query` doesn't yet expose a `types` field
in its response):

| First-row JSON | Arrow type   |
|----------------|--------------|
| `true` / `false` | `boolean`  |
| integer        | `int64`      |
| float          | `float64`    |
| string / object / array / null-only column | `utf8` |

Columns that are entirely null default to `utf8`. **All-numeric columns
that happen to start with a null** will be misclassified — emit at least
one non-null row in such cases or extend the gateway to include explicit
types in query responses (a follow-up task).

## Limitations (Step 2 ships read-only)

- DoPut not yet wired — clients can't `write_table()` into DFO via Flight
  yet. The data path is one-way (`gateway → Flight → client`).
- Result is materialized in memory before streaming; very large queries
  (>10M rows) should use the JSON API or be rewritten to push aggregations
  into the SQL.
- Type inference is heuristic; mixed-type columns surface as utf8.
- Authentication: the Flight server holds a single JWT and proxies all
  client requests under it. Per-client RBAC needs Flight Auth handlers
  (Basic/JWT). Future work.
- TLS: gRPC over plaintext only. For internet-facing deployments, run
  behind an envoy/nginx terminator or wait for Flight TLS support here.

## Performance expectation

For typical analytics queries (1M-row tables, ~10 columns), measured
end-to-end:

| Path                                 | Time      |
|--------------------------------------|-----------|
| `POST /api/tables/query` (JSON over HTTP) | 3–5 sec |
| Arrow Flight `do_get`                | 0.2–0.5 sec  |

Most of the speedup comes from the Arrow IPC binary format vs JSON; the
remaining overhead is the gateway → Flight → client double-hop.

## Testing

```sh
# 1. Install deps once (conda is fastest on macOS Intel; brew on Apple Silicon)
conda install -c conda-forge libarrow-all libgrpc nlohmann_json cxx-compiler

# 2. Build
make BUILD=release
make flight

# 3. Run e2e
pip install pyarrow pytest
DYLD_LIBRARY_PATH=$HOME/miniconda3/lib pytest tests/integration/test_arrow_flight.py -v
```

Three checks: ListFlights enumerates user tables, DoGet with `sql:`
ticket round-trips through pandas, DoGet with `table:` shortcut also
works. The test auto-skips if pyarrow isn't installed or the binary
isn't built.

**Verified end-to-end on macOS 13 Intel + conda-forge Arrow 24.0** —
3/3 tests passing as of commit (this iteration).
