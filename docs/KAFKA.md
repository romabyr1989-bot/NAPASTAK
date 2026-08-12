# DataFlow OS — Kafka Connector

`lib/connector/plugins/kafka/kafka_connector.c` ships as `kafka_connector.so`
and consumes an Apache Kafka topic as a pipeline source, either batch
(`read_batch`) or streaming (`cdc_start`).

## Requirements

- **librdkafka** must be installed at build time, otherwise the Makefile skips
  the plugin entirely and `kafka_connector.so` is never produced — a pipeline
  step of type `kafka` then fails with `connector_load(...) failed`.

  ```sh
  brew install librdkafka        # macOS
  apt-get install librdkafka-dev # Debian/Ubuntu
  ```

  Check the detection result:

  ```sh
  make -n | grep kafka_connector.so   # no output → librdkafka not found
  ls build/debug/lib/kafka_connector.so
  ```

- SCRAM additionally requires a librdkafka built with SASL support (the
  Homebrew and Debian packages are). If it is missing, the connector logs
  `cannot set sasl.mechanism: ... (librdkafka builtin.features=...)` — the
  feature list in that message tells you what the build actually has.

## Pipeline configuration

Connector type is `kafka`. `connector_config`:

| Key | Default | Meaning |
|-----|---------|---------|
| `brokers` | `localhost:9092` | bootstrap servers, `host:port` comma-separated |
| `topic` | — | topic to consume |
| `group_id` | `dfo-consumer` | consumer group |
| `data_format` | `json` | `json` → keys become columns; anything else → `kafka_offset` + `value` |
| `security_protocol` | `plaintext` | `plaintext`, `ssl`, `sasl_plaintext`, `sasl_ssl` |
| `sasl_mechanism` | — | `SCRAM-SHA-256`, `SCRAM-SHA-512`, `PLAIN`, … |
| `sasl_username` | — | SCRAM user |
| `sasl_password` | — | SCRAM password |
| `ssl_ca_location` | — | CA bundle path; empty → system trust store |
| `ssl_certificate_location` | — | client certificate (mTLS) |
| `ssl_key_location` | — | client key (mTLS) |
| `ssl_key_password` | — | passphrase for the client key |
| `ssl_no_verify` | `false` | `true` disables broker hostname verification |

Every key also accepts librdkafka's own dotted spelling (`security.protocol`,
`sasl.username`, …), so a config copied from another Kafka client works as-is.
Protocol and mechanism values are case-insensitive and `-`/`_` are
interchangeable: `SASL-SSL` and `scram_sha_256` are understood.

### SCRAM over TLS

```json
{
  "brokers": "kafka-1:9093,kafka-2:9093",
  "topic": "events",
  "group_id": "dfo-consumer",
  "data_format": "json",
  "security_protocol": "sasl_ssl",
  "sasl_mechanism": "SCRAM-SHA-512",
  "sasl_username": "dfo",
  "sasl_password": "…",
  "ssl_ca_location": "/etc/ssl/certs/kafka-ca.pem"
}
```

### SCRAM without TLS (trusted network only)

```json
{
  "brokers": "kafka:9092",
  "topic": "events",
  "security_protocol": "sasl_plaintext",
  "sasl_mechanism": "SCRAM-SHA-256",
  "sasl_username": "dfo",
  "sasl_password": "…"
}
```

`SASL_PLAINTEXT` sends the SCRAM exchange unencrypted. SCRAM never puts the
password on the wire, but everything else (including the messages) is
cleartext — use it only on a private network.

The same fields are available in the pipeline builder UI: pick **📨 Kafka** as
the step type and choose a protocol; the SASL and TLS fields appear as needed.

## Defaults applied when the config is incomplete

Two combinations are silently wrong on Kafka's side, so the connector fills
them in and logs a `WARN` saying it did:

- `sasl_username` set but no `security_protocol` → `sasl_ssl` when
  `ssl_ca_location` is present, otherwise `sasl_plaintext`. Without this,
  librdkafka speaks PLAINTEXT and the broker drops the connection before SASL
  ever starts.
- `security_protocol` is `sasl_*` but no `sasl_mechanism` → `SCRAM-SHA-512`.
  Kafka's own default is `GSSAPI` (Kerberos), which is never what a
  username/password pair means.

Set both explicitly in production so a config change cannot shift the defaults
under you.

## Troubleshooting

The connector performs one metadata round-trip in `create()`, so authentication
problems appear when the step starts rather than as an empty result 1 second
later. What the gateway log tells you:

| Log line | Cause |
|----------|-------|
| `authentication failed — _AUTHENTICATION: ...` | wrong user/password, or the user has no SCRAM credential on the broker |
| `authentication failed — SASL_AUTHENTICATION_FAILED: ...` | mechanism mismatch — broker offers e.g. SCRAM-SHA-512, config asks for SCRAM-SHA-256 |
| `broker handshake failed: ... Connection refused` | wrong host/port, or the listener speaks a different protocol |
| `cannot set sasl.mechanism: ...` | librdkafka built without SASL |
| `connector_config is not a JSON object` | malformed config; SASL fields are ignored on that fallback path |

Repeated identical broker errors are collapsed — one line per distinct failure
plus an `(N identical suppressed)` counter — because librdkafka retries the
handshake on a backoff.

Verify the broker side independently:

```sh
kafka-configs.sh --bootstrap-server kafka:9092 --describe \
  --entity-type users --entity-name dfo
```

An empty result means the SCRAM credential was never created:

```sh
kafka-configs.sh --bootstrap-server kafka:9092 --alter \
  --add-config 'SCRAM-SHA-512=[password=…]' --entity-type users --entity-name dfo
```

## Security note

`connector_config` is stored as-is in the pipeline definition, so
`sasl_password` sits in the pipeline store in cleartext — the same as
`password` for the PostgreSQL connector. Restrict access to the store and the
pipeline API accordingly. The connector itself never writes credentials to the
log.

## Limits

- `connector_config` is capped at 1024 bytes per step (`PipelineStep`); a full
  SASL + TLS config is ≈400 bytes, but very long certificate paths can hit it.
  Over-long values are logged as truncated instead of failing silently.
- OAUTHBEARER and GSSAPI/Kerberos are passed through to librdkafka but have no
  UI fields and are untested here.
