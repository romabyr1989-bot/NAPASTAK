# NAPASTAK — Kafka Connector

`lib/connector/plugins/kafka/kafka_connector.c` ships as `kafka_connector.so`
and consumes an Apache Kafka topic as a pipeline source, either batch
(`read_batch`) or streaming (`cdc_start`). It also works as a sink, producing
one JSON message per row.

Message formats (`json`, `csv`, `avro`) and the Confluent Schema Registry
integration are documented separately in [KAFKA_AVRO.md](KAFKA_AVRO.md).

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

- SASL additionally requires a librdkafka built with SASL support (the Homebrew
  and Debian packages are). If it is missing, the connector logs
  `librdkafka отвергла sasl.mechanism=...: ... (builtin.features=...)` — the
  feature list in that message tells you what the build actually has.

## Pipeline configuration

Connector type is `kafka`. `connector_config`:

| Key | Default | Meaning |
|-----|---------|---------|
| `brokers` | `localhost:9092` | bootstrap servers, `host:port` comma-separated |
| `topic` | — | topic to consume |
| `group_id` | `dfo-consumer` | consumer group; see *Group semantics* below |
| `data_format` | `json` | `json`, `csv`, `avro` — see [KAFKA_AVRO.md](KAFKA_AVRO.md) |
| `broker_address_family` | `v4` | `v4`, `v6`, `any`. Default is IPv4 because `localhost` often resolves to `::1` first while brokers listen on IPv4 only |
| `offset_reset` | `earliest` | `earliest`, `latest` — where to start with no committed offset |
| `isolation_level` | `read_uncommitted` | `read_committed` skips aborted transactional messages |
| `offset_start_mode` | `logical` | `logical`, `absolute`, or a literal offset. See *Brokers without ListOffsets* |
| `librdkafka_debug` | — | passed to librdkafka `debug=`, e.g. `broker,protocol,feature` |

### Authentication and transport

| Key | Default | Meaning |
|-----|---------|---------|
| `security_protocol` | `PLAINTEXT` | `PLAINTEXT`, `SSL`, `SASL_PLAINTEXT`, `SASL_SSL` |
| `sasl_mechanism` | `PLAIN` when protocol is `SASL_*` | `PLAIN`, `SCRAM-SHA-256`, `SCRAM-SHA-512`, `GSSAPI` |
| `sasl_username` | — | SASL user |
| `sasl_password` | — | SASL password |
| `ssl_ca_location` | — | broker CA; PEM or DER (DER is converted automatically) |
| `ssl_truststore_location` | — | PKCS#12 truststore; unpacked to PEM automatically |
| `ssl_truststore_password` | — | truststore passphrase |
| `ssl_certificate_location` | — | client certificate for mTLS: PEM, DER or PKCS#12 |
| `ssl_key_location` | — | client key (PEM or DER); not needed with a PKCS#12 keystore |
| `ssl_key_password` | — | passphrase for the key or keystore |
| `ssl_keystore_location` | — | explicit PKCS#12 keystore (alternative to cert + key) |
| `sasl_kerberos_service_name` | `kafka` | GSSAPI broker service principal |
| `sasl_kerberos_principal` | — | GSSAPI client principal; overrides `KAFKA_KERBEROS_PRINCIPAL` |
| `sasl_kerberos_keytab` | — | GSSAPI keytab path; overrides `KAFKA_KERBEROS_KEYTAB` |

Every key also accepts librdkafka's own dotted spelling (`security.protocol`,
`sasl.username`, …), so a config copied from another Kafka client works as-is.
Protocol and mechanism values are case-insensitive and `-`/`_` are
interchangeable: `sasl-ssl` and `scram_sha_256` are understood and normalized to
`SASL_SSL` and `SCRAM-SHA-256`.

TLS certificate-chain and hostname verification are always on when the protocol
includes TLS. There is deliberately no switch to disable them.

### SCRAM over TLS

```json
{
  "brokers": "kafka-1:9093,kafka-2:9093",
  "topic": "events",
  "group_id": "dfo-consumer",
  "data_format": "json",
  "security_protocol": "SASL_SSL",
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
  "security_protocol": "SASL_PLAINTEXT",
  "sasl_mechanism": "SCRAM-SHA-256",
  "sasl_username": "dfo",
  "sasl_password": "…"
}
```

`SASL_PLAINTEXT` sends the SCRAM exchange unencrypted. SCRAM never puts the
password on the wire, but everything else (including the messages) is
cleartext — use it only on a private network.

### Kerberos (GSSAPI)

`sasl_mechanism: GSSAPI` keeps all secrets out of the pipeline definition:
everything comes from the gateway host, configured by ops.

- `/etc/krb5.conf` (realm → KDC) is required.
- With `KAFKA_KERBEROS_KEYTAB` + `KAFKA_KERBEROS_PRINCIPAL` set, librdkafka
  renews the ticket itself via `kinit`.
- Without a keytab the connector disables librdkafka's internal `kinit` and uses
  an existing ticket cache (`KRB5CCNAME`), populated by a system/cron `kinit`.

The `sasl_kerberos_*` config keys exist only as an override for API/YAML use;
the environment variables take precedence.

The same fields are available in the pipeline builder UI: pick **📨 Kafka** as
the step type and choose a protocol; the SASL and TLS fields appear as needed.

## Group semantics

`group_id` controls whether a step re-reads the topic or continues where it
stopped, and the default is *not* incremental:

- **No `group_id`** (default `dfo-consumer`) → a unique ephemeral group per run.
  No committed offsets exist, so the step reads the whole topic every time.
- **Explicit `group_id`** → incremental: read from the committed offset and
  commit after a successful read.

Either way the connector uses `assign` rather than `subscribe`, so connecting
never triggers a rebalance of other consumers on the cluster. Group `subscribe`
happens only in continuous CDC mode (`cdc_start`).

## Defaults applied when the config is incomplete

Two combinations are silently wrong on Kafka's side, so the connector fills them
in and logs a `WARN` saying it did:

- `sasl_username` set but no `security_protocol` → `SASL_SSL` when a CA or
  truststore is configured, otherwise `SASL_PLAINTEXT`. Without this, librdkafka
  speaks PLAINTEXT and the broker drops the connection before SASL ever starts.
- `security_protocol` is `SASL_*` but no `sasl_mechanism` → `PLAIN`. Kafka's own
  default is `GSSAPI`, which without a keytab fails immediately and is never
  what a username/password pair means.

A value librdkafka rejects (a typo in `security_protocol`, say) is **not**
ignored: the context is marked invalid and client creation fails with the
reason, because the old behaviour was to silently stay on PLAINTEXT.

Set both explicitly in production so a config change cannot shift the defaults
under you.

## Brokers without ListOffsets

Some brokers and proxies do not advertise the ListOffsets API. The symptom is
`Failed to query logical offset BEGINNING: Required feature not supported by
broker` while Metadata and SASL work fine. `offset_start_mode` works around it:

- `logical` (default) — `OFFSET_BEGINNING`/`OFFSET_END`, resolved by ListOffsets.
- `absolute` — start from numeric offset 0 for `earliest`, skipping ListOffsets.
- a literal offset (e.g. `15230`) — pin an exact position, bypassing ListOffsets
  in both directions. Last resort.

## Troubleshooting

Authentication failures arrive asynchronously on librdkafka's own thread, so the
connector latches them and reports the reason from the first synchronous
operation that fails — the connection probe, `describe`, or `read_batch`.
Without that latch a wrong password looks exactly like an empty topic.

| Log line | Cause |
|----------|-------|
| `ошибка аутентификации SASL (...): _AUTHENTICATION: ...` | wrong user/password, or the user has no SCRAM credential on the broker |
| `ошибка аутентификации SASL (...): SASL_AUTHENTICATION_FAILED: ...` | mechanism mismatch — broker offers e.g. SCRAM-SHA-512, config asks for SCRAM-SHA-256 |
| `ping failed: ... — ошибка аутентификации SASL ...` | the generic transport code joined with the real SASL reason |
| `librdkafka отвергла sasl.mechanism=...` | librdkafka built without SASL — check `builtin.features` in the same line |
| `cannot reach brokers (...)` | wrong host/port, or the listener speaks a different protocol |
| `connector_config не разобран как JSON-объект` | malformed config; the fallback scanner may misread escaped values |

Set `librdkafka_debug` to `broker,feature,protocol,metadata` to see the API set
the broker advertises (`Broker API support:`) in the gateway log.

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
`sasl_password` sits in the pipeline store in cleartext — the same as `password`
for the PostgreSQL connector. Restrict access to the store and the pipeline API
accordingly. The connector never writes credentials to the log: a rejected
`*password` property is reported by key name only.

Private keys are never written to disk — a DER key is converted in memory and
handed to librdkafka as `ssl.key.pem`. Only public certificates are ever
materialised as temporary PEM files, and those are removed on destroy.

## Limits

- `connector_config` is capped at 1024 bytes per step
  (`PipelineStep.connector_config`); a full SASL + TLS config is ≈400 bytes, but
  very long certificate paths can hit it. Over-long values are logged as
  truncated instead of failing silently.
- JKS keystores are not supported — convert them with `keytool` first.
- OAUTHBEARER is passed through to librdkafka but has no UI fields and is
  untested here.
