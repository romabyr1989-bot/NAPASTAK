# Cluster & Replication

## Architecture

One-leader, N-standby topology. The leader handles all writes; standbys receive
WAL data asynchronously and apply it locally. **Failover is manual** — automated
promotion is not yet implemented.

## Leader Configuration

```json
{
  "cluster_mode": true
}
```

Replica endpoints are registered programmatically (see `lib/cluster/cluster.c`).

## Starting a Standby

```bash
./napastak_storage -p 9090 -d ./data_node
```

The standby listens on TCP port 9090 by default and accepts the binary
replication protocol from the leader.

## Replication Protocol

Binary TCP protocol. Each message: `ProtoHeader` (12 bytes) + variable body.

| Message type        | Direction          | Purpose                         |
|--------------------|--------------------|---------------------------------|
| `MSG_PING`          | leader → standby   | Heartbeat                       |
| `MSG_PONG`          | standby → leader   | Heartbeat reply                 |
| `MSG_REPLICATE`     | leader → standby   | Ship WAL bytes                  |
| `MSG_REPL_ACK`      | standby → leader   | Acknowledge receipt             |
| `MSG_STATUS_REQ`    | leader → standby   | Request node status             |
| `MSG_STATUS_RESP`   | standby → leader   | Return node status              |

## Data Flow

1. Every `wal_append()` call on the leader fires a WAL callback.
2. The callback enqueues raw WAL bytes into an async queue (1 024 slots).
3. A worker thread dequeues and sends `MSG_REPLICATE` to each standby.
4. The leader **does not wait** for `MSG_REPL_ACK` before responding to the client.
5. The standby applies bytes verbatim via `table_wal_append()`.

## Cluster Status API

Requires `Authorization: Bearer <token>`.

```bash
curl /api/cluster/status \
  -H "Authorization: Bearer <token>"
```

Returns JSON with leader/standby state, last replicated offset, and lag.

## Failover (Manual)

1. Stop the leader process.
2. Promote the chosen standby by restarting it in leader mode:

```bash
./dfo -c config_promoted.json
```

3. Update client connection strings to point to the new leader.

## Limitations

- Replication is asynchronous — a standby may lag behind the leader.
- No automatic leader election.
- Async queue overflow (>1 024 pending WAL chunks) causes the oldest entries to
  be dropped; standbys may need a full resync in that case.
