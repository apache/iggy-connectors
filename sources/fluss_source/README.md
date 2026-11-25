# Apache Fluss source connector

Reads rows from an [Apache Fluss](https://fluss.apache.org/) log table and publishes them to an Apache Iggy stream as JSON.

Each Fluss row becomes one Apache Iggy message. Offsets are tracked per bucket and persisted through the runtime state API, so a restart resumes where the previous run stopped.
Offsets only advance once the runtime acknowledges a delivered batch. A rejected batch is kept and sent again on the next poll, and a poll that fails partway points the scanner back at the last acknowledged offsets before it reads on. The start position is saved on its own before the first rows are sent, so a restart does not resolve `latest` again past rows written in between, even after those rows were rejected.
After five rejected batches in a row the source stops and needs a manual connector restart.

## Configuration

| Field | Required | Default | Description |
| ----- | -------- | ------- | ----------- |
| `bootstrap_servers` | yes | | Coordinator server address, for example `localhost:9123`, or several of them separated by commas. |
| `database` | yes | | Apache Fluss database name. |
| `table` | yes | | Apache Fluss table name. |
| `table_type` | no | `log` | Only `log` is accepted. See [Limitations](#limitations). |
| `starting_offset` | no | `earliest` | `earliest`, `latest` (each bucket's tail, resolved at startup), or an explicit non-negative offset such as `128`. Applies to the first start only, while no offsets are saved. An explicit offset is used as is for every bucket, so it suits a table whose bucket offsets you know, such as one with a single bucket. Once offsets are saved, a bucket they do not hold was added to the table later, and the next start reads it from its beginning. |
| `columns` | no | all columns | Columns written to each message, in this order. Must name at least one column, and no column twice. They are picked after the rows are read, so the server still sends every column: a projection pushed down to `fluss-rs` 1.0 fails on rows tiered to remote storage before a column was added. |
| `poll_interval` | no | `1s` | Delay before each poll. A poll that follows a full `batch_size` read, or the short read right after one, starts at once, since more rows are likely waiting. After a failed poll the delay is at least `1s`, since the connector is polled again right after an error. |
| `poll_timeout` | no | `5s` | How long a single server poll waits for records. It and `poll_interval` cannot both be zero. Must not exceed `1day`. |
| `request_timeout` | no | `30s` | How long the cluster may take to answer. Opening fails after that long without an answer. While reading, the connector asks each bucket leader for its end offset once per `request_timeout`, and when that request gets no answer in time it reports an error and connects again from the last acknowledged offsets. Must be greater than 0 and must not exceed `1day`. |
| `batch_size` | no | `500` | Maximum records returned per poll (`scanner.log.max-poll-records`). Must be greater than 0. |
| `payload_format` | no | `json` | Only `json` is accepted. See [Limitations](#limitations). |
| `include_metadata` | no | `false` | Adds `_fluss_bucket`, `_fluss_offset` and `_fluss_timestamp` (the record timestamp in epoch milliseconds) to each JSON object. While it is on, a column that `columns` selects is rejected at startup when its name starts with `_fluss_`, since it would be overwritten. A column left out of `columns` is not checked. |
| `sasl_username` | no | | Enables SASL/PLAIN together with `sasl_password`. Set both or neither. `fluss-rs` 1.0 has no TLS setting, so both cross the network in clear text. Use SASL only on a network you trust. |
| `sasl_password` | no | | The connector never logs it and `/stats` never shows it, but the runtime does not redact it: its config API returns the raw plugin config, such as `GET /sources/{key}/configs/plugin`. A password made only of digits has to be set in the configuration file, since an environment variable turns it into a number, which the connector rejects. |
| `verbose_logging` | no | `false` | Logs per-batch counts at info instead of debug. |

A `poll_interval`, `poll_timeout` or `request_timeout` that cannot be parsed is replaced by its default, with a warning in the runtime log.

## Example

```toml
type = "source"
key = "fluss"
enabled = true
version = 0
name = "Apache Fluss source"
path = "libiggy_connector_fluss_source"

[[streams]]
stream = "fluss_events"
topic = "events"
schema = "json"
batch_length = 100

[plugin_config]
bootstrap_servers = "localhost:9123"
database = "mydb"
table = "events"
poll_interval = "1s"
batch_size = 500
```

The connector produces JSON, so give each `[[streams]]` entry `schema = "json"` or another schema the runtime can encode JSON into. `raw` is not one: the runtime rejects every batch, and the source stops after five of them.

Bring up a local cluster with the [official Docker compose recipe](https://fluss.apache.org/docs/install-deploy/deploying-with-docker/) (ZooKeeper, one coordinator server, one tablet server), then start the connectors runtime. The runtime creates a missing stream and topic itself. A topic you create with the Apache Iggy CLI needs `--durability persisted`, since the runtime refuses a source topic with any other durability.

## Type mapping

| Apache Fluss type | JSON |
| ----------------- | ---- |
| `BOOLEAN` | boolean |
| `TINYINT`, `SMALLINT`, `INT`, `BIGINT` | number |
| `FLOAT`, `DOUBLE` | number, `null` when not finite |
| `CHAR`, `STRING` | string |
| `DECIMAL` | string without an exponent, to keep the full precision, `0.00000012` |
| `DATE` | string, `2024-02-29` |
| `TIME` | string, `12:34:56.789` |
| `TIMESTAMP` | string without a timezone, `2023-11-14 22:13:20.123456` |
| `TIMESTAMP_LTZ` | string in UTC, RFC 3339, `2023-11-14T22:13:20.123456+00:00` |
| `BINARY`, `BYTES` | base64 string |
| `ARRAY`, `MAP`, `ROW` | not supported. A column of these types that `columns` selects is rejected at startup, and one left out of `columns` is not checked |

Temporal values keep every fractional digit the column holds, so the default `TIMESTAMP(6)` keeps its microseconds. They are formatted the same way the PostgreSQL source formats them: `TIMESTAMP` carries no timezone and is written without one, while `TIMESTAMP_LTZ` is an instant and is written in UTC.

Rows are decoded with the table schema read when the connector starts. A row written before a column was added carries `null` for it, and a column added after startup stays out of the output until the connector restarts.

Rows the cluster has already moved to remote storage are read from there, with the credentials the cluster hands out. The client reads remote storage on a local file system, S3 or OSS. Reading from a cluster that tiers to another store, such as HDFS, fails. The client downloads those segments into a `fluss-remote-logs*` directory under the system temp directory and removes it when the connector closes, so a runtime that is killed leaves the directory behind.

Every message carries an `id` derived from its bucket and offset, so a consumer can spot a record replayed after an at-least-once redelivery (the Apache Iggy server does not deduplicate on it). The offset fills the low 64 bits, the bucket the next 32, and the top bit is always set, since a zero id would be replaced with a random one when the message is sent.

The connector also hands the runtime the Fluss record timestamp as `origin_timestamp`, but the runtime does not pass it on, so Apache Iggy stamps each message with its own time. Turn on `include_metadata` to keep the record timestamp in the payload as `_fluss_timestamp`.

## Limitations

These are limits of this connector, not of the `fluss-rs` client.

- **Primary-key tables are not supported yet.** `fluss-rs` 1.0 reads their changelog, but mapping its insert, update and delete records onto messages is left for a follow-up, so `table_type = "primary_key"` is rejected at startup.
- **`payload_format = "arrow_ipc"` is not implemented yet.** The client does expose an Arrow `RecordBatch` scanner, but it uses a different offset-tracking path, so it is left for a follow-up.
- **Partitioned tables are not supported.** They are detected and rejected at startup.
- **Do not set `exactly_once` for this source.** The runtime's [exactly-once mode](../../runtime/README.md#exactly-once-sources) needs the same messages from the same state, but after a restart the first poll from the saved offsets can return other rows than the batch in flight. A batch with another row count can land twice. On a table with several buckets, one with the same count passes as a duplicate, so its rows are dropped while the offsets move past them.
- **Rows that cannot be converted are skipped.** A value the JSON mapping cannot hold, such as a date beyond what `chrono` represents, fails the same way on every read. Such a row is dropped and logged at error level with its bucket and offset, the offsets move past it, and the number of skipped rows is logged when the connector closes.
- **A stop right after starting from `latest` can miss rows.** The resolved start offsets stay in memory until the batch that carries them alone is acknowledged, which normally takes one poll. If the connector crashes or is stopped in that window, the next start resolves `latest` again and misses the rows written in between.
- **A state file that cannot be written keeps that window open.** That batch carries no rows, so rejected rows cannot hold it back. A failed state save does: the runtime rejects the batch, and after five rejections in a row it stops the source with the start offsets still unsaved.
- **An unreadable saved state stops the connector at startup.** Starting over from `starting_offset` could skip rows that were never delivered, `latest` above all, so the connector refuses to open until the saved state is removed (the `source_<key>.state` file under the runtime's state path with the default file storage).
- **A saved state only fits the table it was read from.** The state records the table name and id, and the connector refuses to open over a state saved for another table, or for a table dropped and created again under the same name, until that state is removed. Table ids start over in every cluster, so a table with the same name and id on another cluster is not told apart.
- **The reason a start failed is only in the runtime log.** The runtime reports a failed start as a generic initialization error in `last_error`, so the connector logs the cause at error level.
- **A cluster outage does not show in the connector status.** While the servers refuse connections, the `fluss-rs` client retries inside each poll and returns no rows, so the source stays `running` and counts no error. Only warnings in the runtime log show the outage. Reading resumes from the same offsets once the cluster is back.
- **A server that stops answering is noticed only after `request_timeout`.** `fluss-rs` 1.0 puts no deadline on a request, so a connection that stays open without answering would leave the scanner waiting on it. The check runs between polls, so it can take `poll_timeout` plus `poll_interval` plus twice `request_timeout` until the connector reports an error and connects again from the last acknowledged offsets.
- **Connecting again keeps what the connector opened with.** The new client reads the same table with the same columns. If the table was dropped and created again, or one of those columns was dropped, renamed or changed type, polls fail without connecting until the connector is restarted.
- **A replaced client keeps its connections open.** `fluss-rs` 1.0 does not close the connections of a client that still waits for an answer on them, so they stay open until the server closes them or the runtime stops. A server that stays silent for hours adds a few sockets with each reconnect.
- **An offset outside a bucket's range is not reset.** If a configured offset, or one restored from state after retention removed it, falls outside what the bucket still holds, polls keep failing until the offset is changed. The connector does not move to the earliest or latest offset on its own.

## Build and test

```bash
cargo build --release -p iggy_connector_fluss_source
cargo test -p iggy_connector_fluss_source --lib
```

The integration tests need Docker. Testcontainers starts Apache Fluss 1.0.0, and each test starts the `iggy-server` that `./scripts/build-test-server.sh` builds.

```bash
cargo build -p iggy-connectors -p iggy_connector_fluss_source
cargo test -p iggy_connector_fluss_source --test fluss
```
