<!--
  Licensed to the Apache Software Foundation (ASF) under one
  or more contributor license agreements.  See the NOTICE file
  distributed with this work for additional information
  regarding copyright ownership.  The ASF licenses this file
  to you under the Apache License, Version 2.0 (the
  "License"); you may not use this file except in compliance
  with the License.  You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing,
  software distributed under the License is distributed on an
  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
  KIND, either express or implied.  See the License for the
  specific language governing permissions and limitations
  under the License.
-->

# QuestDB Sink Connector

Writes Apache Iggy messages to [QuestDB](https://questdb.com) over QWP,
QuestDB's native binary protocol, on a WebSocket transport. The same build
works with QuestDB open source and QuestDB Enterprise. Enterprise features are
enabled through the connect string.

Requires QuestDB 10.0 or newer.

## Configuration

```toml
[plugin_config]
connection_string = "ws::addr=localhost:9000;"
table = "iggy_events"
timestamp_source = "message"
timestamp_unit = "auto"
symbol_columns = ["region", "device"]
uuid_columns = []
integer_columns = []
include_stream_column = true
include_topic_column = true
include_partition_column = false
include_offset_column = false
include_headers = false
ack_level = "ok"
flush_timeout = "30s"
batch_size = 1000
max_flush_bytes = 1000000
log_rejected_payload = false
numbers_as_double = true
verbose_logging = false
```

| Field | Default | Description |
| ----- | ------- | ----------- |
| `connection_string` | required | QuestDB connect string, `ws::` or `wss::`. Treated as a secret. |
| `table` | required | Target table. |
| `timestamp_source` | `message` | `message`, `origin`, `payload`, or `server`. |
| `timestamp_field` | none | Payload field holding the timestamp. Required when `timestamp_source = "payload"`. |
| `timestamp_unit` | `auto` | Unit of the payload timestamp: `auto`, `seconds` (`s`), `millis` (`ms`), `micros` (`us`), `nanos` (`ns`). `auto` infers the unit from the magnitude. Used only when `timestamp_source = "payload"`, but an invalid value is rejected with any source. |
| `symbol_columns` | `[]` | Payload fields stored as `SYMBOL`. A record with an object or array in one of these fields is rejected. |
| `uuid_columns` | `[]` | Payload fields holding RFC-4122 strings, stored as `UUID`. A record with a non-string value there is rejected. |
| `integer_columns` | `[]` | Payload fields stored as `LONG`. A fractional, non-numeric or out-of-range value there is rejected. |
| `include_stream_column` | `true` | Write the stream name as a `SYMBOL` column named `iggy_stream`. |
| `include_topic_column` | `true` | Write the topic name as a `SYMBOL` column named `iggy_topic`. |
| `include_partition_column` | `false` | Write the partition as a `LONG` column named `iggy_partition_id`. |
| `include_offset_column` | `false` | Write the offset as a `LONG` column named `iggy_offset`. |
| `include_headers` | `false` | Write each header as a `VARCHAR` column named `header_<key>`. Binary header values are base64-encoded. A key that is not a valid QuestDB column name, for example one with `-` or `.` such as `content-type`, rejects the whole record. Rename such headers with a transform. |
| `ack_level` | `ok` | `ok` waits for the server to accept a flush. `durable` waits for the Enterprise durable ack. If you do not set it and the connect string sets `request_durable_ack=on`, the default is `durable`. |
| `flush_timeout` | `30s` | How long a flush waits for its ack without progress. `0s` with `ack_level = "ok"` means fire and forget. `0s` is not allowed with `durable`. |
| `batch_size` | `1000` | Maximum rows per flush. |
| `max_flush_bytes` | `1000000` | Flush when the buffer reaches this size, even before `batch_size` rows. |
| `numbers_as_double` | `true` | Write every JSON number as a `DOUBLE`. See [Numbers](#numbers). |
| `log_rejected_payload` | `false` | Add a truncated payload to the log line of a rejected record. |
| `verbose_logging` | `false` | Log per-batch details at `info` instead of `debug`. |

Field names in the column lists match without regard to case, as in QuestDB.

Credentials, TLS, store-and-forward, reconnect and multi-host failover are set
in the connect string. See the
[connect string reference](https://questdb.com/docs/connect/clients/connect-string/).

### Batch sizing

A QWP frame has a size cap, about 4 MiB by default, and the client refuses a
frame over the cap with all of its rows. `max_flush_bytes` flushes before a
batch of wide rows reaches it. The 1 MB default leaves room, because the buffer
size is an estimate and a frame can also carry the symbol dictionary. The cap
is the smaller of `max_buf_size` (100 MiB by default) and the segment size
`sf_max_segment_bytes` (4 MiB by default) in the connect string, so with the
defaults you raise the cap through `sf_max_segment_bytes`. The server's own
maximum batch size still applies.

The check runs before each row is added. A message larger than the frame cap
minus `max_flush_bytes` can therefore push one frame over the cap. That flush
fails, and the connector does not write the rows buffered with the message or
the rest of its chunk. It logs their offsets.

## Store-and-forward

Set `sf_dir` in the connect string to persist outgoing frames on disk. Frames
the server has not acknowledged are sent again after a disconnect or a runtime
restart.

```toml
connection_string = "ws::addr=localhost:9000;sf_dir=/var/lib/iggy/questdb-sf;sender_id=iggy;"
```

Use it in production. This sink does not support the runtime's
`[delivery] guarantee = "at_least_once"`, so the runtime commits the offset of
each poll before `consume()` runs and does not replay a failed batch. The
queue of the client is the only copy of rows that `consume()` reported.

`sf_dir` keeps frames across a process crash or restart, but not across a host
failure. By default the client does not fsync the frames, so a power loss or a
kernel crash can lose frames that `consume()` already reported. Add
`sf_durability=periodic` to fsync them every `sf_sync_interval_millis` (5000 ms
by default). A host failure can then lose only the frames of the last interval.
To recover rows lost this way, see
[Recovery after a host failure](#recovery-after-a-host-failure).

- The parent directory of `sf_dir` must exist. `~` is not expanded.
- Give each connector instance its own `sender_id` or its own `sf_dir`.
  Otherwise one instance can replay frames the other queued.
- A frame that QuestDB rejects terminally stays on disk and blocks every later
  batch. See [Frames that QuestDB rejects](#frames-that-questdb-rejects).
- If a symbol dictionary fills up, the slot carries it to the next connection,
  and flushes keep failing until the slot drains. This usually means a
  `symbol_columns` field with unbounded cardinality. Store it as a `VARCHAR`.

### Frames that QuestDB rejects

QuestDB rejects a frame terminally on a schema mismatch, a parse error or a
security error, and when a frame keeps failing without progress. A schema
mismatch is the usual cause, for example a producer that sends text in a field
that the table stores as a number. The client cannot catch it before it sends
the frame, because it does not know the table schema.

Without `sf_dir`, the rejection fails the batch that holds the frame, and the
later batches are written.

With `sf_dir`, the client keeps the rejected frame in its slot directory and
sends it first after every reconnect and every restart. QuestDB rejects it
again, so every later batch of the connector fails behind it. The QuestDB
clients do this by design: a schema problem stops ingestion instead of dropping
rows silently. The sink logs each blocked batch with its offsets, in a line
that starts with `QuestDB did not confirm writing`. To recover:

1. Stop the connectors runtime.
2. Fix the cause, for example the producer or the column type in the table.
3. Delete the slot directories under `<sf_dir>/<sender_id>-ingest-*/`. This
   drops every frame that they hold, including the frames of other slots that
   QuestDB did not acknowledge yet.
4. Rewind the consumer group of the sink, as
   [Recovery after a host failure](#recovery-after-a-host-failure) describes,
   to the lowest offset in the sink's `QuestDB did not confirm writing` and
   `acknowledgement has not arrived yet` lines since the rejection. With fire
   and forget, those offsets are logged only at `debug`, so rewind to a point
   before the rejection.
5. Start the connectors runtime.

If you fixed the cause, the rejected record is written again with the rest. If
you cannot fix it, the sink must skip it: rewind past the batch that holds it,
which also skips the valid rows of that batch. The rejection names the column
and the value, not the offset, so finding it can take a search of the topic.

## Connection pool

The connector borrows a pooled connection for each chunk it writes, and the
runtime writes each topic in its own task. `sender_pool_max` caps the pool and
defaults to 4. When the pool is full, a borrow waits `acquire_timeout_ms`
(5 s by default) and then fails, and the rest of that batch is lost. Set
`sender_pool_max` to at least the number of topics the connector consumes:

```toml
connection_string = "ws::addr=localhost:9000;sender_pool_max=16;"
```

## Enterprise

- **Multi-host failover**: list several endpoints, for example
  `addr=node-a:9000,node-b:9000`. The client moves to another endpoint on
  reconnect and keeps buffering while a replica is promoted.
- **TLS**: use `wss::`. For a private CA, point `tls_roots` at its certificate
  file, and add `tls_roots_password` if that file is a JKS or PKCS#12 keystore.
  `tls_verify=unsafe_off` turns off certificate checks. A default build refuses
  it. Build with the `insecure-skip-verify` feature to use it against a test
  server.
- **Durable ack**: set `request_durable_ack=on` in the connect string. The
  sink then waits for the durable ack, unless you set `ack_level = "ok"`. The
  connector does not start with `ack_level = "durable"` alone, because the
  client would refuse every wait. The server also needs replication
  configured. Without it no flush is ever acknowledged.

### Choosing an ack level

`ok` returns when the server accepts a flush. `durable` also waits until the
write-ahead log is shipped. With `ok`, a primary that fails before shipping can
lose rows it already acknowledged. Use `durable` when losing that tail matters.

`durable` adds one wait for log shipping to every flush, whatever the batch
size, so throughput grows with rows per batch.

### Server durability

An `ok` acknowledgement means that QuestDB committed the rows, but by default
QuestDB does not flush a commit to disk. A QuestDB process crash keeps the rows.
An OS crash or a power loss on the QuestDB host can lose them.

QuestDB 10.2.0 and QuestDB Enterprise 4.2.0 add the `adaptive` commit mode,
which closes this gap for WAL tables. Set `cairo.commit.mode=adaptive` in the
server configuration. On Enterprise, set it on every node that can become
primary. Each commit then flushes its write-ahead log files to disk, so a row
the sink got an `ok` for survives a power loss. Commits can share one flush
for up to `cairo.adaptive.commit.group.window` (50 ms by default), and a power
loss can drop commits acknowledged since the last flush. Set the window to `0`
to flush every commit before QuestDB acknowledges it. QuestDB 10.0 and 10.1 and
QuestDB Enterprise 4.0 and 4.1 do not have this mode. See the
[durability documentation](https://questdb.com/docs/concepts/durability/).

The commit mode is a server setting, and the sink needs no change for it.
`request_durable_ack=local`, which waits for that flush, is not supported yet,
because the `questdb-rs` client that the sink uses accepts only `on` and
`off`.

## Types

QuestDB creates the table and its columns from the first rows it receives.
Create the table yourself if you need partitioning, `TTL`, `DEDUP UPSERT KEYS`,
symbol capacities or a specific designated timestamp name.

| JSON | QuestDB |
| ---- | ------- |
| string | `VARCHAR`, or `SYMBOL` / `UUID` when listed in `symbol_columns` / `uuid_columns` |
| number | `DOUBLE`, or `LONG` when listed in `integer_columns` |
| boolean | `BOOLEAN` |
| array of numbers | `DOUBLE[]`, up to 3 dimensions. An empty array is rejected. |
| object | `VARCHAR` holding the JSON text |
| `null` | column omitted |

Integer arrays are stored as `DOUBLE[]`, because QuestDB has no other array
type. `BOOLEAN` has no null, so an omitted boolean reads back as `false`.

A payload that is not a JSON object goes into one `VARCHAR` column named
`payload`. That covers `Raw`, `Text`, `FlatBuffer` and `Avro` payloads, proto
text that is not JSON, and JSON arrays or scalars.

### Numbers

JSON has one number type, so a field can arrive as `20` in one record and
`20.5` in the next. With `numbers_as_double = true`, the default, every number
is written as a `DOUBLE`, and the column type does not depend on which value
came first.

A `DOUBLE` holds integers exactly only up to 2^53 (9007199254740992). Values
such as nanosecond epochs, snowflake ids and hashes are larger. List those
fields in `integer_columns` to store them as `LONG`:

```toml
integer_columns = ["trade_id", "event_nanos"]
```

A whole value written as `20.0` is accepted in an `integer_columns` field. A
fractional value, a non-number or a value outside the `LONG` range there is
rejected. When the connector rounds a
number past 2^53 to a `DOUBLE`, it logs one warning per kind of value with the
column name.

With `numbers_as_double = false`, each value takes its type from its JSON shape:
integers become `LONG` and fractions `DOUBLE`. Two problems follow:

- The client fixes a column's type from the first row of each flush. Later rows
  in that flush with the other number shape are rejected.
- If the first rows of an auto-created column are all whole numbers, QuestDB
  creates it as `LONG` for good.

Turn it off only if every numeric field is a true integer and you create the
table with explicit column types.

### Timestamps

An auto-created table names its designated timestamp `timestamp`. A payload
field with the same name collides with it, and the server rejects the whole
flush. Rename the field with a transform, or create the table with
`timestamp(<name>)`. The field named by `timestamp_field` is used only as the
designated timestamp.

`message` and `origin` timestamps are in microseconds, so an auto-created table
gets a `TIMESTAMP` column. `payload` timestamps are converted to nanoseconds and
give a `TIMESTAMP_NS` column. With `server`, QuestDB stamps each row on arrival.
Use one source per table.

## Delivery semantics

Two boundaries decide delivery.

Between Iggy and the sink, delivery is at most once. The sink does not support
the runtime's `[delivery] guarantee = "at_least_once"`, and a connector that
sets it fails to start. The runtime commits the offset of each poll before
`consume()` runs and does not replay a batch.

Between the sink and QuestDB, delivery is at least once. A reconnect, a failover
or a replay of queued frames can send the same rows again. Declare
[`DEDUP UPSERT KEYS(...)`](https://questdb.com/docs/concepts/deduplication/) on
tables that must not have duplicates.

The client queues unacknowledged frames in memory, or on disk when `sf_dir` is
set. Rows can be lost in these cases:

- With `ack_level = "ok"` on a cluster, a primary that fails before it ships
  its write-ahead log can lose rows it accepted. See
  [Choosing an ack level](#choosing-an-ack-level).
- Without `sf_dir`, a crash, or a shutdown that outlasts
  `close_flush_timeout_millis` in the connect string, loses the queue even
  though `consume()` already reported success.
- With `sf_dir`, a host failure loses the frames that the client did not fsync.
  See [Store-and-forward](#store-and-forward).
- A crash of the runtime after a poll and before the sink flushes loses that
  batch, also with `sf_dir`, because the runtime already committed its offset.
- A flush that is not acknowledged yet, or any flush with fire and forget,
  stays queued while `consume()` reports success. If the connection later
  fails for good, for example on a terminal rejection of an earlier frame or an
  authentication error, the client drops the frames queued in memory. The sink
  logs the offsets of each unacknowledged flush, at `warn` for a pending ack
  and at `debug` for fire and forget, so that you can rewind to them.
- With `ack_level = "ok"`, an OS crash or a power loss on the QuestDB host can
  lose acknowledged rows. See [Server durability](#server-durability).
- With `sf_dir`, a frame that QuestDB rejects terminally makes every later
  batch fail until you delete its slot directory. The sink logs the offsets of
  each of those batches. See
  [Frames that QuestDB rejects](#frames-that-questdb-rejects).
- Rows the sink does not write, such as a rejected record or a failed chunk,
  are lost, as the rest of this section describes.

A batch fails when:

- QuestDB rejects a frame terminally. The client reports some rejections after
  the flush that caused them returned, without saying which topic sent the
  frame. Such a rejection fails the next batch, of any topic on this connector,
  that has no error of its own.
- A chunk fails to write. If the cause is the connection, the connector skips
  the remaining chunks of the batch. It logs the offsets and message ids of the
  messages it did not write.
- 10 batches in a row on one topic each leave a flush unacknowledged within
  `flush_timeout`. A single late ack is only a warning, because the frames stay
  queued and are delivered later.
- The client's inbox of rejection reports overflows and drops events. A terminal
  rejection may then be unreported, so the next batch fails although its own
  rows were written.

The runtime counts a failed batch in `iggy_connector_errors_total`. It does not
retry it.

`flush_timeout` also bounds shutdown. The runtime does not interrupt a flush in
progress. A batch in flight can wait up to `flush_timeout` for each of its
remaining flushes, so shutdown can take several times `flush_timeout`.

### Rejected records

A rejected record is lost, and its log line is the only record of it. The
runtime has already committed its offset, and there is no dead-letter queue.

Each rejection is logged with its stream, topic, partition, offset and message
id. The first 20 per batch are logged at `error` and the rest at `debug`.

Rejections are not counted in the runtime metrics. A batch that writes some
rows and rejects others counts as a success, so `iggy_connector_errors_total`
stays at zero. The plugin cannot report per-row counts through the current SDK.
Alert on the connector's logs instead.

A record is rejected when:

- **The connector rejects it before writing**: a binary payload that is not
  UTF-8, a malformed UUID, a bad array, a value of the wrong type in a listed
  column, a missing or out-of-range timestamp field, an invalid or duplicate
  column name, or a row with no columns. The rest of the batch is written.
- **The client rejects it while writing**, usually because a column already has
  another type in this flush. The rest of the batch is written.
- **The server rejects the frame.** QuestDB rejects whole frames, so the
  connector fails the batch and cannot name the record.

Set `log_rejected_payload = true` to add a truncated payload to these log lines.
This writes user data to the log.

### Recovery after a host failure

Rows that a host failure lost are still in Iggy while the topic retains them.
To write them again, rewind the consumer group of the sink. Its name is the
`consumer_group` of the stream, or `iggy-connect-sink-<key>` when that is not
set.

The highest `iggy_offset` in the table is not a safe place to start. The sink
writes the chunks of a topic through several pooled connections, so rows below
that offset can be missing. Start from an offset before the failure, with a
margin, and let `DEDUP UPSERT KEYS` drop the rows that are written twice.

1. Stop the connectors runtime.
2. For each partition, find the first offset to write again, before the failure.
3. Set the stored offset to one less than that offset. Iggy resumes after the
   stored offset.

   ```sh
   iggy consumer-offset set questdb_sink events questdb_events <partition> <offset> --kind consumer-group
   ```

4. Start the connectors runtime.

The sink writes some rows a second time. Declare `DEDUP UPSERT KEYS` on the
table to keep one copy of each row. This does not work with
`timestamp_source = "server"`, because the second copy gets a new timestamp.

### Reserved column names

The connector writes these columns itself. A record with a payload field of the
same name, ignoring case, is rejected.

| Column | Written when |
| ------ | ------------ |
| `iggy_stream` | `include_stream_column = true`, the default |
| `iggy_topic` | `include_topic_column = true`, the default |
| `iggy_partition_id` | `include_partition_column = true` |
| `iggy_offset` | `include_offset_column = true` |
| `header_<key>` | `include_headers = true` |

The `iggy_` prefix keeps these names apart from common payload fields such as
`stream`, `topic` or `offset`, which are written as ordinary columns. A payload
field named like a reserved column is rare. If you have one, turn the flag off
or rename the field with a transform.

## Testing

Unit tests:

```bash
cargo nextest run -p iggy_connector_questdb_sink --lib
```

The integration tests start a QuestDB container and need Docker. Build the
test server, the runtime and the plugin first. The tests run the server and the
runtime from `target/debug/`, the runtime loads the plugin from there, and
`cargo nextest` builds none of them:

```bash
./scripts/build-test-server.sh
cargo build -p iggy-connectors -p iggy_connector_questdb_sink
cargo nextest run -p iggy_connector_questdb_sink --test questdb
```

The tests share one QuestDB container, `iggy-test-questdb`, and each test
writes to its own table. The container stays up after the run so the next run
can reuse it. Remove it with `docker rm -f iggy-test-questdb`. The outage test
pauses QuestDB, so it runs on a container of its own.

If the plugin is missing, the tests fail at once with the build command to run.
Rebuild it after every change to the sink, because the tests do not detect a
stale build.

### Enterprise

Failover, TLS and durable ack need a QuestDB Enterprise cluster, and the image
is not public, so CI does not cover them. They were tested by hand on a
three-node cluster with token auth over `wss::`: an unreachable first endpoint,
a primary stopped and restarted, graceful promotions, a period with no primary,
and a hard stop of two nodes. Rows were written throughout, and every run ended
with no gaps and no duplicates.
