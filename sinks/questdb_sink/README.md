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

Requires QuestDB 10.0 or QuestDB Enterprise 4.0, or newer.
`ack_level = "local_durable"` requires QuestDB 10.2.0 or QuestDB Enterprise
4.2.1. The tests run against QuestDB 10.2.0 with `cairo.commit.mode=adaptive`.

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
# Leave ack_level unset to follow request_durable_ack in the connect string.
# ack_level = "ok"
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
| `timestamp_source` | `message` | `message`, `origin`, `payload`, or `server`. An auto-created table names this column `timestamp`, so a payload field named `timestamp` collides with it. See [Timestamps](#timestamps). |
| `timestamp_field` | none | Payload field holding the timestamp, as a JSON number since the Unix epoch in the unit of `timestamp_unit`. A string, such as an RFC 3339 date, rejects the record. Required when `timestamp_source = "payload"`, and rejected with any other source. |
| `timestamp_unit` | `auto` | Unit of the payload timestamp: `auto`, `seconds` (`s`), `millis` (`ms`), `micros` (`us`), `nanos` (`ns`). `auto` infers the unit from the magnitude, see [Timestamps](#timestamps). Used only when `timestamp_source = "payload"`. A value other than `auto` is rejected with another source, and an invalid value is rejected with any source. |
| `symbol_columns` | `[]` | Payload fields stored as `SYMBOL`. A record with an object or a non-empty array in one of these fields, or with a value longer than 1 MiB, is rejected. An empty array is left out, as a `null` is. |
| `uuid_columns` | `[]` | Payload fields holding RFC-4122 strings, stored as `UUID`. A record with a non-string value there is rejected. |
| `integer_columns` | `[]` | Payload fields stored as `LONG`. A fractional, non-numeric or out-of-range value there is rejected. |
| `include_stream_column` | `true` | Write the stream name as a `SYMBOL` column named `iggy_stream`. |
| `include_topic_column` | `true` | Write the topic name as a `SYMBOL` column named `iggy_topic`. |
| `include_partition_column` | `false` | Write the partition as a `LONG` column named `iggy_partition_id`. |
| `include_offset_column` | `false` | Write the offset as a `LONG` column named `iggy_offset`. The partition and the offset identify a message, so they can serve as DEDUP keys for a payload that has no key of its own. |
| `include_headers` | `false` | Write each header as a `VARCHAR` column named `header_<name>`, so `content-type` is written to `header_content_type`. See [Headers](#headers). |
| `ack_level` | from the connect string | `ok` waits for the server to accept the rows of a chunk. `durable` waits for the Enterprise durable ack. `local_durable` waits until the server flushed the rows to disk. If you do not set it, `request_durable_ack` in the connect string decides: `on`, `replicated` or `local,replicated` gives `durable`, `local` gives `local_durable`, and `off` or no key gives `ok`. `durable` and `local_durable` must match that key, so only `ok` changes the level. |
| `flush_timeout` | `30s` | How long a chunk waits for its ack without progress. See [Batch sizing](#batch-sizing). `0s` with `ack_level = "ok"` means fire and forget. `0s` is not allowed with `durable` or `local_durable`. |
| `batch_size` | `1000` | Maximum rows per chunk. |
| `max_flush_bytes` | `1000000` | Flush when the buffer reaches this size, even before `batch_size` rows. Keep it below the frame cap, see [Batch sizing](#batch-sizing). |
| `numbers_as_double` | `true` | Write every JSON number as a `DOUBLE`. See [Numbers](#numbers). |
| `log_rejected_payload` | `false` | Add a truncated payload to the log line of a rejected record. |
| `verbose_logging` | `false` | Log per-batch details at `info` instead of `debug`. |

Field names in the column lists and in `timestamp_field` match without regard
to case, as in QuestDB. A record with two fields that match `timestamp_field`
is rejected.

Credentials, TLS, store-and-forward, reconnect and multi-host failover are set
in the connect string. See the
[connect string reference](https://questdb.com/docs/connect/clients/connect-string/).

The delivery guarantee is set in the runtime's `[delivery]` table of the
connector, not in `plugin_config`. Set at least once for every table that must
not lose rows:

```toml
[delivery]
guarantee = "at_least_once"
```

Without this table the runtime delivers at most once, which is best effort: a
failed write loses the messages of its batch, and the sink only logs their
offsets. The example configuration sets at least once. See
[Delivery semantics](#delivery-semantics).

### Batch sizing

A QWP frame has a size cap, about 4 MiB by default, and the client refuses a
frame over the cap with all of its rows. `max_flush_bytes` flushes before a
batch of wide rows reaches it. The 1 MB default leaves room, because the buffer
size is an estimate and a frame can also carry the symbol dictionary. The cap
is the smaller of `max_buf_size` (100 MiB by default) and the segment size
`sf_max_segment_bytes` (4 MiB by default) in the connect string, so with the
defaults you raise the cap through `sf_max_segment_bytes`. The server's own
maximum batch size still applies.

The sink splits each batch into chunks of up to `batch_size` rows. It
publishes each flush of a chunk without a wait, and then waits once for the
acknowledgement of all of them, so a chunk of wide rows costs one server round
trip.

The check runs after each row is added. A row that takes the buffer to
`max_flush_bytes` is taken out again, the rows in front of it are published,
and the row starts the next frame. So a message larger than the frame cap is
alone in its frame, and only that flush fails. The sink then rejects the
message as a [rejected record](#rejected-records).

If the frame cap is below `max_flush_bytes`, the client refuses a frame of
several rows before the check fires. The sink then builds the rows of that frame
again in two halves, and halves again until each frame fits or holds one row.
It also lowers its flush size to half of the refused frame, for every later
frame of the connector, and logs a warning each time it does. Set
`max_flush_bytes` below the frame cap to avoid the refused frames.

## Store-and-forward

Set `sf_dir` in the connect string to persist outgoing frames on disk. Frames
the server has not acknowledged are sent again after a disconnect or a runtime
restart.

```toml
connection_string = "ws::addr=localhost:9000;sf_dir=/var/lib/iggy/questdb-sf;sender_id=iggy;"
```

Use it in production with at-most-once delivery, where the queue of the
client is the only copy of rows that `consume()` reported. At least once,
`sf_dir` only spares a replay. See [Delivery semantics](#delivery-semantics).

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

Without `sf_dir`, the rejection fails the batch that holds the frame. At most
once, the later batches are written. At least once, the error is permanent, so
the runtime stops the delivery of the topic at that batch. Fix the cause, for example the
column type in the table, and restart the connector: the runtime then delivers
the batch again.

A chunk whose acknowledgement timed out returns its connection to the pool with
its frames still queued, and QuestDB can reject one of those frames later:

- If the rejection arrives while the connection is idle, the pool retires the
  connection at the next borrow. The rejection then fails the next batch, of
  any topic on this connector, that has no error of its own. At least once,
  that failure is transient.
- If it arrives while another chunk holds the connection, perhaps of another
  topic, it fails that chunk. At least once, the sink sees that the rejection
  starts at a frame that is not its own and reports a transient error, so the
  runtime delivers that batch again on a fresh connection. This also holds
  when QuestDB rejects the same frame too many times and the client closes the
  connection, because that rejection names every unacknowledged frame, the
  chunk's own frames included.

With `sf_dir`, the client keeps the rejected frame in its slot directory and
sends it first after every reconnect and every restart. QuestDB rejects it
again, so every later batch of the connector fails behind it. The QuestDB
clients do this by design: a schema problem stops ingestion instead of dropping
rows silently. The sink logs each blocked batch with its offsets, in a line
that starts with `QuestDB did not confirm writing`. At least once, the frame
belongs to an earlier batch, so each blocked batch fails with a transient
error, and the runtime retries it until you delete the slot or
`retry_max_attempts` in `[delivery]` runs out. To recover:

1. Stop the connectors runtime.
2. Fix the cause, for example the producer or the column type in the table.
3. Delete the slot directories under `<sf_dir>/<sender_id>-ingest-*/`. If
   the connect string sets no `sender_id`, it is `default`. This drops every
   frame that they hold, including the frames of other slots that QuestDB did
   not acknowledge yet.
4. Rewind the consumer group of the sink, as
   [Recovery after a host failure](#recovery-after-a-host-failure) describes,
   to the lowest offset in the sink's `QuestDB did not confirm writing` and
   `acknowledgement has not arrived yet` lines since the rejection. With fire
   and forget, those offsets are logged only at `debug`, so rewind to a point
   before the rejection. The logged range can start up to one chunk early, at
   rows that QuestDB acknowledged just before the failure. Without DEDUP, the
   table then holds those rows twice.
5. Start the connectors runtime.

If you fixed the cause, the rejected record is written again with the rest. If
you cannot fix it, the sink must skip it: rewind past the batch that holds it,
which also skips the valid rows of that batch. The rejection names the column
and the value, not the offset, so finding it can take a search of the topic.

## Connection pool

The connector borrows a pooled connection for each chunk it writes, and the
runtime writes each topic in its own task. `sender_pool_max` caps the pool and
defaults to 4. When the pool is full, a borrow waits `acquire_timeout_ms`
(5 s by default) and then fails. At most once, the rest of that batch is lost.
At least once, the runtime delivers the batch again after a backoff. Set
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
- **Durable ack**: set `request_durable_ack=on`, `replicated` or
  `local,replicated` in the connect string. The three ask for the same
  acknowledgement. `replicated` and `local,replicated` need Enterprise 4.2.1.
  Older servers accept only `on`. `local,replicated` also needs
  `cairo.commit.mode=adaptive`, as `local` does. The
  sink then waits for the durable ack, unless you set `ack_level = "ok"`, which
  logs a warning at start. The connector does not start with
  `ack_level = "durable"` alone, because the client would refuse every wait.
  The server also needs replication configured. Without it no chunk is ever
  acknowledged.

### Choosing an ack level

`ok` returns when the server accepts a chunk. `durable` also waits until the
write-ahead log is shipped. With `ok`, a primary that fails before shipping can
lose rows it already acknowledged. Use `durable` when losing that tail matters.

`durable` adds one wait for log shipping to every chunk, whatever its size,
so throughput grows with `batch_size`.

`local_durable` waits until the server flushed the rows to its own disk, so
they survive a power loss on the QuestDB host but not the loss of its disk. It
works with QuestDB open source. See [Server durability](#server-durability).
It does not wait for log shipping, so on a cluster a primary that fails before
shipping can lose rows it acknowledged, as with `ok`. Only `durable` covers a
failover.

### Server durability

An `ok` acknowledgement means that QuestDB committed the rows, but by default
QuestDB does not flush a commit to disk. A QuestDB process crash keeps the rows.
An OS crash or a power loss on the QuestDB host can lose them.

QuestDB 10.2.0 and QuestDB Enterprise 4.2.1 add the `adaptive` commit mode,
which closes this gap for WAL tables. Set `cairo.commit.mode=adaptive` in the
server configuration. On Enterprise, set it on every node that can become
primary. Each commit then flushes its write-ahead log files to disk, so a row
the sink got an `ok` for survives a power loss. Commits can share one flush
for up to `cairo.adaptive.commit.group.window` (50 ms by default), and a power
loss can drop commits acknowledged since the last flush. Set the window to `0`
to flush every commit before QuestDB acknowledges it. QuestDB 10.0 and 10.1 and
QuestDB Enterprise 4.0 and 4.1 do not have this mode. See the
[durability documentation](https://questdb.com/docs/concepts/durability/).

The commit mode is a server setting. To make each chunk of the sink wait until
the server flushed its rows to disk, add `request_durable_ack=local` to the
connect string. `ack_level` then defaults to `local_durable`. A row whose
chunk QuestDB acknowledged then survives a power loss, also within the group
window. For a chunk whose acknowledgement does not arrive within
`flush_timeout`, see [Delivery semantics](#delivery-semantics). A server
without the `adaptive` mode
refuses the connection with
`server did not enable durable ACK [requested=local, ...]`, and the connector
does not start.

```toml
connection_string = "ws::addr=localhost:9000;request_durable_ack=local;"
```

## Types

QuestDB creates the table and its columns from the first rows it receives.
Create the table yourself if you need partitioning, `TTL`, `DEDUP` keys,
symbol capacities or a specific designated timestamp name.

| JSON | QuestDB |
| ---- | ------- |
| string | `VARCHAR`, or `SYMBOL` / `UUID` when listed in `symbol_columns` / `uuid_columns` |
| number | `DOUBLE`, or `LONG` when listed in `integer_columns` |
| boolean | `BOOLEAN` |
| array of numbers | `DOUBLE[]`, up to 3 dimensions, when every row of a dimension has the same length |
| any other array | not supported, the record is rejected |
| object | `VARCHAR` holding the JSON text |
| `null`, empty array, also nested as in `[[]]` | column omitted |

Integer arrays are stored as `DOUBLE[]`, because QuestDB has no other array
type. An array that holds anything but numbers, for example strings or a
`null`, one with rows of unequal length, and one nested deeper than 3 levels
have no QuestDB type, so the record is a [rejected record](#rejected-records).
`BOOLEAN` has no null, so an omitted boolean reads back as `false`.

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
rejected. QuestDB stores -9223372036854775808 (`i64::MIN`) in a `LONG` column as
NULL, so a record with that value in a `LONG` column is rejected too. When the
connector rounds a number past 2^53 to a `DOUBLE`, it logs one warning per kind
of value with the column name.

With `numbers_as_double = false`, each value takes its type from its JSON shape:
integers become `LONG` and fractions `DOUBLE`. Two problems follow:

- The client fixes a column's type from the first row of each chunk. Later rows
  in that chunk with the other number shape are rejected.
- If the first rows of an auto-created column are all whole numbers, QuestDB
  creates it as `LONG` for good.

Turn it off only if every numeric field is a true integer and you create the
table with explicit column types.

### Timestamps

An auto-created table names its designated timestamp `timestamp`. A payload
field with the same name collides with it, and the server rejects the whole
flush. Create the table first with `timestamp(<name>)` for another column, or
remove the field with the `delete_fields` transform. The field named by
`timestamp_field` is used only as the designated timestamp.

`message` and `origin` timestamps are in microseconds, so an auto-created table
gets a `TIMESTAMP` column. `payload` timestamps are converted to nanoseconds and
give a `TIMESTAMP_NS` column. With `server`, QuestDB stamps each row on arrival.
Use one source per table.

`timestamp_unit = "auto"` reads the size of the payload value, without its
sign: below 1e11 it is seconds, below 1e14 milliseconds, below 1e17
microseconds, and nanoseconds from there. Each bound is 1973-03-03 in the finer
unit, so `auto` reads a millisecond, microsecond or nanosecond value before
that date as a coarser unit, and the row gets a wrong time or is rejected. Set
the unit if the data can hold such values.

### Headers

With `include_headers = true`, each header is written to a `VARCHAR` column
named `header_<name>`. `<name>` is the `HeaderName` of the key:

- a string key as it is
- a string key that starts with `~` as `~string:<key>`
- a string key that is not UTF-8 as `~string_b64:<base64url>`
- any other kind as `~<kind>:<value>`

Characters that QuestDB refuses in a column name, such as `.`, `-`, `~` and
`:`, become `_`:

| Key | Column |
| --- | ------ |
| string `content-type` | `header_content_type` |
| string `mqtt.protocol` | `header_mqtt_protocol` |
| string `1` | `header_1` |
| integer `1` | `header__int32_1` |
| string `~note` | `header__string__note` |
| string bytes `ff fe` | `header__string_b64___4` |

Raw values, and string values that are not UTF-8, are written as standard
base64. For each such value, the sink also writes the `SYMBOL` column
`iggy_header_<name>_encoding` with the value `base64`. A text value leaves
that column NULL, so a table with only text headers has no encoding columns:

```sql
SELECT header_bin FROM iggy_events WHERE iggy_header_bin_encoding = 'base64';
```

The encoding column starts with `iggy_`, as the metadata columns do, so no
header key can write to it. A header key `bin_encoding` next to `bin` gets its
own column, `header_bin_encoding`.

A name longer than 127 bytes rejects the record, and the encoding column counts
toward that limit.

Different keys can give the same column name, ignoring case. Examples are
`a.b` and `a-b`, two raw keys whose base64url differs only in case or in
`-` and `_`, and keys of different kinds, such as the string key `_int32_1`
and the integer key `1`, which both give `header__int32_1`. Two such keys in one record reject the record. In different
records, the keys write to the same column, and nothing records which key
each value came from.

## Delivery semantics

Two boundaries decide delivery.

Between Iggy and the sink, the runtime's `[delivery]` table decides. Use at
least once for data that must not be lost:

```toml
[delivery]
guarantee = "at_least_once"
```

Without the table, the runtime delivers at most once: it commits the offset of
each poll before `consume()` runs and does not replay a batch. At most once is
best effort. The sink makes one attempt for each message. A chunk that fails
ends its batch, and the messages that were not written are lost. The sink logs
their offsets, so that you can rewind to them as
[Recovery after a host failure](#recovery-after-a-host-failure) describes.

At least once, the runtime stores an offset only after `consume()` succeeds,
and it delivers a failed batch again after a backoff. The sink reports success
only when QuestDB acknowledged every chunk of the batch at `ack_level`:

- A chunk whose acknowledgement does not arrive within `flush_timeout` fails
  the call with a transient error, and the runtime retries the batch. The
  frames of the failed attempt stay queued, and QuestDB still writes them when
  it catches up. So each retry during an acknowledgement stall queues one more
  copy of the batch. `retry_max_attempts` in `[delivery]` has no limit by
  default. Set it to bound the copies: when it runs out, the runtime stops the
  delivery of the topic, and a restart delivers the batch again. Declare the
  DEDUP keys below to keep one copy of each row.
- A record that the sink rejects fails the call with a permanent error. The
  runtime stops the delivery of the whole topic at the record instead of
  skipping it. It first stores the offsets that the other partitions of the
  topic proved, and the other topics of the connector keep delivering. See
  [Rejected records](#rejected-records) for the recovery.
- Fire and forget, `flush_timeout = "0s"`, never waits for an acknowledgement,
  so a connector that sets it does not start with at least once.

At least once is also how the official QuestDB clients work. On a disconnect or
a failover they send again the rows that the server did not acknowledge yet,
and the server removes the duplicates.

A retried batch writes the chunks that QuestDB already took a second time. You
can use the sink without `DEDUP`, and the table then holds those rows twice,
which may or may not be a problem for your data. If duplicates do not matter
to you, leave `DEDUP` out: ingestion is faster and lighter for the server
without it. To keep one copy, declare `DEDUP` keys on the table. QuestDB
removes duplicates by the keys of the table definition, so the keys are yours
to choose: the designated timestamp, plus the columns that identify a row.

QuestDB has two modes, and they differ in which row stays:

- `DEDUP SKIP KEYS(...)` keeps the first row and discards a later one with the
  same keys. Use it to remove retried copies: the copy is the same row, so
  QuestDB has nothing to write. It needs QuestDB 10.2.0 or QuestDB Enterprise
  4.2.1. An older server treats it as `UPSERT`, without an error.
- `DEDUP UPSERT KEYS(...)` replaces the stored row with the later one. Use it
  when a later message with the same keys must overwrite the earlier values.

Use the columns of your payload that identify a row, for example an order id:

```sql
CREATE TABLE orders (
  order_id SYMBOL, amount DOUBLE, timestamp TIMESTAMP
) TIMESTAMP(timestamp) PARTITION BY DAY WAL
DEDUP SKIP KEYS(timestamp, order_id);
```

Only if the payload has no such columns, fall back to the message identity:

1. Set `include_partition_column = true` and `include_offset_column = true`.
   Both are off by default.
2. Create the table with
   `DEDUP SKIP KEYS(timestamp, iggy_stream, iggy_topic, iggy_partition_id, iggy_offset)`,
   with the name of your designated timestamp in place of `timestamp`.

For this fallback, the timestamp, the stream and the topic are not enough. Iggy
gives every message of one producer batch the same timestamp, so keys without
the partition and the offset keep one row for several distinct messages,
without an error.

DEDUP keys must include the designated timestamp, so a retried row is a
duplicate only if it gets the same timestamp again. With
`timestamp_source = "server"`, QuestDB stamps each copy on arrival, and the
connector logs a warning at start. The same holds for a message whose
timestamp is zero, which Iggy uses for "not set": the origin timestamp with
`origin`, and the message timestamp with `message`. The connector then uses its
own clock. Use `message` or `payload` for a table that must not have
duplicates.

Each batch waits for its own acknowledgement. `flush_interval` and
`flush_max_messages` in `[delivery]` delay only when the runtime stores the
offsets, so they do not spread that wait over more messages. Raise
`batch_length` of the stream and `batch_size` in `[plugin_config]` to amortize
it.

Between the sink and QuestDB, delivery is at least once. A reconnect, a failover
or a replay of queued frames can send the same rows again. Declare
[`DEDUP` keys](https://questdb.com/docs/concepts/deduplication/) on
tables that must not have duplicates.

The client queues unacknowledged frames in memory, or on disk when `sf_dir` is
set. Rows can be lost in the cases below. The cases marked "at most once" do
not apply with at least once, because `consume()` then succeeds only after
QuestDB acknowledged the rows, and the runtime delivers a failed batch again.

- With `ack_level = "ok"` or `"local_durable"` on a cluster, a primary that
  fails before it ships its write-ahead log can lose rows it accepted. See
  [Choosing an ack level](#choosing-an-ack-level).
- At most once: without `sf_dir`, a crash, or a shutdown that outlasts
  `close_flush_timeout_millis` in the connect string, loses the queue even
  though `consume()` already reported success.
- At most once: with `sf_dir`, a host failure loses the frames that the client
  did not fsync. See [Store-and-forward](#store-and-forward).
- At most once: a crash of the runtime after a poll and before the sink
  flushes loses that batch, also with `sf_dir`, because the runtime already
  committed its offset.
- At most once: a chunk that is not acknowledged yet, or any chunk with fire
  and forget,
  stays queued while `consume()` reports success. If the connection later
  fails for good, for example on a terminal rejection of an earlier frame or an
  authentication error, the client drops the frames queued in memory. The sink
  logs the offsets of each unacknowledged chunk, at `warn` for a pending ack
  and at `debug` for fire and forget, so that you can rewind to them.
- With `ack_level = "ok"`, an OS crash or a power loss on the QuestDB host can
  lose acknowledged rows. See [Server durability](#server-durability).
- With `sf_dir`, a frame that QuestDB rejects terminally makes every later
  batch fail until you delete its slot directory. The sink logs the offsets of
  each of those batches. See
  [Frames that QuestDB rejects](#frames-that-questdb-rejects).
- At most once: rows the sink does not write are lost, as the rest of this
  section describes. These are a rejected record, the unconfirmed rows of a
  failed chunk, and the chunks behind a failed chunk.

A batch fails when:

- QuestDB rejects a frame terminally. The client reports some rejections after
  the flush that caused them returned, without saying which topic sent the
  frame. Such a rejection fails the next batch, of any topic on this connector,
  that has no error of its own. The client can also report a terminal
  rejection of a frame that QuestDB already acknowledged, without ending the
  connection. That report fails one later batch, although no rows were lost.
  At least once, these failures are transient, so the runtime retries the
  batch instead of stopping.
- A chunk fails to write. The connector does not attempt the remaining chunks
  of the batch. It logs the offsets and message ids of the messages that
  QuestDB did not confirm, and of the messages that it did not attempt. At
  least once, the runtime delivers the whole batch again.
- 10 batches in a row on one topic each leave a chunk unacknowledged within
  `flush_timeout`. A single late ack is only a warning, because the frames stay
  queued and are delivered later.
- The client's inbox of rejection reports overflows and drops events. A terminal
  rejection may then be unreported, so the next batch fails although its own
  rows were written.

The runtime counts a failed batch in `iggy_connector_errors_total`. At most
once, it does not retry it. At least once, it retries a transient failure after
a backoff and stops the delivery at a permanent one.

At most once, the first chunk of a batch whose acknowledgement does not arrive
within `flush_timeout` ends the waits of that batch: its later chunks are
published without a wait and logged as unacknowledged. Otherwise a stalled
server would hold the batch for `flush_timeout` once per chunk.

`flush_timeout` also bounds shutdown. The runtime does not interrupt a chunk in
progress. A batch in flight waits for each of its remaining chunks until
`flush_timeout` passes without progress, so shutdown can take several times
`flush_timeout`, and longer while acknowledgements still arrive.

### Rejected records

At most once, a rejected record is lost, and its log line is the only record
of it. The runtime has already committed its offset, and there is no
dead-letter queue. At least once, the record fails the call with a permanent
error, and the runtime stops the delivery of its topic there, so the record
and the records behind it stay in Iggy. The cases below then stop the
batch instead of letting the rest of it be written. `invalid_messages` in
`[delivery]` covers only the runtime's own decode and transform errors, so
`invalid_messages = "skip"` does not skip a record that the sink rejects.

At least once, a restart alone does not get past a rejected record. The
runtime stored no offset for its batch, so it delivers the same record again,
and the record is rejected again. Before the call fails, the sink writes and
confirms every row in front of the record, so you can skip the record alone.
To recover:

1. Stop the connectors runtime.
2. Read the offset of the record in the `rejected message` log line.
3. If a change to the configuration or the table makes the record valid, make
   that change. Otherwise, set the stored offset of the partition to the offset
   of the record, as
   [Recovery after a host failure](#recovery-after-a-host-failure) describes.
   Iggy resumes after the stored offset, so this skips the record alone.
4. Start the connectors runtime.

The sink has no option to skip rejected records by itself at least once. A skip
loses the record, so it stays a decision that you make by offset.

Each rejection is logged with its stream, topic, partition, offset and message
id. The first 20 per batch are logged at `error` and the rest at `debug`.

At most once, rejections are not counted in the runtime metrics. A batch that
writes some rows and rejects others counts as a success, so
`iggy_connector_errors_total` stays at zero. The plugin cannot report per-row
counts through the current SDK. Alert on the connector's logs instead. At least
once, the first rejection fails the batch, and the runtime counts it.

A record is rejected in the cases below. At most once, the first three lose
only the record, and the rest of the batch is written.

- **The connector rejects it before writing**: a binary payload that is not
  UTF-8, a malformed UUID, a value of the wrong type in a listed column, a
  symbol value longer than 1 MiB, an array that is not a rectangular array of
  numbers, a missing, non-numeric or out-of-range timestamp field, two fields
  that match `timestamp_field`, an invalid or duplicate column name, or a row
  with no columns.
- **The client rejects it while writing**, usually because a column already has
  another type in this chunk. At least once, the runtime delivers the batch
  again from its first message after a restart, so the same conflict usually
  returns. Keep `numbers_as_double = true`, the default, or list the column in
  `integer_columns`, so that every value of a numeric column has one type.
- **The message alone is larger than a frame.** The client refuses the frame
  that holds only this message, see [Batch sizing](#batch-sizing).
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
margin, and let the DEDUP keys drop the rows that are written twice.

1. Stop the connectors runtime.
2. For each partition, find the first offset to write again, before the failure.
3. Set the stored offset to one less than that offset. Iggy resumes after the
   stored offset.

   ```sh
   iggy consumer-offset set questdb_sink events questdb_events <partition> <offset> --kind consumer-group
   ```

   If that offset is 0, no stored offset comes before it. Set a new
   `consumer_group` for the stream instead. A group with no stored offset
   starts every partition at offset 0.

4. Start the connectors runtime.

The sink writes some rows a second time. To keep one copy of each row,
declare the DEDUP keys that [Delivery semantics](#delivery-semantics) names.
This does not work with
`timestamp_source = "server"`, because the second copy gets a new timestamp.

### Reserved column names

The connector writes these columns itself. A record with a payload field of the
same name, ignoring case, is rejected. For the two header columns, this holds
only for a record that also carries that header. In a record without the
header, the payload field is written to the column.

| Column | Written when |
| ------ | ------------ |
| `iggy_stream` | `include_stream_column = true`, the default |
| `iggy_topic` | `include_topic_column = true`, the default |
| `iggy_partition_id` | `include_partition_column = true` |
| `iggy_offset` | `include_offset_column = true` |
| `header_<name>` | `include_headers = true`, for a record that carries the header |
| `iggy_header_<name>_encoding` | `include_headers = true`, for a record that carries the header with a base64 value |

The `iggy_` prefix keeps these names apart from common payload fields such as
`stream`, `topic` or `offset`, which are written as ordinary columns. A payload
field named like a reserved column is rare. If you have one, turn the flag off,
or remove the field with the `delete_fields` transform.

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

The tests share one QuestDB 10.2.0 container, `iggy-test-questdb-10.2.0`, which
runs `cairo.commit.mode=adaptive`, and each test writes to its own table. The
container stays up after the run so the next run can reuse it. Remove it with
`docker rm -f iggy-test-questdb-10.2.0`. The outage test pauses QuestDB, so it
runs on a container of its own.

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
