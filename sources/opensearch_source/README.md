# OpenSearch Source Connector

Polls documents from an OpenSearch index and publishes each one to an Iggy
topic as a JSON message. The connector is read-only: it never writes to
OpenSearch.

Progress is tracked by a watermark: the timestamp of the newest published
document, saved with that document's sort tuple
`(timestamp_field, tiebreaker_field, _index)`. The runtime stores it, so a
restart resumes after the last acknowledged batch. See
[How polling works](#how-polling-works) for documents that appear late.

## Configuration

Build the plugin from the repository root:

```bash
cargo build --release -p iggy_connector_opensearch_source
```

Then point the runtime at a config like this one (a copy lives in
`runtime/example_config/connectors/opensearch_source.toml`):

```toml
type = "source"
key = "opensearch"
enabled = true
version = 0
name = "OpenSearch source"
path = "target/release/libiggy_connector_opensearch_source"
plugin_config_format = "json"

[[streams]]
stream = "opensearch_stream"
topic = "documents"
schema = "json"
batch_length = 100
linger_time = "5ms"

[plugin_config]
url = "http://localhost:9200"
index = "logs-*"
polling_interval = "10s"
batch_size = 100
timestamp_field = "@timestamp"
query = { match_all = {} }
```

| Field | Default | Behavior |
| --- | --- | --- |
| `url` | required | OpenSearch base URL. |
| `index` | required | Index name or pattern, not empty. `open()` fails for a name that does not exist. A pattern that matches nothing yet is accepted, and the connector waits for matching indices to appear. |
| `timestamp_field` | required | Field used for sort order. It must be sortable in the index mapping. |
| `username` / `password` | none | HTTP basic authentication. Set both or neither: `open()` fails with `InvalidConfigValue` when only one is set. |
| `query` | `match_all` | Query DSL object, applied on every poll. |
| `polling_interval` | `10s` | Wait before each poll, so the first poll also waits. |
| `tiebreaker_field` | `_id` | Unique `keyword` field with doc values that orders documents with the same timestamp. When set to a field other than `_id`, it also identifies documents in message ids and in the state. The `opensearch.document_id` header always carries `_id`. |
| `batch_size` | `100` | Documents per search, `1` to `10000` (the OpenSearch default `max_result_window`). |
| `request_timeout` | `30s` | Limit per HTTP request, from connecting to the last byte of the response. A timeout counts as a failed attempt and is retried. |
| `max_retries` | `3` | Total HTTP attempts per search, not the number of retries. |
| `retry_delay` | `1s` | Base delay for exponential backoff, in `open()` as well as in polls. |
| `retry_max_delay` | `30s` | Backoff cap. |
| `max_open_retries` | `5` | Total attempts for each check in `open()`: the index check and, when `tiebreaker_field` is set to a field other than `_id`, the mapping check. |
| `open_retry_max_delay` | `30s` | Backoff cap during `open()`. |
| `circuit_breaker_threshold` | `5` | Consecutive failed polls before the breaker opens. |
| `circuit_breaker_cool_down` | `60s` | How long polls are skipped while the breaker is open. |
| `verbose_logging` | `false` | Log batch counts at `info` instead of `debug`. |

## How polling works

Each poll sleeps for `polling_interval`, then sends `POST /{index}/_search`
sorted ascending by `timestamp_field`, `tiebreaker_field` and `_index`. The last
two keys order documents with the same timestamp, so none is skipped at a batch
boundary, including across the indices of a pattern.

A scan is a run of polls that ends at the first page shorter than `batch_size`.
Within a scan, each poll continues with `search_after` set to the last sort tuple
of the previous page. A new scan restarts at the watermark and skips the hits
already published at that timestamp. A document that becomes searchable after
the scan passed it (a shard that refreshed late, a bulk request still being
indexed) is therefore found if it has the same timestamp as the newest published
document, even when its tie-breaker sorts before the ones already read. See
[Limits](#limits) for the case this misses.

`_id` has no doc values, so OpenSearch sorts on it through heap-backed
fielddata, and a cluster with `indices.id_field_data.enabled=false` rejects the
sort. Set `tiebreaker_field` to a unique `keyword` field with doc values (for
example an event id) to avoid that. `open()` reads its mapping and fails with
`InvalidConfigValue` if the field is missing from a matched index, is not a
`keyword`, or has `doc_values` disabled. It cannot check that the values are
unique, and documents that share a value can still be skipped.

A pattern can match indices created after `open()`, which checks only the ones
that exist then. Map the tie-breaker as a `keyword` with doc values in every
future index too. A later index without the mapping makes the search report a
failed shard, and every poll fails until the mapping is fixed.

`_id` is only unique within a shard. An index with custom routing can hold two
documents with the same `_id` on different shards. They tie on every sort key and
get the same message id, so set `tiebreaker_field` to a field that is unique
across the index. Changing it later makes the first scan treat the documents at
the watermark as new, and they are published again.

Documents without `timestamp_field` sort first (`missing: "_first"`). Left to
OpenSearch they would sort last, under a value larger than any timestamp, and a
watermark stored there would hide every document indexed later. They are
published on the first scan, however many there are. They have no timestamp to
rescan, so the connector keeps no keys for them and resumes right after the last
one published. One indexed later is skipped, like any document older than the
watermark. To leave them out, add an `exists` filter on `timestamp_field` to
`query`. If a hit ever carries `i64::MAX` as its sort value, which is how a
missing field sorts when missing values come last, the poll fails with
`SchemaMismatch` instead of storing it.

For every hit:

- The `_source` becomes the JSON payload.
- The message `id` is a 128-bit BLAKE3 hash of `_index` and the tie-breaker value
  (`_id` unless `tiebreaker_field` names another field). It names the document,
  not a version of it: an updated document that is fetched again keeps the same
  id, so a consumer that drops repeated ids also drops the new body. Read the id
  from the message instead of recomputing it.
- The headers `opensearch.document_id` and `opensearch.index` carry the raw
  values. A header over 255 bytes is dropped with a warning.
- `origin_timestamp` is left unset, so Iggy records the ingestion time. The
  document timestamp only feeds `last_poll_timestamp` in the state, and only when
  `timestamp_field` (a dotted path is followed through nested objects) holds an
  RFC 3339 string or an integer (a magnitude above `1e12` means milliseconds,
  otherwise seconds).

Two cases publish nothing. A hit without `_source` (for example from an index
mapped with `"_source": false`) is skipped with a warning. It is recorded at the
watermark like a published hit, so later scans do not warn again, and it is not
added to the document counter. A hit without a sort tuple or tie-breaker value
fails the poll with `SchemaMismatch`, because the results could not be paginated
or told apart.

## Delivery and state

Delivery is at-least-once. `poll()` returns the batch with a candidate state. The
runtime sends the batch, saves the state and reports the result: the connector
keeps the candidate on `Ack` and drops it on `Nack`, so a rejected batch is
fetched again. The SDK stops the source after five consecutive `Nack`s and
reports `Error`; restart the connector to resume. If the batch reached Iggy but
the state save failed, the batch is replayed, so consumers should dedupe on the
message `id`.

A page where every new hit lacks `_source` publishes nothing, but the watermark
still moves, so it goes through `Ack` and `Nack` and is saved like any other. A
poll that finds nothing new (no hits, or only hits published before) leaves the
watermark alone and updates only the counters.

The state holds the sort tuple of the newest published hit (its first element is
the watermark), the `(index, tie-breaker value)` of every published hit at that
watermark, `last_poll_timestamp` (informational), the document, poll and error
counters, and a fingerprint of `index`, `timestamp_field` and `query`. The hit
list is capped at 10,000: if more documents share the newest timestamp, the poll
fails with `SchemaMismatch` and the watermark stays put, so use a finer
`timestamp_field`. After a restart the first scan starts at the watermark and
skips the hits already published there.

A corrupt saved state makes `open()` fail with `InitError` instead of restarting
from the first document. So does a state that holds a watermark and was saved
under another `index`, `timestamp_field` or `query`, since that watermark would
skip documents. A state saved without a fingerprint is accepted. To start over,
delete the saved state: with the default file storage, the `source_<key>.state`
file in the runtime `[state] path` directory.

With no saved state the connector starts at the beginning of the index and
republishes everything. If you lose the runtime state, keep the `[state] path`
directory on durable storage (or use the `http` state storage), dedupe on the
message `id` downstream, or bound the rescan with a time-window `query`.

## Failures and retries

Network errors, HTTP `429` and `5xx` are retried with exponential backoff and
jitter. Other `4xx` responses and unparsable search responses fail at once. A
`Retry-After` header is ignored: the delay is always computed locally.

Searches are sent with `allow_partial_search_results=false`, and a response that
reports `timed_out` or failed shards is rejected before the state changes. A
partial page could omit documents that sort before its last hit, and the
watermark would pass them for good.

When the retry budget is spent, the poll fails. Network errors, `429`/`5xx`
failures and responses that report a timeout or failed shards count toward the
circuit breaker. Permanent errors do not, because pausing polls cannot fix a bad
query or mapping, and the error should stay visible. While the breaker is open,
`poll()` logs a warning, sleeps `polling_interval` and returns an empty batch
without touching the state. After the cool-down polls resume, and the breaker
reopens once the threshold is reached again. A successful search resets the
count.

| Error | Raised when |
| --- | --- |
| `InitError` | In `open()`: a corrupt saved state or one saved for another `index`, `timestamp_field` or `query`, a failed index check (missing index, other status, unreachable cluster), an unreadable `tiebreaker_field` mapping, or a client that cannot be built. |
| `InvalidConfigValue` | Invalid `url`, an empty `index`, missing `timestamp_field`, only one of `username` and `password`, `batch_size` outside `1..=10000`, a `query` that is not an object, an empty `tiebreaker_field`, or a `tiebreaker_field` that is not a keyword with doc values in every matched index. |
| `Connection` | Network failure during a search, or a poll before `open()`. |
| `HttpRequestFailed` | `429` or `5xx` after the retries are used up, or a search that timed out or failed on some shards. |
| `PermanentHttpError` | Other `4xx`, or a search response that cannot be parsed. |
| `SchemaMismatch` | A hit has no sort tuple or tie-breaker value, sorts on `i64::MAX`, or more than 10,000 documents share the newest timestamp. |

## Limits

- One sequential reader with one watermark. There is no Scroll, point-in-time or
  sliced parallel export.
- Throughput is `batch_size` per `polling_interval`. The defaults give about 10
  documents per second, so a large first load needs tuning, for example
  `polling_interval = "100ms"` and `batch_size = 5000`.
- Use one mapping for `timestamp_field` across all indices of a pattern. A `date`
  index and a `date_nanos` index sort as raw numbers (milliseconds against
  nanoseconds), so the order across them is wrong and documents can be skipped.
  The sort does not set `numeric_type`, because OpenSearch rejects it on a
  `keyword` timestamp.
- The watermark only moves forward. A document that becomes searchable later with
  a `timestamp_field` older than the watermark is not read until the state is
  reset, and a rescan covers only the newest published timestamp.
- A document with a timestamp in the future moves the watermark there when it is
  published, which hides every document indexed before the clock reaches that
  time. If your data can contain future timestamps, add a `range` filter with
  `lte: "now"` on `timestamp_field` to `query` (`date` and `date_nanos` fields
  only). The example configuration shows one.
- The rescan happens when a scan starts. While a scan spans several full pages,
  as in a large backlog, it resumes after the last document read. A document that
  becomes searchable in that window at the newest published timestamp, with a
  tie-breaker below the last one read, is skipped once later pages move the
  watermark on. Late documents are found only when the connector is caught up.
- Each scan reads the hits at the newest published timestamp again, so a
  timestamp shared by many documents costs extra reads on every scan.
- Only the whole `_source` is published, as JSON. There is no field projection.
- The client talks to the single `url` and does not discover other nodes.

## Troubleshooting

| Symptom | Check |
| --- | --- |
| Runtime log says `Plugin initialization failed` | The connector logs the cause just before it, as `Failed to open OpenSearch source connector ID: <id>: <error>`. |
| `open()` fails on a missing index | Index name, URL and credentials. |
| `open()` fails with `state restore failed` | Repair or delete the saved state. A state saved for another `index`, `timestamp_field` or `query` also fails this way. |
| Nothing new after a restart | The `timestamp_field` mapping must match the indexed documents. |
| Duplicate messages | Expected after a `Nack` or a failed state save. Dedupe on the message `id`. |
| Repeated warnings about `_source` | The index disables `_source`, so those hits are skipped. |
| Backfilled documents missing | Their timestamps are older than the watermark. Reset the state or adjust `query`. |
