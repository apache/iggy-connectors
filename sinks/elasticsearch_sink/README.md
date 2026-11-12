# Elasticsearch Sink Connector

A sink connector that consumes messages from Iggy streams and indexes them to Elasticsearch.

## Configuration

- `url`: Elasticsearch cluster URL
- `index`: Target index name
- `username/password`: Optional authentication credentials
- `batch_size`: Accepted but unused; the stream-level `batch_length` determines each incoming batch
- `timeout_seconds`: Client-wide HTTP timeout for `open()` and bulk `consume()` (default: 30s). Values of `0` are clamped to 1s. Raise for slow bulk workloads; a timed-out bulk fails the batch after the poll offset is already committed
- `max_retries`: Total bulk attempts for explicitly rejected items, including the first (default: 3). Values of `0` and `1` disable retries
- `retry_delay`: Initial backoff delay in humantime format (default: `"1s"`)
- `retry_max_delay`: Maximum backoff delay in humantime format (default: `"5s"`)
- `create_index_if_not_exists`: Automatically create index (default: true)
- `index_mapping`: Index mapping configuration
- `include_headers`: Write user headers to `_iggy_headers` (default: true)

Retry delays allow zero; invalid duration strings log a warning and fall back to `1s`. The HTTP timeout applies per request, so a consume call can span multiple requests and backoff delays; there is no separate total time budget.

## Features

- Bulk indexing optimization
- Automatic index creation
- Items rejected with HTTP 429 or 5xx receive up to `max_retries` attempts with exponential backoff and jitter. Only rejected items are retried; accepted documents are not resubmitted. Request failures or malformed responses fail the batch without replaying an ambiguous write. Any remaining rejection fails the callback, including partial failures, while successful items remain indexed
- Metadata field injection
- Support for multiple data formats

## User headers

With `include_headers = true`, each document gets an `_iggy_headers` object with one field per user header:

Upgrade note: earlier versions wrote no user headers, and `include_headers` defaults to `true`. After an upgrade, documents with headers carry `_iggy_headers` for the first time. For an index that already exists, add the templates below by hand. For an index with `"dynamic": "strict"`, set `include_headers = false` or declare `_iggy_headers`, as the limits below explain.

```json
"_iggy_headers": {
  "trace-id": {"data": "abc", "data_encoding": "utf8"},
  "~int32:1": {"data": "7", "data_encoding": "utf8"}
}
```

- A string key keeps its own name. A string key that starts with `~` becomes `~string:<key>`, and a string key that is not valid UTF-8 becomes `~string_b64:<base64url>`.
- A key of any other kind becomes `~<kind>:<value>`, for example `~int32:1`, `~bool:true`, `~float64:3ff8000000000000` (hex of the float bits) or `~raw:AQI` (unpadded base64url).
- `data` holds a string value as text, a raw value or a string value that is not valid UTF-8 as standard base64 (`data_encoding: "base64"`), and every other value as decimal or `true` / `false` text.
- A message without headers drops any `_iggy_headers` field that its payload carries, so that a payload value cannot fix the field type.

When the sink creates the index, it adds the dynamic templates `iggy_header_data` and `iggy_header_data_date`, which map `_iggy_headers.*.data` as `keyword` for string and date-like values. Without them, a date-like first value maps the field as a date, and later values of that header fail to index. If `index_mapping` is set, the templates go first in its `mappings.dynamic_templates`. If the index already exists, or `create_index_if_not_exists = false`, add both templates yourself:

```json
[
  {"iggy_header_data": {"path_match": "_iggy_headers.*.data", "match_mapping_type": "string", "mapping": {"type": "keyword"}}},
  {"iggy_header_data_date": {"path_match": "_iggy_headers.*.data", "match_mapping_type": "date", "mapping": {"type": "keyword"}}}
]
```

Limits:

- A header key that contains `.`, such as `mqtt.qos`, becomes an object path in the mapping. Elasticsearch rejects the whole document for a key that starts with `.`, contains `..`, or holds only dots or whitespace, such as `.a` or `a..b`. Two keys where one is a path inside the other, such as `x` and `x.data`, conflict, and the second one to arrive fails to index.
- Each distinct header name adds mapped fields, which count toward `index.mapping.total_fields.limit`.
- An index with `"dynamic": "strict"` rejects documents with headers unless its mapping declares `_iggy_headers`. Set `include_headers = false` for such an index.
