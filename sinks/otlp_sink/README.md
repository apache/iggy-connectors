# OTLP Sink

The OTLP sink exports logs from Iggy topics to any backend that speaks the OpenTelemetry Protocol: an OpenTelemetry Collector, Grafana Cloud, Honeycomb, Datadog, Elastic, Splunk and others. It sends OTLP over HTTP with protobuf bodies.

## What a message holds

Each message on the consumed topics holds one protobuf `ExportLogsServiceRequest`, with the stream `schema` set to `raw`. This is the `otlp_proto` encoding of the OpenTelemetry Collector's Kafka exporter and receiver, so a topic written by a collector can be exported unchanged.

The sink checks that each message decodes, then forwards it as the bytes it arrived in. Resources, scopes and attributes are not rewritten, and fields this sink does not know still reach the receiver.

A message is dropped and logged when it does not decode, when it holds no resource entry, or when one of its log records has a `trace_id` that is not 16 bytes or a `span_id` that is not 8 bytes. An absent ID is valid. The length is checked because a Collector rejects a whole request for one such ID, and a request packs many messages. The other messages of the batch are still exported.

## Configuration

| Key | Default | Description |
| --- | --- | --- |
| `endpoint` | Required | The base URL, `http(s)://host:4318`, to which `/v1/logs` is appended after any path it has. A query string is kept. An endpoint that already ends with `/v1/logs` is rejected. |
| `compression` | `none` | `none` or `gzip`. |
| `headers` | none | Sent with every export as HTTP headers. Values are treated as secrets and never logged. Names are case-insensitive, so two names that differ only in case are rejected. `content-type` and `content-encoding` are rejected too, because the sink sets both. Set them in the connector's TOML file or through the HTTP config provider: a map cannot come from an environment variable. |
| `timeout` | `10s` | Timeout of one attempt. Must be greater than zero. |
| `max_retries` | `8` | Total attempts per request, including the first. `0` and `1` both mean a single attempt. With the default delays, 8 attempts wait about 45 s in total between them. A higher value holds a batch longer instead of losing it, for retryable failures only. See [Delivery](#delivery). |
| `retry_delay` | `1s` | Backoff before the first retry. It doubles per retry, with ±20% jitter. |
| `max_retry_delay` | `10s` | Upper bound on one backoff. When `retry_delay` is larger, every backoff is `max_retry_delay`. |
| `max_request_bytes` | `4000000` | Upper bound on the encoded size of one request, below the 4 MiB limit most receivers use. Messages are packed whole into requests, so a single message larger than this is sent on its own. |
| `verbose_logging` | `false` | Log each batch at `info` with the running counters, instead of at `debug`. |

A key that is not in this table is rejected, so a misspelled key stops the connector at start instead of taking its default.

TLS is used when the endpoint scheme is `https`, verified against the system's root certificates. Client certificates (mTLS) are not supported.

```toml
[[streams]]
stream = "telemetry"
topics = ["logs"]
schema = "raw"
batch_length = 500
consumer_group = "otlp_sink"

[plugin_config]
endpoint = "https://otlp.example.com:4318"
compression = "gzip"

[plugin_config.headers]
authorization = "Bearer <YOUR_TOKEN_HERE>"
```

## Delivery

The sink delivers at most once: the connectors runtime commits a batch's offsets before handing it to the sink, so a batch that fails is not redelivered. The sink retries instead, inside `consume`, for the failures the OTLP/HTTP specification calls retryable: `429`, `502`, `503`, `504` and transport errors, such as a refused connection.

Every other status, such as `400`, `401` or `413`, is permanent and is not retried. Redirects are not followed, so a `3xx` answer is permanent too. A request is encoded once, so every retry sends the same bytes.

When the receiver accepts a request but reports a `partial_success` with rejected log records, those records are logged and counted, and are not retried, as the specification requires. A success response that cannot be read, that is larger than 64 KiB, or that is not a protobuf `ExportLogsServiceResponse` counts as a failed request. It is not retried either, because the receiver already has the request.

The sink does not support the runtime's [at-least-once delivery](../../runtime/README.md#sink-delivery-guarantees) yet, so a connector with `[delivery] guarantee = "at_least_once"` does not start.

Some limits of this approach:

- The sink uses its own backoff. It does not read the delay that a `Retry-After` header asks for.
- A retry can deliver records twice. That happens when the receiver stored a request but its answer did not arrive in time.
- While a request is retried, `consume` does not return. That is the only backpressure: the runtime polls the next batch only after `consume` returns. With the defaults, one request can take about 45 s of backoff plus up to `timeout` per attempt. A connector stop waits for it.
- A permanent failure has no such wait. A status such as `401` or `404` ends the request after one attempt, and the runtime goes on to read and commit the next batch. An expired token or a wrong endpoint path therefore drains the backlog: every batch is read, refused and lost until the config is fixed.
- The requests of one batch are sent one after another, each with its own retries, so a batch that needs several requests can wait that long once per request. Lower `batch_length` to bound how many requests a batch needs.
- A batch with a dropped message counts as one error, and as zero processed messages, in the runtime's metrics, although its other messages were exported. When an export of the batch also failed, the error the runtime logs is the export failure.

The sink does not contact the endpoint when it opens, so it starts even when the receiver is down.

The warning for dropped messages names the stream, the topic, the partition and the offset of the first dropped message. The error for a failed request names the stream, the topic, the partition and the offsets of its first and last message, so the messages can be found in Iggy.

## Counters

The sink keeps these counters, logs them when it closes and, with `verbose_logging`, after every batch:

| Counter | Meaning |
| --- | --- |
| `messages_sent` | Messages in requests the receiver accepted. |
| `messages_failed` | Messages in requests that failed after every retry. |
| `messages_dropped` | Messages that could not be decoded, that held no resource entry, or that held a `trace_id` or `span_id` of the wrong length. |
| `batches_sent` / `batches_failed` | Export requests that succeeded or failed. |
| `items_rejected` | Log records the receiver reported as rejected. |
| `retries` | Retried attempts. |
| `average_batch_latency_ms` | Average time of one request, retries included. |

They are not on the runtime's `/metrics` endpoint, which plugins cannot publish to yet. The runtime's own `iggy_connector_messages_processed_total` and `iggy_connector_errors_total` cover the sink like every other.
