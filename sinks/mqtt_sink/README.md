# Apache Iggy MQTT Sink Connector (`iggy_connector_mqtt_sink`)

The **MQTT Sink Connector** is a dynamically loaded shared library plugin
(`.so` / `.dylib` / `.dll`) for Apache Iggy. It reads Iggy messages and
publishes them to MQTT brokers such as **EMQX** or Mosquitto.

---

## Features

* **Multi-Protocol Support**: Supports **MQTT 3.1.1** (`mqtt311`) and **MQTT 5** (`mqtt5`).
* **Quality of Service (QoS)**: Publishes with QoS 0, QoS 1, and QoS 2.
* **Topic Routing**: Supports static topics, `{stream}` and `{topic}` templates, and dynamic MQTT topics from headers or JSON envelopes.
* **Bounded Pipelining**: Limits outstanding publications with `max_in_flight`.
* **Reconnect Recovery**: rumqttc retransmits after session resumption. The sink recreates its driver and requeues after a session reset.
* **Idle Connection Keep-Alive**: A managed MQTT task polls the event loop between batches, so it sends and receives keep-alive packets.
* **Malformed Record Reporting**: Logs and skips malformed marked envelopes while it publishes valid records in the batch.
* **MQTT 5 Properties**: Publishes supported envelope properties with the decoded payload.
* **TLS and Mutual TLS**: Supports system roots, custom CA files, client certificates, and client private keys through Rustls.
* **Session Control**: Supports `clean_start` and MQTT 5 `session_expiry_interval`.
* **Bounded Shutdown Cleanup**: Limits lock acquisition and MQTT disconnect cleanup with `shutdown_timeout` after publication work returns.

---

## Configuration

The connector TOML file has two parts. The runtime section identifies the
sink and selects the Iggy records that it reads. The `[plugin_config]` section
defines the MQTT connection and publication behavior.

The default sample uses static routing. It publishes every selected Iggy record
to one MQTT topic. Optional authentication, TLS, template routing, and dynamic
routing remain commented. Uncomment the fields that you need and replace the
active `topic_mode` value when you select another routing mode.

```toml
type = "sink"
key = "mqtt_telemetry"
enabled = true
version = 1
name = "MQTT Telemetry Sink"
path = "target/release/libiggy_connector_mqtt_sink"
plugin_config_format = "json"

[[streams]]
stream = "telemetry"
topics = ["events"]
schema = "raw"
batch_length = 100
max_wait = "1s"
min_count = 1
consumer_group = "mqtt_telemetry_sink"

[plugin_config]
broker_url = "mqtt://127.0.0.1:1883"
protocol = "mqtt5"
clean_start = true
session_expiry_interval = 3600

# Select one routing block. Keep one topic_mode line active.
# Static routing publishes every selected Iggy record to one MQTT topic.
topic_mode = "static"
topic = "iggy.messages"
qos = 1
retain = false
max_packet_size = 1048576

# Template routing creates an MQTT topic from the Iggy stream and topic.
# To select it, comment the active static topic and topic_mode lines above.
# topic_mode = "template"
# topic_template = "{stream}/{topic}"

# Dynamic routing reads mqtt.topic from Iggy headers or an MQTT 5 envelope.
# To select it, comment the active static topic and topic_mode lines above.
# topic_mode = "dynamic"
# fallback_topic = "{stream}/{topic}"

# Optional broker authentication.
# username = "emqx-user"
# password = "replace-me"

# Optional TLS configuration for mqtts:// brokers.
# [plugin_config.tls]
# ca_file = "/etc/iggy/certs/emqx-ca.pem"
# client_cert_file = "/etc/iggy/certs/client-cert.pem"
# client_key_file = "/etc/iggy/certs/client-key.pem"
```

`sinks/mqtt_sink/config.toml` uses this layout. The runtime
example uses the same plugin configuration with its own Iggy route.

## Runtime Configuration Reference

| Field | Description |
| :--- | :--- |
| `type` | Set this value to `"sink"`. |
| `key` | Names this connector instance. Each loaded connector needs a unique key. |
| `enabled` | Starts the connector when the runtime starts. |
| `version` | Selects the stored configuration version. |
| `name` | Provides a readable name for logs and the control API. |
| `path` | Gives the path to the MQTT sink shared library. |
| `plugin_config_format` | Set this value to `"json"`. The runtime sends the plugin section as JSON. |
| `streams[].stream` | Selects an Iggy stream to read. |
| `streams[].topics` | Selects one or more Iggy topics in that stream. |
| `streams[].schema` | Selects the decoder for the Iggy record payload. Use `"raw"` for unchanged bytes. Use `"json"` for MQTT 5 envelope routing. |
| `streams[].batch_length` | Limits the number of Iggy records in one runtime batch. |
| `streams[].max_wait` | Sets the longest wait for a batch to reach `min_count`. |
| `streams[].min_count` | Sets the minimum number of records in a batch before the runtime returns it. |
| `streams[].consumer_group` | Stores the Iggy read position for this sink route. Use a unique name for each sink instance. |

## MQTT Routing Modes

Static mode publishes every Iggy record from one sink route to the value of
`topic`. This is the default mode. `topic_template` and `fallback_topic` do
not select the output topic in this mode.

```toml
topic_mode = "static"
topic = "manual/static"
```

Template mode creates an MQTT topic from the Iggy stream and topic names. The
template accepts only `{stream}` and `{topic}`. The `topic` field does not
select the output topic in this mode.

```toml
topic_mode = "template"
topic_template = "iggy/{stream}/{topic}"
```

Dynamic mode reads `mqtt.topic` from Iggy message headers for every payload.
With MQTT 5, a topic in a versioned JSON envelope takes precedence.
If neither source provides a topic, or the topic is blank, `fallback_topic`
selects the output topic. The sink skips records with invalid MQTT topics.

```toml
topic_mode = "dynamic"
fallback_topic = "iggy/fallback"
```

Dynamic mode accepts this MQTT 5 envelope. The connector decodes
`payload_base64` before it publishes the MQTT payload.

In dynamic mode, the sink treats any JSON object with both top-level `version`
and `payload_base64` keys as an MQTT 5 envelope. These keys are envelope
markers. If a document contains both keys but does not follow the envelope
format, the sink treats it as a malformed envelope and skips the record.

MQTT 5 envelope routing has three required rules:

1. Set `topic_mode = "dynamic"`.
2. Set `protocol = "mqtt5"`.
3. Set `schema = "json"` for every sink stream that carries envelopes.

If an MQTT source produces the input, set `include_metadata = true` in that
source and use `schema = "json"` for its destination stream. With
`schema = "raw"`, the sink treats the envelope as ordinary bytes and publishes
the JSON envelope instead of its decoded MQTT payload.

Static and template routing treat the Iggy payload as opaque. They do not
forward Iggy headers to MQTT, including `mqtt.topic`.

For `message_expiry_interval`, the connector subtracts the time that the
record spent in Iggy. It skips the record when the remaining lifetime is zero.
The connector ignores `topic_alias` because an alias belongs to the MQTT
connection that created it.

```json
{
  "version": 1,
  "topic": "devices/site-a/telemetry",
  "payload_base64": "eyJ0ZW1wZXJhdHVyZSI6MjJ9",
  "properties": {
    "response_topic": "devices/site-a/replies",
    "content_type": "application/json",
    "user_properties": [{"key": "site", "value": "a"}]
  }
}
```

## Plugin Configuration Reference

| Field | Used when | Default | Description |
| :--- | :--- | :--- | :--- |
| `broker_url` | Always | None | Sets the MQTT broker URL. Use `mqtt://`, `mqtts://`, or `ssl://`. Do not add credentials or MQTT options to this URL. |
| `protocol` | All modes | `"mqtt5"` | Selects `"mqtt5"` or `"mqtt311"`. |
| `client_id` | All modes | Generated for clean sessions | Identifies the MQTT client. For persistent sessions, set a stable, non-empty value. For clean sessions, the connector generates a unique 23-character alphanumeric value when it is omitted. |
| `username` | Authenticated brokers | None | Sets the broker user name. Set `password` with this field. |
| `password` | Authenticated brokers | None | Sets the broker password. The connector does not write it to logs. |
| `clean_start` | All modes | `true` | Controls session reuse when the same driver reconnects. Every new driver resets the broker session. MQTT 5 starts a clean session on the first connection, even when this field is `false`. |
| `session_expiry_interval` | MQTT 5 | `3600` | Sets the MQTT 5 session expiry time in seconds. |
| `topic_mode` | All modes | `"static"` | Selects `"static"`, `"template"`, or `"dynamic"`. |
| `topic` | Static mode | `"iggy.messages"` | Sets the MQTT output topic for static routing. It must not begin with `$` or contain `+` or `#`. |
| `topic_template` | Template mode | `"{stream}/{topic}"` | Sets the template for template routing. It accepts only `{stream}` and `{topic}`. The rendered topic must not begin with `$` or contain `+` or `#`. |
| `fallback_topic` | Dynamic mode | `"{stream}/{topic}"` | Sets the MQTT output topic when the dynamic topic is absent or blank. The sink skips records with invalid MQTT topics. The rendered topic must not begin with `$` or contain `+` or `#`. |
| `qos` | All modes | `1` | Sets MQTT publication QoS. Use `0`, `1`, or `2`. |
| `retain` | All modes | `false` | Sets the MQTT retained-message flag. |
| `keep_alive` | All modes | `"30s"` | Sets the MQTT ping interval. MQTT 3.1.1 accepts `"0s"` or at least `"1s"`. MQTT 5 needs at least `"5s"`. The maximum is `"65535s"`. |
| `request_capacity` | All modes | `32` | Limits queued MQTT client requests. Set a value greater than zero. |
| `publish_timeout` | All modes | `"30s"` | Limits the wait for broker connection and publication acknowledgments. The maximum value is `"1h"`. |
| `shutdown_timeout` | All modes | `"30s"` | Limits how long `close()` waits for active `consume()` work and MQTT disconnect cleanup. It does not cancel active publication or reconnect work. |
| `max_retries` | All modes | `3` | Sets the number of retries after the initial connection attempt. Publish windows share one retry budget for the whole batch. The budget covers retries after uncertain publication attempts, so earlier windows can use retries that later windows need. |
| `retry_delay` | All modes | `"1s"` | Sets the first retry delay. |
| `max_retry_delay` | All modes | `"30s"` | Limits the retry delay. |
| `max_in_flight` | All modes | `10` | Limits publications that wait for acknowledgment. It must be greater than zero and must not exceed `request_capacity`. |
| `max_packet_size` | All modes | `1048576` | Limits an encoded MQTT PUBLISH packet in bytes. Oversized records are logged and skipped. MQTT 5 also uses a lower limit sent by the broker. |
| `verbose_logging` | All modes | `false` | Enables extra plugin log records. |
| `tls.ca_file` | TLS brokers | None | Sets a PEM file that contains custom CA certificates. The system trust store is used when it is omitted. |
| `tls.client_cert_file` | Mutual TLS | None | Sets the PEM client certificate for mutual TLS. Set `tls.client_key_file` with this field. |
| `tls.client_key_file` | Mutual TLS | None | Sets the PEM client key for mutual TLS. Set `tls.client_cert_file` with this field. |

Use `broker_url` only for the broker address. Set `client_id`, `keep_alive`,
`clean_start`, and `max_in_flight` as separate fields. The connector rejects
these URL options: `client_id`, `keep_alive_secs`, `clean_session`,
`clean_start`, `request_channel_capacity_num`, `inflight_num`,
`max_incoming_packet_size_bytes`, and `max_outgoing_packet_size_bytes`.

## TLS and Multiple Routes

The connector validates the broker certificate and host name. It cannot disable
certificate validation. Keep the complete `[plugin_config.tls]` table in the
connector TOML file because environment overrides cannot update nested fields.

Use one connector instance for each MQTT route. Give each instance a unique
connector key, client ID, consumer group, and output topic. The runtime loads
each connector TOML file from its configured `config_dir`.

---

## Publication and Acknowledgement Mechanism

For QoS 0, the sink confirms a publication after it queues the request. For
QoS 1, it waits for `PUBACK`. For QoS 2, it waits for `PUBCOMP`. If the result
is uncertain, the sink reconnects and retries. This can publish duplicates.
An MQTT 5 permanent broker rejection skips and counts only that record. The sink
continues with later records. Retries preserve order within one Iggy partition
and MQTT topic, but not across partitions or topics.

For `clean_start = false`, set a stable, unique `client_id` for this sink.
When a new MQTT 3.1.1 driver starts, the sink clears the old broker session,
including its subscriptions and queued messages, then creates a fresh
persistent session. For MQTT 5, the sink starts a clean session when a new
driver connects, even when `clean_start = false`. The same driver can resume
its session after a connection loss, but a new driver cannot resume the old
session.

Iggy offset durability depends on the connectors runtime. The current runtime
commits offsets when it polls messages, before MQTT publication completes.
The sink therefore provides at-most-once delivery from Iggy after a terminal
failure. If retries end or the broker returns a non-retryable rejection, Iggy
does not deliver the batch to this sink again. The MQTT broker delivery level
depends on `qos`. The runtime delivery guarantee remains at-most-once. The
runtime rejects `guarantee = "at_least_once"` in the `[delivery]` table for
this sink.

---

## Failure Modes

| Failure Scenario | Current Behavior | Result |
| :--- | :--- | :--- |
| Invalid configuration | Validation fails before connection | The connector reports an invalid configuration error. |
| MQTT 5 broker capability mismatch | Connection initialization rejects a configured QoS or retained publication mode that the broker does not support. | The connector does not publish records or retry the mismatch. |
| QoS 0 publication | The sink confirms after it queues the local request. | A connection failure before broker delivery can lose the record. |
| Malformed record envelope | The connector logs and skips the malformed record. | The skipped record is lost from this sink. Valid records in the batch are published. |
| Oversized MQTT record | The connector logs and skips a record that exceeds `max_packet_size` or the broker MQTT 5 limit. | The skipped record is lost from this sink. Valid records in the batch are published. |
| Expired MQTT 5 envelope | The connector logs and skips the expired record. | The skipped record is lost from this sink. Other valid records in the batch are published. |
| Invalid dynamic topic | The connector logs and skips the record when the resolved topic begins with `$` or contains `+` or `#`. | The skipped record is lost from this sink. Other valid records in the batch are published. |
| Raw MQTT 5 envelope | The sink treats the JSON envelope as ordinary bytes. | The broker receives the envelope JSON, not the original MQTT payload. |
| Transient connection failure | The event loop reconnects. rumqttc retransmits after session resumption. The sink recreates its driver and requeues after a session reset. | Uncertain publications can duplicate while retries remain. |
| Publication timeout or exhausted retries | The connector cleans and reconnects. It retransmits after session resumption or requeues after a session reset, up to `max_retries`. | After retries end, unconfirmed records are lost from this sink because Iggy already committed the offset. |
| MQTT 5 non-retryable broker rejection | The connector returns the rejection without retrying. | Unconfirmed records are lost from this sink because Iggy already committed the offset. |
| Authentication failure | Connection initialization fails | The sink does not publish records. |
| Shutdown timeout | The sink reports unfinished publications | It returns an error after the timeout. |

---
