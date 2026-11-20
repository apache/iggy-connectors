// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

//! Wraps the messages of one `consume` call in OTLP log records.
//!
//! Every message becomes one log record: the payload is the body and the headers are
//! attributes. The records of a request share one resource and one scope, which are built
//! once from the config.
//!
//! Records are packed whole, as messages are in `batch`. A request grows until the next
//! record would take it past `max_request_bytes`. A single record larger than that is sent
//! on its own.

use crate::batch::{Packed, Prepared};
use iggy_common::{HeaderKind, HeaderValue};
use iggy_connector_sdk::headers::HeaderData;
use iggy_connector_sdk::{ConsumedMessage, HeaderName, MessagesMetadata, Payload, TopicMetadata};
use opentelemetry_proto::tonic::collector::logs::v1::ExportLogsServiceRequest;
use opentelemetry_proto::tonic::common::v1::{
    AnyValue, ArrayValue, InstrumentationScope, KeyValue, KeyValueList, any_value,
};
use opentelemetry_proto::tonic::logs::v1::{LogRecord, ResourceLogs, ScopeLogs};
use opentelemetry_proto::tonic::resource::v1::Resource;
use prost::Message;
use simd_json::prelude::Writable;
use simd_json::{OwnedValue, StaticNode};

/// The scope every record is exported under names this connector, which created the record.
const SCOPE_NAME: &str = "iggy_connector_otlp_sink";
/// The resource entry and its scope entry each carry a length prefix that covers the records.
/// A prefix is 1 to 10 bytes, so each takes at most 9 bytes more than in an empty request.
const LENGTH_PREFIX_GROWTH: usize = 18;
/// How many attributes `include_metadata` adds to a record.
const METADATA_ATTRIBUTES: usize = 5;
/// How many levels of a JSON body stay structured. A level nests up to three protobuf
/// messages and the body sits four deep in its request, so 30 levels reach at most 94 of the
/// 100 that protobuf decoders other than Go's accept. The limit also bounds the encoding
/// work, which prost repeats at every level for each message below it. A deeper value becomes
/// its JSON text.
const MAX_BODY_DEPTH: usize = 30;

/// The resource and the scope that the log records of this sink are exported under.
#[derive(Debug)]
pub(crate) struct LogRecordTemplate {
    resource: Resource,
    scope: InstrumentationScope,
    /// Upper bound on the bytes a request takes on top of its log records.
    pub overhead: usize,
    pub include_metadata: bool,
}

impl LogRecordTemplate {
    pub fn new<'a>(
        resource_attributes: impl IntoIterator<Item = (&'a String, &'a String)>,
        include_metadata: bool,
    ) -> Self {
        let resource = Resource {
            attributes: resource_attributes
                .into_iter()
                .map(|(key, value)| {
                    key_value(key.clone(), any_value::Value::StringValue(value.clone()))
                })
                .collect(),
            ..Default::default()
        };
        let scope = InstrumentationScope {
            name: SCOPE_NAME.to_owned(),
            version: env!("CARGO_PKG_VERSION").to_owned(),
            ..Default::default()
        };
        let mut template = Self {
            resource,
            scope,
            overhead: LENGTH_PREFIX_GROWTH,
            include_metadata,
        };
        template.overhead += template.request(Vec::new()).encoded_len();
        template
    }

    fn request(&self, log_records: Vec<LogRecord>) -> ExportLogsServiceRequest {
        ExportLogsServiceRequest {
            resource_logs: vec![ResourceLogs {
                resource: Some(self.resource.clone()),
                scope_logs: vec![ScopeLogs {
                    scope: Some(self.scope.clone()),
                    log_records,
                    ..Default::default()
                }],
                ..Default::default()
            }],
        }
    }

    fn pack(&self, chunk: Chunk, max_request_bytes: usize) -> Packed {
        let count = chunk.log_records.len() as u64;
        let body = self.request(chunk.log_records).encode_to_vec();
        Packed {
            // Only a request of one record can be over the limit.
            oversized: body.len() > max_request_bytes,
            body,
            messages: count,
            log_records: count,
            first_offset: chunk.first_offset,
            last_offset: chunk.last_offset,
        }
    }
}

/// The log records of one request, before they are encoded.
#[derive(Default)]
struct Chunk {
    log_records: Vec<LogRecord>,
    /// The encoded size of the records, each with its field tag and length prefix.
    size: usize,
    first_offset: u64,
    last_offset: u64,
}

/// Wrap each message in a log record, then pack the records.
pub(crate) fn pack_log_records(
    template: &LogRecordTemplate,
    topic_metadata: &TopicMetadata,
    messages_metadata: &MessagesMetadata,
    messages: Vec<ConsumedMessage>,
    max_request_bytes: usize,
) -> Prepared {
    let budget = max_request_bytes.saturating_sub(template.overhead);
    let mut requests = Vec::new();
    let mut current = Chunk::default();
    for message in messages {
        let offset = message.offset;
        let record = log_record(
            template.include_metadata,
            topic_metadata,
            messages_metadata,
            message,
        );
        let size = framed_len(&record);
        if !current.log_records.is_empty() && current.size + size > budget {
            requests.push(template.pack(std::mem::take(&mut current), max_request_bytes));
        }
        if current.log_records.is_empty() {
            current.first_offset = offset;
        }
        current.last_offset = offset;
        current.size += size;
        current.log_records.push(record);
    }
    if !current.log_records.is_empty() {
        requests.push(template.pack(current, max_request_bytes));
    }

    // Every payload has a log record body, so no message is dropped.
    Prepared {
        requests,
        dropped: 0,
        first_dropped: None,
    }
}

fn log_record(
    include_metadata: bool,
    topic_metadata: &TopicMetadata,
    messages_metadata: &MessagesMetadata,
    message: ConsumedMessage,
) -> LogRecord {
    let header_count = message.headers.as_ref().map_or(0, |headers| headers.len());
    let mut attributes = Vec::with_capacity(METADATA_ATTRIBUTES + header_count);
    if include_metadata {
        attributes.push(key_value(
            "iggy.stream",
            any_value::Value::StringValue(topic_metadata.stream.clone()),
        ));
        attributes.push(key_value(
            "iggy.topic",
            any_value::Value::StringValue(topic_metadata.topic.clone()),
        ));
        attributes.push(key_value(
            "iggy.partition_id",
            any_value::Value::IntValue(i64::from(messages_metadata.partition_id)),
        ));
        attributes.push(key_value("iggy.offset", int_or_text(message.offset)));
        // A message ID is 128 bits wide, and an OTLP integer holds 64.
        attributes.push(key_value(
            "iggy.message_id",
            any_value::Value::StringValue(message.id.to_string()),
        ));
    }
    let metadata_attributes = attributes.len();
    for (key, value) in message.headers.iter().flatten() {
        let name = HeaderName(key).to_string();
        // OTLP requires unique attribute keys. Header names are unique among themselves, so
        // only a header named like a metadata attribute repeats a key. The metadata wins,
        // because it comes from Iggy and not from the producer.
        if attributes[..metadata_attributes]
            .iter()
            .any(|attribute| attribute.key == name)
        {
            continue;
        }
        attributes.push(key_value(name, header_value(value)));
    }

    // A producer that sets no origin timestamp leaves it at zero.
    let occurred = if message.origin_timestamp > 0 {
        message.origin_timestamp
    } else {
        message.timestamp
    };
    LogRecord {
        time_unix_nano: micros_to_nanos(occurred),
        observed_time_unix_nano: micros_to_nanos(message.timestamp),
        body: Some(AnyValue {
            value: payload_value(message.payload),
        }),
        attributes,
        ..Default::default()
    }
}

fn payload_value(payload: Payload) -> Option<any_value::Value> {
    match payload.into_json_document() {
        Payload::Json(document) => json_value(document, 0),
        Payload::Text(text) | Payload::Proto(text) => Some(any_value::Value::StringValue(text)),
        Payload::Raw(bytes) | Payload::FlatBuffer(bytes) | Payload::Avro(bytes) => {
            Some(any_value::Value::BytesValue(bytes))
        }
    }
}

/// A JSON null is an `AnyValue` without a value. `depth` counts the objects and arrays
/// around `value`.
fn json_value(value: OwnedValue, depth: usize) -> Option<any_value::Value> {
    Some(match value {
        OwnedValue::Static(StaticNode::Null) => return None,
        OwnedValue::Static(StaticNode::Bool(flag)) => any_value::Value::BoolValue(flag),
        OwnedValue::Static(StaticNode::I64(number)) => any_value::Value::IntValue(number),
        OwnedValue::Static(StaticNode::U64(number)) => int_or_text(number),
        OwnedValue::Static(StaticNode::F64(number)) => any_value::Value::DoubleValue(number),
        OwnedValue::String(text) => any_value::Value::StringValue(text),
        OwnedValue::Array(_) | OwnedValue::Object(_) if depth >= MAX_BODY_DEPTH => {
            any_value::Value::StringValue(value.encode())
        }
        OwnedValue::Array(items) => any_value::Value::ArrayValue(ArrayValue {
            values: items
                .into_iter()
                .map(|item| AnyValue {
                    value: json_value(item, depth + 1),
                })
                .collect(),
        }),
        OwnedValue::Object(fields) => any_value::Value::KvlistValue(KeyValueList {
            values: fields
                .into_iter()
                .map(|(key, field)| KeyValue {
                    key,
                    value: Some(AnyValue {
                        value: json_value(field, depth + 1),
                    }),
                    ..Default::default()
                })
                .collect(),
        }),
    })
}

/// A header keeps its type where OTLP has one for it.
fn header_value(value: &HeaderValue) -> any_value::Value {
    let typed = match value.kind() {
        HeaderKind::Raw => Some(any_value::Value::BytesValue(value.as_bytes().to_vec())),
        // An OTLP string is UTF-8, which a string header does not have to be.
        HeaderKind::String => Some(match value.as_str() {
            Ok(text) => any_value::Value::StringValue(text.to_owned()),
            Err(_) => any_value::Value::BytesValue(value.as_bytes().to_vec()),
        }),
        HeaderKind::Bool => value.as_bool().ok().map(any_value::Value::BoolValue),
        HeaderKind::Int8 => value.as_int8().ok().map(int_or_text),
        HeaderKind::Int16 => value.as_int16().ok().map(int_or_text),
        HeaderKind::Int32 => value.as_int32().ok().map(int_or_text),
        HeaderKind::Int64 => value.as_int64().ok().map(int_or_text),
        HeaderKind::Int128 => value.as_int128().ok().map(int_or_text),
        HeaderKind::Uint8 => value.as_uint8().ok().map(int_or_text),
        HeaderKind::Uint16 => value.as_uint16().ok().map(int_or_text),
        HeaderKind::Uint32 => value.as_uint32().ok().map(int_or_text),
        HeaderKind::Uint64 => value.as_uint64().ok().map(int_or_text),
        HeaderKind::Uint128 => value.as_uint128().ok().map(int_or_text),
        HeaderKind::Float32 => value
            .as_float32()
            .ok()
            .map(|number| any_value::Value::DoubleValue(f64::from(number))),
        HeaderKind::Float64 => value.as_float64().ok().map(any_value::Value::DoubleValue),
    };
    // A value whose bytes do not fit its kind keeps the text that every sink writes for it.
    typed.unwrap_or_else(|| any_value::Value::StringValue(HeaderData(value).to_string()))
}

/// An OTLP integer is 64 bits wide and signed. A number outside of it keeps every digit as
/// text, because a double would round it.
fn int_or_text<N>(number: N) -> any_value::Value
where
    N: Copy + ToString,
    i64: TryFrom<N>,
{
    match i64::try_from(number) {
        Ok(number) => any_value::Value::IntValue(number),
        Err(_) => any_value::Value::StringValue(number.to_string()),
    }
}

fn key_value(key: impl Into<String>, value: any_value::Value) -> KeyValue {
    KeyValue {
        key: key.into(),
        value: Some(AnyValue { value: Some(value) }),
        ..Default::default()
    }
}

/// Iggy timestamps are microseconds since the Unix epoch.
fn micros_to_nanos(micros: u64) -> u64 {
    micros.saturating_mul(1_000)
}

/// The bytes a record takes in its request: the field tag, the length prefix and the record.
fn framed_len(record: &LogRecord) -> usize {
    let len = record.encoded_len();
    1 + prost::length_delimiter_len(len) + len
}

#[cfg(test)]
mod tests {
    use super::*;
    use iggy_common::HeaderKey;
    use iggy_connector_sdk::Schema;
    use iggy_connector_testing::headers::{header_case_map, header_cases};
    use std::collections::BTreeMap;

    const NO_LIMIT: usize = usize::MAX;

    fn topic() -> TopicMetadata {
        TopicMetadata {
            stream: "otel".to_owned(),
            topic: "logs".to_owned(),
        }
    }

    fn metadata() -> MessagesMetadata {
        MessagesMetadata {
            partition_id: 1,
            current_offset: 10,
            schema: Schema::Json,
        }
    }

    fn message(payload: Payload) -> ConsumedMessage {
        message_at(7, payload)
    }

    fn message_at(offset: u64, payload: Payload) -> ConsumedMessage {
        ConsumedMessage {
            id: 42,
            offset,
            checksum: 0,
            timestamp: 2_000,
            origin_timestamp: 1_000,
            headers: None,
            payload,
        }
    }

    fn message_with_header(key: &str, value: HeaderValue) -> ConsumedMessage {
        let mut consumed = message(Payload::Text("x".to_owned()));
        consumed.headers = Some(BTreeMap::from([(
            HeaderKey::try_from(key).expect("valid key"),
            value,
        )]));
        consumed
    }

    /// One resource attribute, and the Iggy coordinates on every record.
    fn template() -> LogRecordTemplate {
        let attributes = BTreeMap::from([("service.name".to_owned(), "checkout".to_owned())]);
        LogRecordTemplate::new(&attributes, true)
    }

    /// No resource attribute and no Iggy coordinates, so a record holds only its message.
    fn bare_template() -> LogRecordTemplate {
        LogRecordTemplate::new(&BTreeMap::new(), false)
    }

    fn text(value: &str) -> any_value::Value {
        any_value::Value::StringValue(value.to_owned())
    }

    fn attribute<'a>(record: &'a LogRecord, key: &str) -> Option<&'a any_value::Value> {
        record
            .attributes
            .iter()
            .find(|kv| kv.key == key)
            .and_then(|kv| kv.value.as_ref())
            .and_then(|value| value.value.as_ref())
    }

    fn decoded(packed: &Packed) -> ExportLogsServiceRequest {
        ExportLogsServiceRequest::decode(packed.body.as_slice())
            .expect("a packed body is one export request")
    }

    fn first_record(prepared: &Prepared) -> LogRecord {
        decoded(&prepared.requests[0]).resource_logs[0].scope_logs[0].log_records[0].clone()
    }

    fn record_of(template: &LogRecordTemplate, consumed: ConsumedMessage) -> LogRecord {
        first_record(&pack_log_records(
            template,
            &topic(),
            &metadata(),
            vec![consumed],
            NO_LIMIT,
        ))
    }

    fn body(record: &LogRecord) -> Option<&any_value::Value> {
        record.body.as_ref().and_then(|body| body.value.as_ref())
    }

    #[test]
    fn given_text_message_should_become_log_record_body() {
        let record = record_of(
            &template(),
            message(Payload::Text("user signed in".to_owned())),
        );

        assert_eq!(body(&record), Some(&text("user signed in")));
        assert_eq!(record.time_unix_nano, 1_000_000);
        assert_eq!(record.observed_time_unix_nano, 2_000_000);
    }

    #[test]
    fn given_missing_origin_timestamp_should_use_append_time() {
        let mut consumed = message(Payload::Text("x".to_owned()));
        consumed.origin_timestamp = 0;

        let record = record_of(&template(), consumed);

        assert_eq!(record.time_unix_nano, 2_000_000);
    }

    #[test]
    fn given_json_message_should_become_structured_body() {
        let payload = Payload::Json(simd_json::json!({
            "level": "warn",
            "retries": 3,
            "ratio": 0.5,
            "ok": false,
            "tags": ["a", "b"],
            "missing": null
        }));

        let record = record_of(&template(), message(payload));

        let Some(any_value::Value::KvlistValue(fields)) = body(&record) else {
            panic!("expected a key-value body, got {:?}", record.body);
        };
        let field = |name: &str| {
            fields
                .values
                .iter()
                .find(|kv| kv.key == name)
                .and_then(|kv| kv.value.as_ref())
                .map(|value| value.value.clone())
        };
        assert_eq!(field("level"), Some(Some(text("warn"))));
        assert_eq!(field("retries"), Some(Some(any_value::Value::IntValue(3))));
        assert_eq!(
            field("ratio"),
            Some(Some(any_value::Value::DoubleValue(0.5)))
        );
        assert_eq!(field("ok"), Some(Some(any_value::Value::BoolValue(false))));
        assert_eq!(field("missing"), Some(None));
        assert_eq!(
            field("tags"),
            Some(Some(any_value::Value::ArrayValue(ArrayValue {
                values: vec![
                    AnyValue {
                        value: Some(text("a"))
                    },
                    AnyValue {
                        value: Some(text("b"))
                    },
                ],
            })))
        );
    }

    #[test]
    fn given_u64_above_i64_max_should_keep_exact_value_as_string() {
        let value = json_value(simd_json::json!(u64::MAX), 0);

        assert_eq!(value, Some(text(&u64::MAX.to_string())));
    }

    const NESTINGS: [(&str, &str); 2] = [(r#"{"a":"#, "}"), ("[", "]")];

    /// `depth` objects or arrays inside each other, around the number 1.
    fn nested_json(open: &str, close: &str, depth: usize) -> String {
        format!("{}1{}", open.repeat(depth), close.repeat(depth))
    }

    fn json_payload(text: &str) -> Payload {
        let mut bytes = text.as_bytes().to_vec();
        Payload::Json(simd_json::to_owned_value(&mut bytes).expect("valid JSON"))
    }

    /// Follow the first entry of each list or array down from `value`. Return how many it
    /// passed, and the value it stopped at.
    fn descend(mut value: &any_value::Value) -> (usize, &any_value::Value) {
        let mut levels = 0;
        loop {
            let inner = match value {
                any_value::Value::KvlistValue(list) => list.values[0].value.as_ref(),
                any_value::Value::ArrayValue(array) => array.values.first(),
                _ => return (levels, value),
            };
            levels += 1;
            value = inner
                .and_then(|any| any.value.as_ref())
                .expect("a nested value");
        }
    }

    #[test]
    fn given_json_nested_up_to_the_depth_limit_should_stay_structured() {
        for (open, close) in NESTINGS {
            let payload = json_payload(&nested_json(open, close, MAX_BODY_DEPTH));

            let record = record_of(&bare_template(), message(payload));

            let (levels, leaf) = descend(body(&record).expect("a body"));
            assert_eq!(levels, MAX_BODY_DEPTH, "{open}");
            assert_eq!(leaf, &any_value::Value::IntValue(1), "{open}");
        }
    }

    // `record_of` decodes the request with prost, which refuses more than 100 nested messages.
    #[test]
    fn given_json_nested_deeper_than_the_depth_limit_should_keep_the_rest_as_json_text() {
        let depth = 1_000;
        for (open, close) in NESTINGS {
            let payload = json_payload(&nested_json(open, close, depth));

            let record = record_of(&bare_template(), message(payload));

            let (levels, leaf) = descend(body(&record).expect("a body"));
            assert_eq!(levels, MAX_BODY_DEPTH, "{open}");
            assert_eq!(
                leaf,
                &text(&nested_json(open, close, depth - MAX_BODY_DEPTH)),
                "{open}"
            );
        }
    }

    #[test]
    fn given_proto_text_should_become_structured_body_only_when_it_is_json() {
        let document = record_of(
            &template(),
            message(Payload::Proto(r#"{"level":"warn"}"#.to_owned())),
        );
        let plain = record_of(
            &template(),
            message(Payload::Proto("level: warn".to_owned())),
        );

        assert!(
            matches!(body(&document), Some(any_value::Value::KvlistValue(_))),
            "{:?}",
            document.body
        );
        assert_eq!(body(&plain), Some(&text("level: warn")));
    }

    #[test]
    fn given_raw_message_should_become_bytes_body() {
        let record = record_of(&template(), message(Payload::Raw(vec![1, 2, 3])));

        assert_eq!(
            body(&record),
            Some(&any_value::Value::BytesValue(vec![1, 2, 3]))
        );
    }

    #[test]
    fn given_include_metadata_should_attach_iggy_coordinates() {
        let record = record_of(&template(), message(Payload::Text("x".to_owned())));

        assert_eq!(record.attributes.len(), METADATA_ATTRIBUTES);
        assert_eq!(attribute(&record, "iggy.stream"), Some(&text("otel")));
        assert_eq!(attribute(&record, "iggy.topic"), Some(&text("logs")));
        assert_eq!(
            attribute(&record, "iggy.partition_id"),
            Some(&any_value::Value::IntValue(1))
        );
        assert_eq!(
            attribute(&record, "iggy.offset"),
            Some(&any_value::Value::IntValue(7))
        );
        assert_eq!(attribute(&record, "iggy.message_id"), Some(&text("42")));
    }

    #[test]
    fn given_offset_above_i64_max_should_keep_exact_value_as_string() {
        let record = record_of(
            &template(),
            message_at(u64::MAX, Payload::Text("x".to_owned())),
        );

        assert_eq!(
            attribute(&record, "iggy.offset"),
            Some(&text(&u64::MAX.to_string()))
        );
    }

    #[test]
    fn given_metadata_disabled_should_attach_only_headers() {
        let consumed = message_with_header(
            "tenant",
            HeaderValue::try_from("acme").expect("valid value"),
        );

        let record = record_of(&bare_template(), consumed);

        assert_eq!(record.attributes.len(), 1);
        assert_eq!(attribute(&record, "tenant"), Some(&text("acme")));
    }

    #[test]
    fn given_header_named_like_metadata_should_keep_the_metadata_attribute() {
        let consumed = message_with_header(
            "iggy.offset",
            HeaderValue::try_from("spoofed").expect("valid value"),
        );

        let record = record_of(&template(), consumed);

        let offsets: Vec<_> = record
            .attributes
            .iter()
            .filter(|kv| kv.key == "iggy.offset")
            .collect();
        assert_eq!(offsets.len(), 1, "OTLP forbids a repeated attribute key");
        assert_eq!(
            attribute(&record, "iggy.offset"),
            Some(&any_value::Value::IntValue(7))
        );
    }

    #[test]
    fn given_header_named_like_metadata_should_keep_the_header_without_metadata() {
        let consumed = message_with_header(
            "iggy.offset",
            HeaderValue::try_from("from-producer").expect("valid value"),
        );

        let record = record_of(&bare_template(), consumed);

        assert_eq!(
            attribute(&record, "iggy.offset"),
            Some(&text("from-producer"))
        );
    }

    #[test]
    fn given_shared_header_cases_should_name_attributes_by_header_name() {
        let cases = header_cases();
        let mut consumed = message(Payload::Text("x".to_owned()));
        consumed.headers = Some(header_case_map());

        let record = record_of(&bare_template(), consumed);

        assert_eq!(record.attributes.len(), cases.len());
        for case in &cases {
            assert_eq!(
                attribute(&record, &case.expected_name),
                Some(&text(case.label)),
                "case {}",
                case.label
            );
        }
    }

    #[test]
    fn given_typed_headers_should_keep_their_otlp_types() {
        assert_eq!(
            header_value(&HeaderValue::from(7i32)),
            any_value::Value::IntValue(7)
        );
        assert_eq!(
            header_value(&HeaderValue::from(-7i128)),
            any_value::Value::IntValue(-7)
        );
        assert_eq!(
            header_value(&HeaderValue::from(true)),
            any_value::Value::BoolValue(true)
        );
        assert_eq!(
            header_value(&HeaderValue::from(1.5f32)),
            any_value::Value::DoubleValue(1.5)
        );
        assert_eq!(
            header_value(&HeaderValue::from(2.5f64)),
            any_value::Value::DoubleValue(2.5)
        );
        assert_eq!(
            header_value(&HeaderValue::try_from(vec![9u8, 8]).expect("valid value")),
            any_value::Value::BytesValue(vec![9, 8])
        );
    }

    #[test]
    fn given_integer_header_outside_i64_should_keep_exact_value_as_string() {
        assert_eq!(
            header_value(&HeaderValue::from(u64::MAX)),
            text(&u64::MAX.to_string())
        );
        assert_eq!(
            header_value(&HeaderValue::from(i128::MIN)),
            text(&i128::MIN.to_string())
        );
        assert_eq!(
            header_value(&HeaderValue::from(u128::MAX)),
            text(&u128::MAX.to_string())
        );
    }

    #[test]
    fn given_string_header_that_is_not_utf8_should_become_bytes() {
        let value = HeaderValue::from_raw(HeaderKind::String, &[0xff, 0xfe]).expect("valid value");

        assert_eq!(
            header_value(&value),
            any_value::Value::BytesValue(vec![0xff, 0xfe])
        );
    }

    #[test]
    fn given_resource_attributes_should_stamp_every_request() {
        let messages = (0..4)
            .map(|offset| message_at(offset, Payload::Text("x".repeat(50))))
            .collect();

        let prepared = pack_log_records(&template(), &topic(), &metadata(), messages, 400);

        assert!(prepared.requests.len() > 1);
        for packed in &prepared.requests {
            let request = decoded(packed);
            assert_eq!(request.resource_logs.len(), 1);
            let resource_logs = &request.resource_logs[0];
            let resource = resource_logs.resource.as_ref().expect("resource is set");
            assert_eq!(resource.attributes.len(), 1);
            assert_eq!(resource.attributes[0].key, "service.name");
            assert_eq!(
                resource.attributes[0]
                    .value
                    .as_ref()
                    .and_then(|value| value.value.as_ref()),
                Some(&text("checkout"))
            );
            assert_eq!(resource_logs.scope_logs.len(), 1);
            let scope = resource_logs.scope_logs[0]
                .scope
                .as_ref()
                .expect("scope is set");
            assert_eq!(scope.name, SCOPE_NAME);
            assert_eq!(scope.version, env!("CARGO_PKG_VERSION"));
        }
    }

    #[test]
    fn given_messages_within_limit_should_share_one_request() {
        let messages = (3..6)
            .map(|offset| message_at(offset, Payload::Text(format!("record {offset}"))))
            .collect();

        let prepared = pack_log_records(&template(), &topic(), &metadata(), messages, NO_LIMIT);

        assert_eq!(prepared.dropped, 0);
        assert!(prepared.first_dropped.is_none());
        assert_eq!(prepared.requests.len(), 1);
        let packed = &prepared.requests[0];
        assert_eq!((packed.messages, packed.log_records), (3, 3));
        assert_eq!((packed.first_offset, packed.last_offset), (3, 5));
        assert!(!packed.oversized);
        let bodies: Vec<_> = decoded(packed).resource_logs[0].scope_logs[0]
            .log_records
            .iter()
            .map(|record| body(record).cloned())
            .collect();
        assert_eq!(
            bodies,
            [
                Some(text("record 3")),
                Some(text("record 4")),
                Some(text("record 5"))
            ]
        );
    }

    // The limits cover every remainder of the record size, so some of them leave room for
    // exactly the records of a request and none for its length prefixes to grow.
    #[test]
    fn given_any_limit_should_keep_requests_of_several_records_within_it() {
        let template = bare_template();
        for limit in 300..500 {
            let messages = (0..10)
                .map(|offset| message_at(offset, Payload::Text("x".repeat(50))))
                .collect();

            let prepared = pack_log_records(&template, &topic(), &metadata(), messages, limit);

            assert!(prepared.requests[0].messages > 1, "limit {limit}");
            let total: u64 = prepared.requests.iter().map(|packed| packed.messages).sum();
            assert_eq!(total, 10, "limit {limit}");
            for packed in &prepared.requests {
                assert!(
                    packed.body.len() <= limit,
                    "{} bytes with limit {limit}",
                    packed.body.len()
                );
                assert!(!packed.oversized, "limit {limit}");
            }
        }
    }

    #[test]
    fn given_record_larger_than_limit_should_be_sent_alone_and_flagged() {
        let small = || Payload::Text("small".to_owned());
        let large = || Payload::Text("x".repeat(500));
        let messages = vec![
            message_at(1, large()),
            message_at(2, small()),
            message_at(3, small()),
            message_at(4, large()),
            message_at(5, small()),
        ];

        let prepared = pack_log_records(&bare_template(), &topic(), &metadata(), messages, 200);

        let requests: Vec<_> = prepared
            .requests
            .iter()
            .map(|packed| (packed.first_offset, packed.last_offset, packed.oversized))
            .collect();
        assert_eq!(
            requests,
            [(1, 1, true), (2, 3, false), (4, 4, true), (5, 5, false)]
        );
    }
}
