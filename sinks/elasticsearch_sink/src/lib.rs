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

use async_trait::async_trait;
use base64::{Engine as _, engine::general_purpose};
use elasticsearch::{
    BulkParts, Elasticsearch,
    auth::Credentials,
    http::{Url, request::JsonBody, transport::TransportBuilder},
};
use iggy_common::{HeaderKey, HeaderValue, IggyTimestamp};
use iggy_connector_sdk::retry::{RetryPolicy, parse_duration};
use iggy_connector_sdk::{
    ConsumedMessage, Error, JsonHeaders, MessagesMetadata, Payload, Schema, Sink, TopicMetadata,
    sink_connector,
};
use secrecy::{ExposeSecret, SecretString};
use serde::{Deserialize, Serialize};
use simd_json::owned::Object;
use simd_json::{OwnedValue, prelude::*};
use std::collections::BTreeMap;
use std::time::Duration;
use tokio::sync::Mutex;
use tracing::{error, info, warn};

sink_connector!(ElasticsearchSink);

const DEFAULT_TIMEOUT_SECONDS: u64 = 30;
const DEFAULT_MAX_RETRIES: u32 = 3;
const DEFAULT_RETRY_DELAY: &str = "1s";
const DEFAULT_RETRY_MAX_DELAY: &str = "5s";
const DEFAULT_INCLUDE_HEADERS: bool = false;
const HEADERS_FIELD: &str = "_iggy_headers";
const HEADER_DATA_PATH_MATCH: &str = "_iggy_headers.*.data";
/// Name and `match_mapping_type` of each keyword template for header `data`.
const HEADER_DATA_TEMPLATES: [(&str, &str); 2] = [
    ("iggy_header_data", "string"),
    ("iggy_header_data_date", "date"),
];

#[derive(Deserialize)]
struct BulkResponse {
    items: Vec<BulkItem>,
}

#[derive(Deserialize)]
struct BulkItem {
    index: BulkIndexResult,
}

#[derive(Deserialize)]
struct BulkIndexResult {
    status: u16,
    error: Option<serde_json::Value>,
}

#[derive(Debug)]
struct State {
    invocations_count: usize,
    documents_indexed: usize,
    errors_count: usize,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ElasticsearchSinkConfig {
    pub url: String,
    pub index: String,
    pub username: Option<String>,
    #[serde(serialize_with = "iggy_common::serde_secret::serialize_optional_secret")]
    pub password: Option<SecretString>,
    pub batch_size: Option<usize>,
    /// Client-wide HTTP timeout for open() and bulk consume(). Values of `0`
    /// are clamped to 1s (a zero duration would fail every request immediately).
    /// Raise this for slow bulk workloads; until runtime ack/retry (#2927/#2928),
    /// a timeout on consume drops the batch after the poll offset is already
    /// committed.
    pub timeout_seconds: Option<u64>,
    /// Total attempts for explicitly rejected bulk items, including the first.
    /// Defaults to 3; values of 0 and 1 both disable retries.
    pub max_retries: Option<u32>,
    pub retry_delay: Option<String>,
    pub retry_max_delay: Option<String>,
    pub create_index_if_not_exists: Option<bool>,
    pub index_mapping: Option<serde_json::Value>,
    /// Writes user headers to `_iggy_headers` and maps their `data` as keyword
    /// on index creation. Defaults to `false`, so that an upgrade leaves the
    /// documents of an existing index unchanged.
    pub include_headers: Option<bool>,
}

#[derive(Debug)]
pub struct ElasticsearchSink {
    id: u32,
    config: ElasticsearchSinkConfig,
    client: Option<Elasticsearch>,
    retry_policy: RetryPolicy,
    include_headers: bool,
    state: Mutex<State>,
}

impl ElasticsearchSink {
    pub fn new(id: u32, config: ElasticsearchSinkConfig) -> Self {
        let retry_policy = RetryPolicy {
            max_attempts: config.max_retries.unwrap_or(DEFAULT_MAX_RETRIES).max(1),
            base_delay: parse_duration(config.retry_delay.as_deref(), DEFAULT_RETRY_DELAY),
            max_delay: parse_duration(config.retry_max_delay.as_deref(), DEFAULT_RETRY_MAX_DELAY),
        };
        let include_headers = config.include_headers.unwrap_or(DEFAULT_INCLUDE_HEADERS);
        ElasticsearchSink {
            id,
            config,
            client: None,
            retry_policy,
            include_headers,
            state: Mutex::new(State {
                invocations_count: 0,
                documents_indexed: 0,
                errors_count: 0,
            }),
        }
    }

    async fn create_client(&self) -> Result<Elasticsearch, Error> {
        let url = Url::parse(&self.config.url)
            .map_err(|error| Error::Connection(format!("Invalid Elasticsearch URL: {error}")))?;

        let conn_pool = elasticsearch::http::transport::SingleNodeConnectionPool::new(url);
        // elasticsearch-rs defaults to no timeout. This client-global timeout is
        // an infinite-hang backstop for open() and for bulk consume() — not the
        // primary #3728 flake fix (that is the harness readiness gate, which
        // expires sooner than the 30s default). Values of 0 clamp to 1s.
        let timeout_seconds = self
            .config
            .timeout_seconds
            .unwrap_or(DEFAULT_TIMEOUT_SECONDS)
            .max(1);
        let mut transport_builder =
            TransportBuilder::new(conn_pool).timeout(Duration::from_secs(timeout_seconds));

        if let (Some(username), Some(password)) = (&self.config.username, &self.config.password) {
            let credentials =
                Credentials::Basic(username.clone(), password.expose_secret().to_string());
            transport_builder = transport_builder.auth(credentials);
        }

        let transport = transport_builder
            .build()
            .map_err(|e| Error::Connection(format!("Failed to build transport: {}", e)))?;

        Ok(Elasticsearch::new(transport))
    }

    async fn ensure_index_exists(&self, client: &Elasticsearch) -> Result<(), Error> {
        if !self.config.create_index_if_not_exists.unwrap_or(true) {
            return Ok(());
        }

        let response = client
            .indices()
            .exists(elasticsearch::indices::IndicesExistsParts::Index(&[&self
                .config
                .index]))
            .send()
            .await
            .map_err(|e| Error::Connection(format!("Failed to check index existence: {}", e)))?;

        if response.status_code().is_success() {
            info!("Index '{}' already exists", self.config.index);
            return Ok(());
        }

        let indices = client.indices();
        let request = indices.create(elasticsearch::indices::IndicesCreateParts::Index(
            &self.config.index,
        ));
        let response = match self.index_create_body() {
            Some(body) => request.body(body).send().await,
            None => request.send().await,
        }
        .map_err(|e| Error::Connection(format!("Failed to create index: {}", e)))?;

        if response.status_code().is_success() {
            info!("Successfully created index '{}'", self.config.index);
        } else {
            let error_text = response
                .text()
                .await
                .unwrap_or_else(|_| "Unknown error".to_string());
            return Err(Error::Connection(format!(
                "Failed to create index '{}': {}",
                self.config.index, error_text
            )));
        }

        Ok(())
    }

    /// The body of the index-create request. `None` creates the index with no
    /// body.
    fn index_create_body(&self) -> Option<serde_json::Value> {
        let mapping = self.config.index_mapping.clone();
        if !self.include_headers {
            return mapping;
        }
        let mut body = mapping.unwrap_or_else(|| serde_json::json!({}));
        if !prepend_header_data_template(&mut body) {
            warn!(
                "Elasticsearch sink with ID: {} cannot add the header template to index_mapping, because it is not an object with object `mappings` and array `dynamic_templates`. Header data uses dynamic mapping.",
                self.id
            );
        }
        Some(body)
    }

    async fn bulk_index_documents(
        &self,
        client: &Elasticsearch,
        mut documents: Vec<OwnedValue>,
    ) -> Result<usize, Error> {
        if documents.is_empty() {
            return Ok(0);
        }

        let action = simd_json::json!({"index": {"_index": self.config.index.as_str()}});
        let total_documents = documents.len();
        let mut documents_indexed = 0;
        let mut permanent_rejections = 0;
        let mut attempt = 0;
        loop {
            attempt += 1;
            let mut body: Vec<JsonBody<&OwnedValue>> = Vec::with_capacity(documents.len() * 2);
            for document in &documents {
                body.push((&action).into());
                body.push(document.into());
            }

            // Generated document IDs make transport failures ambiguous. Retry only
            // items the server explicitly rejected, never the whole request.
            let response = client
                .bulk(BulkParts::None)
                .body(body)
                .send()
                .await
                .map_err(|error| {
                    Error::Connection(format!("Failed to execute bulk request: {error}"))
                })?;
            if !response.status_code().is_success() {
                let status = response.status_code();
                let reason = response.text().await.unwrap_or_default();
                return Err(Error::Connection(format!(
                    "Bulk indexing failed with status {status}: {reason}"
                )));
            }
            let response: BulkResponse = response.json().await.map_err(|error| {
                Error::Connection(format!("Failed to parse bulk response: {error}"))
            })?;
            if response.items.len() != documents.len() {
                return Err(Error::Connection(format!(
                    "Elasticsearch bulk response carried {} items for {} documents",
                    response.items.len(),
                    documents.len()
                )));
            }

            let mut retry_documents = Vec::new();
            let mut indexed = 0;
            let mut rejected = 0;
            for (document, item) in documents.into_iter().zip(response.items) {
                let result = item.index;
                if (200..300).contains(&result.status) && result.error.is_none() {
                    indexed += 1;
                } else {
                    warn!(
                        "Document indexing error: status {}: {:?}",
                        result.status, result.error
                    );
                    if result.status == 429 || (500..600).contains(&result.status) {
                        retry_documents.push(document);
                    } else {
                        rejected += 1;
                    }
                }
            }
            documents_indexed += indexed;
            permanent_rejections += rejected;
            {
                let mut state = self.state.lock().await;
                state.documents_indexed += indexed;
                state.errors_count += rejected;
            }
            if retry_documents.is_empty() {
                break;
            }
            if attempt >= self.retry_policy.max_attempts {
                self.state.lock().await.errors_count += retry_documents.len();
                return Err(Error::CannotStoreData(format!(
                    "Elasticsearch indexed {documents_indexed} of {total_documents} documents in index '{}'; {} transient rejections remain after {attempt} attempts, {permanent_rejections} permanent rejections",
                    self.config.index,
                    retry_documents.len()
                )));
            }
            let delay = self.retry_policy.backoff(attempt);
            warn!(
                "Retrying {} rejected Elasticsearch documents after {delay:?}, attempt {attempt}/{}",
                retry_documents.len(),
                self.retry_policy.max_attempts
            );
            tokio::time::sleep(delay).await;
            documents = retry_documents;
        }

        if permanent_rejections > 0 {
            let reason = format!(
                "Elasticsearch rejected {permanent_rejections} of {total_documents} documents in index '{}'; indexed {documents_indexed}",
                self.config.index
            );
            error!("{reason}");
            return Err(Error::PermanentHttpError(reason));
        }

        Ok(documents_indexed)
    }
}

/// Prepends the keyword templates for header `data` to
/// `body.mappings.dynamic_templates`, except those whose name is there.
/// Returns `false` when `body` has a shape that cannot hold them.
///
/// Dynamic mapping pins a field type on first write, so a date-like header
/// value would make every later non-date value of that header fail to index.
/// Elasticsearch applies the first matching template, so these go first.
///
/// Each template names its `match_mapping_type`. Without one, a header key
/// such as `x.data` expands to an object at `_iggy_headers.x.data`, the
/// template maps that object as keyword, and Elasticsearch rejects the
/// document. Date detection reports a date-like string as `date`, not
/// `string`, so a `string` template alone misses those values.
fn prepend_header_data_template(body: &mut serde_json::Value) -> bool {
    let Some(root) = body.as_object_mut() else {
        return false;
    };
    let Some(mappings) = root
        .entry("mappings")
        .or_insert_with(|| serde_json::json!({}))
        .as_object_mut()
    else {
        return false;
    };
    let Some(templates) = mappings
        .entry("dynamic_templates")
        .or_insert_with(|| serde_json::json!([]))
        .as_array_mut()
    else {
        return false;
    };
    let missing: Vec<serde_json::Value> = HEADER_DATA_TEMPLATES
        .iter()
        .filter(|(name, _)| {
            !templates
                .iter()
                .any(|template| template.get(*name).is_some())
        })
        .map(|(name, mapping_type)| header_data_template(name, mapping_type))
        .collect();
    templates.splice(0..0, missing);
    true
}

fn header_data_template(name: &str, mapping_type: &str) -> serde_json::Value {
    let mut template = serde_json::Map::new();
    template.insert(
        name.to_owned(),
        serde_json::json!({
            "path_match": HEADER_DATA_PATH_MATCH,
            "match_mapping_type": mapping_type,
            "mapping": {"type": "keyword"}
        }),
    );
    serde_json::Value::Object(template)
}

/// Writes `headers` to `_iggy_headers`. A message without headers loses any
/// `_iggy_headers` field from its payload, so that a payload value cannot pin
/// the field type that later headers need.
fn insert_headers(
    document: &mut Object,
    headers: Option<&BTreeMap<HeaderKey, HeaderValue>>,
) -> Result<(), simd_json::Error> {
    let Some(headers) = headers.filter(|headers| !headers.is_empty()) else {
        document.remove(HEADERS_FIELD);
        return Ok(());
    };
    match simd_json::serde::to_owned_value(JsonHeaders(headers)) {
        Ok(value) => {
            document.insert(HEADERS_FIELD.to_owned(), value);
            Ok(())
        }
        Err(error) => {
            document.remove(HEADERS_FIELD);
            Err(error)
        }
    }
}

/// The document a payload indexes as. `None` skips the message.
fn document_from_payload(payload: Payload, schema: Schema) -> Option<OwnedValue> {
    let payload = payload.into_json_document();
    match payload {
        Payload::Json(value) => Some(value),
        Payload::Raw(bytes) => {
            let mut bytes_copy = bytes.clone();
            match simd_json::from_slice::<OwnedValue>(&mut bytes_copy) {
                Ok(value) => Some(value),
                Err(_) => Some(simd_json::json!({
                    "data": general_purpose::STANDARD.encode(&bytes),
                    "data_type": "raw"
                })),
            }
        }
        Payload::Text(text) | Payload::Proto(text) => Some(simd_json::json!({
            "text": text,
            "data_type": "text"
        })),
        _ => {
            warn!("Unsupported payload format: {schema}");
            None
        }
    }
}

#[async_trait]
impl Sink for ElasticsearchSink {
    async fn open(&mut self) -> Result<(), Error> {
        info!(
            "Opening Elasticsearch sink connector with ID: {} for URL: {}, index: {}",
            self.id, self.config.url, self.config.index
        );

        if self.config.batch_size.is_some() {
            warn!(
                "Elasticsearch plugin_config.batch_size is ignored; use streams[].batch_length to control the input batch size"
            );
        }
        let client = self.create_client().await?;
        self.ensure_index_exists(&client).await?;
        self.client = Some(client);

        info!(
            "Successfully opened Elasticsearch sink connector with ID: {}",
            self.id
        );
        Ok(())
    }

    async fn consume(
        &self,
        topic_metadata: &TopicMetadata,
        messages_metadata: MessagesMetadata,
        messages: Vec<ConsumedMessage>,
    ) -> Result<(), Error> {
        let mut state = self.state.lock().await;
        state.invocations_count += 1;
        let invocation = state.invocations_count;
        drop(state);

        info!(
            "Elasticsearch sink with ID: {} received: {} messages, schema: {}, stream: {}, topic: {}, partition: {}, offset: {}, invocation: {}",
            self.id,
            messages.len(),
            messages_metadata.schema,
            topic_metadata.stream,
            topic_metadata.topic,
            messages_metadata.partition_id,
            messages_metadata.current_offset,
            invocation
        );

        let client = self
            .client
            .as_ref()
            .ok_or_else(|| Error::Connection("Elasticsearch client not initialized".to_string()))?;

        let messages_count = messages.len();
        let mut documents = Vec::with_capacity(messages_count);
        for message in messages {
            let Some(mut doc) = document_from_payload(message.payload, messages_metadata.schema)
            else {
                continue;
            };

            // Add metadata fields
            if let Some(obj) = doc.as_object_mut() {
                obj.insert("_iggy_offset".to_string(), OwnedValue::from(message.offset));
                obj.insert(
                    "_iggy_stream".to_string(),
                    OwnedValue::from(topic_metadata.stream.as_str()),
                );
                obj.insert(
                    "_iggy_topic".to_string(),
                    OwnedValue::from(topic_metadata.topic.as_str()),
                );
                obj.insert(
                    "_iggy_partition".to_string(),
                    OwnedValue::from(messages_metadata.partition_id),
                );
                obj.insert(
                    "_iggy_timestamp".to_string(),
                    OwnedValue::from(IggyTimestamp::now().as_millis() as i64),
                );

                if self.include_headers
                    && let Err(error) = insert_headers(obj, message.headers.as_ref())
                {
                    warn!(
                        "Elasticsearch sink with ID: {} indexes the message at offset: {} without user headers, error: {error}",
                        self.id, message.offset
                    );
                }
            }

            documents.push(doc);
        }

        if !documents.is_empty() {
            let documents_indexed = self.bulk_index_documents(client, documents).await?;
            info!(
                "Successfully indexed {} documents to Elasticsearch index '{}'",
                documents_indexed, self.config.index
            );
        }

        Ok(())
    }

    async fn close(&mut self) -> Result<(), Error> {
        let state = self.state.lock().await;
        info!(
            "Elasticsearch sink connector with ID: {} is closing. Stats: {} invocations, {} documents indexed, {} errors",
            self.id, state.invocations_count, state.documents_indexed, state.errors_count
        );
        drop(state);

        self.client = None;
        info!(
            "Elasticsearch sink connector with ID: {} is closed.",
            self.id
        );
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use iggy_connector_testing::headers::{header_case_map, header_cases};
    use serde_json::json;
    use wiremock::matchers::{method, path};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    use super::*;

    fn test_config() -> ElasticsearchSinkConfig {
        ElasticsearchSinkConfig {
            url: "http://localhost:9200".to_string(),
            index: "test".to_string(),
            username: None,
            password: None,
            batch_size: None,
            timeout_seconds: Some(1),
            max_retries: None,
            retry_delay: None,
            retry_max_delay: None,
            create_index_if_not_exists: Some(false),
            index_mapping: None,
            include_headers: Some(true),
        }
    }

    #[test]
    fn given_omitted_retry_settings_when_loading_should_preserve_defaults() {
        let config = serde_json::from_value(json!({
            "url": "http://localhost:9200",
            "index": "test"
        }))
        .expect("Existing configurations should remain valid");
        let sink = ElasticsearchSink::new(1, config);

        assert_eq!(sink.retry_policy.max_attempts, 3);
        assert_eq!(sink.retry_policy.base_delay, Duration::from_secs(1));
        assert_eq!(sink.retry_policy.max_delay, Duration::from_secs(5));
    }

    #[test]
    fn given_omitted_include_headers_when_loading_should_write_no_headers_or_template() {
        let config = serde_json::from_value(json!({
            "url": "http://localhost:9200",
            "index": "test"
        }))
        .expect("Existing configurations should remain valid");

        let sink = ElasticsearchSink::new(1, config);

        assert!(!sink.include_headers);
        assert_eq!(sink.index_create_body(), None);
    }

    #[test]
    fn given_proto_text_holding_json_when_building_a_document_should_keep_the_original_fields() {
        let payload = Payload::Proto(r#"{"name":"user_1","amount":2.5}"#.to_owned());

        let document = document_from_payload(payload, Schema::Proto)
            .expect("proto text holding JSON is a document");

        assert_eq!(
            document,
            simd_json::json!({"name": "user_1", "amount": 2.5}),
            "the document must keep its fields rather than become a text blob"
        );
    }

    #[test]
    fn given_proto_text_that_is_not_json_when_building_a_document_should_index_it_as_text() {
        let payload = Payload::Proto("name: \"user_1\"".to_owned());

        let document =
            document_from_payload(payload, Schema::Proto).expect("proto text still indexes");

        assert_eq!(
            document,
            simd_json::json!({"text": "name: \"user_1\"", "data_type": "text"})
        );
    }

    #[test]
    fn given_configured_retry_delays_when_loading_should_parse_or_fall_back() {
        for (delay, max_delay, expected_delay, expected_max) in [
            (
                "200ms",
                "2s",
                Duration::from_millis(200),
                Duration::from_secs(2),
            ),
            ("0s", "0s", Duration::ZERO, Duration::ZERO),
            (
                "invalid",
                "invalid",
                Duration::from_secs(1),
                Duration::from_secs(1),
            ),
        ] {
            let config = ElasticsearchSinkConfig {
                retry_delay: Some(delay.to_string()),
                retry_max_delay: Some(max_delay.to_string()),
                ..test_config()
            };
            let sink = ElasticsearchSink::new(1, config);

            assert_eq!(sink.retry_policy.base_delay, expected_delay, "{delay}");
            assert_eq!(sink.retry_policy.max_delay, expected_max, "{max_delay}");
        }
    }

    #[test]
    fn given_bulk_item_results_when_indexing_should_fail_any_rejected_batch() {
        let runtime = tokio::runtime::Runtime::new().expect("test runtime should start");
        runtime.block_on(async {
            let accepted = json!({"index": {"status": 201}});
            let rejected = json!({"index": {
                "status": 400,
                "error": {"type": "document_parsing_exception", "reason": "invalid document"}
            }});
            for (items, expected_indexed) in [
                (vec![rejected.clone(), rejected.clone()], 0),
                (vec![accepted.clone(), rejected], 1),
                (vec![accepted.clone(), accepted], 2),
            ] {
                let server = MockServer::start().await;
                Mock::given(method("POST"))
                    .and(path("/_bulk"))
                    .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                        "errors": expected_indexed < items.len(),
                        "items": items,
                    })))
                    .expect(1)
                    .mount(&server)
                    .await;
                let mut config = test_config();
                config.url = server.uri();
                let sink = ElasticsearchSink::new(1, config);
                let client = sink
                    .create_client()
                    .await
                    .expect("client should initialize");
                let result = sink
                    .bulk_index_documents(
                        &client,
                        vec![simd_json::json!({"id": 1}), simd_json::json!({"id": 2})],
                    )
                    .await;

                if expected_indexed < items.len() {
                    assert!(
                        matches!(result, Err(Error::PermanentHttpError(_))),
                        "{result:?}"
                    );
                } else {
                    assert_eq!(result, Ok(expected_indexed));
                }
                let state = sink.state.lock().await;
                assert_eq!(state.documents_indexed, expected_indexed);
                assert_eq!(state.errors_count, items.len() - expected_indexed);
            }
        });
    }

    #[test]
    fn given_transient_item_rejections_when_indexing_should_not_report_a_permanent_error() {
        let runtime = tokio::runtime::Runtime::new().expect("test runtime should start");
        runtime.block_on(async {
            for status in [429, 503] {
                let server = MockServer::start().await;
                Mock::given(method("POST"))
                    .and(path("/_bulk"))
                    .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                        "errors": true,
                        "items": [json!({"index": {
                            "status": status,
                            "error": {"type": "es_rejected_execution_exception"}
                        }})],
                    })))
                    .expect(u64::from(DEFAULT_MAX_RETRIES))
                    .mount(&server)
                    .await;
                let mut config = test_config();
                config.url = server.uri();
                let sink = ElasticsearchSink::new(1, config);
                let client = sink
                    .create_client()
                    .await
                    .expect("client should initialize");

                let result = sink
                    .bulk_index_documents(&client, vec![simd_json::json!({"id": 1})])
                    .await;

                assert!(
                    matches!(result, Err(Error::CannotStoreData(_))),
                    "status {status}: {result:?}"
                );
            }
        });
    }

    #[test]
    fn given_configured_attempts_when_indexing_should_limit_transient_retries() {
        let runtime = tokio::runtime::Runtime::new().expect("test runtime should start");
        runtime.block_on(async {
            for (max_retries, expected_attempts) in [(0, 1), (1, 1), (2, 2), (4, 4)] {
                let server = MockServer::start().await;
                Mock::given(method("POST"))
                    .and(path("/_bulk"))
                    .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                        "items": [{"index": {"status": 429}}]
                    })))
                    .expect(expected_attempts)
                    .mount(&server)
                    .await;
                let mut config = serde_json::to_value(test_config()).expect("serialize config");
                config["url"] = json!(server.uri());
                config["max_retries"] = json!(max_retries);
                config["retry_delay"] = json!("1ms");
                config["retry_max_delay"] = json!("2ms");
                let config = serde_json::from_value(config).expect("deserialize config");
                let sink = ElasticsearchSink::new(1, config);
                let client = sink
                    .create_client()
                    .await
                    .expect("client should initialize");

                let result = sink
                    .bulk_index_documents(&client, vec![simd_json::json!({"id": 1})])
                    .await;

                assert!(
                    matches!(result, Err(Error::CannotStoreData(_))),
                    "{result:?}"
                );
                let state = sink.state.lock().await;
                assert_eq!(state.documents_indexed, 0);
                assert_eq!(state.errors_count, 1);
            }
        });
    }

    #[test]
    fn given_a_bulk_response_without_items_when_indexing_should_report_a_connection_error() {
        let runtime = tokio::runtime::Runtime::new().expect("test runtime should start");
        runtime.block_on(async {
            let server = MockServer::start().await;
            Mock::given(method("POST"))
                .and(path("/_bulk"))
                .respond_with(ResponseTemplate::new(200).set_body_json(json!({"took": 1})))
                .expect(1)
                .mount(&server)
                .await;
            let mut config = test_config();
            config.url = server.uri();
            let sink = ElasticsearchSink::new(1, config);
            let client = sink
                .create_client()
                .await
                .expect("client should initialize");

            let result = sink
                .bulk_index_documents(&client, vec![simd_json::json!({"id": 1})])
                .await;

            assert!(matches!(result, Err(Error::Connection(_))), "{result:?}");
            let state = sink.state.lock().await;
            assert_eq!(state.documents_indexed, 0);
            assert_eq!(state.errors_count, 0);
        });
    }

    #[test]
    fn given_mixed_bulk_results_when_retrying_should_send_only_transient_rejections() {
        let runtime = tokio::runtime::Runtime::new().expect("test runtime should start");
        runtime.block_on(async {
            for permanent_rejection in [false, true] {
                let server = MockServer::start().await;
                let last_status = if permanent_rejection { 400 } else { 201 };
                Mock::given(method("POST"))
                    .and(path("/_bulk"))
                    .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                        "items": [
                            {"index": {"status": 201}},
                            {"index": {"status": 429, "error": {"type": "es_rejected_execution_exception"}}},
                            {"index": {"status": last_status}},
                        ]
                    })))
                    .up_to_n_times(1)
                    .expect(1)
                    .mount(&server)
                    .await;
                Mock::given(method("POST"))
                    .and(path("/_bulk"))
                    .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                        "items": [{"index": {"status": 201}}]
                    })))
                    .expect(1)
                    .mount(&server)
                    .await;
                let mut config = test_config();
                config.url = server.uri();
                let sink = ElasticsearchSink::new(1, config);
                let client = sink.create_client().await.unwrap();
                let documents = vec![
                    simd_json::json!({"id": 1}),
                    simd_json::json!({"id": 2}),
                    simd_json::json!({"id": 3}),
                ];
                let result = sink.bulk_index_documents(&client, documents).await;
                if permanent_rejection {
                    assert!(matches!(result, Err(Error::PermanentHttpError(_))), "{result:?}");
                } else {
                    assert_eq!(result, Ok(3));
                }
                let requests = server.received_requests().await.unwrap();
                assert_eq!(requests.len(), 2);
                let retry_body: Vec<serde_json::Value> = requests[1].body
                    .split(|byte| *byte == b'\n')
                    .filter(|line| !line.is_empty())
                    .map(|line| serde_json::from_slice(line).unwrap())
                    .collect();
                assert_eq!(retry_body, vec![json!({"index": {"_index": "test"}}), json!({"id": 2})]);
                let state = sink.state.lock().await;
                assert_eq!(state.documents_indexed, if permanent_rejection { 2 } else { 3 });
                assert_eq!(state.errors_count, usize::from(permanent_rejection));
            }
        });
    }

    #[test]
    fn given_ambiguous_bulk_response_when_indexing_should_fail_without_replaying() {
        let runtime = tokio::runtime::Runtime::new().expect("test runtime should start");
        runtime.block_on(async {
            for (status, body) in [
                (503, json!({"error": "unavailable"})),
                (200, json!({"items": []})),
                (
                    200,
                    json!({"items": [{"index": {"error": "missing status"}}]}),
                ),
            ] {
                let server = MockServer::start().await;
                Mock::given(method("POST"))
                    .and(path("/_bulk"))
                    .respond_with(ResponseTemplate::new(status).set_body_json(body))
                    .expect(1)
                    .mount(&server)
                    .await;
                let mut config = test_config();
                config.url = server.uri();
                let sink = ElasticsearchSink::new(1, config);
                let client = sink.create_client().await.unwrap();
                let result = sink
                    .bulk_index_documents(&client, vec![simd_json::json!({"id": 1})])
                    .await;
                assert!(matches!(result, Err(Error::Connection(_))), "{result:?}");
            }
        });
    }

    #[test]
    fn given_empty_batch_when_indexing_should_succeed_without_a_request() {
        let runtime = tokio::runtime::Runtime::new().expect("test runtime should start");
        runtime.block_on(async {
            let server = MockServer::start().await;
            let mut config = test_config();
            config.url = server.uri();
            let sink = ElasticsearchSink::new(1, config);
            let client = sink
                .create_client()
                .await
                .expect("client should initialize");

            assert_eq!(sink.bulk_index_documents(&client, Vec::new()).await, Ok(0));
            assert!(
                server
                    .received_requests()
                    .await
                    .expect("requests should be recorded")
                    .is_empty()
            );
        });
    }

    fn header_templates() -> Vec<serde_json::Value> {
        vec![
            json!({"iggy_header_data": {
                "path_match": HEADER_DATA_PATH_MATCH,
                "match_mapping_type": "string",
                "mapping": {"type": "keyword"}
            }}),
            json!({"iggy_header_data_date": {
                "path_match": HEADER_DATA_PATH_MATCH,
                "match_mapping_type": "date",
                "mapping": {"type": "keyword"}
            }}),
        ]
    }

    fn document_with_headers(
        mut document: OwnedValue,
        headers: Option<&BTreeMap<HeaderKey, HeaderValue>>,
    ) -> serde_json::Value {
        let object = document
            .as_object_mut()
            .expect("test document is an object");
        insert_headers(object, headers).expect("headers should encode");
        iggy_connector_sdk::owned_value_to_serde_json(&document)
    }

    fn json_message(
        document: serde_json::Value,
        headers: Option<BTreeMap<HeaderKey, HeaderValue>>,
    ) -> ConsumedMessage {
        let mut bytes = serde_json::to_vec(&document).expect("serialize test document");
        ConsumedMessage {
            id: 1,
            offset: 7,
            checksum: 0,
            timestamp: 0,
            origin_timestamp: 0,
            headers,
            payload: Payload::Json(
                simd_json::to_owned_value(&mut bytes).expect("test document is JSON"),
            ),
        }
    }

    /// Consumes one message against a mock `_bulk` endpoint and returns the
    /// document that the sink sent.
    async fn consume_and_capture(
        config: ElasticsearchSinkConfig,
        message: ConsumedMessage,
    ) -> serde_json::Value {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/_bulk"))
            .respond_with(
                ResponseTemplate::new(200)
                    .set_body_json(json!({"items": [{"index": {"status": 201}}]})),
            )
            .expect(1)
            .mount(&server)
            .await;
        let mut sink = ElasticsearchSink::new(
            1,
            ElasticsearchSinkConfig {
                url: server.uri(),
                ..config
            },
        );
        sink.client = Some(
            sink.create_client()
                .await
                .expect("client should initialize"),
        );

        sink.consume(
            &TopicMetadata {
                stream: "stream".to_owned(),
                topic: "topic".to_owned(),
            },
            MessagesMetadata {
                partition_id: 0,
                current_offset: 7,
                schema: Schema::Json,
            },
            vec![message],
        )
        .await
        .expect("consume should succeed");

        let requests = server
            .received_requests()
            .await
            .expect("requests should be recorded");
        let body = String::from_utf8(requests[0].body.clone()).expect("bulk body is UTF-8");
        let document_line = body.lines().nth(1).expect("bulk body has a document line");
        serde_json::from_str(document_line).expect("document line is JSON")
    }

    #[test]
    fn given_shared_header_cases_when_inserting_headers_should_write_expected_names() {
        let cases = header_cases();

        let document = document_with_headers(simd_json::json!({}), Some(&header_case_map()));

        let headers = document[HEADERS_FIELD]
            .as_object()
            .expect("headers are an object");
        assert_eq!(headers.len(), cases.len());
        for case in &cases {
            assert_eq!(
                headers.get(&case.expected_name),
                Some(&json!({"data": case.label, "data_encoding": "utf8"})),
                "case {}",
                case.label
            );
        }
    }

    #[test]
    fn given_payload_headers_field_when_message_has_no_headers_should_remove_it() {
        let payload = simd_json::json!({"id": 1, "_iggy_headers": "2024-01-01"});
        let empty = BTreeMap::new();

        for headers in [None, Some(&empty)] {
            let document = document_with_headers(payload.clone(), headers);

            assert_eq!(document, json!({"id": 1}), "{headers:?}");
        }
    }

    #[test]
    fn given_no_index_mapping_when_building_create_body_should_carry_header_templates() {
        let sink = ElasticsearchSink::new(1, test_config());

        assert_eq!(
            sink.index_create_body(),
            Some(json!({"mappings": {"dynamic_templates": header_templates()}}))
        );
    }

    #[test]
    fn given_index_mapping_when_building_create_body_should_prepend_header_templates() {
        let user_template =
            json!({"strings": {"match_mapping_type": "string", "mapping": {"type": "text"}}});
        let sink = ElasticsearchSink::new(
            1,
            ElasticsearchSinkConfig {
                index_mapping: Some(json!({
                    "settings": {"number_of_shards": 1},
                    "mappings": {
                        "dynamic_templates": [user_template.clone()],
                        "properties": {"name": {"type": "keyword"}}
                    }
                })),
                ..test_config()
            },
        );

        let mut body = sink.index_create_body().expect("body is present");

        assert_eq!(
            body,
            json!({
                "settings": {"number_of_shards": 1},
                "mappings": {
                    "dynamic_templates": ([header_templates(), vec![user_template]].concat()),
                    "properties": {"name": {"type": "keyword"}}
                }
            })
        );
        let before = body.clone();
        assert!(prepend_header_data_template(&mut body));
        assert_eq!(
            body, before,
            "a second prepend must not duplicate the template"
        );
    }

    #[test]
    fn given_headers_disabled_when_building_create_body_should_keep_index_mapping() {
        let mapping = json!({"mappings": {"properties": {"name": {"type": "keyword"}}}});
        for index_mapping in [None, Some(mapping)] {
            let sink = ElasticsearchSink::new(
                1,
                ElasticsearchSinkConfig {
                    include_headers: Some(false),
                    index_mapping: index_mapping.clone(),
                    ..test_config()
                },
            );

            assert_eq!(sink.index_create_body(), index_mapping);
        }
    }

    #[test]
    fn given_index_mapping_of_unexpected_shape_when_building_create_body_should_send_it_unchanged()
    {
        for mapping in [
            json!([]),
            json!({"mappings": "not an object"}),
            json!({"mappings": {"dynamic_templates": {}}}),
        ] {
            let sink = ElasticsearchSink::new(
                1,
                ElasticsearchSinkConfig {
                    index_mapping: Some(mapping.clone()),
                    ..test_config()
                },
            );

            assert_eq!(sink.index_create_body(), Some(mapping));
        }
    }

    #[test]
    fn given_missing_index_when_creating_it_should_send_the_header_templates() {
        let runtime = tokio::runtime::Runtime::new().expect("test runtime should start");
        runtime.block_on(async {
            let server = MockServer::start().await;
            Mock::given(method("HEAD"))
                .and(path("/test"))
                .respond_with(ResponseTemplate::new(404))
                .mount(&server)
                .await;
            Mock::given(method("PUT"))
                .and(path("/test"))
                .respond_with(
                    ResponseTemplate::new(200).set_body_json(json!({"acknowledged": true})),
                )
                .expect(1)
                .mount(&server)
                .await;
            let sink = ElasticsearchSink::new(
                1,
                ElasticsearchSinkConfig {
                    url: server.uri(),
                    create_index_if_not_exists: Some(true),
                    ..test_config()
                },
            );
            let client = sink
                .create_client()
                .await
                .expect("client should initialize");

            sink.ensure_index_exists(&client)
                .await
                .expect("index creation should succeed");

            let requests = server
                .received_requests()
                .await
                .expect("requests should be recorded");
            let create = requests
                .iter()
                .find(|request| request.method.as_str() == "PUT")
                .expect("a create request was sent");
            let body: serde_json::Value =
                serde_json::from_slice(&create.body).expect("create body is JSON");
            assert_eq!(
                body,
                json!({"mappings": {"dynamic_templates": header_templates()}})
            );
        });
    }

    #[test]
    fn given_headers_enabled_when_consuming_should_write_header_names() {
        let runtime = tokio::runtime::Runtime::new().expect("test runtime should start");
        runtime.block_on(async {
            let mut headers = BTreeMap::new();
            headers.insert(
                HeaderKey::try_from("a").expect("valid key"),
                HeaderValue::try_from("b").expect("valid value"),
            );
            headers.insert(HeaderKey::from(1i32), HeaderValue::from(2i32));

            let document =
                consume_and_capture(test_config(), json_message(json!({"id": 1}), Some(headers)))
                    .await;

            assert_eq!(
                document[HEADERS_FIELD],
                json!({
                    "a": {"data": "b", "data_encoding": "utf8"},
                    "~int32:1": {"data": "2", "data_encoding": "utf8"}
                })
            );
        });
    }

    #[test]
    fn given_headers_disabled_when_consuming_should_leave_payload_untouched() {
        let runtime = tokio::runtime::Runtime::new().expect("test runtime should start");
        runtime.block_on(async {
            let mut headers = BTreeMap::new();
            headers.insert(
                HeaderKey::try_from("a").expect("valid key"),
                HeaderValue::try_from("b").expect("valid value"),
            );

            let document = consume_and_capture(
                ElasticsearchSinkConfig {
                    include_headers: Some(false),
                    ..test_config()
                },
                json_message(
                    json!({"id": 1, "_iggy_headers": "from payload"}),
                    Some(headers),
                ),
            )
            .await;

            assert_eq!(document[HEADERS_FIELD], json!("from payload"));
        });
    }
}
