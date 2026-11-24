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

use std::io::Read;

use bytes::Bytes;
use flate2::read::GzDecoder;
use iggy::prelude::{IggyMessage, Partitioning};
use iggy_common::Identifier;
use iggy_common::MessageClient;
use iggy_connector_s3_sink::Compression;
use iggy_connector_sdk::api::{ConnectorStatus, SinkInfoResponse};
use integration::harness::{TestHarness, seeds};
use integration::iggy_harness;
use reqwest::Client;
use s3::{
    command::Command,
    request::{Request, tokio_backend::ReqwestRequest},
};

use crate::fixtures::{
    S3SinkFixture, S3SinkGzipFixture, S3SinkOps, S3SinkRotationFixture, S3SinkZstdFixture,
};
use iggy_connector_testing::create_test_messages;

const API_KEY: &str = "test-api-key";
const S3_SINK_KEY: &str = "s3";

#[iggy_harness(
    server(connectors_runtime(config_path = "tests/s3/sink.toml")),
    seed = seeds::connector_stream
)]
async fn s3_sink_initializes_and_runs(harness: &TestHarness, fixture: S3SinkFixture) {
    let api_address = harness
        .connectors_runtime()
        .expect("connector runtime should be available")
        .http_url();
    let http_client = Client::new();

    let response = http_client
        .get(format!("{}/sinks", api_address))
        .header("api-key", API_KEY)
        .send()
        .await
        .expect("Failed to get sinks");

    assert_eq!(response.status(), 200);
    let sinks: Vec<SinkInfoResponse> = response.json().await.expect("Failed to parse sinks");

    assert_eq!(sinks.len(), 1);
    assert_eq!(sinks[0].key, S3_SINK_KEY);
    assert!(sinks[0].enabled);
    assert_eq!(sinks[0].status, ConnectorStatus::Running);

    let keys = fixture.list_objects("").await.expect("List data objects");
    assert!(
        keys.is_empty(),
        "Startup must not publish probe objects: {keys:?}"
    );
    // Floci 2.1.0 omits IsTruncated from empty listings, which rust-s3 requires.
    let uploads_request = ReqwestRequest::new(
        fixture.bucket(),
        "/",
        Command::ListMultipartUploads {
            prefix: None,
            delimiter: None,
            key_marker: None,
            upload_id_marker: None,
            max_uploads: None,
        },
    )
    .await
    .expect("Prepare multipart listing");
    let uploads_response = uploads_request
        .response_data(false)
        .await
        .expect("List multipart uploads");
    assert_eq!(uploads_response.status_code(), 200);
    let uploads_xml = uploads_response.as_str().expect("Read multipart uploads");
    assert!(
        !uploads_xml.contains("<Upload"),
        "Startup must abort its multipart probe: {uploads_xml}"
    );

    drop(fixture);
}

#[iggy_harness(
    server(connectors_runtime(config_path = "tests/s3/sink.toml")),
    seed = seeds::connector_stream
)]
async fn s3_sink_writes_jsonl_with_correct_layout(harness: &TestHarness, fixture: S3SinkFixture) {
    let client = harness.root_client().await.unwrap();
    let stream_id: Identifier = seeds::names::STREAM.try_into().unwrap();
    let topic_id: Identifier = seeds::names::TOPIC.try_into().unwrap();

    let message_count = 5;
    let test_messages = create_test_messages(message_count);
    let payloads: Vec<Bytes> = test_messages
        .iter()
        .map(|m| Bytes::from(serde_json::to_vec(m).expect("serialize")))
        .collect();

    let mut messages: Vec<IggyMessage> = payloads
        .iter()
        .enumerate()
        .map(|(i, p)| {
            IggyMessage::builder()
                .id((i + 1) as u128)
                .payload(p.clone())
                .build()
                .expect("build message")
        })
        .collect();

    client
        .send_messages(
            &stream_id,
            &topic_id,
            &Partitioning::partition_id(0),
            &mut messages,
        )
        .await
        .expect("send messages");

    let prefix = format!("{}/", seeds::names::STREAM);
    let keys = fixture
        .wait_for_objects(&prefix, 1)
        .await
        .expect("wait for S3 objects");

    assert!(!keys.is_empty(), "Expected at least one S3 object");

    let key = &keys[0];
    assert!(
        key.contains(seeds::names::STREAM),
        "Key must contain stream name: {key}"
    );
    assert!(
        key.contains(seeds::names::TOPIC),
        "Key must contain topic name: {key}"
    );
    assert!(key.ends_with(".jsonl"), "Key must end with .jsonl: {key}");
    assert!(
        key.contains("/00000-"),
        "Key must contain partition_id (00000): {key}"
    );

    let data = fixture.get_object(key).await.expect("get S3 object");
    let content = String::from_utf8(data).expect("valid utf8");
    let lines: Vec<&str> = content.trim().lines().collect();
    assert_eq!(
        lines.len(),
        message_count,
        "Expected {message_count} lines in JSONL output"
    );

    for line in &lines {
        let value: serde_json::Value = serde_json::from_str(line).expect("valid JSON line");
        assert!(value.get("offset").is_some(), "Line must have offset");
        assert!(value.get("timestamp").is_some(), "Line must have timestamp");
        assert!(value.get("stream").is_some(), "Line must have stream");
        assert!(value.get("payload").is_some(), "Line must have payload");
    }
}

#[iggy_harness(
    server(connectors_runtime(config_path = "tests/s3/sink_rotation.toml")),
    seed = seeds::connector_stream
)]
async fn s3_sink_rotates_on_message_count(harness: &TestHarness, fixture: S3SinkRotationFixture) {
    let client = harness.root_client().await.unwrap();
    let stream_id: Identifier = seeds::names::STREAM.try_into().unwrap();
    let topic_id: Identifier = seeds::names::TOPIC.try_into().unwrap();

    let message_count = 25;
    let test_messages = create_test_messages(message_count);
    let payloads: Vec<Bytes> = test_messages
        .iter()
        .map(|m| Bytes::from(serde_json::to_vec(m).expect("serialize")))
        .collect();

    let mut messages: Vec<IggyMessage> = payloads
        .iter()
        .enumerate()
        .map(|(i, p)| {
            IggyMessage::builder()
                .id((i + 1) as u128)
                .payload(p.clone())
                .build()
                .expect("build message")
        })
        .collect();

    client
        .send_messages(
            &stream_id,
            &topic_id,
            &Partitioning::partition_id(0),
            &mut messages,
        )
        .await
        .expect("send messages");

    let prefix = format!("{}/", seeds::names::STREAM);
    let keys = fixture
        .wait_for_objects(&prefix, 2)
        .await
        .expect("wait for rotated S3 objects");

    assert!(
        keys.len() >= 2,
        "Expected at least 2 S3 objects from rotation (max_messages_per_file=10, sent 25), got {}",
        keys.len()
    );

    for key in &keys {
        assert!(key.ends_with(".jsonl"), "All keys must end with .jsonl");
        assert!(
            key.contains("/00000-"),
            "All keys must contain partition_id"
        );
    }
}

#[iggy_harness(
    server(connectors_runtime(config_path = "tests/s3/sink.toml")),
    seed = seeds::connector_stream
)]
async fn s3_sink_writes_gzip_jsonl(harness: &TestHarness, fixture: S3SinkGzipFixture) {
    assert_compressed_jsonl(harness, &fixture, Compression::Gzip).await;
}

#[iggy_harness(
    server(connectors_runtime(config_path = "tests/s3/sink.toml")),
    seed = seeds::connector_stream
)]
async fn s3_sink_writes_zstd_jsonl(harness: &TestHarness, fixture: S3SinkZstdFixture) {
    assert_compressed_jsonl(harness, &fixture, Compression::Zstd).await;
}

async fn assert_compressed_jsonl(
    harness: &TestHarness,
    fixture: &impl S3SinkOps,
    compression: Compression,
) {
    let (encoding, suffix) = match compression {
        Compression::None => (None, ".jsonl"),
        Compression::Gzip => (Some("gzip"), ".jsonl.gz"),
        Compression::Zstd => (Some("zstd"), ".jsonl.zst"),
    };
    let client = harness.root_client().await.unwrap();
    let expected = create_test_messages(10);
    let mut messages: Vec<IggyMessage> = expected
        .iter()
        .enumerate()
        .map(|(index, payload)| {
            IggyMessage::builder()
                .id((index + 1) as u128)
                .payload(Bytes::from(serde_json::to_vec(payload).unwrap()))
                .build()
                .unwrap()
        })
        .collect();
    client
        .send_messages(
            &seeds::names::STREAM.try_into().unwrap(),
            &seeds::names::TOPIC.try_into().unwrap(),
            &Partitioning::partition_id(0),
            &mut messages,
        )
        .await
        .expect("Send messages");

    let prefix = format!("{}/", seeds::names::STREAM);
    let mut keys = fixture
        .wait_for_objects(&prefix, 2)
        .await
        .expect("Wait for compressed objects");
    keys.sort();
    assert_eq!(
        keys.len(),
        2,
        "Message-count rotation should produce two files"
    );

    // Read encoded bytes even if another dependency enables automatic HTTP decompression.
    let http_client = Client::builder()
        .no_gzip()
        .no_zstd()
        .no_brotli()
        .no_deflate()
        .build()
        .unwrap();
    let mut records = Vec::new();
    for key in keys {
        assert!(key.ends_with(suffix), "Unexpected object extension: {key}");
        let url = fixture
            .bucket()
            .presign_get(&key, 60, None)
            .await
            .expect("Sign object download");
        let response = http_client
            .get(url)
            .send()
            .await
            .expect("Download compressed object");
        assert_eq!(response.status(), 200);
        let content_encoding = response
            .headers()
            .get("content-encoding")
            .map(|value| value.to_str().unwrap().to_owned());
        assert_eq!(content_encoding.as_deref(), encoding);
        let data = response.bytes().await.unwrap();
        let decoded = decode(compression, &data);
        let content = std::str::from_utf8(&decoded).unwrap();
        let lines: Vec<_> = content.lines().collect();
        assert_eq!(
            lines.len(),
            5,
            "Each file must contain the complete rotated batch"
        );
        for line in lines {
            records.push(serde_json::from_str::<serde_json::Value>(line).unwrap());
        }
    }
    records.sort_by_key(|record| record["offset"].as_u64().unwrap());
    assert_eq!(records.len(), expected.len());
    for (offset, (record, payload)) in records.iter().zip(expected).enumerate() {
        assert_eq!(record["offset"], offset as u64);
        assert_eq!(record["partition_id"], 0);
        assert_eq!(record["stream"], seeds::names::STREAM);
        assert_eq!(record["topic"], seeds::names::TOPIC);
        assert_eq!(record["payload"], serde_json::to_value(payload).unwrap());
    }
}

fn decode(compression: Compression, data: &[u8]) -> Vec<u8> {
    match compression {
        Compression::None => data.to_vec(),
        Compression::Gzip => {
            let mut decoded = Vec::new();
            GzDecoder::new(data).read_to_end(&mut decoded).unwrap();
            decoded
        }
        Compression::Zstd => zstd::stream::decode_all(data).unwrap(),
    }
}
