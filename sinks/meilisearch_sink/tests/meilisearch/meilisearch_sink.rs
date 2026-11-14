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

use crate::fixtures::{MeilisearchOps, MeilisearchSinkFixture, TEST_INDEX};
use bytes::Bytes;
use iggy::prelude::{IggyMessage, Partitioning};
use iggy_common::{HeaderKey, HeaderValue, Identifier, MessageClient};
use integration::harness::seeds;
use integration::iggy_harness;
use std::collections::BTreeMap;

#[iggy_harness(
    server(connectors_runtime(config_path = "tests/meilisearch/sink.toml")),
    seed = seeds::connector_stream
)]
async fn given_json_messages_when_sink_consumes_should_index_documents(
    harness: &TestHarness,
    fixture: MeilisearchSinkFixture,
) {
    let client = harness.root_client().await.unwrap();
    let stream_id: Identifier = seeds::names::STREAM.try_into().unwrap();
    let topic_id: Identifier = seeds::names::TOPIC.try_into().unwrap();

    let payloads = [
        serde_json::json!({"name": "first", "category": "alpha"}),
        serde_json::json!({"name": "second", "category": "beta"}),
    ];

    let mut messages = payloads
        .iter()
        .enumerate()
        .map(|(i, payload)| {
            IggyMessage::builder()
                .id((i + 1) as u128)
                .payload(Bytes::from(serde_json::to_vec(payload).expect("serialize")))
                .build()
                .expect("build message")
        })
        .collect::<Vec<_>>();

    client
        .send_messages(
            &stream_id,
            &topic_id,
            &Partitioning::partition_id(0),
            &mut messages,
        )
        .await
        .expect("send messages");

    let documents = fixture
        .wait_for_documents(TEST_INDEX, payloads.len())
        .await
        .expect("wait for Meilisearch documents");

    assert_eq!(documents.len(), payloads.len());
    assert!(documents.iter().any(|document| document["name"] == "first"));
    assert!(
        documents
            .iter()
            .any(|document| document["name"] == "second")
    );
    assert!(
        documents
            .iter()
            .all(|document| document["iggy_id"].as_str().is_some())
    );
}

#[iggy_harness(
    server(connectors_runtime(config_path = "tests/meilisearch/sink.toml")),
    seed = seeds::connector_stream
)]
async fn given_message_with_user_headers_when_sink_consumes_should_index_them(
    harness: &TestHarness,
    fixture: MeilisearchSinkFixture,
) {
    let client = harness.root_client().await.unwrap();
    let stream_id: Identifier = seeds::names::STREAM.try_into().unwrap();
    let topic_id: Identifier = seeds::names::TOPIC.try_into().unwrap();
    let headers = BTreeMap::from([
        (
            HeaderKey::try_from("a").expect("valid key"),
            HeaderValue::try_from("b").expect("valid value"),
        ),
        (HeaderKey::from(1i32), HeaderValue::from(2i32)),
    ]);
    let mut messages = vec![
        IggyMessage::builder()
            .id(1)
            .payload(Bytes::from(
                serde_json::to_vec(&serde_json::json!({"name": "first"})).expect("serialize"),
            ))
            .user_headers(headers)
            .build()
            .expect("build message"),
    ];

    client
        .send_messages(
            &stream_id,
            &topic_id,
            &Partitioning::partition_id(0),
            &mut messages,
        )
        .await
        .expect("send messages");

    let documents = fixture
        .wait_for_documents(TEST_INDEX, 1)
        .await
        .expect("wait for Meilisearch documents");

    assert_eq!(
        documents[0]["iggy_headers"],
        serde_json::json!({"a": "b", "~int32:1": "2"})
    );
}
