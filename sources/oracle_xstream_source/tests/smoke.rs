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

//! Runs the source against a real XStream outbound server. Ignored by default: it needs a licensed Oracle
//! database, Instant Client and rows written to the captured tables while it runs.

use std::collections::HashSet;
use std::env;
use std::time::{Duration, Instant};

use iggy_connector_oracle_xstream_source::{OracleSource, OracleSourceConfig};
use iggy_connector_sdk::source::SourceBatchResult;
use iggy_connector_sdk::{ConnectorState, Source};
use serde_json::{Value, json};

const FIRST_RUN_DEADLINE: Duration = Duration::from_secs(120);
const RESTART_OBSERVATION: Duration = Duration::from_secs(20);

fn setting(name: &str) -> String {
    env::var(name).unwrap_or_else(|_| panic!("set {name}, see the README"))
}

fn config() -> OracleSourceConfig {
    serde_json::from_value(json!({
        "connect_string": setting("ORACLE_XSTREAM_CONNECT"),
        "username": setting("ORACLE_XSTREAM_USER"),
        "password": setting("ORACLE_XSTREAM_PASSWORD"),
        "outbound_server": setting("ORACLE_XSTREAM_SERVER"),
        "oci_library_path": setting("ORACLE_XSTREAM_LIBRARY"),
        "poll_interval": "50ms",
        "poll_budget": "1s",
    }))
    .expect("valid source configuration")
}

#[tokio::test]
#[ignore = "needs a licensed Oracle with an XStream outbound server, see the README"]
async fn given_changes_on_captured_tables_when_polling_should_deliver_them_once_across_a_restart() {
    let expected: usize = env::var("ORACLE_XSTREAM_EXPECT_ROWS")
        .ok()
        .and_then(|text| text.parse().ok())
        .unwrap_or(1);

    let mut source = OracleSource::new(1, config(), None);
    source
        .open()
        .await
        .expect("open attaches to the outbound server");
    let mut ids = HashSet::new();
    let mut state: Option<ConnectorState> = None;
    let deadline = Instant::now() + FIRST_RUN_DEADLINE;
    while ids.len() < expected && Instant::now() < deadline {
        let batch = source.poll().await.expect("poll");
        for message in &batch.messages {
            let event: Value = serde_json::from_slice(&message.payload).expect("JSON payload");
            println!("{event}");
            assert!(
                event["table"].is_string() && event["op"].is_string(),
                "{event}"
            );
            assert!(
                ids.insert(message.id.expect("message id")),
                "duplicate id inside one run"
            );
        }
        if let Some(batch_state) = batch.state {
            state = Some(batch_state);
        }
        source
            .on_batch_result(SourceBatchResult::Ack)
            .await
            .expect("ack");
    }
    assert_eq!(
        ids.len(),
        expected,
        "expected {expected} changes within {FIRST_RUN_DEADLINE:?}"
    );
    source.close().await.expect("close detaches");

    let mut restarted = OracleSource::new(1, config(), state);
    restarted.open().await.expect("reopen with the saved state");
    let observation = Instant::now() + RESTART_OBSERVATION;
    while Instant::now() < observation {
        let batch = restarted.poll().await.expect("poll after restart");
        for message in &batch.messages {
            assert!(
                !ids.contains(&message.id.expect("message id")),
                "an acknowledged change was delivered again"
            );
        }
        restarted
            .on_batch_result(SourceBatchResult::Ack)
            .await
            .expect("ack");
    }
    restarted.close().await.expect("close");
}
