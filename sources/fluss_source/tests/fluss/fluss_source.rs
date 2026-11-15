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

use super::{API_KEY, POLL_ATTEMPTS, POLL_INTERVAL_MS, SOURCE_KEY, STATE_FILE, TEST_ROW_COUNT};
use crate::fixtures::{
    FlussSourceAllTypesFixture, FlussSourceBucketsFixture, FlussSourceExplicitOffsetFixture,
    FlussSourceFixture, FlussSourceLatestFixture, FlussSourceMissingTableFixture,
    FlussSourceOffsetPastTheEndFixture, FlussSourceOutageFixture,
    FlussSourcePartitionedTableFixture, FlussSourcePrimaryKeyTableFixture,
    FlussSourceProjectedFixture, FlussSourceQuickPollFixture, FlussSourceRawStreamFixture,
    FlussSourceReservedColumnFixture, FlussSourceSaslFixture, FlussSourceSlowPollFixture,
    FlussSourceSmallBatchFixture, FlussSourceTieredProjectionFixture,
    FlussSourceUnconvertibleRowFixture, FlussSourceUnreachableFixture,
    FlussSourceWrongPasswordFixture,
};
use iggy::prelude::IggyClient;
use iggy_common::MessageClient;
use iggy_common::poll_messages::DEFAULT_PARTITION_ID;
use iggy_common::{CommitPolicy, Consumer, Identifier, PollOptions, PollingStrategy};
use iggy_connector_sdk::ConnectorState;
use iggy_connector_sdk::api::{
    ConnectorRuntimeStats, ConnectorStats, ConnectorStatus, SourceInfoResponse,
};
use integration::harness::{TestHarness, seeds};
use integration::iggy_harness;
use reqwest::Client;
use serde::Deserialize;
use serde_json::{Value, json};
use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};
use tokio::time::{sleep, timeout};

/// Redelivery waits out the slow poll interval, the rejected batch and the Apache Iggy restart.
const REDELIVERY_ATTEMPTS: usize = POLL_ATTEMPTS * 3;
const SEND_FAILURE_TIMEOUT: Duration = Duration::from_secs(30);
const STATE_FILE_TIMEOUT: Duration = Duration::from_secs(15);
const SOURCE_STATUS_TIMEOUT: Duration = Duration::from_secs(30);
/// Opening against a port nothing listens on waits out the client's own retries first.
const UNREACHABLE_TIMEOUT: Duration = Duration::from_secs(90);
const DELIVERY_TIMEOUT: Duration = Duration::from_secs(30);
/// Far below the time the same rows take when every full batch waits `poll_interval`.
const FULL_BATCHES_TIMEOUT: Duration = Duration::from_secs(40);
const OUTAGE_TIMEOUT: Duration = Duration::from_secs(60);
const RECOVERY_TIMEOUT: Duration = Duration::from_secs(90);
const CRASH_RECOVERY_TIMEOUT: Duration = Duration::from_secs(120);
/// How long an exact count keeps listening for a row delivered twice.
const SETTLE_TIME: Duration = Duration::from_secs(2);
const DRAIN_BATCH: u32 = 1000;
const DRAIN_IDLE_PAUSE: Duration = Duration::from_millis(100);
const BUCKETED_ROW_COUNT: usize = 90;
const MANY_ROW_COUNT: usize = 500;
const CRASH_ROW_COUNT: usize = 3000;
const RESTART_ROUNDS: usize = 5;
const FAILED_POLL_WINDOW: Duration = Duration::from_secs(5);
const POLL_FAILURE: &str = "failed to poll Apache Fluss";
/// Logged by the Apache Fluss client when no server of the cluster answers.
const CLUSTER_UNREACHABLE: &str = "Unable to initialize cluster from bootstrap servers";
const STATE_SAVE_FAILURE: &str = "Failed to save state for source connector";
const NACK_LIMIT: &str = "consecutive NACK limit reached";
const EARLIEST_OFFSET: i64 = -2;

#[derive(Debug, Deserialize)]
struct FlussRecord {
    id: i32,
    payload: String,
    #[serde(rename = "_fluss_bucket")]
    bucket: i32,
    #[serde(rename = "_fluss_offset")]
    offset: i64,
    #[serde(rename = "_fluss_timestamp")]
    timestamp: i64,
    /// The Apache Iggy message id, read from the message header rather than the payload.
    #[serde(skip)]
    message_id: u128,
}

/// The persisted part of the source state. MessagePack stores the struct as an array, so the
/// fields keep the order of the connector's own `State`.
#[derive(Debug, Deserialize)]
struct SavedState {
    bucket_offsets: HashMap<i32, i64>,
    messages_produced: u64,
    table: Option<SavedTable>,
}

#[derive(Debug, Deserialize)]
struct SavedTable {
    database: String,
    table: String,
    table_id: i64,
}

/// An Apache Iggy message reduced to what the tests check.
struct PolledMessage {
    id: u128,
    payload: Value,
}

#[iggy_harness(
    server(connectors_runtime(config_path = "tests/fluss/source.toml")),
    seed = seeds::connector_stream
)]
async fn given_log_table_rows_should_produce_them_to_iggy(
    harness: &TestHarness,
    fixture: FlussSourceFixture,
) {
    let client = harness.root_client().await.unwrap();

    let payloads = test_payloads();
    fixture
        .append_rows(&payloads)
        .await
        .expect("Failed to append rows");

    let received = poll_records(&client, TEST_ROW_COUNT, POLL_ATTEMPTS).await;

    assert!(
        received.len() >= TEST_ROW_COUNT,
        "Expected at least {TEST_ROW_COUNT} messages, got {}",
        received.len()
    );

    for (index, record) in received.iter().take(TEST_ROW_COUNT).enumerate() {
        assert_eq!(record.id, index as i32, "Column `id` mismatch at {index}");
        assert_eq!(
            record.payload, payloads[index],
            "Column `payload` mismatch at {index}"
        );
        assert_eq!(record.bucket, 0, "Bucket mismatch at {index}");
        assert_eq!(
            record.offset, index as i64,
            "Fluss offset should be preserved and sequential at {index}"
        );
        assert!(
            record.timestamp > 0,
            "Fluss timestamp should be populated at {index}"
        );
    }
    assert_message_ids(&received);
}

#[iggy_harness(
    cluster_nodes = 1,
    server(connectors_runtime(config_path = "tests/fluss/source.toml")),
    seed = seeds::connector_stream
)]
async fn given_rejected_batch_when_iggy_restarts_should_replay_rows_from_fluss(
    harness: &mut TestHarness,
    fixture: FlussSourceSlowPollFixture,
) {
    // Without reconnection retries a send to a stopped server fails at once, so the runtime
    // rejects the batch instead of blocking on it.
    harness
        .server_mut()
        .stop_dependents()
        .expect("Failed to stop connectors runtime");
    harness
        .server_mut()
        .connectors_runtime_mut()
        .expect("connectors runtime")
        .set_iggy_connection_options("reconnection_retries=0");
    harness
        .server_mut()
        .start_dependents()
        .await
        .expect("Failed to restart connectors runtime");

    let api_url = harness
        .connectors_runtime()
        .expect("connectors runtime")
        .http_url();
    let http = Client::new();
    let errors_before_failure = source_stats(&http, &api_url)
        .await
        .expect("Apache Fluss source stats should be present")
        .errors;

    harness.kill_node(0).expect("Failed to kill Iggy server");
    let payloads = test_payloads();
    fixture
        .append_rows(&payloads)
        .await
        .expect("Failed to append rows");
    wait_for_source_errors(&http, &api_url, errors_before_failure + 1).await;

    harness
        .restart_node(0)
        .expect("Failed to restart the Iggy server");

    // The scanner handed these rows over before the batch was rejected, so they only arrive if
    // the source sends the rejected batch again instead of moving on.
    let client = harness.root_client().await.unwrap();
    let received = poll_records(&client, payloads.len(), REDELIVERY_ATTEMPTS).await;
    assert_rows_delivered(&received, &payloads, 0);
}

#[iggy_harness(
    server(connectors_runtime(config_path = "tests/fluss/source.toml")),
    seed = seeds::connector_stream
)]
async fn given_latest_start_when_runtime_restarts_before_any_row_should_deliver_rows_written_meanwhile(
    harness: &mut TestHarness,
    fixture: FlussSourceLatestFixture,
) {
    let state_path = harness
        .connectors_runtime()
        .expect("connectors runtime")
        .state_path()
        .join(STATE_FILE);
    // No row has arrived yet, so only the tail offsets resolved for `latest` can have written it.
    wait_for_file(&state_path).await;

    harness
        .server_mut()
        .stop_dependents()
        .expect("Failed to stop connectors runtime");
    let payloads = test_payloads();
    fixture
        .append_rows(&payloads)
        .await
        .expect("Failed to append rows");
    harness
        .server_mut()
        .start_dependents()
        .await
        .expect("Failed to restart connectors runtime");

    let client = harness.root_client().await.unwrap();
    let received = poll_records(&client, payloads.len(), POLL_ATTEMPTS).await;
    assert!(
        received
            .iter()
            .all(|record| !fixture.existing_payloads().contains(&record.payload)),
        "Rows written before the first start should be skipped by `latest`"
    );
    assert_rows_delivered(
        &received,
        &payloads,
        fixture.existing_payloads().len() as i64,
    );
}

#[iggy_harness(
    server(connectors_runtime(config_path = "tests/fluss/source.toml")),
    seed = seeds::connector_stream
)]
async fn given_acknowledged_rows_when_runtime_restarts_should_resume_after_them(
    harness: &mut TestHarness,
    fixture: FlussSourceFixture,
) {
    let first_payloads = test_payloads();
    fixture
        .append_rows(&first_payloads)
        .await
        .expect("Failed to append rows");
    let client = harness.root_client().await.unwrap();
    let received = poll_records(&client, first_payloads.len(), POLL_ATTEMPTS).await;
    assert_rows_delivered(&received, &first_payloads, 0);

    // The rows reach Apache Iggy before the runtime saves the state that acknowledges them, and
    // a stop in between would replay them, so wait for the saved offset first.
    let state_path = harness
        .connectors_runtime()
        .expect("connectors runtime")
        .state_path()
        .join(STATE_FILE);
    let saved = wait_for_saved_offset(&state_path, 0, TEST_ROW_COUNT as i64).await;
    assert_eq!(
        saved.messages_produced, TEST_ROW_COUNT as u64,
        "Each acknowledged row should be counted once"
    );

    harness
        .server_mut()
        .stop_dependents()
        .expect("Failed to stop connectors runtime");
    let second_payloads: Vec<String> = (0..TEST_ROW_COUNT)
        .map(|index| format!("fluss-payload-after-restart-{index}"))
        .collect();
    fixture
        .append_rows(&second_payloads)
        .await
        .expect("Failed to append rows");
    harness
        .server_mut()
        .start_dependents()
        .await
        .expect("Failed to restart connectors runtime");

    let received = poll_records(&client, second_payloads.len(), POLL_ATTEMPTS).await;
    assert!(
        received
            .iter()
            .all(|record| record.offset >= TEST_ROW_COUNT as i64),
        "Rows acknowledged before the restart should not be read again"
    );
    assert_rows_delivered(&received, &second_payloads, TEST_ROW_COUNT as i64);
}

#[iggy_harness(
    server(connectors_runtime(config_path = "tests/fluss/source.toml")),
    seed = seeds::connector_stream
)]
async fn given_table_created_again_under_the_same_name_when_runtime_restarts_should_refuse_its_offsets(
    harness: &mut TestHarness,
    fixture: FlussSourceFixture,
) {
    fixture
        .append_rows(&test_payloads())
        .await
        .expect("Failed to append rows");
    let state_path = harness
        .connectors_runtime()
        .expect("connectors runtime")
        .state_path()
        .join(STATE_FILE);
    let saved = wait_for_saved_offset(&state_path, 0, TEST_ROW_COUNT as i64).await;
    let saved_table = saved
        .table
        .expect("The saved state should name the table its offsets came from");

    harness
        .server_mut()
        .stop_dependents()
        .expect("Failed to stop connectors runtime");
    let saved_bytes = std::fs::read(&state_path).expect("Failed to read the state file");
    // The new table starts empty, so the saved offset would start it past its first rows.
    fixture
        .recreate_table()
        .await
        .expect("Failed to create the table again");
    harness
        .server_mut()
        .start_dependents()
        .await
        .expect("Failed to restart connectors runtime");

    let runtime = harness.connectors_runtime().expect("connectors runtime");
    wait_for_source_status(&Client::new(), &runtime.http_url(), ConnectorStatus::Error).await;
    let (stdout, _) = runtime.collect_logs();
    let saved_name = format!(
        "the saved state was read from table '{}.{}' (id {})",
        saved_table.database, saved_table.table, saved_table.table_id
    );
    assert!(
        stdout.contains(&saved_name),
        "The source should log that the saved offsets belong to the dropped table"
    );
    assert_eq!(
        std::fs::read(&state_path).expect("Failed to read the state file"),
        saved_bytes,
        "The state of the dropped table should be left in place"
    );
}

#[iggy_harness(
    server(connectors_runtime(config_path = "tests/fluss/source.toml")),
    seed = seeds::connector_stream
)]
async fn given_every_supported_column_type_should_map_each_to_json(
    harness: &TestHarness,
    fixture: FlussSourceAllTypesFixture,
) {
    fixture
        .append_sample_rows()
        .await
        .expect("Failed to append rows");

    let client = harness.root_client().await.unwrap();
    let received = poll_payloads(&client, 3, POLL_ATTEMPTS).await;
    let row_at = |offset: i64| {
        received
            .iter()
            .find(|payload| payload["_fluss_offset"] == offset)
            .unwrap_or_else(|| panic!("Row at Fluss offset {offset} was never delivered"))
    };
    let (filled, empty, extreme) = (row_at(0), row_at(1), row_at(2));

    let expected = json!({
        "bool_col": true,
        "tinyint_col": 7,
        "smallint_col": 300,
        "int_col": 70_000,
        "bigint_col": 9_000_000_000i64,
        "float_col": 1.5,
        "double_col": 2.25,
        "char_col": "abcde",
        "string_col": "hello",
        "decimal_col": "123.45",
        "small_decimal_col": "0.00000012",
        "date_col": "2024-02-29",
        "time_col": "12:34:56.789",
        "timestamp_col": "2023-11-14 22:13:20.123456",
        "timestamp_nanos_col": "2023-11-14 22:13:20.123456789",
        "timestamp_ltz_col": "2023-11-14T22:13:20.123456+00:00",
        "bytes_col": "AQID",
        "binary_col": "BAUG",
    });
    for (column, value) in expected
        .as_object()
        .expect("Expected row should be an object")
    {
        assert_eq!(
            &filled[column], value,
            "Column `{column}` did not map as expected"
        );
        assert_eq!(
            empty[column],
            Value::Null,
            "Column `{column}` should be null in the empty row"
        );
    }

    let expected_extremes = json!({
        "bool_col": false,
        "tinyint_col": i8::MIN,
        "smallint_col": i16::MIN,
        "int_col": i32::MIN,
        "bigint_col": i64::MIN,
        "float_col": -65536.5,
        "double_col": f64::MIN,
        "char_col": "     ",
        "string_col": "\"quoted\" \\ 한글 🚀\n\ttab",
        "decimal_col": "-99999999.99",
        "small_decimal_col": "-0.00000012",
        "date_col": "1969-12-31",
        "time_col": "00:00:00",
        "timestamp_col": "1969-12-31 23:59:59.999",
        "timestamp_nanos_col": "1969-12-31 23:59:59.999999999",
        "timestamp_ltz_col": "1969-12-31T23:59:59.999+00:00",
        "bytes_col": "",
        "binary_col": "AP+A",
    });
    for (column, value) in expected_extremes
        .as_object()
        .expect("Expected row should be an object")
    {
        assert_eq!(
            &extreme[column], value,
            "Column `{column}` did not map its extreme value as expected"
        );
    }
}

#[iggy_harness(
    server(connectors_runtime(config_path = "tests/fluss/source.toml")),
    seed = seeds::connector_stream
)]
async fn given_row_that_cannot_be_converted_should_skip_it_and_deliver_the_rest(
    harness: &TestHarness,
    fixture: FlussSourceUnconvertibleRowFixture,
) {
    fixture
        .append_rows_around_an_unconvertible_date()
        .await
        .expect("Failed to append rows");

    let client = harness.root_client().await.unwrap();
    let received = poll_payloads(&client, 2, POLL_ATTEMPTS).await;
    let delivered: HashSet<i64> = received
        .iter()
        .filter_map(|payload| payload["_fluss_offset"].as_i64())
        .collect();

    assert_eq!(
        delivered,
        HashSet::from([0, 2]),
        "Only the rows around the unconvertible one should be delivered"
    );
    for payload in &received {
        assert_eq!(
            payload["id"], payload["_fluss_offset"],
            "Each row should carry the id written at its offset"
        );
    }
    assert!(
        runtime_log(harness).contains("skipped the row at bucket 0, offset 1"),
        "The skipped row should be logged with its bucket and offset"
    );
}

#[iggy_harness(
    server(connectors_runtime(config_path = "tests/fluss/source.toml")),
    seed = seeds::connector_stream
)]
async fn given_projection_in_reverse_column_order_should_map_each_column_by_name(
    harness: &TestHarness,
    fixture: FlussSourceProjectedFixture,
) {
    let payloads = test_payloads();
    fixture
        .append_rows(&payloads)
        .await
        .expect("Failed to append rows");

    let client = harness.root_client().await.unwrap();
    let received = poll_payloads(&client, payloads.len(), POLL_ATTEMPTS).await;
    for (index, payload) in payloads.iter().enumerate() {
        let offset = index as i64;
        let row = received
            .iter()
            .find(|row| row["_fluss_offset"] == offset)
            .unwrap_or_else(|| panic!("Row at Fluss offset {offset} was never delivered"));
        assert_eq!(
            row,
            &json!({
                "id": index,
                "payload": payload,
                "_fluss_bucket": 0,
                "_fluss_offset": offset,
                "_fluss_timestamp": row["_fluss_timestamp"],
            }),
            "Only the projected columns should arrive, each under its own name"
        );
    }
}

#[iggy_harness(
    server(connectors_runtime(config_path = "tests/fluss/source.toml")),
    seed = seeds::connector_stream
)]
async fn given_projection_over_tiered_rows_written_before_a_column_was_added_should_deliver_them(
    harness: &TestHarness,
    fixture: FlussSourceTieredProjectionFixture,
) {
    let client = harness.root_client().await.unwrap();
    let received = poll_records(&client, fixture.payloads().len(), POLL_ATTEMPTS).await;
    assert_rows_delivered(&received, fixture.payloads(), 0);
}

/// The paired projection test reads the same table successfully, so the failure here comes from
/// the reserved column rather than from the table.
#[iggy_harness(
    server(connectors_runtime(config_path = "tests/fluss/source.toml")),
    seed = seeds::connector_stream
)]
async fn given_column_under_the_metadata_prefix_should_fail_to_start(
    harness: &TestHarness,
    _fixture: FlussSourceReservedColumnFixture,
) {
    assert_fails_to_start(
        harness,
        "column '_fluss_note' starts with '_fluss_'",
        SOURCE_STATUS_TIMEOUT,
    )
    .await;
}

/// Starting over without the saved offsets could skip rows that were never delivered, so a
/// state that does not decode keeps the source from opening and stays on disk for the operator.
#[iggy_harness(
    server(connectors_runtime(config_path = "tests/fluss/source.toml")),
    seed = seeds::connector_stream
)]
async fn given_unreadable_state_when_runtime_restarts_should_refuse_to_open(
    harness: &mut TestHarness,
    _fixture: FlussSourceFixture,
) {
    let state_path = harness
        .connectors_runtime()
        .expect("connectors runtime")
        .state_path()
        .join(STATE_FILE);
    harness
        .server_mut()
        .stop_dependents()
        .expect("Failed to stop connectors runtime");
    let unreadable = b"not a MessagePack state".to_vec();
    std::fs::write(&state_path, &unreadable).expect("Failed to write the state file");
    harness
        .server_mut()
        .start_dependents()
        .await
        .expect("Failed to restart connectors runtime");

    let runtime = harness.connectors_runtime().expect("connectors runtime");
    wait_for_source_status(&Client::new(), &runtime.http_url(), ConnectorStatus::Error).await;

    // The runtime reports a generic initialization failure, so the reason is only in the log.
    let (stdout, _) = runtime.collect_logs();
    assert!(
        stdout.contains("the saved state cannot be decoded"),
        "The source should log why it refused to open"
    );
    assert_eq!(
        std::fs::read(&state_path).expect("Failed to read the state file"),
        unreadable,
        "The unreadable state should be left in place"
    );
}

#[iggy_harness(
    server(connectors_runtime(config_path = "tests/fluss/source.toml")),
    seed = seeds::connector_stream
)]
async fn given_table_with_several_buckets_should_deliver_every_row_once(
    harness: &TestHarness,
    fixture: FlussSourceBucketsFixture,
) {
    let payloads = numbered_payloads("several-buckets", BUCKETED_ROW_COUNT);
    fixture
        .append_rows(&payloads)
        .await
        .expect("Failed to append rows");

    let client = harness.root_client().await.unwrap();
    let received = drain_records_and_settle(&client, payloads.len(), DELIVERY_TIMEOUT).await;

    assert_each_row_once(&received, &payloads);
    let delivered = offsets_by_bucket(&received);
    assert_eq!(
        delivered.keys().copied().collect::<HashSet<_>>(),
        (0..fixture.bucket_count()).collect::<HashSet<_>>(),
        "The rows should spread over every bucket of the table"
    );
    for (bucket, offsets) in &delivered {
        assert_eq!(
            offsets,
            &(0..offsets.len() as i64).collect::<Vec<_>>(),
            "Bucket {bucket} should deliver its offsets from 0 without a gap"
        );
    }

    let next_offsets: HashMap<i32, i64> = delivered
        .iter()
        .map(|(bucket, offsets)| (*bucket, offsets.len() as i64))
        .collect();
    let saved =
        wait_for_saved_offsets(&state_path(harness), &next_offsets, STATE_FILE_TIMEOUT).await;
    assert_eq!(
        saved.messages_produced,
        payloads.len() as u64,
        "Each row should be counted once across the buckets"
    );
}

#[iggy_harness(
    server(connectors_runtime(config_path = "tests/fluss/source.toml")),
    seed = seeds::connector_stream
)]
async fn given_table_with_several_buckets_when_runtime_restarts_should_resume_each_bucket(
    harness: &mut TestHarness,
    fixture: FlussSourceBucketsFixture,
) {
    let first_payloads = numbered_payloads("before-restart", BUCKETED_ROW_COUNT);
    fixture
        .append_rows(&first_payloads)
        .await
        .expect("Failed to append rows");
    let client = harness.root_client().await.unwrap();
    let received = drain_records_and_settle(&client, first_payloads.len(), DELIVERY_TIMEOUT).await;
    assert_each_row_once(&received, &first_payloads);
    let rows_per_round: HashMap<i32, i64> = offsets_by_bucket(&received)
        .into_iter()
        .map(|(bucket, offsets)| (bucket, offsets.len() as i64))
        .collect();
    let mut next_offsets = rows_per_round.clone();
    wait_for_saved_offsets(&state_path(harness), &next_offsets, STATE_FILE_TIMEOUT).await;

    // Rows for one bucket only, under ids that already landed in it. A poll that reads one
    // bucket has to leave the offsets of the others in the state.
    let lone_bucket = 0;
    let lone_rows: Vec<(i32, String)> = received
        .iter()
        .filter(|record| record.bucket == lone_bucket)
        .map(|record| (record.id, format!("one-bucket-{}", record.id)))
        .collect();
    fixture
        .append_rows_with_ids(&lone_rows)
        .await
        .expect("Failed to append rows");
    let lone_payloads: Vec<String> = lone_rows.into_iter().map(|(_, payload)| payload).collect();
    let received = drain_records_and_settle(&client, lone_payloads.len(), DELIVERY_TIMEOUT).await;
    assert_each_row_once(&received, &lone_payloads);
    assert!(
        received.iter().all(|record| record.bucket == lone_bucket),
        "The same ids should hash to the same bucket"
    );
    *next_offsets
        .get_mut(&lone_bucket)
        .expect("The lone bucket should hold rows") += lone_payloads.len() as i64;
    wait_for_saved_offsets(&state_path(harness), &next_offsets, STATE_FILE_TIMEOUT).await;

    harness
        .server_mut()
        .stop_dependents()
        .expect("Failed to stop connectors runtime");
    // The same ids hash to the same buckets, so each bucket grows by as many rows again.
    let second_payloads = numbered_payloads("after-restart", BUCKETED_ROW_COUNT);
    fixture
        .append_rows(&second_payloads)
        .await
        .expect("Failed to append rows");
    harness
        .server_mut()
        .start_dependents()
        .await
        .expect("Failed to restart connectors runtime");

    let received = drain_records_and_settle(&client, second_payloads.len(), DELIVERY_TIMEOUT).await;
    assert_each_row_once(&received, &second_payloads);
    for (bucket, offsets) in offsets_by_bucket(&received) {
        let resumed_at = next_offsets[&bucket];
        assert_eq!(
            offsets,
            (resumed_at..resumed_at + rows_per_round[&bucket]).collect::<Vec<_>>(),
            "Bucket {bucket} should resume at the offset saved for it"
        );
    }
}

#[iggy_harness(
    server(connectors_runtime(config_path = "tests/fluss/source.toml")),
    seed = seeds::connector_stream
)]
async fn given_more_rows_than_a_batch_should_deliver_them_in_order_without_waiting_between_batches(
    harness: &TestHarness,
    fixture: FlussSourceSmallBatchFixture,
) {
    let payloads = numbered_payloads("many-rows", MANY_ROW_COUNT);
    assert_ne!(
        payloads.len() % fixture.batch_size(),
        0,
        "The last batch should be a partial one"
    );
    fixture
        .append_rows(&payloads)
        .await
        .expect("Failed to append rows");

    let client = harness.root_client().await.unwrap();
    let received = drain_records_and_settle(&client, payloads.len(), FULL_BATCHES_TIMEOUT).await;

    assert!(
        received.len() >= payloads.len(),
        "Only {} of {} rows arrived within {FULL_BATCHES_TIMEOUT:?}, as if the source waited \
         poll_interval after each full batch",
        received.len(),
        payloads.len()
    );
    assert_each_row_once(&received, &payloads);
    let offsets: Vec<i64> = received.iter().map(|record| record.offset).collect();
    assert_eq!(
        offsets,
        (0..payloads.len() as i64).collect::<Vec<_>>(),
        "The rows of one bucket should arrive in offset order"
    );
    for record in &received {
        assert_eq!(record.payload, payloads[record.offset as usize]);
    }
    let saved = wait_for_saved_offset(&state_path(harness), 0, payloads.len() as i64).await;
    assert_eq!(saved.messages_produced, payloads.len() as u64);
}

#[iggy_harness(
    server(connectors_runtime(config_path = "tests/fluss/source.toml")),
    seed = seeds::connector_stream
)]
async fn given_numeric_starting_offset_should_start_at_that_row(
    harness: &TestHarness,
    fixture: FlussSourceExplicitOffsetFixture,
) {
    let start = fixture.start_offset();
    let expected = &fixture.existing_payloads()[start..];

    let client = harness.root_client().await.unwrap();
    let received = drain_records_and_settle(&client, expected.len(), DELIVERY_TIMEOUT).await;

    assert_each_row_once(&received, expected);
    let offsets: Vec<i64> = received.iter().map(|record| record.offset).collect();
    assert_eq!(
        offsets,
        (start as i64..fixture.existing_payloads().len() as i64).collect::<Vec<_>>(),
        "Only the rows from the configured offset on should be read"
    );
}

/// The README says such an offset is not reset. With no pause of its own the source would
/// retry at network speed, so this counts the failed polls of a few seconds.
#[iggy_harness(
    server(connectors_runtime(config_path = "tests/fluss/source.toml")),
    seed = seeds::connector_stream
)]
async fn given_starting_offset_past_the_end_should_keep_failing_at_a_bounded_rate(
    harness: &TestHarness,
    _fixture: FlussSourceOffsetPastTheEndFixture,
) {
    wait_for_runtime_log(harness, POLL_FAILURE, OUTAGE_TIMEOUT).await;

    let failures_before = runtime_log(harness).matches(POLL_FAILURE).count();
    sleep(FAILED_POLL_WINDOW).await;
    let failures = runtime_log(harness).matches(POLL_FAILURE).count() - failures_before;

    // One failure a second is what the pause allows. Without it there are hundreds.
    assert!(
        failures <= FAILED_POLL_WINDOW.as_secs() as usize * 2,
        "{failures} polls failed within {FAILED_POLL_WINDOW:?}, so the source did not pause \
         between them"
    );
    let client = harness.root_client().await.unwrap();
    assert!(
        poll_once(&client, DRAIN_BATCH).await.is_empty(),
        "No row should be delivered from an offset the table does not hold"
    );
}

#[iggy_harness(
    server(connectors_runtime(config_path = "tests/fluss/source.toml")),
    seed = seeds::connector_stream
)]
async fn given_unreachable_cluster_should_fail_to_start_and_log_why(
    harness: &TestHarness,
    _fixture: FlussSourceUnreachableFixture,
) {
    assert_fails_to_start(harness, "Apache Fluss client failure", UNREACHABLE_TIMEOUT).await;
}

#[iggy_harness(
    server(connectors_runtime(config_path = "tests/fluss/source.toml")),
    seed = seeds::connector_stream
)]
async fn given_missing_table_should_fail_to_start_and_log_why(
    harness: &TestHarness,
    _fixture: FlussSourceMissingTableFixture,
) {
    assert_fails_to_start(harness, "iggy_test.events", SOURCE_STATUS_TIMEOUT).await;
}

#[iggy_harness(
    server(connectors_runtime(config_path = "tests/fluss/source.toml")),
    seed = seeds::connector_stream
)]
async fn given_primary_key_table_should_fail_to_start_and_log_why(
    harness: &TestHarness,
    _fixture: FlussSourcePrimaryKeyTableFixture,
) {
    assert_fails_to_start(harness, "is a primary-key table", SOURCE_STATUS_TIMEOUT).await;
}

#[iggy_harness(
    server(connectors_runtime(config_path = "tests/fluss/source.toml")),
    seed = seeds::connector_stream
)]
async fn given_partitioned_table_should_fail_to_start_and_log_why(
    harness: &TestHarness,
    _fixture: FlussSourcePartitionedTableFixture,
) {
    assert_fails_to_start(harness, "is partitioned", SOURCE_STATUS_TIMEOUT).await;
}

#[iggy_harness(
    server(connectors_runtime(config_path = "tests/fluss/source.toml")),
    seed = seeds::connector_stream
)]
async fn given_cluster_outage_when_it_returns_should_resume_without_losing_rows(
    harness: &TestHarness,
    fixture: FlussSourceOutageFixture,
) {
    let first_payloads = numbered_payloads("before-outage", TEST_ROW_COUNT);
    fixture
        .append_rows(&first_payloads)
        .await
        .expect("Failed to append rows");
    let client = harness.root_client().await.unwrap();
    let received = poll_records(&client, first_payloads.len(), POLL_ATTEMPTS).await;
    assert_rows_delivered(&received, &first_payloads, 0);
    wait_for_saved_offset(&state_path(harness), 0, TEST_ROW_COUNT as i64).await;

    fixture
        .stop_cluster()
        .await
        .expect("Failed to stop the cluster");
    // The client retries inside each poll and hands back no rows, so the outage shows in its
    // own log and the source reports no error.
    wait_for_runtime_log(harness, CLUSTER_UNREACHABLE, OUTAGE_TIMEOUT).await;
    let api_url = harness
        .connectors_runtime()
        .expect("connectors runtime")
        .http_url();
    let stats = source_stats(&Client::new(), &api_url)
        .await
        .expect("Apache Fluss source stats should be present");
    assert_eq!(stats.status, ConnectorStatus::Running);
    assert_eq!(stats.errors, 0, "An outage should not count as an error");
    fixture
        .start_cluster()
        .await
        .expect("Failed to start the cluster again");

    assert_eq!(
        fixture
            .next_offset(0)
            .await
            .expect("Failed to read the end of the table"),
        TEST_ROW_COUNT as i64,
        "The cluster should come back with the rows it held"
    );
    let second_payloads = numbered_payloads("after-outage", TEST_ROW_COUNT);
    fixture
        .append_rows(&second_payloads)
        .await
        .expect("Failed to append rows");
    let received = into_records(drain_rows(&client, second_payloads.len(), RECOVERY_TIMEOUT).await);
    assert_rows_delivered(&received, &second_payloads, TEST_ROW_COUNT as i64);
}

/// The README says a column added after startup stays out of the output until the connector
/// restarts.
#[iggy_harness(
    server(connectors_runtime(config_path = "tests/fluss/source.toml")),
    seed = seeds::connector_stream
)]
async fn given_column_added_while_running_should_leave_it_out_until_the_runtime_restarts(
    harness: &mut TestHarness,
    fixture: FlussSourceFixture,
) {
    let before = numbered_payloads("before-column", TEST_ROW_COUNT);
    fixture
        .append_rows(&before)
        .await
        .expect("Failed to append rows");
    let client = harness.root_client().await.unwrap();
    let received = poll_payloads(&client, before.len(), POLL_ATTEMPTS).await;
    assert_eq!(received.len(), before.len());

    fixture
        .add_string_column("note")
        .await
        .expect("Failed to add a column");
    let during = numbered_payloads("column-added", TEST_ROW_COUNT);
    fixture
        .append_rows_with_note(TEST_ROW_COUNT, &during, "added while running")
        .await
        .expect("Failed to append rows");
    let received = poll_payloads(&client, during.len(), POLL_ATTEMPTS).await;
    assert_eq!(received.len(), during.len());
    for (index, row) in received.iter().enumerate() {
        assert_eq!(row["payload"], Value::from(during[index].as_str()));
        assert_eq!(row["id"], Value::from(TEST_ROW_COUNT + index));
        assert!(
            row.get("note").is_none(),
            "A column added after startup should stay out of the payload: {row}"
        );
    }
    wait_for_saved_offset(&state_path(harness), 0, (TEST_ROW_COUNT * 2) as i64).await;

    harness
        .server_mut()
        .stop_dependents()
        .expect("Failed to stop connectors runtime");
    let after = numbered_payloads("after-restart", TEST_ROW_COUNT);
    fixture
        .append_rows_with_note(TEST_ROW_COUNT * 2, &after, "read after the restart")
        .await
        .expect("Failed to append rows");
    harness
        .server_mut()
        .start_dependents()
        .await
        .expect("Failed to restart connectors runtime");

    let received = poll_payloads(&client, after.len(), POLL_ATTEMPTS).await;
    assert_eq!(received.len(), after.len());
    for (index, row) in received.iter().enumerate() {
        assert_eq!(row["payload"], Value::from(after[index].as_str()));
        assert_eq!(row["note"], Value::from("read after the restart"));
    }
}

#[iggy_harness(
    server(connectors_runtime(config_path = "tests/fluss/source.toml")),
    seed = seeds::connector_stream
)]
async fn given_empty_table_should_save_its_start_and_stay_idle_until_rows_arrive(
    harness: &TestHarness,
    fixture: FlussSourceQuickPollFixture,
) {
    let state_path = state_path(harness);
    let saved = wait_for_saved_offset(&state_path, 0, EARLIEST_OFFSET).await;
    assert_eq!(saved.messages_produced, 0);
    assert!(
        saved.table.is_some(),
        "The first saved state should already name the table"
    );
    let saved_at = std::fs::metadata(&state_path)
        .and_then(|metadata| metadata.modified())
        .expect("Failed to read the state file time");

    let client = harness.root_client().await.unwrap();
    assert!(
        drain_rows(&client, usize::MAX, SETTLE_TIME)
            .await
            .is_empty(),
        "An empty table should produce no message"
    );
    let api_url = harness
        .connectors_runtime()
        .expect("connectors runtime")
        .http_url();
    let stats = source_stats(&Client::new(), &api_url)
        .await
        .expect("Apache Fluss source stats should be present");
    assert_eq!(stats.status, ConnectorStatus::Running);
    assert_eq!(stats.errors, 0, "Empty polls should not count as errors");
    assert_eq!(
        std::fs::metadata(&state_path)
            .and_then(|metadata| metadata.modified())
            .expect("Failed to read the state file time"),
        saved_at,
        "Empty polls should not write the state again"
    );

    let payloads = test_payloads();
    fixture
        .append_rows(&payloads)
        .await
        .expect("Failed to append rows");
    let received = drain_records_and_settle(&client, payloads.len(), DELIVERY_TIMEOUT).await;
    assert_each_row_once(&received, &payloads);
}

#[iggy_harness(
    server(connectors_runtime(config_path = "tests/fluss/source.toml")),
    seed = seeds::connector_stream
)]
async fn given_repeated_runtime_restarts_should_deliver_every_row_once(
    harness: &mut TestHarness,
    fixture: FlussSourceFixture,
) {
    let state_path = state_path(harness);
    let mut payloads = Vec::with_capacity(RESTART_ROUNDS * TEST_ROW_COUNT);
    for round in 0..RESTART_ROUNDS {
        let round_payloads = numbered_payloads(&format!("round-{round}"), TEST_ROW_COUNT);
        fixture
            .append_rows(&round_payloads)
            .await
            .expect("Failed to append rows");
        payloads.extend(round_payloads);
        wait_for_saved_offset(&state_path, 0, payloads.len() as i64).await;

        harness
            .server_mut()
            .stop_dependents()
            .expect("Failed to stop connectors runtime");
        harness
            .server_mut()
            .start_dependents()
            .await
            .expect("Failed to restart connectors runtime");
    }

    let client = harness.root_client().await.unwrap();
    let received = drain_records_and_settle(&client, payloads.len(), DELIVERY_TIMEOUT).await;

    assert_each_row_once(&received, &payloads);
    let offsets: Vec<i64> = received.iter().map(|record| record.offset).collect();
    assert_eq!(offsets, (0..payloads.len() as i64).collect::<Vec<_>>());
    let saved = wait_for_saved_offset(&state_path, 0, payloads.len() as i64).await;
    assert_eq!(saved.messages_produced, payloads.len() as u64);
}

/// The API restart closes the source and opens it again inside the running runtime, unlike
/// a restart of the whole process.
#[iggy_harness(
    server(connectors_runtime(config_path = "tests/fluss/source.toml")),
    seed = seeds::connector_stream
)]
async fn given_source_restarted_through_the_api_should_resume_without_repeating_rows(
    harness: &TestHarness,
    fixture: FlussSourceFixture,
) {
    let state_path = state_path(harness);
    let api_url = harness
        .connectors_runtime()
        .expect("connectors runtime")
        .http_url();
    let http = Client::new();
    let mut payloads = Vec::with_capacity((RESTART_ROUNDS + 1) * TEST_ROW_COUNT);
    for round in 0..=RESTART_ROUNDS {
        let round_payloads = numbered_payloads(&format!("round-{round}"), TEST_ROW_COUNT);
        fixture
            .append_rows(&round_payloads)
            .await
            .expect("Failed to append rows");
        payloads.extend(round_payloads);
        wait_for_saved_offset(&state_path, 0, payloads.len() as i64).await;
        if round < RESTART_ROUNDS {
            restart_source(&http, &api_url).await;
        }
    }

    let client = harness.root_client().await.unwrap();
    let received = drain_records_and_settle(&client, payloads.len(), DELIVERY_TIMEOUT).await;

    assert_each_row_once(&received, &payloads);
    let offsets: Vec<i64> = received.iter().map(|record| record.offset).collect();
    assert_eq!(offsets, (0..payloads.len() as i64).collect::<Vec<_>>());
}

/// A kill leaves no time to save anything, so the runtime resumes from whatever state the
/// last acknowledged batch left on disk. Rows may arrive twice, none may be missing.
#[iggy_harness(
    server(connectors_runtime(config_path = "tests/fluss/source.toml")),
    seed = seeds::connector_stream
)]
async fn given_runtime_killed_while_rows_arrive_when_it_restarts_should_lose_no_row(
    harness: &mut TestHarness,
    fixture: FlussSourceSmallBatchFixture,
) {
    let payloads = numbered_payloads("crash", CRASH_ROW_COUNT);
    fixture
        .append_rows(&payloads)
        .await
        .expect("Failed to append rows");
    let client = harness.root_client().await.unwrap();
    let mut received = drain_rows(&client, fixture.batch_size(), DELIVERY_TIMEOUT).await;
    assert!(!received.is_empty(), "No row arrived before the kill");

    harness
        .server_mut()
        .connectors_runtime_mut()
        .expect("connectors runtime")
        .kill()
        .expect("Failed to kill connectors runtime");
    let saved_at_kill = saved_state(&state_path(harness))
        .and_then(|state| state.bucket_offsets.get(&0).copied())
        .unwrap_or(EARLIEST_OFFSET);
    assert!(
        saved_at_kill < payloads.len() as i64,
        "The runtime should be killed before it read the whole table"
    );
    // Nothing is sent while the runtime is down, so this takes all it sent out of the topic
    // and whatever arrives later was sent after the restart.
    received.extend(drain_rows(&client, usize::MAX, SETTLE_TIME).await);
    let mut received = into_records(received);
    harness
        .server_mut()
        .start_dependents()
        .await
        .expect("Failed to restart connectors runtime");

    let resumes_at = saved_at_kill.max(0);
    let rows_left = payloads.len() - resumes_at as usize;
    let after_restart = into_records(drain_rows(&client, rows_left, CRASH_RECOVERY_TIMEOUT).await);
    assert_eq!(
        after_restart.iter().map(|record| record.offset).min(),
        Some(resumes_at),
        "The restart should resume at the offset that was on disk when the runtime was killed"
    );
    received.extend(after_restart);
    assert_rows_delivered(&received, &payloads, 0);
    let saved = wait_for_saved_offset(&state_path(harness), 0, payloads.len() as i64).await;
    assert_eq!(
        saved.messages_produced,
        payloads.len() as u64,
        "Rows sent again after the kill should be counted once"
    );
}

/// The rows are already in the topic when the state save fails, so the batch is rejected and
/// its rows are sent again, with the same ids, until the state is on disk.
#[iggy_harness(
    server(connectors_runtime(config_path = "tests/fluss/source.toml")),
    seed = seeds::connector_stream
)]
async fn given_state_that_cannot_be_saved_should_send_the_rows_again_until_it_is(
    harness: &TestHarness,
    fixture: FlussSourceSlowPollFixture,
) {
    let state_path = state_path(harness);
    // The start offsets go out before any row, so wait for them and break the save after.
    wait_for_file(&state_path).await;
    std::fs::remove_file(&state_path).expect("Failed to remove the state file");
    // A directory in its place makes the runtime's rename onto the state file fail.
    std::fs::create_dir(&state_path).expect("Failed to block the state file");

    let payloads = test_payloads();
    fixture
        .append_rows(&payloads)
        .await
        .expect("Failed to append rows");
    wait_for_runtime_log(harness, STATE_SAVE_FAILURE, SEND_FAILURE_TIMEOUT).await;
    std::fs::remove_dir(&state_path).expect("Failed to unblock the state file");

    let saved = wait_for_saved_offsets(
        &state_path,
        &HashMap::from([(0, payloads.len() as i64)]),
        SEND_FAILURE_TIMEOUT,
    )
    .await;
    assert_eq!(
        saved.messages_produced,
        payloads.len() as u64,
        "Rows sent again after a rejected batch should be counted once"
    );
    let client = harness.root_client().await.unwrap();
    let received = into_records(drain_rows(&client, usize::MAX, SETTLE_TIME).await);
    assert_rows_delivered(&received, &payloads, 0);
    for offset in 0..payloads.len() as i64 {
        let copies = received
            .iter()
            .filter(|record| record.offset == offset)
            .count();
        assert!(
            copies >= 2,
            "The row at offset {offset} should be sent again after the failed save, got \
             {copies} copies"
        );
    }
}

/// A `raw` stream rejects every batch that carries rows, and five rejections in a row stop
/// the source. The rejected rows must not move the saved offset, so the next start reads them
/// again.
#[iggy_harness(
    server(connectors_runtime(config_path = "tests/fluss/source.toml")),
    seed = seeds::connector_stream
)]
async fn given_every_batch_rejected_should_stop_with_the_start_offsets_saved(
    harness: &TestHarness,
    fixture: FlussSourceRawStreamFixture,
) {
    let state_path = state_path(harness);
    let start_offset = fixture.existing_payloads().len() as i64;
    wait_for_saved_offset(&state_path, 0, start_offset).await;

    fixture
        .append_rows(&test_payloads())
        .await
        .expect("Failed to append rows");

    let api_url = harness
        .connectors_runtime()
        .expect("connectors runtime")
        .http_url();
    // Each rejected batch already reports an error, so wait for the one that stops the source.
    wait_for_last_error(&Client::new(), &api_url, NACK_LIMIT).await;
    let saved = saved_state(&state_path).expect("The state should still decode");
    assert_eq!(
        saved.bucket_offsets.get(&0),
        Some(&start_offset),
        "Rejected rows should not move the saved offset"
    );
    assert_eq!(saved.messages_produced, 0);
    let client = harness.root_client().await.unwrap();
    assert!(
        poll_once(&client, DRAIN_BATCH).await.is_empty(),
        "A rejected batch should leave nothing in the topic"
    );
}

#[iggy_harness(
    server(connectors_runtime(config_path = "tests/fluss/source.toml")),
    seed = seeds::connector_stream
)]
async fn given_sasl_credentials_should_read_rows_and_keep_the_password_out_of_the_log(
    harness: &TestHarness,
    fixture: FlussSourceSaslFixture,
) {
    let payloads = test_payloads();
    fixture
        .append_rows(&payloads)
        .await
        .expect("Failed to append rows");

    let client = harness.root_client().await.unwrap();
    let received = drain_records_and_settle(&client, payloads.len(), DELIVERY_TIMEOUT).await;

    assert_each_row_once(&received, &payloads);
    assert_password_not_logged(harness, fixture.password());
}

#[iggy_harness(
    server(connectors_runtime(config_path = "tests/fluss/source.toml")),
    seed = seeds::connector_stream
)]
async fn given_wrong_sasl_password_should_fail_to_start_without_logging_it(
    harness: &TestHarness,
    fixture: FlussSourceWrongPasswordFixture,
) {
    assert_fails_to_start(harness, "Authentication failed", SOURCE_STATUS_TIMEOUT).await;
    assert_password_not_logged(harness, fixture.wrong_password());
}

fn assert_password_not_logged(harness: &TestHarness, password: &str) {
    let (stdout, stderr) = harness
        .connectors_runtime()
        .expect("connectors runtime")
        .collect_logs();
    assert!(
        !stdout.contains(password) && !stderr.contains(password),
        "The SASL password should not reach the runtime log"
    );
}

fn test_payloads() -> Vec<String> {
    (0..TEST_ROW_COUNT)
        .map(|index| format!("fluss-payload-{index}"))
        .collect()
}

fn numbered_payloads(name: &str, count: usize) -> Vec<String> {
    (0..count).map(|index| format!("{name}-{index}")).collect()
}

/// With no failure in the way nothing is sent twice, so each row arrives once and no other
/// row does.
fn assert_each_row_once(received: &[FlussRecord], payloads: &[String]) {
    let mut copies: HashMap<&str, usize> = HashMap::with_capacity(received.len());
    for record in received {
        *copies.entry(record.payload.as_str()).or_default() += 1;
    }
    let missing: Vec<&String> = payloads
        .iter()
        .filter(|payload| !copies.contains_key(payload.as_str()))
        .collect();
    let repeated: Vec<(&&str, &usize)> = copies.iter().filter(|(_, count)| **count > 1).collect();
    let expected: HashSet<&str> = payloads.iter().map(String::as_str).collect();
    let unexpected: Vec<&&str> = copies
        .keys()
        .filter(|payload| !expected.contains(**payload))
        .collect();

    assert!(
        missing.is_empty() && repeated.is_empty() && unexpected.is_empty(),
        "Each of {} rows should arrive once. Missing: {missing:?}, repeated: {repeated:?}, \
         unexpected: {unexpected:?}",
        payloads.len()
    );
    assert_message_ids(received);
}

/// The offsets each bucket delivered, in ascending order.
fn offsets_by_bucket(received: &[FlussRecord]) -> HashMap<i32, Vec<i64>> {
    let mut offsets: HashMap<i32, Vec<i64>> = HashMap::new();
    for record in received {
        offsets
            .entry(record.bucket)
            .or_default()
            .push(record.offset);
    }
    for bucket_offsets in offsets.values_mut() {
        bucket_offsets.sort_unstable();
    }
    offsets
}

/// At-least-once delivery may repeat a row, so this checks that every row arrived, not how
/// many times.
fn assert_rows_delivered(received: &[FlussRecord], payloads: &[String], first_offset: i64) {
    for (index, payload) in payloads.iter().enumerate() {
        let offset = first_offset + index as i64;
        assert!(
            received
                .iter()
                .any(|record| record.offset == offset && &record.payload == payload),
            "Row at Fluss offset {offset} ({payload}) was never delivered, got {} messages",
            received.len()
        );
    }
    assert_message_ids(received);
}

/// Every delivery of a row, a repeat included, carries the id derived from its bucket and
/// offset, so a consumer can match a replayed row with the first one.
fn assert_message_ids(received: &[FlussRecord]) {
    for record in received {
        assert_eq!(
            record.message_id,
            expected_message_id(record.bucket, record.offset),
            "Message id of the row at Fluss offset {} should come from its bucket and offset",
            record.offset
        );
    }
}

/// The offset fills the low 64 bits, the bucket the next 32, and the top bit is set so that
/// no id is zero.
fn expected_message_id(bucket: i32, offset: i64) -> u128 {
    (1 << 127) | ((bucket as u128) << 64) | offset as u128
}

async fn poll_records(
    client: &IggyClient,
    expected_rows: usize,
    attempts: usize,
) -> Vec<FlussRecord> {
    into_records(poll_until_rows(client, expected_rows, attempts).await)
}

fn into_records(received: Vec<PolledMessage>) -> Vec<FlussRecord> {
    received
        .into_iter()
        .map(|message| {
            let mut record = FlussRecord::deserialize(&message.payload).unwrap_or_else(|error| {
                panic!("Unexpected Fluss row {}: {error}", message.payload)
            });
            record.message_id = message.id;
            record
        })
        .collect()
}

async fn poll_payloads(client: &IggyClient, expected_rows: usize, attempts: usize) -> Vec<Value> {
    poll_until_rows(client, expected_rows, attempts)
        .await
        .into_iter()
        .map(|message| message.payload)
        .collect()
}

/// Polls until messages for `expected_rows` distinct rows have arrived, keeping any repeats an
/// at-least-once redelivery produced.
async fn poll_until_rows(
    client: &IggyClient,
    expected_rows: usize,
    attempts: usize,
) -> Vec<PolledMessage> {
    let mut received: Vec<PolledMessage> = Vec::new();
    for _ in 0..attempts {
        received.extend(poll_once(client, 10).await);
        if distinct_rows(&received) >= expected_rows {
            break;
        }
        sleep(Duration::from_millis(POLL_INTERVAL_MS)).await;
    }
    received
}

/// The same for many rows: reads up to a thousand messages at a time and waits only while the
/// topic has nothing new, until `limit` has passed.
async fn drain_rows(
    client: &IggyClient,
    expected_rows: usize,
    limit: Duration,
) -> Vec<PolledMessage> {
    let deadline = Instant::now() + limit;
    let mut received: Vec<PolledMessage> = Vec::new();
    while distinct_rows(&received) < expected_rows && Instant::now() < deadline {
        let polled = poll_once(client, DRAIN_BATCH).await;
        if polled.is_empty() {
            sleep(DRAIN_IDLE_PAUSE).await;
        }
        received.extend(polled);
    }
    received
}

/// Reads until `expected_rows` rows have arrived, then listens for [`SETTLE_TIME`] more, so a
/// row sent twice is in the result too.
async fn drain_records_and_settle(
    client: &IggyClient,
    expected_rows: usize,
    limit: Duration,
) -> Vec<FlussRecord> {
    let mut received = drain_rows(client, expected_rows, limit).await;
    received.extend(drain_rows(client, usize::MAX, SETTLE_TIME).await);
    into_records(received)
}

/// A row is told apart by its bucket and offset, which every fixture adds to the payload.
fn distinct_rows(received: &[PolledMessage]) -> usize {
    received
        .iter()
        .filter_map(|message| {
            Some((
                message.payload["_fluss_bucket"].as_i64()?,
                message.payload["_fluss_offset"].as_i64()?,
            ))
        })
        .collect::<HashSet<_>>()
        .len()
}

/// One read of the topic. The consumer commits what it reads, so the next read continues
/// after it.
async fn poll_once(client: &IggyClient, count: u32) -> Vec<PolledMessage> {
    let stream_id: Identifier = seeds::names::STREAM.try_into().unwrap();
    let topic_id: Identifier = seeds::names::TOPIC.try_into().unwrap();
    let consumer_id: Identifier = "fluss_test_consumer".try_into().unwrap();

    let Ok(polled) = client
        .poll_messages(
            &stream_id,
            &topic_id,
            DEFAULT_PARTITION_ID,
            &Consumer::new(consumer_id),
            &PollingStrategy::next(),
            PollOptions {
                count,
                commit: CommitPolicy::OnRead,
                ..PollOptions::default()
            },
        )
        .await
    else {
        return Vec::new();
    };
    polled
        .messages
        .into_iter()
        .map(|message| {
            let payload = serde_json::from_slice(&message.payload).unwrap_or_else(|error| {
                panic!(
                    "Apache Iggy message is not JSON ({error}): {}",
                    String::from_utf8_lossy(&message.payload)
                )
            });
            PolledMessage {
                id: message.header.id,
                payload,
            }
        })
        .collect()
}

async fn source_stats(http: &Client, api_url: &str) -> Option<ConnectorStats> {
    let response = http
        .get(format!("{api_url}/stats"))
        .header("api-key", API_KEY)
        .send()
        .await
        .ok()?;
    let stats = response.json::<ConnectorRuntimeStats>().await.ok()?;
    stats
        .connectors
        .into_iter()
        .find(|connector| connector.key == SOURCE_KEY)
}

async fn wait_for_source_errors(http: &Client, api_url: &str, minimum_errors: u64) {
    timeout(SEND_FAILURE_TIMEOUT, async {
        loop {
            if let Some(source) = source_stats(http, api_url).await
                && source.status == ConnectorStatus::Error
                && source.errors >= minimum_errors
            {
                return;
            }
            sleep(Duration::from_millis(POLL_INTERVAL_MS)).await;
        }
    })
    .await
    .expect("Apache Fluss source did not report the rejected batch");
}

async fn wait_for_source_status(
    http: &Client,
    api_url: &str,
    status: ConnectorStatus,
) -> SourceInfoResponse {
    wait_for_source_status_within(http, api_url, status, SOURCE_STATUS_TIMEOUT).await
}

async fn wait_for_source_status_within(
    http: &Client,
    api_url: &str,
    status: ConnectorStatus,
    limit: Duration,
) -> SourceInfoResponse {
    timeout(limit, async {
        loop {
            if let Ok(response) = http
                .get(format!("{api_url}/sources"))
                .header("api-key", API_KEY)
                .send()
                .await
                && let Ok(sources) = response.json::<Vec<SourceInfoResponse>>().await
                && let Some(source) = sources.into_iter().find(|source| source.key == SOURCE_KEY)
                && source.status == status
            {
                return source;
            }
            sleep(Duration::from_millis(POLL_INTERVAL_MS)).await;
        }
    })
    .await
    .unwrap_or_else(|_| panic!("Apache Fluss source never reached status {status:?}"))
}

async fn wait_for_saved_offset(path: &Path, bucket: i32, offset: i64) -> SavedState {
    wait_for_saved_offsets(path, &HashMap::from([(bucket, offset)]), STATE_FILE_TIMEOUT).await
}

/// Waits until the state on disk holds each of the given offsets for its bucket.
async fn wait_for_saved_offsets(
    path: &Path,
    offsets: &HashMap<i32, i64>,
    limit: Duration,
) -> SavedState {
    timeout(limit, async {
        loop {
            if let Some(state) = saved_state(path)
                && offsets
                    .iter()
                    .all(|(bucket, offset)| state.bucket_offsets.get(bucket) == Some(offset))
            {
                return state;
            }
            sleep(Duration::from_millis(POLL_INTERVAL_MS)).await;
        }
    })
    .await
    .unwrap_or_else(|_| {
        panic!(
            "Apache Fluss source never saved the offsets {offsets:?}, the state holds {:?}",
            saved_state(path).map(|state| state.bucket_offsets)
        )
    })
}

fn saved_state(path: &Path) -> Option<SavedState> {
    let bytes = std::fs::read(path).ok()?;
    ConnectorState(bytes).deserialize::<SavedState>("Apache Fluss test", 1)
}

fn state_path(harness: &TestHarness) -> PathBuf {
    harness
        .connectors_runtime()
        .expect("connectors runtime")
        .state_path()
        .join(STATE_FILE)
}

fn runtime_log(harness: &TestHarness) -> String {
    harness
        .connectors_runtime()
        .expect("connectors runtime")
        .collect_logs()
        .0
}

async fn wait_for_runtime_log(harness: &TestHarness, line: &str, limit: Duration) {
    timeout(limit, async {
        while !runtime_log(harness).contains(line) {
            sleep(Duration::from_millis(POLL_INTERVAL_MS)).await;
        }
    })
    .await
    .unwrap_or_else(|_| panic!("The connectors runtime never logged `{line}`"));
}

/// The source failed to open. The runtime reports that as a generic initialization error, so
/// the reason is looked up in the log the connector writes.
async fn assert_fails_to_start(harness: &TestHarness, reason: &str, limit: Duration) {
    let api_url = harness
        .connectors_runtime()
        .expect("connectors runtime")
        .http_url();

    let source =
        wait_for_source_status_within(&Client::new(), &api_url, ConnectorStatus::Error, limit)
            .await;

    assert!(
        source.last_error.is_some(),
        "A source that failed to open should report an error"
    );
    let log = runtime_log(harness);
    assert!(
        log.contains("Failed to open Apache Fluss source") && log.contains(reason),
        "The source should log why it failed to open, expected `{reason}`"
    );
}

async fn wait_for_last_error(http: &Client, api_url: &str, reason: &str) {
    timeout(SOURCE_STATUS_TIMEOUT, async {
        loop {
            if let Ok(response) = http
                .get(format!("{api_url}/sources"))
                .header("api-key", API_KEY)
                .send()
                .await
                && let Ok(sources) = response.json::<Vec<SourceInfoResponse>>().await
                && let Some(source) = sources.into_iter().find(|source| source.key == SOURCE_KEY)
                && source.status == ConnectorStatus::Error
                && source
                    .last_error
                    .is_some_and(|error| error.message.contains(reason))
            {
                return;
            }
            sleep(Duration::from_millis(POLL_INTERVAL_MS)).await;
        }
    })
    .await
    .unwrap_or_else(|_| panic!("Apache Fluss source never reported `{reason}`"));
}

/// Closes the source and opens it again inside the running runtime.
async fn restart_source(http: &Client, api_url: &str) {
    let response = http
        .post(format!("{api_url}/sources/{SOURCE_KEY}/restart"))
        .header("api-key", API_KEY)
        .send()
        .await
        .expect("The restart request should be sent");
    assert!(
        response.status().is_success(),
        "The source restart should succeed, got {}",
        response.status()
    );
    wait_for_source_status(http, api_url, ConnectorStatus::Running).await;
}

async fn wait_for_file(path: &Path) {
    timeout(STATE_FILE_TIMEOUT, async {
        while !path.exists() {
            sleep(Duration::from_millis(POLL_INTERVAL_MS)).await;
        }
    })
    .await
    .expect("Apache Fluss source did not persist its start offsets");
}
