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

//! Fixtures that more than one connector test package shares.

pub mod elasticsearch;
pub mod floci;
pub mod headers;
pub mod influxdb;
pub mod influxdb_v3;
pub mod postgres;
pub mod random_source_liveness;

use iggy_common::IggyTimestamp;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

/// Prefix on every test container name, so that one `name=^iggy-test-` filter
/// reaps them all. A new fixture only has to use `unique_container_name` (or a
/// fixed `iggy-test-<svc>` for reuse containers) to be covered.
pub const TEST_CONTAINER_PREFIX: &str = "iggy-test-";

/// Unique per-test container name for ephemeral fixtures. Reuse fixtures
/// (elasticsearch, doris) use a fixed `iggy-test-<svc>` literal instead, since
/// the stable name is what lets later test processes attach to the same one.
pub fn unique_container_name(service: &str) -> String {
    format!(
        "{TEST_CONTAINER_PREFIX}{service}-{}",
        Uuid::new_v4().simple()
    )
}

const ONE_DAY_MICROS: u64 = 24 * 60 * 60 * 1_000_000;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct TestMessage {
    pub id: u64,
    pub name: String,
    pub count: u32,
    pub amount: f64,
    pub active: bool,
    pub timestamp: i64,
}

pub fn create_test_messages(count: usize) -> Vec<TestMessage> {
    let base_timestamp = IggyTimestamp::now().as_micros();
    (1..=count)
        .map(|i| TestMessage {
            id: i as u64,
            name: format!("user_{}", i - 1),
            count: ((i - 1) * 10) as u32,
            amount: (i - 1) as f64 * 100.0,
            active: (i - 1) % 2 == 0,
            timestamp: (base_timestamp + (i - 1) as u64 * ONE_DAY_MICROS) as i64,
        })
        .collect()
}
