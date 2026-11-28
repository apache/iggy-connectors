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

use serde::{Deserialize, Serialize};

pub const STATE_VERSION: u16 = 1;

/// Small and constant size: the runtime rewrites it with every batch.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct State {
    pub version: u16,
    pub server_name: String,
    /// Opaque position of the last commit the runtime confirmed. Never parsed here.
    pub last_acked_position: Option<Vec<u8>>,
    pub messages_produced: u64,
}

impl State {
    pub fn new(server_name: &str) -> Self {
        Self {
            version: STATE_VERSION,
            server_name: server_name.to_owned(),
            last_acked_position: None,
            messages_produced: 0,
        }
    }
}
