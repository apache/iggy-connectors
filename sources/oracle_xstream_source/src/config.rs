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

use std::str::FromStr;
use std::time::Duration;

use iggy_connector_sdk::Error;
use secrecy::SecretString;
use serde::Deserialize;

const DEFAULT_POLL_INTERVAL: Duration = Duration::from_millis(100);
const DEFAULT_POLL_BUDGET: Duration = Duration::from_millis(500);
const DEFAULT_REATTACH_BACKOFF: Duration = Duration::from_secs(5);
const DEFAULT_MAX_BATCH_EVENTS: usize = 1000;
const DEFAULT_MAX_REATTACH_ATTEMPTS: u32 = 5;
/// Oracle's documented limit for an XStream server name.
const MAX_SERVER_NAME_BYTES: usize = 30;

#[derive(Debug, Deserialize)]
pub struct OracleSourceConfig {
    /// Easy Connect string, for example `dbhost:1521/ORCLCDB`. The outbound server lives in the root of a CDB.
    pub connect_string: String,
    pub username: String,
    pub password: SecretString,
    pub outbound_server: String,
    pub oci_library_path: Option<String>,
    /// `OWNER.TABLE` names to forward. Empty or absent forwards everything the outbound server captures.
    pub tables: Option<Vec<String>>,
    pub poll_interval: Option<String>,
    pub poll_budget: Option<String>,
    pub max_batch_events: Option<usize>,
    pub reattach_backoff: Option<String>,
    pub max_reattach_attempts: Option<u32>,
    pub verbose_logging: Option<bool>,
}

#[derive(Debug, Clone)]
pub struct Settings {
    pub poll_interval: Duration,
    pub poll_budget: Duration,
    pub max_batch_events: usize,
    pub reattach_backoff: Duration,
    pub max_reattach_attempts: u32,
    pub verbose: bool,
}

impl OracleSourceConfig {
    pub fn settings(&self) -> Result<Settings, Error> {
        if self.connect_string.trim().is_empty() {
            return Err(invalid("connect_string must not be empty"));
        }
        if self.username.trim().is_empty() {
            return Err(invalid("username must not be empty"));
        }
        let server_bytes = self.outbound_server.len();
        if server_bytes == 0 || server_bytes > MAX_SERVER_NAME_BYTES {
            return Err(invalid(&format!(
                "outbound_server must be 1 to {MAX_SERVER_NAME_BYTES} bytes, got {server_bytes}"
            )));
        }
        let max_batch_events = self.max_batch_events.unwrap_or(DEFAULT_MAX_BATCH_EVENTS);
        if max_batch_events == 0 {
            return Err(invalid("max_batch_events must be greater than zero"));
        }
        let max_reattach_attempts = self
            .max_reattach_attempts
            .unwrap_or(DEFAULT_MAX_REATTACH_ATTEMPTS);
        if max_reattach_attempts == 0 {
            return Err(invalid("max_reattach_attempts must be greater than zero"));
        }
        Ok(Settings {
            poll_interval: duration(&self.poll_interval, "poll_interval", DEFAULT_POLL_INTERVAL)?,
            poll_budget: duration(&self.poll_budget, "poll_budget", DEFAULT_POLL_BUDGET)?,
            max_batch_events,
            reattach_backoff: duration(
                &self.reattach_backoff,
                "reattach_backoff",
                DEFAULT_REATTACH_BACKOFF,
            )?,
            max_reattach_attempts,
            verbose: self.verbose_logging.unwrap_or(false),
        })
    }
}

fn duration(raw: &Option<String>, name: &str, default: Duration) -> Result<Duration, Error> {
    match raw {
        None => Ok(default),
        Some(text) => humantime::Duration::from_str(text)
            .map(Duration::from)
            .map_err(|error| invalid(&format!("{name} {text:?}: {error}"))),
    }
}

fn invalid(reason: &str) -> Error {
    Error::InvalidConfigValue(reason.to_owned())
}

#[cfg(test)]
mod tests {
    use super::*;

    type Mutation = fn(&mut OracleSourceConfig);

    fn config() -> OracleSourceConfig {
        OracleSourceConfig {
            connect_string: "localhost:1521/ORCLCDB".to_owned(),
            username: "c##xstrmadmin".to_owned(),
            password: SecretString::from("secret".to_owned()),
            outbound_server: "IGGY_XOUT".to_owned(),
            oci_library_path: None,
            tables: None,
            poll_interval: None,
            poll_budget: None,
            max_batch_events: None,
            reattach_backoff: None,
            max_reattach_attempts: None,
            verbose_logging: None,
        }
    }

    #[test]
    fn given_minimal_config_when_building_settings_should_use_defaults() {
        let settings = config().settings().expect("settings");
        assert_eq!(settings.poll_interval, DEFAULT_POLL_INTERVAL);
        assert_eq!(settings.max_batch_events, DEFAULT_MAX_BATCH_EVENTS);
        assert_eq!(
            settings.max_reattach_attempts,
            DEFAULT_MAX_REATTACH_ATTEMPTS
        );
        assert!(!settings.verbose);
    }

    #[test]
    fn given_overrides_when_building_settings_should_parse_humantime_durations() {
        let mut config = config();
        config.poll_interval = Some("250ms".to_owned());
        config.reattach_backoff = Some("2s".to_owned());
        let settings = config.settings().expect("settings");
        assert_eq!(settings.poll_interval, Duration::from_millis(250));
        assert_eq!(settings.reattach_backoff, Duration::from_secs(2));
    }

    #[test]
    fn given_invalid_values_when_building_settings_should_name_the_field() {
        let cases: [(&str, Mutation); 7] = [
            ("connect_string", |c| c.connect_string = " ".to_owned()),
            ("username", |c| c.username.clear()),
            ("outbound_server", |c| c.outbound_server = "S".repeat(31)),
            ("outbound_server", |c| c.outbound_server.clear()),
            ("max_batch_events", |c| c.max_batch_events = Some(0)),
            ("max_reattach_attempts", |c| {
                c.max_reattach_attempts = Some(0)
            }),
            ("poll_budget", |c| c.poll_budget = Some("soon".to_owned())),
        ];
        for (field, mutate) in cases {
            let mut config = config();
            mutate(&mut config);
            let error = config.settings().expect_err("must be rejected");
            assert!(
                matches!(&error, Error::InvalidConfigValue(reason) if reason.contains(field)),
                "{field}: {error:?}"
            );
        }
    }
}
