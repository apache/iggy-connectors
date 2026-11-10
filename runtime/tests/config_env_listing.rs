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

use assert_cmd::Command;
use std::collections::HashSet;
use std::ffi::OsStr;
use std::path::Path;
use std::time::Duration;

const BINARY: &str = env!("CARGO_BIN_EXE_iggy-connectors");
const LIST_ENV_VARS_TIMEOUT: Duration = Duration::from_secs(5);

#[test]
fn config_env_listing_exits_before_startup() {
    let directory = tempfile::tempdir().expect("temporary directory");
    let config_path = directory.path().join("invalid-config.toml");
    let dotenv_path = directory.path().join("invalid.env");
    std::fs::write(&config_path, "not valid toml = [").expect("write invalid config");
    std::fs::write(&dotenv_path, "not a dotenv assignment").expect("write invalid dotenv");

    let entries_before: HashSet<_> = std::fs::read_dir(directory.path())
        .expect("read temporary directory")
        .map(|entry| entry.expect("directory entry").file_name())
        .collect();
    let names = list_config_env_vars_in(
        directory.path(),
        [
            ("IGGY_CONNECTORS_CONFIG_PATH", config_path.into_os_string()),
            ("IGGY_CONNECTORS_ENV_PATH", dotenv_path.into_os_string()),
            ("IGGY_CONNECTORS_HTTP_ADDRESS", "invalid-value".into()),
        ],
    );

    assert_eq!(
        names,
        list_config_env_vars(),
        "output changed with invalid config, dotenv or env values"
    );
    let entries_after: HashSet<_> = std::fs::read_dir(directory.path())
        .expect("read temporary directory")
        .map(|entry| entry.expect("directory entry").file_name())
        .collect();
    assert_eq!(
        entries_after, entries_before,
        "listing created startup side effects"
    );

    assert!(
        names.windows(2).all(|pair| pair[0] < pair[1]),
        "output must be sorted and deduplicated"
    );
    assert!(
        names
            .iter()
            .all(|name| name == "IGGY_DISPLAY_CONFIG" || name.starts_with("IGGY_CONNECTORS_")),
        "output contained a variable outside IGGY_CONNECTORS_"
    );
}

#[test]
fn config_env_listing_includes_connector_templates() {
    let names = list_config_env_vars();
    let names: HashSet<_> = names.iter().map(String::as_str).collect();

    for expected in [
        "IGGY_CONNECTORS_SINK_<KEY>_PLUGIN_CONFIG_<FIELD>",
        "IGGY_CONNECTORS_SOURCE_<KEY>_PLUGIN_CONFIG_<FIELD>",
        "IGGY_CONNECTORS_SINK_<KEY>_PLUGIN_CONFIG_FORMAT",
        "IGGY_CONNECTORS_SOURCE_<KEY>_PLUGIN_CONFIG_FORMAT",
        "IGGY_CONNECTORS_SINK_<KEY>_ENABLED",
        "IGGY_CONNECTORS_SOURCE_<KEY>_ENABLED",
        "IGGY_CONNECTORS_CONFIG_PATH",
        "IGGY_CONNECTORS_ENV_PATH",
    ] {
        assert!(
            names.contains(expected),
            "connectors did not list {expected}"
        );
    }
    for excluded in [
        "IGGY_CONNECTORS_SINK_<KEY>_KEY",
        "IGGY_CONNECTORS_SINK_<KEY>_VERSION",
        "IGGY_CONNECTORS_SOURCE_<KEY>_KEY",
        "IGGY_CONNECTORS_SOURCE_<KEY>_VERSION",
    ] {
        assert!(!names.contains(excluded), "connectors listed {excluded}");
    }

    assert_eq!(
        names
            .iter()
            .filter(|name| **name == "IGGY_CONNECTORS_CONNECTORS_CONFIG_TYPE")
            .count(),
        1,
        "enum variants should produce one deduplicated tag name"
    );
}

fn list_config_env_vars() -> Vec<String> {
    let directory = tempfile::tempdir().expect("temporary directory");
    list_config_env_vars_in(directory.path(), std::iter::empty::<(&str, &str)>())
}

fn list_config_env_vars_in<I, K, V>(directory: &Path, env: I) -> Vec<String>
where
    I: IntoIterator<Item = (K, V)>,
    K: AsRef<OsStr>,
    V: AsRef<OsStr>,
{
    let output = Command::new(BINARY)
        .current_dir(directory)
        .arg("--list-config-env-vars")
        .envs(env)
        .timeout(LIST_ENV_VARS_TIMEOUT)
        .output()
        .expect("listing command should run");
    assert!(
        output.status.success(),
        "{BINARY} failed: {:?}\nstderr: {}",
        output.status,
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(output.stderr.is_empty(), "{BINARY} wrote to stderr");
    let stdout = String::from_utf8(output.stdout).expect("UTF-8 output");
    let names = stdout.lines().map(str::to_owned).collect::<Vec<_>>();
    assert!(!names.is_empty(), "{BINARY} returned no variables");
    names
}
