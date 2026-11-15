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

//! One set of user-header cases that every sink runs through its header
//! encoder, so that a difference in naming between sinks shows up as a test
//! failure in the sink that differs.
//!
//! The expected names are literals. They are the contract of
//! `iggy_connector_sdk::HeaderName`, written out by hand so that a change in
//! the SDK rendering breaks the sink tests too.

use iggy_common::{HeaderKey, HeaderKind, HeaderValue};
use std::collections::BTreeMap;

/// Label of the case that RabbitMQ cannot publish: its name is longer than
/// the 255-byte AMQP short-string limit.
pub const LONG_RAW_KEY_CASE: &str = "raw_key_255_bytes";

/// One user header and the name that a sink must write for it.
#[derive(Debug, Clone)]
pub struct HeaderCase {
    /// Stable label. A sink that cannot write a case skips it by this label.
    pub label: &'static str,
    pub key: HeaderKey,
    /// A string value equal to `label`, so that a test can match the value
    /// that a backend returns to its case.
    pub value: HeaderValue,
    pub expected_name: String,
}

/// Every header case, in a stable order.
pub fn header_cases() -> Vec<HeaderCase> {
    let long_raw_key = [0u8; 255];
    vec![
        case("plain_string", string_key("trace-id"), "trace-id"),
        case("string_one", string_key("1"), "1"),
        case("int32_one", HeaderKey::from(1i32), "~int32:1"),
        case("uint32_one", HeaderKey::from(1u32), "~uint32:1"),
        case("raw_key", raw_key(HeaderKind::Raw, &[1, 2]), "~raw:AQI"),
        case("tilde_string", string_key("~note"), "~string:~note"),
        case(
            "lookalike_int32",
            string_key("~int32:1"),
            "~string:~int32:1",
        ),
        case(
            "lookalike_string_b64",
            string_key("~string_b64:xyz"),
            "~string:~string_b64:xyz",
        ),
        case(
            "non_utf8_string",
            raw_key(HeaderKind::String, &[0xff, 0xfe]),
            "~string_b64:__4",
        ),
        case("bool_true", HeaderKey::from(true), "~bool:true"),
        case("bool_byte_two", raw_key(HeaderKind::Bool, &[2]), "~bool:02"),
        case(
            "float64_nan",
            HeaderKey::from(f64::NAN),
            "~float64:7ff8000000000000",
        ),
        case(
            "float64_negative_zero",
            HeaderKey::from(-0.0f64),
            "~float64:8000000000000000",
        ),
        HeaderCase {
            label: LONG_RAW_KEY_CASE,
            key: raw_key(HeaderKind::Raw, &long_raw_key),
            value: string_value(LONG_RAW_KEY_CASE),
            expected_name: format!("~raw:{}", "A".repeat(340)),
        },
    ]
}

/// All cases as the header map of one message.
pub fn header_case_map() -> BTreeMap<HeaderKey, HeaderValue> {
    header_cases()
        .into_iter()
        .map(|case| (case.key, case.value))
        .collect()
}

fn case(label: &'static str, key: HeaderKey, expected_name: &str) -> HeaderCase {
    HeaderCase {
        label,
        key,
        value: string_value(label),
        expected_name: expected_name.to_owned(),
    }
}

fn string_key(key: &str) -> HeaderKey {
    HeaderKey::try_from(key).expect("fixture string key is valid")
}

fn raw_key(kind: HeaderKind, bytes: &[u8]) -> HeaderKey {
    HeaderKey::from_raw(kind, bytes).expect("fixture key has a valid width")
}

fn string_value(value: &str) -> HeaderValue {
    HeaderValue::try_from(value).expect("fixture string value is valid")
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeSet;

    #[test]
    fn given_cases_should_have_unique_keys_names_and_labels() {
        let cases = header_cases();
        let keys: BTreeSet<_> = cases.iter().map(|case| &case.key).collect();
        let names: BTreeSet<_> = cases.iter().map(|case| &case.expected_name).collect();
        let labels: BTreeSet<_> = cases.iter().map(|case| case.label).collect();

        assert_eq!(keys.len(), cases.len());
        assert_eq!(names.len(), cases.len());
        assert_eq!(labels.len(), cases.len());
        assert_eq!(header_case_map().len(), cases.len());
    }
}
