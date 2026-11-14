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

//! JSON parsing for connectors.
//!
//! `simd_json::to_owned_value` and `simd_json::to_borrowed_value` keep every copy of a key that
//! repeats in an object of up to 32 keys, and only the last value in a larger object. Readers
//! then disagree: `get` returns the first copy, serde_json and most backends take the last, and
//! Elasticsearch rejects the document. These functions parse through the serde path of
//! simd-json instead, which keeps the last value of a repeated key in every object.

use simd_json::{BorrowedValue, OwnedValue};

/// Parse `json` in place. A key that repeats in an object keeps its last value.
pub fn to_owned_value(json: &mut [u8]) -> simd_json::Result<OwnedValue> {
    simd_json::serde::from_slice(json)
}

/// Parse `json` in place, borrowing its strings. A key that repeats in an object keeps its
/// last value.
pub fn to_borrowed_value(json: &mut [u8]) -> simd_json::Result<BorrowedValue<'_>> {
    simd_json::serde::from_slice(json)
}

#[cfg(test)]
mod tests {
    use super::*;
    use simd_json::prelude::*;

    /// `env` twice, with `others` distinct keys between the two copies.
    fn repeated_env(others: usize) -> Vec<u8> {
        let fields: String = (0..others).map(|i| format!(r#""k{i}":{i},"#)).collect();
        format!(r#"{{"env":"staging",{fields}"env":"prod"}}"#).into_bytes()
    }

    #[test]
    fn given_repeated_key_should_keep_the_last_value_at_any_object_size() {
        // Above 32 keys, simd-json switches to a hash map, which already keeps the last value.
        for others in [1, 40] {
            let owned = to_owned_value(&mut repeated_env(others)).expect("valid JSON");
            let mut bytes = repeated_env(others);
            let borrowed = to_borrowed_value(&mut bytes).expect("valid JSON");

            assert_eq!(
                owned.as_object().map(|o| o.len()),
                Some(others + 1),
                "{others}"
            );
            assert_eq!(owned.get_str("env"), Some("prod"), "{others}");
            assert_eq!(
                borrowed.as_object().map(|o| o.len()),
                Some(others + 1),
                "{others}"
            );
            assert_eq!(borrowed.get_str("env"), Some("prod"), "{others}");
        }
    }

    #[test]
    fn given_repeated_key_in_nested_objects_should_keep_the_last_value() {
        let mut bytes = br#"{"outer":{"k":1,"k":2},"list":[{"k":3,"k":4}]}"#.to_vec();

        let value = to_owned_value(&mut bytes).expect("valid JSON");

        let outer = value.get("outer").expect("outer");
        assert_eq!(outer.as_object().map(|o| o.len()), Some(1));
        assert_eq!(outer.get_i64("k"), Some(2));
        let item = value
            .get("list")
            .and_then(|list| list.as_array())
            .and_then(|items| items.first())
            .expect("list item");
        assert_eq!(item.as_object().map(|o| o.len()), Some(1));
        assert_eq!(item.get_i64("k"), Some(4));
    }

    // Debug output names the variant, so `I64(1)` and `U64(1)` do not compare equal, and an
    // object of up to 32 keys prints in insertion order.
    #[test]
    #[allow(
        clippy::disallowed_methods,
        reason = "compares the result with the parse that this module replaces"
    )]
    fn given_json_without_repeated_keys_should_parse_like_simd_json() {
        let documents = [
            r#"{"int":1,"neg":-2,"u64":18446744073709551615,"i64":-9223372036854775808}"#,
            r#"{"float":0.1,"exp":1.5e300,"true":true,"false":false,"null":null}"#,
            r#"{"text":"tab\tquote\"slash\\éé😀","esc\"aped key":"",
                "empty":{},"none":[],"nested":[1,"two",{"three":[null,{"four":4}]}]}"#,
            r#""a string""#,
            "42",
            "-0.5",
            "[]",
            "null",
        ];
        for document in documents {
            let expected =
                simd_json::to_owned_value(&mut document.as_bytes().to_vec()).expect("valid JSON");
            let actual = to_owned_value(&mut document.as_bytes().to_vec()).expect("valid JSON");
            assert_eq!(format!("{actual:?}"), format!("{expected:?}"), "{document}");

            let mut expected_bytes = document.as_bytes().to_vec();
            let expected = simd_json::to_borrowed_value(&mut expected_bytes).expect("valid JSON");
            let mut actual_bytes = document.as_bytes().to_vec();
            let actual = to_borrowed_value(&mut actual_bytes).expect("valid JSON");
            assert_eq!(format!("{actual:?}"), format!("{expected:?}"), "{document}");
        }
    }

    #[test]
    fn given_invalid_json_should_fail_like_simd_json() {
        for document in ["", "{", r#"{"a":}"#, "[1,]", "nul", r#"{"a":1}x"#] {
            assert!(
                to_owned_value(&mut document.as_bytes().to_vec()).is_err(),
                "{document}"
            );
            assert!(
                to_borrowed_value(&mut document.as_bytes().to_vec()).is_err(),
                "{document}"
            );
        }
    }
}
