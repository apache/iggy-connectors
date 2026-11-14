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
//! Elasticsearch rejects the document. These functions parse with simd-json and then keep the
//! last value of each repeated key.
//!
//! The serde path of simd-json keeps the last value too, but it recurses through several calls
//! per nesting level. At the 1024 levels that simd-json accepts, a debug build needed 2.5 MiB of
//! stack on that path and 640 KiB on the native one (aarch64 macOS), and a tokio worker thread
//! has 2 MiB. The pass below walks the value with a list on the heap, so its stack use does not
//! grow with depth. `given_json_at_the_simd_json_depth_limit_should_parse_within_a_tokio_worker_stack`
//! guards this.

use simd_json::{BorrowedValue, OwnedValue, borrowed, owned};

/// Parse `json` in place. A key that repeats in an object keeps its last value.
pub fn to_owned_value(json: &mut [u8]) -> simd_json::Result<OwnedValue> {
    #[allow(
        clippy::disallowed_methods,
        reason = "the pass below removes repeated keys"
    )]
    let mut value = simd_json::to_owned_value(json)?;
    keep_last_owned(&mut value);
    Ok(value)
}

/// Parse `json` in place, borrowing its strings. A key that repeats in an object keeps its
/// last value.
pub fn to_borrowed_value(json: &mut [u8]) -> simd_json::Result<BorrowedValue<'_>> {
    #[allow(
        clippy::disallowed_methods,
        reason = "the pass below removes repeated keys"
    )]
    let mut value = simd_json::to_borrowed_value(json)?;
    keep_last_borrowed(&mut value);
    Ok(value)
}

// Only containers are queued, so a document without nested objects or arrays never allocates
// the list. An object is rebuilt with `insert`, which keeps the first position and the last
// value, as simd-json does for an object of more than 32 keys.
fn keep_last_owned(root: &mut OwnedValue) {
    let mut pending = Vec::new();
    let mut next = Some(root);
    while let Some(value) = next {
        match value {
            OwnedValue::Array(items) => {
                pending.extend(
                    items.iter_mut().filter(|item| {
                        matches!(item, OwnedValue::Array(_) | OwnedValue::Object(_))
                    }),
                );
            }
            OwnedValue::Object(fields) => {
                if fields.is_vec() && repeats_a_key(|| fields.keys().map(String::as_str)) {
                    let mut unique =
                        owned::Object::with_capacity_and_hasher(fields.len(), Default::default());
                    for (key, field) in std::mem::take(&mut **fields) {
                        unique.insert(key, field);
                    }
                    **fields = unique;
                }
                pending.extend(
                    fields.values_mut().filter(|field| {
                        matches!(field, OwnedValue::Array(_) | OwnedValue::Object(_))
                    }),
                );
            }
            OwnedValue::Static(_) | OwnedValue::String(_) => {}
        }
        next = pending.pop();
    }
}

fn keep_last_borrowed(root: &mut BorrowedValue<'_>) {
    let mut pending = Vec::new();
    let mut next = Some(root);
    while let Some(value) = next {
        match value {
            BorrowedValue::Array(items) => {
                pending.extend(items.iter_mut().filter(|item| {
                    matches!(item, BorrowedValue::Array(_) | BorrowedValue::Object(_))
                }));
            }
            BorrowedValue::Object(fields) => {
                if fields.is_vec() && repeats_a_key(|| fields.keys().map(|key| key.as_ref())) {
                    let mut unique = borrowed::Object::with_capacity_and_hasher(
                        fields.len(),
                        Default::default(),
                    );
                    for (key, field) in std::mem::take(&mut **fields) {
                        unique.insert(key, field);
                    }
                    **fields = unique;
                }
                pending.extend(fields.values_mut().filter(|field| {
                    matches!(field, BorrowedValue::Array(_) | BorrowedValue::Object(_))
                }));
            }
            BorrowedValue::Static(_) | BorrowedValue::String(_) => {}
        }
        next = pending.pop();
    }
}

/// Only an object on the vec backend can hold a repeated key, because simd-json inserts into a
/// hash-backed one with a check. That is why the callers ask `is_vec` first.
fn repeats_a_key<'k, I: Iterator<Item = &'k str>>(keys: impl Fn() -> I) -> bool {
    keys()
        .enumerate()
        .any(|(i, key)| keys().take(i).any(|earlier| earlier == key))
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
        let json = br#"{"outer":{"k":1,"k":2},"list":[{"k":3,"k":4}]}"#;
        let expected = r#"{"outer":{"k":2},"list":[{"k":4}]}"#;

        let owned = to_owned_value(&mut json.to_vec()).expect("valid JSON");
        let mut bytes = json.to_vec();
        let borrowed = to_borrowed_value(&mut bytes).expect("valid JSON");

        assert_eq!(simd_json::to_string(&owned).expect("serialize"), expected);
        assert_eq!(
            simd_json::to_string(&borrowed).expect("serialize"),
            expected
        );
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

    /// The runtime and the plugins parse on tokio worker threads, which have 2 MiB of stack.
    const WORKER_STACK_BYTES: usize = 2 * 1024 * 1024;

    // simd-json refuses more than 1024 levels, so this is the deepest message that gets parsed.
    #[test]
    fn given_json_at_the_simd_json_depth_limit_should_parse_within_a_tokio_worker_stack() {
        std::thread::Builder::new()
            .stack_size(WORKER_STACK_BYTES)
            .spawn(|| {
                let arrays = format!("{}{}", "[".repeat(1024), "]".repeat(1024));
                let objects = format!("{}null{}", r#"{"a":"#.repeat(1024), "}".repeat(1024));
                for document in [arrays, objects] {
                    to_owned_value(&mut document.clone().into_bytes()).expect("valid JSON");
                    let mut bytes = document.into_bytes();
                    to_borrowed_value(&mut bytes).expect("valid JSON");
                }
            })
            .expect("spawn a thread with a worker stack")
            .join()
            .expect("the parse fits in a worker stack");
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
