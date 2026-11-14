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
//! Elasticsearch rejects the document. `to_owned_value` parses with simd-json and then keeps the
//! last value of each repeated key.
//!
//! The serde path of simd-json keeps the last value too, but it recurses through several calls
//! per nesting level. At the 1024 levels that simd-json accepts, a debug build needed more than
//! 2 MiB of stack on that path and less than 1 MiB on the native one, in every run on aarch64
//! macOS, and a tokio worker thread has 2 MiB. The pass below walks the value with a list on the heap, so its stack use does not
//! grow with depth. `given_json_at_the_simd_json_depth_limit_should_parse_within_a_tokio_worker_stack`
//! guards this.

use simd_json::{OwnedValue, owned};

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

// Only containers are queued, so a document without nested objects or arrays never allocates
// the list. An object is rebuilt with `insert`, which keeps the first position of a repeated key
// and its last value. The last value is what simd-json keeps in an object of more than 32 keys.
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
                // Only an object on the vec backend can hold a repeated key, because simd-json
                // inserts into a hash-backed one with a check.
                if fields.is_vec() && repeats_a_key(fields.keys().map(String::as_str)) {
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

/// How many keys `repeats_a_key` sorts on the stack. A vec-backed object holds at most 32, the
/// size of halfbrown's `HashMap`, which simd-json's object is. The size only affects speed.
const SORTED_KEYS: usize = 32;

/// Whether a key repeats. More keys than the buffer holds count as a repeat, so the object is
/// rebuilt, which leaves it unchanged when no key repeats.
fn repeats_a_key<'k>(keys: impl Iterator<Item = &'k str>) -> bool {
    let mut sorted = [""; SORTED_KEYS];
    let mut len = 0;
    for key in keys {
        let Some(slot) = sorted.get_mut(len) else {
            return true;
        };
        *slot = key;
        len += 1;
    }
    let sorted = &mut sorted[..len];
    // Keys of different lengths compare as integers, so mixed keys rarely compare their bytes.
    sorted.sort_unstable_by(|a, b| a.len().cmp(&b.len()).then_with(|| a.cmp(b)));
    sorted.windows(2).any(|pair| pair[0] == pair[1])
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
        // 30 and 31 give 32 and 33 entries, either side of the size where simd-json switches to a
        // hash map, which keeps the last value by itself.
        for others in [1, 30, 31, 40] {
            let owned = to_owned_value(&mut repeated_env(others)).expect("valid JSON");

            assert_eq!(
                owned.as_object().map(|o| o.len()),
                Some(others + 1),
                "{others}"
            );
            assert_eq!(owned.get_str("env"), Some("prod"), "{others}");
        }
    }

    fn keys(count: usize) -> Vec<String> {
        (0..count).map(|i| format!("key_{i:02}")).collect()
    }

    #[test]
    fn given_keys_within_the_sort_buffer_should_report_only_a_real_repeat() {
        let mut keys = keys(SORTED_KEYS);
        assert!(!repeats_a_key(keys.iter().map(String::as_str)));

        keys[SORTED_KEYS - 1] = keys[0].clone();
        assert!(repeats_a_key(keys.iter().map(String::as_str)));
    }

    // No parsed object reaches this case: a vec-backed object holds at most `SORTED_KEYS` keys.
    #[test]
    fn given_more_keys_than_the_sort_buffer_should_report_a_possible_repeat() {
        let mut keys = keys(SORTED_KEYS + 1);
        assert!(repeats_a_key(keys.iter().map(String::as_str)));

        keys[SORTED_KEYS] = keys[0].clone();
        assert!(repeats_a_key(keys.iter().map(String::as_str)));
    }

    #[test]
    fn given_repeated_key_in_nested_objects_should_keep_the_last_value_at_the_first_position() {
        for (json, expected) in [
            (
                r#"{"outer":{"k":1,"k":2},"list":[{"k":3,"k":4}]}"#,
                r#"{"outer":{"k":2},"list":[{"k":4}]}"#,
            ),
            (
                r#"{"a":{"x":1,"x":2},"b":1,"a":{"y":1,"y":2}}"#,
                r#"{"a":{"y":2},"b":1}"#,
            ),
        ] {
            let value = to_owned_value(&mut json.as_bytes().to_vec()).expect("valid JSON");

            assert_eq!(
                simd_json::to_string(&value).expect("serialize"),
                expected,
                "{json}"
            );
        }
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
            r#"{"text":"tab\tquote\"slash\\é\u00e9\ud83d\ude00","esc\"aped key":"",
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
                    to_owned_value(&mut document.into_bytes()).expect("valid JSON");
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
        }
    }
}
