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

//! Stable, unique names and JSON values for user headers in sinks.
//!
//! A sink that writes user headers to a backend names each one with
//! [`HeaderName`]. The name of a key never depends on the other keys in the
//! message, and two different keys never share a name:
//!
//! | Key                                        | Name                        |
//! | ------------------------------------------ | --------------------------- |
//! | String, valid UTF-8, not starting with `~` | the key itself              |
//! | String, valid UTF-8, starting with `~`     | `~string:<key>`             |
//! | String, not valid UTF-8                    | `~string_b64:<base64url>`   |
//! | any other kind                             | `~<kind>:<value>`           |
//!
//! `<kind>` is the `Display` form of [`HeaderKind`]. `<value>` is decimal for
//! integers, the fixed-width lowercase hex of `to_bits()` for floats, `true` or
//! `false` for bool bytes 1 and 0 (two hex digits for any other byte), and
//! unpadded base64url for raw keys. No generated part contains a `.`, which
//! Elasticsearch and OpenSearch would read as an object path.

use base64::display::Base64Display;
use base64::engine::general_purpose::{STANDARD, URL_SAFE_NO_PAD};
use iggy_common::{HeaderField, HeaderKey, HeaderKind, HeaderValue};
use serde::ser::{SerializeMap, SerializeStruct};
use serde::{Serialize, Serializer};
use std::collections::BTreeMap;
use std::fmt::{self, Display, Formatter};

/// `data_encoding` of a header value written as text.
pub const DATA_ENCODING_UTF8: &str = "utf8";
/// `data_encoding` of a header value written as standard base64.
pub const DATA_ENCODING_BASE64: &str = "base64";

/// The backend name of one user-header key.
///
/// `Display` writes the name. `Serialize` writes it as a string, so it can be
/// a map key. A plain string key is written without a copy.
#[derive(Debug, Clone, Copy)]
pub struct HeaderName<'a>(pub &'a HeaderKey);

impl Display for HeaderName<'_> {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        let key = self.0;
        let bytes = key.as_bytes();
        match key.kind() {
            HeaderKind::String => match key.as_str() {
                Ok(name) if !name.starts_with('~') => f.write_str(name),
                Ok(name) => write!(f, "~string:{name}"),
                Err(_) => write!(
                    f,
                    "~string_b64:{}",
                    Base64Display::new(bytes, &URL_SAFE_NO_PAD)
                ),
            },
            HeaderKind::Raw => write!(f, "~raw:{}", Base64Display::new(bytes, &URL_SAFE_NO_PAD)),
            HeaderKind::Float32 => match fixed::<4>(bytes) {
                Some(bits) => write!(f, "~float32:{:08x}", u32::from_le_bytes(bits)),
                None => write_malformed(f, HeaderKind::Float32, bytes),
            },
            HeaderKind::Float64 => match fixed::<8>(bytes) {
                Some(bits) => write!(f, "~float64:{:016x}", u64::from_le_bytes(bits)),
                None => write_malformed(f, HeaderKind::Float64, bytes),
            },
            kind => {
                write!(f, "~{kind}:")?;
                write_scalar(f, key)
            }
        }
    }
}

impl Serialize for HeaderName<'_> {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.collect_str(self)
    }
}

/// All user headers of one message as a JSON object.
///
/// Each key is a [`HeaderName`]. Each value is an object
/// `{"data": <text>, "data_encoding": "utf8" | "base64"}`, so that a search
/// backend maps one field type per key:
///
/// - a valid UTF-8 String value is its text, `utf8`
/// - a Raw value, or a String value that is not valid UTF-8, is standard
///   base64, `base64`
/// - every other kind is its decimal or boolean text, `utf8`
#[derive(Debug, Clone, Copy)]
pub struct JsonHeaders<'a>(pub &'a BTreeMap<HeaderKey, HeaderValue>);

impl Serialize for JsonHeaders<'_> {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let mut map = serializer.serialize_map(Some(self.0.len()))?;
        for (key, value) in self.0 {
            map.serialize_entry(&HeaderName(key), &JsonHeaderValue(value))?;
        }
        map.end()
    }
}

/// One header value as `{"data", "data_encoding"}`.
#[derive(Debug, Clone, Copy)]
pub struct JsonHeaderValue<'a>(pub &'a HeaderValue);

impl JsonHeaderValue<'_> {
    /// The `data_encoding` that [`JsonHeaderValue`] writes for this value.
    pub fn data_encoding(&self) -> &'static str {
        match self.0.kind() {
            HeaderKind::Raw => DATA_ENCODING_BASE64,
            HeaderKind::String if self.0.as_str().is_err() => DATA_ENCODING_BASE64,
            _ => DATA_ENCODING_UTF8,
        }
    }
}

impl Serialize for JsonHeaderValue<'_> {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let mut object = serializer.serialize_struct("JsonHeaderValue", 2)?;
        object.serialize_field("data", &HeaderData(self.0))?;
        object.serialize_field("data_encoding", self.data_encoding())?;
        object.end()
    }
}

/// The `data` text of one header value.
struct HeaderData<'a>(&'a HeaderValue);

impl Display for HeaderData<'_> {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        let value = self.0;
        let bytes = value.as_bytes();
        match value.kind() {
            HeaderKind::String => match value.as_str() {
                Ok(text) => f.write_str(text),
                Err(_) => write!(f, "{}", Base64Display::new(bytes, &STANDARD)),
            },
            HeaderKind::Raw => write!(f, "{}", Base64Display::new(bytes, &STANDARD)),
            HeaderKind::Float32 => match fixed::<4>(bytes) {
                Some(bits) => write!(f, "{}", f32::from_le_bytes(bits)),
                None => write_hex(f, bytes),
            },
            HeaderKind::Float64 => match fixed::<8>(bytes) {
                Some(bits) => write!(f, "{}", f64::from_le_bytes(bits)),
                None => write_hex(f, bytes),
            },
            _ => write_scalar(f, value),
        }
    }
}

impl Serialize for HeaderData<'_> {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.collect_str(self)
    }
}

/// Writes a bool or integer field. `HeaderField::from_raw` checks the width of
/// every fixed-size kind, so the hex fallback only guards against a field that
/// a future core change builds without that check.
fn write_scalar<T>(f: &mut Formatter<'_>, field: &HeaderField<T>) -> fmt::Result {
    let bytes = field.as_bytes();
    let written = match field.kind() {
        HeaderKind::Bool => match bytes {
            [0] => Some(f.write_str("false")),
            [1] => Some(f.write_str("true")),
            [other] => Some(write!(f, "{other:02x}")),
            _ => None,
        },
        HeaderKind::Int8 => fixed::<1>(bytes).map(|b| write!(f, "{}", i8::from_le_bytes(b))),
        HeaderKind::Int16 => fixed::<2>(bytes).map(|b| write!(f, "{}", i16::from_le_bytes(b))),
        HeaderKind::Int32 => fixed::<4>(bytes).map(|b| write!(f, "{}", i32::from_le_bytes(b))),
        HeaderKind::Int64 => fixed::<8>(bytes).map(|b| write!(f, "{}", i64::from_le_bytes(b))),
        HeaderKind::Int128 => fixed::<16>(bytes).map(|b| write!(f, "{}", i128::from_le_bytes(b))),
        HeaderKind::Uint8 => fixed::<1>(bytes).map(|b| write!(f, "{}", u8::from_le_bytes(b))),
        HeaderKind::Uint16 => fixed::<2>(bytes).map(|b| write!(f, "{}", u16::from_le_bytes(b))),
        HeaderKind::Uint32 => fixed::<4>(bytes).map(|b| write!(f, "{}", u32::from_le_bytes(b))),
        HeaderKind::Uint64 => fixed::<8>(bytes).map(|b| write!(f, "{}", u64::from_le_bytes(b))),
        HeaderKind::Uint128 => fixed::<16>(bytes).map(|b| write!(f, "{}", u128::from_le_bytes(b))),
        HeaderKind::Raw | HeaderKind::String | HeaderKind::Float32 | HeaderKind::Float64 => None,
    };
    written.unwrap_or_else(|| write_hex(f, bytes))
}

fn write_malformed(f: &mut Formatter<'_>, kind: HeaderKind, bytes: &[u8]) -> fmt::Result {
    write!(f, "~{kind}:")?;
    write_hex(f, bytes)
}

fn write_hex(f: &mut Formatter<'_>, bytes: &[u8]) -> fmt::Result {
    bytes.iter().try_for_each(|byte| write!(f, "{byte:02x}"))
}

fn fixed<const N: usize>(bytes: &[u8]) -> Option<[u8; N]> {
    bytes.try_into().ok()
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn key(kind: HeaderKind, bytes: &[u8]) -> HeaderKey {
        HeaderKey::from_raw(kind, bytes).expect("valid test key")
    }

    fn value(kind: HeaderKind, bytes: &[u8]) -> HeaderValue {
        HeaderValue::from_raw(kind, bytes).expect("valid test value")
    }

    fn name(key: &HeaderKey) -> String {
        HeaderName(key).to_string()
    }

    #[test]
    fn given_plain_string_key_should_keep_it_unchanged() {
        assert_eq!(name(&HeaderKey::try_from("trace-id").unwrap()), "trace-id");
        assert_eq!(name(&HeaderKey::try_from("mqtt.qos").unwrap()), "mqtt.qos");
    }

    #[test]
    fn given_string_key_starting_with_tilde_should_prefix_string_kind() {
        assert_eq!(
            name(&HeaderKey::try_from("~int32:1").unwrap()),
            "~string:~int32:1"
        );
    }

    #[test]
    fn given_non_utf8_string_key_should_render_base64url() {
        assert_eq!(
            name(&key(HeaderKind::String, &[0xff, 0xfe])),
            "~string_b64:__4"
        );
    }

    #[test]
    fn given_integer_keys_should_render_kind_and_decimal() {
        assert_eq!(name(&HeaderKey::from(1i32)), "~int32:1");
        assert_eq!(name(&HeaderKey::from(1u32)), "~uint32:1");
        assert_eq!(name(&HeaderKey::from(-5i8)), "~int8:-5");
        assert_eq!(
            name(&HeaderKey::from(u128::MAX)),
            "~uint128:340282366920938463463374607431768211455"
        );
    }

    #[test]
    fn given_float_keys_should_render_fixed_width_hex_bits() {
        assert_eq!(name(&HeaderKey::from(1.5f64)), "~float64:3ff8000000000000");
        assert_eq!(name(&HeaderKey::from(1.5f32)), "~float32:3fc00000");
        assert_eq!(name(&HeaderKey::from(-0.0f64)), "~float64:8000000000000000");
        assert_ne!(
            name(&HeaderKey::from(0.0f64)),
            name(&HeaderKey::from(-0.0f64))
        );
    }

    #[test]
    fn given_bool_keys_should_render_text_or_hex_byte() {
        assert_eq!(name(&HeaderKey::from(true)), "~bool:true");
        assert_eq!(name(&HeaderKey::from(false)), "~bool:false");
        assert_eq!(name(&key(HeaderKind::Bool, &[2])), "~bool:02");
    }

    #[test]
    fn given_raw_key_should_render_unpadded_base64url() {
        assert_eq!(name(&key(HeaderKind::Raw, &[1, 2])), "~raw:AQI");
    }

    #[test]
    fn given_keys_that_stringify_alike_should_get_distinct_names() {
        let keys = [
            HeaderKey::try_from("1").unwrap(),
            HeaderKey::from(1i32),
            HeaderKey::from(1u32),
            HeaderKey::from(1u8),
            key(HeaderKind::Raw, b"1"),
        ];
        let mut names: Vec<String> = keys.iter().map(name).collect();
        names.sort();
        names.dedup();
        assert_eq!(names.len(), keys.len());
    }

    #[test]
    fn given_no_generated_name_should_contain_a_dot() {
        let keys = [
            HeaderKey::from(1.5f64),
            HeaderKey::from(f32::NAN),
            HeaderKey::from(-1i64),
            key(HeaderKind::Raw, &[0xfb, 0xff]),
            key(HeaderKind::String, &[0xff]),
        ];
        for key in &keys {
            assert!(!name(key).contains('.'), "{}", name(key));
        }
    }

    #[test]
    fn given_headers_should_serialize_names_as_map_keys() {
        let mut headers = BTreeMap::new();
        headers.insert(
            HeaderKey::try_from("1").unwrap(),
            HeaderValue::try_from("a").unwrap(),
        );
        headers.insert(HeaderKey::from(1i32), HeaderValue::from(7i32));

        let json = serde_json::to_value(JsonHeaders(&headers)).unwrap();

        assert_eq!(
            json,
            json!({
                "1": {"data": "a", "data_encoding": "utf8"},
                "~int32:1": {"data": "7", "data_encoding": "utf8"},
            })
        );
    }

    #[test]
    fn given_binary_values_should_serialize_standard_base64() {
        let mut headers = BTreeMap::new();
        headers.insert(
            HeaderKey::try_from("raw").unwrap(),
            value(HeaderKind::Raw, &[0xfb, 0xff]),
        );
        headers.insert(
            HeaderKey::try_from("lossy").unwrap(),
            value(HeaderKind::String, &[0xff, 0xfe]),
        );

        let json = serde_json::to_value(JsonHeaders(&headers)).unwrap();

        assert_eq!(
            json["raw"],
            json!({"data": "+/8=", "data_encoding": "base64"})
        );
        assert_eq!(
            json["lossy"],
            json!({"data": "//4=", "data_encoding": "base64"})
        );
    }

    #[test]
    fn given_bool_byte_two_value_should_serialize_hex_not_true() {
        let mut headers = BTreeMap::new();
        headers.insert(
            HeaderKey::try_from("flag").unwrap(),
            value(HeaderKind::Bool, &[2]),
        );

        let json = serde_json::to_value(JsonHeaders(&headers)).unwrap();

        assert_eq!(json["flag"]["data"], "02");
    }

    #[test]
    fn given_headers_should_serialize_through_simd_json() {
        let mut headers = BTreeMap::new();
        headers.insert(HeaderKey::from(1i32), HeaderValue::try_from("a").unwrap());

        let value = simd_json::serde::to_owned_value(JsonHeaders(&headers)).unwrap();
        let text = simd_json::to_string(&value).unwrap();

        assert_eq!(text, r#"{"~int32:1":{"data":"a","data_encoding":"utf8"}}"#);
    }
}
