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

use crate::stream::Value;

pub const SQLT_CHR: u16 = 1;
pub const SQLT_VNU: u16 = 6;
pub const SQLT_BIN: u16 = 23;
pub const SQLT_BFLOAT: u16 = 21;
pub const SQLT_BDOUBLE: u16 = 22;
pub const SQLT_AFC: u16 = 96;
pub const SQLT_ODT: u16 = 156;
pub const SQLT_TIMESTAMP: u16 = 187;
pub const SQLT_TIMESTAMP_TZ: u16 = 188;
pub const SQLT_TIMESTAMP_LTZ: u16 = 232;

const NUMBER_ZERO: u8 = 0x80;
const NUMBER_NEGATIVE_TERMINATOR: u8 = 102;
const NUMBER_EXPONENT_BIAS: i32 = 65;
const NUMBER_POSITIVE_DIGIT_OFFSET: i32 = 1;
const NUMBER_NEGATIVE_DIGIT_BASE: i32 = 101;
const OCI_DATE_LEN: usize = 7;

/// Decodes the bytes of a column that is not a datetime descriptor. `None` means the type is not supported.
pub fn decode_scalar(dtype: u16, bytes: &[u8]) -> Option<Value> {
    match dtype {
        SQLT_CHR | SQLT_AFC => Some(Value::Text(String::from_utf8_lossy(bytes).into_owned())),
        SQLT_VNU => decode_number(bytes).map(Value::Number),
        SQLT_ODT => decode_date(bytes).map(Value::Timestamp),
        SQLT_BFLOAT => <[u8; 4]>::try_from(bytes)
            .ok()
            .map(|raw| Value::Float(f64::from(f32::from_ne_bytes(raw)))),
        SQLT_BDOUBLE => <[u8; 8]>::try_from(bytes)
            .ok()
            .map(|raw| Value::Float(f64::from_ne_bytes(raw))),
        SQLT_BIN => Some(Value::Binary(bytes.to_vec())),
        _ => None,
    }
}

pub fn is_datetime_descriptor(dtype: u16) -> bool {
    matches!(
        dtype,
        SQLT_TIMESTAMP | SQLT_TIMESTAMP_TZ | SQLT_TIMESTAMP_LTZ
    )
}

/// Oracle `NUMBER` wire format: a length byte, an exponent byte and base-100 digits. Negative numbers invert the
/// exponent and digits and end with a terminator byte.
pub fn decode_number(raw: &[u8]) -> Option<String> {
    let (&length, rest) = raw.split_first()?;
    let body = rest.get(..usize::from(length))?;
    let (&exponent_byte, digit_bytes) = body.split_first()?;
    if exponent_byte == NUMBER_ZERO && digit_bytes.is_empty() {
        return Some("0".to_owned());
    }
    let positive = exponent_byte & 0x80 != 0;
    let exponent = if positive {
        i32::from(exponent_byte & 0x7f) - NUMBER_EXPONENT_BIAS
    } else {
        i32::from(!exponent_byte & 0x7f) - NUMBER_EXPONENT_BIAS
    };
    let digit_bytes = match (positive, digit_bytes.split_last()) {
        (false, Some((&NUMBER_NEGATIVE_TERMINATOR, without))) => without,
        _ => digit_bytes,
    };
    let pairs: Vec<u32> = digit_bytes
        .iter()
        .map(|&byte| {
            let pair = if positive {
                i32::from(byte) - NUMBER_POSITIVE_DIGIT_OFFSET
            } else {
                NUMBER_NEGATIVE_DIGIT_BASE - i32::from(byte)
            };
            u32::try_from(pair).ok().filter(|pair| *pair < 100)
        })
        .collect::<Option<_>>()?;

    let mut integer = String::new();
    let mut fraction = String::new();
    let integer_pairs = usize::try_from(exponent + 1).unwrap_or(0);
    for index in 0..integer_pairs {
        let pair = pairs.get(index).copied().unwrap_or(0);
        if index == 0 {
            integer.push_str(&pair.to_string());
        } else {
            integer.push_str(&format!("{pair:02}"));
        }
    }
    if exponent < 0 {
        let leading_zero_pairs = usize::try_from(-exponent - 1).unwrap_or(0);
        fraction.push_str(&"00".repeat(leading_zero_pairs));
    }
    for pair in pairs.iter().skip(integer_pairs) {
        fraction.push_str(&format!("{pair:02}"));
    }
    let fraction = fraction.trim_end_matches('0');
    let integer = if integer.is_empty() { "0" } else { &integer };
    let sign = if positive { "" } else { "-" };
    Some(if fraction.is_empty() {
        format!("{sign}{integer}")
    } else {
        format!("{sign}{integer}.{fraction}")
    })
}

/// `OCIDate` and the `SQLT_ODT` column layout: i16 year, then month, day, hour, minute, second.
pub fn decode_date(bytes: &[u8]) -> Option<String> {
    if bytes.len() < OCI_DATE_LEN {
        return None;
    }
    let year = i16::from_ne_bytes([bytes[0], bytes[1]]);
    if year == 0 {
        return None;
    }
    Some(format!(
        "{year:04}-{:02}-{:02}T{:02}:{:02}:{:02}",
        bytes[2], bytes[3], bytes[4], bytes[5], bytes[6]
    ))
}

pub struct DateTimeParts {
    pub year: i16,
    pub month: u8,
    pub day: u8,
    pub hour: u8,
    pub minute: u8,
    pub second: u8,
    pub nanoseconds: u32,
    pub offset: Option<(i8, i8)>,
}

pub fn format_timestamp(parts: &DateTimeParts) -> String {
    let mut text = format!(
        "{:04}-{:02}-{:02}T{:02}:{:02}:{:02}.{:09}",
        parts.year,
        parts.month,
        parts.day,
        parts.hour,
        parts.minute,
        parts.second,
        parts.nanoseconds
    );
    if let Some((hours, minutes)) = parts.offset {
        let sign = if hours < 0 || minutes < 0 { '-' } else { '+' };
        text.push_str(&format!("{sign}{:02}:{:02}", hours.abs(), minutes.abs()));
    }
    text
}

#[cfg(test)]
mod tests {
    use super::*;

    fn number(bytes: &[u8]) -> String {
        decode_number(bytes).expect("valid NUMBER bytes")
    }

    #[test]
    fn given_oracle_number_bytes_when_decoding_should_return_canonical_decimal() {
        assert_eq!(number(&[1, 0x80]), "0");
        assert_eq!(number(&[2, 0xc1, 0x02]), "1");
        assert_eq!(number(&[2, 0xc2, 0x02]), "100");
        assert_eq!(number(&[3, 0xc2, 0x02, 0x18]), "123");
        assert_eq!(number(&[2, 0xc0, 0x33]), "0.5");
        assert_eq!(
            number(&[6, 0xc3, 0x02, 0x18, 0x2e, 0x44, 0x5a]),
            "12345.6789"
        );
    }

    #[test]
    fn given_negative_number_bytes_when_decoding_should_drop_the_terminator() {
        assert_eq!(number(&[3, 0x3e, 0x64, 0x66]), "-1");
        assert_eq!(number(&[3, 0x3d, 0x64, 0x66]), "-100");
    }

    #[test]
    fn given_number_below_one_when_decoding_should_keep_leading_zeros() {
        assert_eq!(number(&[2, 0xbf, 0x02]), "0.0001");
    }

    #[test]
    fn given_malformed_number_bytes_when_decoding_should_return_none() {
        assert_eq!(decode_number(&[]), None);
        assert_eq!(decode_number(&[5, 0xc1]), None);
        assert_eq!(decode_number(&[2, 0xc1, 0xff]), None);
    }

    #[test]
    fn given_oci_date_bytes_when_decoding_should_format_iso_8601() {
        let year = 2026i16.to_ne_bytes();
        let bytes = [year[0], year[1], 11, 25, 10, 11, 12];
        assert_eq!(decode_date(&bytes).as_deref(), Some("2026-11-25T10:11:12"));
        assert_eq!(decode_date(&[0; 7]), None);
        assert_eq!(decode_date(&bytes[..6]), None);
    }

    #[test]
    fn given_timestamp_parts_when_formatting_should_include_offset_only_when_present() {
        let mut parts = DateTimeParts {
            year: 2026,
            month: 11,
            day: 25,
            hour: 10,
            minute: 11,
            second: 12,
            nanoseconds: 123_456_000,
            offset: None,
        };
        assert_eq!(format_timestamp(&parts), "2026-11-25T10:11:12.123456000");
        parts.offset = Some((5, 30));
        assert_eq!(
            format_timestamp(&parts),
            "2026-11-25T10:11:12.123456000+05:30"
        );
        parts.offset = Some((-5, -30));
        assert_eq!(
            format_timestamp(&parts),
            "2026-11-25T10:11:12.123456000-05:30"
        );
    }

    #[test]
    fn given_scalar_columns_when_decoding_should_map_supported_types_only() {
        assert_eq!(
            decode_scalar(SQLT_CHR, "café".as_bytes()),
            Some(Value::Text("café".to_owned()))
        );
        assert_eq!(
            decode_scalar(SQLT_BIN, &[0xde, 0xad]),
            Some(Value::Binary(vec![0xde, 0xad]))
        );
        assert_eq!(
            decode_scalar(SQLT_BFLOAT, &1.5f32.to_ne_bytes()),
            Some(Value::Float(1.5))
        );
        assert_eq!(
            decode_scalar(SQLT_BDOUBLE, &(-2.25f64).to_ne_bytes()),
            Some(Value::Float(-2.25))
        );
        assert_eq!(decode_scalar(112, &[]), None);
        assert_eq!(decode_scalar(SQLT_BFLOAT, &[1, 2, 3]), None);
    }
}
