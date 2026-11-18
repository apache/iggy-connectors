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

use base64::display::Base64Display;
use base64::engine::general_purpose::STANDARD as BASE64;
use chrono::{DateTime, NaiveDate, NaiveTime, Utc};
use fluss::metadata::{DataField, DataType};
use fluss::row::InternalRow;
use iggy_connector_sdk::Error;
use serde::ser::{Error as _, SerializeMap};
use serde::{Serialize, Serializer};

const MILLIS_PER_SECOND: i64 = 1_000;
const NANOS_PER_MILLI: i64 = 1_000_000;
/// The output of `DateTime::<Utc>::to_rfc3339`, written without the `String` it allocates.
const RFC3339_UTC: &str = "%Y-%m-%dT%H:%M:%S%.f+00:00";

/// One row written as a JSON object straight into the payload, with the `metadata` entries
/// after the columns. Names and values are written from the schema and the Arrow batch as
/// they are read, without building a `serde_json::Value` or copying a column name per row.
///
/// Temporal values are formatted with their full fractional precision, the way the
/// PostgreSQL source formats them. A number of milliseconds would drop the microseconds of
/// the default `TIMESTAMP(6)`. `TIMESTAMP` carries no timezone and is written without one,
/// while `TIMESTAMP_LTZ` is an instant and is written in UTC.
pub(crate) struct JsonRow<'a, R: InternalRow + ?Sized> {
    row: &'a R,
    columns: &'a [Column],
    metadata: &'a [(&'a str, i64)],
}

impl<'a, R: InternalRow + ?Sized> JsonRow<'a, R> {
    pub(crate) fn new(row: &'a R, columns: &'a [Column], metadata: &'a [(&'a str, i64)]) -> Self {
        Self {
            row,
            columns,
            metadata,
        }
    }
}

/// A column written into the payload, with its position in the rows the scanner hands over.
/// The scanner reads every column of the table, so a projected column keeps its table
/// position rather than its place in the projection.
pub(crate) struct Column {
    position: usize,
    field: DataField,
}

impl Column {
    pub(crate) fn new(position: usize, field: DataField) -> Self {
        Self { position, field }
    }

    pub(crate) fn field(&self) -> &DataField {
        &self.field
    }

    pub(crate) fn position(&self) -> usize {
        self.position
    }
}

impl<R: InternalRow + ?Sized> Serialize for JsonRow<'_, R> {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let mut object =
            serializer.serialize_map(Some(self.columns.len() + self.metadata.len()))?;
        for column in self.columns {
            let value = JsonField {
                row: self.row,
                position: column.position,
                field: &column.field,
            };
            object.serialize_entry(column.field.name(), &value)?;
        }
        for (name, value) in self.metadata {
            object.serialize_entry(name, value)?;
        }
        object.end()
    }
}

/// Rejects column types with no JSON representation before the first poll, so a table with
/// an unsupported column fails at startup instead of once per batch.
pub(crate) fn ensure_supported_types<'a>(
    fields: impl IntoIterator<Item = &'a DataField>,
) -> Result<(), Error> {
    for field in fields {
        if !is_supported(field.data_type()) {
            return Err(Error::SchemaMismatch(format!(
                "column '{}' has type {}, which the Apache Fluss source cannot map to JSON",
                field.name(),
                field.data_type()
            )));
        }
    }
    Ok(())
}

fn is_supported(data_type: &DataType) -> bool {
    !matches!(
        data_type,
        DataType::Array(_) | DataType::Map(_) | DataType::Row(_)
    )
}

struct JsonField<'a, R: InternalRow + ?Sized> {
    row: &'a R,
    position: usize,
    field: &'a DataField,
}

impl<R: InternalRow + ?Sized> Serialize for JsonField<'_, R> {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let row = self.row;
        let position = self.position;
        let name = self.field.name();
        let data_type = self.field.data_type();
        let read_error = |error: fluss::error::Error| {
            S::Error::custom(format_args!("failed to read column '{name}': {error}"))
        };
        let out_of_range = || {
            S::Error::custom(format_args!(
                "column '{name}' holds a {data_type} value outside the range that can be \
                 formatted"
            ))
        };

        if row.is_null_at(position).map_err(read_error)? {
            return serializer.serialize_none();
        }

        match data_type {
            DataType::Boolean(_) => {
                serializer.serialize_bool(row.get_boolean(position).map_err(read_error)?)
            }
            DataType::TinyInt(_) => {
                serializer.serialize_i8(row.get_byte(position).map_err(read_error)?)
            }
            DataType::SmallInt(_) => {
                serializer.serialize_i16(row.get_short(position).map_err(read_error)?)
            }
            DataType::Int(_) => {
                serializer.serialize_i32(row.get_int(position).map_err(read_error)?)
            }
            DataType::BigInt(_) => {
                serializer.serialize_i64(row.get_long(position).map_err(read_error)?)
            }
            // serde_json writes NaN and infinity as null, since JSON has no encoding for them.
            DataType::Float(_) => {
                serializer.serialize_f64(f64::from(row.get_float(position).map_err(read_error)?))
            }
            DataType::Double(_) => {
                serializer.serialize_f64(row.get_double(position).map_err(read_error)?)
            }
            DataType::Char(inner) => serializer.serialize_str(
                row.get_char(position, inner.length() as usize)
                    .map_err(read_error)?,
            ),
            DataType::String(_) => {
                serializer.serialize_str(row.get_string(position).map_err(read_error)?)
            }
            DataType::Decimal(inner) => {
                let decimal = row
                    .get_decimal(position, inner.precision() as usize, inner.scale() as usize)
                    .map_err(read_error)?;
                // `BigDecimal`'s `Display` writes a value with more than five zeros after the
                // point, such as 0.00000012, as `1.2E-7`.
                serializer.serialize_str(&decimal.to_big_decimal().to_plain_string())
            }
            DataType::Date(_) => {
                let days = row.get_date(position).map_err(read_error)?.get_inner();
                serializer.collect_str(&NaiveDate::from_epoch_days(days).ok_or_else(out_of_range)?)
            }
            DataType::Time(_) => {
                let millis = row.get_time(position).map_err(read_error)?.get_inner();
                serializer.collect_str(&time_of_day(millis).ok_or_else(out_of_range)?)
            }
            DataType::Timestamp(inner) => {
                let timestamp = row
                    .get_timestamp_ntz(position, inner.precision())
                    .map_err(read_error)?;
                let instant = instant(
                    timestamp.get_millisecond(),
                    timestamp.get_nano_of_millisecond(),
                )
                .ok_or_else(out_of_range)?;
                serializer.collect_str(&instant.naive_utc())
            }
            DataType::TimestampLTz(inner) => {
                let timestamp = row
                    .get_timestamp_ltz(position, inner.precision())
                    .map_err(read_error)?;
                let instant = instant(
                    timestamp.get_epoch_millisecond(),
                    timestamp.get_nano_of_millisecond(),
                )
                .ok_or_else(out_of_range)?;
                serializer.collect_str(&instant.naive_utc().format(RFC3339_UTC))
            }
            DataType::Bytes(_) => serializer.collect_str(&Base64Display::new(
                row.get_bytes(position).map_err(read_error)?,
                &BASE64,
            )),
            DataType::Binary(inner) => serializer.collect_str(&Base64Display::new(
                row.get_binary(position, inner.length())
                    .map_err(read_error)?,
                &BASE64,
            )),
            DataType::Array(_) | DataType::Map(_) | DataType::Row(_) => {
                Err(S::Error::custom(format_args!(
                    "column '{name}' has nested type {data_type}, which the Apache Fluss \
                     source does not support"
                )))
            }
        }
    }
}

/// Fluss keeps a timestamp as epoch milliseconds plus the nanoseconds within that
/// millisecond. Euclidean division keeps a pre-epoch value's fraction positive, so -1 ms is
/// 23:59:59.999 of the previous day rather than an invalid negative fraction.
fn instant(epoch_millis: i64, nano_of_millisecond: i32) -> Option<DateTime<Utc>> {
    let seconds = epoch_millis.div_euclid(MILLIS_PER_SECOND);
    let nanos = epoch_millis.rem_euclid(MILLIS_PER_SECOND) * NANOS_PER_MILLI
        + i64::from(nano_of_millisecond);
    DateTime::from_timestamp(seconds, u32::try_from(nanos).ok()?)
}

/// Fluss keeps `TIME` as milliseconds since midnight.
fn time_of_day(millis: i32) -> Option<NaiveTime> {
    let millis = i64::from(millis);
    NaiveTime::from_num_seconds_from_midnight_opt(
        u32::try_from(millis.div_euclid(MILLIS_PER_SECOND)).ok()?,
        u32::try_from(millis.rem_euclid(MILLIS_PER_SECOND) * NANOS_PER_MILLI).ok()?,
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use base64::Engine;
    use fluss::metadata::DataTypes;
    use fluss::row::{Date, Datum, Decimal, GenericRow, Time, TimestampLtz, TimestampNtz};
    use serde_json::Value;
    use std::ops::Range;

    fn field(name: &str, data_type: DataType) -> DataField {
        DataField::new(name, data_type, None)
    }

    /// Every field, at its own position.
    fn columns(fields: &[DataField]) -> Vec<Column> {
        fields
            .iter()
            .enumerate()
            .map(|(position, field)| Column::new(position, field.clone()))
            .collect()
    }

    fn to_json(row: &GenericRow, fields: &[DataField]) -> Value {
        let payload = serde_json::to_vec(&JsonRow::new(row, &columns(fields), &[]))
            .expect("Failed to map row");
        serde_json::from_slice(&payload).expect("The payload should be JSON")
    }

    #[test]
    fn given_temporal_columns_when_mapped_should_keep_full_precision() {
        let fields = vec![
            field("date", DataTypes::date()),
            field("time", DataTypes::time_with_precision(3)),
            field("timestamp", DataTypes::timestamp()),
            field("timestamp_nanos", DataTypes::timestamp_with_precision(9)),
            field("timestamp_ltz", DataTypes::timestamp_ltz()),
        ];
        let mut row = GenericRow::new(5);
        row.set_field(0, Date::new(19_782));
        row.set_field(1, Time::new(45_296_789));
        row.set_field(
            2,
            TimestampNtz::from_millis_nanos(1_700_000_000_123, 456_000)
                .expect("Failed to build timestamp"),
        );
        row.set_field(
            3,
            TimestampNtz::from_millis_nanos(1_700_000_000_123, 456_789)
                .expect("Failed to build timestamp"),
        );
        row.set_field(
            4,
            TimestampLtz::from_millis_nanos(1_700_000_000_123, 456_000)
                .expect("Failed to build timestamp"),
        );

        let object = to_json(&row, &fields);

        assert_eq!(object["date"], Value::from("2024-02-29"));
        assert_eq!(object["time"], Value::from("12:34:56.789"));
        assert_eq!(
            object["timestamp"],
            Value::from("2023-11-14 22:13:20.123456")
        );
        assert_eq!(
            object["timestamp_nanos"],
            Value::from("2023-11-14 22:13:20.123456789")
        );
        assert_eq!(
            object["timestamp_ltz"],
            Value::from("2023-11-14T22:13:20.123456+00:00")
        );
    }

    #[test]
    fn given_timestamp_before_the_epoch_when_mapped_should_borrow_from_the_previous_second() {
        let fields = vec![field("timestamp", DataTypes::timestamp_with_precision(3))];
        let mut row = GenericRow::new(1);
        row.set_field(
            0,
            TimestampNtz::from_millis_nanos(-1, 0).expect("Failed to build timestamp"),
        );

        let object = to_json(&row, &fields);

        assert_eq!(object["timestamp"], Value::from("1969-12-31 23:59:59.999"));
    }

    #[test]
    fn given_date_beyond_what_can_be_formatted_should_fail_naming_the_column() {
        let fields = vec![field("shipped_on", DataTypes::date())];
        let mut row = GenericRow::new(1);
        row.set_field(0, Date::new(i32::MAX));

        let error = serde_json::to_vec(&JsonRow::new(&row, &columns(&fields), &[]))
            .expect_err("A date chrono cannot hold should fail");

        assert!(error.to_string().contains("'shipped_on'"), "{error}");
    }

    #[test]
    fn given_small_decimal_when_mapped_should_write_it_without_an_exponent() {
        let fields = vec![
            field("positive", DataTypes::decimal(10, 8)),
            field("negative", DataTypes::decimal(10, 8)),
        ];
        let mut row = GenericRow::new(2);
        row.set_field(
            0,
            Decimal::from_unscaled_long(12, 10, 8).expect("Failed to build decimal"),
        );
        row.set_field(
            1,
            Decimal::from_unscaled_long(-12, 10, 8).expect("Failed to build decimal"),
        );

        let object = to_json(&row, &fields);

        assert_eq!(object["positive"], Value::from("0.00000012"));
        assert_eq!(object["negative"], Value::from("-0.00000012"));
    }

    #[test]
    fn given_decimal_and_fixed_width_columns_when_mapped_should_produce_strings() {
        let fields = vec![
            field("amount", DataTypes::decimal(10, 2)),
            field("code", DataTypes::char(5)),
            field("digest", DataTypes::binary(3)),
        ];
        let mut row = GenericRow::new(3);
        row.set_field(
            0,
            Decimal::from_unscaled_long(12_345, 10, 2).expect("Failed to build decimal"),
        );
        row.set_field(1, "abcde");
        row.set_field(2, [4u8, 5, 6].as_slice());

        let object = to_json(&row, &fields);

        assert_eq!(object["amount"], Value::from("123.45"));
        assert_eq!(object["code"], Value::from("abcde"));
        assert_eq!(object["digest"], Value::from(BASE64.encode([4u8, 5, 6])));
    }

    #[test]
    fn given_scalar_columns_when_mapped_should_produce_json_object() {
        let fields = vec![
            field("id", DataTypes::int()),
            field("name", DataTypes::string()),
            field("active", DataTypes::boolean()),
            field("ratio", DataTypes::double()),
            field("total", DataTypes::bigint()),
        ];
        let mut row = GenericRow::new(5);
        row.set_field(0, 7i32);
        row.set_field(1, "alice");
        row.set_field(2, true);
        row.set_field(3, 1.5f64);
        row.set_field(4, 90i64);

        let object = to_json(&row, &fields);

        assert_eq!(object["id"], Value::from(7));
        assert_eq!(object["name"], Value::from("alice"));
        assert_eq!(object["active"], Value::from(true));
        assert_eq!(object["ratio"], Value::from(1.5));
        assert_eq!(object["total"], Value::from(90));
    }

    #[test]
    fn given_unset_column_when_mapped_should_produce_null() {
        let fields = vec![
            field("id", DataTypes::int()),
            field("name", DataTypes::string()),
        ];
        let mut row = GenericRow::new(2);
        row.set_field(0, 1i32);

        let object = to_json(&row, &fields);

        assert_eq!(object["id"], Value::from(1));
        assert_eq!(object["name"], Value::Null);
    }

    #[test]
    fn given_binary_column_when_mapped_should_produce_base64() {
        let fields = vec![field("blob", DataTypes::bytes())];
        let mut row = GenericRow::new(1);
        row.set_field(0, [1u8, 2, 3].as_slice());

        let object = to_json(&row, &fields);

        assert_eq!(object["blob"], Value::from(BASE64.encode([1u8, 2, 3])));
    }

    #[test]
    fn given_scalar_columns_when_validated_should_be_accepted() {
        let fields = vec![
            field("a", DataTypes::string()),
            field("b", DataTypes::timestamp()),
            field("c", DataTypes::decimal(10, 2)),
        ];

        assert!(ensure_supported_types(&fields).is_ok());
    }

    #[test]
    fn given_nested_column_when_validated_should_be_rejected() {
        let fields = vec![
            field("id", DataTypes::int()),
            field("tags", DataTypes::array(DataTypes::string())),
        ];

        let error = ensure_supported_types(&fields).expect_err("Nested column should be rejected");

        assert!(matches!(error, Error::SchemaMismatch(message) if message.contains("tags")));
    }

    #[test]
    fn given_projected_columns_should_read_their_table_positions_in_the_requested_order() {
        let mut row = GenericRow::new(3);
        row.set_field(0, 7i32);
        row.set_field(1, "left out");
        row.set_field(2, "kept");
        let projected = vec![
            Column::new(2, field("payload", DataTypes::string())),
            Column::new(0, field("id", DataTypes::int())),
        ];

        let payload =
            serde_json::to_string(&JsonRow::new(&row, &projected, &[])).expect("Failed to map row");

        assert_eq!(payload, r#"{"payload":"kept","id":7}"#);
    }

    #[test]
    fn given_non_finite_floats_should_map_to_null() {
        let fields = vec![
            field("nan", DataTypes::double()),
            field("infinite", DataTypes::float()),
            field("finite", DataTypes::double()),
        ];
        let mut row = GenericRow::new(3);
        row.set_field(0, f64::NAN);
        row.set_field(1, f32::INFINITY);
        row.set_field(2, 2.5f64);

        let object = to_json(&row, &fields);

        assert_eq!(object["nan"], Value::Null);
        assert_eq!(object["infinite"], Value::Null);
        assert_eq!(object["finite"], Value::from(2.5));
    }

    #[test]
    fn given_integer_extremes_when_mapped_should_keep_every_digit() {
        let fields = vec![
            field("tiny_min", DataTypes::tinyint()),
            field("tiny_max", DataTypes::tinyint()),
            field("small_min", DataTypes::smallint()),
            field("small_max", DataTypes::smallint()),
            field("int_min", DataTypes::int()),
            field("int_max", DataTypes::int()),
            field("big_min", DataTypes::bigint()),
            field("big_max", DataTypes::bigint()),
        ];
        let mut row = GenericRow::new(8);
        row.set_field(0, i8::MIN);
        row.set_field(1, i8::MAX);
        row.set_field(2, i16::MIN);
        row.set_field(3, i16::MAX);
        row.set_field(4, i32::MIN);
        row.set_field(5, i32::MAX);
        row.set_field(6, i64::MIN);
        row.set_field(7, i64::MAX);

        let payload = serde_json::to_string(&JsonRow::new(&row, &columns(&fields), &[]))
            .expect("Failed to map row");

        assert_eq!(
            payload,
            r#"{"tiny_min":-128,"tiny_max":127,"small_min":-32768,"small_max":32767,"int_min":-2147483648,"int_max":2147483647,"big_min":-9223372036854775808,"big_max":9223372036854775807}"#
        );
    }

    #[test]
    fn given_text_that_needs_escaping_when_mapped_should_come_back_unchanged() {
        let values = [
            "",
            "quote \" and backslash \\",
            "line\nbreak\ttab\rreturn",
            "\u{0001}\u{001f}\u{007f}",
            "한글 🚀 é",
            "</script>",
        ];
        let mut fields: Vec<DataField> = (0..values.len())
            .map(|index| field(&format!("text_{index}"), DataTypes::string()))
            .collect();
        fields.push(field("quo\"ted\\name", DataTypes::string()));
        let mut row = GenericRow::new(fields.len());
        for (index, value) in values.iter().enumerate() {
            row.set_field(index, *value);
        }
        row.set_field(values.len(), "under an escaped name");

        let object = to_json(&row, &fields);

        for (index, value) in values.iter().enumerate() {
            assert_eq!(object[format!("text_{index}")], Value::from(*value));
        }
        assert_eq!(
            object["quo\"ted\\name"],
            Value::from("under an escaped name")
        );
    }

    #[test]
    fn given_empty_bytes_when_mapped_should_produce_an_empty_string() {
        let fields = vec![field("blob", DataTypes::bytes())];
        let mut row = GenericRow::new(1);
        row.set_field(0, [0u8; 0].as_slice());

        let object = to_json(&row, &fields);

        assert_eq!(object["blob"], Value::from(""));
    }

    #[test]
    fn given_decimals_at_the_edges_when_mapped_should_write_every_digit() {
        // 38 digits do not fit an i64, so these take the client's unscaled-bytes form.
        let wide: i128 = 12_345_678_901_234_567_890_123_456_789_123_456_789;
        let fields = vec![
            field("zero", DataTypes::decimal(10, 2)),
            field("negative", DataTypes::decimal(10, 2)),
            field("whole", DataTypes::decimal(10, 0)),
            field("trailing_zeros", DataTypes::decimal(10, 2)),
            field("wide", DataTypes::decimal(38, 9)),
            field("wide_negative", DataTypes::decimal(38, 9)),
        ];
        let compact = |unscaled, scale| {
            Decimal::from_unscaled_long(unscaled, 10, scale).expect("Failed to build decimal")
        };
        let mut row = GenericRow::new(6);
        row.set_field(0, compact(0, 2));
        row.set_field(1, compact(-12_345, 2));
        row.set_field(2, compact(42, 0));
        row.set_field(3, compact(100, 2));
        row.set_field(
            4,
            Decimal::from_unscaled_bytes(&wide.to_be_bytes(), 38, 9)
                .expect("Failed to build decimal"),
        );
        row.set_field(
            5,
            Decimal::from_unscaled_bytes(&(-wide).to_be_bytes(), 38, 9)
                .expect("Failed to build decimal"),
        );

        let object = to_json(&row, &fields);

        assert_eq!(object["zero"], Value::from("0.00"));
        assert_eq!(object["negative"], Value::from("-123.45"));
        assert_eq!(object["whole"], Value::from("42"));
        assert_eq!(object["trailing_zeros"], Value::from("1.00"));
        assert_eq!(
            object["wide"],
            Value::from("12345678901234567890123456789.123456789")
        );
        assert_eq!(
            object["wide_negative"],
            Value::from("-12345678901234567890123456789.123456789")
        );
    }

    #[test]
    fn given_temporal_values_at_the_edges_when_mapped_should_format_them() {
        let fields = vec![
            field("epoch_day", DataTypes::date()),
            field("day_before_epoch", DataTypes::date()),
            field("midnight", DataTypes::time_with_precision(3)),
            field("last_millisecond", DataTypes::time_with_precision(3)),
            field("whole_second", DataTypes::timestamp_with_precision(0)),
            field(
                "ltz_before_epoch",
                DataTypes::timestamp_ltz_with_precision(3),
            ),
            field("ltz_whole_second", DataTypes::timestamp_ltz()),
        ];
        let mut row = GenericRow::new(7);
        row.set_field(0, Date::new(0));
        row.set_field(1, Date::new(-1));
        row.set_field(2, Time::new(0));
        row.set_field(3, Time::new(86_399_999));
        row.set_field(4, TimestampNtz::new(1_700_000_000_000));
        row.set_field(5, TimestampLtz::new(-1));
        row.set_field(6, TimestampLtz::new(1_700_000_000_000));

        let object = to_json(&row, &fields);

        assert_eq!(object["epoch_day"], Value::from("1970-01-01"));
        assert_eq!(object["day_before_epoch"], Value::from("1969-12-31"));
        assert_eq!(object["midnight"], Value::from("00:00:00"));
        assert_eq!(object["last_millisecond"], Value::from("23:59:59.999"));
        assert_eq!(object["whole_second"], Value::from("2023-11-14 22:13:20"));
        assert_eq!(
            object["ltz_before_epoch"],
            Value::from("1969-12-31T23:59:59.999+00:00")
        );
        assert_eq!(
            object["ltz_whole_second"],
            Value::from("2023-11-14T22:13:20+00:00")
        );
    }

    #[test]
    fn given_time_outside_a_day_should_fail_naming_the_column() {
        let fields = vec![field("opened_at", DataTypes::time_with_precision(3))];

        for millis in [-1, 86_400_000, i32::MAX, i32::MIN] {
            let mut row = GenericRow::new(1);
            row.set_field(0, Time::new(millis));

            let error = serde_json::to_vec(&JsonRow::new(&row, &columns(&fields), &[]))
                .expect_err("A time outside a day should fail");

            assert!(
                error.to_string().contains("'opened_at'"),
                "{millis}: {error}"
            );
        }
    }

    #[test]
    fn given_timestamp_beyond_what_can_be_formatted_should_fail_naming_the_column() {
        let fields = vec![
            field("created_at", DataTypes::timestamp()),
            field("seen_at", DataTypes::timestamp_ltz()),
        ];

        for millis in [i64::MAX, i64::MIN] {
            let mut local = GenericRow::new(2);
            local.set_field(0, TimestampNtz::new(millis));
            local.set_field(1, TimestampLtz::new(0));
            let error = serde_json::to_vec(&JsonRow::new(&local, &columns(&fields), &[]))
                .expect_err("A timestamp chrono cannot hold should fail");
            assert!(
                error.to_string().contains("'created_at'"),
                "{millis}: {error}"
            );

            let mut instant = GenericRow::new(2);
            instant.set_field(0, TimestampNtz::new(0));
            instant.set_field(1, TimestampLtz::new(millis));
            let error = serde_json::to_vec(&JsonRow::new(&instant, &columns(&fields), &[]))
                .expect_err("A timestamp chrono cannot hold should fail");
            assert!(error.to_string().contains("'seen_at'"), "{millis}: {error}");
        }
    }

    /// Repeats exactly and needs no dependency.
    struct Xorshift(u64);

    impl Xorshift {
        fn next(&mut self) -> u64 {
            self.0 ^= self.0 << 13;
            self.0 ^= self.0 >> 7;
            self.0 ^= self.0 << 17;
            self.0
        }

        /// One value in sixteen comes from the whole range of the type, the rest from `sane`.
        fn pick(&mut self, sane: Range<i64>) -> i64 {
            let raw = self.next() as i64;
            if self.next().is_multiple_of(16) {
                raw
            } else {
                sane.start + raw.rem_euclid(sane.end - sane.start)
            }
        }

        fn text(&mut self) -> String {
            let length = self.next() % 12;
            (0..length)
                .filter_map(|_| char::from_u32((self.next() % 0x3000) as u32))
                .collect()
        }
    }

    /// A row either maps to one JSON object holding every column, or fails on a temporal value
    /// chrono cannot hold. It never panics and never writes something that is not JSON.
    #[test]
    fn given_arbitrary_scalar_values_when_mapped_should_produce_an_object_or_a_range_error() {
        const ROWS: usize = 20_000;
        let fields = vec![
            field("bool_col", DataTypes::boolean()),
            field("tinyint_col", DataTypes::tinyint()),
            field("smallint_col", DataTypes::smallint()),
            field("int_col", DataTypes::int()),
            field("bigint_col", DataTypes::bigint()),
            field("float_col", DataTypes::float()),
            field("double_col", DataTypes::double()),
            field("string_col", DataTypes::string()),
            field("bytes_col", DataTypes::bytes()),
            field("decimal_col", DataTypes::decimal(18, 4)),
            field("date_col", DataTypes::date()),
            field("time_col", DataTypes::time_with_precision(3)),
            field("timestamp_col", DataTypes::timestamp()),
            field("timestamp_ltz_col", DataTypes::timestamp_ltz()),
        ];
        let columns = columns(&fields);
        let mut random = Xorshift(0x9E37_79B9_7F4A_7C15);
        let (mut mapped, mut out_of_range) = (0, 0);

        for _ in 0..ROWS {
            let text = random.text();
            let bytes = random.next().to_le_bytes();
            let mut row = GenericRow::new(fields.len());
            row.set_field(0, random.next().is_multiple_of(2));
            row.set_field(1, random.next() as i8);
            row.set_field(2, random.next() as i16);
            row.set_field(3, random.next() as i32);
            row.set_field(4, random.next() as i64);
            row.set_field(5, f32::from_bits(random.next() as u32));
            row.set_field(6, f64::from_bits(random.next()));
            row.set_field(7, text.as_str());
            row.set_field(8, &bytes[..(random.next() % 9) as usize]);
            row.set_field(
                9,
                Decimal::from_unscaled_long(
                    (random.next() as i64) % 1_000_000_000_000_000_000,
                    18,
                    4,
                )
                .expect("Failed to build decimal"),
            );
            row.set_field(10, Date::new(random.pick(-800_000..800_000) as i32));
            row.set_field(11, Time::new(random.pick(0..86_400_000) as i32));
            row.set_field(
                12,
                TimestampNtz::new(random.pick(-60_000_000_000_000..250_000_000_000_000)),
            );
            row.set_field(
                13,
                TimestampLtz::new(random.pick(-60_000_000_000_000..250_000_000_000_000)),
            );
            // A null in a random column, in about half of the rows.
            let null_at = (random.next() % (fields.len() as u64 * 2)) as usize;
            if null_at < fields.len() {
                row.set_field(null_at, Datum::Null);
            }

            match serde_json::to_vec(&JsonRow::new(&row, &columns, &[])) {
                Ok(payload) => {
                    let object: Value =
                        serde_json::from_slice(&payload).expect("The payload should be JSON");
                    let object = object.as_object().expect("The payload should be an object");
                    assert_eq!(object.len(), fields.len());
                    mapped += 1;
                }
                Err(error) => {
                    assert!(
                        error.to_string().contains("outside the range"),
                        "unexpected mapping error: {error}"
                    );
                    out_of_range += 1;
                }
            }
        }

        assert_eq!(mapped + out_of_range, ROWS);
        assert!(mapped > ROWS / 2, "only {mapped} rows mapped");
        assert!(out_of_range > 0, "no row reached the range check");
    }
}
