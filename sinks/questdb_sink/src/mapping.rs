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

use std::borrow::Cow;
use std::collections::HashMap;
use std::fmt::{self, Write};
use std::iter;
use std::mem;
use std::str::FromStr;
use std::sync::atomic::{AtomicBool, Ordering};

use ahash::AHashMap;
use iggy_common::{HeaderKey, HeaderValue};
use iggy_connector_sdk::headers::{DATA_ENCODING_BASE64, HeaderData, JsonHeaderValue};
use iggy_connector_sdk::{ConsumedMessage, HeaderName, Payload};
use questdb::ErrorCode;
use questdb::ingress::{
    Buffer, ColumnName, NdArrayView, TableName, TimestampMicros, TimestampNanos,
};
use simd_json::OwnedValue;
use simd_json::prelude::{ValueAsArray, ValueAsObject};
use simd_json::value::StaticNode;
use tracing::warn;
use uuid::Uuid;

use crate::CONNECTOR_NAME;

/// Where the designated timestamp comes from.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum TimestampSource {
    /// The Apache Iggy message timestamp (microseconds).
    #[default]
    Message,
    /// The producer-supplied origin timestamp (microseconds).
    Origin,
    /// A field inside the payload, named by `timestamp_field`.
    Payload,
    /// Let QuestDB stamp the row on arrival.
    Server,
}

/// A configuration value that names none of the enum's variants.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct UnknownVariant;

/// Matched without regard to case, so `Payload` and `PAYLOAD` also work.
impl FromStr for TimestampSource {
    type Err = UnknownVariant;

    fn from_str(raw: &str) -> Result<Self, Self::Err> {
        match raw.to_ascii_lowercase().as_str() {
            "message" => Ok(Self::Message),
            "origin" => Ok(Self::Origin),
            "payload" => Ok(Self::Payload),
            "server" => Ok(Self::Server),
            _ => Err(UnknownVariant),
        }
    }
}

/// Unit of a payload-sourced timestamp. `Auto` guesses from magnitude, which
/// covers the common case of a producer that switched units between releases.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum TimestampUnit {
    #[default]
    Auto,
    Seconds,
    Millis,
    Micros,
    Nanos,
}

/// Matched without regard to case, so `MILLIS` and `Ms` also work.
impl FromStr for TimestampUnit {
    type Err = UnknownVariant;

    fn from_str(raw: &str) -> Result<Self, Self::Err> {
        match raw.to_ascii_lowercase().as_str() {
            "auto" => Ok(Self::Auto),
            "seconds" | "s" => Ok(Self::Seconds),
            "millis" | "ms" => Ok(Self::Millis),
            "micros" | "us" => Ok(Self::Micros),
            "nanos" | "ns" => Ok(Self::Nanos),
            _ => Err(UnknownVariant),
        }
    }
}

impl TimestampUnit {
    /// `None` when the value cannot be expressed in nanoseconds, which for
    /// `Auto` also catches a magnitude that was bucketed as a coarser unit than
    /// it really is: multiplying it up overflows rather than quietly saturating
    /// to a timestamp centuries away.
    fn to_nanos(self, value: i64) -> Option<i64> {
        value.checked_mul(self.resolve(value).nanos_per_unit())
    }

    /// Converts a float timestamp, keeping its fraction.
    ///
    /// The whole part goes through the exact integer path, and the fraction is
    /// scaled on its own and rounded to the nearest nanosecond. Casting the float
    /// to whole units first would drop up to one unit, for example half a
    /// second of `1788523200.5` seconds.
    fn float_to_nanos(self, value: f64) -> Option<i64> {
        let whole = value.trunc();
        let whole_units = whole as i64;
        let unit = self.resolve(whole_units);
        let fraction = ((value - whole) * unit.nanos_per_unit() as f64).round() as i64;
        unit.to_nanos(whole_units)?.checked_add(fraction)
    }

    /// The concrete unit of `value`. Only `Auto` depends on the value.
    fn resolve(self, value: i64) -> Self {
        match self {
            // Each threshold is the point above which a value cannot be the
            // smaller unit any more: 1e11 seconds is far past any plausible
            // date, so such a value is milliseconds, and so on. `unsigned_abs`
            // rather than `abs` because `i64::MIN` has no positive counterpart.
            Self::Auto => match value.unsigned_abs() {
                0..=99_999_999_999 => Self::Seconds,
                100_000_000_000..=99_999_999_999_999 => Self::Millis,
                100_000_000_000_000..=99_999_999_999_999_999 => Self::Micros,
                _ => Self::Nanos,
            },
            unit => unit,
        }
    }

    fn nanos_per_unit(self) -> i64 {
        match self {
            Self::Seconds => 1_000_000_000,
            Self::Millis => 1_000_000,
            Self::Micros => 1_000,
            Self::Nanos | Self::Auto => 1,
        }
    }
}

/// QuestDB's column-name limit. The client enforces it on the buffer from the
/// server's `cairo.max.file.name.length`, whose default this matches; a server
/// configured lower will refuse a name this accepts, which the caller recovers
/// from as an ordinary rejection.
pub(crate) const MAX_NAME_LEN: usize = 127;

/// The longest symbol value, in bytes, that the client encodes. It matches
/// the client's own limit for a symbol dictionary entry.
const MAX_SYMBOL_LEN: usize = 1 << 20;

/// How many metadata columns the connector can write itself. The array in
/// [`Mapping::reserved_columns_with_flags`] is typed with this length, so the
/// two cannot disagree.
const RESERVED_COLUMNS: usize = 4;

// The metadata columns carry an `iggy_` prefix, as in the other sinks, so that
// common payload fields such as `stream` or `offset` do not collide with them.
pub(crate) const STREAM_COLUMN: &str = "iggy_stream";
pub(crate) const TOPIC_COLUMN: &str = "iggy_topic";
pub(crate) const PARTITION_COLUMN: &str = "iggy_partition_id";
pub(crate) const OFFSET_COLUMN: &str = "iggy_offset";
const HEADER_PREFIX: &str = "header_";
// The symbol that marks a base64 header value wraps the header column in
// `iggy_` and `_encoding`. No header key gives a name that starts with
// `iggy_`, so a key such as `bin_encoding` cannot claim the marker of `bin`.
const HEADER_ENCODING_PREFIX: &str = "iggy_";
const HEADER_ENCODING_SUFFIX: &str = "_encoding";

/// Where a rounded integer came from, which decides what the warning tells the
/// operator to do. An array element and a value past `i64::MAX` have no exact
/// QuestDB form, so pointing them at `integer_columns` would be wrong: that
/// list refuses both. Each source warns once on its own, so an early array
/// rounding cannot hide the `integer_columns` remedy for a scalar column.
#[derive(Debug, Clone, Copy)]
pub(crate) enum Rounding {
    Scalar,
    PastLong,
    ArrayElement,
}

impl Rounding {
    fn remedy(self) -> &'static str {
        match self {
            Self::Scalar => "list the column in integer_columns to keep it exact",
            Self::PastLong => "a value above i64::MAX has no exact QuestDB column",
            Self::ArrayElement => {
                "QuestDB stores only DOUBLE arrays, so an array element has no exact form"
            }
        }
    }
}

/// How many header columns the sink remembers the first key of. Each entry
/// is one column name and one key, so the bound keeps a producer that sends a
/// new key with every message from growing the map without end.
const MAX_TRACKED_HEADER_COLUMNS: usize = 4096;

/// The first header key that wrote to each header column, across the chunks of
/// the connector.
///
/// Two keys can give one column, for example `a.b` and `a-b`, or `Host` and
/// `host`, because the column name replaces refused characters and QuestDB
/// matches names without regard to case. In one record that is a collision,
/// and the row is rejected. In different records, the keys write to the same
/// column, and this is what notices it, once per connector.
#[derive(Debug, Default)]
pub(crate) struct HeaderKeys {
    first: std::sync::Mutex<HashMap<String, HeaderKey>>,
    warned: AtomicBool,
}

impl HeaderKeys {
    fn warn_once(&self, id: u32, column: &str, first: &HeaderKey, second: &HeaderKey) {
        if self.warned.load(Ordering::Relaxed) || self.warned.swap(true, Ordering::Relaxed) {
            return;
        }
        warn!(
            "{CONNECTOR_NAME} ID: {id} header keys {} and {} both write to column {column}, so its values no longer say which key they came from. This warning is logged once per connector",
            HeaderName(first),
            HeaderName(second)
        );
    }
}

/// One flag per [`Rounding`] source, set when that source first rounded an
/// integer on its way to a `DOUBLE`, so each warning is logged once per
/// connector rather than per row.
#[derive(Debug, Default)]
pub(crate) struct RoundingWarned {
    scalar: AtomicBool,
    past_long: AtomicBool,
    array_element: AtomicBool,
}

impl RoundingWarned {
    fn flag(&self, source: Rounding) -> &AtomicBool {
        match source {
            Rounding::Scalar => &self.scalar,
            Rounding::PastLong => &self.past_long,
            Rounding::ArrayElement => &self.array_element,
        }
    }

    /// Whether any warning fired. The destructure fails to compile when a
    /// flag is added and not read here.
    #[cfg(test)]
    fn any(&self) -> bool {
        let Self {
            scalar,
            past_long,
            array_element,
        } = self;
        [scalar, past_long, array_element]
            .iter()
            .any(|flag| flag.load(Ordering::Relaxed))
    }
}

/// Widens an integer to the `DOUBLE` QuestDB stores, and says whether that
/// lost precision. The test goes through `i128`, because casting the double
/// back to the source width saturates at the extremes and would call
/// `i64::MAX` exact after rounding it to 2^63.
fn widen_exact(number: i128) -> (f64, bool) {
    let widened = number as f64;
    (widened, widened as i128 == number)
}

/// A message that could not be turned into a row. The batch continues; the
/// caller counts and logs these.
#[derive(Debug)]
pub enum RowError {
    /// The payload was not a JSON object, or a required field was missing.
    /// Caught before the first write, so the buffer is untouched and the batch
    /// continues.
    Invalid(String),
    /// The QuestDB client rejected the row. Carries the underlying error so the
    /// caller can decide whether it is transient.
    ///
    /// Every column and symbol setter rolls the half-written row back before
    /// returning, so the buffer is still usable and only this record is lost.
    Client(questdb::Error),
    /// `Buffer::at` refused the row. `at` rolls back most of its failures,
    /// but it refuses a negative timestamp without a rollback, so the buffer
    /// can hold a partial row and the caller has to drop it. The other
    /// failures land here too, because the caller cannot tell them apart.
    /// [`Mapping::prepare_timestamp`] rejects every negative value, which is
    /// what keeps this unreachable. `at` also skips the rollback when no
    /// table was set or the timestamp does not convert, and `write_row` always
    /// sets the table first and passes timestamps that convert.
    Unrecoverable(questdb::Error),
}

impl From<questdb::Error> for RowError {
    fn from(error: questdb::Error) -> Self {
        Self::Client(error)
    }
}

/// Configured column names, matched without regard to case.
///
/// QuestDB resolves a column name case-insensitively, so `uuid_columns =
/// ["trade_id"]` has to match a payload field named `Trade_Id`. Matching it
/// case-sensitively instead would write that field as a plain string column and
/// report nothing.
///
/// A short list with a linear scan beats a hash set here: the entries are one
/// per configured column, while the lookup runs for every field of every row,
/// and hashing would need a freshly lowercased copy of each field name.
#[derive(Debug, Default)]
pub struct ColumnNames(Vec<String>);

impl ColumnNames {
    pub fn contains(&self, name: &str) -> bool {
        self.0
            .iter()
            .any(|configured| configured.eq_ignore_ascii_case(name))
    }

    #[cfg(test)]
    pub fn insert(&mut self, name: String) {
        if !self.contains(&name) {
            self.0.push(name);
        }
    }

    pub fn iter(&self) -> impl Iterator<Item = &str> {
        self.0.iter().map(String::as_str)
    }
}

impl FromIterator<String> for ColumnNames {
    fn from_iter<I: IntoIterator<Item = String>>(names: I) -> Self {
        Self(names.into_iter().collect())
    }
}

/// Everything needed to turn a `ConsumedMessage` into a QuestDB row. Held
/// behind an `Arc` so it can cross into `spawn_blocking` without cloning
/// per batch.
#[derive(Debug)]
pub struct Mapping {
    /// The connector ID, for the one log line this module writes.
    pub id: u32,
    pub table: String,
    pub symbol_columns: ColumnNames,
    pub uuid_columns: ColumnNames,
    /// Columns stored as `LONG`, whatever [`Mapping::numbers_as_double`] says.
    ///
    /// A fractional value in a listed column is rejected in both modes,
    /// because storing it as a `DOUBLE` would break the type the declaration
    /// pinned.
    pub integer_columns: ColumnNames,
    pub timestamp_source: TimestampSource,
    pub timestamp_field: Option<String>,
    pub timestamp_unit: TimestampUnit,
    pub include_stream_column: bool,
    pub include_topic_column: bool,
    pub include_partition_column: bool,
    pub include_offset_column: bool,
    pub include_headers: bool,
    /// Write every JSON number as a `DOUBLE`.
    ///
    /// JSON carries one number type and a serializer writes `2.0` as `2`, so a
    /// producer whose values are sometimes whole makes a column's type depend on
    /// which record reached the buffer first. The client pins that type for the
    /// rest of the buffer, which lives for one chunk, and refuses every record
    /// of that chunk which disagrees, so the records lost depend on where the
    /// chunk boundary fell.
    ///
    /// On by default, so a column holding a measurement is always a `DOUBLE`.
    /// The cost is exactness above 2^53: a column holding a nanosecond epoch, a
    /// snowflake identifier or a hash goes in [`Mapping::integer_columns`]
    /// rather than turning this off. Turning it off is only safe when every
    /// numeric field is a genuine integer and the table was pre-created, since
    /// otherwise a whole-valued first window creates the column as a `LONG`.
    pub numbers_as_double: bool,
    pub(crate) rounding_warned: RoundingWarned,
    pub(crate) header_keys: HeaderKeys,
}

/// Per-batch context that is constant across every row.
#[derive(Debug, Clone, Copy)]
pub struct RowContext<'a> {
    pub stream: &'a str,
    pub topic: &'a str,
    pub partition_id: u32,
}

impl Mapping {
    /// Appends one row.
    ///
    /// The row is built in full before the first write, so a message this
    /// mapping rejects never touches the buffer. Building is what validates: a
    /// ragged array, a malformed UUID and an object that will not serialize all
    /// fail while the row is being assembled, which is why there is no separate
    /// validation pass to keep in step.
    ///
    /// That ordering is what the caller relies on, and the reason is narrower
    /// than it looks. The client rolls back a row it refuses itself, on every
    /// column and symbol setter, so a rejection from the client costs one record
    /// and leaves the buffer usable. It cannot roll back a rejection of this
    /// mapping's own: once an earlier column of the row is written, only a
    /// buffer marker removes it, and the first row of a window has none.
    ///
    /// The row is built in the vectors of `scratch`, which the caller keeps
    /// across the rows of a chunk.
    pub(crate) fn append_row_with(
        &self,
        buffer: &mut Buffer,
        message: &ConsumedMessage,
        context: RowContext<'_>,
        scratch: &mut RowScratch,
    ) -> Result<(), RowError> {
        // `json_document` parses proto text that carries a JSON document, so it
        // is resolved once here rather than per field. Only proto text that
        // opens an object can carry fields. Any other proto text takes the
        // payload column whether it parses or not, so it is neither copied nor
        // parsed.
        let document = match &message.payload {
            Payload::Proto(text) if !text.trim_start().starts_with('{') => None,
            payload => payload.json_document(),
        };
        // A document that is not an object carries no fields to map, so it takes
        // the payload column rather than being rejected. That covers a JSON
        // array or scalar from the `json` decoder as well as proto text holding
        // one.
        let fields = document.as_deref().and_then(|value| value.as_object());
        let row = self.prepare_row(message, fields, scratch)?;
        let written = self.write_row(buffer, &row, message, context);
        scratch.keep(row);
        written
    }

    /// Merges the header keys that the rows of one chunk wrote into the
    /// connector's [`HeaderKeys`], and warns once when a chunk wrote a column
    /// with another key than an earlier chunk did. The caller runs it once
    /// per chunk.
    pub(crate) fn note_header_keys(&self, scratch: &mut RowScratch) {
        if scratch.header_keys.is_empty() {
            return;
        }
        // A poisoned lock only means that another chunk panicked while it
        // held it. The map is still a valid map, so the check goes on.
        let mut first = self
            .header_keys
            .first
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        for (column, key) in scratch.header_keys.drain() {
            match first.get(&column) {
                Some(seen) if *seen != key => {
                    self.header_keys.warn_once(self.id, &column, seen, &key);
                }
                Some(_) => {}
                None if first.len() < MAX_TRACKED_HEADER_COLUMNS => {
                    first.insert(column, key);
                }
                None => {}
            }
        }
    }

    /// [`Mapping::append_row_with`] with fresh vectors, for a single row.
    #[cfg(test)]
    pub fn append_row(
        &self,
        buffer: &mut Buffer,
        message: &ConsumedMessage,
        context: RowContext<'_>,
    ) -> Result<(), RowError> {
        self.append_row_with(buffer, message, context, &mut RowScratch::default())
    }

    /// Builds every value of one row, rejecting anything QuestDB would refuse.
    fn prepare_row<'m>(
        &self,
        message: &'m ConsumedMessage,
        fields: Option<&'m simd_json::owned::Object>,
        scratch: &mut RowScratch,
    ) -> Result<PreparedRow<'m>, RowError> {
        // QuestDB resolves column names case-insensitively, so two fields that
        // differ only in case are one column and the second value would be
        // dropped without a word. The claimed names are borrowed, then sorted
        // and compared once the row is complete, so a wide row costs
        // O(n log n) compares and no name is copied.
        let field_count = fields.map_or(0, |fields| fields.len());
        let header_count = match &message.headers {
            Some(headers) if self.include_headers => headers.len(),
            _ => 0,
        };
        let mut seen: Vec<&str> = recycle(mem::take(&mut scratch.seen));
        seen.reserve(RESERVED_COLUMNS + field_count + header_count);
        for (reserved, _) in self.reserved_columns_with_flags() {
            seen.push(reserved);
        }

        let mut row = PreparedRow {
            symbols: recycle(mem::take(&mut scratch.symbols)),
            columns: recycle(mem::take(&mut scratch.columns)),
            payload: None,
            headers: recycle(mem::take(&mut scratch.headers)),
            timestamp: PreparedTimestamp::Now,
        };
        row.columns.reserve(field_count);
        row.headers.reserve(header_count);
        // The value of `timestamp_field`, found by the field loop, which has to
        // visit every field anyway.
        let mut timestamp_value = None;

        match fields {
            Some(fields) => {
                // The timestamp field is matched without regard to case, like
                // QuestDB's own column lookup, so two fields can both claim to
                // be the timestamp. Stamping the row from whichever was read
                // last would be a silent choice, so the row is rejected.
                for (name, value) in fields {
                    if self.is_timestamp_field(name) {
                        if timestamp_value.replace(value).is_some() {
                            return Err(RowError::Invalid(format!(
                                "two fields match timestamp_field {}, so the row has no single timestamp",
                                self.timestamp_field.as_deref().unwrap_or_default()
                            )));
                        }
                        continue;
                    }
                    if self.symbol_columns.contains(name.as_str()) {
                        // A symbol has to be scalar. Dropping a structured
                        // value silently would also mean that listing a field
                        // in `symbol_columns` quietly discards data the default
                        // path would have kept as JSON text.
                        let Some(text) = scalar_to_symbol(value, &mut scratch.names) else {
                            if matches!(value, OwnedValue::Static(StaticNode::Null)) {
                                continue;
                            }
                            return Err(RowError::Invalid(format!(
                                "column {name} is listed in symbol_columns but its value is not a scalar"
                            )));
                        };
                        // The client accepts a longer symbol into the buffer
                        // and refuses it only when the frame is encoded, which
                        // loses the whole window instead of this record.
                        if text.len() > MAX_SYMBOL_LEN {
                            return Err(RowError::Invalid(format!(
                                "column {name} is listed in symbol_columns but its value is {} bytes, more than the {MAX_SYMBOL_LEN} bytes a QuestDB symbol can hold",
                                text.len()
                            )));
                        }
                        validate_name(name)?;
                        seen.push(name.as_str());
                        row.symbols.push((name.as_str(), text));
                        continue;
                    }
                    if matches!(value, OwnedValue::Static(StaticNode::Null)) {
                        // Encoded by omitting the column, so it claims nothing.
                        continue;
                    }
                    validate_name(name)?;
                    seen.push(name.as_str());
                    row.columns.push((
                        name.as_str(),
                        self.prepare_value(name.as_str(), value, scratch)?,
                    ));
                }
            }
            None => {
                row.payload = Some(self.payload_text(message, scratch)?);
            }
        }

        if self.include_headers
            && let Some(headers) = message.headers.as_ref()
        {
            for (key, value) in headers {
                let header = PreparedHeader::new(key, value, &mut scratch.names);
                scratch.note_header_key(self.id, &header.column, key, &self.header_keys);
                row.headers.push(header);
            }
            // The prefixed names are owned, so they are claimed from the row
            // that already holds them rather than being built a second time.
            // `header_column` already replaced every character that the client
            // refuses, so only the length can still be wrong.
            for header in &row.headers {
                for column in header.columns() {
                    // The rewritten name alone does not lead back to a raw or
                    // non-UTF-8 key, so the error names the key too.
                    if let Err(RowError::Invalid(reason)) = validate_name_length(column) {
                        return Err(RowError::Invalid(format!(
                            "{reason}{}",
                            header_keys_of(message, column)
                        )));
                    }
                    seen.push(column);
                }
            }
        }
        if let Some((first, second)) = case_collision(&mut seen) {
            let columns = if first == second {
                format!("column {first} is claimed twice")
            } else {
                format!(
                    "columns {first} and {second} collide, because QuestDB matches column names case-insensitively"
                )
            };
            // A header column starts with `header_` and a metadata column with
            // `iggy_`, so at most one of the two explanations applies.
            let source = match self
                .reserved_columns_with_flags()
                .find(|(reserved, _)| reserved.eq_ignore_ascii_case(first))
            {
                Some((reserved, flag)) => format!(
                    ". The connector writes a metadata column {reserved}, so set {flag} = false or rename the field"
                ),
                None => header_keys_of(message, first),
            };
            return Err(RowError::Invalid(format!("{columns}{source}")));
        }

        // Every QuestDB row needs at least one symbol or column before its
        // designated timestamp; `at` is refused otherwise. `seen` holds every
        // claimed name, reserved columns and headers included, and the payload
        // column is the one write that claims no name.
        if seen.is_empty() && row.payload.is_none() {
            return Err(RowError::Invalid(
                "row would have no columns, so QuestDB cannot accept it".to_owned(),
            ));
        }
        scratch.seen = recycle(seen);

        row.timestamp = self.prepare_timestamp(message, timestamp_value)?;
        Ok(row)
    }

    /// The enabled reserved columns, each with the configuration flag that
    /// enables it, so an error message can name the flag an operator has to
    /// turn off. The flag names are not derivable from the column names:
    /// `iggy_partition_id` is enabled by `include_partition_column`.
    pub(crate) fn reserved_columns_with_flags(
        &self,
    ) -> impl Iterator<Item = (&'static str, &'static str)> {
        let all: [(&'static str, &'static str, bool); RESERVED_COLUMNS] = [
            (
                STREAM_COLUMN,
                "include_stream_column",
                self.include_stream_column,
            ),
            (
                TOPIC_COLUMN,
                "include_topic_column",
                self.include_topic_column,
            ),
            (
                PARTITION_COLUMN,
                "include_partition_column",
                self.include_partition_column,
            ),
            (
                OFFSET_COLUMN,
                "include_offset_column",
                self.include_offset_column,
            ),
        ];
        all.into_iter()
            .filter_map(|(name, flag, included)| included.then_some((name, flag)))
    }

    /// Turns one JSON value into the column the write pass will set.
    ///
    /// The caller skips nulls and validates the name, so neither is rechecked
    /// here. The caller checks the name for collisions once the row is built.
    ///
    /// A nested object and an array are built in buffers taken from
    /// `scratch`, which gets them back once the row is written.
    fn prepare_value<'v>(
        &self,
        name: &str,
        value: &'v OwnedValue,
        scratch: &mut RowScratch,
    ) -> Result<PreparedValue<'v>, RowError> {
        // A column declared as a UUID has to hold one. A number or a boolean
        // there creates a column of the wrong type, which the client accepts and
        // the server then refuses for every later row.
        if self.uuid_columns.contains(name) {
            let OwnedValue::String(text) = value else {
                return Err(RowError::Invalid(format!(
                    "column {name} is listed in uuid_columns but its value is not a string"
                )));
            };
            let (lo, hi) = parse_uuid(text)
                .ok_or_else(|| RowError::Invalid(format!("column {name} is not a valid UUID")))?;
            return Ok(PreparedValue::Uuid(lo, hi));
        }
        // A column declared an integer has to hold a number, for the same
        // reason as the UUID guard above: any other type would create or pin
        // the column as something the declaration ruled out.
        let declared_integer = self.integer_columns.contains(name);
        if declared_integer
            && !matches!(
                value,
                OwnedValue::Static(StaticNode::I64(_) | StaticNode::U64(_) | StaticNode::F64(_))
            )
        {
            return Err(RowError::Invalid(format!(
                "column {name} is listed in integer_columns but its value is not a number"
            )));
        }
        // The rejection reasons below name the column and never the value: the
        // reason reaches the error log whatever `log_rejected_payload` says.
        let as_double = self.numbers_as_double && !declared_integer;
        match value {
            OwnedValue::Static(StaticNode::Bool(flag)) => Ok(PreparedValue::Bool(*flag)),
            OwnedValue::Static(StaticNode::I64(number)) if as_double => Ok(PreparedValue::F64(
                self.widen(name, i128::from(*number), Rounding::Scalar),
            )),
            OwnedValue::Static(StaticNode::U64(number)) if as_double => {
                let source = if i64::try_from(*number).is_ok() {
                    Rounding::Scalar
                } else {
                    Rounding::PastLong
                };
                Ok(PreparedValue::F64(self.widen(
                    name,
                    i128::from(*number),
                    source,
                )))
            }
            // QuestDB uses `i64::MIN` as the NULL of a `LONG` column, so the
            // value would land as NULL with no error from the client or the
            // server.
            OwnedValue::Static(StaticNode::I64(i64::MIN)) => Err(RowError::Invalid(format!(
                "column {name} holds the smallest LONG, which QuestDB stores as NULL"
            ))),
            OwnedValue::Static(StaticNode::I64(number)) => Ok(PreparedValue::I64(*number)),
            // QuestDB has no unsigned 64-bit column, so anything past `i64::MAX`
            // would wrap. A column the operator declared an integer is refused
            // rather than silently sent as a `DOUBLE`, which would change the
            // type the declaration pinned. Otherwise it degrades to `DOUBLE`,
            // which is what `numbers_as_double` would have done anyway, and
            // the remedy differs: no QuestDB column holds such a value exactly.
            OwnedValue::Static(StaticNode::U64(number)) => match i64::try_from(*number) {
                Ok(number) => Ok(PreparedValue::I64(number)),
                Err(_) if declared_integer => Err(RowError::Invalid(format!(
                    "column {name} is listed in integer_columns but its value is larger than a QuestDB LONG"
                ))),
                Err(_) => Ok(PreparedValue::F64(self.widen(
                    name,
                    i128::from(*number),
                    Rounding::PastLong,
                ))),
            },
            // A declared integer column takes a float whose value is whole,
            // because JSON gives no way to tell `20` from `20.0`. A genuinely
            // fractional value is refused: rounding it would store something the
            // producer did not send, and sending it as a `DOUBLE` would break
            // the type the declaration pinned. Both bounds are strict:
            // `i64::MAX as f64` rounds up to 2^63, which `as i64` would
            // saturate one short of the value sent, and -2^63 is `i64::MIN`,
            // which QuestDB stores as NULL.
            OwnedValue::Static(StaticNode::F64(number)) if declared_integer => {
                let number = *number;
                if number.fract() == 0.0 && number > i64::MIN as f64 && number < i64::MAX as f64 {
                    Ok(PreparedValue::I64(number as i64))
                } else {
                    Err(RowError::Invalid(format!(
                        "column {name} is listed in integer_columns but its value is not a whole number in range"
                    )))
                }
            }
            OwnedValue::Static(StaticNode::F64(number)) => Ok(PreparedValue::F64(*number)),
            OwnedValue::String(text) => Ok(PreparedValue::Str(text.as_str())),
            OwnedValue::Array(items) => {
                let (values, rounded) = parse_array(name, items, reuse_array(&mut scratch.arrays))?;
                if rounded {
                    self.warn_rounding(name, Rounding::ArrayElement);
                }
                Ok(PreparedValue::Array(values))
            }
            // Nested objects have no QuestDB column type; store the JSON text so
            // the data is preserved rather than dropped.
            OwnedValue::Object(_) => {
                let not_serializable =
                    || RowError::Invalid(format!("column {name} is not serializable"));
                let mut bytes = reuse_name(&mut scratch.names).into_bytes();
                simd_json::to_writer(&mut bytes, value).map_err(|_| not_serializable())?;
                // The writer emits JSON text, which is UTF-8, so the check
                // never fails. It only turns the bytes back into a `String`.
                String::from_utf8(bytes)
                    .map(PreparedValue::Json)
                    .map_err(|_| not_serializable())
            }
            // The caller skips a null before reaching this.
            OwnedValue::Static(StaticNode::Null) => Err(RowError::Invalid(format!(
                "column {name} holds a null that should have been skipped"
            ))),
        }
    }

    /// Hands back the widened number, and says so once if the widening lost
    /// precision.
    ///
    /// A `DOUBLE` carries integers exactly only up to 2^53. The README states
    /// the limit, but an operator who missed it would otherwise get rounded
    /// identifiers with no signal at all, so the first lossy conversion is
    /// logged, once per connector and source, naming the column and the remedy.
    fn widen(&self, name: &str, number: i128, source: Rounding) -> f64 {
        let (widened, exact) = widen_exact(number);
        if !exact {
            self.warn_rounding(name, source);
        }
        widened
    }

    fn warn_rounding(&self, name: &str, source: Rounding) {
        // The load keeps the flag's cache line shared once the warning fired.
        // A `swap` on every rounded value would write it from every topic task.
        let warned = self.rounding_warned.flag(source);
        if !warned.load(Ordering::Relaxed) && !warned.swap(true, Ordering::Relaxed) {
            warn!(
                "{CONNECTOR_NAME} ID: {} column {name} holds an integer a DOUBLE cannot carry exactly, so it was rounded; {}. This is reported once per connector for this kind of value.",
                self.id,
                source.remedy()
            );
        }
    }

    /// Resolves the designated timestamp, rejecting a value `Buffer::at` would
    /// refuse.
    ///
    /// Every source is checked, not just the payload one: `origin_timestamp`
    /// comes from the producer, and a value above `i64::MAX` wraps negative in
    /// the cast that follows. `at` is the one call that refuses a value without
    /// rolling the row back, so a timestamp it would refuse is the only input
    /// that could leave a partial row.
    ///
    /// `timestamp_value` is the payload field that `timestamp_field` names, if
    /// the payload has it.
    fn prepare_timestamp(
        &self,
        message: &ConsumedMessage,
        timestamp_value: Option<&OwnedValue>,
    ) -> Result<PreparedTimestamp, RowError> {
        match self.timestamp_source {
            TimestampSource::Server => Ok(PreparedTimestamp::Now),
            TimestampSource::Message => Ok(PreparedTimestamp::Micros(micros_timestamp(
                "message timestamp",
                message.timestamp,
            )?)),
            TimestampSource::Origin => Ok(PreparedTimestamp::Micros(micros_timestamp(
                "origin timestamp",
                message.origin_timestamp,
            )?)),
            TimestampSource::Payload => {
                // `QuestDbSink::new` refuses a payload source without a field.
                let field = self.timestamp_field.as_deref().unwrap_or_default();
                let raw = timestamp_value.ok_or_else(|| {
                    RowError::Invalid(format!("timestamp field {field} is missing"))
                })?;
                let number = timestamp_number(raw).ok_or_else(|| {
                    RowError::Invalid(format!("timestamp field {field} is not a number"))
                })?;
                // The conversion to nanoseconds happens here so the sign is the
                // one the buffer will see.
                let nanos = match number {
                    TimestampNumber::Integer(number) => self.timestamp_unit.to_nanos(number),
                    TimestampNumber::Float(number) => self.timestamp_unit.float_to_nanos(number),
                }
                .ok_or_else(|| {
                    RowError::Invalid(format!(
                        "timestamp field {field} is out of range for its unit"
                    ))
                })?;
                if nanos < 0 {
                    return Err(RowError::Invalid(format!(
                        "timestamp field {field} is before the Unix epoch"
                    )));
                }
                Ok(PreparedTimestamp::Nanos(TimestampNanos::new(nanos)))
            }
        }
    }

    /// Writes a prepared row. Every failure here belongs to the client.
    ///
    /// `QuestDbSink::new` checked the table name, and `prepare_row` checked
    /// every column name, so the names go to the client unchecked rather than
    /// being checked again for each row.
    fn write_row(
        &self,
        buffer: &mut Buffer,
        row: &PreparedRow<'_>,
        message: &ConsumedMessage,
        context: RowContext<'_>,
    ) -> Result<(), RowError> {
        buffer.table(TableName::new_unchecked(&self.table))?;

        // QuestDB requires every symbol to precede every non-symbol column.
        if self.include_stream_column {
            buffer.symbol(checked(STREAM_COLUMN), context.stream)?;
        }
        if self.include_topic_column {
            buffer.symbol(checked(TOPIC_COLUMN), context.topic)?;
        }
        for (name, text) in &row.symbols {
            buffer.symbol(checked(name), text.as_ref())?;
        }
        for column in row
            .headers
            .iter()
            .filter_map(|header| header.encoding.as_deref())
        {
            buffer.symbol(checked(column), DATA_ENCODING_BASE64)?;
        }

        if self.include_partition_column {
            buffer.column_i64(checked(PARTITION_COLUMN), i64::from(context.partition_id))?;
        }
        if self.include_offset_column {
            buffer.column_i64(checked(OFFSET_COLUMN), message.offset as i64)?;
        }
        for header in &row.headers {
            buffer.column_str(checked(&header.column), header.text.as_ref())?;
        }
        for (name, value) in &row.columns {
            match value {
                PreparedValue::Bool(flag) => {
                    buffer.column_bool(checked(name), *flag)?;
                }
                PreparedValue::I64(number) => {
                    buffer.column_i64(checked(name), *number)?;
                }
                PreparedValue::F64(number) => {
                    buffer.column_f64(checked(name), *number)?;
                }
                PreparedValue::Str(text) => {
                    buffer.column_str(checked(name), *text)?;
                }
                PreparedValue::Uuid(lo, hi) => {
                    buffer.column_uuid(checked(name), *lo, *hi)?;
                }
                PreparedValue::Array(values) => {
                    buffer.column_arr(checked(name), values)?;
                }
                PreparedValue::Json(text) => {
                    buffer.column_str(checked(name), text.as_str())?;
                }
            }
        }
        // A record with no field structure lands in a single column rather than
        // being silently dropped.
        if let Some(payload) = &row.payload {
            buffer.column_str(checked("payload"), payload.as_ref())?;
        }

        // `at` refuses a negative timestamp without rolling the row back, so
        // that failure leaves a partial row and the caller has to drop the
        // buffer. `prepare_timestamp` rejects every negative value, so that
        // failure does not happen here. The other failures without a rollback,
        // no table set and a timestamp that does not convert, cannot happen
        // either: the table is set above and both timestamp types convert.
        match row.timestamp {
            PreparedTimestamp::Now => buffer.at_now(),
            PreparedTimestamp::Micros(micros) => buffer.at(micros),
            PreparedTimestamp::Nanos(nanos) => buffer.at(nanos),
        }
        .map_err(RowError::Unrecoverable)
    }

    fn is_timestamp_field(&self, name: &str) -> bool {
        self.timestamp_source == TimestampSource::Payload
            && self
                .timestamp_field
                .as_deref()
                .is_some_and(|field| field.eq_ignore_ascii_case(name))
    }

    /// The payload of a record that carries no field structure.
    ///
    /// Nothing copies a payload that is already text. A `Payload::Json` reaches
    /// this only when the document is not an object, which has no fields to map,
    /// and is rendered back to JSON text, in a string that the chunk reuses.
    fn payload_text<'m>(
        &self,
        message: &'m ConsumedMessage,
        scratch: &mut RowScratch,
    ) -> Result<Cow<'m, str>, RowError> {
        match &message.payload {
            Payload::Text(text) | Payload::Proto(text) => Ok(Cow::Borrowed(text.as_str())),
            Payload::Raw(bytes) | Payload::FlatBuffer(bytes) | Payload::Avro(bytes) => {
                std::str::from_utf8(bytes)
                    .map(Cow::Borrowed)
                    .map_err(|_| RowError::Invalid("payload is not valid UTF-8".to_owned()))
            }
            Payload::Json(value) => {
                let not_rendered = |reason: String| {
                    RowError::Invalid(format!("payload cannot be rendered as JSON text: {reason}"))
                };
                let mut bytes = reuse_name(&mut scratch.names).into_bytes();
                simd_json::to_writer(&mut bytes, value)
                    .map_err(|error| not_rendered(error.to_string()))?;
                // The writer emits JSON text, which is UTF-8, so the check
                // never fails. It only turns the bytes back into a `String`.
                String::from_utf8(bytes)
                    .map(Cow::Owned)
                    .map_err(|error| not_rendered(error.to_string()))
            }
        }
    }
}

/// One row with every value built, ready to write.
struct PreparedRow<'m> {
    symbols: Vec<(&'m str, Cow<'m, str>)>,
    columns: Vec<(&'m str, PreparedValue<'m>)>,
    payload: Option<Cow<'m, str>>,
    headers: Vec<PreparedHeader<'m>>,
    timestamp: PreparedTimestamp,
}

/// The vectors that rows are built in, kept across the rows of a chunk so
/// that building a row allocates nothing once they have grown.
///
/// A row borrows from its message and, for proto text, from a document parsed
/// for that row alone, so the vectors are kept empty, with no borrow in them.
/// [`recycle`] gives each one to the next row with that row's lifetime. A
/// rejected row drops its vectors, and the next row grows new ones.
#[derive(Default)]
pub(crate) struct RowScratch {
    symbols: Vec<(&'static str, Cow<'static, str>)>,
    columns: Vec<(&'static str, PreparedValue<'static>)>,
    headers: Vec<PreparedHeader<'static>>,
    seen: Vec<&'static str>,
    /// Emptied strings from earlier rows, so that a header name, a rendered
    /// value, a nested object or a payload of the next row is written into one
    /// that already has room.
    names: Vec<String>,
    /// Emptied array buffers from earlier rows, for the next row's arrays.
    arrays: Vec<Vec<f64>>,
    /// The first header key of each header column in this chunk, keyed by the
    /// lowercase column name. [`Mapping::note_header_keys`] merges it into the
    /// connector's [`HeaderKeys`] once per chunk, so that the shared lock is
    /// not taken for every header of every row. It is probed for every
    /// header, so it uses the faster hasher.
    header_keys: AHashMap<String, HeaderKey>,
    /// The lowercase column name of the header in hand, built in a buffer
    /// that every header of the chunk reuses.
    lowered: String,
}

impl RowScratch {
    /// Keeps the vectors and the owned strings of a written row for the next
    /// one.
    fn keep(&mut self, mut row: PreparedRow<'_>) {
        for header in row.headers.drain(..) {
            self.names
                .extend(iter::once(header.column).chain(header.encoding));
            if let Cow::Owned(text) = header.text {
                self.names.push(text);
            }
        }
        for (_, text) in row.symbols.drain(..) {
            if let Cow::Owned(text) = text {
                self.names.push(text);
            }
        }
        if let Some(Cow::Owned(text)) = row.payload.take() {
            self.names.push(text);
        }
        for (_, value) in row.columns.drain(..) {
            match value {
                PreparedValue::Json(text) => self.names.push(text),
                PreparedValue::Array(array) => self.arrays.push(array.values),
                _ => {}
            }
        }
        self.symbols = recycle(row.symbols);
        self.columns = recycle(row.columns);
        self.headers = recycle(row.headers);
    }

    /// Records that `key` wrote to `column`, and warns once when a second key
    /// of this chunk writes to a column that another key wrote to first.
    ///
    /// The check costs one lookup per header, so it stops once the warning it
    /// exists for was logged. A column that is already lowercase, the common
    /// case, is looked up as it is, without a copy.
    fn note_header_key(&mut self, id: u32, column: &str, key: &HeaderKey, keys: &HeaderKeys) {
        if keys.warned.load(Ordering::Relaxed) {
            return;
        }
        let lowered = if column.bytes().any(|byte| byte.is_ascii_uppercase()) {
            self.lowered.clear();
            self.lowered.extend(
                column
                    .chars()
                    .map(|character| character.to_ascii_lowercase()),
            );
            self.lowered.as_str()
        } else {
            column
        };
        match self.header_keys.get(lowered) {
            Some(first) if first != key => keys.warn_once(id, column, first, key),
            Some(_) => {}
            None => {
                self.header_keys.insert(lowered.to_owned(), key.clone());
            }
        }
    }
}

/// An emptied string from `names`, or a new one when none is left.
fn reuse_name(names: &mut Vec<String>) -> String {
    let mut name = names.pop().unwrap_or_default();
    name.clear();
    name
}

/// An emptied buffer from `arrays`, or a new one when none is left.
fn reuse_array(arrays: &mut Vec<Vec<f64>>) -> Vec<f64> {
    let mut values = arrays.pop().unwrap_or_default();
    values.clear();
    values
}

/// An empty vector with the allocation of `items`, for items of another
/// lifetime. Collecting an emptied `vec::IntoIter` through `map` reuses its
/// buffer when the two item types have the same layout, as they do here.
fn recycle<T, U>(mut items: Vec<T>) -> Vec<U> {
    items.clear();
    items.into_iter().map(|_| unreachable!()).collect()
}

/// One header value and the columns it is written to.
struct PreparedHeader<'m> {
    column: String,
    /// The `iggy_header_<name>_encoding` symbol of a base64 value. A base64
    /// value looks like text, so the symbol records the encoding. A text value
    /// leaves the symbol NULL, and a table of text headers gains no column.
    encoding: Option<String>,
    text: Cow<'m, str>,
}

impl<'m> PreparedHeader<'m> {
    /// Builds the header's column names in strings taken from `names`.
    fn new(key: &HeaderKey, value: &'m HeaderValue, names: &mut Vec<String>) -> Self {
        let column = header_column(HeaderName(key), reuse_name(names));
        // `HeaderData` writes a valid UTF-8 string value as it is, so only the
        // other values need a formatted copy, and only they can be base64.
        let (text, encoding) = match value.as_str() {
            Ok(text) => (Cow::Borrowed(text), None),
            Err(_) => (
                Cow::Owned({
                    let mut text = reuse_name(names);
                    // Writing to a `String` never fails.
                    let _ = write!(text, "{}", HeaderData(value));
                    text
                }),
                (JsonHeaderValue(value).data_encoding() == DATA_ENCODING_BASE64).then(|| {
                    let mut encoding = reuse_name(names);
                    encoding.push_str(HEADER_ENCODING_PREFIX);
                    encoding.push_str(&column);
                    encoding.push_str(HEADER_ENCODING_SUFFIX);
                    encoding
                }),
            ),
        };
        Self {
            column,
            encoding,
            text,
        }
    }

    /// The value column, then the encoding symbol if there is one.
    fn columns(&self) -> impl Iterator<Item = &str> {
        iter::once(self.column.as_str()).chain(self.encoding.as_deref())
    }
}

/// A column value in the shape the client's setter takes.
enum PreparedValue<'m> {
    Bool(bool),
    I64(i64),
    F64(f64),
    Str(&'m str),
    Uuid(u64, u64),
    Array(ArrayValues),
    Json(String),
}

/// Which `Buffer::at` call closes the row.
enum PreparedTimestamp {
    Now,
    Micros(TimestampMicros),
    Nanos(TimestampNanos),
}

/// Turns an Apache Iggy microsecond timestamp into one the buffer will accept.
///
/// Apache Iggy carries these as `u64` and the client takes an `i64`, so a value
/// above `i64::MAX` wraps negative. `Buffer::at` refuses a negative value
/// without rolling the row back, which is the one input that leaves a partial
/// row, so it is caught before anything is written. `0` means unset in Apache
/// Iggy, so the row takes the clock of the connector host rather than landing
/// at the Unix epoch.
fn micros_timestamp(label: &str, micros: u64) -> Result<TimestampMicros, RowError> {
    if micros > i64::MAX as u64 {
        return Err(RowError::Invalid(format!(
            "{label} {micros} is too large for a QuestDB timestamp"
        )));
    }
    if micros == 0 {
        Ok(TimestampMicros::now())
    } else {
        Ok(TimestampMicros::new(micros as i64))
    }
}

/// `None` for anything that is not a usable timestamp.
///
/// A float needs the range test: `as i64` turns `NaN` into zero, which would
/// stamp the row at the epoch, and saturates a large value, which would stamp it
/// centuries away. Both would pass validation and store a wrong time.
fn timestamp_number(value: &OwnedValue) -> Option<TimestampNumber> {
    match value {
        OwnedValue::Static(StaticNode::I64(number)) => Some(TimestampNumber::Integer(*number)),
        OwnedValue::Static(StaticNode::U64(number)) => {
            i64::try_from(*number).ok().map(TimestampNumber::Integer)
        }
        OwnedValue::Static(StaticNode::F64(number)) => {
            let number = *number;
            // Strict on the upper side: `i64::MAX as f64` is 2^63, which `as
            // i64` would saturate to a timestamp one short of the one sent.
            (number.is_finite() && number >= i64::MIN as f64 && number < i64::MAX as f64)
                .then_some(TimestampNumber::Float(number))
        }
        _ => None,
    }
}

/// A payload timestamp before its unit is applied. A float stays a float, so
/// that its fraction survives the scaling to nanoseconds.
#[derive(Debug, Clone, Copy, PartialEq)]
enum TimestampNumber {
    Integer(i64),
    Float(f64),
}

/// Borrows a string value and renders the rest into a string taken from
/// `names`, so a symbol copies nothing and allocates nothing once the row
/// scratch has grown, on a path that runs for every symbol of every row.
fn scalar_to_symbol<'v>(value: &'v OwnedValue, names: &mut Vec<String>) -> Option<Cow<'v, str>> {
    let rendered = |names: &mut Vec<String>, value: &dyn fmt::Display| {
        let mut text = reuse_name(names);
        // Writing to a `String` never fails.
        let _ = write!(text, "{value}");
        Some(Cow::Owned(text))
    };
    match value {
        OwnedValue::Static(StaticNode::Bool(flag)) => rendered(names, flag),
        OwnedValue::Static(StaticNode::I64(number)) => rendered(names, number),
        OwnedValue::Static(StaticNode::U64(number)) => rendered(names, number),
        OwnedValue::Static(StaticNode::F64(number)) => rendered(names, number),
        OwnedValue::String(text) => Some(Cow::Borrowed(text.as_str())),
        _ => None,
    }
}

/// Two claimed names that differ only in case, if any. Sorting puts such a pair
/// next to each other, so one pass over neighbors finds it.
fn case_collision<'n>(seen: &mut [&'n str]) -> Option<(&'n str, &'n str)> {
    // ASCII case folding keeps the length, so the length decides most compares
    // and names that differ only in case still end up next to each other.
    seen.sort_unstable_by(|left, right| {
        left.len().cmp(&right.len()).then_with(|| {
            left.bytes()
                .map(|byte| byte.to_ascii_lowercase())
                .cmp(right.bytes().map(|byte| byte.to_ascii_lowercase()))
        })
    });
    seen.windows(2)
        .find(|pair| pair[0].eq_ignore_ascii_case(pair[1]))
        .map(|pair| (pair[0], pair[1]))
}

/// The column a header is written to: its [`HeaderName`] behind `header_`, with
/// every character QuestDB refuses in a column name replaced by `_`.
///
/// Header keys follow no naming rule, and the MQTT source sets `mqtt.protocol`
/// on every message, so refusing such a key would reject every record that
/// carries it. A non-string key's name holds `~` and `:`, which are refused too.
///
/// Two names that differ only in a replaced character, or only in case, give
/// one column, for example `a.b` and `a-b`, or two raw keys whose base64url
/// differs only in case or in `-` and `_`. In one record that is a collision,
/// and the row is rejected. Across records the keys share the column.
///
/// The name is written straight into `column`, an empty string that the
/// caller can take from an earlier row, so it is not built twice.
fn header_column(name: HeaderName<'_>, mut column: String) -> String {
    // Room for the longest marker, `~string_b64:`, and base64's 4/3 growth, so
    // a key of the common kinds needs no regrowth.
    column.reserve(HEADER_PREFIX.len() + 12 + name.0.as_bytes().len() * 4 / 3);
    column.push_str(HEADER_PREFIX);
    // `ColumnNameWriter` never fails, so neither does the write.
    let _ = write!(ColumnNameWriter(&mut column), "{name}");
    column
}

/// The header keys whose value column or encoding symbol is `column`, ignoring
/// case, for the error of a collision. It runs only when the row is rejected.
fn header_keys_of(message: &ConsumedMessage, column: &str) -> String {
    let keys: Vec<String> = message
        .headers
        .iter()
        .flatten()
        .filter(|(key, value)| {
            let header = PreparedHeader::new(key, value, &mut Vec::new());
            header
                .columns()
                .any(|name| name.eq_ignore_ascii_case(column))
        })
        .map(|(key, _)| HeaderName(key).to_string())
        .collect();
    if keys.is_empty() {
        String::new()
    } else {
        format!(", from the header keys {}", keys.join(", "))
    }
}

/// Appends text with every character QuestDB refuses replaced by `_`.
struct ColumnNameWriter<'c>(&'c mut String);

impl fmt::Write for ColumnNameWriter<'_> {
    fn write_str(&mut self, text: &str) -> fmt::Result {
        self.0.extend(text.chars().map(|character| {
            if refused_in_name(character) {
                '_'
            } else {
                character
            }
        }));
        Ok(())
    }
}

/// The characters that `ColumnName::new` in questdb-rs refuses.
fn refused_in_name(character: char) -> bool {
    matches!(
        character,
        '\0'..='\u{000f}'
            | '\u{007f}'
            | '\u{feff}'
            | '?'
            | '.'
            | ','
            | '\''
            | '"'
            | '\\'
            | '/'
            | ':'
            | ')'
            | '('
            | '+'
            | '-'
            | '*'
            | '%'
            | '~'
    )
}

/// Defers to the client's own character rule so the two cannot drift, and adds
/// the length limit.
fn validate_name(name: &str) -> Result<(), RowError> {
    validate_name_length(name)?;
    ColumnName::new(name)
        .map(|_| ())
        .map_err(|error| RowError::Invalid(format!("column {name} is not a valid name: {error}")))
}

/// A column name that `prepare_row` already checked, for the client setters,
/// which check a plain `&str` again.
fn checked(name: &str) -> ColumnName<'_> {
    ColumnName::new_unchecked(name)
}

/// The length limit, which `ColumnName` does not carry: it lives on the buffer,
/// so without this an over-long name is only refused once the row is half
/// written.
fn validate_name_length(name: &str) -> Result<(), RowError> {
    if name.len() > MAX_NAME_LEN {
        return Err(RowError::Invalid(format!(
            "column {name} is longer than the {MAX_NAME_LEN} bytes QuestDB allows"
        )));
    }
    Ok(())
}

/// A numeric array of up to three dimensions, held flat in row-major order.
///
/// QuestDB stores only `DOUBLE` arrays, so integer input is widened. Nesting is
/// supported to three dimensions. One flat vector means one allocation per
/// array, where nested vectors cost one for every inner row.
#[derive(Debug, PartialEq)]
struct ArrayValues {
    values: Vec<f64>,
    shape: [usize; 3],
    ndim: usize,
}

impl NdArrayView<f64> for ArrayValues {
    type Iter<'a> = std::slice::Iter<'a, f64>;

    fn ndim(&self) -> usize {
        self.ndim
    }

    fn dim(&self, index: usize) -> Result<usize, questdb::Error> {
        self.shape[..self.ndim].get(index).copied().ok_or_else(|| {
            questdb::Error::new(
                ErrorCode::ArrayError,
                format!("array has {} dimensions, not {}", self.ndim, index + 1),
            )
        })
    }

    fn as_slice(&self) -> Option<&[f64]> {
        Some(&self.values)
    }

    fn iter(&self) -> Self::Iter<'_> {
        self.values.iter()
    }
}

/// Why a JSON array cannot be a QuestDB array.
enum ArrayShapeError {
    NotNumeric,
    Ragged,
}

/// Builds the array the write pass will hand to the client.
///
/// Building it is what checks its shape, so the rules cannot be held in two
/// places that have to agree by hand.
///
/// `values` is an empty buffer that the array is written into.
fn parse_array(
    name: &str,
    items: &[OwnedValue],
    mut values: Vec<f64>,
) -> Result<(ArrayValues, bool), RowError> {
    // An empty array carries no values and no shape, so there is nothing to
    // store and nothing to infer a column type from.
    if items.is_empty() {
        return Err(RowError::Invalid(format!(
            "column {name} is an empty array"
        )));
    }
    let ndim = array_depth(items);
    if ndim > 3 {
        return Err(RowError::Invalid(format!(
            "column {name} exceeds the supported array nesting depth of 3"
        )));
    }
    values.reserve(items.len());
    let mut array = ArrayValues {
        values,
        shape: [0; 3],
        ndim,
    };
    // Whether any element lost precision on its way to a `DOUBLE`, so the
    // caller can say so once.
    let mut rounded = false;
    let mut shape = [None; 3];
    flatten_array(items, 0, &mut shape, &mut array, &mut rounded).map_err(|error| {
        RowError::Invalid(match error {
            ArrayShapeError::NotNumeric => format!("column {name} is not a numeric array"),
            // QuestDB arrays are rectangular: every sibling must have the
            // same length, or the client refuses the whole frame.
            ArrayShapeError::Ragged => format!("column {name} is not a rectangular array"),
        })
    })?;
    for (dim, length) in array.shape.iter_mut().zip(shape) {
        *dim = length.unwrap_or(0);
    }
    Ok((array, rounded))
}

/// Appends one level of a numeric array to `array`, in row-major order. The
/// first array met at each depth fixes the length of that dimension.
fn flatten_array(
    items: &[OwnedValue],
    depth: usize,
    shape: &mut [Option<usize>; 3],
    array: &mut ArrayValues,
    rounded: &mut bool,
) -> Result<(), ArrayShapeError> {
    let leaf = depth + 1 == array.ndim;
    // A row's elements are read before its length is compared, so a row
    // that is wrong in both ways is reported as not numeric.
    if !leaf && *shape[depth].get_or_insert(items.len()) != items.len() {
        return Err(ArrayShapeError::Ragged);
    }
    for item in items {
        if leaf {
            let value = array_element(item, rounded).ok_or(ArrayShapeError::NotNumeric)?;
            array.values.push(value);
        } else {
            let nested = item.as_array().ok_or(ArrayShapeError::NotNumeric)?;
            flatten_array(nested, depth + 1, shape, array, rounded)?;
        }
    }
    if leaf && *shape[depth].get_or_insert(items.len()) != items.len() {
        return Err(ArrayShapeError::Ragged);
    }
    Ok(())
}

fn array_depth(items: &[OwnedValue]) -> usize {
    match items.first() {
        Some(OwnedValue::Array(nested)) => 1 + array_depth(nested),
        _ => 1,
    }
}

/// Widens one array element, setting `rounded` when an integer lost
/// precision.
fn array_element(item: &OwnedValue, rounded: &mut bool) -> Option<f64> {
    let number = match item {
        OwnedValue::Static(StaticNode::I64(number)) => i128::from(*number),
        OwnedValue::Static(StaticNode::U64(number)) => i128::from(*number),
        OwnedValue::Static(StaticNode::F64(number)) => return Some(*number),
        _ => return None,
    };
    let (widened, exact) = widen_exact(number);
    *rounded |= !exact;
    Some(widened)
}

/// Splits a canonical RFC-4122 UUID into the `(lo, hi)` pair the client expects.
///
/// `Buffer::column_uuid` follows the `java.util.UUID` convention: `hi` is the
/// big-endian most-significant half and `lo` the least-significant half. Its
/// doc comment describes the little-endian *wire* layout instead, and taking
/// that literally writes a byte-reversed UUID with no error.
fn parse_uuid(text: &str) -> Option<(u64, u64)> {
    // `as_u64_pair` is `(most significant, least significant)`, so the halves
    // are swapped into the `(lo, hi)` order `column_uuid` takes.
    let (hi, lo) = Uuid::parse_str(text).ok()?.as_u64_pair();
    Some((lo, hi))
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use iggy::prelude::{HeaderKey, HeaderKind, HeaderValue};
    use iggy_connector_testing::headers::{LONG_RAW_KEY_CASE, header_cases};
    use questdb::ingress::ProtocolVersion;

    use super::*;

    /// The buffer production actually writes into. Prefer it for anything that
    /// could depend on columnar behaviour: it pins one type per column for the
    /// buffer's lifetime, resolves names case-insensitively and silently keeps
    /// the first write when a column is repeated in a row. The ILP buffer has
    /// none of those rules, so a test that needs them cannot use `buffer()`.
    fn qwp_buffer() -> Buffer {
        Buffer::qwp_ws_with_max_name_len(MAX_NAME_LEN)
    }

    /// `Buffer::new` yields an ILP buffer, which is what makes the rendering
    /// assertions possible: ILP is inspectable through `as_bytes`, so the exact
    /// wire output can be asserted, while the QWP buffer exposes nothing. Use it
    /// only where the assertion is about what was written rather than about
    /// columnar rules, and see `qwp_buffer` for the rest.
    /// V1 is the all-text InfluxDB-compatible encoding. V2 and V3 write
    /// doubles as binary, which would make `as_bytes` unreadable here.
    fn buffer() -> Buffer {
        Buffer::new(ProtocolVersion::V1)
    }

    fn mapping() -> Mapping {
        Mapping {
            id: 1,
            table: "events".to_owned(),
            symbol_columns: ColumnNames::default(),
            uuid_columns: ColumnNames::default(),
            integer_columns: ColumnNames::default(),
            timestamp_source: TimestampSource::Message,
            timestamp_field: None,
            timestamp_unit: TimestampUnit::Auto,
            include_stream_column: false,
            include_topic_column: false,
            include_partition_column: false,
            include_offset_column: false,
            include_headers: false,
            // The production default, so the common path is what the tests
            // exercise. The tests that pin integer typing set it back.
            numbers_as_double: true,
            rounding_warned: Default::default(),
            header_keys: Default::default(),
        }
    }

    /// The header column for a literal name, through the same writer as
    /// `header_columns`, for tests that start from a name rather than a key.
    fn column_for_name(name: impl fmt::Display) -> String {
        let mut column = String::from(HEADER_PREFIX);
        write!(ColumnNameWriter(&mut column), "{name}").unwrap();
        column
    }

    fn context() -> RowContext<'static> {
        RowContext {
            stream: "user_events",
            topic: "trades",
            partition_id: 3,
        }
    }

    fn json_message(json: &str) -> ConsumedMessage {
        let mut bytes = json.as_bytes().to_vec();
        ConsumedMessage {
            id: 7,
            offset: 42,
            checksum: 0,
            timestamp: 1_788_523_200_000_000,
            origin_timestamp: 1_700_000_000_000_000,
            headers: None,
            payload: Payload::Json(simd_json::to_owned_value(&mut bytes).unwrap()),
        }
    }

    fn rendered(buffer: &Buffer) -> String {
        String::from_utf8_lossy(buffer.as_bytes()).into_owned()
    }

    #[test]
    fn given_symbol_columns_when_appending_should_emit_symbols_before_columns() {
        let mut mapping = mapping();
        mapping.symbol_columns.insert("side".to_owned());
        let mut buffer = buffer();

        mapping
            .append_row(
                &mut buffer,
                &json_message(r#"{"side":"buy","price":1.5}"#),
                context(),
            )
            .unwrap();

        let line = rendered(&buffer);
        let symbol_at = line.find("side=buy").expect("symbol missing");
        let column_at = line.find("price=").expect("column missing");
        assert!(
            symbol_at < column_at,
            "symbol must precede column, got: {line}"
        );
    }

    #[test]
    fn given_metadata_flags_when_appending_should_add_stream_topic_partition_offset() {
        let mut mapping = mapping();
        mapping.include_stream_column = true;
        mapping.include_topic_column = true;
        mapping.include_partition_column = true;
        mapping.include_offset_column = true;
        let mut buffer = buffer();

        mapping
            .append_row(&mut buffer, &json_message(r#"{"price":1.5}"#), context())
            .unwrap();

        let line = rendered(&buffer);
        assert!(line.contains(",iggy_stream=user_events"), "{line}");
        assert!(line.contains(",iggy_topic=trades"), "{line}");
        assert!(line.contains("iggy_partition_id=3i"), "{line}");
        assert!(line.contains("iggy_offset=42i"), "{line}");
    }

    #[test]
    fn given_payload_fields_named_like_the_metadata_when_appending_should_keep_both() {
        // The prefix exists so that ordinary payload fields with these common
        // names are written next to the metadata columns, not rejected.
        let mut mapping = mapping();
        mapping.include_stream_column = true;
        mapping.include_topic_column = true;
        mapping.include_offset_column = true;
        let mut buffer = buffer();

        mapping
            .append_row(
                &mut buffer,
                &json_message(r#"{"stream":"web","topic":"orders","offset":7}"#),
                context(),
            )
            .unwrap();

        let line = rendered(&buffer);
        assert!(line.contains(",iggy_stream=user_events"), "{line}");
        assert!(line.contains("stream=\"web\""), "{line}");
        assert!(line.contains("topic=\"orders\""), "{line}");
        assert!(line.contains("iggy_offset=42i"), "{line}");
    }

    #[test]
    fn given_null_field_when_appending_should_omit_the_column() {
        let mut buffer = buffer();
        mapping()
            .append_row(
                &mut buffer,
                &json_message(r#"{"price":1.5,"missing":null}"#),
                context(),
            )
            .unwrap();

        let line = rendered(&buffer);
        assert!(line.contains("price="), "{line}");
        assert!(
            !line.contains("missing"),
            "null must be omitted, got: {line}"
        );
    }

    #[test]
    fn given_nested_object_when_appending_should_store_json_text() {
        let mut buffer = buffer();
        mapping()
            .append_row(
                &mut buffer,
                &json_message(r#"{"meta":{"venue":"nyse"}}"#),
                context(),
            )
            .unwrap();

        assert!(rendered(&buffer).contains("venue"), "{}", rendered(&buffer));
    }

    #[test]
    fn given_message_timestamp_source_when_appending_should_use_message_timestamp() {
        let mut buffer = buffer();
        mapping()
            .append_row(&mut buffer, &json_message(r#"{"price":1.5}"#), context())
            .unwrap();

        // Message timestamp is microseconds; ILP renders nanoseconds.
        assert!(
            rendered(&buffer).contains("1788523200000000000"),
            "{}",
            rendered(&buffer)
        );
    }

    #[test]
    fn given_origin_timestamp_source_when_appending_should_use_origin_timestamp() {
        let mut mapping = mapping();
        mapping.timestamp_source = TimestampSource::Origin;
        let mut buffer = buffer();

        mapping
            .append_row(&mut buffer, &json_message(r#"{"price":1.5}"#), context())
            .unwrap();

        assert!(
            rendered(&buffer).contains("1700000000000000000"),
            "{}",
            rendered(&buffer)
        );
    }

    #[test]
    fn given_payload_timestamp_source_when_appending_should_consume_the_field() {
        let mut mapping = mapping();
        mapping.timestamp_source = TimestampSource::Payload;
        mapping.timestamp_field = Some("event_time".to_owned());
        mapping.timestamp_unit = TimestampUnit::Micros;
        let mut buffer = buffer();

        mapping
            .append_row(
                &mut buffer,
                &json_message(r#"{"event_time":1788523200000000,"price":1.5}"#),
                context(),
            )
            .unwrap();

        let line = rendered(&buffer);
        assert!(line.contains("1788523200000000000"), "{line}");
        assert!(
            !line.contains("event_time="),
            "timestamp field must not double as a column, got: {line}"
        );
    }

    #[test]
    fn given_a_fractional_float_payload_timestamp_when_appending_should_keep_the_fraction() {
        let mut mapping = mapping();
        mapping.timestamp_source = TimestampSource::Payload;
        mapping.timestamp_field = Some("event_time".to_owned());
        mapping.timestamp_unit = TimestampUnit::Seconds;
        let mut buffer = buffer();

        mapping
            .append_row(
                &mut buffer,
                &json_message(r#"{"event_time":1788523200.5,"price":1.5}"#),
                context(),
            )
            .unwrap();

        let line = rendered(&buffer);
        assert!(line.contains("1788523200500000000"), "{line}");
    }

    #[test]
    fn given_missing_payload_timestamp_when_appending_should_reject_row() {
        let mut mapping = mapping();
        mapping.timestamp_source = TimestampSource::Payload;
        mapping.timestamp_field = Some("event_time".to_owned());
        let mut buffer = buffer();

        let error = mapping
            .append_row(&mut buffer, &json_message(r#"{"price":1.5}"#), context())
            .unwrap_err();

        assert!(matches!(error, RowError::Invalid(_)));
        assert!(buffer.is_empty(), "rejected row must leave nothing behind");
    }

    #[test]
    fn given_non_object_json_payload_when_appending_should_take_the_payload_column() {
        let mut buffer = buffer();
        mapping()
            .append_row(&mut buffer, &json_message("[1,2,3]"), context())
            .expect("a JSON array has no fields to map, so it takes the payload column");

        let line = rendered(&buffer);
        assert!(line.contains("payload="), "{line}");
        assert!(line.contains("[1,2,3]"), "{line}");
    }

    #[test]
    fn given_scalar_json_payload_when_appending_should_take_the_payload_column() {
        let mut buffer = buffer();
        mapping()
            .append_row(&mut buffer, &json_message("42"), context())
            .expect("a JSON scalar has no fields to map, so it takes the payload column");

        assert!(rendered(&buffer).contains("payload=\"42\""));
    }

    #[test]
    fn given_two_fields_matching_the_timestamp_field_when_appending_should_reject_the_row() {
        // QuestDB resolves a column name without regard to case, so `ts` and
        // `TS` are one column and one of the two values would be dropped.
        let mut mapping = mapping();
        mapping.timestamp_source = TimestampSource::Payload;
        mapping.timestamp_field = Some("ts".to_owned());
        let mut buffer = qwp_buffer();

        let error = mapping
            .append_row(
                &mut buffer,
                &json_message(r#"{"ts":1700000000000000,"TS":1800000000000000,"price":1.5}"#),
                context(),
            )
            .unwrap_err();

        assert!(matches!(error, RowError::Invalid(_)), "{error:?}");
        assert_eq!(buffer.row_count(), 0);
    }

    #[test]
    fn given_message_timestamp_above_the_client_range_when_appending_should_reject_the_row() {
        // `Buffer::at` refuses a negative value without rolling the row back, so
        // this is the one input that could leave a partial row behind.
        let mut message = json_message(r#"{"price":1.5}"#);
        message.timestamp = i64::MAX as u64 + 1;
        let mut buffer = qwp_buffer();

        let error = mapping()
            .append_row(&mut buffer, &message, context())
            .unwrap_err();

        assert!(matches!(error, RowError::Invalid(_)), "{error:?}");
        assert_eq!(buffer.row_count(), 0);
    }

    #[test]
    fn given_origin_timestamp_above_the_client_range_when_appending_should_reject_the_row() {
        // `origin_timestamp` is producer-supplied, so this one is reachable from
        // outside the server.
        let mut mapping = mapping();
        mapping.timestamp_source = TimestampSource::Origin;
        let mut message = json_message(r#"{"price":1.5}"#);
        message.origin_timestamp = u64::MAX;
        let mut buffer = qwp_buffer();

        let error = mapping
            .append_row(&mut buffer, &message, context())
            .unwrap_err();

        assert!(matches!(error, RowError::Invalid(_)), "{error:?}");
        assert_eq!(buffer.row_count(), 0);
    }

    #[test]
    fn given_mid_row_failure_when_appending_should_leave_the_buffer_untouched() {
        // The bad UUID sits after fields that would already have been encoded,
        // so this pins the guarantee that validation runs before any write
        // rather than being rolled back afterwards. It runs on the buffer
        // production uses: the ILP buffer rolls a refused row back by itself, so
        // it cannot tell whether the guarantee holds.
        let mut mapping = mapping();
        mapping.symbol_columns.insert("side".to_owned());
        mapping.uuid_columns.insert("trade_id".to_owned());
        let mut buffer = qwp_buffer();

        mapping
            .append_row(
                &mut buffer,
                &json_message(r#"{"side":"buy","price":1.0}"#),
                context(),
            )
            .unwrap();
        assert_eq!(buffer.row_count(), 1);

        let error = mapping
            .append_row(
                &mut buffer,
                &json_message(r#"{"side":"sell","price":2.0,"trade_id":"not-a-uuid"}"#),
                context(),
            )
            .unwrap_err();
        assert!(matches!(error, RowError::Invalid(_)));
        assert_eq!(
            buffer.row_count(),
            1,
            "rejected row must not reach the buffer"
        );

        // The buffer is still usable for the next record, which it would not be
        // if the rejection had left a half-written row behind.
        mapping
            .append_row(
                &mut buffer,
                &json_message(r#"{"side":"buy","price":3.0}"#),
                context(),
            )
            .unwrap();
        assert_eq!(buffer.row_count(), 2);
    }

    #[test]
    fn given_field_name_the_client_rejects_when_appending_should_reject_the_row() {
        // A dot is illegal in a column name. Validating names up front turns
        // what the client would raise mid-row into an ordinary row rejection,
        // so the rest of the batch still flushes.
        let mapping = mapping();
        let mut buffer = buffer();

        let error = mapping
            .append_row(
                &mut buffer,
                &json_message(r#"{"price.usd":1.0}"#),
                context(),
            )
            .unwrap_err();

        assert!(matches!(error, RowError::Invalid(_)), "got {error:?}");
        assert_eq!(buffer.row_count(), 0);

        mapping
            .append_row(&mut buffer, &json_message(r#"{"price":2.0}"#), context())
            .unwrap();
        assert_eq!(buffer.row_count(), 1);
    }

    /// Each of these reached the QuestDB client as a write-time failure before
    /// validation covered it. They are grouped so a regression in any one of
    /// them is visible as a change of error kind, not only of message.
    #[test]
    fn given_inputs_the_client_would_refuse_when_appending_should_reject_the_row() {
        let long_name = "a".repeat(MAX_NAME_LEN + 1);
        let cases: &[(&str, Mapping, String)] = &[
            (
                "name longer than the column limit",
                mapping(),
                format!(r#"{{"{long_name}":1}}"#),
            ),
            (
                "array rows of differing length",
                mapping(),
                r#"{"m":[[1,2],[3]]}"#.to_owned(),
            ),
            (
                "array planes of differing length",
                mapping(),
                r#"{"m":[[[1,2]],[[3,4],[5,6]]]}"#.to_owned(),
            ),
        ];

        for (label, mapping, payload) in cases {
            let mut buffer = qwp_buffer();
            let error = mapping
                .append_row(&mut buffer, &json_message(payload), context())
                .unwrap_err();
            assert!(
                matches!(error, RowError::Invalid(_)),
                "{label} should be an up-front rejection, got {error:?}"
            );
            assert_eq!(buffer.row_count(), 0, "{label} must not write a row");
        }
    }

    #[test]
    fn given_payload_timestamp_out_of_range_when_appending_should_reject_the_row() {
        let mut mapping = mapping();
        mapping.timestamp_source = TimestampSource::Payload;
        mapping.timestamp_field = Some("ts".to_owned());

        // Negative, and the extreme that used to panic in `abs`.
        for payload in [
            r#"{"ts":-1,"price":1.0}"#,
            r#"{"ts":-9223372036854775808,"price":1.0}"#,
        ] {
            let error = mapping
                .append_row(&mut qwp_buffer(), &json_message(payload), context())
                .unwrap_err();
            assert!(matches!(error, RowError::Invalid(_)), "got {error:?}");
        }

        // 1972 in milliseconds looks like seconds, and scaling it up overflows.
        // Reported rather than saturated to a date centuries away.
        let error = mapping
            .append_row(
                &mut qwp_buffer(),
                &json_message(r#"{"ts":63072000000,"price":1.0}"#),
                context(),
            )
            .unwrap_err();
        assert!(matches!(error, RowError::Invalid(_)), "got {error:?}");
    }

    #[test]
    fn given_column_name_collision_when_appending_should_reject_the_row() {
        // QuestDB matches column names case-insensitively, so each of these is
        // one column receiving two values. The client keeps the first and
        // discards the second without reporting it.
        let with_stream_column = || {
            let mut mapping = mapping();
            mapping.include_stream_column = true;
            mapping
        };
        let against_the_stream_symbol = || {
            let mut mapping = with_stream_column();
            mapping.symbol_columns.insert("iggy_stream".to_owned());
            mapping
        };

        let cases: &[(&str, Mapping, &str)] = &[
            (
                "payload field against the stream column",
                with_stream_column(),
                r#"{"IGGY_STREAM":"web","price":1.0}"#,
            ),
            (
                "payload field against the stream symbol",
                against_the_stream_symbol(),
                r#"{"iggy_stream":"web"}"#,
            ),
            (
                "two fields differing only in case",
                with_stream_column(),
                r#"{"Price":1.0,"price":2.0}"#,
            ),
        ];

        for (label, mapping, payload) in cases {
            let error = mapping
                .append_row(&mut qwp_buffer(), &json_message(payload), context())
                .unwrap_err();
            assert!(
                matches!(error, RowError::Invalid(_)),
                "{label} should be rejected rather than silently deduped, got {error:?}"
            );
        }
    }

    #[test]
    fn given_non_scalar_in_a_symbol_column_when_appending_should_reject_the_row() {
        // Without this the value is dropped and the row still counts as written,
        // so declaring a field as a symbol would quietly discard data the
        // default path keeps as JSON text.
        let mut mapping = mapping();
        mapping.symbol_columns.insert("region".to_owned());

        for payload in [
            r#"{"region":{"code":"eu"},"price":1.0}"#,
            r#"{"region":["eu","west"],"price":1.0}"#,
        ] {
            let error = mapping
                .append_row(&mut qwp_buffer(), &json_message(payload), context())
                .unwrap_err();
            assert!(matches!(error, RowError::Invalid(_)), "got {error:?}");
        }

        // A null symbol stays an omission rather than becoming a rejection.
        mapping
            .append_row(
                &mut qwp_buffer(),
                &json_message(r#"{"region":null,"price":1.0}"#),
                context(),
            )
            .unwrap();
    }

    #[test]
    fn given_a_symbol_past_the_client_limit_when_appending_should_reject_the_row() {
        // The client refuses it only when the frame is encoded, which would
        // lose every row of the window, so the row is rejected first.
        let mut mapping = mapping();
        mapping.symbol_columns.insert("region".to_owned());
        let payload = |len: usize| format!(r#"{{"region":"{}","price":1.0}}"#, "e".repeat(len));

        let error = mapping
            .append_row(
                &mut qwp_buffer(),
                &json_message(&payload(MAX_SYMBOL_LEN + 1)),
                context(),
            )
            .unwrap_err();
        assert!(
            matches!(&error, RowError::Invalid(reason) if reason.contains("region")),
            "got {error:?}"
        );
        mapping
            .append_row(
                &mut qwp_buffer(),
                &json_message(&payload(MAX_SYMBOL_LEN)),
                context(),
            )
            .unwrap();
    }

    #[test]
    fn given_configured_column_differing_in_case_when_appending_should_still_match() {
        // QuestDB resolves a column name without regard to case, so a
        // configured name has to match a field that differs only in case.
        // Matching case-sensitively would write the field as a plain column and
        // report nothing.
        let mut mapping = mapping();
        mapping.symbol_columns.insert("region".to_owned());
        mapping.uuid_columns.insert("trade_id".to_owned());

        // The uuid column matches, so a bad value there is rejected.
        let error = mapping
            .append_row(
                &mut qwp_buffer(),
                &json_message(r#"{"Trade_Id":"not-a-uuid"}"#),
                context(),
            )
            .unwrap_err();
        assert!(matches!(error, RowError::Invalid(_)), "got {error:?}");

        // The symbol column matches, so a non-scalar there is rejected.
        let error = mapping
            .append_row(
                &mut qwp_buffer(),
                &json_message(r#"{"REGION":{"a":1},"price":1.0}"#),
                context(),
            )
            .unwrap_err();
        assert!(matches!(error, RowError::Invalid(_)), "got {error:?}");
    }

    #[test]
    fn given_payload_timestamp_field_differing_in_case_when_appending_should_still_match() {
        let mut mapping = mapping();
        mapping.timestamp_source = TimestampSource::Payload;
        mapping.timestamp_field = Some("event_time".to_owned());

        // Found despite the case difference, so the row is written and the
        // field is not also emitted as a data column.
        let mut buffer = qwp_buffer();
        mapping
            .append_row(
                &mut buffer,
                &json_message(r#"{"Event_Time":1788523200000000,"price":1.0}"#),
                context(),
            )
            .unwrap();
        assert_eq!(buffer.row_count(), 1);
    }

    #[test]
    fn given_non_string_in_a_uuid_column_when_appending_should_reject_the_row() {
        // Without this the value is written as a LONG or a BOOLEAN into a column
        // the operator declared as a UUID.
        let mut mapping = mapping();
        mapping.uuid_columns.insert("trade_id".to_owned());

        for payload in [
            r#"{"trade_id":7,"price":1.0}"#,
            r#"{"trade_id":true,"price":1.0}"#,
            r#"{"trade_id":[1,2],"price":1.0}"#,
        ] {
            let error = mapping
                .append_row(&mut qwp_buffer(), &json_message(payload), context())
                .unwrap_err();
            assert!(
                matches!(error, RowError::Invalid(_)),
                "{payload} should be rejected, got {error:?}"
            );
        }

        // A null stays an omission.
        mapping
            .append_row(
                &mut qwp_buffer(),
                &json_message(r#"{"trade_id":null,"price":1.0}"#),
                context(),
            )
            .unwrap();
    }

    #[test]
    fn given_unusable_float_timestamp_when_appending_should_reject_the_row() {
        // `as i64` turns NaN into zero and saturates a huge value, so both would
        // store a wrong time with no error.
        assert_eq!(
            timestamp_number(&OwnedValue::Static(StaticNode::F64(f64::NAN))),
            None
        );
        assert_eq!(
            timestamp_number(&OwnedValue::Static(StaticNode::F64(f64::INFINITY))),
            None
        );
        assert_eq!(
            timestamp_number(&OwnedValue::Static(StaticNode::F64(1e30))),
            None
        );
        // An ordinary float still works, and keeps its fraction.
        assert_eq!(
            timestamp_number(&OwnedValue::Static(StaticNode::F64(1_788_523_200.5))),
            Some(TimestampNumber::Float(1_788_523_200.5))
        );
    }

    #[test]
    fn given_array_shapes_when_appending_should_accept_rectangular_and_reject_the_rest() {
        let mapping = mapping();
        let rejected = [
            (r#"{"m":[]}"#, "empty"),
            (r#"{"m":[[1,2],[3]]}"#, "ragged rows"),
            (r#"{"m":[[[1,2]],[[3,4],[5,6]]]}"#, "ragged planes"),
            (r#"{"m":[[[[1]]]]}"#, "depth four"),
            (r#"{"m":[1,"two"]}"#, "mixed types"),
            (r#"{"m":[1,[2]]}"#, "mixed nesting"),
        ];
        for (payload, label) in rejected {
            let error = mapping
                .append_row(&mut qwp_buffer(), &json_message(payload), context())
                .unwrap_err();
            assert!(
                matches!(error, RowError::Invalid(_)),
                "{label} should be rejected, got {error:?}"
            );
        }

        let accepted = [
            (r#"{"m":[1,2,3]}"#, "one dimension"),
            (r#"{"m":[[1,2],[3,4]]}"#, "two dimensions"),
            (r#"{"m":[[[1,2]],[[3,4]]]}"#, "three dimensions"),
        ];
        for (payload, label) in accepted {
            let mut buffer = qwp_buffer();
            mapping
                .append_row(&mut buffer, &json_message(payload), context())
                .unwrap_or_else(|error| panic!("{label} was rejected: {error:?}"));
            assert_eq!(buffer.row_count(), 1, "{label} did not write a row");
        }
    }

    #[test]
    fn given_nested_arrays_when_flattened_should_encode_like_nested_vectors() {
        // The client takes any `NdArrayView`, so the flat array has to give the
        // same bytes as the nested vectors that it replaced.
        let encode = |write: &dyn Fn(&mut Buffer)| {
            let mut buffer = Buffer::new(ProtocolVersion::V2);
            buffer.table("t").unwrap();
            write(&mut buffer);
            buffer.at_now().unwrap();
            buffer.as_bytes().to_vec()
        };
        let flat = |text: &str| {
            let value = simd_json::to_owned_value(&mut text.as_bytes().to_vec()).unwrap();
            let (array, _) = parse_array("m", value.as_array().unwrap(), Vec::new()).unwrap();
            encode(&|buffer| {
                buffer.column_arr("m", &array).unwrap();
            })
        };

        let two = vec![vec![1.0, 2.0, 3.0], vec![4.0, 5.0, 6.0]];
        assert_eq!(
            flat("[[1,2,3],[4,5,6]]"),
            encode(&|buffer| {
                buffer.column_arr("m", &two).unwrap();
            })
        );
        let three = vec![vec![vec![1.5, 2.0]], vec![vec![3.0, 4.5]]];
        assert_eq!(
            flat("[[[1.5,2.0]],[[3.0,4.5]]]"),
            encode(&|buffer| {
                buffer.column_arr("m", &three).unwrap();
            })
        );
    }

    #[test]
    fn given_u64_above_i64_max_when_appending_should_widen_to_double() {
        // QuestDB has no unsigned 64-bit column, so a value past `i64::MAX`
        // is written as a double whatever `numbers_as_double` says, and its
        // warning names that no column holds it exactly.
        for numbers_as_double in [true, false] {
            let mut buffer = buffer();
            let mut mapping = mapping();
            mapping.numbers_as_double = numbers_as_double;
            mapping
                .append_row(
                    &mut buffer,
                    &json_message(r#"{"fingerprint":18446744073709551615}"#),
                    context(),
                )
                .unwrap();
            let line = rendered(&buffer);
            assert!(
                line.contains("fingerprint=1.8446744073709552e19"),
                "numbers_as_double={numbers_as_double}: expected a double, got {line}"
            );
            let flags = &mapping.rounding_warned;
            assert!(flags.flag(Rounding::PastLong).load(Ordering::Relaxed));
            assert!(!flags.flag(Rounding::Scalar).load(Ordering::Relaxed));
            assert!(!flags.flag(Rounding::ArrayElement).load(Ordering::Relaxed));
        }
    }

    #[test]
    fn given_unset_message_timestamp_when_appending_should_use_the_server_clock() {
        // Apache Iggy writes zero for an unset timestamp, which would otherwise
        // stamp the row at the Unix epoch.
        let before = TimestampMicros::now();
        let mut message = json_message(r#"{"price":1.0}"#);
        message.timestamp = 0;
        let mut buffer = buffer();
        mapping()
            .append_row(&mut buffer, &message, context())
            .unwrap();
        let rendered = rendered(&buffer);
        let stamped: i64 = rendered
            .rsplit(' ')
            .next()
            .expect("no timestamp")
            .trim()
            .parse()
            .expect("timestamp is not a number");
        assert!(
            stamped >= before.as_i64() * 1000,
            "expected the server clock, got {stamped}"
        );
    }

    #[test]
    fn given_non_string_scalars_in_a_symbol_column_when_appending_should_render_them() {
        let mut mapping = mapping();
        mapping.symbol_columns.insert("flag".to_owned());
        mapping.symbol_columns.insert("count".to_owned());
        let mut buffer = buffer();
        mapping
            .append_row(
                &mut buffer,
                &json_message(r#"{"flag":true,"count":7,"price":1.0}"#),
                context(),
            )
            .unwrap();
        let line = rendered(&buffer);
        assert!(line.contains("flag=true"), "{line}");
        assert!(line.contains("count=7"), "{line}");
    }

    #[test]
    fn given_a_client_rejection_when_appending_should_leave_the_buffer_usable() {
        // The caller keeps the same buffer after a client rejection, which only
        // holds because the client rolls the half-written row back itself before
        // returning the error. Pin that: the rejected row leaves no trace and the
        // next record still writes.
        let mut mapping = mapping();
        // Mixed numbers are the cheapest way to make the client refuse a row,
        // and the default resolves them, so this opts out of that.
        mapping.numbers_as_double = false;
        let mut buffer = qwp_buffer();
        mapping
            .append_row(&mut buffer, &json_message(r#"{"temp":21.5}"#), context())
            .unwrap();
        assert_eq!(buffer.row_count(), 1);

        // Conflicts with the type the first row pinned.
        let error = mapping
            .append_row(&mut buffer, &json_message(r#"{"temp":21}"#), context())
            .unwrap_err();
        assert!(matches!(error, RowError::Client(_)), "got {error:?}");
        assert_eq!(
            buffer.row_count(),
            1,
            "the refused row must not be left half written"
        );

        // The same buffer still takes a well-formed record.
        mapping
            .append_row(&mut buffer, &json_message(r#"{"temp":22.5}"#), context())
            .unwrap();
        assert_eq!(buffer.row_count(), 2, "the buffer must still be usable");
    }

    #[test]
    fn given_numbers_as_double_when_appending_should_make_the_type_follow_the_name() {
        // Without this, an integer and a decimal for the same field pin two
        // different column types, and the client refuses whichever came second.
        let mut mapping = mapping();
        mapping.numbers_as_double = true;
        let mut buffer = qwp_buffer();
        mapping
            .append_row(&mut buffer, &json_message(r#"{"temp":21}"#), context())
            .unwrap();
        mapping
            .append_row(&mut buffer, &json_message(r#"{"temp":21.5}"#), context())
            .unwrap();
        assert_eq!(
            buffer.row_count(),
            2,
            "both rows should share one column type"
        );
    }

    #[test]
    fn given_the_default_when_appending_mixed_numbers_should_accept_both() {
        // JSON writes 21.0 as 21, so one field arriving whole and fractional is
        // ordinary data rather than a producer mistake. The default has to take
        // both, because which one the buffer saw first is an accident of
        // batching.
        let mapping = mapping();
        let mut buffer = qwp_buffer();
        mapping
            .append_row(&mut buffer, &json_message(r#"{"temp":21}"#), context())
            .unwrap();
        mapping
            .append_row(&mut buffer, &json_message(r#"{"temp":21.5}"#), context())
            .unwrap();
        assert_eq!(buffer.row_count(), 2);
    }

    #[test]
    fn given_numbers_as_double_off_when_appending_mixed_numbers_should_conflict() {
        // Opting out is what reintroduces the conflict, which is the cost the
        // README names for keeping integers exact without naming the columns.
        let mut mapping = mapping();
        mapping.numbers_as_double = false;
        let mut buffer = qwp_buffer();
        mapping
            .append_row(&mut buffer, &json_message(r#"{"temp":21}"#), context())
            .unwrap();
        let error = mapping
            .append_row(&mut buffer, &json_message(r#"{"temp":21.5}"#), context())
            .unwrap_err();
        assert!(matches!(error, RowError::Client(_)), "got {error:?}");
    }

    #[test]
    fn given_an_integer_column_when_appending_should_keep_it_exact() {
        // A column named in integer_columns stays LONG while every other number
        // is a DOUBLE, so an identifier past 2^53 survives the round trip.
        let mut mapping = mapping();
        mapping.integer_columns.insert("trade_id".to_owned());
        let mut buffer = buffer();
        mapping
            .append_row(
                &mut buffer,
                &json_message(r#"{"trade_id":9007199254740993,"price":1.5}"#),
                context(),
            )
            .unwrap();

        let line = rendered(&buffer);
        assert!(line.contains("trade_id=9007199254740993i"), "{line}");
        assert!(line.contains("price=1.5"), "{line}");
    }

    #[test]
    fn given_an_integer_column_holding_a_whole_float_when_appending_should_accept_it() {
        // JSON gives no way to tell 20 from 20.0, so a whole float in a declared
        // integer column is the same value and must not cost the record.
        let mut mapping = mapping();
        mapping.integer_columns.insert("count".to_owned());
        let mut buffer = buffer();
        mapping
            .append_row(&mut buffer, &json_message(r#"{"count":20.0}"#), context())
            .unwrap();

        assert!(rendered(&buffer).contains("count=20i"));
    }

    #[test]
    fn given_an_integer_column_holding_a_non_number_when_appending_should_reject_the_row() {
        // Any other type would create or pin the declared LONG column as
        // something else, which is the same harm the uuid_columns guard
        // prevents, so every non-numeric variant is refused before a write.
        for payload in [
            r#"{"count":"20"}"#,
            r#"{"count":true}"#,
            r#"{"count":[1,2]}"#,
            r#"{"count":{"n":1}}"#,
        ] {
            let mut mapping = mapping();
            mapping.integer_columns.insert("count".to_owned());
            let mut buffer = qwp_buffer();
            let error = mapping
                .append_row(&mut buffer, &json_message(payload), context())
                .unwrap_err();
            assert!(
                matches!(&error, RowError::Invalid(reason) if reason.contains("not a number")),
                "{payload}: {error:?}"
            );
            assert_eq!(buffer.row_count(), 0, "{payload}");
        }
    }

    #[test]
    fn given_an_integer_column_rejection_when_logged_should_not_carry_the_value() {
        // The reason reaches the error log whatever log_rejected_payload says,
        // so it names the column and never the payload value.
        for (payload, value) in [
            (r#"{"count":20.5}"#, "20.5"),
            (r#"{"count":18446744073709551615}"#, "18446744073709551615"),
            (r#"{"count":"20"}"#, "\"20\""),
        ] {
            let mut mapping = mapping();
            mapping.integer_columns.insert("count".to_owned());
            let error = mapping
                .append_row(&mut qwp_buffer(), &json_message(payload), context())
                .unwrap_err();
            let RowError::Invalid(reason) = error else {
                panic!("{payload}: expected a mapping rejection, got {error:?}");
            };
            assert!(
                reason.contains("count") && !reason.contains(value),
                "{payload}: the reason must name the column and not the value: {reason}"
            );
        }
    }

    #[test]
    fn given_a_number_past_2_to_53_when_widened_should_set_the_warned_flag() {
        // The README states the limit; this is the runtime signal for an
        // operator who missed it. The crate has no `tracing` subscriber in its
        // dev-dependencies, so the flag is what the test can observe: it
        // proves the lossy conversion was detected, not that the line was
        // written exactly once.
        let mapping = mapping();
        mapping
            .append_row(
                &mut qwp_buffer(),
                &json_message(r#"{"small":42,"temp":20.5}"#),
                context(),
            )
            .unwrap();
        assert!(
            !mapping.rounding_warned.any(),
            "an exact widening must not warn"
        );

        mapping
            .append_row(
                &mut qwp_buffer(),
                &json_message(r#"{"big":9007199254740993}"#),
                context(),
            )
            .unwrap();
        assert!(mapping.rounding_warned.any());

        // A declared integer column is never widened, so it never trips it.
        let mut declared = Mapping {
            rounding_warned: Default::default(),
            header_keys: Default::default(),
            ..mapping
        };
        declared.integer_columns.insert("big".to_owned());
        declared
            .append_row(
                &mut qwp_buffer(),
                &json_message(r#"{"big":9007199254740993}"#),
                context(),
            )
            .unwrap();
        assert!(!declared.rounding_warned.any());
    }

    #[test]
    fn given_the_extreme_integers_when_widened_should_still_count_as_rounded() {
        // `i64::MAX as f64` is 2^63 and `as i64` saturates back to `i64::MAX`,
        // so a check through the source width would call that exact. The
        // check goes through `i128`, where nothing saturates.
        for (payload, source) in [
            (r#"{"big":9223372036854775807}"#, Rounding::Scalar),
            (r#"{"big":18446744073709551615}"#, Rounding::PastLong),
        ] {
            let mapping = mapping();
            mapping
                .append_row(&mut qwp_buffer(), &json_message(payload), context())
                .unwrap();
            assert!(
                mapping.rounding_warned.flag(source).load(Ordering::Relaxed),
                "{payload} must count as a {source:?} rounding"
            );
        }
    }

    #[test]
    fn given_an_unsigned_value_within_i64_when_widened_should_tag_a_scalar_rounding() {
        // The JSON parser only yields an unsigned node past `i64::MAX`, so
        // this side of the unsigned arm is reachable only with a built value.
        let mapping = mapping();
        let value = OwnedValue::Static(StaticNode::U64((1 << 53) + 1));
        mapping
            .prepare_value("big", &value, &mut RowScratch::default())
            .unwrap();
        let flags = &mapping.rounding_warned;
        assert!(flags.flag(Rounding::Scalar).load(Ordering::Relaxed));
        assert!(!flags.flag(Rounding::PastLong).load(Ordering::Relaxed));
    }

    #[test]
    fn given_an_array_element_past_2_to_53_when_appending_should_set_the_warned_flag() {
        // The array path widens too, so it has to report the loss as well.
        let lossy = mapping();
        lossy
            .append_row(
                &mut qwp_buffer(),
                &json_message(r#"{"ids":[1,9007199254740993]}"#),
                context(),
            )
            .unwrap();
        assert!(lossy.rounding_warned.any());

        let exact = mapping();
        exact
            .append_row(
                &mut qwp_buffer(),
                &json_message(r#"{"ids":[1,2,3]}"#),
                context(),
            )
            .unwrap();
        assert!(!exact.rounding_warned.any());
    }

    #[test]
    fn given_an_array_rounding_first_when_a_scalar_rounds_later_should_still_warn_for_the_scalar() {
        // Each source has its own flag, so the array warning cannot use up the
        // one that points a scalar column at integer_columns.
        let mapping = mapping();
        mapping
            .append_row(
                &mut qwp_buffer(),
                &json_message(r#"{"ids":[9007199254740993]}"#),
                context(),
            )
            .unwrap();
        assert!(
            mapping
                .rounding_warned
                .flag(Rounding::ArrayElement)
                .load(Ordering::Relaxed)
        );
        assert!(
            !mapping
                .rounding_warned
                .flag(Rounding::Scalar)
                .load(Ordering::Relaxed)
        );

        mapping
            .append_row(
                &mut qwp_buffer(),
                &json_message(r#"{"big":9007199254740993}"#),
                context(),
            )
            .unwrap();
        assert!(
            mapping
                .rounding_warned
                .flag(Rounding::Scalar)
                .load(Ordering::Relaxed)
        );
    }

    #[test]
    fn given_an_integer_column_holding_2_to_63_as_a_float_when_appending_should_reject_the_row() {
        // `i64::MAX as f64` rounds up to 2^63, so an inclusive bound would admit
        // it and `as i64` would store one less than the producer sent. The
        // same value as an integer literal is refused, and the two must agree.
        let mut mapping = mapping();
        mapping.integer_columns.insert("count".to_owned());
        let mut buffer = qwp_buffer();
        let error = mapping
            .append_row(
                &mut buffer,
                &json_message(r#"{"count":9223372036854775808.0}"#),
                context(),
            )
            .unwrap_err();
        assert!(matches!(error, RowError::Invalid(_)), "{error:?}");
        assert_eq!(buffer.row_count(), 0);
    }

    #[test]
    fn given_the_smallest_long_when_appending_should_reject_the_row() {
        // QuestDB stores `i64::MIN` in a LONG column as NULL, so the row would
        // land with the value missing and no error anywhere. Both the integer
        // literal and the whole float -2^63 reach a LONG column.
        let mut integer_only = mapping();
        integer_only.numbers_as_double = false;
        let mut declared = mapping();
        declared.integer_columns.insert("count".to_owned());
        for (mapping, payload) in [
            (&integer_only, r#"{"count":-9223372036854775808}"#),
            (&declared, r#"{"count":-9223372036854775808}"#),
            (&declared, r#"{"count":-9223372036854775808.0}"#),
        ] {
            let mut buffer = qwp_buffer();
            let error = mapping
                .append_row(&mut buffer, &json_message(payload), context())
                .unwrap_err();
            assert!(
                matches!(error, RowError::Invalid(_)),
                "{payload}: {error:?}"
            );
            assert_eq!(buffer.row_count(), 0, "{payload}");
        }
    }

    #[test]
    fn given_one_above_the_smallest_long_when_appending_should_keep_it() {
        let mut mapping = mapping();
        mapping.integer_columns.insert("count".to_owned());
        let mut buffer = buffer();
        mapping
            .append_row(
                &mut buffer,
                &json_message(r#"{"count":-9223372036854775807}"#),
                context(),
            )
            .unwrap();
        let line = rendered(&buffer);
        assert!(line.contains("count=-9223372036854775807i"), "{line}");
    }

    #[test]
    fn given_the_smallest_long_when_numbers_are_doubles_should_keep_it_as_a_double() {
        // A DOUBLE column has its own NULL, NaN, so -2^63 is an ordinary value
        // there.
        let mut buffer = buffer();
        mapping()
            .append_row(
                &mut buffer,
                &json_message(r#"{"count":-9223372036854775808}"#),
                context(),
            )
            .unwrap();
        assert_eq!(buffer.row_count(), 1);
    }

    #[test]
    fn given_a_float_timestamp_of_2_to_63_when_resolved_should_be_out_of_range() {
        // The same strict bound as the integer column, for the same reason.
        assert_eq!(
            timestamp_number(&OwnedValue::Static(StaticNode::F64(9223372036854775808.0))),
            None
        );
        assert_eq!(
            timestamp_number(&OwnedValue::Static(StaticNode::F64(1.0))),
            Some(TimestampNumber::Float(1.0))
        );
    }

    #[test]
    fn given_a_raw_header_when_appending_should_write_it_as_base64() {
        // A `VARCHAR` cannot hold arbitrary bytes, so a Raw value is written as
        // standard base64, which keeps the bytes recoverable.
        let mut mapping = mapping();
        mapping.include_headers = true;
        let mut message = json_message(r#"{"price":1.5}"#);
        let mut headers = BTreeMap::new();
        headers.insert(
            HeaderKey::from_raw(HeaderKind::String, b"bin").unwrap(),
            HeaderValue::from_raw(HeaderKind::Raw, &[1, 2, 255]).unwrap(),
        );
        message.headers = Some(headers);
        let mut buffer = buffer();

        mapping
            .append_row(&mut buffer, &message, context())
            .unwrap();

        let line = rendered(&buffer);
        assert!(line.contains("header_bin=\"AQL/\""), "{line}");
        // The symbol tells a reader that the column holds base64.
        assert!(line.contains(",iggy_header_bin_encoding=base64 "), "{line}");
    }

    #[test]
    fn given_an_over_long_column_name_when_appending_should_say_bytes() {
        // The check counts bytes, which matches the client and the server
        // limit, so the message says bytes rather than characters.
        let name = "é".repeat(64);
        let payload = format!(r#"{{"{name}":1}}"#);
        let error = mapping()
            .append_row(&mut qwp_buffer(), &json_message(&payload), context())
            .unwrap_err();
        let RowError::Invalid(reason) = error else {
            panic!("expected a mapping rejection, got {error:?}");
        };
        assert!(reason.contains("bytes QuestDB allows"), "{reason}");
    }

    #[test]
    fn given_an_integer_column_holding_a_fraction_when_appending_should_reject_the_row() {
        // Rounding would store a value the producer never sent, and sending a
        // DOUBLE would break the type the declaration pinned.
        let mut mapping = mapping();
        mapping.integer_columns.insert("count".to_owned());
        let mut buffer = qwp_buffer();
        let error = mapping
            .append_row(&mut buffer, &json_message(r#"{"count":20.5}"#), context())
            .unwrap_err();

        assert!(matches!(error, RowError::Invalid(_)), "got {error:?}");
        assert_eq!(buffer.row_count(), 0);
    }

    #[test]
    fn given_proto_payload_holding_json_when_appending_should_take_the_field_path() {
        // A transform that cannot encode against its descriptor hands the sink
        // proto-tagged JSON text. Treating that as one opaque column would lose
        // the field structure the document carries.
        let mut message = json_message(r#"{"unused":1}"#);
        message.payload = Payload::Proto(r#"{"side":"buy","price":1.5}"#.to_owned());
        let mut buffer = buffer();
        mapping()
            .append_row(&mut buffer, &message, context())
            .unwrap();
        let line = rendered(&buffer);
        assert!(line.contains("price=1.5"), "{line}");
        assert!(!line.contains("payload="), "{line}");
    }

    #[test]
    fn given_proto_payload_without_json_when_appending_should_use_the_payload_column() {
        // Text that is not a document still belongs in the single column.
        let mut message = json_message(r#"{"unused":1}"#);
        message.payload = Payload::Proto("not a document".to_owned());
        let mut buffer = buffer();
        mapping()
            .append_row(&mut buffer, &message, context())
            .unwrap();
        assert!(
            rendered(&buffer).contains("payload="),
            "{}",
            rendered(&buffer)
        );
    }

    #[test]
    fn given_mixed_case_enum_names_when_parsing_should_be_accepted() {
        // Both parsers lowercase the input, so an operator writing "Payload" or
        // "MILLIS" gets what they meant rather than a configuration error.
        assert_eq!("PAYLOAD".parse(), Ok(TimestampSource::Payload));
        assert_eq!("Origin".parse(), Ok(TimestampSource::Origin));
        assert_eq!("MILLIS".parse(), Ok(TimestampUnit::Millis));
        assert_eq!("Nanos".parse(), Ok(TimestampUnit::Nanos));
    }

    #[test]
    fn given_row_with_no_columns_when_appending_should_reject_the_row() {
        // Every QuestDB row needs a column before its designated timestamp.
        let mut mapping = mapping();
        mapping.include_stream_column = false;
        mapping.include_topic_column = false;
        mapping.timestamp_source = TimestampSource::Payload;
        mapping.timestamp_field = Some("ts".to_owned());

        let error = mapping
            .append_row(
                &mut qwp_buffer(),
                &json_message(r#"{"ts":1700000000}"#),
                context(),
            )
            .unwrap_err();
        assert!(matches!(error, RowError::Invalid(_)), "got {error:?}");
    }

    #[test]
    fn given_ordinary_rows_when_appending_to_the_production_buffer_should_succeed() {
        // The guard for the checks above: they must not reject valid rows.
        let mut mapping = mapping();
        mapping.symbol_columns.insert("region".to_owned());
        mapping.include_stream_column = true;
        mapping.include_topic_column = true;

        for payload in [
            r#"{"region":"eu","price":1.0}"#,
            r#"{"price":1.0,"missing":null}"#,
            r#"{"m":[[1,2],[3,4]]}"#,
            r#"{"nested":{"a":1},"price":2.0}"#,
        ] {
            let mut buffer = qwp_buffer();
            mapping
                .append_row(&mut buffer, &json_message(payload), context())
                .unwrap_or_else(|error| panic!("{payload} was rejected: {error:?}"));
            assert_eq!(buffer.row_count(), 1, "{payload} did not write a row");
        }
    }

    #[test]
    fn given_a_marker_when_rewound_after_a_wide_row_should_keep_the_rows_in_front() {
        // The chunk loop rolls a row that fills the window back to the marker
        // and appends it again in the next window. That needs the production
        // buffer to drop exactly the last row, including a column it added.
        let mapping = mapping();
        let mut buffer = qwp_buffer();
        mapping
            .append_row(&mut buffer, &json_message(r#"{"seq":1}"#), context())
            .unwrap();
        let before = buffer.len();
        buffer.set_marker().unwrap();
        let wide = json_message(&format!(r#"{{"seq":2,"blob":"{}"}}"#, "x".repeat(4096)));
        mapping.append_row(&mut buffer, &wide, context()).unwrap();
        assert!(buffer.len() > before);

        buffer.rewind_to_marker().unwrap();
        assert_eq!(buffer.row_count(), 1);
        assert_eq!(buffer.len(), before);

        // The rewound buffer stays usable for the row it dropped.
        mapping.append_row(&mut buffer, &wide, context()).unwrap();
        assert_eq!(buffer.row_count(), 2);
    }

    #[test]
    fn given_headers_enabled_when_appending_should_prefix_header_columns() {
        let mut mapping = mapping();
        mapping.include_headers = true;
        let mut message = json_message(r#"{"price":1.5}"#);
        let mut headers = BTreeMap::new();
        headers.insert(
            HeaderKey::from_raw(HeaderKind::String, b"source").unwrap(),
            HeaderValue::from_raw(HeaderKind::String, b"gateway").unwrap(),
        );
        message.headers = Some(headers);
        let mut buffer = buffer();

        mapping
            .append_row(&mut buffer, &message, context())
            .unwrap();

        let line = rendered(&buffer);
        assert!(line.contains("header_source="), "{line}");
        // A text value adds no encoding symbol.
        assert!(!line.contains("_encoding"), "{line}");
    }

    #[test]
    fn given_header_keys_with_refused_characters_when_appending_should_replace_them() {
        // The MQTT source sets `mqtt.protocol` on every message, so a refused
        // character must not cost the record. The structureless path is used
        // because headers are written there too.
        let mut mapping = mapping();
        mapping.include_headers = true;
        let mut message = json_message(r#"{"unused":1}"#);
        message.payload = Payload::Text("body".to_owned());
        let mut headers = BTreeMap::new();
        for key in [b"mqtt.protocol".as_slice(), b"content-type"] {
            headers.insert(
                HeaderKey::from_raw(HeaderKind::String, key).unwrap(),
                HeaderValue::from_raw(HeaderKind::String, b"v").unwrap(),
            );
        }
        message.headers = Some(headers);
        let mut buffer = buffer();

        mapping
            .append_row(&mut buffer, &message, context())
            .unwrap();

        let rendered = rendered(&buffer);
        assert!(rendered.contains("header_mqtt_protocol="), "{rendered}");
        assert!(rendered.contains("header_content_type="), "{rendered}");
    }

    #[test]
    fn given_header_keys_equal_after_replacement_when_appending_should_reject_the_row() {
        let mut mapping = mapping();
        mapping.include_headers = true;
        let mut message = json_message(r#"{"price":1.5}"#);
        let mut headers = BTreeMap::new();
        for key in [b"a.b".as_slice(), b"a-b"] {
            headers.insert(
                HeaderKey::from_raw(HeaderKind::String, key).unwrap(),
                HeaderValue::from_raw(HeaderKind::String, b"v").unwrap(),
            );
        }
        message.headers = Some(headers);
        let mut buffer = buffer();

        let error = mapping
            .append_row(&mut buffer, &message, context())
            .unwrap_err();

        // The error names the column and both keys, so the operator can find
        // the producer that sets them.
        assert!(
            matches!(&error, RowError::Invalid(reason)
                if reason.contains("column header_a_b is claimed twice")
                    && reason.contains("header keys a-b, a.b")),
            "got {error:?}"
        );
        assert_eq!(buffer.row_count(), 0);
    }

    #[test]
    fn given_a_header_key_ending_in_encoding_when_appending_should_keep_it_apart_from_the_marker() {
        let mut mapping = mapping();
        mapping.include_headers = true;
        let mut message = json_message(r#"{"price":1.5}"#);
        message.headers = Some(BTreeMap::from([
            (
                HeaderKey::try_from("bin").unwrap(),
                HeaderValue::from_raw(HeaderKind::Raw, &[1, 2, 255]).unwrap(),
            ),
            (
                HeaderKey::try_from("bin_encoding").unwrap(),
                HeaderValue::from_raw(HeaderKind::String, b"v").unwrap(),
            ),
        ]));
        let mut buffer = buffer();

        mapping
            .append_row(&mut buffer, &message, context())
            .unwrap();

        // The marker starts with `iggy_`, which no header column does, so the
        // value of `bin_encoding` and the marker of `bin` get two columns.
        let line = rendered(&buffer);
        assert!(line.contains(",iggy_header_bin_encoding=base64"), "{line}");
        assert!(line.contains("header_bin_encoding=\"v\""), "{line}");
    }

    #[test]
    fn given_two_header_keys_with_one_column_when_appended_in_different_records_should_warn_once() {
        let mut mapping = mapping();
        mapping.include_headers = true;
        let with_key = |key: &str| {
            let mut message = json_message(r#"{"price":1.5}"#);
            message.headers = Some(BTreeMap::from([(
                HeaderKey::try_from(key).unwrap(),
                HeaderValue::from_raw(HeaderKind::String, b"v").unwrap(),
            )]));
            message
        };

        // One chunk, two records: the chunk notices it on its own.
        let mut scratch = RowScratch::default();
        for key in ["a.b", "a-b"] {
            mapping
                .append_row_with(&mut buffer(), &with_key(key), context(), &mut scratch)
                .unwrap();
        }
        assert!(mapping.header_keys.warned.load(Ordering::Relaxed));

        // Two chunks, one record each: the merge notices it.
        let mapping = Mapping {
            header_keys: Default::default(),
            ..mapping
        };
        for key in ["Host", "host"] {
            let mut scratch = RowScratch::default();
            mapping
                .append_row_with(&mut buffer(), &with_key(key), context(), &mut scratch)
                .unwrap();
            assert!(!mapping.header_keys.warned.load(Ordering::Relaxed));
            mapping.note_header_keys(&mut scratch);
        }
        assert!(mapping.header_keys.warned.load(Ordering::Relaxed));

        // The same key in every chunk is not a merge.
        let mapping = Mapping {
            header_keys: Default::default(),
            ..mapping
        };
        for _ in 0..2 {
            let mut scratch = RowScratch::default();
            mapping
                .append_row_with(&mut buffer(), &with_key("a.b"), context(), &mut scratch)
                .unwrap();
            mapping.note_header_keys(&mut scratch);
        }
        assert!(!mapping.header_keys.warned.load(Ordering::Relaxed));
    }

    #[test]
    fn given_a_vector_of_borrows_when_recycled_should_keep_its_allocation() {
        // `RowScratch` relies on this to build every row of a chunk in the same
        // vectors, so a change in how `collect` treats an emptied `IntoIter`
        // shows up here rather than as a silent allocation per row.
        let text = String::from("borrowed");
        let mut names: Vec<&str> = Vec::with_capacity(32);
        names.push(text.as_str());
        let capacity = names.capacity();
        let recycled: Vec<&'static str> = recycle(names);
        assert!(recycled.is_empty());
        assert_eq!(recycled.capacity(), capacity);
    }

    #[test]
    fn given_rows_built_in_one_scratch_when_appending_should_match_fresh_rows() {
        let mut mapping = mapping();
        mapping.include_headers = true;
        mapping.symbol_columns.insert("side".to_owned());
        let mut first = json_message(r#"{"side":"buy","price":1.5,"qty":2,"meta":{"desk":"fx"}}"#);
        first.headers = Some(BTreeMap::from([(
            HeaderKey::try_from("bin").unwrap(),
            HeaderValue::from_raw(HeaderKind::Raw, &[1, 2, 255]).unwrap(),
        )]));
        let second = json_message(r#"{"side":"sell","price":2.5,"meta":{"desk":"rates"}}"#);
        let rejected = json_message(r#"{"a":1,"A":2}"#);

        let mut fresh = buffer();
        for message in [&first, &second] {
            mapping.append_row(&mut fresh, message, context()).unwrap();
        }
        // A rejected row in the middle drops its vectors, and the rows around
        // it still come out the same.
        let mut reused = buffer();
        let mut scratch = RowScratch::default();
        for message in [&first, &rejected, &second] {
            let _ = mapping.append_row_with(&mut reused, message, context(), &mut scratch);
        }

        assert_eq!(reused.row_count(), 2);
        assert_eq!(rendered(&reused), rendered(&fresh));
    }

    #[test]
    fn given_proto_text_without_an_object_when_appending_should_take_the_payload_column() {
        // The text is not parsed, and it still lands where a failed parse put
        // it before, including text that is valid JSON but not an object.
        let mapping = mapping();
        for text in ["plain text", "[1,2]", "  42", "{not json"] {
            let mut message = json_message("{}");
            message.payload = Payload::Proto(text.to_owned());
            let mut buffer = buffer();
            mapping
                .append_row(&mut buffer, &message, context())
                .unwrap();
            let line = rendered(&buffer);
            assert!(line.contains("payload="), "{text}: {line}");
        }
    }

    #[test]
    fn given_a_field_named_like_an_encoding_symbol_when_appending_should_name_the_header_key() {
        let mut mapping = mapping();
        mapping.include_headers = true;
        let mut message = json_message(r#"{"iggy_header_bin_encoding":"x"}"#);
        message.headers = Some(BTreeMap::from([(
            HeaderKey::try_from("bin").unwrap(),
            HeaderValue::from_raw(HeaderKind::Raw, &[1, 2, 255]).unwrap(),
        )]));
        let mut buffer = buffer();

        let error = mapping
            .append_row(&mut buffer, &message, context())
            .unwrap_err();

        // `bin` claims the column through its encoding symbol, so the error
        // names that key.
        assert!(
            matches!(&error, RowError::Invalid(reason)
                if reason.contains("column iggy_header_bin_encoding is claimed twice")
                    && reason.contains("header keys bin")),
            "got {error:?}"
        );
        assert_eq!(buffer.row_count(), 0);
    }

    #[test]
    fn given_a_field_named_like_an_enabled_metadata_column_when_appending_should_name_the_flag() {
        let mut mapping = mapping();
        mapping.include_stream_column = true;
        let mut buffer = buffer();

        let error = mapping
            .append_row(
                &mut buffer,
                &json_message(r#"{"iggy_stream":"web"}"#),
                context(),
            )
            .unwrap_err();

        assert!(
            matches!(&error, RowError::Invalid(reason)
                if reason.contains("column iggy_stream is claimed twice")
                    && reason.contains("set include_stream_column = false or rename the field")),
            "got {error:?}"
        );
        assert_eq!(buffer.row_count(), 0);
    }

    #[test]
    fn given_shared_header_cases_when_appending_should_name_them_by_header_name() {
        // The long raw key's name is past the 127 bytes QuestDB allows, so it
        // is tested on its own below.
        let cases: Vec<_> = header_cases()
            .into_iter()
            .filter(|case| case.label != LONG_RAW_KEY_CASE)
            .collect();
        let mut mapping = mapping();
        mapping.include_headers = true;
        let mut message = json_message(r#"{"price":1.5}"#);
        message.headers = Some(
            cases
                .iter()
                .map(|case| (case.key.clone(), case.value.clone()))
                .collect(),
        );

        // The QWP buffer matches names without regard to case, as QuestDB
        // does, so this also proves that no two names collide there.
        mapping
            .append_row(&mut qwp_buffer(), &message, context())
            .expect("the shared cases must not collide");

        let mut buffer = buffer();
        mapping
            .append_row(&mut buffer, &message, context())
            .unwrap();
        let line = rendered(&buffer);
        for case in &cases {
            let column = column_for_name(&case.expected_name);
            assert!(
                line.contains(&format!("{column}=\"{}\"", case.label)),
                "{}: {line}",
                case.label
            );
        }
    }

    #[test]
    fn given_a_header_name_past_the_length_limit_when_appending_should_reject_the_row() {
        let mut mapping = mapping();
        mapping.include_headers = true;
        let mut message = json_message(r#"{"price":1.5}"#);
        let case = header_cases()
            .into_iter()
            .find(|case| case.label == LONG_RAW_KEY_CASE)
            .unwrap();
        let key_name = HeaderName(&case.key).to_string();
        message.headers = Some(BTreeMap::from([(case.key, case.value)]));
        let mut buffer = qwp_buffer();

        let error = mapping
            .append_row(&mut buffer, &message, context())
            .unwrap_err();

        // The error names the key as well, because the rewritten column does
        // not lead back to a raw key.
        assert!(
            matches!(&error, RowError::Invalid(reason)
                if reason.contains("longer than")
                    && reason.contains(&format!("from the header keys {key_name}"))),
            "got {error:?}"
        );
        assert_eq!(buffer.row_count(), 0);
    }

    #[test]
    fn given_a_non_utf8_string_header_value_when_appending_should_write_it_as_base64() {
        let mut mapping = mapping();
        mapping.include_headers = true;
        let mut message = json_message(r#"{"price":1.5}"#);
        let mut headers = BTreeMap::new();
        headers.insert(
            HeaderKey::try_from("lossy").unwrap(),
            HeaderValue::from_raw(HeaderKind::String, &[0xff, 0xfe]).unwrap(),
        );
        message.headers = Some(headers);
        let mut buffer = buffer();

        mapping
            .append_row(&mut buffer, &message, context())
            .unwrap();

        let line = rendered(&buffer);
        assert!(line.contains("header_lossy=\"//4=\""), "{line}");
        assert!(
            line.contains(",iggy_header_lossy_encoding=base64 "),
            "{line}"
        );
    }

    #[test]
    fn given_every_ascii_character_when_naming_a_header_should_replace_exactly_what_the_client_refuses()
     {
        // The client is the authority, so the rule is checked against it one
        // character at a time: a character it accepts must survive unchanged.
        for character in (0..=0x7f_u8).map(char::from).chain(['\u{feff}', 'é']) {
            let refused = ColumnName::new(&format!("a{character}")).is_err();
            assert_eq!(refused_in_name(character), refused, "{character:?}");
            let column = column_for_name(character);
            assert!(ColumnName::new(&column).is_ok(), "{column:?}");
        }
    }

    #[test]
    fn given_text_payload_when_appending_should_store_it_in_payload_column() {
        let mut buffer = buffer();
        let mut message = json_message(r#"{"unused":1}"#);
        message.payload = Payload::Text("raw body".to_owned());

        mapping()
            .append_row(&mut buffer, &message, context())
            .unwrap();

        assert!(
            rendered(&buffer).contains("payload="),
            "{}",
            rendered(&buffer)
        );
    }

    #[test]
    fn given_every_structureless_payload_when_appending_should_store_it_verbatim() {
        // Raw, Proto, FlatBuffer and Avro all reach QuestDB through the same
        // `payload` column, so each variant must round-trip its bytes rather
        // than being dropped for having no JSON fields.
        let body = "structureless body";
        let variants = [
            Payload::Raw(body.as_bytes().to_vec()),
            Payload::Proto(body.to_owned()),
            Payload::FlatBuffer(body.as_bytes().to_vec()),
            Payload::Avro(body.as_bytes().to_vec()),
        ];

        for payload in variants {
            let label = format!("{payload:?}");
            let mut buffer = buffer();
            let mut message = json_message(r#"{"unused":1}"#);
            message.payload = payload;

            mapping()
                .append_row(&mut buffer, &message, context())
                .unwrap();

            let line = rendered(&buffer);
            assert!(line.contains(body), "{label} did not round-trip: {line}");
        }
    }

    #[test]
    fn given_non_utf8_binary_payload_when_appending_should_reject_row() {
        // 0xFF is never valid UTF-8, and a VARCHAR column cannot hold it.
        let mut buffer = buffer();
        let mut message = json_message(r#"{"unused":1}"#);
        message.payload = Payload::Raw(vec![0xFF, 0xFE, 0xFD]);

        let error = mapping()
            .append_row(&mut buffer, &message, context())
            .unwrap_err();

        assert!(matches!(error, RowError::Invalid(_)));
        assert!(buffer.is_empty(), "rejected row must leave nothing behind");
    }

    #[test]
    fn given_structureless_payload_when_metadata_enabled_should_still_add_columns() {
        // A payload with no fields must not skip the stream/topic/offset
        // columns, which are the only way to trace such a row back.
        let mut mapping = mapping();
        mapping.include_stream_column = true;
        mapping.include_topic_column = true;
        mapping.include_offset_column = true;
        let mut buffer = buffer();
        let mut message = json_message(r#"{"unused":1}"#);
        message.payload = Payload::Text("body".to_owned());

        mapping
            .append_row(&mut buffer, &message, context())
            .unwrap();

        let line = rendered(&buffer);
        assert!(line.contains("stream=user_events"), "{line}");
        assert!(line.contains("topic=trades"), "{line}");
        assert!(line.contains("offset=42i"), "{line}");
        assert!(line.contains("payload="), "{line}");
    }

    #[test]
    fn given_canonical_uuid_when_parsed_should_use_java_uuid_halves() {
        let (lo, hi) = parse_uuid("123e4567-e89b-12d3-a456-426614174000").unwrap();
        assert_eq!(hi, 0x123e_4567_e89b_12d3);
        assert_eq!(lo, 0xa456_4266_1417_4000);
    }

    #[test]
    fn given_uuid_without_dashes_when_parsed_should_succeed() {
        let dashed = parse_uuid("123e4567-e89b-12d3-a456-426614174000").unwrap();
        let plain = parse_uuid("123e4567e89b12d3a456426614174000").unwrap();
        assert_eq!(dashed, plain);
    }

    #[test]
    fn given_malformed_uuid_when_parsed_should_return_none() {
        assert!(parse_uuid("not-a-uuid").is_none());
        assert!(parse_uuid("123e4567-e89b-12d3-a456-42661417400").is_none());
        assert!(parse_uuid("123e4567-e89b-12d3-a456-4266141740000").is_none());
    }

    #[test]
    fn given_float_timestamps_when_converting_should_keep_the_fraction() {
        assert_eq!(
            TimestampUnit::Seconds.float_to_nanos(1_788_523_200.5),
            Some(1_788_523_200_500_000_000)
        );
        assert_eq!(
            TimestampUnit::Millis.float_to_nanos(1_788_523_200_000.25),
            Some(1_788_523_200_000_250_000)
        );
        // `Auto` buckets by the whole part, then keeps the fraction of that unit.
        assert_eq!(
            TimestampUnit::Auto.float_to_nanos(1_788_523_200.5),
            Some(1_788_523_200_500_000_000)
        );
        // A whole float matches the integer path exactly.
        assert_eq!(
            TimestampUnit::Micros.float_to_nanos(1_788_523_200_000_000.0),
            TimestampUnit::Micros.to_nanos(1_788_523_200_000_000)
        );
        // Overflow is still refused rather than saturated.
        assert_eq!(TimestampUnit::Seconds.float_to_nanos(1e18), None);
    }

    #[test]
    fn given_auto_unit_when_converting_should_pick_bucket_by_magnitude() {
        // 2026-09-04T12:00:00Z in each unit maps to the same nanosecond value.
        let nanos = 1_788_523_200_000_000_000i64;
        assert_eq!(TimestampUnit::Auto.to_nanos(1_788_523_200), Some(nanos));
        assert_eq!(TimestampUnit::Auto.to_nanos(1_788_523_200_000), Some(nanos));
        assert_eq!(
            TimestampUnit::Auto.to_nanos(1_788_523_200_000_000),
            Some(nanos)
        );
        assert_eq!(TimestampUnit::Auto.to_nanos(nanos), Some(nanos));
    }

    #[test]
    fn given_explicit_unit_when_converting_should_ignore_magnitude() {
        assert_eq!(TimestampUnit::Seconds.to_nanos(1), Some(1_000_000_000));
        assert_eq!(TimestampUnit::Millis.to_nanos(1), Some(1_000_000));
        assert_eq!(TimestampUnit::Micros.to_nanos(1), Some(1_000));
        assert_eq!(TimestampUnit::Nanos.to_nanos(1), Some(1));
    }

    #[test]
    fn given_i64_min_when_converting_should_not_panic() {
        // `abs` has no positive counterpart for `i64::MIN` and panics under
        // overflow checks, so the bucket test uses `unsigned_abs`. The value
        // falls through as nanoseconds and is refused later for being negative.
        assert_eq!(TimestampUnit::Auto.to_nanos(i64::MIN), Some(i64::MIN));
    }

    #[test]
    fn given_millis_below_the_seconds_threshold_when_auto_should_not_saturate() {
        // 1972-01-01 in milliseconds is small enough to look like seconds.
        // Multiplying it up overflows, which is reported rather than saturating
        // to a timestamp centuries in the future.
        assert_eq!(TimestampUnit::Auto.to_nanos(63_072_000_000), None);
    }

    #[test]
    fn given_unknown_names_when_parsing_enums_should_be_refused() {
        assert_eq!("nonsense".parse::<TimestampSource>(), Err(UnknownVariant));
        assert_eq!("fortnights".parse::<TimestampUnit>(), Err(UnknownVariant));
        // An unset value takes the default, which is what `QuestDbSink::new`
        // falls back to.
        assert_eq!(TimestampSource::default(), TimestampSource::Message);
        assert_eq!(TimestampUnit::default(), TimestampUnit::Auto);
    }

    #[test]
    fn given_nested_arrays_when_measuring_depth_should_count_levels() {
        let mut flat = br#"[1.0, 2.0]"#.to_vec();
        let flat: OwnedValue = simd_json::to_owned_value(&mut flat).unwrap();
        let mut nested = br#"[[1.0], [2.0]]"#.to_vec();
        let nested: OwnedValue = simd_json::to_owned_value(&mut nested).unwrap();

        assert_eq!(array_depth(flat.as_array().unwrap()), 1);
        assert_eq!(array_depth(nested.as_array().unwrap()), 2);
    }
}
