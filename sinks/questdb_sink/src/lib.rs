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

mod mapping;

use std::collections::HashMap;
use std::fmt;
use std::ops::Range;
use std::str::FromStr;
use std::sync::atomic::{AtomicI64, AtomicU64, Ordering};
use std::sync::{Arc, Once};
use std::time::Duration;

use async_trait::async_trait;
use humantime::Duration as HumanDuration;
use iggy_connector_sdk::{
    ConsumedMessage, Error, MessagesMetadata, Payload, Sink, TopicMetadata, sink_connector,
};
use questdb::ConnectHandlers;
use questdb::ErrorCode;
use questdb::QuestDb;
use questdb::ingress::AckLevel;
use questdb::ingress::TableName;
use questdb::ingress::{QwpWsErrorCategory, QwpWsErrorHandler, QwpWsErrorPolicy, QwpWsSenderError};
use secrecy::{ExposeSecret, SecretString};
use serde::Deserialize;
use tokio::sync::Mutex;
use tracing::{debug, error, info, warn};

use mapping::{
    MAX_NAME_LEN, Mapping, RowContext, RowError, RowScratch, TimestampSource, TimestampUnit,
    UnknownVariant,
};

sink_connector!(QuestDbSink);

const CONNECTOR_NAME: &str = "QuestDB sink";
const DEFAULT_BATCH_SIZE: u32 = 1000;
const DEFAULT_FLUSH_TIMEOUT: Duration = Duration::from_secs(30);

/// Rejections are logged one line per record so a bad message can be traced
/// back to its offset. A batch where every record is malformed would otherwise
/// emit one line per message, so the tail is collapsed into a single summary.
const MAX_LOGGED_REJECTIONS_PER_BATCH: usize = 20;

/// Upper bound on the payload text included when `log_rejected_payload` is on.
const REJECTED_PAYLOAD_PREVIEW_BYTES: usize = 512;

/// A sustained lack of acknowledgement stops looking like lag and starts
/// looking like a sink that is quietly committing nothing, so after this many
/// consecutive unacknowledged batches the connector reports a failure to make
/// the condition visible in the runtime's metrics.
const PENDING_ACK_BATCHES_BEFORE_ESCALATION: u64 = 10;

/// Installs a process-wide rustls crypto provider exactly once.
///
/// `rustls` picks a provider from crate features only when exactly one of
/// `ring` and `aws-lc-rs` is linked. This plugin's own dependency graph links
/// both: `aws-lc-rs` arrives through `iggy_connector_sdk`, `iggy` and
/// `reqwest`, and `ring` through `questdb-rs`. So `ClientConfig::builder()`
/// cannot choose and **panics** instead of returning an error, taking down any
/// `wss://` connection attempt. Installing one up front makes the choice
/// explicit.
///
/// The plugin is a dlopened cdylib with its own copy of `rustls`, so this
/// install reaches the plugin's slot only and cannot affect the host. Inside
/// the plugin the slot is still process-wide, which is why the install runs
/// once and ignores a second attempt. This runs only for a connect string that
/// needs TLS, so a plain `ws://` sink makes no choice it does not use.
static INSTALL_CRYPTO_PROVIDER: Once = Once::new();

fn ensure_crypto_provider() {
    INSTALL_CRYPTO_PROVIDER.call_once(|| {
        let _ = rustls::crypto::ring::default_provider().install_default();
    });
}

/// Flush once the encoded buffer reaches this many bytes, independently of
/// `batch_size`.
///
/// QWP caps a single frame at the smallest of `max_buf_size`, the server's
/// advertised maximum batch size, and the store-and-forward segment payload
/// capacity, which is about 4 MiB with the default 4 MiB segments. The row API
/// does not split an oversized buffer, so without a byte bound a batch of wide
/// rows is rejected outright and every row in it is lost. The chunk loop also
/// gives a row that reaches this bound a window of its own, so a row larger
/// than the cap fails alone.
///
/// The margin between this default and that cap is deliberate, and larger than
/// one row's worth. `Buffer::len` is a local estimate on this transport rather
/// than the encoded frame size: it does not model the connection-scoped symbol
/// dictionary, which a frame can have to re-ship in full. A frame cap below
/// this bound, from `max_buf_size` or a server that advertises a smaller batch
/// size, refuses a window before the bound fires. The chunk then builds the
/// window again in halves until each frame fits.
const DEFAULT_MAX_FLUSH_BYTES: usize = 1_000_000;

/// Deserialize only. Nothing re-serializes a plugin config, and leaving
/// `Serialize` off is what keeps `connection_string` unserializable rather than
/// merely un-serialized. Unknown keys are refused, so a misspelled option
/// fails the start instead of leaving its default in force.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct QuestDbSinkConfig {
    /// QuestDB connect string, `ws::` or `wss::`. May carry credentials, so it
    /// is treated as a secret throughout.
    pub connection_string: SecretString,
    pub table: String,
    pub timestamp_source: Option<String>,
    pub timestamp_field: Option<String>,
    pub timestamp_unit: Option<String>,
    pub symbol_columns: Option<Vec<String>>,
    pub uuid_columns: Option<Vec<String>>,
    /// Payload fields to store as `LONG`, whatever `numbers_as_double` says.
    pub integer_columns: Option<Vec<String>>,
    pub include_stream_column: Option<bool>,
    pub include_topic_column: Option<bool>,
    pub include_partition_column: Option<bool>,
    pub include_offset_column: Option<bool>,
    pub include_headers: Option<bool>,
    pub ack_level: Option<String>,
    pub flush_timeout: Option<String>,
    pub batch_size: Option<u32>,
    /// Flush once the encoded buffer reaches this many bytes, regardless of
    /// `batch_size`. Guards the QWP per-frame cap.
    pub max_flush_bytes: Option<usize>,
    /// Include a truncated payload in the log line for a rejected message.
    /// Off by default: a rejected payload is still user data and may carry
    /// personal or otherwise sensitive fields.
    pub log_rejected_payload: Option<bool>,
    /// Write every JSON number as a `DOUBLE`, so a column holding a measurement
    /// is always a `DOUBLE`. On by default. Name the columns that must stay
    /// exact past 2^53 in `integer_columns` rather than turning this off.
    pub numbers_as_double: Option<bool>,
    pub verbose_logging: Option<bool>,
}

#[derive(Debug)]
pub struct QuestDbSink {
    pub id: u32,
    connection_string: SecretString,
    mapping: Arc<Mapping>,
    ack_level: AckLevelSetting,
    /// The ack level that the connect string's `request_durable_ack` asks
    /// for, read once in `new` and checked against `ack_level` in `open`.
    /// `None` when the key is unset or holds a value that the client refuses.
    requested_ack_level: Option<AckLevelSetting>,
    flush_timeout: Duration,
    batch_size: usize,
    max_flush_bytes: usize,
    verbose: bool,
    log_rejected_payload: bool,
    /// Set by the runtime's at-least-once handshake. A record that the sink
    /// cannot write, or a chunk that QuestDB did not acknowledge, then fails
    /// the call, and the runtime delivers the batch again.
    at_least_once: bool,
    /// The `sink_connector!` macro requires `new` to be infallible, so a bad
    /// config is recorded here and surfaced from `open` instead.
    init_error: Option<Error>,
    /// Terminal rejections the client reported minus the chunks that failed
    /// on one. Signed on purpose: the client reports on its own thread, after
    /// the chunk has already seen the latched error, so the chunk's retire can
    /// run first and the balance has to be allowed to dip below zero.
    server_rejections: Arc<AtomicI64>,
    /// The client's dropped-event count the last time a batch looked, so a
    /// change is noticed exactly once.
    rejection_drops_seen: AtomicU64,
    db: Option<Arc<QuestDb>>,
    state: Mutex<State>,
}

#[derive(Debug)]
struct State {
    messages_processed: u64,
    rows_written: u64,
    rejected_rows: u64,
    pending_acks: u64,
    /// Consecutive batches per stream and topic that finished with at least one
    /// chunk still unacknowledged. An acknowledged batch clears its own entry.
    consecutive_pending_ack_batches: HashMap<(String, String), u64>,
}

impl QuestDbSink {
    pub fn new(id: u32, config: QuestDbSinkConfig) -> Self {
        let mut init_error = None;
        let mut reject = |error: Error| {
            if init_error.is_none() {
                init_error = Some(error);
            }
        };

        let timestamp_source = match config.timestamp_source.as_deref() {
            None => TimestampSource::default(),
            Some(raw) => raw.parse().unwrap_or_else(|UnknownVariant| {
                reject(Error::InvalidConfigValue(format!(
                    "timestamp_source {raw:?} is not one of message, origin, payload, server"
                )));
                TimestampSource::default()
            }),
        };
        let timestamp_unit = match config.timestamp_unit.as_deref() {
            None => TimestampUnit::default(),
            Some(raw) => raw.parse().unwrap_or_else(|UnknownVariant| {
                reject(Error::InvalidConfigValue(format!(
                    "timestamp_unit {raw:?} is not one of auto, seconds (s), millis (ms), micros (us), nanos (ns)"
                )));
                TimestampUnit::default()
            }),
        };
        match (
            timestamp_source == TimestampSource::Payload,
            config.timestamp_field.is_some(),
        ) {
            (true, false) => reject(Error::InvalidConfigValue(
                "timestamp_field is required when timestamp_source is \"payload\"".to_owned(),
            )),
            // Setting only the field would keep the other source's time with no
            // sign that the field was ignored.
            (false, true) => reject(Error::InvalidConfigValue(
                "timestamp_field is used only when timestamp_source is \"payload\"".to_owned(),
            )),
            _ => {}
        }
        // The same holds for a concrete unit. `auto` is the default, and the
        // example configurations set it with every source, so it stays allowed.
        if timestamp_source != TimestampSource::Payload && timestamp_unit != TimestampUnit::Auto {
            reject(Error::InvalidConfigValue(
                "timestamp_unit other than \"auto\" is used only when timestamp_source is \"payload\""
                    .to_owned(),
            ));
        }
        // An unset ack level follows the connect string, as in the client: a
        // connect string that asks for durable acks gets them. Otherwise the
        // flag would only cost the extra acknowledgements, while every flush
        // still waits only for the server to accept it.
        let durable_ack_request = request_durable_ack(config.connection_string.expose_secret());
        // A value that the client refuses asks for nothing, and the client then
        // refuses the connect string.
        let requested_ack_level = durable_ack_request.as_deref().and_then(ack_level_for);
        let ack_level = match config.ack_level.as_deref() {
            None => requested_ack_level.unwrap_or(AckLevelSetting::Ok),
            Some(raw) => raw.parse().unwrap_or_else(|UnknownVariant| {
                reject(Error::InvalidConfigValue(format!(
                    "ack_level {raw:?} is not one of ok, durable, local_durable"
                )));
                AckLevelSetting::Ok
            }),
        };
        // An explicit `ok` is allowed with a durable request, because the
        // client accepts the pair, but it waits for less than the connect
        // string asks for, so the operator hears of it.
        if config.ack_level.is_some()
            && ack_level == AckLevelSetting::Ok
            && let Some(requested) =
                requested_ack_level.filter(|requested| *requested != AckLevelSetting::Ok)
        {
            warn!(
                "{CONNECTOR_NAME} ID: {id} ack_level = \"ok\" overrides request_durable_ack in the connection string, which asks for {}; each chunk waits only for the server to accept it. Leave ack_level unset to wait for the durable acknowledgement",
                requested
            );
        }

        let flush_timeout = match config.flush_timeout.as_deref() {
            None => DEFAULT_FLUSH_TIMEOUT,
            Some(raw) => HumanDuration::from_str(raw)
                .map(|duration| *duration)
                .unwrap_or_else(|error| {
                    reject(Error::InvalidConfigValue(format!(
                        "flush_timeout {raw:?} is not a duration such as \"30s\": {error}"
                    )));
                    DEFAULT_FLUSH_TIMEOUT
                }),
        };
        // The client reads a zero timeout as "no deadline". At the default ack
        // level the sink treats it as fire and forget and never calls `wait`,
        // but a durable ack level has to wait, so the pair would block the FFI
        // call for good with no way to stop the connector.
        if ack_level != AckLevelSetting::Ok && flush_timeout.is_zero() {
            let source = if config.ack_level.is_none() {
                ", which request_durable_ack in the connection string selects"
            } else {
                ""
            };
            reject(Error::InvalidConfigValue(format!(
                "flush_timeout must be greater than zero when ack_level is \"{}\"{source}, because a zero timeout never expires",
                ack_level
            )));
        }

        // A bad table name fails on every row of every batch, so it is caught
        // once at construction instead of being reported per record forever.
        // `TableName::new` checks the characters only; the length limit lives
        // on the buffer, so it is checked here against the same bound.
        if config.table.len() > MAX_NAME_LEN {
            reject(Error::InvalidConfigValue(format!(
                "table {} is longer than the {MAX_NAME_LEN} bytes QuestDB allows",
                config.table
            )));
        } else if let Err(error) = TableName::new(config.table.as_str()) {
            reject(Error::InvalidConfigValue(format!(
                "table {} is not a valid QuestDB table name: {error}",
                config.table
            )));
        }

        let mapping = Mapping {
            id,
            table: config.table,
            symbol_columns: config
                .symbol_columns
                .unwrap_or_default()
                .into_iter()
                .collect(),
            uuid_columns: config
                .uuid_columns
                .unwrap_or_default()
                .into_iter()
                .collect(),
            integer_columns: config
                .integer_columns
                .unwrap_or_default()
                .into_iter()
                .collect(),
            timestamp_source,
            timestamp_field: config.timestamp_field,
            timestamp_unit,
            include_stream_column: config.include_stream_column.unwrap_or(true),
            include_topic_column: config.include_topic_column.unwrap_or(true),
            include_partition_column: config.include_partition_column.unwrap_or(false),
            include_offset_column: config.include_offset_column.unwrap_or(false),
            include_headers: config.include_headers.unwrap_or(false),
            numbers_as_double: config.numbers_as_double.unwrap_or(true),
            rounding_warned: Default::default(),
            header_keys: Default::default(),
        };
        if let Err(error) = validate_column_overlap(&mapping) {
            reject(error);
        }
        // Zero would make the chunking loop take nothing on every pass and never
        // finish, holding the FFI call open for good, so it is refused as the
        // other settings that cannot work are.
        let mut size = |field: &str, value: Option<usize>, default: usize| {
            size_or_default(field, value, default).unwrap_or_else(|error| {
                reject(error);
                default
            })
        };
        let batch_size = size(
            "batch_size",
            config.batch_size.map(|rows| rows as usize),
            DEFAULT_BATCH_SIZE as usize,
        );
        let max_flush_bytes = size(
            "max_flush_bytes",
            config.max_flush_bytes,
            DEFAULT_MAX_FLUSH_BYTES,
        );

        Self {
            id,
            connection_string: config.connection_string,
            mapping: Arc::new(mapping),
            ack_level,
            requested_ack_level,
            flush_timeout,
            batch_size,
            max_flush_bytes,
            verbose: config.verbose_logging.unwrap_or(false),
            log_rejected_payload: config.log_rejected_payload.unwrap_or(false),
            at_least_once: false,
            init_error,
            server_rejections: Arc::new(AtomicI64::new(0)),
            rejection_drops_seen: AtomicU64::new(0),
            db: None,
            state: Mutex::new(State {
                messages_processed: 0,
                rows_written: 0,
                rejected_rows: 0,
                pending_acks: 0,
                consecutive_pending_ack_batches: HashMap::new(),
            }),
        }
    }

    /// `already_logged` is the number of records earlier chunks of the same
    /// runtime batch rejected, so the per-batch log cap spans the batch rather
    /// than restarting for each chunk.
    async fn write_batch(
        &self,
        db: Arc<QuestDb>,
        messages: Vec<ConsumedMessage>,
        topic_metadata: &TopicMetadata,
        partition_id: u32,
        already_logged: usize,
        mode: ChunkMode,
    ) -> Result<BatchOutcome, Box<ChunkFailure>> {
        let mapping = Arc::clone(&self.mapping);
        let stream = topic_metadata.stream.clone();
        let topic = topic_metadata.topic.clone();
        let ack_level = self.ack_level;
        let flush_timeout = self.flush_timeout;
        let id = mode.id;
        let max_flush_bytes = self.max_flush_bytes;
        // Taken before the messages move into the task, so a panic in it can
        // still be reported by offset.
        let whole_chunk = MessageRange::of(&messages);

        // `questdb-rs` is synchronous: the QWP driver owns its own I/O thread
        // and `flush` / `wait` block. Row building is cheap but is done here
        // too, so the whole batch crosses the boundary exactly once.
        let handle = tokio::task::spawn_blocking(
            move || -> Result<BatchOutcome, Box<ChunkFailure>> {
                let db: &QuestDb = &db;
                let context = RowContext {
                    stream: &stream,
                    topic: &topic,
                    partition_id,
                };
                let mut outcome = BatchOutcome::default();
                // The windows published so far. Only the final `wait` asks for
                // an acknowledgement, so a window counts as confirmed when the
                // acknowledged watermark covers its frame, and when the chunk
                // stops everything past the last such window is unconfirmed.
                let mut windows = ChunkWindows::new(ack_level, flush_timeout, mode, already_logged);
                // Index of the first message in the window being buffered, so
                // a window that leaves unconfirmed can be named by offset.
                let mut window_start = 0usize;

                let mut sender = match db.borrow_sender() {
                    Ok(sender) => sender,
                    Err(error) => {
                        let failure = StopCause::Flush(FlushError {
                            phase: FlushPhase::Borrowing,
                            error,
                        });
                        return Err(chunk_stop(failure, outcome, (0, 0), 0, &messages, mode));
                    }
                };
                let mut buffer = sender.new_buffer();
                let mut scratch = RowScratch::default();
                let rows = ChunkRows {
                    mapping: &mapping,
                    context,
                    messages: &messages,
                };
                for (index, message) in messages.iter().enumerate() {
                    // Publish before the buffer can outgrow the QWP per-frame
                    // cap. `batch_size` bounds rows, not bytes, so a batch of
                    // wide rows would otherwise be rejected whole and lose
                    // every row in it. A row that takes the buffer to
                    // `max_flush_bytes` is rolled back to the marker, the rows
                    // in front of it are published, and the row starts the
                    // next window. A message wider than a frame then fails in a
                    // window of its own, which names it by offset and costs no
                    // other row. The window is only published here: waiting
                    // for each one would cost a server round trip per window,
                    // while the final `wait` covers every frame of the lease.
                    // The size read takes an uncontended lock and sums cheap
                    // counters over the columns of the table, so its cost
                    // grows with the column count, not with the rows buffered.
                    // A marker that is not rewound stays set: the next one
                    // replaces it and reuses its storage, while clearing it
                    // would free that storage and cost an allocation per row.
                    // A window starts at its first written row, so that a
                    // window of one row names that row's message.
                    if buffer.row_count() == 0 {
                        window_start = index;
                    }
                    let marked = buffer.row_count() > 0 && buffer.set_marker().is_ok();
                    let mut appended =
                        mapping.append_row_with(&mut buffer, message, context, &mut scratch);
                    if marked
                        && appended.is_ok()
                        && buffer.len() >= max_flush_bytes
                        && buffer.rewind_to_marker().is_ok()
                    {
                        windows.publish_window(
                            &mut sender,
                            &mut buffer,
                            window_start..index,
                            &mut outcome,
                            &rows,
                            &mut scratch,
                        )?;
                        window_start = index;
                        appended =
                            mapping.append_row_with(&mut buffer, message, context, &mut scratch);
                    }

                    let reason = match appended {
                        Ok(()) => {
                            outcome.rows_written += 1;
                            continue;
                        }
                        Err(RowError::Invalid(reason)) => reason,
                        // The record reached the buffer and was refused there, by
                        // a rule only the buffer knows: a column's type is pinned by
                        // whichever row defined it first, so a later row disagreeing
                        // with it cannot be judged from that row alone. The client
                        // rolls the half-written row back before returning the error,
                        // on every column and symbol setter, so the buffer is still
                        // usable and only this record is lost. At least once, the
                        // record stops the delivery like any other rejection. A
                        // transient error would not help: the runtime delivers the
                        // batch again from its first message, so the row that
                        // pinned the column usually shares the chunk again, and
                        // the retries would repeat the conflict until they ran out.
                        Err(RowError::Client(error)) => error.to_string(),
                        // `Buffer::at` refused the row. For a negative timestamp
                        // it skips the rollback, so the buffer can hold a partial
                        // row that only the marker can remove.
                        // `Mapping::prepare_timestamp` rejects every negative
                        // value, which is what keeps this unreachable.
                        Err(RowError::Unrecoverable(error)) => {
                            // The marker taken before the row removes the
                            // partial row and keeps the rows in front of it.
                            // Without one, publishing the buffer would risk
                            // carrying the partial row along, so the whole
                            // window goes. The rows already counted as written
                            // are uncounted again rather than reported as
                            // delivered.
                            if !(marked && buffer.rewind_to_marker().is_ok()) {
                                let discarded = buffer.row_count();
                                if let window @ Some(_) =
                                    MessageRange::of(&messages[window_start..index])
                                {
                                    error!(
                                        "{CONNECTOR_NAME} ID: {id} discarded {discarded} unflushed rows after the client refused a designated timestamp, stream: {stream}, topic: {topic}, partition_id: {partition_id}, {}: {error}",
                                        WindowLabel(window)
                                    );
                                }
                                outcome.rows_written -= discarded;
                                outcome.rejected_rows += discarded;
                                buffer = sender.new_buffer();
                            }
                            error.to_string()
                        }
                    };

                    windows.note_rejection(&mut outcome, context, message, &reason);
                    if mode.at_least_once {
                        if buffer.row_count() > 0 {
                            windows.publish_window(
                                &mut sender,
                                &mut buffer,
                                window_start..index,
                                &mut outcome,
                                &rows,
                                &mut scratch,
                            )?;
                        }
                        return Err(windows.reject(
                            &mut sender,
                            outcome,
                            context,
                            &messages,
                            index,
                            reason,
                        ));
                    }
                    outcome.last_rejection = Some(reason);
                    windows.skipped.push(index);
                }

                mapping.note_header_keys(&mut scratch);
                if buffer.row_count() > 0 {
                    windows.publish_window(
                        &mut sender,
                        &mut buffer,
                        window_start..messages.len(),
                        &mut outcome,
                        &rows,
                        &mut scratch,
                    )?;
                }
                if let Err(failure) =
                    windows.await_all(&mut sender, &mut outcome, context, &messages)
                {
                    return Err(windows.stop(failure, outcome, messages.len(), &messages));
                }
                Ok(outcome)
            },
        );

        match handle.await {
            Ok(result) => result,
            // The blocking task panicked, so nothing about the chunk is known.
            // Every message is reported as undelivered and the connection as
            // suspect, which is the conservative reading of both unknowns.
            Err(error) => Err(Box::new(ChunkFailure {
                error: Error::CannotStoreData(format!(
                    "{CONNECTOR_NAME} ID: {}: blocking flush task failed: {error}",
                    mode.id
                )),
                stops_batch: true,
                server_rejection: false,
                undelivered: whole_chunk,
                attempted: 0,
                outcome: BatchOutcome::default(),
            })),
        }
    }

    /// A rejection the handler reported that no failed chunk accounted for,
    /// or a sign that one went unreported, for the batch-level check.
    ///
    /// This reports `PermanentHttpError` even for a schema rejection, where
    /// `map_client_error` reports `SchemaMismatch`, because the balance is a
    /// count with no category attached. The runtime branches on neither
    /// variant, and the handler's own log line carries the category.
    ///
    /// At least once, every chunk waits, so a terminal rejection fails its own
    /// chunk, and an unclaimed count is only a credit that landed before its
    /// debit, an event that latched no connection, or the rejection of a frame
    /// that a timed-out chunk left queued on a connection that then latched
    /// idle in the pool, which the next borrow retires. The error is transient
    /// then: a permanent one would stop the delivery, and the runtime's retry
    /// of this batch costs only a second copy of rows that QuestDB already
    /// took, which DEDUP keys on the message identity remove.
    fn unclaimed_rejection(&self, db: &QuestDb) -> Option<Error> {
        let message = match settle_rejections(
            &self.server_rejections,
            &self.rejection_drops_seen,
            db.rejection_events_dropped(),
        )? {
            Unclaimed::Rejection => format!(
                "{CONNECTOR_NAME} ID: {}: a QuestDB connection of this connector ended on a terminal error; the log line of that error carries the category and the frame range",
                self.id
            ),
            Unclaimed::EventsDropped(dropped) => format!(
                "{CONNECTOR_NAME} ID: {}: the client's rejection inbox dropped events, {dropped} so far, so a terminal rejection may be unreported; this batch's rows were written",
                self.id
            ),
        };
        Some(if self.at_least_once {
            Error::CannotStoreData(message)
        } else {
            Error::PermanentHttpError(message)
        })
    }

    /// Names the messages a chunk failure left undelivered.
    ///
    /// At most once, their offsets are already committed and the runtime does
    /// not replay a failed batch, so this line is the only record that they
    /// existed. At least once, the runtime delivers them again, and the line
    /// says why. Either way it is the only place every failed chunk's reason is
    /// logged: `consume` returns one error for the batch, and the runtime logs
    /// that one.
    /// `what` says which kind: the failed chunk's own messages from its last
    /// acknowledged window onward, or the chunks behind it that the loop did
    /// not attempt because the connection was at fault.
    fn log_undelivered(
        &self,
        topic_metadata: &TopicMetadata,
        partition_id: u32,
        what: &str,
        range: Option<MessageRange>,
        reason: &Error,
    ) {
        let Some(range) = range else {
            return;
        };
        error!(
            "{CONNECTOR_NAME} ID: {} {what}: {} messages, stream: {}, topic: {}, partition_id: {partition_id}, {}, reason: {reason}",
            self.id,
            range.count,
            topic_metadata.stream,
            topic_metadata.topic,
            WindowLabel(Some(range))
        );
    }

    /// Validates the connect string and connects the pool. `open` logs the
    /// error this returns.
    async fn open_connector(&mut self) -> Result<(), Error> {
        if let Some(error) = self.init_error.take() {
            return Err(error);
        }
        let connection_string = self.connection_string.expose_secret().to_owned();
        let uses_tls = connection_string
            .trim_start()
            .to_ascii_lowercase()
            .starts_with("wss:");
        if uses_tls {
            ensure_crypto_provider();
        }
        // A connect string that disables verification is a test convenience, and
        // the client only honours it when this crate was built with the
        // `insecure-skip-verify` feature. Say which of the two applies rather
        // than letting a production deployment carry it silently, or letting a
        // default build fail with the client's generic configuration error.
        if connection_string
            .to_ascii_lowercase()
            .contains("tls_verify=unsafe_off")
        {
            if cfg!(feature = "insecure-skip-verify") {
                warn!(
                    "{CONNECTOR_NAME} ID: {} has TLS certificate verification disabled through tls_verify=unsafe_off; use this only against a test server",
                    self.id
                );
            } else {
                return Err(Error::InvalidConfigValue(
                    "the connection string sets tls_verify=unsafe_off, which this build of \
                     the connector does not accept; rebuild it with the \
                     insecure-skip-verify feature if a test server with a self-signed \
                     certificate needs it"
                        .to_owned(),
                ));
            }
        }
        // Durable acks are refused outright unless the connect string asks the
        // server for them, so a config that sets one without the other fails
        // every batch while the rows themselves land. Catch it here rather than
        // letting the operator read that as data loss.
        // With `local,replicated` the client trims on the replicated
        // acknowledgement and refuses a local wait, so each level needs its own
        // values.
        let requested = self.requested_ack_level;
        if self.ack_level != AckLevelSetting::Ok && requested != Some(self.ack_level) {
            // Read again only for the message, on this path that fails the
            // start.
            let durable_ack_request = request_durable_ack(&connection_string);
            let found = match (durable_ack_request.as_deref(), requested) {
                (Some(value), Some(level)) => format!(
                    "sets request_durable_ack={value}, which gives ack_level \"{}\"",
                    level
                ),
                (Some(value), None) => format!(
                    "sets request_durable_ack={value}, which the QuestDB client does not accept"
                ),
                // A key that the sink cannot find, although the string holds
                // it, is a key that the client cannot parse either, such as one
                // after a space.
                (None, _)
                    if connection_string
                        .to_ascii_lowercase()
                        .contains("request_durable_ack") =>
                {
                    "does not set request_durable_ack in a form that the QuestDB client \
                     reads, which is the key in lowercase, right after `::` or a `;`, \
                     with no space around it"
                        .to_owned()
                }
                (None, _) => "does not set request_durable_ack".to_owned(),
            };
            let needed = if self.ack_level == AckLevelSetting::LocalDurable {
                "local"
            } else {
                "on, replicated or local,replicated"
            };
            return Err(Error::InvalidConfigValue(format!(
                "ack_level is \"{}\" but the connection string {found}. The QuestDB client \
                 needs request_durable_ack set to {needed} for that ack level",
                self.ack_level
            )));
        }

        let id = self.id;
        // The handler is how the sink learns of a terminal rejection that no
        // `wait` reported, so that a batch can fail for it. It also logs each
        // rejection with the connector ID, the status and the frame range. The
        // `questdb::Error` the sink sees carries none of that detail, and the
        // client's own log lines do not name the connector.
        let mut handlers = ConnectHandlers::default();
        let rejections = Arc::clone(&self.server_rejections);
        handlers.error_handler = Some(QwpWsErrorHandler::new(move |error| {
            report_server_rejection(id, &rejections, error)
        }));

        // `QuestDb::connect_with_handlers` dials the server, so this doubles as
        // the connectivity check the sink contract asks for in `open`.
        let db = tokio::task::spawn_blocking(move || {
            QuestDb::connect_with_handlers(&connection_string, handlers)
        })
        .await
        .map_err(|error| Error::InitError(format!("connect task failed: {error}")))?
        .map_err(|error| Error::InitError(format!("cannot connect to QuestDB: {}", error)))?;
        self.db = Some(Arc::new(db));
        info!(
            "Opened {CONNECTOR_NAME} connector ID: {}, table: {}, ack_level: {}, numbers_as_double: {}, symbol_columns: {:?}, uuid_columns: {:?}, integer_columns: {:?}",
            self.id,
            self.mapping.table,
            self.ack_level,
            self.mapping.numbers_as_double,
            self.mapping.symbol_columns.iter().collect::<Vec<_>>(),
            self.mapping.uuid_columns.iter().collect::<Vec<_>>(),
            self.mapping.integer_columns.iter().collect::<Vec<_>>()
        );
        if self.ack_level == AckLevelSetting::Durable {
            // A node without WAL shipping accepts the rows and simply never
            // advances the durable watermark, so the failure shows up as
            // acknowledgement lag rather than a connect error.
            warn!(
                "{CONNECTOR_NAME} ID: {} requests durable acks; this needs QuestDB Enterprise with \
                 replication configured, otherwise no chunk is ever acknowledged within {:?}",
                self.id, self.flush_timeout
            );
        }
        Ok(())
    }
}

#[async_trait]
impl Sink for QuestDbSink {
    async fn open(&mut self) -> Result<(), Error> {
        // The SDK drops the error that `open` returns, and the runtime logs only
        // "Plugin initialization failed", so this line is the only record of
        // why the connector did not start.
        let result = self.open_connector().await;
        if let Err(error) = &result {
            error!("{CONNECTOR_NAME} ID: {} failed to open: {error}", self.id);
        }
        result
    }

    async fn consume(
        &self,
        topic_metadata: &TopicMetadata,
        messages_metadata: MessagesMetadata,
        messages: Vec<ConsumedMessage>,
    ) -> Result<(), Error> {
        let Some(db) = self.db.as_ref() else {
            return Err(Error::InitError(format!(
                "{CONNECTOR_NAME} ID: {}: the QuestDB pool is not initialized",
                self.id
            )));
        };
        if messages.is_empty() {
            return Ok(());
        }

        let received = messages.len();
        if self.verbose {
            info!(
                "{CONNECTOR_NAME} ID: {} consuming {received} messages, stream: {}, topic: {}, partition_id: {}, current_offset: {}",
                self.id,
                topic_metadata.stream,
                topic_metadata.topic,
                messages_metadata.partition_id,
                messages_metadata.current_offset
            );
        } else {
            debug!(
                "{CONNECTOR_NAME} ID: {} consuming {received} messages, current_offset: {}",
                self.id, messages_metadata.current_offset
            );
        }

        let mut total = BatchOutcome::default();
        let mut failure: Option<Error> = None;
        // Messages the chunk loop appended or individually rejected, which is
        // what `messages_processed` means. A chunk that stopped early and the
        // chunks behind a connection failure were never processed.
        let mut attempted = 0usize;
        let mut mode = ChunkMode {
            id: self.id,
            at_least_once: self.at_least_once,
            wait: true,
            log_payload: self.log_rejected_payload,
        };
        let mut messages = messages.into_iter();
        loop {
            // The last chunk takes the iterator itself. Collecting an owned
            // `vec::IntoIter` reuses its allocation, so a batch no larger than
            // `batch_size`, the common case, is not copied into a second vector.
            let chunk: Vec<ConsumedMessage> = if messages.len() <= self.batch_size {
                std::mem::take(&mut messages).collect()
            } else {
                messages.by_ref().take(self.batch_size).collect()
            };
            if chunk.is_empty() {
                break;
            }
            let chunk_len = chunk.len();
            match self
                .write_batch(
                    Arc::clone(db),
                    chunk,
                    topic_metadata,
                    messages_metadata.partition_id,
                    total.rejected_rows,
                    mode,
                )
                .await
            {
                Ok(outcome) => {
                    // At least once, a pending acknowledgement fails the chunk,
                    // so only an at-most-once chunk gets here with one.
                    mode.wait &= outcome.pending_acks == 0;
                    total.merge(outcome);
                    attempted += chunk_len;
                }
                Err(chunk_failure) => {
                    let ChunkFailure {
                        error,
                        stops_batch,
                        server_rejection,
                        undelivered,
                        attempted: chunk_attempted,
                        outcome,
                    } = *chunk_failure;
                    // The chunk's confirmed rows and its individual rejections
                    // count, so the summary and the log cap see them.
                    total.merge(outcome);
                    attempted += chunk_attempted;
                    // The chunk reported its server rejection through its own
                    // error, and the handler counts the same event on the
                    // client's thread. Retire it now, so that a concurrent
                    // batch of another topic cannot claim it as unreported.
                    // Only these are retired, so a transport failure never
                    // retires a rejection that it had nothing to do with. The
                    // handler's count can land after this point: the balance
                    // then dips below zero and nets to zero when it does.
                    if server_rejection {
                        self.server_rejections.fetch_sub(1, Ordering::Relaxed);
                    }
                    // At least once, the runtime delivers the whole batch
                    // again, or stops at a permanent error, so the line says
                    // which follows. A record that the sink rejected never
                    // reached QuestDB, so its line says that instead.
                    let what = match (self.at_least_once, &error) {
                        (false, _) => "QuestDB did not confirm writing",
                        (true, Error::InvalidRecordValue(_)) => {
                            "the sink rejected a record, and the runtime stops the delivery at this batch"
                        }
                        (true, error) if error.is_permanent() => {
                            "QuestDB did not confirm writing, and the runtime stops the delivery at this batch"
                        }
                        (true, _) => {
                            "QuestDB did not confirm writing, and the runtime delivers the batch again"
                        }
                    };
                    self.log_undelivered(
                        topic_metadata,
                        messages_metadata.partition_id,
                        what,
                        undelivered,
                        &error,
                    );
                    // At least once, the chunks behind this one would only be
                    // written twice, so the loop ends. At most once, ending it
                    // would discard every chunk still queued behind this one,
                    // at offsets the runtime committed before `consume` ran.
                    // Only a failure that belongs to the connection ends it
                    // then, because the chunks behind it would fail the same
                    // way and each would spend its own timeout finding out.
                    if stops_batch && !self.at_least_once {
                        self.log_undelivered(
                            topic_metadata,
                            messages_metadata.partition_id,
                            "abandoned without a write attempt after a connection failure",
                            MessageRange::of(messages.as_slice()),
                            &error,
                        );
                    }
                    failure.get_or_insert(error);
                    if self.at_least_once || stops_batch {
                        break;
                    }
                }
            }
        }

        if total.rejected_rows > 0 {
            warn!(
                "{CONNECTOR_NAME} ID: {} rejected {} of {received} messages, last reason: {}",
                self.id,
                total.rejected_rows,
                total.last_rejection.as_deref().unwrap_or("unknown")
            );
        }
        if total.rejected_rows > MAX_LOGGED_REJECTIONS_PER_BATCH {
            error!(
                "{CONNECTOR_NAME} ID: {} logged {} of those rejections at debug level only, to keep the error log bounded",
                self.id,
                total.rejected_rows - MAX_LOGGED_REJECTIONS_PER_BATCH
            );
        }

        let mut state = self.state.lock().await;
        state.messages_processed += attempted as u64;
        state.rows_written += total.rows_written as u64;
        state.rejected_rows += total.rejected_rows as u64;
        state.pending_acks += total.pending_acks as u64;
        // Keyed by stream and topic, because the runtime runs one `consume` per
        // topic against this one instance. A shared counter would let a healthy
        // topic reset the run belonging to a stalled one, and the escalation
        // would never fire. `partition_id` is not part of the key: it varies
        // between batches inside one task, which would reset the run just as
        // wrongly.
        let stalled = if total.pending_acks > 0 {
            let run = state
                .consecutive_pending_ack_batches
                .entry((topic_metadata.stream.clone(), topic_metadata.topic.clone()))
                .or_insert(0);
            *run += 1;
            *run
        } else {
            // A healthy batch copies no names: the map holds one entry per
            // stalled topic, so the scan is short and usually empty.
            state
                .consecutive_pending_ack_batches
                .retain(|(stream, topic), _| {
                    stream != &topic_metadata.stream || topic != &topic_metadata.topic
                });
            0
        };
        drop(state);

        if let Some(error) = failure.or(total.window_failure) {
            return Err(error);
        }
        // A terminal rejection reported through the handler has to fail a batch.
        // The handler runs on the client's thread and cannot know which batch
        // caused it, so this counts them and takes one per batch. A flag would
        // collapse several rejections into one failure when batches for
        // different topics run at the same time. The count stays per instance
        // rather than per topic, because the handler sees a connection and a
        // frame range and has no way to name the topic that filled it; a
        // rejection raised with no flush error therefore fails whichever
        // topic's batch observes it next.
        if let Some(error) = self.unclaimed_rejection(db) {
            return Err(error);
        }
        // One unacknowledged chunk is lag. A run of them means the server has
        // stopped acknowledging altogether, and reporting every one of those
        // batches as a success would leave the runtime's metrics showing a
        // healthy connector while nothing is being committed. Escalate so the
        // error counter moves; the rows stay queued either way, and at most
        // once the runtime does not replay a failed batch, so nothing is lost
        // by saying so. The error is transient, because the frames are still
        // queued and the stall ends when the server acknowledges again. At
        // least once, a pending acknowledgement already fails its own batch,
        // so no run starts.
        if should_escalate_stall(stalled) {
            return Err(Error::CannotStoreData(format!(
                "{CONNECTOR_NAME} ID: {}: the last {stalled} batches left chunks that QuestDB did not acknowledge at ack_level {}; the frames are queued but nothing is being committed",
                self.id, self.ack_level
            )));
        }
        Ok(())
    }

    /// Writes happen inside `consume`, so the default flush holds: a
    /// successful call already waited for QuestDB to acknowledge each chunk.
    /// Fire and forget never waits, so it cannot keep that promise.
    fn enable_at_least_once(&mut self) -> Result<(), Error> {
        if self.flush_timeout.is_zero() {
            return Err(Error::Unsupported(format!(
                "{CONNECTOR_NAME} ID: {}: flush_timeout = \"0s\" never waits for QuestDB to acknowledge a write, so it cannot deliver at least once; set flush_timeout above zero",
                self.id
            )));
        }
        // A retried batch still lands once per attempt, but with a clock as
        // its source each copy has a new designated timestamp, and DEDUP keys
        // must include that column, so they cannot remove the copies. The
        // delivery is still at least once, so this warns instead of refusing.
        match self.mapping.timestamp_source {
            TimestampSource::Server => warn!(
                "{CONNECTOR_NAME} ID: {} delivers at least once with timestamp_source = \"server\": QuestDB stamps each retried copy anew, so DEDUP UPSERT KEYS cannot remove the duplicates; use the message timestamp to make retries idempotent",
                self.id
            ),
            TimestampSource::Origin => warn!(
                "{CONNECTOR_NAME} ID: {} delivers at least once with timestamp_source = \"origin\": a message without an origin timestamp takes the connector's clock, so DEDUP UPSERT KEYS cannot remove its retried copies",
                self.id
            ),
            TimestampSource::Message | TimestampSource::Payload => {}
        }
        self.at_least_once = true;
        Ok(())
    }

    async fn close(&mut self) -> Result<(), Error> {
        if let Some(db) = self.db.take() {
            // Dropping the pool drains what it still holds, bounded by the
            // connect string's `close_flush_timeout_millis`, and discards the
            // rest.
            //
            // There is no useful wait to add here. `borrow_sender` rebases the
            // new lease onto the connection's current published frame number,
            // so a wait on it returns at once and proves nothing about the
            // frames earlier batches left queued. The drop is the drain:
            // `Drop for QuestDb` walks every idle store-and-forward connection
            // under one shared `close_flush_timeout_millis` deadline. What it cannot
            // deliver, it reports only in a warning of the client, which names
            // neither the connector nor the offsets. Set `sf_dir` in the
            // connect string for a tail that survives both the drain window
            // and a restart; the README says so under Store-and-forward.
            if let Err(error) = tokio::task::spawn_blocking(move || drop(db)).await {
                warn!(
                    "{CONNECTOR_NAME} ID: {} close task failed: {error}",
                    self.id
                );
            }
        }
        let state = self.state.lock().await;
        // `rows buffered` rather than `rows written`: the count holds the rows
        // that chunks published, so it leads what the server has accepted. A
        // failed chunk adds only the rows that QuestDB confirmed, and a lost
        // window adds none.
        info!(
            "Closed {CONNECTOR_NAME} connector ID: {}, processed: {}, rows buffered: {}, rejected: {}, chunks awaiting ack: {}",
            self.id,
            state.messages_processed,
            state.rows_written,
            state.rejected_rows,
            state.pending_acks
        );
        Ok(())
    }
}

#[derive(Debug, Default)]
struct BatchOutcome {
    rows_written: usize,
    rejected_rows: usize,
    /// Chunks whose acknowledgement had not arrived before the timeout. The
    /// rows stay queued and are still being delivered, so this is lag, not
    /// loss. The queue survives a process crash only when `sf_dir` is set.
    pending_acks: usize,
    last_rejection: Option<String>,
    /// The first window an at-most-once chunk lost to its own publish failure.
    /// The chunk went on without it, and the batch fails once it is written.
    window_failure: Option<Error>,
}

impl BatchOutcome {
    fn merge(&mut self, other: BatchOutcome) {
        self.rows_written += other.rows_written;
        self.rejected_rows += other.rejected_rows;
        self.pending_acks += other.pending_acks;
        if other.last_rejection.is_some() {
            self.last_rejection = other.last_rejection;
        }
        if let Some(error) = other.window_failure {
            self.window_failure.get_or_insert(error);
        }
    }
}

/// Renders a rejected payload for the log, truncated on a char boundary so a
/// large or binary message cannot blow up the log line.
fn payload_preview(payload: &Payload) -> String {
    match payload {
        // Rendered through a writer that keeps only the preview's worth of
        // bytes, so a large rejected document is never held as one string.
        Payload::Json(value) => {
            let mut capped = CappedWriter::default();
            if simd_json::to_writer(&mut capped, value).is_err() {
                return "<unrenderable>".to_owned();
            }
            capped.into_preview()
        }
        Payload::Text(text) | Payload::Proto(text) => truncate_preview(text, text.len()),
        Payload::Raw(bytes) | Payload::FlatBuffer(bytes) | Payload::Avro(bytes) => {
            format!("<{} bytes>", bytes.len())
        }
    }
}

/// Cuts `head` to the preview limit on a char boundary. `total` is the full
/// rendered length, which `head` may already have been cut short of.
fn truncate_preview(head: &str, total: usize) -> String {
    if total <= REJECTED_PAYLOAD_PREVIEW_BYTES {
        return head.to_owned();
    }
    let end = head.floor_char_boundary(REJECTED_PAYLOAD_PREVIEW_BYTES);
    format!("{}… ({total} bytes total)", &head[..end])
}

/// Keeps the first preview's worth of what is written to it and counts the
/// rest.
#[derive(Default)]
struct CappedWriter {
    head: Vec<u8>,
    total: usize,
}

impl CappedWriter {
    fn into_preview(self) -> String {
        // The cap can split a multi-byte character, so only the valid prefix
        // is kept.
        let head = self
            .head
            .utf8_chunks()
            .next()
            .map_or("", |chunk| chunk.valid());
        truncate_preview(head, self.total)
    }
}

impl std::io::Write for CappedWriter {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        let room = REJECTED_PAYLOAD_PREVIEW_BYTES.saturating_sub(self.head.len());
        self.head.extend_from_slice(&bytes[..room.min(bytes.len())]);
        self.total += bytes.len();
        Ok(bytes.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

/// The configured size, or the default when it is unset. Zero is refused.
fn size_or_default(field: &str, value: Option<usize>, default: usize) -> Result<usize, Error> {
    match value {
        None => Ok(default),
        Some(0) => Err(Error::InvalidConfigValue(format!(
            "{field} must be greater than zero"
        ))),
        Some(value) => Ok(value),
    }
}

/// The ack level that a `request_durable_ack` value selects, or `None` for a
/// value that the client refuses. The client trims on the replicated
/// acknowledgement whenever the set holds `replicated`, or its original name
/// `on`, so `local,replicated` selects `Durable`, and `local` alone selects
/// `LocalDurable`.
fn ack_level_for(value: &str) -> Option<AckLevelSetting> {
    match value.trim().to_ascii_lowercase().as_str() {
        "on" | "replicated" | "local,replicated" | "replicated,local" => {
            Some(AckLevelSetting::Durable)
        }
        "local" => Some(AckLevelSetting::LocalDurable),
        "off" => Some(AckLevelSetting::Ok),
        _ => None,
    }
}

/// The value of `request_durable_ack`, read with the grammar of the client:
/// the keys follow `<service>::`, and `;;` in a value is a literal `;`.
/// `questdb_confstr::parse_conf_str` refuses a repeated key, but the client
/// accepts a repeated `addr` for failover, so it cannot read the raw string.
fn request_durable_ack(connection_string: &str) -> Option<String> {
    let (_, mut rest) = connection_string.split_once("::")?;
    while !rest.is_empty() {
        let (key, after_key) = rest.split_once('=')?;
        let end = value_end(after_key);
        if key == "request_durable_ack" {
            // Every `;` before `end` is half of a `;;` pair.
            return Some(after_key[..end].replace(";;", ";"));
        }
        rest = after_key.get(end + 1..).unwrap_or_default();
    }
    None
}

/// The length of the value at the start of `text`: up to the first `;` that
/// does not start a `;;` pair, as the client reads it.
fn value_end(text: &str) -> usize {
    let bytes = text.as_bytes();
    let mut index = 0;
    while index < bytes.len() {
        if bytes[index] == b';' {
            if bytes.get(index + 1) == Some(&b';') {
                index += 2;
                continue;
            }
            return index;
        }
        index += 1;
    }
    bytes.len()
}

/// The `ack_level` setting. The client's `AckLevel` has no names, so this
/// carries the configuration spelling for parsing and for log lines.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum AckLevelSetting {
    Ok,
    Durable,
    LocalDurable,
}

/// Matched without regard to case, as the timestamp settings are.
impl FromStr for AckLevelSetting {
    type Err = UnknownVariant;

    fn from_str(raw: &str) -> Result<Self, Self::Err> {
        match raw.to_ascii_lowercase().as_str() {
            "ok" => Ok(Self::Ok),
            "durable" => Ok(Self::Durable),
            "local_durable" => Ok(Self::LocalDurable),
            _ => Err(UnknownVariant),
        }
    }
}

impl fmt::Display for AckLevelSetting {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::Ok => "ok",
            Self::Durable => "durable",
            Self::LocalDurable => "local_durable",
        })
    }
}

impl From<AckLevelSetting> for AckLevel {
    fn from(setting: AckLevelSetting) -> Self {
        match setting {
            AckLevelSetting::Ok => Self::Ok,
            AckLevelSetting::Durable => Self::Durable,
            AckLevelSetting::LocalDurable => Self::LocalDurable,
        }
    }
}

/// A column has one type. It cannot be two of `SYMBOL`, `UUID` and `LONG`, and
/// none of them can double as the designated timestamp. Catching it here beats a
/// per-row failure later.
fn validate_column_overlap(mapping: &Mapping) -> Result<(), Error> {
    let lists = [
        ("symbol_columns", &mapping.symbol_columns),
        ("uuid_columns", &mapping.uuid_columns),
        ("integer_columns", &mapping.integer_columns),
    ];
    for (index, (name, columns)) in lists.iter().enumerate() {
        for (other_name, other) in &lists[index + 1..] {
            if let Some(column) = columns.iter().find(|column| other.contains(column)) {
                return Err(Error::InvalidConfigValue(format!(
                    "column {column} is listed in both {name} and {other_name}"
                )));
            }
        }
    }
    if let Some(field) = mapping.timestamp_field.as_deref()
        && let Some((name, _)) = lists.iter().find(|(_, columns)| columns.contains(field))
    {
        return Err(Error::InvalidConfigValue(format!(
            "timestamp_field {field} cannot also be listed in {name}"
        )));
    }
    // The connector writes the enabled metadata columns itself, and a payload
    // field of the same name is rejected per record. A configured column of
    // that name would therefore reject every record that carries it, which is
    // a configuration mistake rather than a data one.
    for (reserved, flag) in mapping.reserved_columns_with_flags() {
        if let Some((name, _)) = lists.iter().find(|(_, columns)| columns.contains(reserved)) {
            return Err(Error::InvalidConfigValue(format!(
                "column {reserved} is listed in {name} but the connector writes a metadata column of that name; set {flag} = false or rename the field"
            )));
        }
    }
    Ok(())
}

/// Which half of the flush failed.
///
/// `flush_buffer_and_get_fsn` either appends the frame to the local
/// publication log or it does not, so a failure there can be safe to re-send.
/// By the time `wait` runs the frame is already published and the rows may
/// already be committed, so at most once a failure there is never re-sent. A
/// durable-ACK stall against a server without replication configured is
/// exactly this case: the rows land, only the watermark never advances. At
/// least once, [`map_client_error`] makes a retryable failure there transient,
/// so the runtime delivers the batch again, and the DEDUP keys remove the
/// copies.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum FlushPhase {
    /// `borrow_sender` failed, before any row was built. The pool was
    /// exhausted, or the dial it makes for a new connection was refused.
    Borrowing,
    Publishing,
    Awaiting,
}

#[derive(Debug)]
struct FlushError {
    phase: FlushPhase,
    error: questdb::Error,
}

/// The first and last message of a range, for the log lines that name what a
/// failure cost. Two reads of the slice, so the success path copies nothing.
#[derive(Debug, Clone, Copy)]
struct MessageRange {
    count: usize,
    first_offset: u64,
    last_offset: u64,
    first_id: u128,
    last_id: u128,
}

impl MessageRange {
    fn of(messages: &[ConsumedMessage]) -> Option<Self> {
        let (first, last) = (messages.first()?, messages.last()?);
        Some(Self {
            count: messages.len(),
            first_offset: first.offset,
            last_offset: last.offset,
            first_id: first.id,
            last_id: last.id,
        })
    }
}

/// The offsets and ids of a run of messages, formatted only when a log line
/// that names them is actually written.
struct WindowLabel(Option<MessageRange>);

impl fmt::Display for WindowLabel {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self.0 {
            Some(range) => write!(
                formatter,
                "offsets: [{}, {}], message_ids: [{}, {}]",
                range.first_offset, range.last_offset, range.first_id, range.last_id
            ),
            None => formatter.write_str("offsets: unknown"),
        }
    }
}

/// One window that a chunk published: its frame sequence number, the index
/// after its last message, and the chunk's written rows at that point.
#[derive(Debug, Clone, Copy)]
struct PublishedWindow {
    fsn: u64,
    end: usize,
    rows: usize,
}

/// What the rows of a chunk are built from, so that a window that is too
/// large for a frame can be built again in parts.
struct ChunkRows<'a> {
    mapping: &'a Mapping,
    context: RowContext<'a>,
    messages: &'a [ConsumedMessage],
}

/// How the chunks of one `consume` call are written.
#[derive(Debug, Clone, Copy)]
struct ChunkMode {
    id: u32,
    /// The runtime delivers at least once. A record that the sink cannot
    /// write then fails the call instead of being skipped, and a chunk counts
    /// as written only once QuestDB acknowledged it.
    at_least_once: bool,
    /// Wait for the acknowledgement. An at-most-once batch stops waiting after
    /// its first chunk whose acknowledgement did not arrive, because each chunk
    /// behind it would wait the whole `flush_timeout` on the same stalled
    /// watermark.
    wait: bool,
    /// `log_rejected_payload`: add a truncated payload to each line that
    /// names a rejected record.
    log_payload: bool,
}

/// Why a chunk stopped.
enum StopCause {
    Flush(FlushError),
    /// At least once, QuestDB terminally rejected only frames that an earlier
    /// lease left queued on the borrowed connection, so the rows of this
    /// chunk are not at fault. See [`ChunkWindows::cause`].
    Inherited(FlushError),
    /// At least once, the sink refused a record. The call fails at it, so
    /// that the runtime stops the delivery there instead of losing it.
    Rejected(String),
}

/// Classifies a stopped chunk for `consume`.
///
/// `confirmed` is the index after the last window that an acknowledged
/// watermark the sink read covers, with the written rows at that point.
/// `undelivered` runs from there to the end of the chunk: everything there
/// was in a window whose acknowledgement never came, in the buffer the flush
/// refused, or never appended. A connection that latched keeps the last
/// watermark read before the latch, so the windows that it covers stay
/// confirmed. `attempted` is the index at which the loop stopped appending,
/// so the messages before it were written or individually rejected and logged.
///
/// Three things are decided here and nowhere else. Whether the error is
/// reported as transient or permanent is [`map_client_error`]. Whether the
/// batch stops is a different question, and only at most once: at least
/// once, `consume` ends the loop on every failure, because the runtime
/// delivers the whole batch again. At most once, a
/// failure that belongs to the connection, which is every borrow failure and
/// every transient code, would recur on the next chunk and cost it a timeout,
/// so the loop ends. A full symbol dictionary is the one transient fault that
/// does not recur, because returning the sender retires that connection and
/// the next borrow dials a fresh one, so it does not stop the loop. Under
/// `sf_dir` the next connection re-seeds the dictionary from the slot until
/// the slot drains, so each chunk can fail once more. It still does not stop
/// the loop, because each later chunk can succeed. A failure that belongs to
/// the frame, such as a server rejection, does not stop the chunks behind it
/// either. Whether a token has to be retired is the third: the handler counts
/// a terminal rejection on its own thread, so a chunk that failed on one has
/// to retire that count.
fn chunk_stop(
    cause: StopCause,
    mut outcome: BatchOutcome,
    (confirmed_to, confirmed_rows): (usize, usize),
    attempted: usize,
    messages: &[ConsumedMessage],
    mode: ChunkMode,
) -> Box<ChunkFailure> {
    // `rows_written` grew at append time, so the rows of every unconfirmed
    // window are in it and come out again here.
    outcome.rows_written = confirmed_rows;
    let (error, stops_batch, server_rejection) = match cause {
        // The runtime names the first offset of the run in its stop reason,
        // so the error names the record that stopped it.
        StopCause::Rejected(reason) => (
            Error::InvalidRecordValue(match messages.get(attempted) {
                Some(message) => format!(
                    "{CONNECTOR_NAME} ID: {}: rejected message at offset {}: {reason}",
                    mode.id, message.offset
                ),
                None => format!("{CONNECTOR_NAME} ID: {}: {reason}", mode.id),
            }),
            false,
            false,
        ),
        // The frame is another chunk's, and that chunk's own retry meets the
        // rejection. This batch is worth delivering again on a fresh
        // connection, which the next borrow dials, because the client retires
        // a latched one when it is returned.
        StopCause::Inherited(failure) => (
            Error::CannotStoreData(format!(
                "{CONNECTOR_NAME} ID: {}: QuestDB rejected a frame that an earlier chunk left on this connection: {}",
                mode.id, failure.error
            )),
            false,
            true,
        ),
        StopCause::Flush(failure) => {
            let code = failure.error.code();
            let stops_batch = failure.phase == FlushPhase::Borrowing
                || (is_transient(&failure.error) && code != ErrorCode::SymbolDictFull);
            (
                map_client_error(mode, failure),
                stops_batch,
                code == ErrorCode::ServerRejection,
            )
        }
    };
    Box::new(ChunkFailure {
        error,
        stops_batch,
        server_rejection,
        undelivered: MessageRange::of(&messages[confirmed_to..]),
        attempted,
        outcome,
    })
}

fn map_client_error(mode: ChunkMode, failure: FlushError) -> Error {
    let FlushError { phase, error } = failure;
    let message = format!("{CONNECTOR_NAME} ID: {}: {error}", mode.id);
    // At most once, a write in doubt is not worth a retry, because the runtime
    // never delivers the batch again and a second copy could only duplicate
    // rows. At least once, the runtime delivers the batch again anyway, and
    // its duplicates are the contract, so the code alone decides.
    let retryable = if mode.at_least_once {
        is_retryable(error.code(), false)
    } else {
        is_transient(&error)
    };
    let transient = match phase {
        // A borrow failure is classified by its own code, because
        // `borrow_sender` also dials: a refused certificate or a bad
        // credential arrives here and is permanent, while a dead host is
        // transient. An exhausted pool is `InvalidApiCall`, a code the client
        // otherwise uses for caller mistakes, so it counts as transient in
        // this phase only. The one other borrow `InvalidApiCall`, a closed
        // pool, cannot reach `consume`, because `close` takes the pool first.
        FlushPhase::Borrowing => retryable || error.code() == ErrorCode::InvalidApiCall,
        FlushPhase::Publishing => retryable,
        // The frame is already published. At most once, nothing here is safe
        // to send again. At least once, the runtime sends it again, and an
        // acknowledgement that did not arrive in time is worth that retry.
        FlushPhase::Awaiting => mode.at_least_once && retryable,
    };
    if transient {
        return Error::CannotStoreData(message);
    }
    // `ServerSchemaMismatch` is a query-path code and never reaches a sender.
    // A schema rejection arrives as `ServerRejection` with the structured
    // rejection attached, so the category is read from there.
    if error
        .qwp_ws_rejection()
        .is_some_and(|rejection| rejection.category == QwpWsErrorCategory::SchemaMismatch)
    {
        return Error::SchemaMismatch(message);
    }
    Error::PermanentHttpError(message)
}

/// Whether an at-most-once chunk can go on past a window whose publish
/// failed.
///
/// A permanent error that is not a server rejection belongs to that frame
/// alone, for example a frame over the size cap, and the connection can
/// still publish the rest of the chunk. A server rejection latches the
/// connection, and a transient error belongs to it, so both stop the chunk.
/// At least once, the call fails instead, and the runtime delivers the batch
/// again.
fn skip_window(failure: &FlushError, mode: ChunkMode) -> bool {
    !mode.at_least_once
        && !is_transient(&failure.error)
        && failure.error.code() != ErrorCode::ServerRejection
}

/// Records a window that an at-most-once chunk lost to its own publish
/// failure, and logs the messages that it held by offset. The rows of the
/// window come out of `rows_written`, and the error fails the batch once the
/// rest of it is written.
fn lose_window(
    outcome: &mut BatchOutcome,
    rows: usize,
    failure: FlushError,
    mode: ChunkMode,
    context: RowContext<'_>,
    window: Option<MessageRange>,
) {
    let RowContext {
        stream,
        topic,
        partition_id,
    } = context;
    let error = map_client_error(mode, failure);
    error!(
        "{CONNECTOR_NAME} ID: {} QuestDB did not take a window of {rows} rows, and the chunk goes on without it, stream: {stream}, topic: {topic}, partition_id: {partition_id}, {}: {error}",
        mode.id,
        WindowLabel(window)
    );
    outcome.rows_written -= rows;
    outcome.window_failure.get_or_insert(error);
}

/// A failed chunk, classified for `consume`.
struct ChunkFailure {
    error: Error,
    /// The failure belongs to the connection rather than to the frame, so the
    /// chunks behind this one would fail the same way. Read at most once only.
    stops_batch: bool,
    /// The failure was a terminal server rejection, which the handler counts
    /// as well, so one token has to be retired for it.
    server_rejection: bool,
    /// The messages of this chunk that QuestDB did not confirm.
    undelivered: Option<MessageRange>,
    /// How many messages of this chunk were appended or individually rejected.
    attempted: usize,
    /// The chunk's counts up to the failure.
    outcome: BatchOutcome,
}

/// What the batch-level rejection check found.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Unclaimed {
    /// A terminal rejection the handler reported that no failed chunk retired.
    Rejection,
    /// The client's bounded rejection inbox dropped events, so a terminal one
    /// may have gone unreported.
    EventsDropped(u64),
}

/// Settles the rejection balance for one batch.
///
/// The handler credits one per terminal rejection on the client's thread, and
/// a chunk that failed on one debits one on the Tokio thread, in no fixed
/// order, so a debit can precede its credit and the balance goes negative
/// until the credit lands. Only a positive balance is a rejection nobody
/// accounted for. A negative one is forgiven only when the client reports
/// that its inbox dropped events: a dropped terminal event can never repay its
/// debit, and left alone that debt would absorb the next rejection the
/// handler alone reports. Forgiving on every check instead would reopen the
/// race, because a debit whose credit is merely late would be erased too.
///
/// One case stays open, because the counters cannot tell a dropped credit from
/// a late one. If a drop and a still-pending credit coincide, the forgive step
/// erases debt that the late credit was going to repay, and that credit then
/// fails one later batch. The drop itself is reported, so the operator already
/// has a reason to distrust the next failure.
///
/// A second case is a plain race. If another topic's batch settles a credit
/// as an unclaimed rejection before the chunk that failed on it debits, the
/// rejection is reported twice, once per batch, and the debit leaves the
/// balance at -1, where it absorbs the next rejection only the handler
/// reports. Nothing ties a credit to a batch, so the sink cannot tell the two
/// orders apart. The handler still logs every rejection, but the absorbed one
/// fails no batch.
fn settle_rejections(
    balance: &AtomicI64,
    drops_seen: &AtomicU64,
    dropped_now: u64,
) -> Option<Unclaimed> {
    // `fetch_max`, not `swap`: two batches of different topics can read the
    // count at different moments, and a store of the older one would move it
    // back, so that the next batch reported the same drop again. The load
    // first keeps the shared line read-only while the count stands still,
    // which it does for almost every batch.
    if drops_seen.load(Ordering::Relaxed) < dropped_now
        && drops_seen.fetch_max(dropped_now, Ordering::Relaxed) < dropped_now
    {
        let _ = balance.try_update(Ordering::Relaxed, Ordering::Relaxed, |debt| {
            (debt < 0).then_some(0)
        });
        return Some(Unclaimed::EventsDropped(dropped_now));
    }
    balance
        .try_update(Ordering::Relaxed, Ordering::Relaxed, |count| {
            (count > 0).then(|| count - 1)
        })
        .is_ok()
        .then_some(Unclaimed::Rejection)
}

/// Whether this rejection still fits the batch's `error` budget.
///
/// `already_logged` carries the rejections of earlier chunks of the same runtime
/// batch, so the budget spans the batch rather than restarting for each chunk.
/// Past it the record is logged at `debug` instead, which bounds the error log
/// without losing the record's identity.
fn should_log_rejection(already_logged: usize, rejected_in_chunk: usize) -> bool {
    already_logged + rejected_in_chunk <= MAX_LOGGED_REJECTIONS_PER_BATCH
}

/// Whether a run of unacknowledged batches has gone on long enough to report.
///
/// One unacknowledged flush is delivery lag. A run of them means the server has
/// stopped acknowledging, and reporting every one of those batches as a success
/// would leave the runtime's metrics showing a healthy connector while nothing
/// is committed.
fn should_escalate_stall(consecutive_batches: u64) -> bool {
    consecutive_batches >= PENDING_ACK_BATCHES_BEFORE_ESCALATION
}

/// The windows a chunk published, and how far QuestDB confirmed them.
///
/// The acknowledged watermark is read after every publish and kept. The
/// client refuses to report it once the connection latched a terminal error,
/// and without the kept value a latch would confirm nothing. A window that
/// QuestDB acknowledged after the last read still counts as unconfirmed.
/// Reading more often takes a lock that the client's I/O thread also takes,
/// so once per window is the bound.
#[derive(Debug)]
struct ChunkWindows {
    ack_level: AckLevelSetting,
    flush_timeout: Duration,
    mode: ChunkMode,
    /// The rejections that earlier chunks of the batch logged, for the
    /// `error` budget of [`should_log_rejection`].
    already_logged: usize,
    /// The messages that the chunk rejected at most once, in order. They hold
    /// no row, so a window that is too large for a frame is built again
    /// without them.
    skipped: Vec<usize>,
    windows: Vec<PublishedWindow>,
    acked: Option<u64>,
}

impl ChunkWindows {
    fn new(
        ack_level: AckLevelSetting,
        flush_timeout: Duration,
        mode: ChunkMode,
        already_logged: usize,
    ) -> Self {
        Self {
            ack_level,
            flush_timeout,
            mode,
            already_logged,
            skipped: Vec::new(),
            windows: Vec::new(),
            acked: None,
        }
    }

    /// Counts a record that the sink rejected and logs it by its identity.
    ///
    /// At most once, a rejected record is unrecoverable: the runtime committed
    /// the offset before `consume` ran and never replays the batch, so this
    /// log line is the only trace that survives. At least once, the call fails
    /// at the record, and the runtime stops the delivery there, so the record
    /// is not lost. Either way, the line names the message, one line per
    /// record rather than a batch summary. Past the `error` budget the line
    /// drops to `debug` rather than disappearing, so an operator tracing a bad
    /// producer still finds its identity, while `error` stays bounded for
    /// alerting.
    fn note_rejection(
        &self,
        outcome: &mut BatchOutcome,
        context: RowContext<'_>,
        message: &ConsumedMessage,
        reason: &str,
    ) {
        let id = self.mode.id;
        let RowContext {
            stream,
            topic,
            partition_id,
        } = context;
        outcome.rejected_rows += 1;
        if should_log_rejection(self.already_logged, outcome.rejected_rows) {
            let payload = if self.mode.log_payload {
                format!(", payload: {}", payload_preview(&message.payload))
            } else {
                String::new()
            };
            error!(
                "{CONNECTOR_NAME} ID: {id} rejected message, stream: {stream}, topic: {topic}, partition_id: {partition_id}, offset: {}, message_id: {}, reason: {reason}{payload}",
                message.offset, message.id
            );
        } else {
            debug!(
                "{CONNECTOR_NAME} ID: {id} rejected message, stream: {stream}, topic: {topic}, partition_id: {partition_id}, offset: {}, message_id: {}, reason: {reason}",
                message.offset, message.id
            );
        }
    }

    /// Publishes the buffer as one window, without waiting for the server.
    /// `end` is the index after the window's last message, and `rows` the
    /// chunk's written rows at that point.
    ///
    /// A publish refused by the connection itself, a latch left by an earlier
    /// frame's terminal rejection or a full symbol dictionary, fails the
    /// window rather than being re-sent on a fresh borrow. Re-sending looks
    /// cheap but needs a second pool slot while the first is still held,
    /// splits the chunk's acknowledgement across two senders, and drops the
    /// first mid-loop. Failing keeps one sender per chunk, so the caller names
    /// the lost window by offset and retires the rejection token against the
    /// batch that saw it. The sender is retired when the chunk returns it, so
    /// the next chunk borrows a fresh one.
    fn publish(
        &mut self,
        sender: &mut questdb::BorrowedSender<'_>,
        buffer: &mut questdb::ingress::Buffer,
        end: usize,
        rows: usize,
    ) -> Result<(), FlushError> {
        let fsn = sender
            .flush_buffer_and_get_fsn(buffer)
            .map_err(|error| FlushError {
                phase: FlushPhase::Publishing,
                error,
            })?;
        // The caller publishes only a buffer that holds rows, so a frame
        // always has a sequence number.
        if let Some(fsn) = fsn {
            self.windows.push(PublishedWindow { fsn, end, rows });
        }
        self.observe(sender);
        Ok(())
    }

    /// Publishes the buffer as the window of `messages[window]`, or stops the
    /// chunk at its end.
    ///
    /// A row that alone is larger than a frame is a rejected record. A window
    /// of several rows that is larger than a frame is built again in two
    /// halves. At most once, a window that only its own frame failed is lost
    /// and logged by offset. Either way, at most once the chunk goes on with a
    /// fresh buffer.
    fn publish_window(
        &mut self,
        sender: &mut questdb::BorrowedSender<'_>,
        buffer: &mut questdb::ingress::Buffer,
        window: Range<usize>,
        outcome: &mut BatchOutcome,
        rows: &ChunkRows<'_>,
        scratch: &mut RowScratch,
    ) -> Result<(), Box<ChunkFailure>> {
        let ChunkRows {
            mapping,
            context,
            messages,
        } = *rows;
        let Err(failure) = self.publish(sender, buffer, window.end, outcome.rows_written) else {
            return Ok(());
        };
        // A frame of one row that the client found larger than a frame is
        // that record's fault, so it is rejected like any other record. The
        // window starts at its row, and the loop gives such a row a window of
        // its own.
        if buffer.row_count() == 1 && failure.error.code() == ErrorCode::BatchTooLarge {
            let reason = failure.error.to_string();
            self.note_rejection(outcome, context, &messages[window.start], &reason);
            if self.mode.at_least_once {
                let outcome = std::mem::take(outcome);
                return Err(self.reject(sender, outcome, context, messages, window.start, reason));
            }
            outcome.rows_written -= 1;
            outcome.last_rejection = Some(reason);
            *buffer = sender.new_buffer();
            return Ok(());
        }
        // A window of several rows that is larger than a frame means that the
        // frame cap is below `max_flush_bytes`, for example from `max_buf_size`
        // or from the server's advertised batch size. The client sent none of
        // it, so the halves are built again from their messages, until every
        // frame fits or one row is left. Each half starts at a written row, so
        // a half of one row names its message by `window.start`.
        let written: Vec<usize> =
            if buffer.row_count() > 1 && failure.error.code() == ErrorCode::BatchTooLarge {
                self.written(window.clone()).collect()
            } else {
                Vec::new()
            };
        if written.len() > 1 && written.len() == buffer.row_count() {
            let middle = written[written.len() / 2];
            // `publish` records the chunk's written rows at the end of each
            // window, so the rows of the later half are out of the count while
            // the earlier half is published.
            let mut later = written.len() - written.len() / 2;
            outcome.rows_written -= later;
            for half in [window.start..middle, middle..window.end] {
                *buffer = sender.new_buffer();
                // A row that the mapping took once takes again, so a rebuild
                // that fails stops the chunk on the size error.
                let rebuilt = self.written(half.clone()).all(|index| {
                    mapping
                        .append_row_with(buffer, &messages[index], context, scratch)
                        .is_ok()
                });
                if !rebuilt {
                    self.settle(sender);
                    return Err(self.stop(failure, std::mem::take(outcome), window.end, messages));
                }
                self.publish_window(sender, buffer, half, outcome, rows, scratch)?;
                outcome.rows_written += std::mem::take(&mut later);
            }
            return Ok(());
        }
        if skip_window(&failure, self.mode) {
            lose_window(
                outcome,
                buffer.row_count(),
                failure,
                self.mode,
                context,
                MessageRange::of(&messages[window]),
            );
            *buffer = sender.new_buffer();
            return Ok(());
        }
        self.settle(sender);
        Err(self.stop(failure, std::mem::take(outcome), window.end, messages))
    }

    /// The messages of `window` that hold a row.
    fn written(&self, window: Range<usize>) -> impl Iterator<Item = usize> + '_ {
        let first = self.skipped.partition_point(|&index| index < window.start);
        let last = self.skipped.partition_point(|&index| index < window.end);
        let skipped = &self.skipped[first..last];
        window.filter(move |index| skipped.binary_search(index).is_err())
    }

    /// Fails the chunk at the record at `index` once QuestDB confirmed every
    /// window in front of it. At least once, the runtime stores no offset for
    /// the batch, so an operator skips the record by its offset, and that skip
    /// must not take unwritten rows along with it.
    fn reject(
        &mut self,
        sender: &mut questdb::BorrowedSender<'_>,
        mut outcome: BatchOutcome,
        context: RowContext<'_>,
        messages: &[ConsumedMessage],
        index: usize,
        reason: String,
    ) -> Box<ChunkFailure> {
        let cause = match self.await_all(sender, &mut outcome, context, messages) {
            Ok(()) => StopCause::Rejected(reason),
            Err(failure) => self.cause(failure),
        };
        chunk_stop(cause, outcome, self.confirmed(), index, messages, self.mode)
    }

    /// Stops the chunk on a failure of its publish or wait. `attempted` is the
    /// index at which the loop stopped appending.
    fn stop(
        &self,
        failure: FlushError,
        outcome: BatchOutcome,
        attempted: usize,
        messages: &[ConsumedMessage],
    ) -> Box<ChunkFailure> {
        chunk_stop(
            self.cause(failure),
            outcome,
            self.confirmed(),
            attempted,
            messages,
            self.mode,
        )
    }

    /// `Duration::ZERO` means "no deadline" to the client, not "do not wait",
    /// so the sink treats it as fire and forget and never calls `wait`, where
    /// it would block without bound. `QuestDbSink::new` refuses a zero timeout
    /// with a durable ack level, so only the default level gets here.
    fn fire_and_forget(&self) -> bool {
        self.flush_timeout.is_zero()
    }

    /// Classifies a failure of this chunk's publish or wait.
    ///
    /// The pool returns a connection whose chunk failed on a timeout, and the
    /// frames that chunk published stay queued on it. A later chunk, perhaps
    /// of another topic, can borrow it, and the terminal rejection of one of
    /// those frames can arrive during that lease. A rejection that arrives
    /// while the connection is idle never reaches a chunk, because the next
    /// borrow retires the latched connection, and the unclaimed count reports
    /// it instead. Frame numbers grow on a connection, and a lease starts
    /// after every frame published before it. A rejection of one frame names
    /// only that frame, but a protocol violation names every frame from the
    /// oldest unacknowledged one to the newest, and so can end inside this
    /// lease. A rejection that starts before this chunk's first frame
    /// therefore belongs to an earlier lease. At least once, a permanent error
    /// for it would stop the delivery of a topic whose rows were valid. If the
    /// chunk's own frame was the bad one, the retry publishes it on a fresh
    /// connection, where the rejection starts at that frame and is permanent.
    fn cause(&self, failure: FlushError) -> StopCause {
        let inherited = self.mode.at_least_once
            && failure.error.code() == ErrorCode::ServerRejection
            && failure.error.qwp_ws_rejection().is_some_and(|rejection| {
                rejects_an_earlier_lease(
                    self.windows.first().map(|window| window.fsn),
                    rejection.from_fsn,
                )
            });
        if inherited {
            StopCause::Inherited(failure)
        } else {
            StopCause::Flush(failure)
        }
    }

    /// Keeps the acknowledged watermark, if the client still reports it.
    ///
    /// With durable acknowledgements the watermark follows the durable level
    /// that the connect string requests, which can be stronger than the
    /// configured `ack_level`. It then lags behind a successful `wait`, which
    /// is why a successful `wait` confirms every window on its own.
    ///
    /// The client documents that the watermark also covers a frame that the
    /// server rejected and skipped, but questdb-rs 7.3.0 applies no such
    /// policy: every rejection is retried or latched as terminal. A client
    /// that skipped rejected frames would make the sink count their windows
    /// as written. The integration test
    /// `given_a_terminal_server_rejection_when_consumed_should_fail_the_batch_and_write_the_next`
    /// fails when that happens.
    fn observe(&mut self, sender: &questdb::BorrowedSender<'_>) {
        if let Ok(acked) = sender.acked_fsn() {
            self.acked = self.acked.max(acked);
        }
    }

    /// Records that a `wait` at the configured level covered every window.
    fn confirm_all(&mut self) {
        self.acked = self.acked.max(self.windows.last().map(|window| window.fsn));
    }

    /// Gives the windows published before a failed publish the wait that the
    /// end of the chunk would have given them.
    ///
    /// A publish fails on its own frame, for example an oversized one or a
    /// full symbol dictionary, and the earlier frames stay queued and are
    /// delivered. Without the wait they would be reported as undelivered, and
    /// an operator who rewinds to them would write them twice. A latched
    /// connection fails the wait at once, and the kept watermark then stands.
    ///
    /// At least once, the runtime delivers the whole batch again whatever the
    /// wait finds, so the wait would only delay that retry by up to
    /// `flush_timeout`.
    fn settle(&mut self, sender: &mut questdb::BorrowedSender<'_>) {
        if self.windows.is_empty()
            || self.fire_and_forget()
            || !self.mode.wait
            || self.mode.at_least_once
        {
            return;
        }
        match sender.wait(self.ack_level.into(), self.flush_timeout) {
            Ok(()) => self.confirm_all(),
            Err(_) => self.observe(sender),
        }
    }

    /// The index after the last window that the kept watermark covers, and
    /// the written rows at that point.
    fn confirmed(&self) -> (usize, usize) {
        self.acked
            .and_then(|acked| self.windows.iter().rev().find(|window| window.fsn <= acked))
            .map_or((0, 0), |window| (window.end, window.rows))
    }

    /// Waits once for the configured acknowledgement of every window the
    /// chunk published.
    ///
    /// A pending acknowledgement is lag rather than loss, so it is counted and
    /// warned about instead of failing the batch: the frames are queued and
    /// the client keeps delivering them. `wait` already polls until
    /// `flush_timeout` passes without progress, so there is nothing to retry
    /// there. A stall that outlasts several batches is escalated by the caller
    /// instead, where the run of them is visible.
    ///
    /// The unconfirmed messages are logged with their offsets. The batch still
    /// reports success, and if the connection later fails for good the client
    /// drops the queued frames and reports it only in a warning of the client,
    /// without the offsets, so this line is what lets an operator rewind to
    /// those rows.
    ///
    /// At least once, an acknowledgement that did not arrive fails the chunk
    /// instead, because the runtime stores the offsets of a successful call.
    fn await_all(
        &mut self,
        sender: &mut questdb::BorrowedSender<'_>,
        outcome: &mut BatchOutcome,
        context: RowContext<'_>,
        messages: &[ConsumedMessage],
    ) -> Result<(), FlushError> {
        let Some(last) = self.windows.last().copied() else {
            return Ok(());
        };
        let id = self.mode.id;
        let RowContext {
            stream,
            topic,
            partition_id,
        } = context;
        let published = WindowLabel(MessageRange::of(&messages[..last.end]));

        // This runs on every chunk, so it logs at `debug`.
        if self.fire_and_forget() {
            debug!(
                "{CONNECTOR_NAME} ID: {id} flushed rows without waiting for an acknowledgement, stream: {stream}, topic: {topic}, partition_id: {partition_id}, {published}"
            );
            return Ok(());
        }
        if !self.mode.wait {
            outcome.pending_acks += 1;
            warn!(
                "{CONNECTOR_NAME} ID: {id} flushed rows without waiting for an acknowledgement, because an earlier chunk of this batch is still unacknowledged, stream: {stream}, topic: {topic}, partition_id: {partition_id}, {published}. The frames are queued and delivery continues in the background"
            );
            return Ok(());
        }

        match sender.wait(self.ack_level.into(), self.flush_timeout) {
            Ok(()) => {
                self.confirm_all();
                debug!(
                    "{CONNECTOR_NAME} ID: {id} flushed rows that QuestDB acknowledged at ack_level {}, stream: {stream}, topic: {topic}, partition_id: {partition_id}, {published}",
                    self.ack_level
                );
                Ok(())
            }
            Err(error) if is_pending_ack(&error) && !self.mode.at_least_once => {
                self.observe(sender);
                outcome.pending_acks += 1;
                let (confirmed_to, _) = self.confirmed();
                let pending = WindowLabel(MessageRange::of(&messages[confirmed_to..last.end]));
                warn!(
                    "{CONNECTOR_NAME} ID: {id} flushed rows whose acknowledgement has not arrived yet, stream: {stream}, topic: {topic}, partition_id: {partition_id}, {pending}. The frames are queued and delivery continues in the background, so this is lag rather than a failed write, but it is only durable across a restart when the connect string sets `sf_dir`: {error}"
                );
                Ok(())
            }
            Err(error) => {
                self.observe(sender);
                Err(FlushError {
                    phase: FlushPhase::Awaiting,
                    error,
                })
            }
        }
    }
}

/// Whether a rejection whose span starts at `rejected_from` names a frame
/// published before the lease whose first frame is `first_fsn`. A lease that
/// published nothing owns no frame, so every rejection it meets is an earlier
/// one's.
fn rejects_an_earlier_lease(first_fsn: Option<u64>, rejected_from: u64) -> bool {
    first_fsn.is_none_or(|first| rejected_from < first)
}

/// Re-emits a QuestDB server rejection through `tracing`.
///
/// A terminal rejection reaches the sink as an error code with none of this
/// detail attached, and the client's own log lines do not name the connector.
/// This line gives an operator the category, the status and the frame range,
/// also for a retriable rejection, which tells that the server is pushing
/// back. The count lets a batch fail for a rejection that no `wait` reported.
fn report_server_rejection(id: u32, rejections: &AtomicI64, error: &QwpWsSenderError) {
    let status = error
        .status
        .map_or_else(|| "none".to_owned(), |status| format!("0x{status:02x}"));
    let sequence = error
        .message_sequence
        .map_or_else(|| "none".to_owned(), |sequence| sequence.to_string());
    let message = error.message.as_deref().unwrap_or("");
    let terminal = error.applied_policy == QwpWsErrorPolicy::Terminal;
    if terminal {
        // The handler runs on the client's own thread and is the only report of
        // a rejection the sink never asks `wait` about, which happens whenever
        // the ack level and the timeout make the sink skip the wait. Credit it
        // so a batch can fail rather than reporting success. The log line goes
        // first on purpose: it is the one record of the detail, and a batch
        // that claims the credit points the operator to it. A terminal error
        // is a server rejection or a protocol violation that the client found,
        // and the category names which.
        error!(
            "{CONNECTOR_NAME} ID: {id} QuestDB connection ended on a terminal error, category: {:?}, status: {status}, frames: [{}, {}], sequence: {sequence}, message: {message}",
            error.category, error.from_fsn, error.to_fsn
        );
        rejections.fetch_add(1, Ordering::Relaxed);
    } else {
        // The client replays these itself, so they are back-pressure rather
        // than loss. Still worth seeing: a stream of them is the only warning
        // before the queue stops draining.
        warn!(
            "{CONNECTOR_NAME} ID: {id} QuestDB rejected a batch, and the client applies its policy, category: {:?}, policy: {:?}, status: {status}, frames: [{}, {}], sequence: {sequence}, message: {message}",
            error.category, error.applied_policy, error.from_fsn, error.to_fsn
        );
    }
}

/// A `wait` that timed out without the server advancing its watermark.
///
/// The frames are already in the publication log, and the client keeps
/// delivering them in the background, so this is delivery lag rather than a
/// failure: the rows arrive once the server catches up or a reconnect
/// completes. Re-flushing would duplicate them, and reporting it as a failure
/// tells an operator they lost data they did not lose. Only `FailoverRetry`
/// carries this meaning in the awaiting phase; a schema, parse or security
/// rejection there is a genuine terminal error.
fn is_pending_ack(error: &questdb::Error) -> bool {
    error.code() == ErrorCode::FailoverRetry
}

/// Transport-level failures are worth retrying; server rejections are not,
/// because a rejection is deterministic. The server refused the frame, so the
/// same bytes will be refused again and the rows were never stored.
///
/// A retryable code alone is not enough. The client sets `in_doubt` when
/// delivery is unknown, and documents that `FailoverRetry` in particular can
/// carry it, so re-sending would duplicate rows the server may already hold.
/// Both must hold for a failure to count as transient.
///
/// The classification decides whether the rest of a batch is abandoned after
/// a chunk fails, and how the failure is reported. The runtime logs a failed
/// batch and counts it in `iggy_connector_errors_total`. At most once, it does
/// not replay the batch. At least once, [`map_client_error`] ignores
/// `in_doubt`, because the runtime delivers the batch again anyway, and the
/// class decides whether it retries or stops.
fn is_transient(error: &questdb::Error) -> bool {
    is_retryable(error.code(), error.in_doubt())
}

/// Split from [`is_transient`] so the rule can be exercised directly: the
/// client keeps `with_in_doubt` crate-private, so a test cannot build an
/// in-doubt `questdb::Error`.
fn is_retryable(code: ErrorCode, in_doubt: bool) -> bool {
    if in_doubt {
        return false;
    }
    matches!(
        code,
        ErrorCode::SocketError
            | ErrorCode::ConnectTimeout
            | ErrorCode::FailoverRetry
            | ErrorCode::RoleMismatch
            | ErrorCode::ServerFlushError
            | ErrorCode::ServerInternalError
            | ErrorCode::CouldNotResolveAddr
            // A full symbol dictionary belongs to one connection. The frame is
            // refused before any byte reaches the wire and the buffer is rolled
            // back, and returning the sender retires that connection, so the
            // next batch borrows a fresh one and succeeds.
            | ErrorCode::SymbolDictFull
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    fn config() -> QuestDbSinkConfig {
        QuestDbSinkConfig {
            connection_string: SecretString::from("ws::addr=localhost:9000;"),
            table: "iggy_events".to_owned(),
            timestamp_source: None,
            timestamp_field: None,
            timestamp_unit: None,
            symbol_columns: None,
            uuid_columns: None,
            integer_columns: None,
            include_stream_column: None,
            include_topic_column: None,
            include_partition_column: None,
            include_offset_column: None,
            include_headers: None,
            ack_level: None,
            flush_timeout: None,
            batch_size: None,
            max_flush_bytes: None,
            log_rejected_payload: None,
            numbers_as_double: None,
            verbose_logging: None,
        }
    }

    #[test]
    fn given_minimal_config_when_constructed_should_apply_defaults() {
        let sink = QuestDbSink::new(1, config());
        assert!(sink.init_error.is_none());
        assert_eq!(sink.batch_size, DEFAULT_BATCH_SIZE as usize);
        assert_eq!(sink.flush_timeout, Duration::from_secs(30));
        assert_eq!(sink.ack_level, AckLevelSetting::Ok);
        assert_eq!(sink.mapping.timestamp_source, TimestampSource::Message);
        assert!(sink.mapping.include_stream_column);
        assert!(sink.mapping.include_topic_column);
        assert!(!sink.mapping.include_partition_column);
    }

    #[test]
    fn given_durable_acks_with_a_zero_flush_timeout_when_constructed_should_record_init_error() {
        // The client reads a zero timeout as "no deadline", so a durable ack
        // level would wait for good inside the FFI call and the connector could
        // not be stopped.
        for level in ["durable", "local_durable"] {
            let mut config = config();
            config.ack_level = Some(level.to_owned());
            config.flush_timeout = Some("0s".to_owned());
            let sink = QuestDbSink::new(1, config);
            assert!(
                matches!(sink.init_error, Some(Error::InvalidConfigValue(_))),
                "{level}"
            );
        }
    }

    #[test]
    fn given_zero_flush_timeout_at_the_default_ack_level_when_constructed_should_be_accepted() {
        // At the default ack level a zero timeout is fire and forget, which is a
        // legitimate choice, so the guard above must not reject it.
        let mut config = config();
        config.flush_timeout = Some("0s".to_owned());
        let sink = QuestDbSink::new(1, config);
        assert!(sink.init_error.is_none());
        assert_eq!(sink.flush_timeout, Duration::ZERO);
    }

    #[test]
    fn given_rejections_spread_over_chunks_when_deciding_should_cap_across_the_batch() {
        // The budget spans the runtime batch, so a later chunk inherits what the
        // earlier ones spent rather than starting over.
        assert!(should_log_rejection(0, 1));
        assert!(should_log_rejection(0, MAX_LOGGED_REJECTIONS_PER_BATCH));
        assert!(!should_log_rejection(
            0,
            MAX_LOGGED_REJECTIONS_PER_BATCH + 1
        ));
        assert!(should_log_rejection(MAX_LOGGED_REJECTIONS_PER_BATCH - 1, 1));
        assert!(!should_log_rejection(
            MAX_LOGGED_REJECTIONS_PER_BATCH - 1,
            2
        ));
        assert!(!should_log_rejection(MAX_LOGGED_REJECTIONS_PER_BATCH, 1));
    }

    #[test]
    fn given_a_run_of_unacknowledged_batches_when_deciding_should_escalate_at_the_threshold() {
        assert!(!should_escalate_stall(0));
        assert!(!should_escalate_stall(
            PENDING_ACK_BATCHES_BEFORE_ESCALATION - 1
        ));
        assert!(should_escalate_stall(PENDING_ACK_BATCHES_BEFORE_ESCALATION));
        assert!(should_escalate_stall(
            PENDING_ACK_BATCHES_BEFORE_ESCALATION + 1
        ));
    }

    #[test]
    fn given_a_short_payload_when_previewing_should_return_it_whole() {
        let preview = payload_preview(&Payload::Text("hello".to_owned()));
        assert_eq!(preview, "hello");
    }

    #[test]
    fn given_a_long_payload_when_previewing_should_truncate_and_report_the_size() {
        let text = "a".repeat(REJECTED_PAYLOAD_PREVIEW_BYTES * 2);
        let preview = payload_preview(&Payload::Text(text.clone()));
        assert_eq!(
            preview,
            format!(
                "{}… ({} bytes total)",
                "a".repeat(REJECTED_PAYLOAD_PREVIEW_BYTES),
                text.len()
            )
        );
    }

    #[test]
    fn given_a_long_json_payload_when_previewing_should_keep_only_the_head_and_report_the_size() {
        let value: simd_json::OwnedValue = simd_json::json!({
            "note": "é".repeat(REJECTED_PAYLOAD_PREVIEW_BYTES)
        });
        let full = simd_json::to_string(&value).expect("render");
        // `{"note":"` is 9 bytes and `é` is 2, so an odd remainder puts the cap
        // inside an `é` and makes the writer drop the half character.
        let prefix = "{\"note\":\"";
        assert_eq!((REJECTED_PAYLOAD_PREVIEW_BYTES - prefix.len()) % 2, 1);
        let kept = (REJECTED_PAYLOAD_PREVIEW_BYTES - prefix.len()) / 2;
        let preview = payload_preview(&Payload::Json(value));
        assert_eq!(
            preview,
            format!("{prefix}{}… ({} bytes total)", "é".repeat(kept), full.len())
        );
    }

    #[test]
    fn given_a_short_json_payload_when_previewing_should_render_it_whole() {
        let preview = payload_preview(&Payload::Json(simd_json::json!({"price": 1.5})));
        assert_eq!(preview, "{\"price\":1.5}");
    }

    #[test]
    fn given_a_multibyte_boundary_when_previewing_should_not_split_a_character() {
        // Three bytes per character puts the boundaries on multiples of three.
        // The cut is at 512, which is not one of them, so it has to round down
        // to a character boundary rather than slicing a character in half. A
        // two-byte character would not test this: every boundary would be
        // even, and so is the cut.
        assert_ne!(
            REJECTED_PAYLOAD_PREVIEW_BYTES % "€".len(),
            0,
            "the cut has to land inside a character for this test to mean anything"
        );
        let text = "€".repeat(REJECTED_PAYLOAD_PREVIEW_BYTES);
        let preview = payload_preview(&Payload::Text(text));
        assert!(preview.contains('€'));
        assert!(
            preview.starts_with(&"€".repeat(REJECTED_PAYLOAD_PREVIEW_BYTES / "€".len())),
            "the preview keeps every whole character before the cut"
        );
    }

    #[test]
    fn given_a_binary_payload_when_previewing_should_report_the_byte_count_only() {
        let preview = payload_preview(&Payload::Raw(vec![0xff, 0xfe, 0xfd]));
        assert_eq!(preview, "<3 bytes>");
    }

    #[tokio::test]
    async fn given_durable_acks_without_the_connect_string_flag_when_opened_should_fail() {
        // The QuestDB client refuses `AckLevel::Durable` unless the connect string asked
        // for it, which would otherwise fail every batch while the rows landed
        // anyway, reading to an operator as total data loss.
        let mut config = config();
        config.ack_level = Some("durable".to_owned());
        let mut sink = QuestDbSink::new(1, config);
        assert!(matches!(
            sink.open().await,
            Err(Error::InvalidConfigValue(_))
        ));
    }

    #[test]
    fn given_the_durable_flag_without_an_ack_level_when_constructed_should_wait_for_durable_acks() {
        let mut config = config();
        config.connection_string = "ws::addr=127.0.0.1:1;request_durable_ack=on;"
            .to_owned()
            .into();
        assert_eq!(
            QuestDbSink::new(1, config.clone()).ack_level,
            AckLevelSetting::Durable
        );

        // An explicit level still wins over the flag.
        config.ack_level = Some("ok".to_owned());
        assert_eq!(QuestDbSink::new(1, config).ack_level, AckLevelSetting::Ok);
    }

    #[test]
    fn given_the_durable_ack_key_anywhere_in_the_connect_string_when_read_should_match_the_client()
    {
        for (connection_string, expected) in [
            // The first key follows `::`, with no `;` before it.
            (
                "ws::request_durable_ack=on;addr=a:9000;",
                AckLevelSetting::Durable,
            ),
            (
                "wss::request_durable_ack=local;addr=a:9000",
                AckLevelSetting::LocalDurable,
            ),
            // Failover repeats `addr`, which the client accepts.
            (
                "ws::addr=a:9000;addr=b:9000;request_durable_ack=replicated;",
                AckLevelSetting::Durable,
            ),
            (
                "ws::addr=a:9000,b:9000;request_durable_ack=local;",
                AckLevelSetting::LocalDurable,
            ),
            // `;;` is a literal `;` inside a value, so this is a password.
            (
                "ws::addr=a:9000;password=x;;request_durable_ack=on;;y;",
                AckLevelSetting::Ok,
            ),
            (
                "ws::password=x;;y;request_durable_ack=local;",
                AckLevelSetting::LocalDurable,
            ),
            // The client refuses these sets, so they ask for nothing.
            (
                "ws::addr=a:9000;request_durable_ack=on,local;",
                AckLevelSetting::Ok,
            ),
            (
                "ws::addr=a:9000;request_durable_ack=local, replicated;",
                AckLevelSetting::Ok,
            ),
            // Keys match exactly, as in the client.
            (
                "ws::addr=a:9000;REQUEST_DURABLE_ACK=on;",
                AckLevelSetting::Ok,
            ),
            ("ws::addr=a:9000;", AckLevelSetting::Ok),
        ] {
            let requested = request_durable_ack(connection_string)
                .as_deref()
                .and_then(ack_level_for)
                .unwrap_or(AckLevelSetting::Ok);
            assert_eq!(requested, expected, "{connection_string}");
        }
    }

    #[test]
    fn given_each_durable_ack_value_without_an_ack_level_when_constructed_should_follow_it() {
        // `on`, `replicated` and `local,replicated` all make the client trim on
        // the replicated acknowledgement. `local` alone asks for the local
        // flush only, so it selects the local wait and never the replicated one.
        for (value, expected) in [
            ("on", AckLevelSetting::Durable),
            ("ON", AckLevelSetting::Durable),
            ("replicated", AckLevelSetting::Durable),
            ("local,replicated", AckLevelSetting::Durable),
            ("replicated,local", AckLevelSetting::Durable),
            ("local", AckLevelSetting::LocalDurable),
            ("LOCAL", AckLevelSetting::LocalDurable),
            ("off", AckLevelSetting::Ok),
        ] {
            let mut config = config();
            config.connection_string =
                format!("ws::addr=127.0.0.1:1;request_durable_ack={value};").into();
            assert_eq!(
                QuestDbSink::new(1, config).ack_level,
                expected,
                "request_durable_ack={value}"
            );
        }
    }

    #[tokio::test]
    async fn given_local_durable_acks_without_the_local_flag_when_opened_should_fail() {
        // The client refuses a local wait unless the set is exactly `local`:
        // with `local,replicated` it trims on the replicated acknowledgement.
        // The error names what the string sets, and which level that gives,
        // so the operator can see that `durable` is the level to use.
        for (flag, expected) in [
            ("", "does not set request_durable_ack"),
            ("request_durable_ack=on;", "gives ack_level \"durable\""),
            (
                "request_durable_ack=local,replicated;",
                "sets request_durable_ack=local,replicated, which gives ack_level \"durable\"",
            ),
            (
                "request_durable_ack=local, replicated;",
                "sets request_durable_ack=local, replicated, which the QuestDB client does not accept",
            ),
            // A space after `;` hides the key from the client too.
            (
                "foo=bar; request_durable_ack=local;",
                "does not set request_durable_ack in a form that the QuestDB client reads",
            ),
        ] {
            let mut config = config();
            config.connection_string = format!("ws::addr=127.0.0.1:1;{flag}").into();
            config.ack_level = Some("local_durable".to_owned());
            let mut sink = QuestDbSink::new(1, config);
            match sink.open().await {
                Err(Error::InvalidConfigValue(reason)) => {
                    assert!(reason.contains(expected), "{flag:?}: {reason}");
                    // The hint about spaces fits only a string that holds
                    // the key.
                    assert_eq!(
                        reason.contains("no space around it"),
                        flag.contains("request_durable_ack") && !reason.contains("sets"),
                        "{flag:?}: {reason}"
                    );
                }
                other => panic!("{flag:?}: expected InvalidConfigValue, got {other:?}"),
            }
        }
    }

    #[tokio::test]
    async fn given_durable_acks_with_the_connect_string_flag_when_opened_should_pass_validation() {
        // The pairing is accepted; only the connect itself then fails, which
        // proves the guard above is not rejecting a valid configuration.
        let mut config = config();
        config.ack_level = Some("durable".to_owned());
        config.connection_string = "ws::addr=127.0.0.1:1;request_durable_ack=on;"
            .to_owned()
            .into();
        let mut sink = QuestDbSink::new(1, config);
        assert!(matches!(sink.open().await, Err(Error::InitError(_))));
    }

    #[tokio::test]
    async fn given_local_durable_acks_with_the_local_flag_when_opened_should_pass_validation() {
        let mut config = config();
        config.ack_level = Some("local_durable".to_owned());
        config.connection_string = "ws::addr=127.0.0.1:1;request_durable_ack=local;"
            .to_owned()
            .into();
        let mut sink = QuestDbSink::new(1, config);
        assert!(matches!(sink.open().await, Err(Error::InitError(_))));
    }

    #[tokio::test]
    async fn given_payload_timestamp_without_field_when_opened_should_fail() {
        let mut config = config();
        config.timestamp_source = Some("payload".to_owned());
        let mut sink = QuestDbSink::new(1, config);
        let result = sink.open().await;
        assert!(
            matches!(&result, Err(Error::InvalidConfigValue(reason)) if reason.contains("timestamp_field")),
            "got {result:?}"
        );
    }

    #[tokio::test]
    async fn given_malformed_connection_string_when_opened_should_fail() {
        let mut config = config();
        config.connection_string = SecretString::from("not-a-connect-string");
        let mut sink = QuestDbSink::new(1, config);
        assert!(matches!(sink.open().await, Err(Error::InitError(_))));
    }

    #[cfg(not(feature = "insecure-skip-verify"))]
    #[tokio::test]
    async fn given_the_tls_bypass_without_the_feature_when_opened_should_fail() {
        // The default build must not accept a connect string that would skip
        // certificate verification, and must say why rather than reporting the
        // client's generic configuration error.
        let mut config = config();
        config.connection_string =
            SecretString::from("wss::addr=localhost:9000;tls_verify=unsafe_off;");
        let mut sink = QuestDbSink::new(1, config);
        let Err(Error::InvalidConfigValue(message)) = sink.open().await else {
            panic!("a default build must refuse tls_verify=unsafe_off");
        };
        assert!(message.contains("insecure-skip-verify"), "{message}");
    }

    #[tokio::test]
    async fn given_unsupported_transport_when_opened_should_fail() {
        // The sink is QWP-only. An ILP connect string parses but must not be
        // accepted, since the QWP-only column types would fail per row later.
        let mut config = config();
        config.connection_string = SecretString::from("http::addr=localhost:9000;");
        let mut sink = QuestDbSink::new(1, config);
        let Err(Error::InitError(message)) = sink.open().await else {
            panic!("an ILP connect string must not be accepted");
        };
        // Without this the test would pass on a failed dial to a free port,
        // which reports the same variant.
        assert!(
            message.contains("QWP/WebSocket"),
            "the error must name the protocol the sink requires: {message}"
        );
    }

    #[tokio::test]
    async fn given_unreachable_server_when_opened_should_fail_fast() {
        // Port 1 has no listener. `open` doubles as the connectivity check, so
        // a dead endpoint must surface at startup rather than at first flush.
        let mut config = config();
        config.connection_string = SecretString::from("ws::addr=127.0.0.1:1;");
        let mut sink = QuestDbSink::new(1, config);
        assert!(matches!(sink.open().await, Err(Error::InitError(_))));
    }

    #[tokio::test]
    async fn given_never_opened_sink_when_consuming_should_report_missing_pool() {
        let sink = QuestDbSink::new(1, config());
        let topic_metadata = TopicMetadata {
            stream: "s".to_owned(),
            topic: "t".to_owned(),
        };
        let messages_metadata = MessagesMetadata {
            partition_id: 1,
            current_offset: 0,
            schema: iggy_connector_sdk::Schema::Json,
        };
        let result = sink
            .consume(&topic_metadata, messages_metadata, Vec::new())
            .await;
        assert!(matches!(result, Err(Error::InitError(_))));
    }

    #[test]
    fn given_timestamp_field_with_another_source_when_constructed_should_record_init_error() {
        for source in ["message", "origin", "server"] {
            let mut config = config();
            config.timestamp_source = Some(source.to_owned());
            config.timestamp_field = Some("event_time".to_owned());
            let sink = QuestDbSink::new(1, config);
            assert!(
                matches!(&sink.init_error, Some(Error::InvalidConfigValue(reason)) if reason.contains("timestamp_field")),
                "source {source}: got {:?}",
                sink.init_error
            );
        }
    }

    #[test]
    fn given_concrete_timestamp_unit_with_another_source_when_constructed_should_record_init_error()
    {
        for source in ["message", "origin", "server"] {
            let mut millis_config = config();
            millis_config.timestamp_source = Some(source.to_owned());
            millis_config.timestamp_unit = Some("millis".to_owned());
            let sink = QuestDbSink::new(1, millis_config);
            assert!(
                matches!(&sink.init_error, Some(Error::InvalidConfigValue(reason)) if reason.contains("timestamp_unit")),
                "source {source}: got {:?}",
                sink.init_error
            );

            // `auto` is the default, and the example configurations set it with
            // every source.
            let mut auto_config = config();
            auto_config.timestamp_source = Some(source.to_owned());
            auto_config.timestamp_unit = Some("auto".to_owned());
            let sink = QuestDbSink::new(1, auto_config);
            assert!(
                sink.init_error.is_none(),
                "source {source}: got {:?}",
                sink.init_error
            );
        }
    }

    #[test]
    fn given_payload_timestamp_with_field_when_constructed_should_succeed() {
        let mut config = config();
        config.timestamp_source = Some("payload".to_owned());
        config.timestamp_field = Some("event_time".to_owned());
        let sink = QuestDbSink::new(1, config);
        assert!(sink.init_error.is_none());
        assert_eq!(sink.mapping.timestamp_source, TimestampSource::Payload);
        assert_eq!(sink.mapping.timestamp_field.as_deref(), Some("event_time"));
    }

    #[test]
    fn given_unknown_enum_values_when_constructed_should_record_init_error() {
        for (field, value) in [
            ("timestamp_source", "yesterday"),
            ("timestamp_unit", "fortnights"),
            ("ack_level", "maybe"),
        ] {
            let mut config = config();
            match field {
                "timestamp_source" => config.timestamp_source = Some(value.to_owned()),
                "timestamp_unit" => config.timestamp_unit = Some(value.to_owned()),
                _ => config.ack_level = Some(value.to_owned()),
            }
            let sink = QuestDbSink::new(1, config);
            assert!(
                matches!(&sink.init_error, Some(Error::InvalidConfigValue(reason))
                    if reason.contains(field) && reason.contains(value)),
                "{field} should reject {value} and name both, got {:?}",
                sink.init_error
            );
        }
    }

    #[test]
    fn given_each_ack_level_name_when_parsed_should_round_trip_and_convert() {
        for (name, setting, level) in [
            ("ok", AckLevelSetting::Ok, AckLevel::Ok),
            ("durable", AckLevelSetting::Durable, AckLevel::Durable),
            (
                "local_durable",
                AckLevelSetting::LocalDurable,
                AckLevel::LocalDurable,
            ),
        ] {
            assert_eq!(name.parse(), Ok(setting));
            assert_eq!(name.to_ascii_uppercase().parse(), Ok(setting));
            assert_eq!(setting.to_string(), name);
            assert_eq!(AckLevel::from(setting), level);
        }
    }

    #[test]
    fn given_column_in_symbol_and_uuid_lists_when_constructed_should_record_init_error() {
        let mut config = config();
        config.symbol_columns = Some(vec!["id".to_owned()]);
        config.uuid_columns = Some(vec!["id".to_owned()]);
        let sink = QuestDbSink::new(1, config);
        assert!(matches!(
            sink.init_error,
            Some(Error::InvalidConfigValue(_))
        ));
    }

    #[test]
    fn given_minimal_config_when_constructed_should_write_numbers_as_doubles() {
        // The default has to be the one that does not depend on where the flush
        // boundary fell, so a mixed-number producer loses nothing.
        let sink = QuestDbSink::new(1, config());
        assert!(sink.mapping.numbers_as_double);
    }

    #[test]
    fn given_a_column_named_like_a_metadata_column_when_constructed_should_record_init_error() {
        // The connector writes `iggy_stream` itself while the flag is on, so a
        // symbol column of that name would reject every record carrying it.
        // Every reserved name, with the flag that really enables it: the flag
        // names are not derivable from the column names. The two optional
        // metadata columns are turned on so all four names are reserved.
        for (column, flag) in [
            ("Iggy_Stream", "include_stream_column"),
            ("iggy_topic", "include_topic_column"),
            ("iggy_partition_id", "include_partition_column"),
            ("iggy_offset", "include_offset_column"),
        ] {
            let mut listed = config();
            listed.include_partition_column = Some(true);
            listed.include_offset_column = Some(true);
            listed.symbol_columns = Some(vec![column.to_owned()]);
            let sink = QuestDbSink::new(1, listed);
            assert!(
                matches!(&sink.init_error, Some(Error::InvalidConfigValue(reason)) if reason.contains(flag)),
                "{column}: {:?}",
                sink.init_error
            );
        }

        // With the flag off the name is free.
        let mut flag_off = config();
        flag_off.symbol_columns = Some(vec!["iggy_stream".to_owned()]);
        flag_off.include_stream_column = Some(false);
        assert!(QuestDbSink::new(1, flag_off).init_error.is_none());
    }

    #[test]
    fn given_an_over_long_table_name_when_constructed_should_record_init_error() {
        // `TableName::new` checks characters only; the length limit is the
        // same one the column names get, and it fails per row otherwise.
        let mut too_long = config();
        too_long.table = "t".repeat(MAX_NAME_LEN + 1);
        let sink = QuestDbSink::new(1, too_long);
        assert!(
            matches!(&sink.init_error, Some(Error::InvalidConfigValue(reason)) if reason.contains("bytes")),
            "{:?}",
            sink.init_error
        );
        let mut at_limit = config();
        at_limit.table = "t".repeat(MAX_NAME_LEN);
        assert!(QuestDbSink::new(1, at_limit).init_error.is_none());
    }

    #[test]
    fn given_an_acknowledged_watermark_when_confirming_should_count_only_the_windows_it_covers() {
        let windows = [
            PublishedWindow {
                fsn: 4,
                end: 10,
                rows: 9,
            },
            PublishedWindow {
                fsn: 5,
                end: 20,
                rows: 18,
            },
            PublishedWindow {
                fsn: 6,
                end: 25,
                rows: 23,
            },
        ];
        // The watermark counts frames of the whole lease, so it can stop
        // between two windows, or before the first one of this chunk.
        let confirmed = |acked: Option<u64>, windows: &[PublishedWindow]| {
            let mut chunk =
                ChunkWindows::new(AckLevelSetting::Ok, Duration::from_secs(1), AT_MOST_ONCE, 0);
            chunk.windows = windows.to_vec();
            chunk.acked = acked;
            chunk.confirmed()
        };
        assert_eq!(confirmed(None, &windows), (0, 0));
        assert_eq!(confirmed(Some(3), &windows), (0, 0));
        assert_eq!(confirmed(Some(5), &windows), (20, 18));
        assert_eq!(confirmed(Some(9), &windows), (25, 23));
        assert_eq!(confirmed(Some(9), &[]), (0, 0));
    }

    #[test]
    fn given_a_successful_wait_when_the_watermark_lags_should_confirm_every_window() {
        // With `ack_level = "ok"` and a durable `request_durable_ack`, the
        // watermark follows the durable level and can still be behind when the
        // `ok` wait returns. The wait itself covers every window.
        let mut chunk =
            ChunkWindows::new(AckLevelSetting::Ok, Duration::from_secs(1), AT_MOST_ONCE, 0);
        chunk.windows = vec![
            PublishedWindow {
                fsn: 4,
                end: 10,
                rows: 9,
            },
            PublishedWindow {
                fsn: 5,
                end: 20,
                rows: 18,
            },
        ];
        chunk.acked = Some(4);
        chunk.confirm_all();
        assert_eq!(chunk.confirmed(), (20, 18));
    }

    const AT_MOST_ONCE: ChunkMode = ChunkMode {
        id: 1,
        at_least_once: false,
        wait: true,
        log_payload: false,
    };
    const AT_LEAST_ONCE: ChunkMode = ChunkMode {
        id: 1,
        at_least_once: true,
        wait: true,
        log_payload: false,
    };

    fn stop(
        phase: FlushPhase,
        code: ErrorCode,
        messages: usize,
        undelivered_from: usize,
    ) -> ChunkFailure {
        stop_in(AT_MOST_ONCE, phase, code, messages, undelivered_from)
    }

    fn stop_in(
        mode: ChunkMode,
        phase: FlushPhase,
        code: ErrorCode,
        messages: usize,
        undelivered_from: usize,
    ) -> ChunkFailure {
        let messages: Vec<ConsumedMessage> = (0..messages as u64)
            .map(|offset| ConsumedMessage {
                id: u128::from(offset) + 1,
                offset,
                checksum: 0,
                timestamp: 1,
                origin_timestamp: 1,
                headers: None,
                payload: Payload::Text("x".to_owned()),
            })
            .collect();
        *chunk_stop(
            StopCause::Flush(FlushError {
                phase,
                error: questdb::Error::new(code, "failed"),
            }),
            BatchOutcome::default(),
            (undelivered_from, 0),
            undelivered_from,
            &messages,
            mode,
        )
    }

    #[test]
    fn given_a_borrow_failure_when_classified_should_stop_the_batch_by_its_own_code() {
        // Pool exhaustion arrives as InvalidApiCall. It clears itself when
        // another topic returns its sender, so it is transient, but the next
        // chunk would wait the acquire timeout again, so the loop stops. A
        // refused dial is permanent, and must not read as transient just
        // because it happened while borrowing.
        let exhausted = stop(FlushPhase::Borrowing, ErrorCode::InvalidApiCall, 0, 0);
        assert!(exhausted.stops_batch);
        assert!(!exhausted.server_rejection);
        assert!(matches!(exhausted.error, Error::CannotStoreData(_)));

        let refused = stop(FlushPhase::Borrowing, ErrorCode::TlsError, 0, 0);
        assert!(refused.stops_batch);
        assert!(matches!(refused.error, Error::PermanentHttpError(_)));

        let dead_host = stop(FlushPhase::Borrowing, ErrorCode::SocketError, 0, 0);
        assert!(dead_host.stops_batch);
        assert!(matches!(dead_host.error, Error::CannotStoreData(_)));

        // Outside the borrow phase the same code is a caller mistake.
        let misuse = stop(FlushPhase::Publishing, ErrorCode::InvalidApiCall, 0, 0);
        assert!(matches!(misuse.error, Error::PermanentHttpError(_)));
    }

    #[test]
    fn given_a_server_rejection_when_classified_should_continue_and_flag_the_token() {
        // The rejection belongs to the frame, so the chunks behind it are
        // still worth attempting, and the handler counted the same event.
        for phase in [FlushPhase::Publishing, FlushPhase::Awaiting] {
            let failure = stop(phase, ErrorCode::ServerRejection, 0, 0);
            assert!(!failure.stops_batch, "{phase:?}");
            assert!(failure.server_rejection, "{phase:?}");
            assert!(matches!(failure.error, Error::PermanentHttpError(_)));
        }
    }

    #[test]
    fn given_a_transient_failure_when_classified_should_stop_the_batch() {
        // A connection fault recurs on the next chunk. In the awaiting phase
        // it is still a connection fault, but never reported as retryable,
        // because the frame is already published.
        let publishing = stop(FlushPhase::Publishing, ErrorCode::SocketError, 0, 0);
        assert!(publishing.stops_batch);
        assert!(!publishing.server_rejection);
        assert!(matches!(publishing.error, Error::CannotStoreData(_)));

        let awaiting = stop(FlushPhase::Awaiting, ErrorCode::SocketError, 0, 0);
        assert!(awaiting.stops_batch);
        assert!(matches!(awaiting.error, Error::PermanentHttpError(_)));
    }

    #[test]
    fn given_at_least_once_when_a_chunk_fails_should_let_the_runtime_retry() {
        // An acknowledgement that did not arrive, or a connection that failed
        // after the frames were published, is worth the runtime's retry of the
        // whole batch.
        for (phase, code) in [
            (FlushPhase::Awaiting, ErrorCode::FailoverRetry),
            (FlushPhase::Awaiting, ErrorCode::SocketError),
            (FlushPhase::Publishing, ErrorCode::SymbolDictFull),
        ] {
            let failure = stop_in(AT_LEAST_ONCE, phase, code, 0, 0);
            assert!(
                matches!(failure.error, Error::CannotStoreData(_)),
                "{phase:?} {code:?}: {:?}",
                failure.error
            );
        }
        // A server rejection is the same frame again on every retry, so it
        // stays permanent and the runtime stops the delivery at it.
        let rejected = stop_in(
            AT_LEAST_ONCE,
            FlushPhase::Awaiting,
            ErrorCode::ServerRejection,
            0,
            0,
        );
        assert!(rejected.server_rejection);
        assert!(rejected.error.is_permanent());
    }

    #[test]
    fn given_at_least_once_when_a_record_is_refused_should_fail_the_call_permanently() {
        // The runtime stops the delivery at a permanent error, so the record
        // stays in the stream instead of being skipped. Its stop reason names
        // the first offset of the run, so the error names the record.
        let messages: Vec<ConsumedMessage> = (40..43u64)
            .map(|offset| ConsumedMessage {
                id: u128::from(offset),
                offset,
                checksum: 0,
                timestamp: 1,
                origin_timestamp: 1,
                headers: None,
                payload: Payload::Text("x".to_owned()),
            })
            .collect();
        let failure = chunk_stop(
            StopCause::Rejected("column a is claimed twice".to_owned()),
            BatchOutcome::default(),
            (1, 1),
            1,
            &messages,
            AT_LEAST_ONCE,
        );
        assert!(!failure.server_rejection);
        assert!(
            matches!(&failure.error, Error::InvalidRecordValue(reason)
                if reason.contains("claimed twice") && reason.contains("offset 41")),
            "{:?}",
            failure.error
        );
        assert!(failure.error.is_permanent());
    }

    #[test]
    fn given_a_rejection_of_an_earlier_lease_when_classified_should_let_the_runtime_retry() {
        // The frame belongs to another chunk, and the rows of this one are
        // valid, so a permanent error would stop the delivery of a healthy topic.
        let failure = chunk_stop(
            StopCause::Inherited(FlushError {
                phase: FlushPhase::Awaiting,
                error: questdb::Error::new(ErrorCode::ServerRejection, "schema mismatch"),
            }),
            BatchOutcome::default(),
            (0, 0),
            0,
            &[],
            AT_LEAST_ONCE,
        );
        assert!(
            matches!(&failure.error, Error::CannotStoreData(reason) if reason.contains("earlier chunk")),
            "{:?}",
            failure.error
        );
        // The handler counted the same rejection, so the token is retired.
        assert!(failure.server_rejection);
    }

    #[test]
    fn given_a_rejected_span_when_compared_with_a_lease_should_tell_an_earlier_frame() {
        // Frame numbers grow on a connection, and a lease starts after every
        // frame published before it. A protocol violation spans from the
        // oldest unacknowledged frame, so a span that starts at 9 and ends
        // inside the lease still names an earlier frame.
        assert!(rejects_an_earlier_lease(Some(10), 9));
        assert!(!rejects_an_earlier_lease(Some(10), 10));
        assert!(!rejects_an_earlier_lease(Some(10), 12));
        // A lease that published nothing owns no frame.
        assert!(rejects_an_earlier_lease(None, 0));
    }

    #[test]
    fn given_fire_and_forget_when_enabling_at_least_once_should_refuse() {
        let mut config = config();
        config.flush_timeout = Some("0s".to_owned());
        let mut sink = QuestDbSink::new(1, config);
        assert!(matches!(
            sink.enable_at_least_once(),
            Err(Error::Unsupported(_))
        ));
        assert!(!sink.at_least_once);

        let mut sink = QuestDbSink::new(1, self::config());
        sink.enable_at_least_once()
            .expect("a sink that waits for its acknowledgements delivers at least once");
        assert!(sink.at_least_once);
    }

    #[test]
    fn given_server_timestamps_when_enabling_at_least_once_should_accept() {
        // Each retried copy gets a new timestamp, so DEDUP cannot remove it,
        // but duplicates are what at least once allows. The sink warns.
        let mut config = config();
        config.timestamp_source = Some("server".to_owned());
        let mut sink = QuestDbSink::new(1, config);
        sink.enable_at_least_once()
            .expect("server timestamps still deliver at least once");
        assert!(sink.at_least_once);
    }

    #[test]
    fn given_a_publish_failure_when_deciding_should_skip_only_a_frame_level_window_at_most_once() {
        let failure = |code| FlushError {
            phase: FlushPhase::Publishing,
            error: questdb::Error::new(code, "failed"),
        };
        // A frame that the client refuses on its own does not end the chunk.
        assert!(skip_window(
            &failure(ErrorCode::InvalidApiCall),
            AT_MOST_ONCE
        ));
        // A rejection latches the connection, and a transient code belongs to
        // the connection, so the chunk stops.
        assert!(!skip_window(
            &failure(ErrorCode::ServerRejection),
            AT_MOST_ONCE
        ));
        assert!(!skip_window(&failure(ErrorCode::SocketError), AT_MOST_ONCE));
        // At least once, the call fails, and the runtime delivers the batch
        // again.
        assert!(!skip_window(
            &failure(ErrorCode::InvalidApiCall),
            AT_LEAST_ONCE
        ));
    }

    #[test]
    fn given_a_full_symbol_dictionary_when_classified_should_not_stop_the_batch() {
        // The dictionary belongs to the connection, but returning the sender
        // retires that connection and the next borrow dials a fresh one, so
        // the chunks behind this one are worth attempting.
        let full = stop(FlushPhase::Publishing, ErrorCode::SymbolDictFull, 0, 0);
        assert!(!full.stops_batch);
        assert!(!full.server_rejection);
        assert!(matches!(full.error, Error::CannotStoreData(_)));
    }

    #[test]
    fn given_a_debit_before_its_credit_when_settled_should_net_to_zero() {
        // The handler credits on the client's thread and the failed chunk
        // debits on the Tokio thread, in no fixed order. A debit that lands
        // first must not make the pending credit look like a token.
        let balance = AtomicI64::new(0);
        let drops = AtomicU64::new(0);
        balance.fetch_sub(1, Ordering::Relaxed);
        assert_eq!(
            settle_rejections(&balance, &drops, 0),
            None,
            "a pending credit must not be mistaken for a token"
        );
        balance.fetch_add(1, Ordering::Relaxed);
        assert_eq!(settle_rejections(&balance, &drops, 0), None);
        assert_eq!(balance.load(Ordering::Relaxed), 0);
    }

    #[test]
    fn given_another_batch_takes_the_credit_before_the_debit_when_settled_should_leave_a_debt() {
        // The documented residual case: another topic's batch settles the
        // credit as an unclaimed rejection before the failed chunk debits it,
        // so the rejection is reported twice and the balance stays at -1,
        // where it absorbs the next handler-only rejection. That rejection
        // then fails no batch, though the handler still logs it. Pinned so
        // that a change to this behaviour is a deliberate one.
        let balance = AtomicI64::new(0);
        let drops = AtomicU64::new(0);
        balance.fetch_add(1, Ordering::Relaxed);
        assert_eq!(
            settle_rejections(&balance, &drops, 0),
            Some(Unclaimed::Rejection)
        );
        balance.fetch_sub(1, Ordering::Relaxed);
        assert_eq!(balance.load(Ordering::Relaxed), -1);
        balance.fetch_add(1, Ordering::Relaxed);
        assert_eq!(
            settle_rejections(&balance, &drops, 0),
            None,
            "the debt absorbs the next handler-only rejection"
        );
        assert_eq!(balance.load(Ordering::Relaxed), 0);
    }

    #[test]
    fn given_an_unretired_credit_when_settled_should_report_one_rejection_once() {
        let balance = AtomicI64::new(0);
        let drops = AtomicU64::new(0);
        balance.fetch_add(1, Ordering::Relaxed);
        assert_eq!(
            settle_rejections(&balance, &drops, 0),
            Some(Unclaimed::Rejection)
        );
        assert_eq!(settle_rejections(&balance, &drops, 0), None);
    }

    #[test]
    fn given_an_older_drop_count_when_settled_late_should_not_report_the_drop_again() {
        // Two batches read the client's count at different moments. The one
        // that read the older count settles last, and must not move the
        // count back, or the next batch reports the same drop once more.
        let balance = AtomicI64::new(0);
        let drops = AtomicU64::new(0);
        assert_eq!(
            settle_rejections(&balance, &drops, 2),
            Some(Unclaimed::EventsDropped(2))
        );
        assert_eq!(settle_rejections(&balance, &drops, 1), None);
        assert_eq!(settle_rejections(&balance, &drops, 2), None);
    }

    #[test]
    fn given_dropped_events_when_settled_should_forgive_the_debt_and_report_the_drop() {
        // A dropped terminal event can never repay a debit, so the debt would
        // absorb the next handler-only rejection. Forgiving it is gated on the
        // drop so a merely late credit is not erased.
        let balance = AtomicI64::new(0);
        let drops = AtomicU64::new(0);
        balance.fetch_sub(1, Ordering::Relaxed);
        assert_eq!(
            settle_rejections(&balance, &drops, 1),
            Some(Unclaimed::EventsDropped(1))
        );
        assert_eq!(balance.load(Ordering::Relaxed), 0);
        // The same drop count is not reported twice.
        assert_eq!(settle_rejections(&balance, &drops, 1), None);
        // The next handler-only rejection is reported rather than masked.
        balance.fetch_add(1, Ordering::Relaxed);
        assert_eq!(
            settle_rejections(&balance, &drops, 1),
            Some(Unclaimed::Rejection)
        );
    }

    #[test]
    fn given_a_stopped_chunk_when_classified_should_hand_back_the_unconfirmed_tail() {
        // Messages before the last acknowledged window were written or
        // individually logged; from there on, nothing is known to have landed.
        let failure = stop(FlushPhase::Publishing, ErrorCode::ServerRejection, 5, 2);
        assert_eq!(failure.attempted, 2);
        let range = failure.undelivered.expect("three messages are unconfirmed");
        assert_eq!(
            (range.count, range.first_offset, range.last_offset),
            (3, 2, 4)
        );
        assert_eq!((range.first_id, range.last_id), (3, 5));
    }

    #[test]
    fn given_a_stopped_chunk_when_classified_should_keep_only_the_acknowledged_rows() {
        // `rows_written` grows at append time, so the rows of an unconfirmed
        // window come out again when the chunk stops, and those messages are
        // the ones named as undelivered.
        let messages: Vec<ConsumedMessage> = (0..6u64)
            .map(|offset| ConsumedMessage {
                id: u128::from(offset) + 1,
                offset,
                checksum: 0,
                timestamp: 1,
                origin_timestamp: 1,
                headers: None,
                payload: Payload::Text("x".to_owned()),
            })
            .collect();
        let outcome = BatchOutcome {
            rows_written: 7,
            rejected_rows: 2,
            ..BatchOutcome::default()
        };
        let stop = chunk_stop(
            StopCause::Flush(FlushError {
                phase: FlushPhase::Publishing,
                error: questdb::Error::new(ErrorCode::SocketError, "reset"),
            }),
            outcome,
            (4, 4),
            6,
            &messages,
            AT_MOST_ONCE,
        );
        assert_eq!(stop.outcome.rows_written, 4);
        assert_eq!(stop.outcome.rejected_rows, 2);
        let range = stop.undelivered.expect("two messages are unconfirmed");
        assert_eq!(
            (range.count, range.first_offset, range.last_offset),
            (2, 4, 5)
        );
    }

    #[test]
    fn given_a_column_in_two_type_lists_when_constructed_should_record_init_error() {
        // A column has one type, so overlapping declarations are a config error
        // rather than a per-record failure forever.
        for (first, second) in [
            ("symbol_columns", "uuid_columns"),
            ("symbol_columns", "integer_columns"),
            ("uuid_columns", "integer_columns"),
        ] {
            let mut config = config();
            for name in [first, second] {
                let list = Some(vec!["trade_id".to_owned()]);
                match name {
                    "symbol_columns" => config.symbol_columns = list,
                    "uuid_columns" => config.uuid_columns = list,
                    _ => config.integer_columns = list,
                }
            }
            let sink = QuestDbSink::new(1, config);
            assert!(
                matches!(sink.init_error, Some(Error::InvalidConfigValue(_))),
                "{first} and {second} must not be allowed to overlap"
            );
        }
    }

    #[test]
    fn given_timestamp_field_also_listed_as_symbol_when_constructed_should_record_init_error() {
        let mut config = config();
        config.timestamp_source = Some("payload".to_owned());
        config.timestamp_field = Some("event_time".to_owned());
        config.symbol_columns = Some(vec!["event_time".to_owned()]);
        let sink = QuestDbSink::new(1, config);
        assert!(matches!(
            sink.init_error,
            Some(Error::InvalidConfigValue(_))
        ));
    }

    #[test]
    fn given_no_max_flush_bytes_when_constructed_should_use_the_frame_safe_default() {
        let sink = QuestDbSink::new(1, config());
        assert_eq!(sink.max_flush_bytes, DEFAULT_MAX_FLUSH_BYTES);
    }

    #[test]
    fn given_zero_sizes_when_constructed_should_record_init_error() {
        // A zero batch size would make the chunking loop take nothing on every
        // pass, and a zero byte bound would flush after every row. Both are
        // refused at start, as the other settings that cannot work are.
        let mut zero_batch = config();
        zero_batch.batch_size = Some(0);
        let mut zero_bytes = config();
        zero_bytes.max_flush_bytes = Some(0);
        for (field, config) in [("batch_size", zero_batch), ("max_flush_bytes", zero_bytes)] {
            let sink = QuestDbSink::new(1, config);
            assert!(
                matches!(&sink.init_error, Some(Error::InvalidConfigValue(reason)) if reason.contains(field)),
                "{field}: got {:?}",
                sink.init_error
            );
        }
    }

    #[test]
    fn given_explicit_max_flush_bytes_when_constructed_should_honour_it() {
        let mut config = config();
        config.max_flush_bytes = Some(4096);
        let sink = QuestDbSink::new(1, config);
        assert_eq!(sink.max_flush_bytes, 4096);
    }

    #[test]
    fn given_invalid_flush_timeout_when_constructed_should_record_init_error() {
        // "500" has no unit. A silent fallback would turn it into 30s.
        for value in ["not-a-duration", "500"] {
            let mut config = config();
            config.flush_timeout = Some(value.to_owned());
            let sink = QuestDbSink::new(1, config);
            assert!(
                matches!(&sink.init_error, Some(Error::InvalidConfigValue(reason))
                    if reason.contains("flush_timeout") && reason.contains(value)),
                "flush_timeout should reject {value} and name both, got {:?}",
                sink.init_error
            );
        }
    }

    #[test]
    fn given_toml_config_when_deserialized_should_populate_fields() {
        let raw = r#"
            connection_string = "ws::addr=questdb:9000;"
            table = "trades"
            timestamp_source = "payload"
            timestamp_field = "ts"
            symbol_columns = ["symbol", "side"]
            integer_columns = ["trade_id"]
            numbers_as_double = false
            batch_size = 500
        "#;
        let config: QuestDbSinkConfig = toml::from_str(raw).unwrap();
        let sink = QuestDbSink::new(7, config);
        assert!(sink.init_error.is_none());
        assert_eq!(sink.batch_size, 500);
        assert_eq!(sink.mapping.table, "trades");
        assert!(sink.mapping.symbol_columns.contains("side"));
        assert!(sink.mapping.integer_columns.contains("trade_id"));
        assert!(!sink.mapping.numbers_as_double);
    }

    #[test]
    fn given_an_unknown_key_when_deserialized_should_refuse_the_config() {
        let config = serde_json::json!({
            "connection_string": "ws::addr=localhost:9000;",
            "table": "iggy_events",
            "integer_column": ["trade_id"],
        });
        let error = serde_json::from_value::<QuestDbSinkConfig>(config)
            .expect_err("a misspelled key must not leave its default in force");
        assert!(
            error.to_string().contains("integer_column"),
            "the error must name the key: {error}"
        );
    }

    #[test]
    fn given_retryable_code_when_not_in_doubt_should_be_retryable() {
        assert!(is_retryable(ErrorCode::FailoverRetry, false));
        assert!(is_retryable(ErrorCode::SocketError, false));
    }

    #[test]
    fn given_retryable_code_when_in_doubt_should_not_be_retryable() {
        // The client documents that `FailoverRetry` in particular can carry
        // `in_doubt`, so the code alone must not authorise a re-send.
        assert!(!is_retryable(ErrorCode::FailoverRetry, true));
        assert!(!is_retryable(ErrorCode::SocketError, true));
    }

    #[test]
    fn given_server_rejection_when_classified_should_not_be_retryable() {
        assert!(!is_retryable(ErrorCode::ServerSchemaMismatch, false));
        assert!(!is_retryable(ErrorCode::ServerSecurityError, false));
        assert!(!is_retryable(ErrorCode::ServerParseError, false));
    }

    #[test]
    fn given_ack_timeout_when_classified_should_be_pending_not_failed() {
        // The frames are already published and delivery continues, so this
        // must not be reported as a lost batch.
        let error = questdb::Error::new(
            ErrorCode::FailoverRetry,
            "wait(ok) timed out with no ack progress",
        );
        assert!(is_pending_ack(&error));
    }

    #[test]
    fn given_terminal_rejection_when_classified_should_not_be_pending() {
        for code in [
            ErrorCode::ServerSchemaMismatch,
            ErrorCode::ServerSecurityError,
            ErrorCode::ServerParseError,
        ] {
            let error = questdb::Error::new(code, "rejected");
            assert!(!is_pending_ack(&error), "{code:?} must stay terminal");
        }
    }

    #[test]
    fn given_ack_wait_failure_when_mapped_should_be_permanent() {
        // The rows are already published by the time `wait` runs, so even a
        // retryable code must not produce a retryable sink error: re-sending
        // would duplicate rows QuestDB may already hold. This is the
        // durable-ACK stall against a server without replication.
        let failure = FlushError {
            phase: FlushPhase::Awaiting,
            error: questdb::Error::new(
                ErrorCode::FailoverRetry,
                "wait(durable) timed out with no ack progress",
            ),
        };
        assert!(matches!(
            map_client_error(AT_MOST_ONCE, failure),
            Error::PermanentHttpError(_)
        ));
    }

    #[test]
    fn given_publish_failure_when_mapped_should_stay_retryable() {
        let failure = FlushError {
            phase: FlushPhase::Publishing,
            error: questdb::Error::new(ErrorCode::SocketError, "connection reset"),
        };
        assert!(matches!(
            map_client_error(AT_MOST_ONCE, failure),
            Error::CannotStoreData(_)
        ));
    }

    #[test]
    fn given_server_rejection_when_mapped_should_be_permanent() {
        // A schema rejection reaches a sender as `ServerRejection`, not as the
        // query path's `ServerSchemaMismatch`, so there is nothing to map it to
        // `Error::SchemaMismatch` from and every rejection is permanent.
        for code in [ErrorCode::ServerRejection, ErrorCode::ServerSchemaMismatch] {
            let failure = FlushError {
                phase: FlushPhase::Publishing,
                error: questdb::Error::new(code, "rejected"),
            };
            assert!(
                matches!(
                    map_client_error(AT_MOST_ONCE, failure),
                    Error::PermanentHttpError(_)
                ),
                "{code:?} should map to a permanent error"
            );
        }
    }

    #[test]
    fn given_debug_output_when_formatted_should_not_leak_connection_string() {
        let mut config = config();
        config.connection_string =
            SecretString::from("ws::addr=questdb:9000;username=admin;password=hunter2;");
        let sink = QuestDbSink::new(1, config);
        let rendered = format!("{sink:?}");
        assert!(!rendered.contains("hunter2"), "{rendered}");
    }
}
