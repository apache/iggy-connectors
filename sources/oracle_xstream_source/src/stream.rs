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

use std::collections::HashSet;
use std::time::{Duration, Instant};

use thiserror::Error;

pub const ORA_STALE_SESSION: i32 = 26812;
pub const ORA_POSITION_BELOW_LOW_POSITION: i32 = 21560;

/// One decoded column value, owned so that no OCI memory escapes the `oci` module.
#[derive(Debug, Clone, PartialEq)]
pub enum Value {
    Null,
    Text(String),
    Number(String),
    Float(f64),
    Binary(Vec<u8>),
    /// ISO 8601 text, with an offset when the column carries a time zone.
    Timestamp(String),
    /// A column type the connector does not decode (LOB, LONG, XMLType, INTERVAL and others).
    Unsupported,
}

#[derive(Debug, Clone, PartialEq)]
pub struct Column {
    pub name: String,
    pub value: Value,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Operation {
    Insert,
    Update,
    Delete,
}

impl Operation {
    pub fn as_str(self) -> &'static str {
        match self {
            Operation::Insert => "insert",
            Operation::Update => "update",
            Operation::Delete => "delete",
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct RowChange {
    /// `OWNER.TABLE`
    pub table: String,
    pub operation: Operation,
    pub transaction_id: String,
    pub position: Vec<u8>,
    pub source_time: Option<String>,
    pub old: Vec<Column>,
    pub new: Vec<Column>,
    /// Row number inside its transaction, assigned by [`drain_batch`].
    pub ordinal: u32,
}

#[derive(Debug, Clone, PartialEq)]
pub struct DdlChange {
    /// `OWNER.OBJECT`
    pub table: String,
    /// Oracle's command name, for example `ALTER TABLE`.
    pub command: String,
    pub statement: String,
    pub transaction_id: String,
    pub position: Vec<u8>,
    pub source_time: Option<String>,
    /// Number inside its transaction, shared with the row changes and assigned by [`drain_batch`].
    pub ordinal: u32,
}

#[derive(Debug, Clone, PartialEq)]
pub enum Change {
    Row(RowChange),
    Ddl(DdlChange),
}

#[derive(Debug, Clone, PartialEq)]
pub enum Received {
    /// The receive returned without an LCR. Never proof that the stream is caught up.
    Idle,
    Row(RowChange),
    Commit {
        position: Vec<u8>,
    },
    Ddl(DdlChange),
}

#[derive(Debug, Error)]
pub enum StreamError {
    #[error("Oracle client library: {0}")]
    Library(String),
    #[error("{operation} failed: ORA-{code:05}: {message}")]
    Oci {
        operation: &'static str,
        code: i32,
        message: String,
    },
    #[error("unexpected LCR: {0}")]
    Protocol(String),
}

impl StreamError {
    pub fn ora_code(&self) -> Option<i32> {
        match self {
            StreamError::Oci { code, .. } => Some(*code),
            _ => None,
        }
    }

    pub fn is_stale_session(&self) -> bool {
        self.ora_code() == Some(ORA_STALE_SESSION)
    }

    pub fn is_position_below_low_position(&self) -> bool {
        self.ora_code() == Some(ORA_POSITION_BELOW_LOW_POSITION)
    }
}

/// The seam between the connector and OCI, so tests can replace the database.
pub trait LcrStream: Send {
    fn receive(&mut self) -> Result<Received, StreamError>;
    fn set_processed_low_position(&mut self, position: &[u8]) -> Result<(), StreamError>;
    /// Ordinal of the next change in the transaction being read. It lives with the attachment, not with a batch:
    /// a batch can end mid-transaction, and a new attach replays that transaction from its start.
    fn next_ordinal(&mut self) -> &mut u32;
    /// Receives until the stream is idle, then detaches. A receive that last returned an LCR is still
    /// active, and Oracle refuses to detach it (ORA-03127).
    fn detach(&mut self) -> Result<(), StreamError>;
}

#[derive(Debug, Clone, Copy)]
pub struct Limits {
    pub max_events: usize,
    pub budget: Duration,
}

#[derive(Debug, Default, PartialEq)]
pub struct Drained {
    pub changes: Vec<Change>,
    /// Position of the last commit LCR seen. Rows after it belong to a transaction still in progress.
    pub last_commit: Option<Vec<u8>>,
    /// Set when a forwarded row has a column the connector cannot decode. `changes` then ends at `last_commit`, so
    /// the commits read before the row can still be delivered and acknowledged.
    pub blocked: Option<UnsupportedColumn>,
}

#[derive(Debug, Clone, PartialEq, Error)]
#[error("unsupported column type in {table}.{column}")]
pub struct UnsupportedColumn {
    pub table: String,
    pub column: String,
}

/// DDL that changes neither the shape of a row nor which rows exist. Anything else, including statements not in
/// this list, is forwarded so that nothing is dropped silently.
const HARMLESS_DDL: [&str; 3] = ["COMMENT", "CREATE INDEX", "GRANT OBJECT"];
const SCHEMA_WILDCARD_SUFFIX: &str = ".*";

/// `OWNER.TABLE` entries, plus `OWNER.*` entries that also cover tables created later. An empty filter allows all.
#[derive(Debug, Default)]
pub struct TableFilter {
    tables: HashSet<String>,
    schemas: HashSet<String>,
}

impl TableFilter {
    pub fn new(entries: &[String]) -> Self {
        let mut filter = Self::default();
        for entry in entries.iter().map(|entry| entry.to_uppercase()) {
            match entry.strip_suffix(SCHEMA_WILDCARD_SUFFIX) {
                Some(owner) => filter.schemas.insert(owner.to_owned()),
                None => filter.tables.insert(entry),
            };
        }
        filter
    }

    pub fn allows(&self, table: &str) -> bool {
        if self.tables.is_empty() && self.schemas.is_empty() {
            return true;
        }
        let table = table.to_uppercase();
        self.tables.contains(&table)
            || table
                .split_once('.')
                .is_some_and(|(owner, _)| self.schemas.contains(owner))
    }
}

/// Reads LCRs until the batch is full, the stream is idle, or a row has an unsupported column.
/// A batch that fills mid-transaction ends there: its `last_commit` stays at the previous commit, so
/// acknowledging it never moves the processed low position into the open transaction.
pub fn drain_batch(
    stream: &mut dyn LcrStream,
    limits: Limits,
    filter: &TableFilter,
) -> Result<Drained, StreamError> {
    let deadline = Instant::now() + limits.budget;
    let mut drained = Drained::default();
    let mut transaction_open = false;
    let mut committed = 0;
    while drained.changes.len() < limits.max_events
        && (transaction_open || Instant::now() < deadline)
    {
        match stream.receive()? {
            Received::Idle => break,
            Received::Row(mut row) => {
                transaction_open = true;
                if !filter.allows(&row.table) {
                    continue;
                }
                if let Some(unsupported) = unsupported_column(&row) {
                    drained.changes.truncate(committed);
                    drained.blocked = Some(unsupported);
                    break;
                }
                let ordinal = stream.next_ordinal();
                row.ordinal = *ordinal;
                *ordinal += 1;
                drained.changes.push(Change::Row(row));
            }
            Received::Commit { position } => {
                transaction_open = false;
                *stream.next_ordinal() = 0;
                drained.last_commit = Some(position);
                committed = drained.changes.len();
            }
            Received::Ddl(mut ddl) => {
                transaction_open = true;
                if !filter.allows(&ddl.table) || HARMLESS_DDL.contains(&ddl.command.as_str()) {
                    continue;
                }
                let ordinal = stream.next_ordinal();
                ddl.ordinal = *ordinal;
                *ordinal += 1;
                drained.changes.push(Change::Ddl(ddl));
            }
        }
    }
    Ok(drained)
}

fn unsupported_column(row: &RowChange) -> Option<UnsupportedColumn> {
    row.old
        .iter()
        .chain(&row.new)
        .find(|column| column.value == Value::Unsupported)
        .map(|column| UnsupportedColumn {
            table: row.table.clone(),
            column: column.name.clone(),
        })
}

#[cfg(test)]
pub(crate) mod testing {
    use std::collections::VecDeque;
    use std::sync::{Arc, Mutex};

    use super::*;

    pub type ProcessedPositions = Arc<Mutex<Vec<Vec<u8>>>>;

    /// Plays back a fixed list of receives, then reports idle forever.
    pub struct ScriptedStream {
        script: VecDeque<Result<Received, StreamError>>,
        processed: ProcessedPositions,
        next_ordinal: u32,
    }

    impl ScriptedStream {
        pub fn new(script: Vec<Result<Received, StreamError>>) -> (Self, ProcessedPositions) {
            let processed = ProcessedPositions::default();
            let stream = Self {
                script: script.into(),
                processed: Arc::clone(&processed),
                next_ordinal: 0,
            };
            (stream, processed)
        }
    }

    impl LcrStream for ScriptedStream {
        fn receive(&mut self) -> Result<Received, StreamError> {
            self.script.pop_front().unwrap_or(Ok(Received::Idle))
        }

        fn set_processed_low_position(&mut self, position: &[u8]) -> Result<(), StreamError> {
            self.processed.lock().expect("lock").push(position.to_vec());
            Ok(())
        }

        fn next_ordinal(&mut self) -> &mut u32 {
            &mut self.next_ordinal
        }

        fn detach(&mut self) -> Result<(), StreamError> {
            Ok(())
        }
    }

    pub fn column(name: &str, value: Value) -> Column {
        Column {
            name: name.to_owned(),
            value,
        }
    }

    pub fn insert(table: &str, transaction_id: &str, id: i64) -> Received {
        Received::Row(RowChange {
            table: table.to_owned(),
            operation: Operation::Insert,
            transaction_id: transaction_id.to_owned(),
            position: vec![9, id as u8],
            source_time: Some("2026-11-25T10:11:12".to_owned()),
            old: Vec::new(),
            new: vec![column("ID", Value::Number(id.to_string()))],
            ordinal: 0,
        })
    }

    pub fn ddl(table: &str, command: &str, transaction_id: &str) -> Received {
        Received::Ddl(DdlChange {
            table: table.to_owned(),
            command: command.to_owned(),
            statement: format!("{command} {table}"),
            transaction_id: transaction_id.to_owned(),
            position: vec![8],
            source_time: Some("2026-11-25T10:11:12".to_owned()),
            ordinal: 0,
        })
    }

    pub fn commit(position: u8) -> Received {
        Received::Commit {
            position: vec![position],
        }
    }
}

#[cfg(test)]
mod tests {
    use super::testing::{ScriptedStream, column, commit, ddl, insert};
    use super::*;

    const LONG_BUDGET: Duration = Duration::from_secs(30);

    fn limits(max_events: usize) -> Limits {
        Limits {
            max_events,
            budget: LONG_BUDGET,
        }
    }

    fn drain(
        script: Vec<Received>,
        max_events: usize,
        filter: &TableFilter,
    ) -> Result<Drained, StreamError> {
        let (mut stream, _) = ScriptedStream::new(script.into_iter().map(Ok).collect());
        drain_batch(&mut stream, limits(max_events), filter)
    }

    #[test]
    fn given_two_transactions_when_draining_should_restart_ordinals() {
        let drained = drain(
            vec![
                insert("APP.T", "1.1.1", 1),
                insert("APP.T", "1.1.1", 2),
                commit(1),
                insert("APP.T", "2.2.2", 3),
                commit(2),
            ],
            100,
            &TableFilter::default(),
        )
        .expect("drain");
        let ordinals: Vec<u32> = drained
            .changes
            .iter()
            .map(|change| match change {
                Change::Row(row) => row.ordinal,
                Change::Ddl(ddl) => ddl.ordinal,
            })
            .collect();
        assert_eq!(ordinals, vec![0, 1, 0]);
        assert_eq!(drained.last_commit, Some(vec![2]));
    }

    #[test]
    fn given_transaction_split_across_batches_when_draining_should_keep_ordinals_unique() {
        let (mut stream, _) = ScriptedStream::new(
            vec![
                insert("APP.T", "1.1.1", 1),
                insert("APP.T", "1.1.1", 2),
                insert("APP.T", "1.1.1", 3),
                commit(1),
            ]
            .into_iter()
            .map(Ok)
            .collect(),
        );
        let filter = TableFilter::default();
        let first = drain_batch(&mut stream, limits(2), &filter).expect("first batch");
        let second = drain_batch(&mut stream, limits(2), &filter).expect("second batch");
        let keys: Vec<(String, u32)> = first
            .changes
            .iter()
            .chain(&second.changes)
            .map(|change| match change {
                Change::Row(row) => (row.transaction_id.clone(), row.ordinal),
                Change::Ddl(ddl) => (ddl.transaction_id.clone(), ddl.ordinal),
            })
            .collect();
        let unique: HashSet<&(String, u32)> = keys.iter().collect();
        assert_eq!(
            unique.len(),
            keys.len(),
            "duplicate (xid, ordinal): {keys:?}"
        );
    }

    #[test]
    fn given_full_batch_mid_transaction_when_draining_should_keep_previous_commit() {
        let drained = drain(
            vec![
                insert("APP.T", "1.1.1", 1),
                commit(1),
                insert("APP.T", "2.2.2", 2),
                insert("APP.T", "2.2.2", 3),
                commit(2),
            ],
            2,
            &TableFilter::default(),
        )
        .expect("drain");
        assert_eq!(drained.changes.len(), 2);
        assert_eq!(drained.last_commit, Some(vec![1]));
    }

    #[test]
    fn given_table_filter_when_draining_should_skip_other_tables_keep_commits() {
        let filter = TableFilter::new(&["app.orders".to_owned()]);
        let drained = drain(
            vec![
                insert("APP.OTHER", "1.1.1", 1),
                commit(1),
                insert("APP.ORDERS", "2.2.2", 2),
                commit(2),
            ],
            100,
            &filter,
        )
        .expect("drain");
        assert_eq!(drained.changes.len(), 1);
        assert!(matches!(&drained.changes[0], Change::Row(row) if row.table == "APP.ORDERS"));
        assert_eq!(drained.last_commit, Some(vec![2]));
    }

    #[test]
    fn given_shape_changing_ddl_when_draining_should_forward_it_in_order() {
        let drained = drain(
            vec![
                insert("APP.T", "1.1.1", 1),
                commit(1),
                ddl("APP.T", "ALTER TABLE", "2.2.2"),
                commit(2),
                ddl("APP.NEW_T", "CREATE TABLE", "3.3.3"),
                commit(3),
                insert("APP.NEW_T", "4.4.4", 1),
                commit(4),
            ],
            100,
            &TableFilter::default(),
        )
        .expect("drain");
        let kinds: Vec<&str> = drained
            .changes
            .iter()
            .map(|change| match change {
                Change::Row(_) => "row",
                Change::Ddl(ddl) => ddl.command.as_str(),
            })
            .collect();
        assert_eq!(kinds, vec!["row", "ALTER TABLE", "CREATE TABLE", "row"]);
        assert_eq!(drained.last_commit, Some(vec![4]));
    }

    #[test]
    fn given_destructive_or_unknown_ddl_when_draining_should_forward_it() {
        let drained = drain(
            vec![
                ddl("APP.T", "TRUNCATE TABLE", "1.1.1"),
                commit(1),
                ddl("APP.T", "DROP TABLE", "2.2.2"),
                commit(2),
                ddl("APP.T", "SOMETHING NEW", "3.3.3"),
                commit(3),
            ],
            100,
            &TableFilter::default(),
        )
        .expect("drain");
        assert_eq!(drained.changes.len(), 3);
        assert_eq!(drained.last_commit, Some(vec![3]));
    }

    #[test]
    fn given_harmless_ddl_when_draining_should_skip_it_and_advance() {
        let drained = drain(
            vec![
                ddl("APP.T", "COMMENT", "1.1.1"),
                commit(1),
                ddl("APP.T", "GRANT OBJECT", "2.2.2"),
                commit(2),
                ddl("APP.T", "CREATE INDEX", "3.3.3"),
                commit(3),
            ],
            100,
            &TableFilter::default(),
        )
        .expect("drain");
        assert!(drained.changes.is_empty());
        assert_eq!(drained.last_commit, Some(vec![3]));
    }

    #[test]
    fn given_ddl_on_a_filtered_out_table_when_draining_should_ignore_it() {
        let filter = TableFilter::new(&["APP.ORDERS".to_owned()]);
        let drained = drain(
            vec![ddl("APP.OTHER", "ALTER TABLE", "1.1.1"), commit(1)],
            100,
            &filter,
        )
        .expect("drain");
        assert!(drained.changes.is_empty());
        assert_eq!(drained.last_commit, Some(vec![1]));
    }

    #[test]
    fn given_schema_wildcard_when_filtering_should_cover_tables_created_later() {
        let filter = TableFilter::new(&["app.*".to_owned(), "hr.people".to_owned()]);
        assert!(filter.allows("APP.NEW_TABLE"));
        assert!(filter.allows("HR.PEOPLE"));
        assert!(!filter.allows("HR.PAYROLL"));
        assert!(!filter.allows("OTHER.T"));
    }

    #[test]
    fn given_unsupported_column_when_draining_should_name_table_and_column() {
        let mut row = insert("APP.DOCS", "1.1.1", 1);
        if let Received::Row(change) = &mut row {
            change.new.push(column("BODY", Value::Unsupported));
        }
        let drained = drain(vec![row], 100, &TableFilter::default()).expect("drain");
        assert_eq!(
            drained.blocked,
            Some(UnsupportedColumn {
                table: "APP.DOCS".to_owned(),
                column: "BODY".to_owned(),
            })
        );
    }

    #[test]
    fn given_unsupported_column_after_a_commit_when_draining_should_keep_only_the_committed_changes()
     {
        let mut bad = insert("APP.DOCS", "2.2.2", 3);
        if let Received::Row(change) = &mut bad {
            change.new.push(column("BODY", Value::Unsupported));
        }
        let script = vec![
            insert("APP.T", "1.1.1", 1),
            commit(7),
            insert("APP.T", "2.2.2", 2),
            bad,
            insert("APP.T", "2.2.2", 4),
        ];
        let drained = drain(script, 100, &TableFilter::default()).expect("drain");
        assert_eq!(drained.changes.len(), 1);
        assert_eq!(drained.last_commit, Some(vec![7]));
        assert!(drained.blocked.is_some());
    }

    #[test]
    fn given_idle_stream_when_draining_should_return_an_empty_batch() {
        let drained = drain(Vec::new(), 100, &TableFilter::default()).expect("drain");
        assert_eq!(drained, Drained::default());
    }

    #[test]
    fn given_receive_error_when_draining_should_propagate_the_stream_error() {
        let (mut stream, _) = ScriptedStream::new(vec![Err(StreamError::Oci {
            operation: "OCIXStreamOutLCRReceive",
            code: 3113,
            message: "end-of-file on communication channel".to_owned(),
        })]);
        let error =
            drain_batch(&mut stream, limits(10), &TableFilter::default()).expect_err("error");
        assert_eq!(error.ora_code(), Some(3113));
    }

    #[test]
    fn given_ora_codes_when_classifying_should_flag_stale_and_old_position() {
        let oci = |code| StreamError::Oci {
            operation: "attach",
            code,
            message: String::new(),
        };
        assert!(oci(ORA_STALE_SESSION).is_stale_session());
        assert!(oci(ORA_POSITION_BELOW_LOW_POSITION).is_position_below_low_position());
        assert!(!StreamError::Library("x".to_owned()).is_stale_session());
    }
}
