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

use base64::Engine;
use base64::engine::general_purpose::STANDARD;
use iggy_connector_sdk::Error;
use serde_json::{Map, Number, Value as Json, json};

use crate::stream::{Change, Column, DdlChange, Operation, RowChange, Value};

const MESSAGE_ID_BYTES: usize = 16;
pub const OP_DDL: &str = "ddl";

/// A serialized event, kept so that a NACKed batch can be sent again without touching Oracle.
#[derive(Debug, Clone, PartialEq)]
pub struct StagedMessage {
    pub id: u128,
    pub table: String,
    /// `insert`, `update`, `delete` or [`OP_DDL`].
    pub op: &'static str,
    pub payload: Vec<u8>,
}

/// Identical on every replay of a transaction: both inputs are fixed by the transaction's content.
pub fn message_id(transaction_id: &str, ordinal: u32) -> u128 {
    let mut hasher = blake3::Hasher::new();
    hasher.update(transaction_id.as_bytes());
    hasher.update(&ordinal.to_be_bytes());
    let mut id = [0u8; MESSAGE_ID_BYTES];
    id.copy_from_slice(&hasher.finalize().as_bytes()[..MESSAGE_ID_BYTES]);
    u128::from_be_bytes(id)
}

pub fn build_message(change: &Change) -> Result<StagedMessage, Error> {
    match change {
        Change::Row(row) => build_row_message(row),
        Change::Ddl(ddl) => build_ddl_message(ddl),
    }
}

fn build_row_message(row: &RowChange) -> Result<StagedMessage, Error> {
    let before = match row.operation {
        Operation::Insert => Json::Null,
        Operation::Update | Operation::Delete => columns_to_json(&row.old),
    };
    let after = match row.operation {
        Operation::Delete => Json::Null,
        Operation::Insert | Operation::Update => columns_to_json(&row.new),
    };
    let event = json!({
        "table": row.table,
        "op": row.operation.as_str(),
        "position": hex::encode_upper(&row.position),
        "xid": row.transaction_id,
        "source_time": row.source_time,
        "before": before,
        "after": after,
    });
    staged(
        message_id(&row.transaction_id, row.ordinal),
        &row.table,
        row.operation.as_str(),
        &event,
    )
}

/// Shares the transaction's ordinals with the rows, so a consumer sees the new shape exactly where it applies.
fn build_ddl_message(ddl: &DdlChange) -> Result<StagedMessage, Error> {
    let event = json!({
        "table": ddl.table,
        "op": OP_DDL,
        "command": ddl.command,
        "statement": ddl.statement,
        "position": hex::encode_upper(&ddl.position),
        "xid": ddl.transaction_id,
        "source_time": ddl.source_time,
    });
    staged(
        message_id(&ddl.transaction_id, ddl.ordinal),
        &ddl.table,
        OP_DDL,
        &event,
    )
}

fn staged(id: u128, table: &str, op: &'static str, event: &Json) -> Result<StagedMessage, Error> {
    let payload = serde_json::to_vec(event)
        .map_err(|error| Error::Serialization(format!("event serialize: {error}")))?;
    Ok(StagedMessage {
        id,
        table: table.to_owned(),
        op,
        payload,
    })
}

fn columns_to_json(columns: &[Column]) -> Json {
    let map: Map<String, Json> = columns
        .iter()
        .map(|column| (column.name.clone(), value_to_json(&column.value)))
        .collect();
    Json::Object(map)
}

fn value_to_json(value: &Value) -> Json {
    match value {
        Value::Null | Value::Unsupported => Json::Null,
        Value::Text(text) | Value::Timestamp(text) => Json::String(text.clone()),
        Value::Number(text) => number_to_json(text),
        Value::Float(number) => Number::from_f64(*number)
            .map(Json::Number)
            .unwrap_or_else(|| Json::String(number.to_string())),
        Value::Binary(bytes) => Json::String(STANDARD.encode(bytes)),
    }
}

/// A JSON number only when it round-trips exactly, so a 38 digit `NUMBER` is not silently rounded.
fn number_to_json(text: &str) -> Json {
    if let Ok(integer) = text.parse::<i64>() {
        return Json::Number(integer.into());
    }
    match text.parse::<f64>() {
        Ok(number) if number.to_string() == text => Number::from_f64(number)
            .map(Json::Number)
            .unwrap_or_else(|| Json::String(text.to_owned())),
        _ => Json::String(text.to_owned()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::stream::testing::column;

    fn update_row() -> RowChange {
        RowChange {
            table: "APP.ORDERS".to_owned(),
            operation: Operation::Update,
            transaction_id: "10.25.831".to_owned(),
            position: vec![0xab, 0xcd],
            source_time: Some("2026-11-25T10:11:12".to_owned()),
            old: vec![
                column("ID", Value::Number("7".to_owned())),
                column("STATUS", Value::Text("NEW".to_owned())),
            ],
            new: vec![
                column("ID", Value::Number("7".to_owned())),
                column("STATUS", Value::Text("SHIPPED".to_owned())),
            ],
            ordinal: 3,
        }
    }

    #[test]
    fn given_same_transaction_and_ordinal_when_hashing_should_return_the_same_id() {
        assert_eq!(message_id("10.25.831", 3), message_id("10.25.831", 3));
        assert_ne!(message_id("10.25.831", 3), message_id("10.25.831", 4));
        assert_ne!(message_id("10.25.831", 3), message_id("10.25.832", 3));
    }

    #[test]
    fn given_update_row_when_building_should_carry_before_after_and_position() {
        let message = build_message(&Change::Row(update_row())).expect("message");
        let event: Json = serde_json::from_slice(&message.payload).expect("json");
        assert_eq!(event["table"], "APP.ORDERS");
        assert_eq!(event["op"], "update");
        assert_eq!(event["position"], "ABCD");
        assert_eq!(event["xid"], "10.25.831");
        assert_eq!(event["before"]["STATUS"], "NEW");
        assert_eq!(event["after"]["STATUS"], "SHIPPED");
        assert_eq!(event["after"]["ID"], 7);
        assert_eq!(message.id, message_id("10.25.831", 3));
    }

    #[test]
    fn given_insert_and_delete_when_building_should_leave_the_missing_side_null() {
        let mut row = update_row();
        row.operation = Operation::Insert;
        let insert: Json = serde_json::from_slice(
            &build_message(&Change::Row(row.clone()))
                .expect("message")
                .payload,
        )
        .expect("json");
        assert!(insert["before"].is_null());
        assert!(!insert["after"].is_null());
        row.operation = Operation::Delete;
        let delete: Json = serde_json::from_slice(
            &build_message(&Change::Row(row.clone()))
                .expect("message")
                .payload,
        )
        .expect("json");
        assert!(delete["after"].is_null());
        assert!(!delete["before"].is_null());
    }

    #[test]
    fn given_ddl_when_building_should_carry_the_statement_and_a_stable_id() {
        let ddl = DdlChange {
            table: "APP.ORDERS".to_owned(),
            command: "ALTER TABLE".to_owned(),
            statement: "ALTER TABLE APP.ORDERS ADD (NOTE VARCHAR2(20))".to_owned(),
            transaction_id: "11.2.9".to_owned(),
            position: vec![0x01, 0xff],
            source_time: None,
            ordinal: 0,
        };
        let message = build_message(&Change::Ddl(ddl)).expect("message");
        let event: Json = serde_json::from_slice(&message.payload).expect("json");
        assert_eq!(event["op"], "ddl");
        assert_eq!(event["command"], "ALTER TABLE");
        assert_eq!(
            event["statement"],
            "ALTER TABLE APP.ORDERS ADD (NOTE VARCHAR2(20))"
        );
        assert_eq!(event["position"], "01FF");
        assert!(event.get("before").is_none());
        assert_eq!(message.op, OP_DDL);
        assert_eq!(message.id, message_id("11.2.9", 0));
    }

    #[test]
    fn given_oracle_numbers_when_converting_should_never_round() {
        assert_eq!(number_to_json("42"), json!(42));
        assert_eq!(number_to_json("-7"), json!(-7));
        assert_eq!(number_to_json("12.5"), json!(12.5));
        assert_eq!(number_to_json("0.1"), json!(0.1));
        assert_eq!(
            number_to_json("123456789012345678901234567890"),
            json!("123456789012345678901234567890")
        );
        assert_eq!(
            number_to_json("0.1234567890123456789"),
            json!("0.1234567890123456789")
        );
    }

    #[test]
    fn given_binary_and_float_when_converting_should_use_base64_and_number() {
        assert_eq!(
            value_to_json(&Value::Binary(vec![0xde, 0xad, 0xbe, 0xef])),
            json!("3q2+7w==")
        );
        assert_eq!(value_to_json(&Value::Float(1.5)), json!(1.5));
        assert_eq!(value_to_json(&Value::Float(f64::NAN)), json!("NaN"));
        assert_eq!(value_to_json(&Value::Null), Json::Null);
    }
}
