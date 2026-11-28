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

mod config;
mod event;
mod oci;
mod state;
mod stream;

use std::collections::BTreeMap;
use std::sync::Arc;

use async_trait::async_trait;
use iggy_common::{HeaderKey, HeaderValue};
use iggy_connector_sdk::source::SourceBatchResult;
use iggy_connector_sdk::{
    ConnectorState, Error, ProducedMessage, ProducedMessages, Schema, Source, source_connector,
};
use secrecy::ExposeSecret;
use tokio::sync::Mutex;
use tokio::time::sleep;
use tracing::{debug, info, warn};

pub use config::OracleSourceConfig;
use config::Settings;
use event::{StagedMessage, build_message};
use oci::{Client, ConnectParams, Session};
use state::{STATE_VERSION, State};
use stream::{Drained, LcrStream, Limits, TableFilter, drain_batch};

source_connector!(OracleSource);

const CONNECTOR_NAME: &str = "Oracle source";
const HEADER_TABLE: &str = "table";
const HEADER_OPERATION: &str = "op";
/// ORA codes that mean the database or the network is not reachable yet, so a later attempt can succeed.
const TRANSIENT_ORA_CODES: [i32; 8] = [3113, 3114, 3135, 12170, 12514, 12537, 12541, 12571];

type BoxedStream = Box<dyn LcrStream>;

pub struct OracleSource {
    id: u32,
    config: OracleSourceConfig,
    settings: Result<Settings, Error>,
    filter: Arc<TableFilter>,
    restored: Option<State>,
    state: Mutex<State>,
    staged: Mutex<Option<Staged>>,
    session: Mutex<Option<BoxedStream>>,
    fatal: Mutex<Option<String>>,
    client: Option<Arc<Client>>,
    params: Option<Arc<ConnectParams>>,
}

impl std::fmt::Debug for OracleSource {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("OracleSource")
            .field("id", &self.id)
            .field("config", &self.config)
            .finish_non_exhaustive()
    }
}

/// A batch handed to the runtime and not yet acknowledged. Waits till a NACK can resend it
/// before detaching from the outbound serve. This is used to restart the server's capture pipeline.
struct Staged {
    messages: Vec<StagedMessage>,
    candidate: State,
    persisted: Vec<u8>,
    commit_position: Option<Vec<u8>>,
}

impl OracleSource {
    pub fn new(id: u32, config: OracleSourceConfig, state: Option<ConnectorState>) -> Self {
        let restored = state
            .and_then(|stored| stored.deserialize::<State>(CONNECTOR_NAME, id))
            .inspect(|restored| {
                info!(
                    "Restored state for {CONNECTOR_NAME} ID: {id}, server: {}, position: {}",
                    restored.server_name,
                    restored
                        .last_acked_position
                        .as_deref()
                        .map(hex::encode_upper)
                        .unwrap_or_else(|| "none".to_owned())
                );
            });
        let fresh = State::new(&config.outbound_server);
        Self {
            id,
            settings: config.settings(),
            filter: Arc::new(TableFilter::new(config.tables.as_deref().unwrap_or(&[]))),
            state: Mutex::new(restored.clone().unwrap_or(fresh)),
            restored,
            config,
            staged: Mutex::new(None),
            session: Mutex::new(None),
            fatal: Mutex::new(None),
            client: None,
            params: None,
        }
    }

    fn settings(&self) -> Result<&Settings, Error> {
        self.settings.as_ref().map_err(Clone::clone)
    }

    fn validate_restored_state(&self) -> Result<(), Error> {
        let Some(restored) = &self.restored else {
            return Ok(());
        };
        if restored.version != STATE_VERSION {
            return Err(Error::InitError(format!(
                "saved state has version {}, this connector reads version {STATE_VERSION}",
                restored.version
            )));
        }
        if restored.server_name != self.config.outbound_server {
            return Err(Error::InitError(format!(
                "saved state belongs to outbound server {}, but {} is configured",
                restored.server_name, self.config.outbound_server
            )));
        }
        Ok(())
    }

    async fn take_session(&self) -> Result<BoxedStream, Error> {
        if let Some(session) = self.session.lock().await.take() {
            return Ok(session);
        }
        self.connect().await
    }

    /// A saved position below the server's processed low position is retried once from the server's own start.
    /// Stale sessions and unreachable databases are retried with a backoff.
    async fn connect(&self) -> Result<BoxedStream, Error> {
        let settings = self.settings()?;
        let (Some(client), Some(params)) = (&self.client, &self.params) else {
            return Err(Error::InitError("the source is not open".to_owned()));
        };
        let mut position = self.state.lock().await.last_acked_position.clone();
        let mut attempt = 0u32;
        loop {
            attempt += 1;
            let (client, params, from) = (Arc::clone(client), Arc::clone(params), position.clone());
            let outcome = blocking("attach", move || {
                Session::connect(client, &params, from.as_deref())
            })
            .await?;
            let error = match outcome {
                Ok(session) => return Ok(Box::new(session)),
                Err(error) => error,
            };
            if error.is_position_below_low_position() && position.is_some() {
                warn!(
                    "Saved position is below the processed low position of {CONNECTOR_NAME} ID: {}, \
                     resuming from the server's position: {error}",
                    self.id
                );
                position = None;
                continue;
            }
            let transient = error.is_stale_session()
                || error
                    .ora_code()
                    .is_some_and(|code| TRANSIENT_ORA_CODES.contains(&code));
            if transient && attempt < settings.max_reattach_attempts {
                warn!(
                    "Attach attempt {attempt} of {CONNECTOR_NAME} ID: {} failed, retrying in {:?}: {error}",
                    self.id, settings.reattach_backoff
                );
                sleep(settings.reattach_backoff).await;
                continue;
            }
            return Err(Error::Connection(error.to_string()));
        }
    }

    async fn stage(&self, drained: Drained) -> Result<ProducedMessages, Error> {
        if drained.changes.is_empty() && drained.last_commit.is_none() {
            return Ok(produced(Vec::new(), None));
        }
        let messages = drained
            .changes
            .iter()
            .map(build_message)
            .collect::<Result<Vec<_>, _>>()?;
        let mut candidate = self.state.lock().await.clone();
        candidate.messages_produced += messages.len() as u64;
        if let Some(position) = &drained.last_commit {
            candidate.last_acked_position = Some(position.clone());
        }
        let persisted = ConnectorState::serialize(&candidate, CONNECTOR_NAME, self.id)
            .ok_or_else(|| Error::Serialization("failed to serialize source state".to_owned()))?;
        let staged = Staged {
            messages,
            candidate,
            persisted: persisted.0,
            commit_position: drained.last_commit,
        };
        let batch = staged.to_produced();
        *self.staged.lock().await = Some(staged);
        Ok(batch)
    }

    async fn set_low_position(&self, position: Vec<u8>) {
        let Some(mut session) = self.session.lock().await.take() else {
            return;
        };
        let outcome = blocking("low position", move || {
            let result = session.set_processed_low_position(&position);
            (session, result)
        })
        .await;
        match outcome {
            Ok((session, Ok(()))) => *self.session.lock().await = Some(session),
            Ok((session, Err(error))) => {
                warn!(
                    "Failed to set the processed low position of {CONNECTOR_NAME} ID: {}, \
                     the next attach replays from the previous one: {error}",
                    self.id
                );
                drop_blocking(session).await;
            }
            Err(error) => warn!("{CONNECTOR_NAME} ID: {} {error}", self.id),
        }
    }
}

impl Staged {
    fn to_produced(&self) -> ProducedMessages {
        produced(
            self.messages.iter().map(to_produced_message).collect(),
            Some(ConnectorState(self.persisted.clone())),
        )
    }
}

#[async_trait]
impl Source for OracleSource {
    async fn open(&mut self) -> Result<(), Error> {
        self.settings()?;
        self.validate_restored_state()?;
        let client = Client::load(self.config.oci_library_path.as_deref())
            .map_err(|error| Error::InitError(error.to_string()))?;
        self.client = Some(Arc::new(client));
        self.params = Some(Arc::new(ConnectParams {
            connect_string: self.config.connect_string.clone(),
            username: self.config.username.clone(),
            password: self.config.password.expose_secret().to_owned(),
            outbound_server: self.config.outbound_server.clone(),
        }));
        let session = self
            .connect()
            .await
            .map_err(|error| Error::InitError(error.to_string()))?;
        *self.session.lock().await = Some(session);
        info!(
            "Opened {CONNECTOR_NAME} connector ID: {}, server: {}, tables: {:?}",
            self.id, self.config.outbound_server, self.config.tables
        );
        Ok(())
    }

    async fn poll(&self) -> Result<ProducedMessages, Error> {
        let settings = self.settings()?;
        sleep(settings.poll_interval).await;
        if let Some(staged) = self.staged.lock().await.as_ref() {
            return Ok(staged.to_produced());
        }
        if let Some(reason) = self.fatal.lock().await.clone() {
            return Err(Error::SchemaMismatch(reason));
        }
        let session = self.take_session().await?;
        let limits = Limits {
            max_events: settings.max_batch_events,
            budget: settings.poll_budget,
        };
        let filter = Arc::clone(&self.filter);
        let (session, outcome) = blocking("receive", move || {
            let mut session = session;
            let outcome = drain_batch(session.as_mut(), limits, &filter);
            (outcome.is_ok().then_some(session), outcome)
        })
        .await?;
        match outcome {
            Ok(drained) => {
                *self.session.lock().await = session;
                if let Some(blocked) = &drained.blocked {
                    let reason = blocked.to_string();
                    warn!(
                        "{CONNECTOR_NAME} ID: {} will stop after this batch is acknowledged: {reason}",
                        self.id
                    );
                    *self.fatal.lock().await = Some(reason);
                }
                if settings.verbose {
                    info!(
                        "{CONNECTOR_NAME} ID: {} drained {} changes, last commit: {}",
                        self.id,
                        drained.changes.len(),
                        drained.last_commit.is_some()
                    );
                }
                self.stage(drained).await
            }
            Err(error) => {
                warn!(
                    "Receive failed for {CONNECTOR_NAME} ID: {}, will re-attach: {error}",
                    self.id
                );
                Err(Error::Connection(error.to_string()))
            }
        }
    }

    async fn on_batch_result(&self, result: SourceBatchResult) -> Result<(), Error> {
        match result {
            SourceBatchResult::Ack => {
                let staged = self.staged.lock().await.take();
                if let Some(staged) = staged {
                    *self.state.lock().await = staged.candidate;
                    if let Some(position) = staged.commit_position {
                        self.set_low_position(position).await;
                    }
                }
                if let Some(reason) = self.fatal.lock().await.clone() {
                    return Err(Error::SchemaMismatch(reason));
                }
            }
            SourceBatchResult::Nack => {
                debug!(
                    "Batch of {CONNECTOR_NAME} ID: {} was not delivered, it will be sent again",
                    self.id
                );
            }
        }
        Ok(())
    }

    async fn close(&mut self) -> Result<(), Error> {
        if let Some(mut session) = self.session.lock().await.take() {
            match blocking("detach", move || session.detach()).await {
                Ok(Ok(())) => {}
                Ok(Err(error)) => {
                    warn!("Detach of {CONNECTOR_NAME} ID: {} failed: {error}", self.id)
                }
                Err(error) => warn!("{CONNECTOR_NAME} ID: {} {error}", self.id),
            }
        }
        let state = self.state.lock().await;
        info!(
            "Closed {CONNECTOR_NAME} connector ID: {}, total produced: {}",
            self.id, state.messages_produced
        );
        Ok(())
    }
}

fn produced(messages: Vec<ProducedMessage>, state: Option<ConnectorState>) -> ProducedMessages {
    ProducedMessages {
        schema: Schema::Json,
        messages,
        state,
    }
}

fn to_produced_message(message: &StagedMessage) -> ProducedMessage {
    ProducedMessage {
        id: Some(message.id),
        checksum: None,
        timestamp: None,
        origin_timestamp: None,
        headers: headers(message),
        payload: message.payload.clone(),
    }
}

fn headers(message: &StagedMessage) -> Option<BTreeMap<HeaderKey, HeaderValue>> {
    let table = (
        HeaderKey::try_from(HEADER_TABLE).ok()?,
        HeaderValue::try_from(message.table.as_str()).ok()?,
    );
    let operation = (
        HeaderKey::try_from(HEADER_OPERATION).ok()?,
        HeaderValue::try_from(message.op).ok()?,
    );
    Some(BTreeMap::from([table, operation]))
}

/// OCI calls block, so they run off the async threads.
async fn blocking<T: Send + 'static>(
    task_name: &str,
    task: impl FnOnce() -> T + Send + 'static,
) -> Result<T, Error> {
    tokio::task::spawn_blocking(task)
        .await
        .map_err(|error| Error::Connection(format!("{task_name} task failed: {error}")))
}

async fn drop_blocking(session: BoxedStream) {
    if let Err(error) = blocking("release", move || drop(session)).await {
        warn!("{error}");
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::stream::testing::{ProcessedPositions, ScriptedStream, commit, ddl, insert};
    use crate::stream::{Received, StreamError};
    use secrecy::SecretString;

    fn test_config() -> OracleSourceConfig {
        OracleSourceConfig {
            connect_string: "localhost:1521/ORCLCDB".to_owned(),
            username: "c##xstrmadmin".to_owned(),
            password: SecretString::from("secret".to_owned()),
            outbound_server: "IGGY_XOUT".to_owned(),
            oci_library_path: None,
            tables: None,
            poll_interval: Some("1ms".to_owned()),
            poll_budget: Some("5s".to_owned()),
            max_batch_events: None,
            reattach_backoff: None,
            max_reattach_attempts: None,
            verbose_logging: None,
        }
    }

    fn state_with(
        version: u16,
        server: &str,
        position: Option<Vec<u8>>,
        produced: u64,
    ) -> ConnectorState {
        let state = State {
            version,
            server_name: server.to_owned(),
            last_acked_position: position,
            messages_produced: produced,
        };
        ConnectorState(rmp_serde::to_vec(&state).expect("serialize state"))
    }

    async fn source_with(
        script: Vec<Result<Received, StreamError>>,
    ) -> (OracleSource, ProcessedPositions) {
        let source = OracleSource::new(1, test_config(), None);
        let (stream, processed) = ScriptedStream::new(script);
        *source.session.lock().await = Some(Box::new(stream));
        (source, processed)
    }

    fn two_row_transaction() -> Vec<Result<Received, StreamError>> {
        vec![
            Ok(insert("APP.T", "1.1.1", 1)),
            Ok(insert("APP.T", "1.1.1", 2)),
            Ok(commit(7)),
        ]
    }

    #[tokio::test]
    async fn given_saved_state_when_opening_should_restore_position_and_counter() {
        let stored = state_with(STATE_VERSION, "IGGY_XOUT", Some(vec![1, 2, 3]), 500);
        let source = OracleSource::new(1, test_config(), Some(stored));
        let state = source.state.lock().await;
        assert_eq!(state.messages_produced, 500);
        assert_eq!(state.last_acked_position, Some(vec![1, 2, 3]));
    }

    #[tokio::test]
    async fn given_no_state_when_opening_should_start_fresh() {
        let source = OracleSource::new(1, test_config(), None);
        let state = source.state.lock().await;
        assert_eq!(*state, State::new("IGGY_XOUT"));
    }

    #[tokio::test]
    async fn given_invalid_state_when_opening_should_start_fresh() {
        let source = OracleSource::new(
            1,
            test_config(),
            Some(ConnectorState(b"not valid msgpack".to_vec())),
        );
        assert_eq!(*source.state.lock().await, State::new("IGGY_XOUT"));
    }

    #[test]
    fn given_state_when_serializing_should_round_trip() {
        let original = State {
            version: STATE_VERSION,
            server_name: "IGGY_XOUT".to_owned(),
            last_acked_position: Some(vec![0, 255, 2]),
            messages_produced: 1000,
        };
        let stored = ConnectorState::serialize(&original, CONNECTOR_NAME, 1).expect("serialize");
        assert_eq!(
            stored.deserialize::<State>(CONNECTOR_NAME, 1),
            Some(original)
        );
    }

    #[tokio::test]
    async fn given_other_state_version_when_opening_should_fail() {
        let stored = state_with(STATE_VERSION + 1, "IGGY_XOUT", None, 0);
        let mut source = OracleSource::new(1, test_config(), Some(stored));
        let error = source
            .open()
            .await
            .expect_err("version mismatch must fail open");
        assert!(
            matches!(&error, Error::InitError(reason) if reason.contains("version")),
            "{error:?}"
        );
    }

    #[tokio::test]
    async fn given_other_server_state_when_opening_should_fail() {
        let stored = state_with(STATE_VERSION, "OTHER_XOUT", None, 0);
        let mut source = OracleSource::new(1, test_config(), Some(stored));
        let error = source
            .open()
            .await
            .expect_err("server mismatch must fail open");
        assert!(
            matches!(&error, Error::InitError(reason) if reason.contains("OTHER_XOUT") && reason.contains("IGGY_XOUT")),
            "{error:?}"
        );
    }

    #[tokio::test]
    async fn given_invalid_config_when_opening_should_fail() {
        let mut config = test_config();
        config.max_batch_events = Some(0);
        let mut source = OracleSource::new(1, config, None);
        assert!(matches!(
            source.open().await,
            Err(Error::InvalidConfigValue(_))
        ));
    }

    #[tokio::test]
    async fn given_transaction_when_polling_should_stage_candidate_state() {
        let (source, _) = source_with(two_row_transaction()).await;
        let batch = source.poll().await.expect("poll");
        assert_eq!(batch.messages.len(), 2);
        assert!(
            batch
                .messages
                .iter()
                .all(|message| message.id.is_some() && message.headers.is_some())
        );
        let persisted = batch
            .state
            .expect("state")
            .deserialize::<State>(CONNECTOR_NAME, 1)
            .expect("state");
        assert_eq!(persisted.last_acked_position, Some(vec![7]));
        assert_eq!(persisted.messages_produced, 2);
        assert_eq!(
            source.state.lock().await.messages_produced,
            0,
            "polling must not commit state"
        );
    }

    #[tokio::test]
    async fn given_ack_when_batch_staged_should_commit_state_and_low_position() {
        let (source, processed) = source_with(two_row_transaction()).await;
        source.poll().await.expect("poll");
        source
            .on_batch_result(SourceBatchResult::Ack)
            .await
            .expect("ack");
        let state = source.state.lock().await;
        assert_eq!(state.messages_produced, 2);
        assert_eq!(state.last_acked_position, Some(vec![7]));
        assert_eq!(*processed.lock().expect("lock"), vec![vec![7u8]]);
        assert!(source.staged.lock().await.is_none());
        assert!(
            source.session.lock().await.is_some(),
            "the session must survive an ack"
        );
    }

    #[tokio::test]
    async fn given_nack_when_polling_again_should_resend_same_batch() {
        let (source, processed) = source_with(two_row_transaction()).await;
        let first = source.poll().await.expect("poll");
        source
            .on_batch_result(SourceBatchResult::Nack)
            .await
            .expect("nack");
        let second = source.poll().await.expect("poll again");
        let ids = |batch: &ProducedMessages| {
            batch
                .messages
                .iter()
                .map(|message| message.id)
                .collect::<Vec<_>>()
        };
        assert_eq!(ids(&first), ids(&second));
        assert_eq!(
            first.state.map(|state| state.0),
            second.state.map(|state| state.0)
        );
        assert_eq!(source.state.lock().await.messages_produced, 0);
        assert!(
            processed.lock().expect("lock").is_empty(),
            "a nack must not move the low position"
        );
    }

    #[tokio::test]
    async fn given_empty_commit_when_polling_should_advance_position_on_ack() {
        let (source, processed) = source_with(vec![Ok(commit(9))]).await;
        let batch = source.poll().await.expect("poll");
        assert!(batch.messages.is_empty());
        assert!(batch.state.is_some());
        source
            .on_batch_result(SourceBatchResult::Ack)
            .await
            .expect("ack");
        assert_eq!(source.state.lock().await.last_acked_position, Some(vec![9]));
        assert_eq!(*processed.lock().expect("lock"), vec![vec![9u8]]);
    }

    #[tokio::test]
    async fn given_idle_stream_when_polling_should_return_empty_batch() {
        let (source, _) = source_with(Vec::new()).await;
        let batch = source.poll().await.expect("poll");
        assert!(batch.messages.is_empty());
        assert!(batch.state.is_none());
    }

    #[tokio::test]
    async fn given_ddl_between_transactions_when_polling_should_emit_in_order() {
        let (source, processed) = source_with(vec![
            Ok(insert("APP.T", "1.1.1", 1)),
            Ok(commit(1)),
            Ok(ddl("APP.T", "ALTER TABLE", "2.2.2")),
            Ok(commit(2)),
            Ok(insert("APP.T", "3.3.3", 2)),
            Ok(commit(3)),
        ])
        .await;
        let batch = source.poll().await.expect("poll");
        let ops: Vec<String> = batch
            .messages
            .iter()
            .map(|message| {
                let event: serde_json::Value =
                    serde_json::from_slice(&message.payload).expect("json");
                event["op"].as_str().expect("op").to_owned()
            })
            .collect();
        assert_eq!(ops, vec!["insert", "ddl", "insert"]);
        source
            .on_batch_result(SourceBatchResult::Ack)
            .await
            .expect("ack");
        assert_eq!(*processed.lock().expect("lock"), vec![vec![3u8]]);
    }

    #[tokio::test]
    async fn given_unsupported_type_when_polling_should_deliver_earlier_commits_then_stop_on_ack() {
        let mut row = insert("APP.T", "1.1.1", 1);
        if let Received::Row(change) = &mut row {
            change.new.push(crate::stream::testing::column(
                "BODY",
                crate::stream::Value::Unsupported,
            ));
        }
        let (source, processed) = source_with(vec![
            Ok(insert("APP.T", "1.1.1", 1)),
            Ok(commit(1)),
            Ok(row),
        ])
        .await;
        let batch = source
            .poll()
            .await
            .expect("the committed prefix is delivered");
        assert_eq!(batch.messages.len(), 1);
        source
            .on_batch_result(SourceBatchResult::Nack)
            .await
            .expect("a nack does not stop the source");
        let resent = source.poll().await.expect("the prefix is sent again");
        assert_eq!(resent.messages.len(), 1);
        let stop = source
            .on_batch_result(SourceBatchResult::Ack)
            .await
            .expect_err("the ack stops the source");
        assert!(
            matches!(&stop, Error::SchemaMismatch(reason) if reason.contains("APP.T.BODY")),
            "{stop:?}"
        );
        assert_eq!(*processed.lock().expect("lock"), vec![vec![1u8]]);
    }

    #[tokio::test]
    async fn given_unsupported_type_without_earlier_commit_when_acknowledging_should_stop() {
        let mut row = insert("APP.T", "1.1.1", 1);
        if let Received::Row(change) = &mut row {
            change.new.push(crate::stream::testing::column(
                "BODY",
                crate::stream::Value::Unsupported,
            ));
        }
        let (source, _) = source_with(vec![Ok(row)]).await;
        let batch = source.poll().await.expect("an empty batch");
        assert!(batch.messages.is_empty());
        let stop = source
            .on_batch_result(SourceBatchResult::Ack)
            .await
            .expect_err("the ack stops the source");
        assert!(matches!(stop, Error::SchemaMismatch(_)), "{stop:?}");
        let later = source.poll().await.expect_err("later polls keep failing");
        assert!(matches!(later, Error::SchemaMismatch(_)), "{later:?}");
    }

    #[tokio::test]
    async fn given_receive_error_when_polling_should_drop_session_and_fail() {
        let failure = StreamError::Oci {
            operation: "OCIXStreamOutLCRReceive",
            code: 3113,
            message: "end-of-file on communication channel".to_owned(),
        };
        let (source, _) = source_with(vec![Err(failure)]).await;
        let error = source.poll().await.expect_err("receive failure");
        assert!(
            matches!(error, Error::Connection(ref reason) if reason.contains("3113")),
            "{error:?}"
        );
        assert!(
            source.session.lock().await.is_none(),
            "the next poll must re-attach"
        );
    }

    #[tokio::test]
    async fn given_table_filter_when_polling_should_forward_only_listed_tables() {
        let mut config = test_config();
        config.tables = Some(vec!["APP.T".to_owned()]);
        let source = OracleSource::new(1, config, None);
        let script = vec![
            Ok(insert("APP.OTHER", "1.1.1", 1)),
            Ok(commit(1)),
            Ok(insert("APP.T", "2.2.2", 2)),
            Ok(commit(2)),
        ];
        let (stream, _) = ScriptedStream::new(script);
        *source.session.lock().await = Some(Box::new(stream));
        let batch = source.poll().await.expect("poll");
        assert_eq!(batch.messages.len(), 1);
    }
}
