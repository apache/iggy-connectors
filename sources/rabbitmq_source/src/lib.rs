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
mod driver;
mod message;

mod batch {
    use iggy_connector_sdk::{Error, ProducedMessage, ProducedMessages, Schema};
    use lapin::{Acker, options::BasicAckOptions};

    pub struct PendingBatch {
        schema: Schema,
        deliveries: Vec<(ProducedMessage, Acker)>,
    }

    impl PendingBatch {
        pub fn new(schema: Schema, deliveries: Vec<(ProducedMessage, Acker)>) -> Self {
            Self { schema, deliveries }
        }

        pub fn next_attempt(&self) -> ProducedMessages {
            // Keep the originals and their ackers for replay if the runtime returns a NACK.
            ProducedMessages {
                schema: self.schema,
                messages: self
                    .deliveries
                    .iter()
                    .map(|(message, _)| ProducedMessage {
                        id: message.id,
                        checksum: message.checksum,
                        timestamp: message.timestamp,
                        origin_timestamp: message.origin_timestamp,
                        headers: message.headers.clone(),
                        payload: message.payload.clone(),
                    })
                    .collect(),
                state: None,
            }
        }

        pub fn is_usable(&self) -> bool {
            self.deliveries
                .last()
                .is_none_or(|(_, acker)| acker.usable())
        }

        pub async fn acknowledge(&self) -> Result<(), Error> {
            let Some((_, acker)) = self.deliveries.last() else {
                return Ok(());
            };
            // The source processes one batch at a time on a dedicated channel.
            match acker.ack(BasicAckOptions { multiple: true }).await {
                Ok(true) => Ok(()),
                Ok(false) => Err(Error::Connection(
                    "RabbitMQ acknowledgement channel is closed".into(),
                )),
                Err(_) => Err(Error::Connection("RabbitMQ acknowledgement failed".into())),
            }
        }
    }
}

mod tls {
    use std::{io::Cursor, sync::Arc};

    use iggy_connector_sdk::Error;
    use lapin::tcp::{OwnedIdentity, OwnedTLSConfig};
    use rustls::{ClientConfig, RootCertStore};

    use crate::config::RabbitMqTlsConfig;

    pub fn build(config: Option<&RabbitMqTlsConfig>) -> Result<OwnedTLSConfig, Error> {
        let Some(config) = config else {
            return Ok(OwnedTLSConfig::default());
        };
        let mut tls = OwnedTLSConfig::default();
        if let Some(path) = &config.ca_file {
            let data = std::fs::read(path).map_err(|error| {
                Error::InvalidConfigValue(format!("cannot read tls.ca_file: {error}"))
            })?;
            let certificates = rustls_pemfile::certs(&mut Cursor::new(&data))
                .collect::<Result<Vec<_>, _>>()
                .map_err(|error| {
                    Error::InvalidConfigValue(format!("invalid CA certificates: {error}"))
                })?;
            if certificates.is_empty() {
                return Err(Error::InvalidConfigValue(
                    "tls.ca_file contains no certificates".into(),
                ));
            }
            tls.cert_chain = Some(
                String::from_utf8(data)
                    .map_err(|_| Error::InvalidConfigValue("CA file is not UTF-8 PEM".into()))?,
            );
        }
        if let (Some(cert_path), Some(key_path)) =
            (&config.client_cert_file, &config.client_key_file)
        {
            let pem = std::fs::read(cert_path).map_err(|error| {
                Error::InvalidConfigValue(format!("cannot read client certificate: {error}"))
            })?;
            let key = std::fs::read(key_path).map_err(|error| {
                Error::InvalidConfigValue(format!("cannot read client key: {error}"))
            })?;
            let certificates = rustls_pemfile::certs(&mut Cursor::new(&pem))
                .collect::<Result<Vec<_>, _>>()
                .map_err(|error| {
                    Error::InvalidConfigValue(format!("invalid client certificate: {error}"))
                })?;
            if certificates.is_empty() {
                return Err(Error::InvalidConfigValue(
                    "client certificate file contains no certificates".into(),
                ));
            }
            let private_key = rustls_pemfile::private_key(&mut Cursor::new(&key))
                .map_err(|error| Error::InvalidConfigValue(format!("invalid client key: {error}")))?
                .ok_or_else(|| {
                    Error::InvalidConfigValue("client key file contains no private key".into())
                })?;
            // Validate that the client certificate matches its private key.
            ClientConfig::builder_with_provider(Arc::new(rustls::crypto::ring::default_provider()))
                .with_safe_default_protocol_versions()
                .map_err(|error| {
                    Error::InvalidConfigValue(format!("invalid TLS versions: {error}"))
                })?
                .with_root_certificates(RootCertStore::empty())
                .with_client_auth_cert(certificates, private_key)
                .map_err(|error| {
                    Error::InvalidConfigValue(format!(
                        "client certificate and key do not match: {error}"
                    ))
                })?;
            tls.identity = Some(OwnedIdentity::PKCS8 { pem, key });
        }
        Ok(tls)
    }

    #[cfg(test)]
    mod tests {
        use std::path::PathBuf;

        use super::build;
        use crate::config::RabbitMqTlsConfig;

        #[test]
        fn given_matching_certificates_should_build_tls_configuration() {
            let certs = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../certs");
            let config = RabbitMqTlsConfig {
                ca_file: Some(
                    certs
                        .join("iggy_ca_cert.pem")
                        .to_string_lossy()
                        .into_owned(),
                ),
                client_cert_file: Some(certs.join("iggy_cert.pem").to_string_lossy().into_owned()),
                client_key_file: Some(certs.join("iggy_key.pem").to_string_lossy().into_owned()),
            };
            let tls = build(Some(&config)).expect("matching certificate and key must load");
            assert!(tls.cert_chain.is_some());
            assert!(tls.identity.is_some());
        }

        #[test]
        fn given_non_pem_ca_should_reject_tls_configuration() {
            let config = RabbitMqTlsConfig {
                ca_file: Some(
                    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
                        .join("Cargo.toml")
                        .to_string_lossy()
                        .into_owned(),
                ),
                client_cert_file: None,
                client_key_file: None,
            };
            assert!(build(Some(&config)).is_err());
        }

        #[test]
        fn given_mismatched_certificate_and_key_should_reject_tls_configuration() {
            let certs = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../certs");
            let config = RabbitMqTlsConfig {
                ca_file: None,
                client_cert_file: Some(
                    certs
                        .join("iggy_ca_cert.pem")
                        .to_string_lossy()
                        .into_owned(),
                ),
                client_key_file: Some(certs.join("iggy_key.pem").to_string_lossy().into_owned()),
            };
            assert!(build(Some(&config)).is_err());
        }
    }
}

use std::{
    collections::BTreeMap,
    num::NonZeroU32,
    sync::atomic::{AtomicU32, Ordering},
    time::Duration,
};

use async_trait::async_trait;
use iggy_connector_sdk::{
    ConnectorState, Error, ProducedMessage, ProducedMessages, Schema, Source,
    source::{BatchPolicy, SourceBatchResult},
    source_connector,
};
use lapin::{
    Acker, Connection, ConnectionProperties, message::Delivery, options::BasicRejectOptions,
    tcp::OwnedTLSConfig,
};
use secrecy::ExposeSecret;
use tokio::{
    sync::Mutex,
    time::{Instant, sleep, timeout},
};
use tracing::{debug, error, info, warn};

use crate::{
    batch::PendingBatch,
    config::RabbitMqSourceConfig,
    driver::RabbitMqDriver,
    message::{MAX_PAYLOAD_BYTES, prepare_delivery},
};

source_connector!(RabbitMqSource);

pub struct RabbitMqSource {
    id: u32,
    config: RabbitMqSourceConfig,
    batch_timeout: Duration,
    tls: OwnedTLSConfig,
    driver: Mutex<Option<RabbitMqDriver>>,
    pending: Mutex<Option<PendingBatch>>,
    overflow: Mutex<Option<(ProducedMessage, Acker, Instant)>>,
    deferred_failure: Mutex<Option<Failure>>,
    retry_attempts: AtomicU32,
    consecutive_ack_failures: AtomicU32,
    last_warning: Mutex<BTreeMap<(&'static str, Option<String>), Instant>>,
}

struct Failure {
    operation: &'static str,
    reply_code: Option<String>,
    error: Error,
}

enum RejectOutcome {
    Continue(RabbitMqDriver),
    FinishBatch(RabbitMqDriver),
    Failed(Error),
}

impl Failure {
    fn simple(operation: &'static str, message: &str) -> Self {
        Self {
            operation,
            reply_code: None,
            error: Error::Connection(message.into()),
        }
    }

    fn lapin(operation: &'static str, error: lapin::Error) -> Self {
        // Lapin errors can contain connection details or credentials, so retain only safe details.
        let (reply_code, detail) = match error.kind() {
            lapin::ErrorKind::ProtocolError(error) => (
                Some(error.get_id().to_string()),
                format!("{} (reply code {})", error.kind(), error.get_id()),
            ),
            lapin::ErrorKind::IOError(error) => (None, format!("I/O error: {:?}", error.kind())),
            _ => (None, "client error".to_string()),
        };
        Self {
            operation,
            reply_code,
            error: Error::Connection(format!("RabbitMQ {operation} failed: {detail}")),
        }
    }
}

impl std::fmt::Debug for RabbitMqSource {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("RabbitMqSource")
            .field("id", &self.id)
            .field("queue", &self.config.queue)
            .finish_non_exhaustive()
    }
}

const AMQP_FRAME_MAX: u32 = 4096;

/// Adds a bounded frame_max wish only when the user did not set frame_max explicitly.
fn merge_frame_max(amqp_url: &str) -> String {
    let mut url = match url::Url::parse(amqp_url) {
        Ok(url) => url,
        Err(_) => return amqp_url.to_owned(),
    };
    if url
        .query_pairs()
        .any(|(name, _)| name.eq_ignore_ascii_case("frame_max"))
    {
        return url.to_string();
    }

    url.query_pairs_mut()
        .append_pair("frame_max", &AMQP_FRAME_MAX.to_string());
    url.to_string()
}

impl RabbitMqSource {
    pub fn new(id: u32, config: RabbitMqSourceConfig, _state: Option<ConnectorState>) -> Self {
        Self {
            id,
            config,
            batch_timeout: Duration::ZERO,
            tls: OwnedTLSConfig::default(),
            driver: Mutex::new(None),
            pending: Mutex::new(None),
            overflow: Mutex::new(None),
            deferred_failure: Mutex::new(None),
            retry_attempts: AtomicU32::new(0),
            consecutive_ack_failures: AtomicU32::new(0),
            last_warning: Mutex::new(BTreeMap::new()),
        }
    }

    fn schema(&self) -> Schema {
        if self.config.include_metadata {
            Schema::Json
        } else {
            Schema::Raw
        }
    }

    async fn connect(&self) -> Result<RabbitMqDriver, Failure> {
        let duration = Duration::from_secs(self.config.timeout_secs);
        let runtime =
            lapin::runtime::default_runtime().map_err(|error| Failure::lapin("runtime", error))?;
        let amqp_url = merge_frame_max(self.config.amqp_url.expose_secret());
        let connection = timeout(
            duration,
            Connection::connect_with_config(
                &amqp_url,
                ConnectionProperties::default(),
                self.tls.clone(),
                runtime,
            ),
        )
        .await
        .map_err(|_| Failure::simple("connect", "RabbitMQ connection timed out"))?
        .map_err(|error| Failure::lapin("connect", error))?;
        let driver = timeout(
            duration,
            RabbitMqDriver::consume(connection, &self.config.queue, self.config.batch_size),
        )
        .await
        .map_err(|_| Failure::simple("consumer_setup", "RabbitMQ consumer setup timed out"))?
        .map_err(|error| Failure::lapin(error.operation, error.error))?;
        self.retry_attempts.store(0, Ordering::Relaxed);
        Ok(driver)
    }

    async fn wait_before_retry(&self, failure: &Failure) {
        let attempts = self.retry_attempts.fetch_add(1, Ordering::Relaxed);
        self.back_off(attempts, failure).await;
    }

    async fn wait_before_ack_retry(&self, failure: &Failure) {
        let attempts = self
            .consecutive_ack_failures
            .fetch_add(1, Ordering::Relaxed);
        self.back_off(attempts, failure).await;
    }

    async fn back_off(&self, attempts: u32, failure: &Failure) {
        let maximum_ms = (1000u64 << attempts.min(3)).min(5000);
        let delay_ms = maximum_ms + rand::random::<u64>() % 1000;
        let mut last = self.last_warning.lock().await;
        let key = (failure.operation, failure.reply_code.clone());
        if last
            .get(&key)
            .is_none_or(|instant| instant.elapsed() >= Duration::from_secs(60))
        {
            warn!(
                connector_id = self.id,
                queue = %self.config.queue,
                operation = failure.operation,
                reply_code = failure.reply_code.as_deref().unwrap_or("none"),
                error = %failure.error,
                "RabbitMQ source operation failed: operation={}, reply_code={}, error={}",
                failure.operation,
                failure.reply_code.as_deref().unwrap_or("none"),
                failure.error
            );
            last.insert(key, Instant::now());
        }
        drop(last);
        sleep(Duration::from_millis(delay_ms)).await;
    }

    async fn close_driver(&self, driver: RabbitMqDriver) {
        match timeout(
            Duration::from_secs(self.config.timeout_secs),
            driver.close(),
        )
        .await
        {
            Ok(Ok(())) => {}
            Ok(Err(_)) => {
                warn!(connector_id = self.id, queue = %self.config.queue, "Failed to close RabbitMQ connection")
            }
            Err(_) => {
                warn!(connector_id = self.id, queue = %self.config.queue, "RabbitMQ connection close timed out")
            }
        }
    }

    async fn reject_unsupported(
        &self,
        reason: &str,
        delivery: Delivery,
        driver: RabbitMqDriver,
        deliveries: &mut [(ProducedMessage, Acker)],
    ) -> RejectOutcome {
        warn!(connector_id = self.id, queue = %self.config.queue, reason, "Rejecting RabbitMQ delivery: {reason}");
        let result = timeout(
            Duration::from_secs(self.config.timeout_secs),
            delivery.acker.reject(BasicRejectOptions { requeue: false }),
        )
        .await;
        let failure = match result {
            Ok(Ok(true)) => return RejectOutcome::Continue(driver),
            Ok(Err(error)) => Failure::lapin("poison_reject", error),
            _ => Failure::simple("poison_reject", "RabbitMQ poison rejection failed"),
        };
        if !deliveries.is_empty() {
            // The valid deliveries need their original channel until they are acknowledged.
            *self.deferred_failure.lock().await = Some(failure);
            return RejectOutcome::FinishBatch(driver);
        }
        self.close_driver(driver).await;
        self.wait_before_retry(&failure).await;
        RejectOutcome::Failed(failure.error)
    }

    fn empty_batch(&self) -> ProducedMessages {
        ProducedMessages {
            schema: self.schema(),
            messages: Vec::new(),
            state: None,
        }
    }
}

#[async_trait]
impl Source for RabbitMqSource {
    async fn open(&mut self) -> Result<(), Error> {
        self.batch_timeout = match self.config.validate() {
            Ok(batch_timeout) => batch_timeout,
            Err(error) => {
                error!(
                    connector_id = self.id,
                    "Failed to validate RabbitMQ source configuration: {error}"
                );
                return Err(error);
            }
        };
        self.tls = match tls::build(self.config.tls.as_ref()) {
            Ok(tls) => tls,
            Err(error) => {
                error!(
                    connector_id = self.id,
                    "Failed to build TLS configuration for RabbitMQ source: {error}"
                );
                return Err(error);
            }
        };
        let driver = match self.connect().await {
            Ok(driver) => driver,
            Err(failure) => {
                error!(
                    connector_id = self.id,
                    queue = %self.config.queue,
                    "Failed to connect RabbitMQ source to broker: connector ID {}, queue {}: {}",
                    self.id,
                    self.config.queue,
                    failure.error
                );
                return Err(failure.error);
            }
        };
        *self.driver.get_mut() = Some(driver);
        info!(
            connector_id = self.id,
            queue = %self.config.queue,
            prefetch_count = self.config.batch_size,
            "Opened RabbitMQ source"
        );
        Ok(())
    }

    fn batch_policy(&self) -> BatchPolicy {
        BatchPolicy::default()
            .with_max_consecutive_nacks(self.config.max_consecutive_nacks.and_then(NonZeroU32::new))
    }

    async fn poll(&self) -> Result<ProducedMessages, Error> {
        let poisoned_batch = {
            let mut pending = self.pending.lock().await;
            match pending.as_mut() {
                Some(batch) if batch.is_usable() => {
                    return Ok(batch.next_attempt());
                }
                Some(_) => {
                    pending.take();
                    true
                }
                None => false,
            }
        };
        if poisoned_batch {
            self.overflow.lock().await.take();
            self.deferred_failure.lock().await.take();
            if let Some(driver) = self.driver.lock().await.take() {
                self.close_driver(driver).await;
            }
            warn!(connector_id = self.id, queue = %self.config.queue, "Discarded RabbitMQ batch with a closed channel; deliveries will be requeued");
        }
        let current_driver = self.driver.lock().await.take();
        let mut driver = match current_driver {
            Some(driver) => driver,
            None => match self.connect().await {
                Ok(driver) => {
                    info!(connector_id = self.id, queue = %self.config.queue, "RabbitMQ source reconnected");
                    driver
                }
                Err(failure) => {
                    self.wait_before_retry(&failure).await;
                    return Err(failure.error);
                }
            },
        };
        let batch_size = usize::from(self.config.batch_size);
        let mut deliveries = Vec::with_capacity(batch_size);
        let mut payload_bytes = 0;
        let mut deadline = None;
        if let Some((message, acker, received_at)) = self.overflow.lock().await.take() {
            payload_bytes = message.payload.len();
            deliveries.push((message, acker));
            deadline = Some(received_at + self.batch_timeout);
        }
        loop {
            if deliveries.len() >= batch_size || payload_bytes == MAX_PAYLOAD_BYTES {
                break;
            }
            let wait = deadline.map_or(Duration::from_secs(1), |deadline: Instant| {
                deadline.saturating_duration_since(Instant::now())
            });
            if wait.is_zero() {
                break;
            }
            let mut delivery = match timeout(wait, driver.next()).await {
                Err(_) => break,
                Ok(Some(Ok(delivery))) => delivery,
                Ok(Some(Err(error))) => {
                    let failure = Failure::lapin("consume", error);
                    self.close_driver(driver).await;
                    self.wait_before_retry(&failure).await;
                    return Err(failure.error);
                }
                Ok(None) => {
                    let failure = Failure::simple("consume", "RabbitMQ consumer cancelled");
                    self.close_driver(driver).await;
                    self.wait_before_retry(&failure).await;
                    return Err(failure.error);
                }
            };
            let received_at = Instant::now();
            deadline.get_or_insert(received_at + self.batch_timeout);
            let (message, rejection_reason) =
                match prepare_delivery(&mut delivery, self.config.include_metadata, self.id) {
                    Ok(Some(message)) => (Some(message), None),
                    Ok(None) => (
                        None,
                        Some(std::borrow::Cow::Borrowed("unsupported delivery")),
                    ),
                    Err(error) => (
                        None,
                        Some(std::borrow::Cow::Owned(format!(
                            "preparation failed: {error}"
                        ))),
                    ),
                };
            if let Some(reason) = rejection_reason {
                match self
                    .reject_unsupported(&reason, delivery, driver, &mut deliveries)
                    .await
                {
                    RejectOutcome::Continue(next_driver) => {
                        driver = next_driver;
                        continue;
                    }
                    RejectOutcome::FinishBatch(next_driver) => {
                        driver = next_driver;
                        break;
                    }
                    RejectOutcome::Failed(error) => return Err(error),
                }
            }
            if let Some(message) = message {
                if message.payload.len() > MAX_PAYLOAD_BYTES - payload_bytes {
                    *self.overflow.lock().await = Some((message, delivery.acker, received_at));
                    break;
                }
                payload_bytes += message.payload.len();
                deliveries.push((message, delivery.acker));
                if deliveries.len() >= batch_size {
                    break;
                }
            }
        }
        *self.driver.lock().await = Some(driver);
        if deliveries.is_empty() {
            return Ok(self.empty_batch());
        }
        if self.config.verbose_logging.unwrap_or(false) {
            info!(connector_id = self.id, queue = %self.config.queue, batch_size = deliveries.len(), "Prepared RabbitMQ source batch");
        } else {
            debug!(connector_id = self.id, queue = %self.config.queue, batch_size = deliveries.len(), "Prepared RabbitMQ source batch");
        }
        let batch = PendingBatch::new(self.schema(), deliveries);
        let messages = batch.next_attempt();
        *self.pending.lock().await = Some(batch);
        Ok(messages)
    }

    async fn on_batch_result(&self, result: SourceBatchResult) -> Result<(), Error> {
        let mut pending = self.pending.lock().await;
        if result == SourceBatchResult::Nack {
            return Ok(());
        }
        let Some(batch) = pending.take() else {
            return Ok(());
        };
        drop(pending);
        let result = timeout(
            Duration::from_secs(self.config.timeout_secs),
            batch.acknowledge(),
        )
        .await
        .unwrap_or_else(|_| {
            Err(Error::Connection(
                "RabbitMQ acknowledgement timed out".into(),
            ))
        });
        if let Err(error) = result {
            /* An acker is single-use and the broker outcome is uncertain. Closing the
             * connection lets RabbitMQ requeue unsettled deliveries, possibly causing duplicates.
             * Back off per failure so a broker that keeps rejecting acknowledgements cannot turn
             * the redelivery into an unthrottled duplicate loop. */
            self.overflow.lock().await.take();
            self.deferred_failure.lock().await.take();
            let driver = self.driver.lock().await.take();
            if let Some(driver) = driver {
                self.close_driver(driver).await;
            }
            let failure = Failure {
                operation: "ack",
                reply_code: None,
                error: Error::Connection(format!(
                    "{error}; closing the connection so RabbitMQ can redeliver unsettled messages"
                )),
            };
            self.wait_before_ack_retry(&failure).await;
            return Ok(());
        }

        let ack_failures = self.consecutive_ack_failures.swap(0, Ordering::Relaxed);
        let failure = self.deferred_failure.lock().await.take();
        if let Some(failure) = failure {
            let driver = self.driver.lock().await.take();
            if let Some(driver) = driver {
                self.close_driver(driver).await;
            }
            self.wait_before_retry(&failure).await;
        } else {
            let retry_attempts = self.retry_attempts.swap(0, Ordering::Relaxed);
            if ack_failures > 0 || retry_attempts > 0 {
                info!(connector_id = self.id, queue = %self.config.queue, "RabbitMQ source recovered");
            }
        }
        Ok(())
    }

    async fn close(&mut self) -> Result<(), Error> {
        self.pending.get_mut().take();
        self.overflow.get_mut().take();
        self.deferred_failure.get_mut().take();
        if let Some(driver) = self.driver.get_mut().take() {
            self.close_driver(driver).await;
        }
        Ok(())
    }
}

#[cfg(test)]
mod source_tests {
    use super::{AMQP_FRAME_MAX, Failure, RabbitMqSource, merge_frame_max};
    use crate::{batch::PendingBatch, config::RabbitMqSourceConfig};
    use iggy_connector_sdk::{ProducedMessage, Schema, Source, source::SourceBatchResult};
    use lapin::message::Delivery;
    use std::sync::atomic::Ordering;
    use std::time::Duration;

    fn source() -> RabbitMqSource {
        let config: RabbitMqSourceConfig =
            serde_json::from_str(r#"{"amqp_url":"amqp://localhost","queue":"events"}"#)
                .expect("configuration must deserialize");
        RabbitMqSource::new(1, config, None)
    }

    fn message(payload: Vec<u8>) -> ProducedMessage {
        ProducedMessage {
            id: None,
            checksum: None,
            timestamp: None,
            origin_timestamp: None,
            headers: None,
            payload,
        }
    }

    #[test]
    fn given_frame_max_should_be_added_but_never_overridden() {
        let adjusted = merge_frame_max("amqp://localhost/%2f?heartbeat=30");
        let url = url::Url::parse(&adjusted).expect("adjusted URL must be valid");
        assert_eq!(
            url.query_pairs()
                .find(|(name, _)| name == "frame_max")
                .map(|(_, value)| value.into_owned()),
            Some(AMQP_FRAME_MAX.to_string())
        );
        assert!(adjusted.contains("heartbeat=30"));

        for input in [
            "amqp://localhost/%2f?frame_max=65536",
            "amqp://localhost/%2f?frame_max=4096&heartbeat=30",
        ] {
            assert_eq!(merge_frame_max(input), input);
        }
    }

    #[test]
    fn given_lapin_error_with_url_secrets_should_not_expose_error_details() {
        let secret_url = "amqp://user:password@broker.example/private?token=secret";
        let error = std::io::Error::other(secret_url);
        let failure = Failure::lapin("consumer_setup", error.into());

        assert_eq!(failure.operation, "consumer_setup");
        let message = failure.error.to_string();
        assert!(message.contains("consumer_setup"));
        for secret in [
            "user",
            "password",
            "broker.example",
            "private",
            "token",
            "secret",
        ] {
            assert!(!message.contains(secret));
        }
    }

    #[test]
    fn given_broker_protocol_error_should_preserve_safe_details() {
        for (code, description) in [(403, "ACCESS-REFUSED"), (404, "NOT-FOUND")] {
            let broker_error = lapin::protocol::AMQPError::from_id(
                code,
                "amqp://user:password@broker?token=secret".into(),
            )
            .unwrap();
            let error = lapin::ErrorKind::ProtocolError(broker_error).into();
            let failure = Failure::lapin("basic_consume", error);
            let message = failure.error.to_string();

            assert_eq!(failure.reply_code, Some(code.to_string()));
            assert!(message.contains(&code.to_string()));
            assert!(message.to_uppercase().contains(description));
            assert!(!message.contains("password"));
            assert!(!message.contains("secret"));
        }
    }

    #[test]
    fn given_invalid_url_with_credentials_should_not_expose_credentials() {
        let config: RabbitMqSourceConfig = serde_json::from_str(
            r#"{"amqp_url":"amqp://user:password@[invalid/private?token=secret","queue":"events"}"#,
        )
        .expect("configuration must deserialize");
        let error = config
            .validate()
            .expect_err("URL must be invalid")
            .to_string();
        for secret in ["user", "password", "private", "token", "secret"] {
            assert!(!error.contains(secret));
        }
    }

    #[test]
    fn given_pending_batch_when_nacked_should_make_it_available_for_replay() {
        let runtime = tokio::runtime::Runtime::new().expect("runtime must start");
        runtime.block_on(async {
            let source = source();
            let delivery = Delivery::mock(1, "".into(), "events".into(), false, vec![1]);
            let batch =
                PendingBatch::new(Schema::Raw, vec![(message(delivery.data), delivery.acker)]);
            let first = batch.next_attempt();
            assert!(first.state.is_none());
            *source.pending.lock().await = Some(batch);
            source
                .on_batch_result(SourceBatchResult::Nack)
                .await
                .expect("Nack must succeed");
            let mut pending = source.pending.lock().await;
            let replay = pending
                .as_mut()
                .expect("batch must remain pending")
                .next_attempt();
            assert_eq!(replay.messages[0].payload, first.messages[0].payload);
        });
    }

    #[test]
    fn given_supplied_state_should_start_without_persisted_state() {
        let config: RabbitMqSourceConfig =
            serde_json::from_str(r#"{"amqp_url":"amqp://localhost","queue":"events"}"#)
                .expect("configuration must deserialize");
        let source = RabbitMqSource::new(
            1,
            config,
            Some(iggy_connector_sdk::ConnectorState(b"unused".to_vec())),
        );
        assert!(source.empty_batch().state.is_none());
    }

    #[test]
    fn given_pending_batch_when_closed_twice_should_not_acknowledge() {
        let runtime = tokio::runtime::Runtime::new().expect("runtime must start");
        runtime.block_on(async {
            let mut source = source();
            let delivery = Delivery::mock(1, "".into(), "events".into(), false, vec![1]);
            let acker = delivery.acker.clone();
            *source.pending.lock().await = Some(PendingBatch::new(
                Schema::Raw,
                vec![(message(delivery.data), delivery.acker)],
            ));
            source.close().await.expect("first close must succeed");
            source.close().await.expect("second close must succeed");
            assert!(source.pending.lock().await.is_none());
            assert!(
                acker
                    .ack(Default::default())
                    .await
                    .expect("acknowledgement must succeed")
            );
        });
    }

    #[tokio::test(start_paused = true)]
    async fn given_used_acknowledgement_when_batch_is_accepted_should_continue() {
        let source = source();
        let delivery = Delivery::mock(1, "".into(), "events".into(), false, vec![1]);
        assert!(
            delivery
                .acker
                .ack(Default::default())
                .await
                .expect("first acknowledgement must succeed")
        );
        *source.pending.lock().await = Some(PendingBatch::new(
            Schema::Raw,
            vec![(message(delivery.data), delivery.acker)],
        ));
        source
            .on_batch_result(SourceBatchResult::Ack)
            .await
            .expect("a failed acknowledgement must not stop the source");
        assert!(source.pending.lock().await.is_none());
        assert_eq!(source.consecutive_ack_failures.load(Ordering::Relaxed), 1);
        assert_eq!(source.retry_attempts.load(Ordering::Relaxed), 0);
    }

    #[tokio::test(start_paused = true)]
    async fn given_repeated_failed_acknowledgements_should_count_and_back_off() {
        let source = source();
        for attempt in 0u32..2 {
            let started = tokio::time::Instant::now();
            let delivery = Delivery::mock(1, "".into(), "events".into(), false, vec![1]);
            assert!(
                delivery
                    .acker
                    .ack(Default::default())
                    .await
                    .expect("first acknowledgement must succeed")
            );
            *source.pending.lock().await = Some(PendingBatch::new(
                Schema::Raw,
                vec![(message(delivery.data), delivery.acker)],
            ));
            source
                .on_batch_result(SourceBatchResult::Ack)
                .await
                .expect("a failed acknowledgement must not stop the source");
            assert_eq!(
                source.consecutive_ack_failures.load(Ordering::Relaxed),
                attempt + 1
            );
            assert!(started.elapsed() >= Duration::from_secs(1u64 << attempt));
        }
    }

    #[tokio::test(start_paused = true)]
    async fn given_successful_acknowledgement_should_reset_retry_counters() {
        let source = source();
        source.retry_attempts.store(3, Ordering::Relaxed);
        source.consecutive_ack_failures.store(2, Ordering::Relaxed);
        let delivery = Delivery::mock(1, "".into(), "events".into(), false, vec![1]);
        *source.pending.lock().await = Some(PendingBatch::new(
            Schema::Raw,
            vec![(message(delivery.data), delivery.acker)],
        ));
        source
            .on_batch_result(SourceBatchResult::Ack)
            .await
            .expect("Ack must succeed");
        assert_eq!(source.retry_attempts.load(Ordering::Relaxed), 0);
        assert_eq!(source.consecutive_ack_failures.load(Ordering::Relaxed), 0);
    }

    #[test]
    fn given_pending_delivery_when_nacked_then_acked_should_acknowledge_once() {
        let runtime = tokio::runtime::Runtime::new().expect("runtime must start");
        runtime.block_on(async {
            let source = source();
            let delivery = Delivery::mock(1, "".into(), "events".into(), false, vec![1]);
            let acker = delivery.acker.clone();
            let batch =
                PendingBatch::new(Schema::Raw, vec![(message(delivery.data), delivery.acker)]);
            batch.next_attempt();
            *source.pending.lock().await = Some(batch);
            source
                .on_batch_result(SourceBatchResult::Nack)
                .await
                .expect("Nack must succeed");
            assert!(acker.usable());
            source
                .pending
                .lock()
                .await
                .as_mut()
                .expect("batch must remain pending")
                .next_attempt();
            source
                .on_batch_result(SourceBatchResult::Ack)
                .await
                .expect("Ack must succeed");
            assert!(!acker.usable());
            assert!(source.pending.lock().await.is_none());
        });
    }

    #[test]
    fn given_deferred_preparation_failure_when_batch_is_retried_should_wait_for_ack() {
        let runtime = tokio::runtime::Runtime::new().expect("runtime must start");
        runtime.block_on(async {
            let source = source();
            let delivery = Delivery::mock(1, "".into(), "events".into(), false, vec![1]);
            let acker = delivery.acker.clone();
            let batch =
                PendingBatch::new(Schema::Raw, vec![(message(delivery.data), delivery.acker)]);
            batch.next_attempt();
            *source.pending.lock().await = Some(batch);
            *source.deferred_failure.lock().await = Some(Failure::simple("prepare", "failed"));

            source
                .on_batch_result(SourceBatchResult::Nack)
                .await
                .expect("Nack must succeed");
            assert!(source.deferred_failure.lock().await.is_some());
            assert!(acker.usable());
            source
                .pending
                .lock()
                .await
                .as_mut()
                .expect("batch must remain pending")
                .next_attempt();
            source
                .on_batch_result(SourceBatchResult::Ack)
                .await
                .expect("Ack must succeed");
            assert!(!acker.usable());
            assert!(source.deferred_failure.lock().await.is_none());
        });
    }
}

#[cfg(test)]
mod tests {
    use super::RabbitMqSource;
    use super::batch::PendingBatch;
    use super::config::RabbitMqSourceConfig;
    use iggy_connector_sdk::{ProducedMessage, Schema, Source};
    use lapin::message::Delivery;
    use std::num::NonZeroU32;

    #[test]
    fn given_minimal_configuration_should_apply_defaults() {
        let config: RabbitMqSourceConfig = serde_json::from_str(
            r#"{"amqp_url":"amqp://guest:guest@localhost:5672","queue":"events"}"#,
        )
        .expect("configuration must deserialize");
        assert_eq!(config.batch_size, 100);
        assert_eq!(config.batch_timeout, "10ms");
        assert!(!config.include_metadata);
        assert_eq!(config.timeout_secs, 30);
        assert_eq!(config.max_consecutive_nacks, None);
        assert_eq!(
            config.validate().unwrap(),
            std::time::Duration::from_millis(10)
        );
    }

    #[test]
    fn given_minimal_configuration_should_disable_the_nack_limit() {
        let config: RabbitMqSourceConfig =
            serde_json::from_str(r#"{"amqp_url":"amqp://localhost","queue":"events"}"#)
                .expect("configuration must deserialize");
        let source = RabbitMqSource::new(1, config, None);
        assert_eq!(source.batch_policy().max_consecutive_nacks(), None);
    }

    #[test]
    fn given_max_consecutive_nacks_should_set_the_sdk_limit() {
        let config: RabbitMqSourceConfig = serde_json::from_str(
            r#"{"amqp_url":"amqp://localhost","queue":"events","max_consecutive_nacks":12}"#,
        )
        .expect("configuration must deserialize");
        let source = RabbitMqSource::new(1, config, None);
        assert_eq!(
            source.batch_policy().max_consecutive_nacks(),
            NonZeroU32::new(12)
        );
    }

    #[test]
    fn given_zero_max_consecutive_nacks_should_reject_configuration() {
        let config: RabbitMqSourceConfig = serde_json::from_str(
            r#"{"amqp_url":"amqp://localhost","queue":"events","max_consecutive_nacks":0}"#,
        )
        .expect("configuration must deserialize");
        assert!(config.validate().is_err());
    }

    #[test]
    fn given_zero_batch_size_should_reject_configuration() {
        let config: RabbitMqSourceConfig = serde_json::from_str(
            r#"{"amqp_url":"amqp://localhost","queue":"events","batch_size":0}"#,
        )
        .expect("configuration must deserialize");
        assert!(config.validate().is_err());
    }

    #[test]
    fn given_maximum_batch_size_should_accept_configuration() {
        let config: RabbitMqSourceConfig = serde_json::from_value(serde_json::json!({
            "amqp_url": "amqp://localhost",
            "queue": "events",
            "batch_size": u16::MAX,
        }))
        .expect("configuration must deserialize");
        assert!(config.validate().is_ok());
        assert_eq!(config.batch_size, u16::MAX);
    }

    #[test]
    fn given_batch_size_above_protocol_bounds_should_fail_deserialization() {
        let config = serde_json::from_value::<RabbitMqSourceConfig>(serde_json::json!({
            "amqp_url": "amqp://localhost",
            "queue": "events",
            "batch_size": u32::from(u16::MAX) + 1,
        }));
        assert!(config.is_err());
    }

    #[test]
    fn given_invalid_configuration_should_reject_it() {
        let cases = [
            serde_json::json!({"amqp_url":"http://localhost","queue":"events"}),
            serde_json::json!({"amqp_url":"amqp://localhost","queue":""}),
            serde_json::json!({"amqp_url":"amqp://localhost","queue":"events","batch_timeout":"0s"}),
            serde_json::json!({"amqp_url":"amqp://localhost","queue":"events","timeout_secs":0}),
            serde_json::json!({"amqp_url":"amqp://localhost","queue":"events","timeout_secs":u64::MAX}),
            serde_json::json!({"amqp_url":"amqp://localhost","queue":"events","tls":{}}),
            serde_json::json!({"amqp_url":"amqps://localhost","queue":"events","tls":{"client_cert_file":"cert.pem"}}),
            serde_json::json!({"amqp_url":"amqp://localhost","queue":"x".repeat(256)}),
        ];
        for value in cases {
            let config: RabbitMqSourceConfig =
                serde_json::from_value(value).expect("configuration must deserialize");
            assert!(config.validate().is_err());
        }
    }

    #[test]
    fn given_pending_batch_should_return_same_messages_for_each_attempt() {
        let delivery = Delivery::mock(1, "".into(), "events".into(), false, vec![1, 2]);
        let message = ProducedMessage {
            id: Some(42),
            checksum: None,
            timestamp: None,
            origin_timestamp: None,
            headers: None,
            payload: delivery.data.clone(),
        };
        let batch = PendingBatch::new(Schema::Raw, vec![(message, delivery.acker)]);
        let first = batch.next_attempt();
        let replay = batch.next_attempt();
        assert_eq!(replay.schema, first.schema);
        assert_eq!(replay.messages[0].id, first.messages[0].id);
        assert_eq!(replay.messages[0].payload, first.messages[0].payload);
    }

    #[test]
    fn given_used_acknowledgement_when_acknowledged_should_return_error() {
        let runtime = tokio::runtime::Runtime::new().expect("runtime must start");
        runtime.block_on(async {
            let delivery = Delivery::mock(1, "".into(), "events".into(), false, vec![1]);
            assert!(
                delivery
                    .acker
                    .ack(Default::default())
                    .await
                    .expect("first acknowledgement must succeed")
            );
            let message = ProducedMessage {
                id: None,
                checksum: None,
                timestamp: None,
                origin_timestamp: None,
                headers: None,
                payload: delivery.data,
            };
            let batch = PendingBatch::new(Schema::Raw, vec![(message, delivery.acker)]);
            assert!(batch.acknowledge().await.is_err());
        });
    }
}
