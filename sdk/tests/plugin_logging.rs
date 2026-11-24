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

//! The logging of a plugin is process-wide, so it is tested in a binary of its own.

use async_trait::async_trait;
use iggy_connector_sdk::sink::SinkContainer;
use iggy_connector_sdk::source::SourceContainer;
use iggy_connector_sdk::{
    ConnectorState, ConsumedMessage, Error, MessagesMetadata, ProducedMessages, Sink, Source,
    TopicMetadata,
};
use std::sync::Mutex;

const WARN: u8 = 3;
const ERROR: u8 = 4;

/// Level, target and message of every event the plugin handed to the runtime.
static FORWARDED: Mutex<Vec<(u8, String, String)>> = Mutex::new(Vec::new());

extern "C" fn record_event(
    level: u8,
    target_ptr: *const u8,
    target_len: usize,
    message_ptr: *const u8,
    message_len: usize,
) {
    let (target, message) = unsafe {
        (
            std::slice::from_raw_parts(target_ptr, target_len),
            std::slice::from_raw_parts(message_ptr, message_len),
        )
    };
    FORWARDED.lock().expect("recorder was poisoned").push((
        level,
        String::from_utf8_lossy(target).into_owned(),
        String::from_utf8_lossy(message).into_owned(),
    ));
}

#[derive(Debug)]
struct QuietSink;

#[async_trait]
impl Sink for QuietSink {
    async fn open(&mut self) -> Result<(), Error> {
        Ok(())
    }

    async fn consume(
        &self,
        _topic_metadata: &TopicMetadata,
        _messages_metadata: MessagesMetadata,
        _messages: Vec<ConsumedMessage>,
    ) -> Result<(), Error> {
        Ok(())
    }

    async fn close(&mut self) -> Result<(), Error> {
        Ok(())
    }
}

#[test]
fn given_an_open_plugin_when_a_dependency_logs_through_the_log_crate_should_forward_the_record() {
    let mut container = SinkContainer::<QuietSink>::new(1);
    let config = b"{}";
    let status = unsafe {
        container.open(
            1,
            config.as_ptr(),
            config.len(),
            record_event,
            |_, _: serde_json::Value| QuietSink,
        )
    };
    assert_eq!(status, 0, "the sink should open");

    // How OpenDAL's retry layer reports a retry.
    tracing_log::log::warn!(target: "opendal::layers::retry", "will retry Write after 1s");

    let forwarded = FORWARDED.lock().expect("recorder was poisoned");
    assert!(
        forwarded
            .iter()
            .any(|(level, target, message)| *level == WARN
                && target == "opendal::layers::retry"
                && message.contains("will retry Write")),
        "the record should reach the runtime with its own target and level: {forwarded:?}"
    );
}

#[derive(Debug)]
struct FailingSink;

#[async_trait]
impl Sink for FailingSink {
    async fn open(&mut self) -> Result<(), Error> {
        Err(Error::InitError(
            "sink endpoint refused the connection".to_owned(),
        ))
    }

    async fn consume(
        &self,
        _topic_metadata: &TopicMetadata,
        _messages_metadata: MessagesMetadata,
        _messages: Vec<ConsumedMessage>,
    ) -> Result<(), Error> {
        Ok(())
    }

    async fn close(&mut self) -> Result<(), Error> {
        Ok(())
    }
}

#[derive(Debug)]
struct FailingSource;

#[async_trait]
impl Source for FailingSource {
    async fn open(&mut self) -> Result<(), Error> {
        Err(Error::InitError(
            "source endpoint refused the connection".to_owned(),
        ))
    }

    async fn poll(&self) -> Result<ProducedMessages, Error> {
        Err(Error::InitError("never polled".to_owned()))
    }

    async fn close(&mut self) -> Result<(), Error> {
        Ok(())
    }
}

/// A copy, so a failed assert does not poison the recorder while another test logs.
fn forwarded_events() -> Vec<(u8, String, String)> {
    FORWARDED.lock().expect("recorder was poisoned").clone()
}

fn has_error(events: &[(u8, String, String)], reason: &str, id: u32) -> bool {
    events.iter().any(|(level, _, message)| {
        *level == ERROR && message.contains(reason) && message.contains(&format!("ID: {id}"))
    })
}

#[test]
fn given_a_sink_whose_open_fails_when_opening_should_forward_the_reason() {
    let mut container = SinkContainer::<FailingSink>::new(7);
    let config = b"{}";
    let status = unsafe {
        container.open(
            7,
            config.as_ptr(),
            config.len(),
            record_event,
            |_, _: serde_json::Value| FailingSink,
        )
    };

    assert_ne!(status, 0, "the sink should fail to open");
    let events = forwarded_events();
    assert!(
        has_error(&events, "sink endpoint refused the connection", 7),
        "the reason should reach the runtime at error level: {events:?}"
    );
}

#[test]
fn given_a_source_whose_open_fails_when_opening_should_forward_the_reason() {
    let mut container = SourceContainer::<FailingSource>::new(8);
    let config = b"{}";
    let status = unsafe {
        container.open(
            8,
            config.as_ptr(),
            config.len(),
            std::ptr::null(),
            0,
            record_event,
            |_, _: serde_json::Value, _: Option<ConnectorState>| FailingSource,
        )
    };

    assert_ne!(status, 0, "the source should fail to open");
    let events = forwarded_events();
    assert!(
        has_error(&events, "source endpoint refused the connection", 8),
        "the reason should reach the runtime at error level: {events:?}"
    );
}
