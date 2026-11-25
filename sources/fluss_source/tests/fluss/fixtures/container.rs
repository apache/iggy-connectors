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

use iggy_connector_testing::unique_container_name;
use integration::harness::TestBinaryError;
use std::net::TcpListener;
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use testcontainers_modules::testcontainers::core::{IntoContainerPort, Mount, WaitFor};
use testcontainers_modules::testcontainers::runners::AsyncRunner;
use testcontainers_modules::testcontainers::{ContainerAsync, GenericImage, ImageExt};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::tcp::{OwnedReadHalf, OwnedWriteHalf};
use tokio::net::{TcpListener as RelayListener, TcpStream};

const FLUSS_IMAGE: &str = "apache/fluss";
const FLUSS_VERSION: &str = "1.0.0";
const READY_MESSAGE: &str = "Registered tablet server 0";
const CLIENT_LISTENER: &str = "CLIENT";
const RELAYED_LISTENER: &str = "RELAYED";
const RELAY_BUFFER_SIZE: usize = 64 * 1024;
const RESTARTABLE_PROPERTIES: &str = "zookeeper.client.session-timeout: 6s\n\
     log.flush.interval-messages: 1\n\
     log.flush.offset.checkpoint-interval: 1s\n\
     log.replica.high-watermark.checkpoint-interval: 1s\n";

pub(super) const ENV_SOURCE_BOOTSTRAP_SERVERS: &str =
    "IGGY_CONNECTORS_SOURCE_FLUSS_PLUGIN_CONFIG_BOOTSTRAP_SERVERS";
pub(super) const ENV_SOURCE_DATABASE: &str = "IGGY_CONNECTORS_SOURCE_FLUSS_PLUGIN_CONFIG_DATABASE";
pub(super) const ENV_SOURCE_TABLE: &str = "IGGY_CONNECTORS_SOURCE_FLUSS_PLUGIN_CONFIG_TABLE";
pub(super) const ENV_SOURCE_COLUMNS: &str = "IGGY_CONNECTORS_SOURCE_FLUSS_PLUGIN_CONFIG_COLUMNS";
pub(super) const ENV_SOURCE_STARTING_OFFSET: &str =
    "IGGY_CONNECTORS_SOURCE_FLUSS_PLUGIN_CONFIG_STARTING_OFFSET";
pub(super) const ENV_SOURCE_POLL_INTERVAL: &str =
    "IGGY_CONNECTORS_SOURCE_FLUSS_PLUGIN_CONFIG_POLL_INTERVAL";
pub(super) const ENV_SOURCE_POLL_TIMEOUT: &str =
    "IGGY_CONNECTORS_SOURCE_FLUSS_PLUGIN_CONFIG_POLL_TIMEOUT";
pub(super) const ENV_SOURCE_REQUEST_TIMEOUT: &str =
    "IGGY_CONNECTORS_SOURCE_FLUSS_PLUGIN_CONFIG_REQUEST_TIMEOUT";
pub(super) const ENV_SOURCE_INCLUDE_METADATA: &str =
    "IGGY_CONNECTORS_SOURCE_FLUSS_PLUGIN_CONFIG_INCLUDE_METADATA";
pub(super) const ENV_SOURCE_SASL_USERNAME: &str =
    "IGGY_CONNECTORS_SOURCE_FLUSS_PLUGIN_CONFIG_SASL_USERNAME";
pub(super) const ENV_SOURCE_SASL_PASSWORD: &str =
    "IGGY_CONNECTORS_SOURCE_FLUSS_PLUGIN_CONFIG_SASL_PASSWORD";
pub(super) const ENV_SOURCE_BATCH_SIZE: &str =
    "IGGY_CONNECTORS_SOURCE_FLUSS_PLUGIN_CONFIG_BATCH_SIZE";
pub(super) const ENV_SOURCE_STREAMS_0_STREAM: &str =
    "IGGY_CONNECTORS_SOURCE_FLUSS_STREAMS_0_STREAM";
pub(super) const ENV_SOURCE_STREAMS_0_TOPIC: &str = "IGGY_CONNECTORS_SOURCE_FLUSS_STREAMS_0_TOPIC";
pub(super) const ENV_SOURCE_STREAMS_0_SCHEMA: &str =
    "IGGY_CONNECTORS_SOURCE_FLUSS_STREAMS_0_SCHEMA";
pub(super) const ENV_SOURCE_PATH: &str = "IGGY_CONNECTORS_SOURCE_FLUSS_PATH";

/// A whole Fluss cluster (embedded ZooKeeper, coordinator server, tablet server) inside one
/// container.
///
/// The image ships `local-cluster.sh`, which starts the same three processes, but it rewrites
/// the tablet server's bind port to 0 so it cannot collide with the coordinator. A random port
/// inside the container cannot be published to the host, so the tablet server is given an
/// explicit second port here instead.
///
/// Both servers advertise `localhost`, which resolves to the coordinator/tablet pair inside the
/// container and to the published ports from the test process. That only holds while the host
/// and container port numbers match, so free host ports are reserved up front and mapped
/// one-to-one rather than letting Docker assign them.
pub(super) struct FlussContainer {
    container: ContainerAsync<GenericImage>,
    pub(super) bootstrap_servers: String,
    relay: Option<(Relay, String)>,
}

impl FlussContainer {
    pub(super) async fn start() -> Result<Self, TestBinaryError> {
        Self::start_with(None, "", None, false).await
    }

    /// A cluster with a second client listener that both servers advertise at a [`Relay`] on
    /// the host. A client that bootstraps through the relay keeps every connection on it.
    pub(super) async fn start_behind_relay() -> Result<Self, TestBinaryError> {
        Self::start_with(None, "", None, true).await
    }

    /// The bootstrap address that goes through the relay, on a cluster started behind one.
    pub(super) fn relayed_bootstrap_servers(&self) -> Option<&str> {
        self.relay.as_ref().map(|(_, address)| address.as_str())
    }

    pub(super) fn relay(&self) -> Option<&Relay> {
        self.relay.as_ref().map(|(relay, _)| relay)
    }

    /// A cluster whose client listener takes SASL/PLAIN from one user only. The two servers
    /// talk to each other over a second listener without it.
    pub(super) async fn start_with_sasl(
        username: &str,
        password: &str,
    ) -> Result<Self, TestBinaryError> {
        Self::start_with(None, "", Some((username, password)), false).await
    }

    /// A cluster that is stopped and started again within a test. Stopping the container
    /// kills the coordinator, whose leadership stays registered in ZooKeeper until its session
    /// times out, so the timeout is kept short and the next coordinator takes over sooner.
    /// The tablet server flushes every append and writes its checkpoints every second, since
    /// with the defaults it can come back without the rows written just before the stop.
    pub(super) async fn start_restartable() -> Result<Self, TestBinaryError> {
        Self::start_with(None, RESTARTABLE_PROPERTIES, None, false).await
    }

    /// Keeps the log and its tiered copy under `storage_dir`, mounted at the same path inside
    /// the container. The client reads a tiered segment from the path the server reports, so
    /// the path has to reach the same files on both sides. Segments are kept small and tiered
    /// every second, so a few appends reach remote storage within a test.
    pub(super) async fn start_with_tiered_storage(
        storage_dir: &Path,
    ) -> Result<Self, TestBinaryError> {
        Self::start_with(Some(storage_dir), "", None, false).await
    }

    async fn start_with(
        storage_dir: Option<&Path>,
        extra_properties: &str,
        sasl_user: Option<(&str, &str)>,
        behind_relay: bool,
    ) -> Result<Self, TestBinaryError> {
        let [coordinator_port, tablet_port] = reserve_host_ports()?;
        let relayed = if behind_relay {
            Some(RelayedListeners::reserve()?)
        } else {
            None
        };
        let coordinator_relayed = relayed.as_ref().map(|relayed| relayed.coordinator);
        let tablet_relayed = relayed.as_ref().map(|relayed| relayed.tablet);

        let request = GenericImage::new(FLUSS_IMAGE, FLUSS_VERSION)
            .with_wait_for(WaitFor::message_on_stdout(READY_MESSAGE))
            .with_entrypoint("/bin/bash")
            .with_container_name(unique_container_name("fluss"))
            .with_mapped_port(coordinator_port, coordinator_port.tcp())
            .with_mapped_port(tablet_port, tablet_port.tcp())
            .with_env_var(
                "FLUSS_PROPERTIES",
                server_properties(
                    coordinator_port,
                    storage_dir,
                    extra_properties,
                    sasl_user,
                    coordinator_relayed,
                ),
            )
            .with_cmd([
                "-c",
                &startup_script(tablet_port, sasl_user.is_some(), tablet_relayed),
            ]);
        let request = [coordinator_relayed, tablet_relayed]
            .into_iter()
            .flatten()
            .fold(request, |request, listener| {
                request.with_mapped_port(listener.port, listener.port.tcp())
            });
        let request = match storage_dir {
            Some(dir) => {
                let dir = dir.to_string_lossy();
                request
                    .with_mount(Mount::bind_mount(dir.as_ref(), dir.as_ref()))
                    .with_security_opt("label=disable")
            }
            None => request,
        };
        let container = request
            .start()
            .await
            .map_err(|error| TestBinaryError::FixtureSetup {
                fixture_type: "FlussContainer".to_string(),
                message: format!("Failed to start container: {error}"),
            })?;

        let relay = relayed.map(RelayedListeners::start).transpose()?;
        Ok(Self {
            container,
            bootstrap_servers: format!("localhost:{coordinator_port}"),
            relay,
        })
    }

    /// Stops ZooKeeper and both servers. The tables and their logs stay on the container's
    /// disk, and the published ports stay the same for [`Self::start_again`].
    pub(super) async fn stop(&self) -> Result<(), TestBinaryError> {
        self.container
            .stop_with_timeout(None)
            .await
            .map_err(|error| TestBinaryError::FixtureSetup {
                fixture_type: "FlussContainer".to_string(),
                message: format!("Failed to stop container: {error}"),
            })
    }

    /// Returns once the container runs again. The cluster needs a moment more before it takes
    /// requests, which the fixture's retries cover.
    pub(super) async fn start_again(&self) -> Result<(), TestBinaryError> {
        self.container
            .start()
            .await
            .map_err(|error| TestBinaryError::FixtureSetup {
                fixture_type: "FlussContainer".to_string(),
                message: format!("Failed to start container again: {error}"),
            })
    }
}

/// The coordinator settings land in `server.yaml`. The tablet server inherits them and
/// overrides only its listeners on the command line.
fn server_properties(
    coordinator_port: u16,
    storage_dir: Option<&Path>,
    extra_properties: &str,
    sasl_user: Option<(&str, &str)>,
    relayed: Option<RelayedListener>,
) -> String {
    let (relayed_bind, relayed_advertised) = relayed_listeners(relayed);
    let security = match sasl_user {
        Some((username, password)) => format!(
            "security.protocol.map: {CLIENT_LISTENER}:sasl\n\
             security.sasl.enabled.mechanisms: plain\n\
             security.sasl.plain.jaas.config: \
             org.apache.fluss.security.auth.sasl.plain.PlainLoginModule required \
             user_{username}=\"{password}\";\n"
        ),
        None => String::new(),
    };
    let (internal_bind, internal_listener) = internal_listener(sasl_user.is_some());
    let storage = match storage_dir {
        Some(dir) => format!(
            "data.dir: {dir}/data\n\
             remote.data.dir: {dir}/remote\n\
             log.segment.file-size: 1kb\n\
             remote.log.task-interval-duration: 1s\n",
            dir = dir.display()
        ),
        None => "data.dir: /tmp/fluss/data\nremote.data.dir: /tmp/fluss/remote-data\n".to_owned(),
    };
    format!(
        "zookeeper.address: localhost:2181\n\
         bind.listeners: {internal_bind}{CLIENT_LISTENER}://0.0.0.0:{coordinator_port}{relayed_bind}\n\
         advertised.listeners: {CLIENT_LISTENER}://localhost:{coordinator_port}{relayed_advertised}\n\
         internal.listener.name: {internal_listener}\n\
         default.bucket.number: 1\n\
         default.replication.factor: 1\n\
         {storage}\
         {security}\
         {extra_properties}\
         tablet-server.id: 0\n"
    )
}

/// `/docker-entrypoint.sh true` only runs the image's configuration step, which appends
/// `FLUSS_PROPERTIES` to `server.yaml`. The tablet server runs in the foreground so the
/// container stays alive with it.
fn startup_script(tablet_port: u16, with_sasl: bool, relayed: Option<RelayedListener>) -> String {
    let (internal_bind, _) = internal_listener(with_sasl);
    let (relayed_bind, relayed_advertised) = relayed_listeners(relayed);
    format!(
        "set -e\n\
         /docker-entrypoint.sh true\n\
         /opt/fluss/bin/fluss-daemon.sh start zookeeper /opt/fluss/conf/zookeeper.properties\n\
         /opt/fluss/bin/coordinator-server.sh start\n\
         exec /opt/fluss/bin/tablet-server.sh start-foreground \
         -Dbind.listeners={internal_bind}{CLIENT_LISTENER}://0.0.0.0:{tablet_port}{relayed_bind} \
         -Dadvertised.listeners={CLIENT_LISTENER}://localhost:{tablet_port}{relayed_advertised}\n"
    )
}

/// The start of `bind.listeners` and the name of the listener the servers use among
/// themselves. Without SASL that is the client listener. With SASL on the client listener
/// they get a listener of their own on a port the container keeps to itself.
fn internal_listener(with_sasl: bool) -> (&'static str, &'static str) {
    if with_sasl {
        ("INTERNAL://localhost:0,", "INTERNAL")
    } else {
        ("", CLIENT_LISTENER)
    }
}

/// What a server adds to `bind.listeners` and to `advertised.listeners` for its relayed
/// listener, if it has one.
fn relayed_listeners(relayed: Option<RelayedListener>) -> (String, String) {
    match relayed {
        Some(listener) => (
            format!(",{RELAYED_LISTENER}://0.0.0.0:{}", listener.port),
            format!(",{RELAYED_LISTENER}://127.0.0.1:{}", listener.relay_port),
        ),
        None => (String::new(), String::new()),
    }
}

/// The port a server's relayed listener binds, and the port of the relay it advertises there.
#[derive(Clone, Copy)]
struct RelayedListener {
    port: u16,
    relay_port: u16,
}

/// The relayed listeners of both servers, with the relay sockets already bound. The servers
/// have to be told the relay ports before they start, so the sockets are bound first and
/// start relaying once the servers are up.
struct RelayedListeners {
    coordinator: RelayedListener,
    tablet: RelayedListener,
    coordinator_relay: TcpListener,
    tablet_relay: TcpListener,
}

impl RelayedListeners {
    fn reserve() -> Result<Self, TestBinaryError> {
        let [coordinator_port, tablet_port] = reserve_host_ports()?;
        let coordinator_relay = TcpListener::bind("127.0.0.1:0").map_err(relay_error)?;
        let tablet_relay = TcpListener::bind("127.0.0.1:0").map_err(relay_error)?;
        Ok(Self {
            coordinator: RelayedListener {
                port: coordinator_port,
                relay_port: coordinator_relay.local_addr().map_err(relay_error)?.port(),
            },
            tablet: RelayedListener {
                port: tablet_port,
                relay_port: tablet_relay.local_addr().map_err(relay_error)?.port(),
            },
            coordinator_relay,
            tablet_relay,
        })
    }

    /// Returns the relay and the bootstrap address that goes through it.
    fn start(self) -> Result<(Relay, String), TestBinaryError> {
        let relay = Relay::default();
        relay.forward(self.coordinator_relay, self.coordinator.port)?;
        relay.forward(self.tablet_relay, self.tablet.port)?;
        Ok((relay, format!("127.0.0.1:{}", self.coordinator.relay_port)))
    }
}

/// Passes TCP connections on to the cluster until it is told to silence them. A silenced
/// connection stays open on both sides and carries nothing any more, which is how a server
/// looks that died without closing its sockets. Connections made afterwards pass again.
#[derive(Clone, Default)]
pub(super) struct Relay {
    connections: Arc<AtomicU64>,
    silenced_through: Arc<AtomicU64>,
}

impl Relay {
    /// Silences every connection made so far.
    pub(super) fn silence_connections(&self) {
        self.silenced_through
            .store(self.connections.load(Ordering::SeqCst), Ordering::SeqCst);
    }

    /// How many connections the relay took since it started.
    pub(super) fn connections(&self) -> u64 {
        self.connections.load(Ordering::SeqCst)
    }

    fn forward(&self, listener: TcpListener, server_port: u16) -> Result<(), TestBinaryError> {
        listener.set_nonblocking(true).map_err(relay_error)?;
        let listener = RelayListener::from_std(listener).map_err(relay_error)?;
        let relay = self.clone();
        tokio::spawn(async move {
            while let Ok((client, _)) = listener.accept().await {
                let connection = relay.connections.fetch_add(1, Ordering::SeqCst) + 1;
                let Ok(server) = TcpStream::connect(("127.0.0.1", server_port)).await else {
                    continue;
                };
                let (client_read, client_write) = client.into_split();
                let (server_read, server_write) = server.into_split();
                tokio::spawn(relay.clone().pass(connection, client_read, server_write));
                tokio::spawn(relay.clone().pass(connection, server_read, client_write));
            }
        });
        Ok(())
    }

    /// Copies one direction of a connection. Once the connection is silenced it keeps reading
    /// and drops what it reads, and never returns, so neither socket is closed.
    async fn pass(self, connection: u64, mut from: OwnedReadHalf, mut to: OwnedWriteHalf) {
        let mut buffer = vec![0u8; RELAY_BUFFER_SIZE];
        loop {
            let read = from.read(&mut buffer).await;
            let silenced = connection <= self.silenced_through.load(Ordering::SeqCst);
            match read {
                Ok(count) if count > 0 && silenced => {}
                _ if silenced => std::future::pending::<()>().await,
                Ok(count) if count > 0 => {
                    if to.write_all(&buffer[..count]).await.is_err() {
                        return;
                    }
                }
                _ => return,
            }
        }
    }
}

/// Takes connections and never answers on them. Returns the address it listens on.
pub(super) fn start_silent_server() -> Result<String, TestBinaryError> {
    let listener = TcpListener::bind("127.0.0.1:0").map_err(relay_error)?;
    let port = listener.local_addr().map_err(relay_error)?.port();
    listener.set_nonblocking(true).map_err(relay_error)?;
    let listener = RelayListener::from_std(listener).map_err(relay_error)?;
    tokio::spawn(async move {
        let mut connections = Vec::new();
        while let Ok((connection, _)) = listener.accept().await {
            connections.push(connection);
        }
    });
    Ok(format!("127.0.0.1:{port}"))
}

fn relay_error(error: std::io::Error) -> TestBinaryError {
    TestBinaryError::FixtureSetup {
        fixture_type: "Relay".to_string(),
        message: format!("Failed to set up a socket: {error}"),
    }
}

/// Binds port 0 once per port, reads back what the kernel picked, then releases them all.
/// Every listener stays open until the last port is read, so the kernel cannot hand out the
/// same port twice. The ports are only reserved by convention until the container claims
/// them, which is the same trade every fixture that needs a known port ahead of time makes.
pub(super) fn reserve_host_ports<const N: usize>() -> Result<[u16; N], TestBinaryError> {
    let mut listeners = Vec::with_capacity(N);
    let mut ports = [0; N];
    for port in &mut ports {
        let listener =
            TcpListener::bind("127.0.0.1:0").map_err(|error| TestBinaryError::FixtureSetup {
                fixture_type: "FlussContainer".to_string(),
                message: format!("Failed to reserve a host port: {error}"),
            })?;
        *port = listener
            .local_addr()
            .map_err(|error| TestBinaryError::FixtureSetup {
                fixture_type: "FlussContainer".to_string(),
                message: format!("Failed to read the reserved host port: {error}"),
            })?
            .port();
        listeners.push(listener);
    }
    Ok(ports)
}
