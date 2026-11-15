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
use testcontainers_modules::testcontainers::core::{IntoContainerPort, Mount, WaitFor};
use testcontainers_modules::testcontainers::runners::AsyncRunner;
use testcontainers_modules::testcontainers::{ContainerAsync, GenericImage, ImageExt};

const FLUSS_IMAGE: &str = "apache/fluss";
const FLUSS_VERSION: &str = "1.0.0";
const READY_MESSAGE: &str = "Registered tablet server 0";
const CLIENT_LISTENER: &str = "CLIENT";
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
}

impl FlussContainer {
    pub(super) async fn start() -> Result<Self, TestBinaryError> {
        Self::start_with(None, "", None).await
    }

    /// A cluster whose client listener takes SASL/PLAIN from one user only. The two servers
    /// talk to each other over a second listener without it.
    pub(super) async fn start_with_sasl(
        username: &str,
        password: &str,
    ) -> Result<Self, TestBinaryError> {
        Self::start_with(None, "", Some((username, password))).await
    }

    /// A cluster that is stopped and started again within a test. Stopping the container
    /// kills the coordinator, whose leadership stays registered in ZooKeeper until its session
    /// times out, so the timeout is kept short and the next coordinator takes over sooner.
    /// The tablet server flushes every append and writes its checkpoints every second, since
    /// with the defaults it can come back without the rows written just before the stop.
    pub(super) async fn start_restartable() -> Result<Self, TestBinaryError> {
        Self::start_with(None, RESTARTABLE_PROPERTIES, None).await
    }

    /// Keeps the log and its tiered copy under `storage_dir`, mounted at the same path inside
    /// the container. The client reads a tiered segment from the path the server reports, so
    /// the path has to reach the same files on both sides. Segments are kept small and tiered
    /// every second, so a few appends reach remote storage within a test.
    pub(super) async fn start_with_tiered_storage(
        storage_dir: &Path,
    ) -> Result<Self, TestBinaryError> {
        Self::start_with(Some(storage_dir), "", None).await
    }

    async fn start_with(
        storage_dir: Option<&Path>,
        extra_properties: &str,
        sasl_user: Option<(&str, &str)>,
    ) -> Result<Self, TestBinaryError> {
        let [coordinator_port, tablet_port] = reserve_host_ports()?;

        let request = GenericImage::new(FLUSS_IMAGE, FLUSS_VERSION)
            .with_wait_for(WaitFor::message_on_stdout(READY_MESSAGE))
            .with_entrypoint("/bin/bash")
            .with_container_name(unique_container_name("fluss"))
            .with_mapped_port(coordinator_port, coordinator_port.tcp())
            .with_mapped_port(tablet_port, tablet_port.tcp())
            .with_env_var(
                "FLUSS_PROPERTIES",
                server_properties(coordinator_port, storage_dir, extra_properties, sasl_user),
            )
            .with_cmd(["-c", &startup_script(tablet_port, sasl_user.is_some())]);
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

        Ok(Self {
            container,
            bootstrap_servers: format!("localhost:{coordinator_port}"),
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
) -> String {
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
         bind.listeners: {internal_bind}{CLIENT_LISTENER}://0.0.0.0:{coordinator_port}\n\
         advertised.listeners: {CLIENT_LISTENER}://localhost:{coordinator_port}\n\
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
fn startup_script(tablet_port: u16, with_sasl: bool) -> String {
    let (internal_bind, _) = internal_listener(with_sasl);
    format!(
        "set -e\n\
         /docker-entrypoint.sh true\n\
         /opt/fluss/bin/fluss-daemon.sh start zookeeper /opt/fluss/conf/zookeeper.properties\n\
         /opt/fluss/bin/coordinator-server.sh start\n\
         exec /opt/fluss/bin/tablet-server.sh start-foreground \
         -Dbind.listeners={internal_bind}{CLIENT_LISTENER}://0.0.0.0:{tablet_port} \
         -Dadvertised.listeners={CLIENT_LISTENER}://localhost:{tablet_port}\n"
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
