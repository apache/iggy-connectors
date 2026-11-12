# Apache Iggy Connectors

Connectors move data between [Apache Iggy](https://github.com/apache/iggy) streams and external systems. A source reads data from an external system and produces messages to an Iggy stream. A sink consumes messages from an Iggy stream and writes them to an external system. Each connector is a Rust plugin, and the connectors runtime loads the plugins and runs them.

This repository holds the runtime, the plugin SDK, the sinks and the sources. The connectors started in apache/iggy and moved here with their git history. The Iggy server, the client SDKs, the CLI and the Web UI stay in apache/iggy.

[Documentation](https://iggy.apache.org/docs/connectors/introduction) | [Apache Iggy](https://github.com/apache/iggy) | [Discussions](https://github.com/apache/iggy-connectors/discussions) | [Discord](https://discord.gg/apache-iggy) | [Docker image](https://hub.docker.com/r/apache/iggy-connect)

## Features

- **Plugins**: each connector is a shared library (`.so`, `.dylib`, `.dll`). It implements the `Sink` or the `Source` trait of the SDK, and the runtime loads it at startup.
- **Typed configuration**: each plugin has its own configuration type, and the runtime passes the configuration from TOML to the plugin.
- **Transforms**: configured transforms add, change, delete or filter the fields of each message between Iggy and the plugin.
- **Formats**: decoders and encoders for JSON, raw bytes, text, Protocol Buffers, FlatBuffers and Avro.
- **Configuration providers**: connector configurations come from local TOML files or from an HTTP API.
- **Delivery**: sources keep their state across restarts, sinks can deliver at least once, and sources can write each batch exactly once.
- **Observability**: Prometheus metrics with per-stage latency histograms, and an opt-in per-batch timing log.
- **Logging**: text or JSON logs, selected in the `[logging]` configuration.

The runtime ships as the Docker image [`apache/iggy-connect`](https://hub.docker.com/r/apache/iggy-connect). Run `docker pull apache/iggy-connect:edge` to fetch the image of the latest `master` commit.

## Quick start

Run these commands from the root of this repository. You need Docker and the Rust toolchain.

1. Build the runtime and the two plugins of this example:

    ```bash
    cargo build --release -p iggy-connectors \
      -p iggy_connector_random_source -p iggy_connector_stdout_sink
    ```

2. Start an Iggy server with the credentials of the example configuration:

    ```bash
    docker run -d --name iggy \
      --cap-add=SYS_NICE --security-opt seccomp=unconfined --ulimit memlock=-1:-1 \
      -e IGGY_ROOT_USERNAME=iggy -e IGGY_ROOT_PASSWORD=iggy \
      -e IGGY_TCP_ADDRESS=0.0.0.0:8090 -e IGGY_HTTP_ADDRESS=0.0.0.0:3000 \
      -e IGGY_NODE_ADVERTISED_ADDRESS=localhost \
      -p 8090:8090 -p 3000:3000 \
      apache/iggy
    ```

    The [Docker page](https://iggy.apache.org/docs/server/docker) of the Iggy documentation explains the options.

3. Create the stream and the topic of the example with the `iggy` CLI of the server image:

    ```bash
    docker exec iggy iggy -u iggy -p iggy stream create example_stream
    docker exec iggy iggy -u iggy -p iggy topic create example_stream example_topic 1 none 1d --durability persisted
    ```

4. Start the runtime with the example configuration:

    ```bash
    IGGY_CONNECTORS_CONFIG_PATH=runtime/example_config/config.toml \
      cargo run --release --bin iggy-connectors
    ```

The Random source produces messages to `example_stream`, and the Stdout sink prints them. The transforms of both connectors add a field to each message. The [Web UI](https://iggy.apache.org/docs/web_ui/start) of Iggy shows the stream too.

The example directory `runtime/example_config/connectors/` also enables other connectors, for example ClickHouse, Delta Lake and Quickwit. Without their plugin or their backend, the runtime reports them with the `Error` status, and the other connectors continue. Set `enabled = false` in their files to skip them.

For a debug build, remove `--release` from the commands and change `target/release` to `target/debug` in the `path` of the connector files.

### Quickwit sink

The Quickwit sink of the example reads the `qw` stream. To try it, you need a checkout of [apache/iggy](https://github.com/apache/iggy) for its example producer [`sink-data-producer`](https://github.com/apache/iggy/tree/master/examples/rust/src/sink-data-producer).

1. Build the plugin:

    ```bash
    cargo build --release -p iggy_connector_quickwit_sink
    ```

2. In the apache/iggy checkout, start Quickwit:

    ```bash
    docker compose -f examples/rust/src/sink-data-producer/docker-compose.yml up -d
    ```

3. Create the stream and the topic that the sink reads:

    ```bash
    docker exec iggy iggy -u iggy -p iggy stream create qw
    docker exec iggy iggy -u iggy -p iggy topic create qw records 1 none 1d
    ```

4. In the apache/iggy checkout, send 100 batches of records to the topic:

    ```bash
    cargo run --example sink-data-producer --release
    ```

5. If the runtime runs, stop it. Then start it again, as in step 4 of the quick start.

The Quickwit sink writes the records to the `events` index. The Quickwit UI at <http://localhost:7280> shows the index.

## Repository layout

| Path              | Package                  | Content                                               |
| ----------------- | ------------------------ | ----------------------------------------------------- |
| `runtime/`        | `iggy-connectors`        | The runtime binary and its example configuration      |
| `sdk/`            | `iggy_connector_sdk`     | Traits, FFI macros, decoders, encoders and transforms |
| `sinks/<name>/`   | `iggy_connector_<name>`  | One sink plugin with its integration tests            |
| `sources/<name>/` | `iggy_connector_<name>`  | One source plugin with its integration tests          |
| `testing/`        | `iggy_connector_testing` | Test fixtures that several test targets share         |
| `scripts/`        |                          | CI checks, test server build and the core pin bump    |
| `.claude/skills/` |                          | Skills for AI agents, linked from `.agents/skills/`   |

Only `iggy_connector_sdk` goes to crates.io. The runtime ships only as the Docker image, and the sinks and sources ship as source code.

## The core pin

This repository builds against one commit of apache/iggy, called the core pin. The root `Cargo.toml` holds the core pin as the `rev` of each core crate that comes from git: `iggy`, `iggy_common`, `configs`, `configs_derive`, `system_stats` and the test crate `integration`. The core pin is the last core release, unless a connector needs an unreleased core change, and `scripts/bump-core-pin.sh` moves it.

## Build

```bash
cargo build              # runtime and all plugins, into target/debug/
cargo build --release    # the same, into target/release/
```

The runtime binary is `target/<profile>/iggy-connectors`. On Linux, each plugin builds to `target/<profile>/libiggy_connector_<name>.so`.

## Tests

Unit tests live in `src/` of each crate and need no services:

```bash
cargo nextest run --workspace --lib --bins
```

Integration tests live in `tests/` of each package. They need Docker, an Iggy server built at the core pin, and fresh builds of the runtime and the plugins in `target/debug/`:

```bash
./scripts/build-test-server.sh
cargo build
cargo nextest run -p iggy_connector_postgres_sink
```

`scripts/build-test-server.sh` builds `iggy-server` from apache/iggy at the core pin and copies it to `target/debug/iggy-server`. Run it again after the core pin changes. The tests start that server and `target/debug/iggy-connectors`, but they never build them. After a change to the runtime or a plugin, run `cargo build` again.

`cargo nextest run --workspace` runs all tests, as CI does. The Doris tests need `sudo sysctl -w vm.max_map_count=2000000` on Linux. Per-test logs go to `test_logs/`.

## Runtime

The runtime manages the lifecycle of the connectors and gives them the infrastructure that they need to run. The [runtime documentation](runtime/README.md) describes its configuration, the HTTP API, the state storage and the delivery guarantees.

## Plugin path resolution

The `path` field in connector configs points to the shared library (`.so`, `.dylib`, `.dll`). The runtime resolves it as follows:

1. **Extension**: if the path has no known extension, the runtime adds the extension of the OS. That is `.so` on Linux, `.dylib` on macOS and `.dll` on Windows.

2. **Absolute paths**: used as they are.

3. **Relative paths**: the runtime tries these locations in order and uses the first match:
   - the literal relative path, from the working directory
   - the file name in the directory of the runtime binary
   - the file name in the working directory
   - the file name in `/usr/lib`, `/usr/lib64`, `/lib`, `/lib64`, `/usr/local/lib` and `/usr/local/lib64`

**Examples:**

```toml
# Relative: resolved against the search directories, extension added on Linux
path = "target/release/libiggy_connector_stdout_sink"

# Absolute: used directly
path = "/opt/iggy/plugins/libiggy_connector_stdout_sink.so"
```

If the runtime does not find the library, it logs all the paths that it tried.

## Sinks

A sink consumes the messages from the configured streams and topics and sends them to its destination. For example, the Quickwit sink sends the messages to the Quickwit indexer. The [sink documentation](sinks/README.md) describes the configuration and a sample implementation.

A `Sink` implementation uses the `sink_connector!` macro, which exposes the FFI interface that the runtime calls. The macro also exports the version of the connector from its `Cargo.toml`, and the runtime reports it in its `/stats` endpoint. Each sink has its own configuration type, and its `new()` method receives that configuration with the unique plugin ID.

| Sink                                                | Destination                                                             |
| --------------------------------------------------- | ----------------------------------------------------------------------- |
| [BigQuery](sinks/bigquery_sink/README.md)           | a Google BigQuery table, through the Storage Write API                  |
| [ClickHouse](sinks/clickhouse_sink/README.md)       | ClickHouse tables                                                       |
| [Delta Lake](sinks/delta_sink/README.md)            | Delta Lake tables                                                       |
| [Apache Doris](sinks/doris_sink/README.md)          | Apache Doris tables, through the Stream Load HTTP API                   |
| [Elasticsearch](sinks/elasticsearch_sink/README.md) | Elasticsearch indices                                                   |
| [HTTP](sinks/http_sink/README.md)                   | any HTTP endpoint, for example a webhook or a REST API                  |
| [Apache Iceberg](sinks/iceberg_sink/README.md)      | Apache Iceberg tables, through a REST catalog                           |
| [InfluxDB](sinks/influxdb_sink/README.md)           | InfluxDB V2 and V3, as line-protocol points                             |
| [Meilisearch](sinks/meilisearch_sink/README.md)     | a Meilisearch index                                                     |
| [MongoDB](sinks/mongodb_sink/README.md)             | a MongoDB collection                                                    |
| [PostgreSQL](sinks/postgres_sink/README.md)         | PostgreSQL tables                                                       |
| [Quickwit](sinks/quickwit_sink/README.md)           | a Quickwit index                                                        |
| [RabbitMQ](sinks/rabbitmq_sink/README.md)           | RabbitMQ exchanges, through AMQP 0.9.1                                  |
| [Amazon Redshift](sinks/redshift_sink/README.md)    | Redshift tables, with Parquet files in S3 as the staging area           |
| [Amazon S3](sinks/s3_sink/README.md)                | Amazon S3 and S3-compatible stores (MinIO, R2, B2, DigitalOcean Spaces) |
| [Stdout](sinks/stdout_sink/README.md)               | the standard output, for debugging and development                      |
| [SurrealDB](sinks/surrealdb_sink/README.md)         | SurrealDB, with deterministic record IDs for an idempotent replay       |

## Sources

A source produces messages to an Iggy stream and topic. Configure one `[[streams]]` entry for each source instance, because the runtime keeps only the last configured producer. For example, the Random source generates messages for its stream and topic. The [source documentation](sources/README.md) describes the configuration and a sample implementation.

| Source                                                  | Origin                                                                                       |
| ------------------------------------------------------- | -------------------------------------------------------------------------------------------- |
| [Elasticsearch](sources/elasticsearch_source/README.md) | documents of Elasticsearch indices, by polling with timestamp tracking                       |
| [HTTP](sources/http_source/README.md)                   | `POST` bodies on the paths of an embedded webhook server, with bearer or HMAC authentication |
| [InfluxDB](sources/influxdb_source/README.md)           | InfluxDB V2 (Flux) and V3 (SQL), by polling                                                  |
| [MQTT](sources/mqtt_source/README.md)                   | the topics of an MQTT 3.1.1 or MQTT 5 broker, with QoS 0, 1 or 2                             |
| [PostgreSQL](sources/postgres_source/README.md)         | PostgreSQL tables, by table polling or Change Data Capture                                   |
| [Random](sources/random_source/README.md)               | random test messages, for testing and development                                            |

## Write a connector

A new connector implements the `Sink` or the `Source` trait. Read the [sink](sinks/README.md) or the [source](sources/README.md) documentation, and use the existing plugins in `sinks/` and `sources/` as examples. Add the new crate to the members of the root `Cargo.toml`.

Starting from a blank crate re-solves the same framework plumbing every time: config validation, `SecretString` secrets, retry with a circuit breaker, and ACK/NACK cursor staging. Copy [`sinks/sink_template`](sinks/sink_template/README.md) or [`sources/source_template`](sources/source_template/README.md) instead.

They are compiling, tested starting points with only the backend-specific calls marked `TODO(ConnectorDeveloper)`. The [authoring guide](docs/authoring-sinks-and-sources.md) has the full checklist and the review rules it is built from.

## Transformations

Field transformations can change messages before a source sends them to a topic, or before a sink consumes them. The payload format decides which transformations apply. To add a transformation, implement the `Transform` trait in the SDK, add its `TransformType` variant and extend `transforms::from_config`. Each transform can have its own configuration.

The [SDK documentation](sdk/README.md) describes the transforms, the stream decoders and the encoders.

## Contributing

Read [CONTRIBUTING.md](CONTRIBUTING.md) before you open a pull request. Report security issues as [SECURITY.md](SECURITY.md) describes, and never in public.

## Community

- [Discussions](https://github.com/apache/iggy-connectors/discussions) of this repository
- [Discord](https://discord.gg/apache-iggy)
- [Mailing lists](https://iggy.apache.org/community/mailing-lists/)
- [Documentation](https://iggy.apache.org/docs/)

## License

Apache Iggy Connectors is licensed under the [Apache License 2.0](LICENSE).
