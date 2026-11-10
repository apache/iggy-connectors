---
name: connector-testing
description: Write tests for Apache Iggy connectors - sinks, sources, transforms, SDK, runtime. Covers BDD unit-test naming, the canonical source state round-trip tests, and the real-infra integration test targets under `<package>/tests/<suite>/` with `testcontainers-modules` + `#[iggy_harness]`. Load when writing or reviewing tests anywhere in this repository (`src/` unit tests, `tests/` targets, `testing/`). Use for connector test authoring. NOT for runtime internals or non-connector test patterns.
---

# Testing Apache Iggy Connectors

> **Universal connector rules** live in
> [connectors-overview](../connectors-overview/SKILL.md). Repo-wide testing rules (`#[iggy_harness]`, BDD naming, harness layout) live in [AGENTS.md](../../../AGENTS.md#testing). This skill covers connector-specific test conventions.

## Contents

- [STOP and ask the user before](#stop-and-ask-the-user-before)
- [Unit tests](#unit-tests)
- [Integration tests (real infra via Docker)](#integration-tests-real-infra-via-docker)
- [SDK + runtime tests](#sdk--runtime-tests)
- [CI / verification flow](#ci--verification-flow)
- [Common pitfalls](#common-pitfalls)
- [Examples worth copying](#examples-worth-copying)

## STOP and ask the user before

- Mocking the database or backend in an integration test - we hit real infra via `testcontainers-modules` (mocks miss migration bugs).
- Adding a unit test that requires Docker - that belongs in the package's test target `<plugin>/tests/<backend>/`, not in `<plugin>/src/lib.rs`.
- Skipping the four canonical source state tests - they're mandatory for every source plugin.
- Naming a test `test_foo` / `does_bar` - we use BDD `given_X_when_Y_should_Z`.

There are **two distinct test layers**. Use both - they catch different bugs.

| Layer                 | Location                                                 | What it covers                                                                                 | When required                                        |
| --------------------- | -------------------------------------------------------- | ---------------------------------------------------------------------------------------------- | ---------------------------------------------------- |
| **Unit tests**        | `#[cfg(test)] mod tests` at EOF of `<plugin>/src/lib.rs` | Config parsing, defaults, fallbacks, payload conversion, query/document building, state ser/de | Every plugin                                         |
| **Integration tests** | `<plugin>/tests/<suite>/` (uses real backend via Docker) | End-to-end: runtime + plugin + real Postgres/ES/Iceberg/Mongo/etc. + Iggy server               | Every plugin that integrates with an external system |

## Unit tests

### Naming - BDD `given_when_should`

Match the convention used in the codebase (`sources/random_source/src/lib.rs::tests`, `sinks/postgres_sink/src/lib.rs::tests`):

```rust
#[test]
fn given_persisted_state_should_restore_messages_produced() { ... }

#[test]
fn given_no_state_should_start_fresh() { ... }

#[test]
fn given_invalid_duration_should_fall_back_to_default() { ... }

#[test]
fn given_payload_json_should_serialize_correctly() { ... }
```

You may **omit one part** when the test is small (`given_X_should_Y` instead of full `given_X_when_Y_should_Z`), but **stay consistent within a file**. Imperative `test_foo` / `does_bar` names are not the convention here.

A test whose subject is a **type** rather than a scenario has no "given" to name, and drops the prefix: it reads `<subject>_should_<property>`. The canonical state round trip below is `state_should_be_serializable_and_deserializable` in all three shipped sources (`random_source`, `postgres_source`, `http_source`). Leave those as they are rather than bending them into the scenario form.

### Prove a test can fail, before trusting it

A passing test says nothing until you have watched it fail. Break the behaviour it names, run the
suite, and put the code back:

```bash
# commit first: this reverts with `git checkout --`, which discards
# every uncommitted change in the file, not only the mutation
git status --porcelain -- <file>            # must be empty
<edit the guard out>
cargo nextest run -p <crate>                # expect a failure naming that test
git checkout -- <file>
```

Two failures this catches, both seen in review of `http_source`:

- **A fixture that makes the interesting case unreachable.** A guard test for endpoint resurrection
  passed `&[]` as the static config, so there was nothing to resurrect and the assertion held
  whatever `restore` did.
- **An assertion aimed at something self-healing.** A test for a teardown guard asserted on gauges,
  which `Metrics::encode` rebuilds from the live instances on every scrape, so it passed with the
  guard deleted. Delete such a test rather than keep it; a test that cannot fail is worse than none,
  because it reads as coverage.

Watch for an arm hidden behind a catch-all: `StatusClass::from` ends in `_ => ServerError`, so the
redirect arm could be removed with every existing assertion still green.

### `test_config()` helper

Every plugin's tests start with a small helper returning a tuned-down version of the production config. New tests extend it. don't construct from scratch each time.

```rust
fn test_config() -> RandomSourceConfig {
    RandomSourceConfig {
        interval: Some("100ms".to_string()),
        max_count: Some(100),
        messages_range: Some((5, 10)),
        payload_size: Some(50),
    }
}
```

### Async testing pattern

The trait methods are `async`. For unit tests, build a runtime locally - don't add `#[tokio::test]` in `src/lib.rs` (it pulls test macros into prod build):

```rust
#[test]
fn given_X_should_Y() {
    let runtime = tokio::runtime::Runtime::new().unwrap();
    runtime.block_on(async {
        // await calls here
    });
}
```

### Mandatory: four canonical source state tests

Every source must have these. Copy from `sources/random_source/src/lib.rs::tests`:

```rust
#[test]
fn given_persisted_state_should_restore_<field>() {
    let state = State { /* fields */ };
    let serialized = rmp_serde::to_vec(&state).expect("Failed to serialize state");
    let connector_state = ConnectorState(serialized);

    let source = MySource::new(1, test_config(), Some(connector_state));

    let runtime = tokio::runtime::Runtime::new().unwrap();
    runtime.block_on(async {
        let restored = source.state.lock().await;
        assert_eq!(restored.<field>, /* expected */);
    });
}

#[test]
fn given_no_state_should_start_fresh() {
    let source = MySource::new(1, test_config(), None);
    /* assert default state */
}

#[test]
fn given_invalid_state_should_start_fresh() {
    let invalid = ConnectorState(b"not valid msgpack".to_vec());
    let source = MySource::new(1, test_config(), Some(invalid));
    /* assert default state */
}

#[test]
fn state_should_be_serializable_and_deserializable() {
    let original = State { /* fields */ };
    let bytes = rmp_serde::to_vec(&original).unwrap();
    let restored: State = rmp_serde::from_slice(&bytes).unwrap();
    assert_eq!(original.<field>, restored.<field>);
}
```

State is the only thing that survives a plugin restart. Silent corruption here means lost data on the next deploy.

**The third test has two correct shapes, and which one applies is a design decision rather than a style choice.**

Starting fresh is right when the state is a cursor. The worst case is re-reading from the beginning, which the at-least-once contract already permits, so discarding an undecodable cursor costs duplicates and nothing else.

It is wrong when the state carries something whose *absence* is a downgrade rather than a repeat. `http_source` keeps its revocation tombstones in state, so starting fresh after a decode failure would serve every revoked endpoint again, with its secret. Its `restore` returns `Err` instead and the connector refuses to open. A source in that position asserts the refusal:

```rust
#[test]
fn given_invalid_state_should_refuse_to_open() {
    let invalid = ConnectorState(b"not valid msgpack".to_vec());

    let restored = MyState::restore(&config_entries(), Some(invalid), 1);

    assert!(
        restored.is_err(),
        "discarding this state silently would re-serve what it was revoking"
    );
}
```

Name the test for whichever it is and say why in a comment. What is never right is starting fresh silently when the state that was discarded was load-bearing.

### Sink unit tests

Cover pure per-message logic:

- Config defaults + optional fields + invalid values → fallback.
- Payload conversion for each `Payload` variant the sink claims to handle.
- Header encoding (especially base64 for binary headers).
- Query/document building (e.g., `postgres_sink::build_batch_insert_query`).
- Transient vs permanent error classification (e.g., `postgres_sink::is_transient_error`).

### Transform tests

Four branches must be covered (see `connector-transform`):

1. Happy path: matching input → expected output.
2. Filter path: input that should be dropped returns `Ok(None)`.
3. Pass-through path: non-JSON payload (or unmatched condition) returns `Ok(Some(unchanged))`.
4. Invalid config: `new()` returns `Err(Error::InvalidConfigValue(_))`.

### What NOT to mock

- The underlying client lib (sqlx, reqwest, mongodb). Tests would exercise the mock, not the code.
- The Iggy server.
- The runtime.

If you need a fake backend, use `wiremock` (a real local HTTP server) - see `sinks/http_sink/tests/http/`.

## Integration tests (real infra via Docker)

Each plugin that integrates with an external service (Postgres, Elasticsearch, Iceberg, MongoDB, InfluxDB, Quickwit, HTTP/wiremock) owns its integration tests: one test target per backend suite in `tests/<suite>/` of its own package, with a paired fixture + test file there. A backend with both a sink and a source has a target of the same name in both packages. Fixtures that more than one package needs live in `testing/` (crate `iggy_connector_testing`).

### Layout

```text
sinks/postgres_sink/tests/postgres/      Test target `postgres` of iggy_connector_postgres_sink
├── main.rs                   mod declarations, install_log_capture!(), shared constants (TEST_MESSAGE_COUNT, POLL_ATTEMPTS, ...)
├── fixtures/
│   ├── sink.rs               PostgresSinkFixture, PostgresSinkByteaFixture, PostgresSinkJsonFixture
│   └── mod.rs                re-exports, plus PostgresOps from iggy_connector_testing
├── postgres_sink.rs          The actual #[iggy_harness] tests
├── restart.rs                Sink restart tests through the runtime HTTP API
└── sink.toml                 Runtime config used by the tests

sources/postgres_source/tests/postgres/  Test target `postgres` of iggy_connector_postgres_source
├── main.rs
├── fixtures/
│   ├── source.rs             PostgresSourceJsonFixture, PostgresSourceJsonbFixture, ...
│   ├── cdc.rs                PostgresSourceCdcFixture, PostgresSourceCdcSlowPollFixture
│   └── mod.rs                re-exports, plus PostgresOps and PostgresSourceOps from iggy_connector_testing
├── postgres_source.rs  postgres_source_cdc.rs
├── source.toml  source_cdc_restart.toml
└── cdc_restart_connectors/   Connector directory of source_cdc_restart.toml

testing/src/                  iggy_connector_testing
├── lib.rs                    TestMessage, create_test_messages, TEST_CONTAINER_PREFIX, unique_container_name
├── postgres.rs               PostgresContainer (testcontainers wrapper), PostgresOps, PostgresSourceOps, env-var consts
├── random_source_liveness.rs assert_produces_messages
└── elasticsearch.rs  influxdb.rs  influxdb_v3.rs  floci.rs

runtime/tests/
├── runtime/                  Runtime-only tests (error isolation, missing plugin, ...)
├── api/                      HTTP control plane tests
├── http_config_provider/     HTTP config provider end-to-end, WireMock fixture in fixtures.rs
└── config_env_listing.rs     Env-var listing of the runtime binary
```

A container that only one package uses lives in that target's `fixtures/container.rs` (for example `sinks/http_sink/tests/http/fixtures/container.rs`).

### Container naming + cleanup

Every test container MUST be named with the `iggy-test-` prefix so a single
sweep reaps them: `docker ps -aqf 'name=^iggy-test-' | xargs -r docker rm -f`.
Reuse fixtures keep their container running, and a crashed run can orphan any
fixture's container, so run the sweep after a test session. New fixtures get
this for free by naming through `iggy_connector_testing::unique_container_name("<svc>")`, which
appends a uuid for parallel-safe ephemeral containers. Reuse fixtures use a fixed
`iggy-test-<svc>` literal instead, because the stable name is what lets later
nextest processes attach to the same container via `ReuseDirective::Always`. Do
not hand-roll a different prefix - the sweep only finds `iggy-test-*`.

As of now only `elasticsearch` and `doris` share a container. Both have a slow
boot (JVM / Doris FE+BE, tens of seconds), so reusing one across the whole
regression beats spinning a fresh one per test. Every other fixture boots a
fresh, ephemeral container per test - cheap enough that the isolation is worth
more than the reuse.

### The `#[iggy_harness]` proc macro

Each integration test is annotated with this macro from the `integration` crate of apache/iggy at R. It launches the prebuilt `target/debug/iggy-server` (from `./scripts/build-test-server.sh`) and optionally `target/debug/iggy-connectors` for the duration of the test, runs the seeds, then injects a `&TestHarness` and your fixture. It never builds them: run `cargo build` first, or a stale binary runs.

```rust
use integration::harness::seeds;
use integration::iggy_harness;

#[iggy_harness(
    server(connectors_runtime(config_path = "tests/postgres/sink.toml")),
    seed = seeds::connector_stream
)]
async fn json_messages_sink_stores_as_bytea(
    harness: &TestHarness,
    fixture: PostgresSinkFixture,
) {
    let client = harness.root_client().await.unwrap();
    let pool = fixture.create_pool().await.expect("Failed to create pool");
    fixture.wait_for_table(&pool, "iggy_messages").await;
    // ... send messages, assert on rows
}
```

Key pieces:

- `server(connectors_runtime(config_path = ...))` boots the connectors runtime against the given TOML.
  The path is relative to the package directory: cargo and nextest run each test binary there, and the runtime inherits that working directory.
  Most of these TOMLs set `config_dir = "."`, so the runtime loads the plugin's own `config.toml` from the package root (it skips `Cargo.toml`).
  The fixture then sets the plugin path to the bare file name, for example `ENV_SINK_PATH` = `libiggy_connector_postgres_sink`, and the runtime finds it next to its own binary in `target/debug/`.
  A test that needs other connector files keeps them in a directory under `tests/<suite>/`, for example `tests/postgres/cdc_restart_connectors/` in `sources/postgres_source/`.
- `seed = seeds::connector_stream` runs the named seed to create the test stream/topic.
- `fixture: PostgresSinkFixture` - the test harness calls `PostgresSinkFixture::setup()` (which spins up the testcontainer) and injects the result.
- `harness: &TestHarness` (defined in `core/integration/src/harness/orchestrator/harness.rs` of apache/iggy) exposes `.root_client().await`, `.connectors_runtime().expect("...").http_url()`, etc. - `connectors_runtime()` returns `Option`, so `.expect()` first.

### Fixture pattern (`TestFixture` trait)

Each fixture implements `integration::harness::TestFixture`. Pattern:

```rust
// testing/src/<backend>.rs when several packages use it, else tests/<suite>/fixtures/container.rs

use testcontainers_modules::{
    <backend_module>,
    testcontainers::{ContainerAsync, runners::AsyncRunner},
};

pub struct MyContainer {
    container: ContainerAsync<<backend_module>::Image>,
    pub connection_string: String,
}

impl MyContainer {
    pub async fn start() -> Result<Self, TestBinaryError> {
        let container = <backend_module>::Image::default().start().await?;
        let port = container.get_host_port_ipv4(DEFAULT_PORT).await?;
        Ok(Self {
            container,
            connection_string: format!("...://localhost:{port}"),
        })
    }
}
```

The fixture itself wraps the container and implements `TestFixture::setup()` to spin it up and inject env vars that the runtime picks up.

### Env-var injection via `ConfigEnv`

The runtime's config structs derive `ConfigEnv` (`configs_derive::ConfigEnv`, applied in `runtime/src/configs/connectors.rs` to `SinkConfig`, `SourceConfig`, `StreamConsumerConfig`, `StreamProducerConfig`). Fields are addressable by env var with a path-built prefix. Three concrete forms observed in `testing/src/postgres.rs`:

```rust
// Plugin-config fields (your sink/source's own config struct):
//   IGGY_CONNECTORS_<TYPE>_<KEY>_PLUGIN_CONFIG_<FIELD>
pub const ENV_SINK_CONNECTION_STRING: &str =
    "IGGY_CONNECTORS_SINK_POSTGRES_PLUGIN_CONFIG_CONNECTION_STRING";

// Indexed nested fields (per-stream entries are a Vec):
//   IGGY_CONNECTORS_<TYPE>_<KEY>_STREAMS_<INDEX>_<FIELD>
pub const ENV_SINK_STREAMS_0_STREAM: &str =
    "IGGY_CONNECTORS_SINK_POSTGRES_STREAMS_0_STREAM";

// Top-level scalar fields on SinkConfig/SourceConfig itself:
//   IGGY_CONNECTORS_<TYPE>_<KEY>_<FIELD>
pub const ENV_SINK_PATH: &str =
    "IGGY_CONNECTORS_SINK_POSTGRES_PATH";
```

`<TYPE>` is `SINK` or `SOURCE`. `<KEY>` is the connector key (TOML key field, uppercased). `PLUGIN_CONFIG` is the literal segment used when reaching into your plugin's nested config. Fields marked `#[config_env(skip)]` (e.g., `transforms`, `plugin_config` as a whole) are NOT env-addressable. primitive leaves marked `#[config_env(leaf)]` are.

The fixture sets these in `setup()` before the runtime reads its config. This is how a dynamic container port reaches a static TOML.

### Test naming for integration tests

Integration tests use a **descriptive declarative** style, not the unit-test `given_should` form. Match the codebase:

```rust
async fn json_messages_sink_stores_as_bytea(...) { ... }
async fn binary_messages_sink_stores_as_bytea(...) { ... }
async fn json_messages_sink_stores_as_jsonb(...) { ... }
async fn delete_after_read_source_removes_rows_after_producing(...) { ... }
async fn processed_column_source_marks_rows_after_producing(...) { ... }
async fn state_persists_across_connector_restart(...) { ... }
async fn restart_sink_connector_continues_processing(...) { ... }
```

Pattern: `<subject>_<action>_<observation>`. Reads like a sentence describing what the test proves.

### Polling vs sleeping

Integration tests poll until an expected condition - never `sleep(big_duration)`. Constants `POLL_ATTEMPTS` and `POLL_INTERVAL_MS` defined in each test target's `main.rs`. Fixture helpers like `PostgresSinkFixture::fetch_rows_as` and `PostgresSinkFixture::wait_for_table` encapsulate the pattern.

### When to add an integration test

- New sink/source plugin → at minimum one happy-path test per supported `Schema` and per supported plugin mode. Concrete coverage in the `postgres` targets of `sinks/postgres_sink/` and `sources/postgres_source/`:
  3 sink tests (`json_messages_sink_stores_as_bytea`, `binary_messages_sink_stores_as_bytea`, `json_messages_sink_stores_as_jsonb`) backed by 3 sink fixtures, plus 5 source fixtures (`Json`, `Jsonb`, `Bytea`, `Delete`, `Mark`), `state_persists_across_connector_restart` for state survival, and a dedicated `restart.rs` for sink restarts.
- Bug fix in a plugin → regression test reproducing the bug.
- Behavior that crosses the FFI boundary (state restart, transform chain, schema decode) → integration test, not unit test.

### When NOT to add an integration test

- Pure logic (query builder, payload converter) - that's a unit test.
- Config validation - unit test.
- Code paths already covered by an equivalent test for another backend, when the new backend has identical behavior.

## SDK + runtime tests

- `sdk/tests/` - cross-module SDK concerns. In tree: `protobuf_integration.rs`, `flatbuffer_integration.rs` (round-trip schema decoders/encoders), `plugin_logging.rs`.
- Runtime tests live under `runtime/tests/runtime/`, `runtime/tests/api/` and `runtime/tests/http_config_provider/`. They exercise the runtime alone (no plugin-specific backend, only `random_source` and `stdout_sink`) - error isolation, missing plugin, invalid config, HTTP API contract.

## CI / verification flow

Before pushing:

```text
cargo fmt --all
cargo sort --no-format --workspace
cargo clippy -p <package> --all-targets -- -D warnings
cargo nextest run -p <package> --lib        # --bins for the runtime
```

Integration tests run per test target. Build what the harness launches first, then filter by target and test name:

```text
./scripts/build-test-server.sh                                          # once per R
cargo build -p iggy-connectors -p iggy_connector_postgres_sink
cargo nextest run -p iggy_connector_postgres_sink --test postgres postgres_sink::   # one module
cargo nextest run -p iggy_connector_postgres_sink --test postgres                   # the whole suite
cargo nextest run --workspace                                                        # all tests, as CI
```

A test that apache/iggy named `connectors::<suite>::<rest>` is `<rest>` in the test target `<suite>` of the package that owns it.

Integration tests assume Docker is available locally and pull images from a public registry on first run.

## Common pitfalls

1. **Skipping integration tests for a real-infra plugin.** Unit tests don't catch FFI/lifecycle/encoding regressions.
2. **Forgetting the four canonical source state tests.** This is the most common review comment.
3. **Mocking the client library** instead of using `testcontainers` - test exercises the mock.
4. **`#[tokio::test]` in `src/lib.rs`** - pulls tokio test macros into prod build. Use `Runtime::new()` instead. (`#[tokio::test]` is fine in `tests/` directories, which compile separately.)
5. **`unwrap()` in test assertions** - prefer `assert_eq!` / `expect("descriptive")` so failures tell you which step blew up.
6. **Tests depending on system time** - inject the clock or use `tokio::time::pause()`.
7. **Hidden coupling via shared globals** (static `AtomicU64`, env vars) - tests must be independent and parallelizable.
8. **Asserting on `Display` of an error** - use `matches!(err, Error::Variant(_))`. `assert_eq!(err.to_string(), ...)` breaks on message tweaks.
9. **Sleeping a fixed duration** instead of polling - causes flakes when CI is slow.

## Examples worth copying

| Concern                             | Where                                                                                                          |
| ----------------------------------- | -------------------------------------------------------------------------------------------------------------- |
| Source state round-trip             | `sources/random_source/src/lib.rs::tests`                                                                      |
| Config + query-building unit tests  | `sinks/postgres_sink/src/lib.rs::tests`                                                                        |
| Real-infra sink integration tests   | `sinks/postgres_sink/tests/postgres/postgres_sink.rs`                                                          |
| Real-infra source integration tests | `sources/postgres_source/tests/postgres/postgres_source.rs`                                                    |
| Restart + state survival            | `sinks/postgres_sink/tests/postgres/restart.rs`, `postgres_source.rs::state_persists_across_connector_restart` |
| Wiremock-driven HTTP sink test      | `sinks/http_sink/tests/http/http_sink.rs`                                                                      |
| Testcontainer wrapper               | `testing/src/postgres.rs`                                                                                      |
| Fixture impl `TestFixture`          | `sinks/postgres_sink/tests/postgres/fixtures/sink.rs`                                                          |
| Runtime-only error-isolation        | `runtime/tests/runtime/error_isolation.rs`                                                                     |
| Transform tests                     | `sdk/src/transforms/add_fields.rs::tests`, `filter_fields.rs::tests`                                           |

---

Discussion / help: see [AGENTS.md](../../../AGENTS.md#discussion-and-support).
