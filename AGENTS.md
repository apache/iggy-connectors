# Apache Iggy Connectors Agent Guidelines

**Apache Iggy Connectors** move data between Apache Iggy streams and
external systems. The runtime `iggy-connectors` loads sink and source
plugins (`cdylib`) with `dlopen` and drives them through a small FFI.
Plugins build on the SDK `iggy_connector_sdk`. The Iggy server, the
client SDKs and the CLI live in [apache/iggy](https://github.com/apache/iggy).
This repository builds against one commit of apache/iggy, called the core pin.

> Skills live under `.claude/skills/`, with per-skill links under
> `.agents/skills/` for Codex. Load
> [connectors-overview](.claude/skills/connectors-overview/SKILL.md)
> first for any change in this repository.

## Contents

- [Apache Iggy Connectors Agent Guidelines](#apache-iggy-connectors-agent-guidelines)
  - [Contents](#contents)
  - [STOP and ask the user before](#stop-and-ask-the-user-before)
  - [Helping someone contribute](#helping-someone-contribute)
  - [Quick reference](#quick-reference)
  - [Structure](#structure)
  - [The core pin](#the-core-pin)
  - [Where to look](#where-to-look)
  - [Skills](#skills)
  - [Repo-wide principles](#repo-wide-principles)
  - [Tooling](#tooling)
  - [Testing](#testing)
  - [Pitfalls](#pitfalls)
  - [Local state directories](#local-state-directories)
  - [Commits, PRs, releases](#commits-prs-releases)
  - [Discussion and support](#discussion-and-support)

## STOP and ask the user before

- Writing code for an issue not assigned to the user
  (`gh issue view <N> --json assignees`). If it is not, or if you cannot
  check it, they must ask on the issue and wait until a maintainer
  assigns it.
- Bumping `iggy_connector_sdk` MAJOR version or changing any FFI
  signature in `sdk/src/{sink,source}.rs` (breaks every pre-built
  plugin .so).
- Touching the state-save path (`runtime/src/state.rs`) in a way that
  changes the atomic-rename protocol (corruption risk).
- Renaming default consumer groups, plugin path resolution rules,
  environment variables, the image `apache/iggy-connect`, the tag
  patterns `connectors-X.Y.Z` and `connectors-sdk-X.Y.Z`, or any other
  operator-facing identifier.
- Moving the core pin to an apache/iggy commit that is not a release
  (see [The core pin](#the-core-pin)).
- Force-pushing to `master`, amending shipped commits, or skipping
  hooks with `--no-verify` / `--no-gpg-sign`.

## Helping someone contribute

Most PRs from people new to Iggy are written with an agent. The person opening the PR is responsible for it, and maintainers close PRs that read as a relay between reviewer and model. Read the AI Assistance, Agent-Driven PRs, Bugs Found by an Agent and Close Policy sections of [CONTRIBUTING.md](CONTRIBUTING.md) before helping open a PR.

- The PR must link an issue the user was assigned to before any work started. See the STOP list above.
- Fill in every section of the PR template. Under AI Usage, name the agent and say what it wrote. When an agent wrote any of the change, never leave the section out or write "None".
- Run `prek run` before the PR is opened. If prek is not installed, ask the user to install it (`cargo install prek`, then `prek install`). Do not run the individual checks in its place.
- If a check cannot run, tell the user which one and why, and treat the PR as not ready. Never write text in a PR asking a maintainer to run something.
- The user drives the change. They pick the issue, decide the approach, write the rationale in their own words and explain every change without asking you. Maintainers can close agent-driven PRs, where the agent did the work and the user only approved it.
- New contributors keep one open PR at a time.
- If you found a vulnerability, follow [SECURITY.md](SECURITY.md) and open no public issue or PR. For any other bug, the user opens an issue and waits until a maintainer agrees on the approach and assigns it. Then you can write one PR with the test and the fix. If the fix is hard, the PR can hold only the test, marked `#[ignore]`.

## Quick reference

```bash
# CI enforces this order (.github/actions/rust/pre-merge).
cargo fmt --all                                            # step 1
cargo sort --no-format --workspace                         # step 2 (--no-format mandatory)
cargo clippy --all-features --all-targets -- -D warnings   # step 3
cargo machete --with-metadata                              # step 4
cargo nextest run -p <package>                             # step 5

# Integration tests need Docker, the server at the core pin and fresh builds in target/debug/.
./scripts/build-test-server.sh                             # once per core pin
cargo build                                                # runtime and every plugin
cargo nextest run -p iggy_connector_postgres_sink --test postgres

# Other CI checks (mirror in pre-commit; pass --fix where supported)
./scripts/ci/taplo.sh             # TOML
./scripts/ci/markdownlint.sh      # Markdown
./scripts/ci/skills.sh            # skill metadata, links, invocation policies
./scripts/ci/shellcheck.sh        # Shell
./scripts/ci/license-headers.sh   # Apache headers
./scripts/ci/trailing-whitespace.sh
./scripts/ci/trailing-newline.sh
./scripts/ci/sync-rustc-version.sh
./scripts/ci/binary-artifacts.sh --check
./scripts/ci/third-party-licenses.sh --validate --manifest <path>
./scripts/ci/check-core-pin.sh    # each core `version` matches the crate at the core pin
./scripts/verify-sdk-publish.sh   # SDK into a scratch local registry

# Move the core pin, the apache/iggy commit
./scripts/bump-core-pin.sh <core tag or commit>

# Run the runtime with the example configuration
IGGY_CONNECTORS_CONFIG_PATH=runtime/example_config/config.toml cargo run --bin iggy-connectors
```

Pre-commit hooks wire the same scripts. Install them with `pre-commit
install` (Python) or `prek install` (Rust drop-in). Both read
`.pre-commit-config.yaml`.

## Structure

```text
iggy-connectors/
├── runtime/              iggy-connectors: the runtime binary, image apache/iggy-connect
│   ├── config.toml       default configuration, compiled in as the fallback
│   ├── example_config/   example runtime and connector configurations
│   └── tests/            runtime/, api/, http_config_provider/, config_env_listing.rs
├── sdk/                  iggy_connector_sdk: traits, FFI macros, decoders, encoders, transforms
├── sinks/<name>/         one sink plugin, iggy_connector_<name>, with its tests/
├── sources/<name>/       one source plugin, iggy_connector_<name>, with its tests/
├── testing/              iggy_connector_testing: fixtures that several test targets share
├── scripts/              CI checks, test server build, core pin bump
├── .config/              nextest.toml, rail.toml
├── .github/              workflows, actions, components.yml, publish.yml
└── .claude/skills/       connector and review skills, plus the .agents/skills links
```

## The core pin

The root `Cargo.toml` is the only place for the core pin. Six
`[workspace.dependencies]` entries take core crates from git, with
the core pin as `rev`: `configs`, `configs_derive`, `iggy`,
`iggy_common`, `integration` (the test `harness` and
`#[iggy_harness]`) and `system_stats`. `iggy` and `iggy_common` also
carry a `version`, because the published SDK takes them from
crates.io. Each `version` must equal the crate version at the core
pin, and `scripts/ci/check-core-pin.sh` fails otherwise.

- The core pin stays at the last core release. When a connector needs
  an unreleased core change, the core pin moves to a core `master`
  commit. That change lands in apache/iggy first.
- `scripts/bump-core-pin.sh` rewrites the core pin and the two
  versions. Never edit them by hand.
- No `[patch]` section, no submodule and no sibling checkout of
  apache/iggy. A local edit under `~/.cargo/git/checkouts/` is lost on
  the next fetch.
- The nightly workflow tests the tip of core `master`. If that run
  fails, it opens an issue.

## Where to look

| Task                         | Location                                             |
| ---------------------------- | ---------------------------------------------------- |
| Runtime                      | `runtime/src/`                                       |
| Plugin SDK                   | `sdk/src/`                                           |
| Sinks and sources            | `sinks/<name>/`, `sources/<name>/`                   |
| Runtime configuration        | `runtime/config.toml`, `runtime/example_config/`     |
| Integration tests            | `<package dir>/tests/<suite>/`, `runtime/tests/`     |
| Shared test fixtures         | `testing/`                                           |
| `harness` module, server     | `core/integration/`, `core/server/` in apache/iggy   |
| CI test scope                | `.config/rail.toml`, `.github/config/components.yml` |
| Releases and publishing      | `.github/config/publish.yml`                         |

## Skills

Each `SKILL.md` has YAML frontmatter (name, description). Load
`connectors-overview` first as router.
Claude Code uses `/skill-name`. Codex uses `$skill-name` or `/skills`.
Keep one source under `.claude/skills/` and link its directory from
`.agents/skills/`. Keep templates and references inside the source directory.

- [connectors-overview](.claude/skills/connectors-overview/SKILL.md): router and universal connector rules
- [connector-runtime](.claude/skills/connector-runtime/SKILL.md): FFI host, lifecycle, state, metrics
- [connector-sdk](.claude/skills/connector-sdk/SKILL.md): traits, decoders and encoders, retry
- [connector-sink](.claude/skills/connector-sink/SKILL.md): sink plugin authoring
- [connector-source](.claude/skills/connector-source/SKILL.md): source plugin authoring
- [connector-transform](.claude/skills/connector-transform/SKILL.md): transform authoring
- [connector-testing](.claude/skills/connector-testing/SKILL.md): unit and integration test patterns

Review skills are user-invoked only. Keep `disable-model-invocation: true`
for Claude Code and `policy.allow_implicit_invocation: false` in
`agents/openai.yaml` for Codex. Do not replicate their steps without an
explicit invocation. When a non-trivial change passes its checks,
suggest `/team-review <target>` in Claude Code or `$team-review <target>`
in Codex.

- [connector-review](.claude/skills/connector-review/SKILL.md): adversarial connectors PR or branch review with validation
- [team-review](.claude/skills/team-review/SKILL.md): four expert review of a PR, branch, or ref range

Review skills share [execution instructions](.claude/skills/team-review/references/execution.md)
for target preparation and client-specific agent tools. `team-review`
is a copy of the apache/iggy skill. Run
`./scripts/ci/skills.sh` to check skill metadata, links, and invocation policies.

## Repo-wide principles

1. **Apache 2.0 header on every new source file:** Follow the comment style that `licenserc.toml` configures. For example, `.rs` uses `// ...`, and `Cargo.toml` and shell use `# ...`.
2. **Order of the checks:** `fmt --all` -> `sort --no-format --workspace` -> `clippy --all-features --all-targets -- -D warnings` -> `machete` -> `nextest run`. Plus `taplo.sh --check`. CI enforces all.
3. **Always pass `--no-format` to `cargo sort`.** Without it multi-line feature arrays flatten and `taplo` re-expands them, churning the workspace.
4. **One virtual workspace.** Every dependency of every member is declared once in the root `[workspace.dependencies]`, and members use `{ workspace = true }`.
5. **Avoid LLM-slop tells: em dashes, gratuitous semicolons, hedging narrative, trailing summaries.** Defaults, not absolute bans. Same rule for code, comments, commits, PR descriptions.
6. **Idiomatic Rust over free helpers.** `FromStr` not `parse_foo`, `Display` not `format_foo`, `From`/`TryFrom` not `to_foo_from_bar`. Structured `Err` enums, not `String`.
7. **Imports at the top.** No inline `use` inside functions or blocks. Group: std, external crates, then `crate::`.
8. **Precise names.** No `b`, `p`, `t`, `m`. Match literal API field names as log labels.
9. **Comments explain WHY, not WHAT.** Default to no comment. Add a comment only for a hidden constraint or a workaround that can surprise a future reader. Never reference the current task / PR.
10. **Never `cargo install`** without authorization. Toolchain pinned in `rust-toolchain.toml`. CI installs additional cargo tools on demand. If a required tool such as prek is missing, ask the user to install it rather than skipping the check. `scripts/build-test-server.sh` is the exception: it installs `iggy-server` under `target/`, not into `~/.cargo/bin`.
11. **No `unwrap()` / `expect()` on Results from external I/O** outside tests.
12. **`tokio::sync::Mutex` (not `std::sync::Mutex`)** wherever a lock is held across `.await`. Hold locks briefly: lock, clone, drop guard, then do I/O.
13. **Code reads top to bottom.** Public consts and public fns first, helpers below, deeper helpers below those.
14. **Zero clone in hot loops.** `&self`, `&str`, `&[T]`. If the size is known, use `Vec::with_capacity(n)`.
15. **Forward-compatible configuration.** New TOML fields use `#[serde(default)]` or `Option<T>`.

## Tooling

| Tool                                      | Where                                    |
| ----------------------------------------- | ---------------------------------------- |
| `cargo fmt`, `cargo sort`, `cargo clippy` | pre-commit + CI                          |
| `cargo machete`                           | CI `machete` task                        |
| `cargo nextest` + `cargo-rail`            | CI `test-*` DAG-scoped                   |
| `cargo llvm-cov`                          | CI coverage                              |
| `cargo http-registry`                     | `scripts/verify-sdk-publish.sh`          |
| `taplo`, `markdownlint-cli`, `shellcheck` | pre-commit + CI                          |
| `typos`                                   | pre-commit                               |
| `cargo about`                             | `scripts/ci/third-party-licenses.sh`     |
| HawkEye / `license-headers.sh`            | pre-commit + CI                          |

## Testing

- **Unit:** `#[cfg(test)] mod tests` at EOF of `src/lib.rs` (or per-module). They need no Docker: `cargo nextest run -p <package> --lib` (`--bins` for the runtime).
- **Integration:** each plugin owns its suite in `<package dir>/tests/<suite>/`, for example `sinks/postgres_sink/tests/postgres/`. A backend with a sink and a source has a target in both packages. The runtime owns `runtime/tests/runtime/`, `runtime/tests/api/`, `runtime/tests/http_config_provider/` and `runtime/tests/config_env_listing.rs`. `bigquery_sink` has no `#[iggy_harness]` tests. Its `tests/bigquery_sink.rs` runs the sink against an in-process fake BigQuery and needs no Docker.
- **`#[iggy_harness]`:** the tests use this macro and the `harness` types of the core crate `integration` at the core pin. A test starts `target/debug/iggy-server` and `target/debug/iggy-connectors`. The runtime loads each plugin by its bare file name, such as `libiggy_connector_stdout_sink`, from its own directory.
- **Dev-dependencies:** the macro emits `::tokio::` and `::serial_test::` paths, so each package with integration tests declares both. A test binary installs the per-test log capture with `integration::install_log_capture!()`, which needs `ctor` and `dtor` too.
- **Backends:** external systems run in Docker through `testcontainers-modules`. Container names start with `iggy-test-`. Fixtures that several test targets share live in `testing/`.
- **Logs:** per-test logs land in `test_logs/`, because `.cargo/config.toml` sets `IGGY_TEST_LOGS_DIR`.
- **CI scope:** `.config/rail.toml` has one `require` row per test target. The row lists the runtime binary and every plugin that the target loads. A new test target needs its row.

BDD test naming: `given_X_when_Y_should_Z` (3-part). Promote to 4-part `given_X_when_Y_then_Z_should_W` only with a distinct "then" intermediate. Be consistent inside a file.

Filter a single integration test by its path inside the test target. Build the binaries that `#[iggy_harness]` starts first. It runs
whatever is already in `target/debug/`, so a stale one fails as though the change under test broke it.

```bash
cargo build -p iggy-connectors -p iggy_connector_stdout_sink -p iggy_connector_random_source
cargo nextest run -p iggy-connectors --test runtime benchmark::given_logging_format_json
```

A core test named `connectors::<suite>::<rest>` became the test `<rest>` in the target `<suite>` of its package.

## Pitfalls

- **Docker is required for the integration tests.** `testcontainers-modules` spawns containers per test.
- **The Doris tests need `vm.max_map_count` of at least 2000000 on Linux.** Run `sudo sysctl -w vm.max_map_count=2000000`.
- **The tests run against the server at the core pin.** `scripts/build-test-server.sh` builds it into `target/iggy-server-<core-pin>/` and copies it to `target/debug/iggy-server`. Run it again after each core pin bump. A server from another commit fails in ways that look like the change under test.
- **`#[iggy_harness]` runs prebuilt binaries and never builds them.** A stale `target/debug/iggy-connectors` or plugin library is used silently. Rebuild the runtime and the plugins that a target loads before you run it.
- **`cargo sort` without `--no-format` will churn the workspace.** Use the pre-commit hook.
- **Rust toolchain is pinned in `rust-toolchain.toml`.** Older system `cargo` fails on transitive deps. prefer rustup so the pin auto-resolves.
- **The first build fetches apache/iggy from git.** Later builds use `~/.cargo/git`.
- **`test_logs/` grows quickly.** Wipe between major refactors.

## Local state directories

Created by the runtime or tests. Gitignored. Safe to delete to reset:

- `local_state/` - runtime state files of the example configuration
- `test_logs/` - per-test logs
- `target/` - cargo build, plus the test server in `target/iggy-server-<core-pin>/`

## Commits, PRs, releases

- Conventional commits with scope: `feat(connectors): ...`. Subject <= 72 chars. Allowed types/scopes enforced by `.github/workflows/pr-title.yml`.
- Body: why before what. No bullet lists of files changed.
- If a change has co-authors, add a `Co-authored-by` trailer for each.
- The runtime ships only as the image `apache/iggy-connect`, tagged `connectors-X.Y.Z`. The SDK goes to crates.io, tagged `connectors-sdk-X.Y.Z`. Sinks and sources are `publish = false`.
- Post-merge publishes an `edge` SDK version that has no tag yet, and then tags it. A version without a suffix is a release and goes out only after the vote.
- A release that ships with a core release has one vote for both. A connectors-only release has its own vote, with the core pin at the last core release.
- Third-party license enumeration: `scripts/ci/third-party-licenses.sh` (ASF policy).

## Discussion and support

- Design: <https://github.com/apache/iggy-connectors/discussions>
- Core design: <https://github.com/apache/iggy/discussions>
- Chat: <https://discord.gg/apache-iggy>
- Docs: <https://iggy.apache.org/docs/>
