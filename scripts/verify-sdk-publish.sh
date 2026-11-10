#!/usr/bin/env bash
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

# End-to-end check that iggy_connector_sdk can be uploaded to a crates.io-like
# registry. Spins up cargo-http-registry as a local alt-registry, publishes
# the core crates that the SDK depends on from a copy of core at R, and then
# publishes the SDK. Catches missing versions on dependencies and any other
# manifest-level regression that only surfaces at `cargo publish` time.
#
# The core crates come from R, not from crates.io. So the check also works
# while crates.io lacks the core versions at R, where a plain
# `cargo publish --dry-run` fails.
#
# This script is invoked both by developers (locally) and by CI. It must
# leave the working tree untouched on exit, even on failure.
#
# Usage:
#   scripts/verify-sdk-publish.sh
#
# Dependencies (install manually, or re-run with AUTO_INSTALL=1):
#   cargo-http-registry  (cargo install cargo-http-registry --locked)
#
# Environment variables (all optional):
#   REGISTRY_PORT         Port for the local registry          (default: 35503)
#   CA_BUNDLE             Path to the system CA bundle used by libgit2.
#                         Auto-detected on common Linux distros if unset.
#   AUTO_INSTALL          If "1", install cargo-http-registry  (default: 0)
#
# Exit codes:
#   0  success, the core crates and the SDK are in the local registry
#   1  tooling missing or preflight failure
#   2  publish chain failed on one of the crates

set -euo pipefail

# shellcheck source-path=SCRIPTDIR
source "$(dirname "${BASH_SOURCE[0]}")/ci/lib/core-pin.sh"

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$REPO_ROOT"

REGISTRY_NAME="local-dev"
REGISTRY_PORT="${REGISTRY_PORT:-35503}"
REGISTRY_DIR="$(mktemp -d -t iggy-local-reg.XXXXXX)"
REGISTRY_LOG="$(mktemp -t iggy-local-reg-log.XXXXXX)"
CORE_COPY="$(mktemp -d -t iggy-core-at-r.XXXXXX)"
# Byte-exact snapshots, restored by cleanup() instead of `git checkout`, so
# the revert is independent of git state and keeps any in-flight edit.
CARGO_TOML_BACKUP="$(mktemp -t iggy-cargo-toml.XXXXXX)"
CARGO_LOCK_BACKUP="$(mktemp -t iggy-cargo-lock.XXXXXX)"
cp Cargo.toml "$CARGO_TOML_BACKUP"
cp Cargo.lock "$CARGO_LOCK_BACKUP"
AUTO_INSTALL="${AUTO_INSTALL:-0}"

# Ordered topologically: each crate depends only on the ones before it.
# MUST be lowercase (crates-index path is derived from the name directly).
CORE_CRATES=(
    iggy_binary_protocol
    iggy_common
    iggy
)
SDK_CRATE="iggy_connector_sdk"

REGISTRY_PID=""
CARGO_FILES_PATCHED=false

log() { printf '>>> %s\n' "$*"; }
err() { printf '!!! %s\n' "$*" >&2; }

# Invoked via `trap ... EXIT INT TERM`. shellcheck cannot see indirect calls.
# shellcheck disable=SC2329
cleanup() {
    local ec=$?
    set +e
    trap - EXIT INT TERM

    if [[ "$CARGO_FILES_PATCHED" == "true" ]]; then
        log "Reverting Cargo.toml and Cargo.lock"
        cp "$CARGO_TOML_BACKUP" Cargo.toml
        cp "$CARGO_LOCK_BACKUP" Cargo.lock
    fi
    rm -f "$CARGO_TOML_BACKUP" "$CARGO_LOCK_BACKUP"

    if [[ -n "$REGISTRY_PID" ]] && kill -0 "$REGISTRY_PID" 2>/dev/null; then
        log "Stopping local registry (pid $REGISTRY_PID)"
        kill -TERM "$REGISTRY_PID" 2>/dev/null || true
        # Give it half a second then force
        for _ in 1 2 3 4 5; do
            kill -0 "$REGISTRY_PID" 2>/dev/null || break
            sleep 0.1
        done
        kill -KILL "$REGISTRY_PID" 2>/dev/null || true
        wait "$REGISTRY_PID" 2>/dev/null || true
    fi

    rm -rf "$REGISTRY_DIR" "$REGISTRY_LOG" "$CORE_COPY"
    exit "$ec"
}
trap cleanup EXIT INT TERM

# Fails unless every line in $2 for the crates in $3.. carries the registry.
require_registry_patch() {
    local what=$1 manifest=$2 crate missing=0
    shift 2
    for crate in "$@"; do
        if ! grep -Eq "^${crate} = \{.*registry = \"${REGISTRY_NAME}\".*\}$" "$manifest"; then
            err "Failed to patch the ${what} dependency on ${crate}"
            missing=1
        fi
    done
    if (( missing )); then
        err "Current state of ${manifest}:"
        grep -nE "^($(IFS='|'; echo "$*")) = " "$manifest" >&2 || true
        exit 1
    fi
}

publish() {
    local manifest=$1 crate=$2
    log "Publishing ${crate} to ${REGISTRY_NAME}"
    # --no-verify: the PR build pipeline already compiles the workspace. This
    #              job only checks the publish path.
    # --allow-dirty: the manifests are patched by design. The cleanup trap
    #                reverts the patch on exit.
    if ! cargo publish \
        --manifest-path "$manifest" \
        -p "$crate" \
        --registry "$REGISTRY_NAME" \
        --token ci \
        --allow-dirty \
        --no-verify
    then
        err "Publish failed for ${crate}"
        exit 2
    fi
}

# ---------------------------------------------------------------------------
# 0. Preconditions
# ---------------------------------------------------------------------------
if ! command -v cargo >/dev/null 2>&1; then
    err "cargo is not on PATH"
    exit 1
fi

if ! command -v jq >/dev/null 2>&1; then
    err "jq is required but is not on PATH"
    exit 1
fi

if ! command -v cargo-http-registry >/dev/null 2>&1; then
    if [[ "$AUTO_INSTALL" == "1" ]]; then
        log "Installing cargo-http-registry"
        cargo install cargo-http-registry --locked
    else
        err "cargo-http-registry is not installed"
        err "Install it with: cargo install cargo-http-registry --locked"
        err "Or re-run this script with AUTO_INSTALL=1"
        exit 1
    fi
fi

# libgit2 inside cargo needs an explicit CA bundle on many Linux distros.
# Auto-detect common locations when the caller hasn't overridden it.
if [[ -z "${CA_BUNDLE:-}" ]]; then
    for candidate in \
        /etc/ssl/certs/ca-certificates.crt \
        /etc/pki/tls/certs/ca-bundle.crt \
        /etc/ssl/cert.pem \
        /etc/ssl/ca-bundle.pem \
        /opt/homebrew/etc/ca-certificates/cert.pem \
        /usr/local/etc/ca-certificates/cert.pem
    do
        if [[ -r "$candidate" ]]; then
            CA_BUNDLE="$candidate"
            break
        fi
    done
fi

if [[ -z "${CA_BUNDLE:-}" || ! -r "$CA_BUNDLE" ]]; then
    err "Could not locate a system CA bundle. Set CA_BUNDLE=/path/to/ca.crt"
    exit 1
fi
log "Using CA bundle: $CA_BUNDLE"

# ---------------------------------------------------------------------------
# 0b. MSRV contract: the SDK must advertise an MSRV on crates.io. The MSRV
#     CI gate compile-checks against the floor, but it pins the toolchain
#     regardless of the manifest, so it cannot notice the `rust-version`
#     field silently disappearing. Assert it here, at the publish boundary.
# ---------------------------------------------------------------------------
sdk_msrv="$(cargo metadata --format-version 1 --no-deps |
    jq -r --arg c "$SDK_CRATE" '.packages[] | select(.name == $c) | .rust_version // ""')"
if [[ -z "$sdk_msrv" ]]; then
    err "${SDK_CRATE} declares no rust-version, so its published MSRV would be unset"
    exit 1
fi
log "${SDK_CRATE} declares MSRV ${sdk_msrv}"

# ---------------------------------------------------------------------------
# 1. Copy core at R out of the cargo git checkout. The copy gets patched,
#    and the checkout must stay pristine because builds read it.
# ---------------------------------------------------------------------------
R="$(core_rev Cargo.toml)"
iggy_manifest="$(cargo metadata --format-version 1 --locked |
    jq -r --arg source "$(core_package_source "$R")" \
        '.packages[] | select(.name == "iggy" and .source == $source) | .manifest_path')"
if [[ -z "$iggy_manifest" ]]; then
    err "cargo metadata lists no iggy package at R $R"
    exit 1
fi
core_root="$(dirname "$(cargo locate-project --workspace --message-format plain --manifest-path "$iggy_manifest")")"
log "Copying core at R $R from $core_root"
tar -C "$core_root" --exclude=./.git --exclude=./target -cf - . | tar -xf - -C "$CORE_COPY"

# ---------------------------------------------------------------------------
# 2. Start the local registry
# ---------------------------------------------------------------------------
log "Starting cargo-http-registry at 127.0.0.1:${REGISTRY_PORT}"
cargo-http-registry --addr "127.0.0.1:${REGISTRY_PORT}" "$REGISTRY_DIR" \
    > "$REGISTRY_LOG" 2>&1 &
REGISTRY_PID=$!

# cargo-http-registry speaks git-http, so `git ls-remote` is the closest
# thing to a health check. A GET on any URL path returns 405 because the
# git-upload-pack endpoint expects POST, so don't probe with plain curl.
for attempt in $(seq 1 20); do
    if git ls-remote "http://127.0.0.1:${REGISTRY_PORT}/git" HEAD >/dev/null 2>&1; then
        log "Registry is up (pid $REGISTRY_PID)"
        break
    fi
    if ! kill -0 "$REGISTRY_PID" 2>/dev/null; then
        err "cargo-http-registry died before becoming ready"
        cat "$REGISTRY_LOG" >&2 || true
        exit 1
    fi
    sleep 0.5
    if (( attempt == 20 )); then
        err "Registry failed to become ready within 10s"
        cat "$REGISTRY_LOG" >&2 || true
        exit 1
    fi
done

# The repository keeps its own .cargo/config.toml, so the registry is
# configured through the environment instead of a scratch config file.
export CARGO_REGISTRIES_LOCAL_DEV_INDEX="http://127.0.0.1:${REGISTRY_PORT}/git"
export CARGO_NET_GIT_FETCH_WITH_CLI=true
export CARGO_HTTP_CAINFO="$CA_BUNDLE"

# ---------------------------------------------------------------------------
# 3. Publish the core crates from the copy. Its workspace dependencies get
#    `registry = "local-dev"` so they resolve against local-dev, as in
#    core's scripts/verify-crates-publish.sh.
# ---------------------------------------------------------------------------
log "Patching the workspace dependencies of the core copy"
for crate in "${CORE_CRATES[@]}"; do
    sed -i.bak -E \
        "s|^(${crate} = \{ path = \"[^\"]+\", version = \"[^\"]+\")( \})$|\1, registry = \"${REGISTRY_NAME}\"\2|" \
        "$CORE_COPY/Cargo.toml"
done
rm -f "$CORE_COPY/Cargo.toml.bak"
require_registry_patch core "$CORE_COPY/Cargo.toml" "${CORE_CRATES[@]}"

for crate in "${CORE_CRATES[@]}"; do
    publish "$CORE_COPY/Cargo.toml" "$crate"
done

# ---------------------------------------------------------------------------
# 4. Publish the SDK. Cargo rejects `registry` next to `git`, so the core
#    crates that the SDK pins become path dependencies on the copy, with the
#    pinned version and the registry. The cleanup trap reverts the patch.
# ---------------------------------------------------------------------------
log "Patching workspace.dependencies to target ${REGISTRY_NAME}"
CARGO_FILES_PATCHED=true
versioned_crates=()
for entry in "${CORE_VERSIONED_CRATES[@]}"; do
    IFS=: read -r crate crate_dir _ <<< "$entry"
    version="$(core_pinned_version Cargo.toml "$crate")"
    sed -i.bak -E \
        "s|^${crate} = \{ *git = .*\}$|${crate} = { path = \"${CORE_COPY}/${crate_dir}\", version = \"${version}\", registry = \"${REGISTRY_NAME}\" }|" \
        Cargo.toml
    versioned_crates+=("$crate")
done
rm -f Cargo.toml.bak
require_registry_patch workspace Cargo.toml "${versioned_crates[@]}"

publish Cargo.toml "$SDK_CRATE"

# ---------------------------------------------------------------------------
# 5. Cross-check: every crate's tarball must actually exist on the registry
# ---------------------------------------------------------------------------
log "Verifying all crates appear in the local registry"
# crates-index path layout for 4+ char names: "<first-2>/<next-2>/<name>".
# Every published crate here is >=4 chars, so the shorter-name buckets
# (1/, 2/, 3/<c>/) from the full layout are intentionally not handled.
for crate in "${CORE_CRATES[@]}" "$SDK_CRATE"; do
    lc="$crate"  # relies on the crate names being lowercase (see definition above)
    rel="${lc:0:2}/${lc:2:2}/$lc"
    if [[ ! -f "$REGISTRY_DIR/$rel" ]]; then
        err "Crate ${crate} missing from local registry (expected at $rel)"
        ls -la "$REGISTRY_DIR" >&2 || true
        exit 2
    fi
done

log "The core crates at R and ${SDK_CRATE} published successfully to ${REGISTRY_NAME}"
log "Registry directory: $REGISTRY_DIR (will be cleaned up)"
exit 0
