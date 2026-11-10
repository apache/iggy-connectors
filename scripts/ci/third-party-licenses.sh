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

# Validate or generate a LICENSE-binary manifest for a convenience
# binary artifact (the Docker image). ASF release policy requires that
# each artifact bundling third-party code carries a license enumeration
# of the packages it actually bundles. This script drives cargo-about to
# produce that enumeration.
#
# Usage: third-party-licenses.sh <action> --manifest <path> [--manifest <path> ...] [--output <file>]
#
# Actions (exactly one required):
#   --validate   Verify deps satisfy the accept list. No file written.
#                Exits non-zero on disallowed license.
#   --generate   Render the manifest to --output.
#
# --manifest takes the path to a Cargo.toml. Multiple --manifest flags
# are rendered as a deduplicated union via a synthetic root crate.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"

# cargo-about shells out to `cargo metadata`, which refreshes the crates.io
# index and downloads dep manifests. Both can fail transiently on a network
# blip ("Connection reset by peer") even past cargo's own net retries. Retry
# the whole invocation a few times so a flake never reddens a release gate.
CARGO_ABOUT_ATTEMPTS=3
CARGO_ABOUT_RETRY_DELAY_SECONDS=5

ACTION=""
MANIFESTS=()
OUTPUT=""

usage() {
  sed -n '/^# Usage:/,/^set -euo/{/^set -euo/d;p}' "$0" | sed 's/^# \{0,1\}//' >&2
}

die() {
  echo "error: $*" >&2
  exit 1
}

while (($# > 0)); do
  case "$1" in
    --validate)
      [[ -n "$ACTION" ]] && die "actions --validate and --generate are mutually exclusive"
      ACTION="validate"
      shift
      ;;
    --generate)
      [[ -n "$ACTION" ]] && die "actions --validate and --generate are mutually exclusive"
      ACTION="generate"
      shift
      ;;
    --manifest)
      [[ $# -lt 2 ]] && die "--manifest requires a path argument"
      MANIFESTS+=("$2")
      shift 2
      ;;
    --output)
      [[ $# -lt 2 ]] && die "--output requires a file argument"
      OUTPUT="$2"
      shift 2
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      die "unknown argument: $1"
      ;;
  esac
done

[[ -z "$ACTION" ]] && { usage; die "missing action (--validate or --generate)"; }
[[ ${#MANIFESTS[@]} -eq 0 ]] && die "at least one --manifest is required"
[[ "$ACTION" == "generate" && -z "$OUTPUT" ]] && die "--generate requires --output"

RUST_MANIFESTS=()
for m in "${MANIFESTS[@]}"; do
  if [[ -f "$m" && "$(basename "$m")" == "Cargo.toml" ]]; then
    RUST_MANIFESTS+=("$m")
  else
    die "manifest is not a Cargo.toml: $m"
  fi
done

require_cmd() {
  command -v "$1" >/dev/null 2>&1 || die "required command not installed: $1"
}

SYNTH_DIR=""
TMP_OUT=""

cleanup() {
  [[ -n "$SYNTH_DIR" && -d "$SYNTH_DIR" ]] && rm -rf "$SYNTH_DIR"
  [[ -n "$TMP_OUT" && -f "$TMP_OUT" ]] && rm -f "$TMP_OUT"
  return 0
}
trap cleanup EXIT

# Resolve the Cargo.toml that cargo-about should target. For a single
# Rust manifest, return it as-is. For multiple Rust manifests, build a
# synthetic root crate under /tmp that path-deps onto each requested
# member; cargo-about then renders the deduplicated union in a single
# pass. The empty [workspace] table prevents the synthetic root from
# attaching to any ancestor workspace.
build_rust_target_manifest() {
  local count=${#RUST_MANIFESTS[@]}
  if (( count == 1 )); then
    echo "${RUST_MANIFESTS[0]}"
    return
  fi

  SYNTH_DIR="$(mktemp -d -t iggy-license-root-XXXXXX)"
  # Reuse the repo lock. A fresh resolve drifts from what the binaries
  # link and fails once every matching version of a dep is yanked.
  cp "$REPO_ROOT/Cargo.lock" "$SYNTH_DIR/Cargo.lock"

  {
    printf '[workspace]\n\n'
    printf '[package]\n'
    printf 'name = "iggy-license-union"\n'
    printf 'version = "0.0.0"\n'
    printf 'edition = "2021"\n'
    printf 'license = "Apache-2.0"\n'
    printf 'publish = false\n\n'
    printf '[dependencies]\n'
    for m in "${RUST_MANIFESTS[@]}"; do
      local crate_dir crate_name
      crate_dir="$(cd "$(dirname "$m")" && pwd)"
      crate_name="$(awk -F'"' '/^\[package\]/{p=1; next} p && /^name *= *"/{print $2; exit}' "$m")"
      [[ -z "$crate_name" ]] && die "could not extract package.name from $m"
      printf '%s = { path = "%s" }\n' "$crate_name" "$crate_dir"
    done
  } > "$SYNTH_DIR/Cargo.toml"

  mkdir -p "$SYNTH_DIR/src"
  : > "$SYNTH_DIR/src/lib.rs"

  echo "$SYNTH_DIR/Cargo.toml"
}

# Run cargo-about against the prepared manifest, writing the result to
# the supplied path. Always invoked from REPO_ROOT so that
# `--config about.toml` and the about.hbs template resolve correctly.
# `--fail` makes a disallowed license abort with a non-zero exit code.
run_cargo_about() {
  local target_manifest="$1"
  local out="$2"
  require_cmd cargo-about

  local attempt
  for ((attempt = 1; attempt <= CARGO_ABOUT_ATTEMPTS; attempt++)); do
    if ( cd "$REPO_ROOT" && \
      cargo about generate \
        --config about.toml \
        --manifest-path "$target_manifest" \
        --fail \
        -o "$out" \
        about.hbs ); then
      return 0
    fi
    if ((attempt < CARGO_ABOUT_ATTEMPTS)); then
      echo "warning: cargo-about failed (attempt ${attempt}/${CARGO_ABOUT_ATTEMPTS}), retrying in ${CARGO_ABOUT_RETRY_DELAY_SECONDS}s" >&2
      sleep "$CARGO_ABOUT_RETRY_DELAY_SECONDS"
    fi
  done
  return 1
}

case "$ACTION" in
  validate)
    TMP_OUT="$(mktemp)"
    target="$(build_rust_target_manifest)"
    echo "Validating Rust manifests: ${RUST_MANIFESTS[*]}"
    if ! run_cargo_about "$target" "$TMP_OUT"; then
      die "validation failed for: ${RUST_MANIFESTS[*]}"
    fi
    echo "All manifests pass third-party license validation."
    ;;

  generate)
    target="$(build_rust_target_manifest)"
    echo "Generating Rust manifest section: ${RUST_MANIFESTS[*]}"
    run_cargo_about "$target" "$OUTPUT"
    echo "Done. Wrote $OUTPUT"
    ;;
esac
