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

# Builds iggy-server from apache/iggy at R, the core commit that Cargo.toml
# pins, and copies it to <target>/debug/iggy-server. The integration tests
# start the server from there. A build for the same R is reused, so CI can
# cache <target>/iggy-server-<R> by R.
#
# Usage:
#   scripts/build-test-server.sh              build if needed, then copy
#   scripts/build-test-server.sh --print-dir  print <target>/iggy-server-<R>

set -euo pipefail

# shellcheck source-path=SCRIPTDIR
source "$(dirname "${BASH_SOURCE[0]}")/ci/lib/core-pin.sh"

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$REPO_ROOT"

R=$(core_rev Cargo.toml)
TARGET_DIR=$(cargo metadata --format-version 1 --no-deps --offline | jq -r .target_directory)
SERVER_ROOT="$TARGET_DIR/iggy-server-$R"
SERVER_BIN="$SERVER_ROOT/bin/iggy-server"

case "${1:-}" in
  --print-dir)
    printf '%s\n' "$SERVER_ROOT"
    exit 0
    ;;
  "") ;;
  *)
    echo "Usage: $0 [--print-dir]" >&2
    exit 1
    ;;
esac

if [[ -x "$SERVER_BIN" ]]; then
  echo "Reusing $SERVER_BIN"
else
  cargo install --locked --debug --git "$CORE_GIT_URL" --rev "$R" --root "$SERVER_ROOT" --bin iggy-server server
fi

mkdir -p "$TARGET_DIR/debug"
# A rename replaces a binary that a running test still executes.
cp "$SERVER_BIN" "$TARGET_DIR/debug/iggy-server.tmp"
mv -f "$TARGET_DIR/debug/iggy-server.tmp" "$TARGET_DIR/debug/iggy-server"
echo "Copied iggy-server at $R to $TARGET_DIR/debug/iggy-server"
