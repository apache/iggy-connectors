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

set -euo pipefail

# shellcheck source-path=SCRIPTDIR
source "$(dirname "${BASH_SOURCE[0]}")/lib/init.sh"

# runtime-image-smoke.sh -- Check that an apache/iggy-connect image can build
# the runtime's HTTP client.
#
# reqwest verifies TLS against the system CA store and refuses to build a
# client when the store is empty, even for http:// URLs. Host-based tests
# always run with system certificates, so only a run of the image itself can
# catch an image that ships without them.
#
# The check starts the image with HTTP state storage, which builds the client
# at startup before the runtime connects to Iggy. It passes only when the
# runtime logs that the client was built. No Iggy server or state server is
# needed, because the runtime builds the client without connecting to either.
#
# Usage: runtime-image-smoke.sh <image>
# Exit codes: 0 = the client was built, 1 = it was not, or an error.

readonly SUCCESS_LINE="State will be stored via HTTP"
readonly CA_FAILURE_LINE="Failed to build the state HTTP client"
readonly TIMEOUT_SECONDS=30

if [ "$#" -ne 1 ]; then
  echo "Usage: $0 <image>" >&2
  exit 1
fi
image="$1"

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
container="iggy-connect-smoke-$$"

trap 'docker rm -f "$container" >/dev/null 2>&1 || true' EXIT

docker run --detach --name "$container" \
  --volume "$repo_root/runtime/config.toml:/app/config.toml:ro" \
  --env IGGY_CONNECTORS_CONFIG_PATH=/app/config.toml \
  --env IGGY_CONNECTORS_STATE_STORAGE=http \
  --env IGGY_CONNECTORS_STATE_HTTP_URL=http://127.0.0.1:9999 \
  "$image" >/dev/null

deadline=$((SECONDS + TIMEOUT_SECONDS))
while :; do
  logs="$(docker logs "$container" 2>&1)"
  if grep -qF "$SUCCESS_LINE" <<<"$logs"; then
    echo "OK: $image built the runtime HTTP client"
    exit 0
  fi
  running="$(docker inspect --format '{{.State.Running}}' "$container")"
  if [ "$running" != "true" ] || [ "$SECONDS" -ge "$deadline" ]; then
    break
  fi
  sleep 1
done

# The container can print its last lines between the read above and the exit
# check, so read the full output once more before reporting it.
logs="$(docker logs "$container" 2>&1)"
echo "FAIL: $image did not build the runtime HTTP client" >&2
if grep -qF "$CA_FAILURE_LINE" <<<"$logs"; then
  echo "The image has no system CA certificates. Install ca-certificates in the runtime stage of runtime/Dockerfile." >&2
fi
echo "--- container output ---" >&2
echo "$logs" >&2
exit 1
