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

# Checks the core pin, the apache/iggy commit in the root Cargo.toml.
# Cargo accepts `version = "0.12.0"` next to a git rev whose crate is 0.12.3,
# but the published SDK would then ask crates.io for a core it was never
# tested with. So each pinned version must equal the version at the core
# pin. The resolved graph must also hold one copy of core only, the one at
# the core pin.

set -euo pipefail

# shellcheck source-path=SCRIPTDIR
source "$(dirname "${BASH_SOURCE[0]}")/lib/init.sh"
# shellcheck source-path=SCRIPTDIR
source "$(dirname "${BASH_SOURCE[0]}")/lib/core-pin.sh"

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$REPO_ROOT"

CORE_REV=$(core_rev Cargo.toml)
EXPECTED_SOURCE=$(core_package_source "$CORE_REV")
METADATA=$(cargo metadata --format-version 1 --locked)

FAILED=0

other_sources=$(jq -r --arg url "$CORE_GIT_URL" --arg expected "$EXPECTED_SOURCE" '
  [.packages[] | .source // ""
   | select(startswith("git+\($url)") and (ltrimstr("git+\($url)") | test("^($|[?#])")))]
  | unique | map(select(. != $expected)) | .[]' <<< "$METADATA")
if [[ -n "$other_sources" ]]; then
  echo "error: the dependency graph holds core from other sources than $EXPECTED_SOURCE:" >&2
  echo "$other_sources" >&2
  FAILED=1
fi

while read -r crate _ pinned; do
  [[ "$pinned" == - ]] && continue
  resolved=$(jq -r --arg name "$crate" --arg source "$EXPECTED_SOURCE" '
    [.packages[] | select(.name == $name and .source == $source) | .version] | join(" ")' <<< "$METADATA")
  if [[ "$resolved" == "$pinned" ]]; then
    echo "$crate $pinned matches the core pin $CORE_REV"
  else
    echo "error: Cargo.toml pins $crate at $pinned, but $crate at the core pin $CORE_REV is ${resolved:-missing}" >&2
    echo "Run scripts/bump-core-pin.sh $CORE_REV to fix the pin." >&2
    FAILED=1
  fi
done < <(core_dependencies Cargo.toml)

exit "$FAILED"
