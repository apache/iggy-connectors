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

# Prints "true" when core released the crates that the SDK pins from the
# core pin, and "false" with the reason on stderr otherwise. The published
# SDK resolves those crates from crates.io, and crates.io can hold the same
# version from an older commit than the core pin. Core tags a crate release
# only after it published the crate, so a release tag that points to the
# core pin proves the crates.io copy.
#
# Usage: scripts/ci/core-released-at-pin.sh

set -euo pipefail

# shellcheck source-path=SCRIPTDIR
source "$(dirname "${BASH_SOURCE[0]}")/lib/core-pin.sh"
# shellcheck source-path=SCRIPTDIR
source "$(dirname "${BASH_SOURCE[0]}")/lib/git-tag.sh"

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$REPO_ROOT"

CORE_REV=$(core_rev Cargo.toml)

released=true
for entry in "${CORE_VERSIONED_CRATES[@]}"; do
  IFS=: read -r crate _ tag_prefix <<< "$entry"
  tag="$tag_prefix-$(core_pinned_version Cargo.toml "$crate")"
  commit=$(tag_commit "$CORE_GIT_URL" "$tag")
  if [[ "$commit" != "$CORE_REV" ]]; then
    echo "core tag $tag points to ${commit:-no commit}, not to the core pin $CORE_REV" >&2
    released=false
  fi
done
echo "$released"
