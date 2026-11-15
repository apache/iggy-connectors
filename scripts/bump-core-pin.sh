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
source "$(dirname "${BASH_SOURCE[0]}")/ci/lib/init.sh"
# shellcheck source-path=SCRIPTDIR
source "$(dirname "${BASH_SOURCE[0]}")/ci/lib/core-pin.sh"

usage() {
  cat <<'USAGE'
Usage: scripts/bump-core-pin.sh <core tag, branch or commit>

Moves the core pin, the apache/iggy commit that the workspace pins, to
the given ref. It sets the rev of every core git dependency in the root
Cargo.toml to that commit, and the iggy and iggy_common versions to the
versions at that commit. Then it runs `cargo update` for the core git
packages.
USAGE
}

die() {
  echo "error: $*" >&2
  exit 1
}

REF=""
while (($# > 0)); do
  case "$1" in
    -h|--help)
      usage
      exit 0
      ;;
    -*)
      die "unknown option: $1"
      ;;
    *)
      [[ -z "$REF" ]] || die "expected one core ref, got '$REF' and '$1'"
      REF=$1
      shift
      ;;
  esac
done
if [[ -z "$REF" ]]; then
  usage >&2
  exit 1
fi

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
MANIFEST="$REPO_ROOT/Cargo.toml"

OLD_REV=$(core_rev "$MANIFEST")

SCRATCH_DIR="$(mktemp -d -t iggy-bump-core-pin.XXXXXX)"
trap 'rm -rf "$SCRATCH_DIR"' EXIT
CORE_GIT_DIR="$SCRATCH_DIR/core.git"
git init --quiet --bare "$CORE_GIT_DIR"
git --git-dir="$CORE_GIT_DIR" fetch --quiet --depth=1 "$CORE_GIT_URL" "$REF"
NEW_REV=$(git --git-dir="$CORE_GIT_DIR" rev-parse --verify 'FETCH_HEAD^{commit}')

# Prints the [package] version of a manifest at NEW_REV.
package_version() {
  git --git-dir="$CORE_GIT_DIR" show "$NEW_REV:$1" |
    awk -F'"' '/^\[/ { in_package = ($0 == "[package]") } in_package && /^version *=/ { print $2; exit }'
}

declare -A NEW_VERSIONS=()
for entry in "${CORE_VERSIONED_CRATES[@]}"; do
  IFS=: read -r crate crate_dir _ <<< "$entry"
  version=$(package_version "$crate_dir/Cargo.toml")
  [[ "$version" =~ ^[0-9]+\.[0-9]+\.[0-9]+(-[0-9A-Za-z.-]+)?$ ]] ||
    die "$crate_dir/Cargo.toml has no plain [package] version at $NEW_REV"
  NEW_VERSIONS[$crate]=$version
done

# Prints line $1 with the string value of key $2 set to $3.
set_field() {
  sed -E "s/([{ ,]$2 = \")[^\"]*\"/\\1$3\"/" <<< "$1"
}

rewrite_manifest() {
  local line name
  while IFS= read -r line || [[ -n "$line" ]]; do
    if [[ "$line" =~ $CORE_DEPENDENCY_RE ]]; then
      name=${BASH_REMATCH[1]}
      line=$(set_field "$line" rev "$NEW_REV")
      if [[ -n "${NEW_VERSIONS[$name]:-}" ]]; then
        line=$(set_field "$line" version "${NEW_VERSIONS[$name]}")
      fi
    fi
    printf '%s\n' "$line"
  done < "$MANIFEST"
}

rewrite_manifest > "$SCRATCH_DIR/Cargo.toml"
cat "$SCRATCH_DIR/Cargo.toml" > "$MANIFEST"

[[ "$(core_rev "$MANIFEST")" == "$NEW_REV" ]] || die "$MANIFEST still pins another rev"
for crate in "${!NEW_VERSIONS[@]}"; do
  [[ "$(core_pinned_version "$MANIFEST" "$crate")" == "${NEW_VERSIONS[$crate]}" ]] ||
    die "$MANIFEST does not pin $crate at ${NEW_VERSIONS[$crate]}"
done

echo "core pin: $OLD_REV -> $NEW_REV ($REF)"
for crate in "${!NEW_VERSIONS[@]}"; do
  echo "$crate: ${NEW_VERSIONS[$crate]}"
done

update_args=()
while read -r name _; do
  update_args+=(-p "$name")
done < <(core_dependencies "$MANIFEST")
(cd "$REPO_ROOT" && cargo update "${update_args[@]}")
