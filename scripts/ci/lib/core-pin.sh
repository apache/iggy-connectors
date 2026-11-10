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

# The root Cargo.toml pins every apache/iggy git dependency to one core
# commit, R. These helpers read that pin from the manifest text, so they
# work on a lone copy of the file and need no network. Each core dependency
# is a one-line inline table that starts with its git key:
#
#   iggy = { git = "https://github.com/apache/iggy", rev = "<R>", version = "<v>" }
#
# scripts/ci/check-r-pin.sh compares the result with the resolved graph.

CORE_GIT_URL="https://github.com/apache/iggy"
CORE_DEPENDENCY_RE='^([A-Za-z0-9_-]+) = \{ *git = "https://github\.com/apache/iggy"[ ,}]'
CORE_REV_RE='[{ ,]rev = "([^"]*)"'
CORE_VERSION_RE='[{ ,]version = "([^"]*)"'
# The published SDK depends on these core crates through crates.io, so the
# workspace pins their version next to R. Each entry is
# "<crate>:<directory in apache/iggy>:<tag prefix of its core release>",
# with the tag prefix from tag_pattern in core's .github/config/publish.yml.
# shellcheck disable=SC2034 # read by the scripts that source this file
CORE_VERSIONED_CRATES=(iggy_common:core/common:common iggy:core/sdk:iggy)

# Prints the package source that cargo metadata reports for core at rev $1.
core_package_source() {
  printf 'git+%s?rev=%s#%s\n' "$CORE_GIT_URL" "$1" "$1"
}

# Prints "<name> <rev> <version>" for each core dependency of manifest $1.
# A missing rev or version prints as "-".
core_dependencies() {
  local manifest=$1 line name rev version
  while IFS= read -r line || [[ -n "$line" ]]; do
    [[ "$line" =~ $CORE_DEPENDENCY_RE ]] || continue
    name=${BASH_REMATCH[1]}
    rev=-
    version=-
    if [[ "$line" =~ $CORE_REV_RE ]]; then
      rev=${BASH_REMATCH[1]}
    fi
    if [[ "$line" =~ $CORE_VERSION_RE ]]; then
      version=${BASH_REMATCH[1]}
    fi
    printf '%s %s %s\n' "$name" "$rev" "$version"
  done < "$manifest"
}

# Prints R. Fails unless every core dependency of manifest $1 uses the same
# full commit hash, because a second rev builds a second copy of core.
core_rev() {
  local manifest=$1 revs
  revs=$(core_dependencies "$manifest" | cut -d' ' -f2 | sort -u)
  if [[ ! "$revs" =~ ^[0-9a-f]{40}$ ]]; then
    echo "error: every $CORE_GIT_URL dependency in $manifest must use one full commit rev, found: ${revs//$'\n'/ }" >&2
    return 1
  fi
  printf '%s\n' "$revs"
}

# Prints the version that manifest $1 pins next to the rev of core crate $2.
core_pinned_version() {
  local manifest=$1 crate=$2 version
  version=$(core_dependencies "$manifest" | awk -v crate="$crate" '$1 == crate { print $3 }')
  if [[ ! "$version" =~ ^[0-9]+\.[0-9]+\.[0-9]+(-[0-9A-Za-z.-]+)?$ ]]; then
    echo "error: $crate in $manifest must have one plain version next to its git rev, found: ${version:-no entry}" >&2
    return 1
  fi
  printf '%s\n' "$version"
}
