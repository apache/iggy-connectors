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

# Ports an apache/iggy pull request to this repository. It fetches the diff
# of the PR with gh, moves each path with the rules in scripts/port-pr.map,
# and applies the result with `git apply --3way`. Files that no rule maps
# stay in core, and the script lists them. A core file that this repository
# split into several files has the rule target -, and the script lists it
# to port by hand. If the patch does not apply cleanly, the script keeps
# the rewritten patch and prints its path.
#
# Usage: scripts/port-pr.sh <core PR number>

set -euo pipefail

CORE_REPO="apache/iggy"

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$REPO_ROOT"
MAP_FILE="scripts/port-pr.map"

if [[ $# -ne 1 || ! "$1" =~ ^[0-9]+$ ]]; then
  echo "Usage: $0 <core PR number>" >&2
  exit 1
fi
PR=$1

if ! command -v gh >/dev/null 2>&1; then
  echo "error: gh is required, see https://cli.github.com" >&2
  exit 1
fi

PATCH_FILE="$(mktemp -t "iggy-port-pr-$PR.XXXXXX")"
KEEP_PATCH=false
trap '[[ "$KEEP_PATCH" == true ]] || rm -f "$PATCH_FILE"' EXIT

CORE_DIFF="$(gh pr diff "$PR" --repo "$CORE_REPO")"

# Only the header lines of each file diff hold paths. A content line such as
# "--- a/x" inside a hunk stays as it is.
NO_MAPPED_PATH=2
BY_HAND_TARGET="-"
status=0
awk -v map_file="$MAP_FILE" -v no_mapped_path="$NO_MAPPED_PATH" -v by_hand_target="$BY_HAND_TARGET" '
  function map_path(path,    i, best, best_len) {
    best = 0
    best_len = 0
    for (i = 1; i <= rule_count; i++) {
      if (length(rule_from[i]) > best_len && substr(path, 1, length(rule_from[i])) == rule_from[i]) {
        best = i
        best_len = length(rule_from[i])
      }
    }
    by_hand = best && rule_to[best] == by_hand_target
    return best && !by_hand ? rule_to[best] substr(path, best_len + 1) : ""
  }
  BEGIN {
    while ((getline line < map_file) > 0) {
      sub(/#.*/, "", line)
      field_count = split(line, fields, " ")
      if (field_count == 0) continue
      if (field_count != 2) {
        print "error: " map_file " needs <old prefix> <new prefix> per rule, got: " line > "/dev/stderr"
        failed = 1
        exit
      }
      rule_count++
      rule_from[rule_count] = fields[1]
      rule_to[rule_count] = fields[2]
    }
  }
  /^diff --git / {
    if (NF != 4 || substr($3, 1, 2) != "a/" || substr($4, 1, 2) != "b/") {
      print "error: unsupported diff header: " $0 > "/dev/stderr"
      failed = 1
      exit
    }
    old_path = map_path(substr($3, 3))
    old_by_hand = by_hand
    new_path = map_path(substr($4, 3))
    skipping = (old_path == "" || new_path == "")
    if (skipping) {
      reason = (old_by_hand || by_hand) ? "Port by hand: " : "Stays in core: "
      print reason substr($4, 3) > "/dev/stderr"
      next
    }
    in_header = 1
    ported++
    print "diff --git a/" old_path " b/" new_path
    next
  }
  skipping { next }
  /^@@ / { in_header = 0 }
  in_header && /^--- a\// { print "--- a/" old_path; next }
  in_header && /^\+\+\+ b\// { print "+++ b/" new_path; next }
  in_header && /^(rename|copy) from / { print $1 " from " old_path; next }
  in_header && /^(rename|copy) to / { print $1 " to " new_path; next }
  { print }
  END {
    if (failed) exit 1
    if (!ported) exit no_mapped_path
  }
' <<< "$CORE_DIFF" > "$PATCH_FILE" || status=$?
if [[ $status -eq $NO_MAPPED_PATH ]]; then
  echo "error: PR #$PR changes no path that $MAP_FILE maps" >&2
fi
if [[ $status -ne 0 ]]; then
  exit 1
fi

if ! git apply --3way "$PATCH_FILE"; then
  KEEP_PATCH=true
  echo "error: the patch did not apply cleanly. Resolve the conflicts that git reports above." >&2
  echo "The rewritten patch is at $PATCH_FILE" >&2
  exit 1
fi
echo "Applied the connector part of $CORE_REPO#$PR. Review it with git diff HEAD."
