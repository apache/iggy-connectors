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

# Prints the commit that tag $2 points to in git remote $1, or nothing. An
# annotated tag is a separate object, so its peeled "^{}" line holds the
# commit.
tag_commit() {
  local remote=$1 ref="refs/tags/$2"
  git ls-remote --tags "$remote" "$ref" "$ref^{}" |
    awk -v ref="$ref" '$2 == ref { sha = $1 } $2 == ref "^{}" { peeled = $1 } END { print (peeled != "" ? peeled : sha) }'
}
