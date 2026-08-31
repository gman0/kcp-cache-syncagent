#!/usr/bin/env bash

# Copyright 2025 The KCP Authors.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

set -euo pipefail

cd $(dirname $0)/../..
source hack/lib.sh

EXIT_CODE=0
SUMMARY=

try() {
  local title="$1"
  shift

  heading "$title"
  echo

  start_time=$(date +%s)

  set +e
  $@
  exitCode=$?
  set -e

  elapsed_time=$(($(date +%s) - $start_time))
  TEST_NAME="$title" write_junit $exitCode "$elapsed_time"

  local status
  if [[ $exitCode -eq 0 ]]; then
    echo -e "\n[${elapsed_time}s] SUCCESS :)"
    status=OK
  else
    echo -e "\n[${elapsed_time}s] FAILED."
    status=FAIL
    EXIT_CODE=1
  fi

  SUMMARY="$SUMMARY\n$(printf "%-35s %s" "$title" "$status")"

  git reset --hard --quiet
  git clean --force

  echo
}

verify_codegen() {
  make codegen

  echo "Diffing…"
  if ! git diff --exit-code deploy internal sdk; then
    echo "The generated code / CRDs are out of date. Please run 'make codegen'."
    return 1
  fi

  echo "The generated code / CRDs is up to date."
}

verify_gomod() {
  go mod tidy
	go mod verify
	git diff --exit-code
}

verify_imports() {
  make imports

  echo "Diffing…"
  if ! git diff --exit-code; then
    echo "Some import statements are not properly grouped. Please run 'make imports'."
    return 1
  fi

  echo "Your Go import statements are in order :-)"
}

try "Verify code generation" verify_codegen
try "Verify go.mod" verify_gomod
try "Verify Go imports" verify_imports
try "Verify license compatibility" ./hack/verify-licenses.sh
try "Verify boilerplate" ./hack/verify-boilerplate.sh

echo
echo "SUMMARY"
echo "======="
echo
echo "Check                               Result"
echo -n "------------------------------------------"
echo -e "$SUMMARY"

exit $EXIT_CODE
