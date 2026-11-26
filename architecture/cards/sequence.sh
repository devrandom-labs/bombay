#!/usr/bin/env bash
# Set one issue's board Sequence and print its old and new values.
# Usage: sequence.sh <card> <sequence> [--dry-run]
# To reverse, set the old value or use gh project item-edit --clear if it was unset.
set -euo pipefail
# shellcheck source=architecture/cards/common.sh
. "$(dirname -- "$0")/common.sh"
set -- "${args[@]+"${args[@]}"}"

if [ "$#" != 2 ] || ! issue_number "$1" || ! sequence_number "$2"; then
  usage '<card> <sequence>'
fi
change_field Sequence "$1" "$2"
