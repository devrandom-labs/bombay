#!/usr/bin/env bash
# Set one issue's board Status and print its old and new values.
# Usage: status.sh <card> <status> [--dry-run]
# To reverse, set the old value or use gh project item-edit --clear if it was unset.
set -euo pipefail
# shellcheck source=architecture/cards/common.sh
. "$(dirname -- "$0")/common.sh"
set -- "${args[@]+"${args[@]}"}"

if [ "$#" != 2 ] || ! issue_number "$1" || [ -z "$2" ]; then
  usage '<card> <status>'
fi
change_field Status "$1" "$2"
