#!/usr/bin/env bash
# Exercise the board reader itself with paginated GitHub responses.
# Run from the repository root. An optional script path permits guard tests.
set -euo pipefail

board=${1:-$PWD/architecture/board.sh}
fixtures=$PWD/architecture/tests/fixtures/board
work=$(mktemp -d)
trap 'rm -rf "$work"' EXIT
fail() { echo "board-test: FAILED: $1"; cat "$work/error" 2> /dev/null; exit 1; }

mkdir "$work/bin"
cat > "$work/bin/gh" <<'STUB'
#!/bin/sh
[ "$#" = 9 ] && [ "$1 $2 $3 $4" = 'api graphql --paginate -F' ] \
  && [ "$6 $7 $8 $9" = '-f login=labcast -F number=2' ] || exit 98
case "$5" in query=@*) [ -r "${5#query=@}" ] || exit 98 ;; *) exit 98 ;; esac
cat "$BOARD_PAGES"
exit "$BOARD_CODE"
STUB
chmod +x "$work/bin/gh"
export PATH="$work/bin:$PATH" BOARD_PAGES="$fixtures/pages.json" BOARD_CODE=0

bash "$board" > "$work/out" 2> "$work/error" || fail 'two pages: reader failed'
jq -S . "$work/out" > "$work/actual"
jq -S . "$fixtures/expected.json" > "$work/expected"
cmp "$work/actual" "$work/expected" || fail 'two pages: document differs'

(cd "$(dirname "$board")" && bash "$(basename "$board")") > "$work/out" 2> "$work/error" || fail 'bare name: reader failed'
jq -S . "$work/out" > "$work/actual"
cmp "$work/actual" "$work/expected" || fail 'bare name: document differs'

# A failed gh may still have printed a complete response.
if BOARD_CODE=1 bash "$board" > "$work/out" 2> "$work/error"; then fail 'gh failure: reader succeeded'; fi
[ ! -s "$work/out" ] || fail 'gh failure: reader printed a document'

# Errors on the last page must reject even a complete item count.
jq -s '.[1].errors = [{message:"unavailable field"}] | .[]' "$fixtures/pages.json" > "$work/errors.json"
if BOARD_PAGES="$work/errors.json" bash "$board" > "$work/out" 2> "$work/error"; then fail 'GraphQL errors: reader succeeded'; fi
[ ! -s "$work/out" ] || fail 'GraphQL errors: reader printed a document'

jq -s '.[1].data.organization.projectV2.items.nodes = [] | .[]' "$fixtures/pages.json" > "$work/short.json"
if BOARD_PAGES="$work/short.json" bash "$board" > "$work/out" 2> "$work/error"; then fail 'item count: reader succeeded'; fi
[ ! -s "$work/out" ] || fail 'item count: reader printed a document'

jq -s '.[1].data.organization.projectV2.items.nodes[0].id = .[0].data.organization.projectV2.items.nodes[0].id | .[]' \
  "$fixtures/pages.json" > "$work/duplicate.json"
if BOARD_PAGES="$work/duplicate.json" bash "$board" > "$work/out" 2> "$work/error"; then fail 'duplicate: reader succeeded'; fi
[ ! -s "$work/out" ] || fail 'duplicate: reader printed a document'
grep -q 'board: duplicate item IDs' "$work/error" || fail 'duplicate: no diagnostic'

# A changed page total must reject even when all items of the first total arrive.
jq -s '.[1].data.organization.projectV2.items.totalCount = 4 | .[]' "$fixtures/pages.json" > "$work/changed.json"
if BOARD_PAGES="$work/changed.json" bash "$board" > "$work/out" 2> "$work/error"; then fail 'page total: reader succeeded'; fi
[ ! -s "$work/out" ] || fail 'page total: reader printed a document'

echo 'board-test: two pages, bare name and five failure cases pass'
