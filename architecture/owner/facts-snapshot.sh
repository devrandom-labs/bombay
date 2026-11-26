#!/usr/bin/env bash
# Usage: facts-snapshot.sh <state folder>
# Report the design and quality facts of HEAD and changes since the previous
# distinct snapshot. Keep TSV and commit records in <state folder>/facts and
# N-Quads in <state folder> for graph/build.sh. Restore those files from a
# backup to undo a snapshot or remove them to start over.
set -euo pipefail
export LC_ALL=C CDPATH=
umask 077

if [ "$#" != 1 ] || [ -z "$1" ]; then
  echo 'Usage: facts-snapshot.sh <state folder>' >&2
  exit 2
fi
architecture=$(cd -- "$(dirname -- "$0")/.." && pwd)
root=$(dirname -- "$architecture")
# shellcheck source=architecture/project.env
source "$architecture/project.env"
project=${HARNESS_REPO##*/}
commit=$(git -C "$root" rev-parse --short=12 HEAD)
mkdir -p -- "$1/facts"
state=$(cd -- "$1" && pwd)
facts=$state/facts
work=$(mktemp -d)
trap 'rm -rf -- "$work"' EXIT

snapshot() {
  local name=$1 tool=$2
  shift 2
  local latest=$facts/$name-latest previous=$facts/$name-previous hash prior
  if [ ! -f "$architecture/$tool" ]; then
    printf '%s: not in this tree (%s)\n' "$name" "architecture/$tool"
    return 0
  fi
  if ! (cd -- "$root" && bash "$architecture/$tool" "$@") > "$work/new.tsv" 2> "$work/error"; then
    printf '%s: the tool failed: %s\n' "$name" "$(tail -n 2 "$work/error" | tr '\n' ' ')"
    return 1
  fi
  if ! awk -F '\t' '!/^#/ { if (NF != 4) bad = 1; else rows++ }
    END { exit (bad || !rows) }' "$work/new.tsv"; then
    printf '%s: the tool failed: expected at least one row with four tab-separated fields\n' "$name"
    return 1
  fi
  jq -R -r --arg project "$project" --arg source "$name" --arg commit "$commit" \
    -f "$architecture/graph/facts.jq" "$work/new.tsv" > "$work/new.nq" || return 1
  hash=$(sha256sum < "$work/new.tsv" | cut -c1-12) || return 1

  if [ -f "$latest.tsv" ] && ! cmp -s "$work/new.tsv" "$latest.tsv"; then
    cp -- "$latest.tsv" "$previous.tsv" || return 1
    if [ -f "$latest.commit" ]; then
      cp -- "$latest.commit" "$previous.commit" || return 1
    fi
  fi
  mv -f -- "$work/new.tsv" "$latest.tsv" || return 1
  printf '%s\n' "$commit" > "$latest.commit" || return 1
  mv -f -- "$work/new.nq" "$state/$name.nq" || return 1

  printf '%s at %s (%s):\n' "$name" "$commit" "$hash"
  awk -F '\t' '!/^#/ && NF >= 3 { n[$1]++; s[$1] += $3 }
    END { for (k in n) printf "  %-18s rows %4d  sum %6d\n", k, n[k], s[k] }' "$latest.tsv" | sort || return 1
  if [ ! -f "$previous.tsv" ]; then
    echo '  no snapshot before this one'
    return 0
  fi
  prior=$(cat -- "$previous.commit" 2> /dev/null || echo 'the snapshot before')
  printf '  changed since %s:\n' "$prior"
  awk -F '\t' '
    /^#/ || NF < 3 { next }
    FILENAME == ARGV[1] { old[$1 "\t" $2] = $3; next }
    {
      k = $1 "\t" $2; seen[k] = 1
      if (!(k in old)) printf "    new   %s  %s  = %s\n", $1, substr($2, 1, 70), $3
      else if (old[k] != $3) printf "    %s  %s  %s  %s -> %s\n", ($3 + 0 > old[k] + 0 ? "up  " : "down"), $1, substr($2, 1, 70), old[k], $3
    }
    END { for (k in old) if (!(k in seen)) { split(k, p, "\t"); printf "    gone  %s  %s  (was %s)\n", p[1], substr(p[2], 1, 70), old[k] } }
  ' "$previous.tsv" "$latest.tsv" | sort | awk '
    NR <= 25 { print }
    END { if (NR > 25) printf "    %d changed rows left out\n", NR - 25 }
  '
}

result=0
snapshot design-facts design/facts.sh apps/server/src apps/studio crates || result=1
snapshot quality-facts quality/facts.sh apps/server apps/studio crates || result=1
exit "$result"
