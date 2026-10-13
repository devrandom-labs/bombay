#!/usr/bin/env bash
set -euo pipefail
base=6d3130f7a917659c3cae2de242baa7435d287472
proposal="$PWD/research/allocation-diagnostic"
evidence="$RUNNER_TEMP/allocation-diagnostic"
mkdir -p "$evidence/baseline" "$evidence/trace"
git archive "$base" > "$evidence/source.tar"
cp "$proposal/trace.patch" "$evidence/trace.patch"
sha256sum "$evidence/source.tar" "$evidence/trace.patch" > "$evidence/research-inputs.sha256"
for phase in baseline trace; do
  tar -xf "$evidence/source.tar" -C "$evidence/$phase"
done
(cd "$evidence/trace" && git apply "$evidence/trace.patch")
for phase in baseline trace; do
  source="$evidence/$phase"
  (cd "$source" && find . -type f -print0 | sort -z | xargs -0 sha256sum) > "$evidence/$phase.inputs.sha256"
  for attempt in 1 2 3; do
    prefix="$evidence/$phase.$attempt"
    (cd "$source" && sha256sum --check "$evidence/$phase.inputs.sha256") > "$prefix.before"
    set +e
    (cd "$source" && nix build --no-update-lock-file --no-link --rebuild \
      --print-build-logs .#checks.x86_64-linux.bombay-example-counter) \
      > "$prefix.stdout" 2> "$prefix.stderr"
    status=$?
    set -e
    printf '%s\n' "$status" > "$prefix.exit"
    (cd "$source" && sha256sum --check "$evidence/$phase.inputs.sha256") > "$prefix.after"
  done
done
