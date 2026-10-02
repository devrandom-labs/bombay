#!/usr/bin/env bash
set -euo pipefail

fuzz_root=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
campaign_corpus=$(mktemp -d)
mkdir -p "$fuzz_root/artifacts/causal_turns"
run_artifacts=$(mktemp -d "$fuzz_root/artifacts/causal_turns/run.XXXXXXXX")
{
  rustc --version --verbose
  cargo fuzz --version
  printf '%s\n' 'target=causal_turns' 'seed=20260928' 'runs=2048' 'max_len=4096' 'sanitizer=address'
} > "$run_artifacts/campaign.txt"

retain_corpus() {
  cp -R "$campaign_corpus" "$run_artifacts/corpus"
  rm -rf "$campaign_corpus"
}
trap retain_corpus EXIT

cp "$fuzz_root"/seed-corpus/causal_turns/* "$campaign_corpus"/
cd "$fuzz_root"
cargo fuzz run --fuzz-dir "$fuzz_root" --sanitizer address \
  causal_turns "$campaign_corpus" -- \
  -runs=2048 \
  -seed=20260928 \
  -max_len=4096 \
  "-artifact_prefix=$run_artifacts/" \
  2>&1 | tee "$run_artifacts/result.log"
