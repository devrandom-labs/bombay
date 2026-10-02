#!/usr/bin/env bash
set -euo pipefail

if [[ -z "${IN_NIX_SHELL:-}" && -z "${NIX_BUILD_TOP:-}" ]]; then
  printf '%s\n' 'Observe fuzz campaign must run inside the pinned Nix shell' >&2
  exit 2
fi

fuzz_root=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
campaign_corpus=$(mktemp -d)
mkdir -p "$fuzz_root/artifacts"
run_artifacts=$(mktemp -d "$fuzz_root/artifacts/run.XXXXXXXX")
{
  rustc --version --verbose
  cargo fuzz --version
  printf '%s\n' 'seed=20261001' 'runs=1024 per target' 'max_len=512' 'sanitizer=address'
} > "$run_artifacts/environment.txt"

retain_corpus() {
  cp -R "$campaign_corpus" "$run_artifacts/corpus"
  rm -rf "$campaign_corpus"
}
trap retain_corpus EXIT

cd "$fuzz_root"
for target in ops future_ops promotion_ops waker_ops; do
  mkdir -p "$campaign_corpus/$target" "$run_artifacts/$target"
  cp "$fuzz_root"/seed-corpus/"$target"/* "$campaign_corpus/$target/"
  printf 'target=%s\nseed=20261001\nruns=1024\nmax_len=512\n' "$target" \
    > "$run_artifacts/$target/campaign.txt"
  cargo fuzz run --fuzz-dir "$fuzz_root" --sanitizer address \
    "$target" "$campaign_corpus/$target" -- \
    -runs=1024 \
    -seed=20261001 \
    -max_len=512 \
    -print_final_stats=1 \
    "-artifact_prefix=$run_artifacts/$target/" \
    2>&1 | tee "$run_artifacts/$target/result.log"
done
