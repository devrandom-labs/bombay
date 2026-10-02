#!/usr/bin/env bash
set -euo pipefail

if [[ -z "${IN_NIX_SHELL:-}" && -z "${NIX_BUILD_TOP:-}" ]]; then
  printf '%s\n' 'Observe Miri campaign must run inside the pinned Nix shell' >&2
  exit 2
fi

repository_root=$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)
result_dir="$repository_root/target/miri-evidence"
mkdir -p "$result_dir"
cd "$repository_root"

{
  rustc --version --verbose
  cargo miri --version
  printf '%s\n' 'MIRIFLAGS=-Zmiri-many-seeds=0..4'
  printf '%s\n' 'owner=bombay-rs --lib observe::external_tests::affine::'
} > "$result_dir/environment.txt"

cargo miri setup 2>&1 | tee "$result_dir/setup.log"
MIRIFLAGS='-Zmiri-many-seeds=0..4' \
  cargo miri test --locked -p bombay-rs --lib observe::external_tests::affine:: \
  2>&1 | tee "$result_dir/result.log"
