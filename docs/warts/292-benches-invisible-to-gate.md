# Wart (card #292): benches never compile in the gate — a broken bench ships green

Surfaced by the #292 rename sweep: `benches/registry_vs_kameo.rs` had not
compiled since #294 merged (its lookups were re-spelled to
`.lookup::<ActorRef<..>>` without adding the top-level `ActorRef` import),
yet every gate stayed green. `nix flake check` builds the lib, the tests
(nextest), the doctests, clippy, fmt, audit — but nothing in the gate runs
`cargo check --benches` (or `--all-targets`), so a bench-only compile error
is invisible until a human runs the benches by hand. This is the same
blindness class as the fuzz workspace being invisible to `cargo check`
(#257) and the untracked-file false green (flake sources from the git
tree): a lane that is not in the gate does not exist.

The #292 PR fixes the import as a drive-by; the wart is the missing lane.
Candidate fix: add a `bombay-bench-check` flake check (crane
`cargoBuild`/`cargoClippy` over `--benches` only — compile, don't run), or
fold `--all-targets` into the existing clippy check if its scope decision
(lib-only) is revisited. Bench compile is cheap next to the mutants lane;
the point is compile visibility, not measurement.

Fix card: #300.
