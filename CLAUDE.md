# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What this is

**Bombay** is a Zenoh-native fork of the [kameo](https://github.com/tqwewe/kameo) actor framework: event-sourced, identity-bearing, dataspace-native actors. It replaces kameo's libp2p `remote` layer with a thin layer over a Zenoh `Session`, keeps a generic transport/domain-agnostic core, and adds an adapter that maps **nexus** event-sourced aggregates onto actors (actor = single-writer consistency boundary).

> **Current state: planning / M0.** No production Rust has landed yet (the #62 spike is throwaway on its own branch). Everything below is the intended architecture, captured from the GitHub issues. The fork of kameo (#1) has not happened; it is gated by M0 de-risking and the design cards. M0 de-risking is largely done — see the reference docs and issues #62/#64.

## Working method: cards-driven + TDD

This project is **GitHub-project-cards-driven with test-driven development**. Do not freelance work.

> **Before a card: read the reference docs.** Distilled, durable knowledge lives in [`docs/`](docs/) — consult the relevant doc before implementing. In particular, **any card touching Zenoh → read [`docs/zenoh/capabilities.md`](docs/zenoh/capabilities.md)** (capability matrix core/ext/pico + stability gates + per-card "gap → nexus/bombay coverage" flags) so you never build on a feature that is missing, `unstable`-gated, or actually nexus's job. Remember the co-design: Zenoh = transport/addressing/discovery, nexus = consistency/persistence/ordering — a Zenoh gap is often covered by nexus, and we patch whichever layer gives the best result.

1. **Start from a card.** All work is scoped by GitHub issues ("cards") on the **Bombay** project board (project #4, owner `devrandom-labs`), organized into milestones M0→M7. Pick the next unblocked card; reference its number in branches, commits, and PRs.
2. **Test first.** Every code change is test-driven — write the failing test, watch it fail, then implement to green. Use the `superpowers:test-driven-development` skill.
3. **Keep `README.md` a user-facing public-API document — maintain it per *card*, not per *commit*.** The README describes the public API and how to use/test it; card numbers, per-module test narratives, and internal progress never belong in it (those live in commits, PRs, and `docs/`). When you finish a card, *before the PR*, classify what it changed and update the README accordingly:
   - **Public API changed** (new/renamed/removed public item, new feature flag, changed default behavior, new example) → update the relevant *"public API at a glance"* bullet and the usage example if user-visible. **This is the main case.**
   - **No API change, tests/coverage moved** → update [`docs/testing/coverage-baseline.md`](docs/testing/coverage-baseline.md); the README only *links* to it and carries no coverage number (nothing to go stale).
   - **No API change, no coverage change** (refactor / perf / robustness / bugfix) → if it makes the library meaningfully better, refresh **one** salient-feature line (what it now does *better*); otherwise no README change is needed.
   - **Every ~10 cards, or when a section bloats / the README passes ~120 lines** → **consolidate**: re-tighten to the public-API essentials, fold or drop accumulated notes, remove anything stale.

   Enable the tracked hooks once per clone (mirrors nexus): `git config core.hooksPath .githooks`. The `pre-commit` hook lints any staged GitHub Actions workflow; it no longer forces a per-commit README change (that mechanical rule is what bloated the README).
4. **`nix flake check` is the single gate** (build + clippy + fmt + tests).
5. **No Claude/Anthropic attribution** in commit messages or PR bodies (no `Co-Authored-By` trailer, no "Generated with" line).

### Checking cards — use `gh`, never Linear

`gh` is not installed globally; run it via a temporary nix shell. Use the **`joeldsouzax`** account (the user's `trivejoel` account is for other repos) — `gh auth status` should show `joeldsouzax` active; if not, `gh auth switch --user joeldsouzax`.

```bash
nix run nixpkgs#gh -- issue list --repo devrandom-labs/bombay --state open
nix run nixpkgs#gh -- issue view <N> --repo devrandom-labs/bombay
nix run nixpkgs#gh -- project item-list 4 --owner devrandom-labs
```

The Status field on the board is **Todo / In Progress / Done**. Issues carry milestone + topic labels (`foundation`, `design`, `state`, `qos`, `discovery`, `transport`, `security`, `runtime`, `cleanup`, `epic`, …).

## Milestones (the roadmap)

| | Milestone | Gist |
|---|---|---|
| **M0** | Pre-flight | De-risk *before* forking 19k LOC: throwaway Zenoh spike (#62), fork strategy (#63), Zenoh feature matrix (#64). |
| **M1** | Foundation: Zenoh remote layer | Replace libp2p `src/remote/` with a thin Zenoh `Session` layer; ~4,200 LOC of plumbing → ~800. Exit gate: remote actor ask/tell + death-watch across 2 nodes (#67). |
| **M2** | nexus runtime adapter | Separate crate mapping nexus aggregates → actors. Done = bank-account aggregate runs as a Zenoh actor across two nodes (#10). |
| **M3** | Novel Zenoh-native features | What Zenoh gives that kameo lacks: wildcard/hierarchical key-expr group addressing, queryable + persistent actor state via storages, liveliness supervision. |
| **M4** | KERI identity on the edge | Model a KERI Key Event Log as a nexus aggregate; actor identity = its AID. |
| **M6** | lite-bombay | React Native actor-client SDK on zenoh-pico — client mode only, hosts no actors/storages. |
| **M7** | De-handroll | Replace kameo hand-rolled code with best-in-class crates (only the *local* core that survives the Zenoh migration). |

(M5 "agency" is the downstream product, scoped later.)

## Architecture (intended)

The big picture spans several crates/layers; understand these before touching code:

- **Generic core** — `Actor`, mailbox, supervision. Kept **transport- and domain-agnostic** so the nexus adapter and lite-bombay sit on top. The kameo `remote` (libp2p) feature is replaced by a `zenoh` feature; `macros`/`tracing`/`console`/`otel`/`metrics` features are repurposed. Crate/feature layout is design card **#66** and gates the rewrite shape.
- **Zenoh remote layer** — actors are addressable in the dataspace by **key-expr** (the one invasive change escaping `src/remote/` is `ActorId`: libp2p `PeerId` → Zenoh key-expr, #2). `ask` = Session get / query-reply; `tell` = put; **death-watch** = one liveliness token per actor (subscriber gets a `Delete` on drop). DHT registry → key-expr discovery (#3).
- **nexus adapter** — nexus ships *primitives, never a runner*. The loop, dispatch, cursor, lifecycle, supervision, command bus, projection/saga runners are **Bombay's** to build. This boundary is dense and easy to get subtly wrong — **read issue #59 ("nexus integration contract") before implementing anything in the adapter.** Key invariants: single-writer-per-aggregate (optimistic-concurrency conflicts are surfaced, never retried internally); `GlobalSeq` monotonic but not gapless; `Version` is 1-based `NonZeroU64`; the subscription cursor never returns `None` (caught-up = wait, forever-driver); exactly-once is the runner's to assemble from at-least-once + atomic commit; the kernel is `no_std`/`no_alloc`/WASM-capable.
- **lite-bombay** — thin but self-sovereign mobile client; hosts its own KERI identity, discovers a nearby router in client mode.

## Build & tooling (per the foundation cards)

Conventions follow the sibling **nexus**/**agency** repos, with one deliberate deviation: **STABLE Rust only** (so non-Nix users can build with plain `rustup`).

- **Toolchain:** pinned `rust-toolchain.toml` (exact stable channel, edition 2024 ⇒ ≥ 1.85), fed to fenix via `fromToolchainFile` so Nix and rustup resolve the *same* toolchain (#60).
- **Nix:** crane + fenix + flake-utils + advisory-db; `use flake` direnv. `nix flake check` is the single CI gate.
- **Clippy:** adopt nexus's "god-level" lint config verbatim (#61) — `clippy.toml` (cognitive-complexity 9, max 5 args, max 80 lines/fn, banned methods like `std::process::exit` / `std::thread::spawn`) plus a ruthless `[workspace.lints.clippy]` block; every member crate opts in with `[lints] workspace = true`.

Once code exists, expect the usual `cargo test` / `cargo test <name>` / `cargo clippy` underneath, but treat `nix flake check` as the authoritative gate.

## Engineering rules (distilled from the nexus project)

Bombay is the runtime/adapter for nexus and holds the **same hygiene bar** (the god-level clippy config in #61 is nexus's, verbatim). These rules are distilled from `../nexus/CLAUDE.md`; in nexus each one "exists because of a real bug found in this codebase." They are non-negotiable here too.

**0. Facts only — no assumptions, no opinions.** If you don't know, say so and research it; don't fill gaps with plausible-sounding guesses about APIs, crate behavior, or performance. No "I think / cleaner / feels better." Technical claims about algorithms, concurrency, or crypto must cite a primary source (papers, the actual repo, RFCs/specs) — "common knowledge" is not a source. Uncertainty is a fact: state it rather than collapsing it into confidence.

**1. Atomicity.** Any operation doing 2+ store calls (multi-key reads, read-then-write) must share one transaction/snapshot — never two independent reads. Derived state (projections, snapshots) is best-effort and re-derivable; it must never block event persistence. (Mirrors nexus contract #5/#13 in issue #59.)

**2. Arithmetic safety.** No bare arithmetic in production — use `checked_add`/`checked_sub`, return `Err` on overflow. `saturating_add` is banned (silently stops progress). No `try_from(...).unwrap_or(MAX)`. `debug_assert` is NOT a safety check (compiled out in release) — use a runtime check for anything that would corrupt data.

**3. Error handling.** One variant = one failure domain; never reuse an unrelated variant. Never erase typed errors into `Box<dyn Error>` when callers match on them; never discard the original with `|_|` (wrap via `#[source]`/`#[from]`). Unknown values are `Option`, not sentinels (`version: 0`). Overflow/limit errors must NOT reuse a retry-eligible code like `Conflict`. All error types use `thiserror` — no manual `Display`/`Error` impls. No `#[non_exhaustive]` on enums (exhaustive matching catches real bugs). Each crate validates at its own boundary — don't trust the upstream crate's guarantees.

**4. API design.** No unused generics/associated types (YAGNI — add at the second concrete use). Internal wire helpers are `pub(crate)`, not `pub`; prefer `mod` + controlled `pub use` over `pub mod`. `#[doc(hidden)]` is not access control — test-only methods are `#[cfg(test)]`/`#[cfg(feature = "testing")]`. Panics are for programmer bugs, never capacity/data limits (return `Result`; capacity hit = `Result`, not panic — see nexus contract #22). `new_unchecked` means no validation; if it `assert!`s, it's `new`. Document trait semantics (is `from` inclusive/exclusive? is `INITIAL` a valid value or a sentinel?) on the trait.

**5. Concurrency.** `Relaxed` ordering requires a structural proof, not a comment about some library's behavior. Make the invariant self-contained with `Acquire`/`Release` or a mutex.

**6. Functional-first, allocate-last.** Prefer combinators (`map`/`and_then`/`filter`/`fold`) over imperative `if`/`match` for simple data flow. Lazy over eager — `.collect()` only when you need the concrete collection. Borrow before own (`&T`, `Cow<'a, T>`); justify every `clone`/`to_string`/`Box::new` in a hot path; prefer `ArrayVec`/`SmallVec` for bounded collections (this is also what keeps the kernel `no_std`/`no_alloc`). `let ... else` over `if let ... else { return }`. All `use` at the top of the file — no mid-file imports, no deep inline path qualification in match arms.

**7. Testing — the 4 cross-cutting categories come first** (before any other methodology), and pair with TDD (write them failing first):
1. **Sequence/Protocol** — multi-step interactions on the same object, not ops in isolation.
2. **Lifecycle** — create/close/corrupt/reopen; for anything persistent: write-close-reopen-verify, write-corrupt-reopen-detect, write-crash-reopen-recover.
3. **Defensive boundary** — feed each crate inputs that violate its upstream crate's guarantees.
4. **Linearizability/isolation** — concurrent readers+writers with snapshot-consistency assertions.

**8. Test quality (every test must satisfy all).** Calls the actual SUT — don't reimplement prod logic and test the reimplementation. Can actually fail — no `if empty {..} else {assert..}` that passes both ways. Asserts the *specific* correct value (`assert_eq!`, not `contains('3')`); `println!` is not an assertion. "Concurrent" tests use real overlap (`tokio::spawn` + `Barrier`), not sequential-then-check. Proptest ranges include boundaries (`0, 1, MAX-1, MAX`; empty/max/max+1 strings). Bug-probe tests FAIL when the bug exists (`#[ignore]`, never a green test documenting a known bug). Benchmarks measure production code, separating setup from measurement.

**Shared conventions:** Rust edition 2024; conventional commits with scope (`feat(zenoh):`, `fix(adapter):`); dual MIT OR Apache-2.0; workspace dependencies declared once in root `[workspace.dependencies]` with `workspace = true` in members; `nix flake check` is the one CI gate. Adopt **`cargo-hakari`** (workspace-hack crate) as nexus does — run `cargo hakari generate` after any dependency change.

## Fork etiquette

kameo is MIT/Apache-2.0 — carry upstream LICENSE + attribution into Bombay. The plan favors a **hard-fork** (diverge freely; the Zenoh rewrite + M7 de-handroll make rebaseability not worth preserving), forking from kameo 0.21. Confirm the recorded decision on #63 before executing #1.
