# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What this is

**Bombay** is a Zenoh-native fork of the [kameo](https://github.com/tqwewe/kameo) actor framework: identity-bearing, dataspace-native actors. It replaces kameo's libp2p `remote` layer with a thin layer over a Zenoh `Session`, and keeps a **generic transport- and domain-agnostic core** (actor = single-writer consistency boundary).

Actors here are a *general* single-writer abstraction — ephemeral, stateful, or nexus-backed. Event-sourcing is one backing, not the definition; the adapter that maps **nexus** aggregates onto actors lives in a sibling repo, not in this crate.

> **Current state: M1, core rebuild in flight.** M0 is closed (#62/#64 de-risking done; see the reference docs). The fork (#1) and the fork-strategy decision (#63) are closed. Production Rust lives in `bombay-core/src/` — `mailbox` (flume-backed), `actor` (loop/`ActorRef`/`Recipient`/spawn), `reply`, `error`.
>
> The core is being **rebuilt from scratch part-by-part with kameo as a reference oracle** — epic #122, cards #112–#121 — not carried over wholesale. When this file and the cards disagree, the cards win: verify against `gh` before relying on anything here.

## Working method: cards-driven + TDD

This project is **GitHub-project-cards-driven with test-driven development**. Do not freelance work.

> **Before a card: read the reference docs.** Distilled, durable knowledge lives in [`docs/`](docs/) — consult the relevant doc before implementing. In particular, **any card touching Zenoh → read [`docs/zenoh/capabilities.md`](docs/zenoh/capabilities.md)** (capability matrix core/ext/pico + stability gates + per-card "gap → nexus/bombay coverage" flags) so you never build on a feature that is missing, `unstable`-gated, or actually nexus's job. Remember the co-design: Zenoh = transport/addressing/discovery, nexus = consistency/persistence/ordering — a Zenoh gap is often covered by nexus, and we patch whichever layer gives the best result.

1. **Start from a card.** All work is scoped by GitHub issues ("cards") on the **Bombay** project board (project #4, owner `devrandom-labs`), organized into milestones M0→M7. Pick the next unblocked card; reference its number in branches, commits, and PRs.
2. **Test first.** Every code change is test-driven — write the failing test, watch it fail, then implement to green. Use the `superpowers:test-driven-development` skill.
3. **One invariant per checkbox — a card's scope is a checklist, never prose.** When writing *or* closing a card:
   - **One invariant per bullet.** Never bundle several into a prose list. A card that says "assert no panic, FIFO + exactly-once, self-pin drain-or-abandon per stop mode" reads as *one checked box* when only the first ships; as three bullets it visibly reads 1/3 done, and nobody closes that.
   - **Split wiring from invariants.** A bullet about a workspace, a flake check, or a CI lane is *wiring* — objectively done when it runs. A bullet about a property being asserted is an *invariant*. Bundled in one card, the wiring completes, the card feels done, and the invariants get silently trimmed. Prefer separate cards, or at minimum separate bullets.
   - **Close `COMPLETED` only when every bullet is either shipped (name the file/test/check) or explicitly deferred to a named follow-up card.** Silence is not a deferral. Record deferrals in the PR body.
   - *Why this rule exists:* #149 shipped every wiring bullet (isolated `fuzz/` workspace, in-gate replay check, nightly quarantine, capacity boundaries) and dropped its one semantic bullet (`recv`/`send_message`/self-pin drain-or-abandon per stop mode), then closed `COMPLETED`. The result was a fuzz lane pointed at flume rather than bombay's invariants — which #152 then reported green over 3.5M executions. Green lanes over the wrong surface look exactly like green lanes over the right one. See #164/#165/#166.
4. **Keep `README.md` a user-facing public-API document — maintain it per *card*, not per *commit*.** The README describes the public API and how to use/test it; card numbers, per-module test narratives, and internal progress never belong in it (those live in commits, PRs, and `docs/`). When you finish a card, *before the PR*, classify what it changed and update the README accordingly:
   - **Public API changed** (new/renamed/removed public item, new feature flag, changed default behavior, new example) → update the relevant *"public API at a glance"* bullet and the usage example if user-visible. **This is the main case.**
   - **No API change, tests/coverage moved** → update [`docs/testing/coverage-baseline.md`](docs/testing/coverage-baseline.md); the README only *links* to it and carries no coverage number (nothing to go stale).
   - **No API change, no coverage change** (refactor / perf / robustness / bugfix) → if it makes the library meaningfully better, refresh **one** salient-feature line (what it now does *better*); otherwise no README change is needed.
   - **Every ~10 cards, or when a section bloats / the README passes ~120 lines** → **consolidate**: re-tighten to the public-API essentials, fold or drop accumulated notes, remove anything stale.

   Enable the tracked hooks once per clone (mirrors nexus): `git config core.hooksPath .githooks`. The `pre-commit` hook lints any staged GitHub Actions workflow; it no longer forces a per-commit README change (that mechanical rule is what bloated the README).
5. **`nix flake check` is the single gate** (build + clippy + fmt + tests). It sources from the **git tree** — an untracked file is invisible to it, so a check over a new file passes vacuously until you `git add`. A silent `nix build` means *cached*; a derivation that truly ran logs `building '...drv'`.
6. **No Claude/Anthropic attribution** in commit messages or PR bodies (no `Co-Authored-By` trailer, no "Generated with" line).

### Checking cards — use `gh`, never Linear

`gh` is on `PATH` globally (via the user's nix-config) — call it directly, never `nix run nixpkgs#gh` (that violates the flake-reproducibility rule and pulls an unpinned nixpkgs). Use the **`joeldsouzax`** account (the user's `trivejoel` account is for other repos) — `gh auth status` should show `joeldsouzax` active; if not, `gh auth switch --user joeldsouzax`.

```bash
gh issue list --repo devrandom-labs/bombay --state open
gh issue view <N> --repo devrandom-labs/bombay
gh project item-list 4 --owner devrandom-labs
```

The Status field on the board is **Todo / In Progress / Done**. Issues carry milestone + topic labels (`foundation`, `design`, `state`, `qos`, `discovery`, `transport`, `security`, `runtime`, `cleanup`, `epic`, …).

## Milestones (the roadmap)

These are the milestones that **exist on this board**. Verify with `gh api repos/devrandom-labs/bombay/milestones` — this table goes stale, the API does not.

| | Milestone | State | Gist |
|---|---|---|---|
| **M0** | Pre-flight | **closed** | De-risked before the fork: throwaway Zenoh spike (#62), fork strategy (#63), Zenoh feature matrix (#64). |
| **M1** | Foundation: Zenoh remote layer | **active** | The core rebuild (epic #122) + the thin Zenoh `Session` layer replacing libp2p. Exit gate: remote actor ask/tell + death-watch across 2 nodes (#67). |
| **M3** | Novel Zenoh-native features | open | What Zenoh gives that kameo lacks: wildcard/hierarchical key-expr group addressing, queryable + persistent actor state via storages, liveliness supervision. |
| **M4** | KERI identity | 1 card | Identity-first `ActorId` — AID/SAID/delegation, key-expr addressable (#121). |
| **M7** | De-handroll | **closed** | Replace hand-rolled code with best-in-class crates (only the *local* core surviving the Zenoh migration). |

**The other layers live in private sibling repos, not here** — the nexus aggregate-runner (`bombay-nexus`), KERI (`bombay-keri`), the mobile client SDK (`bombay-sdk`), and the downstream `agency` product. Former milestones M2 (nexus runtime adapter) and M6 (lite-bombay) no longer exist on this board. This is why the bombay core stays **transport- and domain-agnostic**: those layers sit on top of it. Do not decompose the bombay crate to accommodate them.

## Architecture (intended)

The big picture spans several crates/layers; understand these before touching code:

- **Generic core** — `Actor`, mailbox, supervision. Kept **transport- and domain-agnostic** so the sibling layers (nexus runner, KERI, mobile SDK) sit on top. The kameo `remote` (libp2p) feature is replaced by a `zenoh` feature; `macros`/`tracing`/`console`/`otel`/`metrics` features are repurposed. Crate/feature layout is design card **#66** and gates the rewrite shape.
- **Zenoh remote layer** — actors are addressable in the dataspace by **key-expr** (the one invasive change escaping `src/remote/` is `ActorId`: libp2p `PeerId` → Zenoh key-expr, #2). `ask` = Session get / query-reply; `tell` = put; **death-watch** = one liveliness token per actor (subscriber gets a `Delete` on drop). DHT registry → key-expr discovery (#3).
- **Upper layers are NOT in this repo** — the nexus aggregate-runner (`bombay-nexus`), KERI (`bombay-keri`), the mobile client SDK (`bombay-sdk`), and the `agency` product live in private sibling repos. They consume the core; they do not shape it. If a card here seems to need one of them, it is the wrong card. The nexus boundary in particular is dense and easy to get subtly wrong — **read the nexus integration contract (private `bombay-nexus#4`) before touching anything in that repo** (single-writer-per-aggregate; conflicts surfaced, never retried internally; `GlobalSeq` monotonic but not gapless; `Version` 1-based `NonZeroU64`; the cursor never returns `None`; the kernel is `no_std`/`no_alloc`/WASM-capable).

## Build & tooling (per the foundation cards)

Conventions follow the sibling **nexus**/**agency** repos, with one deliberate deviation: **STABLE Rust only** (so non-Nix users can build with plain `rustup`).

- **Toolchain:** pinned `rust-toolchain.toml` (exact stable channel, edition 2024 ⇒ ≥ 1.85), fed to fenix via `fromToolchainFile` so Nix and rustup resolve the *same* toolchain (#60).
- **Nix:** crane + fenix + flake-utils + advisory-db; `use flake` direnv. `nix flake check` is the single CI gate.
- **Clippy:** adopt nexus's "god-level" lint config verbatim (#61) — `clippy.toml` (cognitive-complexity 9, max 5 args, max 80 lines/fn, banned methods like `std::process::exit` / `std::thread::spawn`) plus a ruthless `[workspace.lints.clippy]` block; every member crate opts in with `[lints] workspace = true`.

Once code exists, expect the usual `cargo test` / `cargo test <name>` / `cargo clippy` underneath, but treat `nix flake check` as the authoritative gate.

## Engineering rules (distilled from the nexus project)

Bombay is the runtime/adapter for nexus and holds the **same hygiene bar** (the god-level clippy config in #61 is nexus's, verbatim). The shared devrandom engineering rules — **Facts Only, Arithmetic Safety, Error Handling, API Design, Concurrency, Functional-First/Allocate-Last style, Test Quality, Clippy policy, shared conventions** — are EXTREMELY IMPORTANT and apply here in full; canonical text lives in the user-global `~/.claude/CLAUDE.md` ("Engineering rules"). In nexus each one "exists because of a real bug found in this codebase." Only the bombay-specific rules are spelled out below.

**1. Atomicity.** Any operation doing 2+ store calls (multi-key reads, read-then-write) must share one transaction/snapshot — never two independent reads. Derived state (projections, snapshots) is best-effort and re-derivable; it must never block event persistence. (Mirrors nexus contract #5/#13 in `bombay-nexus#4`.)

Bombay-specific addenda to the shared rules: no `#[non_exhaustive]` on enums during this phase (exhaustive matching catches real bugs); capacity hit = `Result`, not panic (nexus contract #22); the allocate-last discipline (`ArrayVec`/`SmallVec` for bounded collections) is also what keeps the kernel `no_std`/`no_alloc`.

**7. Testing — the 4 cross-cutting categories come first** (before any other methodology), and pair with TDD (write them failing first):
1. **Sequence/Protocol** — multi-step interactions on the same object, not ops in isolation.
2. **Lifecycle** — create/close/corrupt/reopen; for anything persistent: write-close-reopen-verify, write-corrupt-reopen-detect, write-crash-reopen-recover.
3. **Defensive boundary** — feed each crate inputs that violate its upstream crate's guarantees.
4. **Linearizability/isolation** — concurrent readers+writers with snapshot-consistency assertions.

**Shared conventions** are in the global CLAUDE.md (edition 2024, conventional commits with scope, dual license, root workspace deps, `nix flake check` as the one gate). Bombay-specific: adopt **`cargo-hakari`** (workspace-hack crate) as nexus does — run `cargo hakari generate` after any dependency change.

## Fork etiquette

kameo is MIT/Apache-2.0 — upstream LICENSE + attribution are carried in `LICENSE-MIT`, `LICENSE-APACHE`, and `NOTICE`. Keep them intact.

**Decided and executed** (#63, #1 — both closed): **hard-fork from `v0.21.0`**, diverging freely (the Zenoh rewrite + M7 de-handroll make rebaseability not worth preserving). The decision record is [`docs/superpowers/specs/2026-06-23-fork-strategy-design.md`](docs/superpowers/specs/2026-06-23-fork-strategy-design.md) — read it rather than re-deriving the choice.

Two trees coexist deliberately: **`src/`** is the vendored kameo fork (the reference oracle, M7-doomed), **`bombay-core/`** is the from-scratch rebuild (epic #122). New core work goes in `bombay-core/`; `src/` is consulted, not extended.
