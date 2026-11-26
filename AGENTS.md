# Working on Bombay

This file holds every standing rule of this repository. [Patstack](.agents/skills/patstack/SKILL.md) owns the step procedure, and the [project bindings](.agents/project.md) bind its tools, lanes, models, and gates here. [Bombay code rules](.agents/skills/rust-best-practices/references/bombay.md) and the resolved `bombay-behavior` `AGENTS.md` own Rust interface and concurrency discipline.

## Workflow

Read Patstack and `.agents/project.md` before editing or reviewing. Load specialist skills only when `.agents/project.md` routes your changed paths to them.

### Roles

- Joel decides product, scope, stack, authority, and any change to an existing test oracle (`needs-joel`). Decide engineering mechanisms inside the accepted stack and [ADR 0001](docs/adr/0001-layer-bombay-crates.md) yourself and record the reason in the PR.
- Until 2026-12-05, Codex (`codex exec`) writes production code and tests and runs R2 step reviews in a fresh read-only session that wrote none of the change. Antigravity (`agy`) runs the `harness-owner` daemon (`architecture/owner/session.sh`), Claude Opus `## Design` / crate / layer reviews and R3 councils, Gemini Carbon weekly layer reviews, and fast Hydrogen signal/reuse lookups (`.agents/project.md#models`). After 2026-12-05, `.agents/project.md` switches implementation to Google AI Ultra (`agy`).
- The author of a change never reviews it. Every card is assigned to `joeldsouzax`.

### Lanes

Each lane owns disjoint paths in its own worktree (`bombay-worktrees/<lane>`) and Cargo target directory:

| Lane | Board Workstreams | Owns |
| --- | --- | --- |
| `runtime` | `Runtime`, `Shared` | `crates/bombay-runtime/` (`crates/bombay/src/{actors,application,entity,local}`), `crates/{bombay-engine,bombay-macros,observe-perf}/`, local `examples/*`, `tests/renamed-downstream/` |
| `domain` | `Domain` | `crates/bombay-observe/`, `crates/bombay-domain-{auth,wire,placement,durable}/` |
| `adapters` | `Adapters` | `crates/bombay-zenoh/`, `crates/bombay-axum/`, `crates/bombay/src/remote/`, `crates/bombay/src/application/{node,http}*`, `examples/remote-counter/` |
| `nix` | `DevOps` | `flake.nix`, `flake.lock`, `nix/`, `tools/`, `justfile`, `.githooks/`, `.github/`, `.cargo/`, `.config/`, `crates/mutants-gate/`, `ops/` |

- Edit only your lane's files. Cross-lane findings become cards for the owning lane.
- Shared files (`Cargo.toml`, `Cargo.lock`, `deny.toml`, `crates/bombay/Cargo.toml`, `crates/bombay/src/lib.rs`, `AGENTS.md`, `README.md`, `docs/user-facing-api.md`, `docs/adr/`, `architecture/`, `.agents/`) change in dedicated single-PR steps that merge before dependent lane work.

### Steps, Reviews & Merge

- [Bombay Project #6](https://github.com/orgs/devrandom-labs/projects/6) is the execution backlog (`bombay` finishes 100% standalone first, followed sequentially by `mnesis-bombay` on Project #10 and `selo` on Project #7).
- **One step is one PR and one fresh session (<200,000 context tokens).** The GitHub card body, PR body, and code hold all state. **Never append chronological logs or execution ledgers to `docs/prds/` or `docs/prd-backlog/status.md`.**
- Reuse before code: any new module or `> 150` added `src` lines requires `Reuse: <crate/primitive used, or concrete gap>`.
- `architecture/step.sh` rejects touched production items that pack tuple parameters (`tuple-parameter-packing`), take 3+ closure parameters (`callback-soup`), declare 3+ single-letter type parameters (`single-letter-generics`), or return unnamed `Result` tuples from `pub fn` (`pub-result-tuple`); rejects rising `unreachable!()` counts in changed test files (`test-unreachable`); and blocks multi-lane diffs without `Lanes:`, rule+code diffs without `Rules:`, assertion removals/edits without `Oracle:`, and increases in book-grounded AST fitness counters (`architecture/design/fitness.sh`) without `Design:`.
- Any card introducing a domain concept, extracting or restructuring a crate or module, or containing more than one `logic` step requires one holistic upfront `## Design` section (complete Rust struct/enum/Port signatures and `Cargo.toml` dependencies — never interim callback-soup or generic-erasure wrappers) reviewed by read-only Codex (`gpt-6.2-astra`) and `agy` Claude Opus (`claude-opus-5-5-high`) before code begins. Lead and R2/R3 reviewers must read all matched paths in `.agents/project.md#route-by-the-work` and record them under `Skills:` in `.cache/pr.md`.
- Every step receives an independent fresh-session R2 review; auth, wire, placement fencing, durability, or public API changes receive an R3 Council (`.agents/project.md#models`).
- A step merges under the serialized merge turn (`~/.cache/bombay/turn.lock`) once its reviewed head passes `just architecture` and `just gate` (`nix flake check`). `.github/workflows/checks.yml` qualifies `main` on push and nightly schedule. Never bypass rules, force-push `main`, or weaken an oracle.

## Architecture & Behavior Laws (Non-Negotiable)

Follow the 5-layer standalone crate pyramid in [ADR 0001](docs/adr/0001-layer-bombay-crates.md) and [architecture.md](docs/arch/architecture.md):

```text
Layer 0: bombay-behavior (pure Behavior -> Actions algebra)
Layer 1: bombay-behavior-actors | bombay-engine | bombay-address | bombay-communication | bombay-timers | bombay-observe
Layer 2a: bombay-domain-{auth,wire,placement,durable} (100% sans-IO pure state machines, clippy.toml + domain-is-pure.yml)
Layer 2b: bombay-runtime (Local Environment, Entity Directory, Narrow Port Traits, Deterministic SIM Host)
Layer 3: bombay-zenoh | bombay-axum (single-technology adapters enforced by cargo-deny)
Layer 4: bombay-rs facade | examples/* | ops/k8s
```

1. **Pure Fold & `sans-IO` Boundary:** Inside a `Behavior` transition or any Layer 2a domain crate, compute state/values and return typed `Actions` or `#[must_use]` decisions; never perform I/O, touch clocks/channels/locks, or spawn tasks.
2. **Authoritative-Fact Conservation:** Once Bombay obtains an authoritative typed outcome from Address, Communication, Observe, Timers, Driver, or a Port, no layer may erase or misclassify it (`map_err(|_| ...)` is banned by `keep-error-sources.yml`). Independent observers coexist; disconnect/timeout uncertainty is never reported as proven non-execution.
3. **Macros Last:** Only the distilled `#[bombay::actor]` and `topology!` projections exist. Prototype plain functions, traits, and types first.
4. **Strict Naming & Module-Scope Imports:** Keep all `use` statements at module scope (`no-inline-use.yml`). Never encode semantic state, policy, or authority in a `bool`. Vague/mechanic names (`Manager`, `Handler`, `Helper`, `Utils`, `Thing`, `Data`, `Process`, `Fold`, `Fact`, `Boundary`, `Installation`, `Incarnation`) are forbidden everywhere, including private and test code (`bombay-banned-names.yml`).
5. **Pinned Nix Tooling & Defect Inversions:** Run every Cargo, Rust, format, Clippy, and gate command inside `nix develop` (`just gate`, `just architecture`). Never place mutations inside `assert!` / `debug_assert!`. Every new guard or regression test must fail when the guarded defect is simulated.
