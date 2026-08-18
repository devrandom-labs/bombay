# Repository guidelines

## Scope and sources of truth

This is a Rust 2024 workspace for the Bombay Behavior runtime. Production code
lives in `crates/bombay/src/`; the actor-independent Driver lives in
`crates/bombay-engine/src/`; root `examples/` contains public-API examples.
Keep internal mechanism tests in their owning crate.

Read these documents before changing architecture:

- `docs/open-design-ledger.md`: live backlog, blockers, and current feature
  verification;
- `docs/runtime-capability-interfaces.md`: capability ownership and the target
  Environment spine;
- `docs/module-boundaries.md`: current source ownership;
- `docs/driver-law.md`: normative Driver semantics;
- `docs/driver-test-strategy.md`: Driver verification requirements.

`docs/historical-design-decisions.md` is historical context, not current API
guidance. Existing Bombay code and earlier conversation are not architectural
authority.

## Architecture

There is one executable composition:

```text
Behavior + Driver
  inside Environment / ActiveEnvironment
    + Communication mailbox
    + Address lease and AddressSpace
    + Observe activation/termination facts
    + actor-owned TimerQueue
    + typed effect-lane interpreters
    + Bombay-owned task hierarchy
```

Ownership is strict:

- Behavior and Behavior Actors own deterministic algebra, actor templates,
  topology policy, supervision policy, and named capability requests.
- Communication owns the two-lane mailbox, delivery, backpressure, closure,
  and exact rejected-payload recovery.
- Address owns endpoint claim, opaque resolution, and exact lease retirement.
- Observe owns completion publication and waiting.
- Timers owns generation-safe scheduling state.
- Engine owns only the universal causal Driver and affine Environment port.
- Bombay owns concrete local composition, incarnation tasks, capability
  interpretation, activation order, and retirement order.

Behaviors contain no I/O, channels, runtime handles, address spaces, clocks, or
executor tasks. Do not add a second actor trait, effect algebra, mailbox,
registry, lifecycle framework, supervision policy, timer service, observation
cell, runtime object, or dynamic capability map. Prefer static composition of
the owning primitives.

The standard local runtime includes Address, Communication, Observe, and
Timers. Ordinary users do not choose those adapters. Pluggability belongs at
typed effect-lane interpreters; whole-Environment replacement is an advanced
extension and test-host boundary.

## Mandatory feature verification

Before designing, implementing, unblocking, or auditing every Bombay feature:

1. Inspect the exact dependency version selected by `Cargo.lock` and any
   `[patch]` entry.
2. Inspect current source, public API/algebra, relevant tests, and relevant
   documentation for Behavior, Behavior Actors, Address, Communication,
   Observe, and Timers. Inspect Entity or other neighbors when touched.
3. Record the feature-specific versions, ownership map, blockers, and
   dependency edges in `docs/open-design-ledger.md` before implementation.
4. Keep implementation blocked while any dependency contract, protocol
   consumer, version relationship, or ownership boundary is unverified.

A previous audit does not waive this requirement. A local checkout at a
different revision is evidence only, never the selected build contract.

## Behavior effect composition

Derive effect handling from the exact locked Behavior and Behavior Actors APIs
and tests.

- Use named semantic send structs and typed `SendAlgebra::send`.
- Use Behavior's existing `InstallBirth`/`DispatchBirth` and closed child
  products for creation.
- Do not add positional effect traversal or mutate nested lanes directly.
- Do not handwrite application `SendAlgebra`, `SendInput`, `RouteSends`,
  `ObservesCreations`, or product-routing errors when generic interpretation
  covers the leaves.
- Keep only application delivery routers that select genuine external
  endpoints.
- Use explicit nominal `Behavior` implementations until an owning Behavior
  API supplies and proves another authoring form. Bombay owns no actor macro or
  reduced `Effect` language.

Audit the entire repository for obsolete composition patterns before claiming
completion.

## Ledger states

Select work from the reciprocal dependency graph in
`docs/open-design-ledger.md`. Every unresolved ID in `Blocked by` must name the
dependent item in `Unblocks`. Repair inconsistent edges before selecting work.

- `blocked`: an unresolved prerequisite exists;
- `active`: feature-local verification is recorded and implementation is
  eligible;
- `feature-complete`: feature gates pass but final minimization is pending;
- `distilled`: project-wide audit proved the remaining types, objects, public
  interfaces, and ownership boundaries minimal.

Never use `done`. Building or passing focused tests is not distillation.

## Documentation

When a contract changes, audit every tracked document, example, benchmark,
test, research probe, diagnostic fixture, and public re-export. Update current
guidance in the same change. Move useful superseded decisions to the historical
record or delete them; never leave contradictory documents appearing current.

## Development

- `nix develop` enters the pinned shell.
- `cargo build --workspace` builds the workspace.
- `cargo test --workspace` runs unit, integration, and documentation tests.
- `cargo fmt --all -- --check` checks formatting.
- `cargo clippy --workspace --all-targets -- -D warnings` runs strict Clippy.

Use Rust 2024 idioms and rustfmt defaults. Prefer explicit behavioral names.
Avoid `unsafe` unless a measured need and safety proof are documented. Add an
observable invariant and an inversion test for every new law. Preserve exact
payloads in typed errors and use flat semantic `thiserror` variants instead of
nested positional product errors.

Use short imperative commit subjects with a scope-like prefix. Keep commits
focused and report the exact verification commands and results.
