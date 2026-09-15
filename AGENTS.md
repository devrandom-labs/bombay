# Repository guidelines

## Behavior engineering discipline

Before any software-design or implementation work, resolve the exact Behavior
revision selected by `Cargo.lock` and read that revision's complete `AGENTS.md`.
Its engineering, modeling, naming, ownership, testability, compiler-friction,
and verification rules apply repository-wide here as well. Apply Bombay's
pinned-Nix command requirement when executing its verification commands.

Keep imports at module scope. Never add an inline `use` inside a function,
method, block, test, macro arm, or generated body. Do not encode semantic state,
phase, policy, authority, provenance, or transition choice in a `bool`; use a
closed sum type. A predicate may return `bool` only when it preserves all domain
information and answers an ordinary local yes/no question.

Use idiomatic Rust ownership, algebraic data types, exhaustive matching, and
standard library data structures to express the domain directly. Compiler
output may veto a model but may not invent architecture or generic plumbing.
Check public naming and interface design against the official Rust API
Guidelines and the selected Behavior instructions before retaining a change.

Every variable, field, function, type, object, module, and filename must use
Bombay's ubiquitous domain language and reveal what it owns. Vague or
implementation-mechanic names are forbidden, including `manager`, `handler`,
`helper`, `utils`, `thing`, `data`, `process`, numbered experiments, and the
additional prohibited architectural vocabulary in Behavior's `AGENTS.md`.
Private and test code receive no naming exemption.

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

- Foundational Behavior owns the deterministic `Behavior -> Actions` algebra,
  `#[behavior]` generation, closed typed products, and named capability
  requests. A Bombay `actor` authoring facade, if the macro-last evidence
  selects one, may only derive syntax into that exact owning expansion; it
  never owns a second actor contract.
- The `bombay-behavior-actors` library API owns reusable actor templates and
  their topology, supervision, shutdown, timing, and terminal-disposition
  policies. “Behavior Actors” names this owning library layer, not actors that
  construct or configure other actors at runtime.
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

The Behavior fold boundary is absolute:

> Inside a Behavior fold, compute state and return `Actions`; never perform an
> effect directly. Every communication, creation, lifecycle request, timer
> request, or observation must be represented in a typed `Actions` lane and
> interpreted only by the runtime.

An example or test that calls a channel, sender, callback, task, clock,
observation publisher, or runtime service from `init`, `receive`, or
`transition` is invalid semantic evidence even if its assertions pass.

Macros are the last resort. For every authoring or composition seam, first
prototype and compare plain functions, inherent methods, type inference,
associated types, extension traits, named products, builders, typestate, and
the other applicable stable Rust mechanisms. Record the alternatives, their
measured application syntax, compiler diagnostics, static denials, and the
specific remaining gap in the feature's research and ledger entry. A macro is
eligible only when that evidence proves ordinary Rust cannot provide an
equally direct, statically checked, diagnosable, and maintainable experience.
DX42 accepts exactly one distilled Bombay actor facade: `#[bombay::actor]`
turns ordinary state into the exact Behavior-owned actor type and materially
improves subsequent `ActorExt` template composition. It supplies Bombay's
fixed address and fold boilerplate, then delegates to the owning Behavior
macro without defining another actor contract. Any other actor or protocol
macro present in a research branch or worktree remains only a probe until the
recorded ordinary-Rust comparison proves its exact residual gap; do not infer
or retain one merely because Bombay owns the outer application layer.

If that evidence eventually justifies a macro, Bombay may evaluate the Rust
forms appropriate to the exact seam: attribute and derive procedural macros,
function-like procedural macros, or `macro_rules!`. Macro availability is not
an API decision and does not waive the ordinary-Rust experiment requirement.

Macros own syntax and static derivation, never a second semantic
implementation. Every accepted expansion must lower to the exact selected
Behavior algebra, Bombay actor templates, and Bombay runtime composition; it
must preserve concrete protocols, effects, births, errors, authoritative
facts, and policy inputs. Prefer the macro form with the clearest application
syntax and diagnostics only after ordinary Rust alternatives have been
measured and rejected for that exact seam.
Generated code remains subject to compile-pass, compile-fail, differential,
hygiene, crate-renaming, expansion, and forbidden-erasure checks. A macro is
rejected when it hides policy, weakens static denial, introduces runtime
lookup or type erasure, or duplicates an owning semantic abstraction.

The authoritative-fact conservation law is fundamental:

> Once Bombay obtains an authoritative typed fact, no reusable layer may
> accidentally erase or misclassify it. Any preservation, transformation, or
> discharge must be an explicit typed policy.

This does not mean every failure bubbles upward. Restart, retirement, shutdown
discharge, and exact propagation are distinct static Behavior policies.
Bombay must preserve the source fact and allow independent typed consumers to
coexist; it must not cancel one observation because another consumer watches
the same incarnation, silently collapse provenance, or impose global failure
propagation in the runtime.

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
- Ordinary application state may use the distilled `#[bombay::actor]` facade,
  which delegates to the owning Behavior macro. Use explicit nominal
  `Behavior` implementations for advanced or library authoring where that
  facade does not fit. Bombay owns no reduced `Effect` language and may not
  implement Behavior semantics independently.
- Keep `bombay::prelude` application-facing. Foundational `Behavior`,
  interpreter traits, installation requirements, and structural effect paths
  remain deliberate imports through `bombay::behavior`; do not restore a glob
  re-export of the full owner crate into the ordinary prelude.

Actor-template recipes, when selected by the ledger, are derived statically
dispatched library constructions over existing Behavior Actors templates. They
are deliberate API composition policy, not new actor-model laws or runtime
actors. Separate their requirements into:

1. construction law: explicit inputs, fixed wrapper order, and one exact
   existing concrete output type;
2. error law: the exact fallibility of the existing construction steps, with
   no aggregate or erased recipe error;
3. semantic regression law: differential observable traces against the
   equivalent manual composition.

Each input governs its designated existing template or effect lane without
substitution, inference, dropping, or cross-lane consumption. Copying a `Copy`
value is not itself a violation. Prefer differential tests of initialization
effects, ordered actions, typed errors, next states, and terminal outcomes over
private-field inspection. Type inversions need prove only that reordered or
incomplete compositions have a distinct type or trace, not that every other
composition is impossible.

Do not introduce new recipe-specific behavior, protocol, effect, runtime,
registry, type-erasure abstraction, or macro. A named input product is allowed
only when it truthfully groups coexisting explicit inputs without defaults or
hidden policy. Existing Behavior Actors types are valid recipe outputs; “no
new recipe types” never means “no existing Bombay types.” Keep validation with
the owning input type: a recipe receiving an already validated value must not
claim or aggregate that value's earlier construction errors.

Audit the entire repository for obsolete composition patterns before claiming
completion.

Public examples must teach the actor-template path first. Whenever an existing
behavior is decorated with stash, timer, deadline, receive-timeout, or shutdown
policy, use the applicable `bombay::actors::ActorExt` method. Use owning
constructors for standalone roles such as `Machine`, `Task`, supervisors, and
worker pools because their inputs are semantic policy, not an inner behavior.
Do not hand-roll a template law inside a domain behavior, retain a competing
ordinary Bombay spelling, or add a semantically irrelevant wrapper merely to
exercise the extension trait. Every selected template in an executable example
must perform its actual policy during the example or its tests.

## Change containment and abstraction budget

Correct architecture does not justify unlimited code growth. Treat source
size, public surface, and reviewability as design constraints. A typed wrapper
that merely relocates complexity is not a successful composition.

Keep the requested blocker separate from later cleanup. Complete and verify
the blocker before editing production code for a broader audit or refactor. An
audit may add independent tests and identify later work, but it must not grow
the current production design unless a failing law independently proves that
the additional machinery is necessary.

Before the first production edit, add a change ledger to the active design
ledger containing:

- the exact blocker and the smallest end-to-end failing regression;
- expected files touched and expected production line delta;
- public types expected to be added and removed; and
- the existing owners, interpreters, products, and compositions that will be
  reused or deleted.

The following are automatic stop thresholds for the cumulative task, not
targets to evade by splitting commits or ledger items:

- more than 15 changed files;
- more than 500 net new production lines; or
- more than three new public types.

When any threshold is reached, stop before further production edits. Report
the current ledger and obtain explicit user authorization for the expanded
surface. Prior instructions to “finish,” “audit everything,” or “do it
holistically” do not waive this checkpoint.

Every new wrapper, builder, trait, facade, recipe, or public product must
answer all of these questions before implementation:

1. What unique semantic state does it own?
2. What unique event or effect transformation does it implement?
3. Why can the existing concrete composition not express the law?
4. What existing production code or caller-side machinery does it delete?
5. Which concrete use demonstrates that the abstraction belongs here?

Reject an abstraction that only renames a nested type, forwards unchanged
events or effects, stores another wrapper, hides a structural path, or makes a
single example look shorter while increasing the total public surface. Start
with a compile-only, differential, or pure-fold test that attempts the desired
syntax using existing owning types. Add production machinery only after that
test isolates the precise compositional gap.

At each logical checkpoint, measure the complete working tree, including
untracked files, and record:

```text
production: +A / -B / net C
tests:      +A / -B / net C
public API: +N types / -M types
```

Do not describe a change as cleanup, consolidation, or code reduction when its
production delta is net-positive. Separate new capability code from deletion
work so each can be judged honestly. Work in independently reviewable stages;
do not combine the blocker, a repository-wide redesign, wrapper cleanup, and a
test expansion into one undifferentiated patch.

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

- Run every Rust, Cargo, example, formatting, lint, and test command through
  the pinned Nix shell. Never invoke `cargo`, `rustc`, or a built example
  directly from the host environment.
- `nix develop -c cargo build --workspace` builds the workspace.
- `nix develop -c cargo test --workspace` runs unit, integration, and
  documentation tests.
- `nix develop -c cargo fmt --all -- --check` checks formatting.
- `nix develop -c cargo clippy --workspace --all-targets -- -D warnings` runs
  strict Clippy.

Use Rust 2024 idioms and rustfmt defaults. Prefer explicit behavioral names.
Avoid `unsafe` unless a measured need and safety proof are documented. Add an
observable invariant and an inversion test for every new law. Preserve exact
payloads in typed errors and use flat semantic `thiserror` variants instead of
nested positional product errors.

Before broadening a semantic change, run its focused regressions in both debug
and optimized builds. Assertions must be observational only: never place a
state transition, mutation, ownership transfer, or required function call
inside `assert!`, `debug_assert!`, or their equality variants. Treat
`clippy::debug_assert_with_mut_call` and `clippy::let_underscore_must_use` as
denied. For lifecycle and generation laws, explicitly replay the same fact in
an optimized test and prove that it cannot be accepted twice.

A regression is not accepted merely because it passes after the fix. Restore
or simulate the original defect and establish that the test fails for the
intended law. Tests must assert complete typed effect lanes or an independent
observable trace; repeated assertions of the same field, discarded actions,
predicted nonces, or models copied from implementation branches are invalid
evidence.

Prefer one module-level `use` over repeated fully qualified paths. In
particular, do not scatter `crate::...` or `std::...` through signatures and
function bodies; import the module or semantic names once. A single qualified
use is acceptable when it resolves real ambiguity, but qualification must not
become visual bookkeeping.

Use short imperative commit subjects with a scope-like prefix. Keep commits
focused and report the exact verification commands and results. Completion
also requires a final change ledger covering the complete tracked and
untracked delta.
