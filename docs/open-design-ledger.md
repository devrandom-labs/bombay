# Bombay open design ledger

This is the canonical executable backlog. Keep it short. It contains only the
live dependency graph, current feature verification, blockers, and decisions
that determine the next change.

Historical decisions are distilled in
[`historical-design-decisions.md`](historical-design-decisions.md). Runtime
capability ownership and proposed interfaces are normative in
[`runtime-capability-interfaces.md`](runtime-capability-interfaces.md).

## Working rules

For every feature:

1. Reinspect the exact locked source, public API, tests, and relevant docs for
   Behavior, Behavior Actors, Address, Communication, Observe, and Timers.
   Inspect Entity and other neighboring capabilities when the feature touches
   them.
2. Record that feature-specific verification here before implementation.
3. Keep the feature blocked if any dependency contract, exact version,
   protocol consumer, or ownership boundary is unverified.
4. Use Behavior Actors' named semantic send products and typed
   `SendEffects::send`. Do not handwrite application product routing.
5. Add an observable invariant and an inversion test for every new law.
6. Before completion, try to remove every added type, trait, field, task,
   channel, adapter, and public export without an independent invariant.
7. Synchronize all tracked docs, examples, tests, benchmarks, research probes,
   diagnostics, and re-exports affected by a changed contract.
8. Use `feature-complete` after feature gates pass. Use `distilled` only after
   the project-wide minimization and ownership audit. Never use `done`.

Dependency edges are reciprocal: every unresolved ID in `Blocked by` names the
blocked item in its `Unblocks` cell. Select work from this graph, not row order.

## Current dependency graph

| ID | Work | Priority | State | Blocked by | Unblocks |
|---|---|---:|---|---|---|
| UC1 | Communication affine user-admission retirement | P0 | feature-complete | — | E5 |
| UO1 | Observe unkeyed one-publication pair | P0 | feature-complete | — | E5 |
| E3 | Root examples and public usage guidance | P1 | feature-complete | — | E9 |
| E5 | Distilled local application runtime and routing | P0 | feature-complete | — | E7, E8, E9 |
| E2 | Static inference diagnostics | P1 | feature-complete | — | E9 |
| E6 | Concise functional Behavior authoring | P1 | deferred | optional Behavior-owned authoring convenience; explicit `Behavior` remains canonical | — |
| E7 | Live reference and terminal-result ergonomics | P1 | feature-complete | — | E9 |
| E8 | Facade, prelude, and system-construction coherence | P1 | feature-complete | — | E9 |
| E9 | Akka-IoT-sized reference application distillation | P0 | active | — | M4 |
| M4 | Developer-experience milestone distillation | P1 | blocked | E9 | FV1 |
| S1 | Comparative actor-model and composition synthesis | P1 | blocked | fresh S1 verification and synthesis | M5 |
| M2 | Runtime-operations milestone | P2 | deferred | external executor, stream, portability, and operations consumers | FV1 |
| M5 | Competitive-verification milestone | P1 | blocked | S1 | FV1 |
| FV1 | Competitive local-framework release audit | P1 | blocked | M2, M4, M5 | local framework release |

Completed component prerequisites—Driver, Engine environment spine,
Incarnation, Address/mailbox launch, named-send interpretation, recursive
creation/delivery, observation/timers/shutdown, Guardian root launch, and
one-value child creation—are historical evidence, not live graph nodes. Their
distilled contracts are linked from the top of this document. Fresh feature
verification remains mandatory.

Deferred future transport, identity, distributed discovery, durable hosting,
and optional-library projects are intentionally absent from the active local
framework graph. Add them only when an owning repository and concrete consumer
exist.

## Selected item: M4

### Fresh verification — 2026-08-19

M4 was re-verified independently against the exact dependency graph used by
this workspace, not against earlier ledger entries:

| Dependency | Exact source | M4 ownership finding |
|---|---|---|
| Behavior / Behavior Actors | patched checkout commit `643b9f14e4dac5ecd4fd545126c2807097a2c338`, package `bombay-behavior-actors 0.12.0` | owns `Protocol`, `Behavior`, `Actions`, typed sends and births, `Children`, the actor-authoring macros, and the complete template catalogue; templates are directly constructed values and do not expose an application-local hosting closure |
| Address | patched checkout commit `7df3bedc5f3177ddbdb617cefe4b6ffcd60ecda3`, package `bombay-address 0.2.0` | owns typed generation-safe endpoint storage and leases; it does not choose which protocol spaces an application hosts |
| Communication | locked registry package `bombay-communication 0.1.2`, checksum `fc3d06aaf88ef9fe5392506d13e208c2141e6978563e1b802b97b489b1a071e2` | owns the two-lane mailbox and affine user-admission lifecycle; no application-facing topology or actor policy belongs here |
| Observe | locked registry package `bombay-observe 0.1.1`, checksum `7017c773ae142628b6a244cddad0db987cfba22df4c93730844a7d43cc657304` | owns retained keyed observations and the unkeyed affine publisher pair; it contributes no user-visible actor-system configuration |
| Timers | locked registry package `bombay-timers 0.1.0`, checksum `5b3fc2dab4a030fd1838d0492a1c62d3c43ece1355125e3fc628da3ca436352d` | owns one generation-safe timer queue; timer-backed actor policy remains in Behavior Actors and queue operation remains private to Bombay |

The current public sources, semantic/runtime-contract tests, recursive protocol
proofs, compile-fail contracts, concurrency suites, and relevant crate-level
documentation were inspected again. Behavior Actors now supplies Guardian,
static and dynamic supervision, backoff supervision, worker pools, routing,
observation-driven lifecycle templates, shutdown coordination, timers,
discovery, workflows, persistence, and operations actors. Bombay must not wrap
those values in duplicate façade actors or policy builders.

The verification clears M4 for implementation. The user surface is ordinary
Rust only: Bombay's generic `App` separates a pure root Behavior from a named
product of typed local `ActorSpace<P>` values. Concrete `Hosts<P>`
implementations provide the static protocol-to-space mapping. Bombay has no
entry or topology macros, erased registry, endpoint enum, or positional public
API. Address, Communication, Observe, Timers, Engine, Incarnation, and all
interpreter types remain absent from Behavior values.

### Functional construction target — 2026-08-19

The authoritative entry is an ordinary Rust function returning
`App::new(root, actors).run()`, with actor
policy expressed by directly constructed Behavior Actors values and Bombay's
application boundary expressed by ordinary newtypes and consuming builders.
The desired shape is:

```rust,ignore
fn main() -> Result<(), RunError> {
    App::new(root(), AppActors::default()).run()
}
```

There is no `Bombay::new()` runtime object and no `System::new()` description
wrapper. Neither value would own an independent law: Bombay constructs its
executor internally, and the root Behavior value already is the application
composition. A wrapper introduced solely to hold host declarations would move
the transitional manifest without removing it.

### Typed Address-space composition decision — 2026-08-19

The protocol on a typed destination already identifies the exact endpoint
type. Bombay therefore keeps one `AddressSpace<P::Addr, ActorRef<P>>` for each
locally hosted protocol. The address selects an incarnation inside that typed
space; no runtime actor-type discovery is required.

Bombay must compose those typed spaces and provide each concrete delivery or
creation interpreter with the space selected by its protocol. It must not
collapse actor references into an enum, introduce an Address-owned erased
collection, use `TypeId`/`Any`, or add another namespace/addressing primitive.
The named product and handwritten `Hosts<P>` implementations are the complete
ordinary-Rust construction surface.

The remaining work is narrowly an ordinary Rust composition problem: retain
the protocol-indexed `AddressSpace` values as runtime-owned state and expose
the appropriate typed value to each monomorphized interpreter. Any proposed
functional construction API must preserve that static selection and shared
space identity without changing Address's ownership boundary.

### Macro-free actor-system implementation — 2026-08-19

The ordinary construction path is implemented. Bombay's generic `App` owns one
pure root Behavior and its named actor-space product. `ActorSpace<P>` is the exact typed Address space for live
local `ActorRef<P>` incarnations, and handwritten `Hosts<P>` implementations
select concrete fields. `App::run` constructs the executor and Guardian,
then shares the supplied actor-space product through all creation, delivery,
observation, and shutdown interpreters.

The Bombay macro crate, both Bombay macros, their exports and dependency, the
manifest traits, namespace terminology, and macro-specific compile tests were
deleted. The basic and supervision examples contain ordinary structs and
ordinary trait implementations only. No `TypeId`, `Any`, endpoint enum, HList,
positional witness, or dynamic dispatch was introduced.

The previously unfinished worker-pool extension in `supervision.rs` was
removed while establishing this baseline because it did not satisfy recursive
creation dispatch before this change. The committed supervision scenario still
proves heterogeneous recursive creation and multiple hosted protocols through
the new `Hosts<P>` path; worker-pool acceptance remains E9 work rather than
being hidden inside the composition migration.

`cargo test --workspace`, workspace all-target Clippy with warnings denied,
format checking, both runnable examples, and `git diff --check` pass.

The first M4 acceptance audit reopened E9. The existing public supervision
example proves recursive heterogeneous creation, typed local delivery, dynamic
membership, and an actor-owned timer, but it is not the acceptance application
specified by `docs/user-facing-api.md`. It does not exercise a configured
restart policy with backoff, worker-pool scheduling, exact-incarnation watch
cleanup, coordinated recursive shutdown, and exact terminal failure reporting
as one complex public composition. Unit tests for those isolated runtime lanes
are necessary but do not prove that ordinary application composition remains
usable at their intersection. E9 therefore returns to `active`, and M4 is
again blocked by E9. The M4 cross-crate verification remains current evidence,
but implementation cannot be selected ahead of the missing acceptance proof.

## Selected item: E5

### Objective

Construct the minimum standard local environment that executes a closed
Behavior application using the existing Engine spine and primitive
capabilities. Delete duplicated legacy plumbing. Preserve a functional
`App::run` path; Bombay defines no entry macros.

### Fresh verification — 2026-08-17

The exact selected sources were re-read for E5:

| Dependency | Exact source | Verified ownership |
|---|---|---|
| Behavior / Actors | local patch `79b66f0041df956345aee8624504f82df4114f9b`, package 0.12.0 | pure Behavior algebra, Driver action contract, closed heterogeneous `Children`/`ChildChoice`, named semantic sends, actor policy templates, explicit proxy-subtree shutdown |
| Address | local patch `7df3bedc5f3177ddbdb617cefe4b6ffcd60ecda3`, package 0.2.0 | typed `AddressSpace`, exact registration `Lease`, opaque `Resolved` endpoint |
| Communication | locked crates.io 0.1.2, checksum `fc3d06aaf88ef9fe5392506d13e208c2141e6978563e1b802b97b489b1a071e2` | two-lane mailbox, priority/aging, backpressure, affine `MailboxOwner`, cloneable `MailboxRef`, user-admission retirement, Consumer teardown, exact payload recovery |
| Observe | locked crates.io 0.1.1, checksum `7017c773ae142628b6a244cddad0db987cfba22df4c93730844a7d43cc657304`; released pair source also present at owning commit `45c9143` | keyed exact-generation publication plus fresh unkeyed `pair`, affine consuming `Publisher`, cloneable retained `Observation`, cancellable waiting |
| Timers | locked crates.io 0.1.0, checksum `5b3fc2dab4a030fd1838d0492a1c62d3c43ece1355125e3fc628da3ca436352d`; matching checkout `4e515ed176f503bf6a5bd0d736ffa0394cb7f1f2` | single-owner generation-safe `TimerQueue`, schedule/cancel/deadline/pop-due |
| Entity | canonical neighboring checkout `68d0f503205a569ddda88124f5add8e8a652e18f` | entity activation, generation, admission, fencing, draining, and retirement laws; current directory/runtime/executor façade is legacy |

The locked registry artifacts, not matching local Observe/Timers checkouts, are
the Bombay build authority. Communication 0.1.2 source, retirement tests, and
published documentation were inspected from the exact registry artifact.

Relevant unit, compile, model/property, Loom, fuzz, allocation, teardown,
reclamation, and documentation evidence in each owning repository was audited.
The complete mapping and laws are in
[`runtime-capability-interfaces.md`](runtime-capability-interfaces.md).

Timer integration was re-verified immediately before its E5 change against
the locked 0.1.0 source and its semantic, differential/model, stale-token,
equal-deadline, compaction, allocation, fuzz, and Loom evidence. `TimerQueue`
already owns replacement generations, ordering, cancellation, deadline lookup,
and due-value extraction; Bombay needs only to schedule typed values and poll
the queue beside mailbox ingress.

### Ownership decision

The standard local incarnation is one static composition:

```text
Behavior + Driver
  inside Environment / ActiveEnvironment
    + Communication mailbox
    + Address lease and shared AddressSpace
    + Observe activation and termination facts
    + actor-owned TimerQueue
    + typed capability-lane interpreter product
    + Bombay-owned task and child-task ownership
```

`Environment<B>` / `ActiveEnvironment<B>` is the single Behavior spine.
Pluggability belongs at statically typed effect-lane interpreters. Whole
Environment replacement is an extension/test-host boundary. Ordinary users do
not choose Address, Communication, Observe, or Timers adapters for the standard
local runtime.

The following have no independent law and must be removed:

- `LocalActors`, `LocalProtocol`, and Bombay “namespace” wrappers;
- Tokio activation and termination channels;
- runtime-stop oneshot (removed after Communication 0.1.2 integration);
- timer task and timer command channel;
- `TerminationCell` over `ObservationSpace<(), O>`;
- unused manifest `outbound` and `provided` declarations;
- any dynamic/erased runtime capability registry.

Bombay may retain a private closed static product of concrete Address spaces
for locally hosted endpoint types. That is application topology evidence, not
a new namespace primitive. Local hosting cannot be inferred solely from every
protocol mentioned by a Behavior.

### Completed prerequisite: UC1

Communication 0.1.2 supplies
`mailbox_channel(Config) -> (ControlSender<C>, MailboxOwner<U>, MailboxRef<U>, Consumer<C, U>)`.
The affine owner closes only user admission; already admitted user values
drain, blocked and later sends recover their exact payload, stale references
cannot reopen admission, and the control lane deliberately remains available
for lifecycle facts until its final sender drops. Bombay now owns one strong
admission authority per live incarnation, stores only weak admission authority
in public references, closes admission before typed shutdown, and has deleted
the parallel runtime-stop oneshot. `local.rs` directly exercises ordering and
continued control delivery after user closure.
The exact published owning-suite command
`cargo test --manifest-path …/bombay-communication-0.1.2/Cargo.toml --test mailbox_retirement`
passes all three close/drain/race tests.

#### Completed prerequisite: UO1

Observe 0.1.1 supplies
`pair<O>() -> (Publisher<O>, Observation<O>)`. `Publisher` is affine,
non-cloneable, and consumed by completion; `Observation` clones without an
`O` bound and retains clone-based fan-out outcomes. Each pair has one fresh,
non-pooled slot. Incomplete publisher drop deliberately leaves observations
pending. Published unit, compile-fail, cancellation, panic-waker, allocation,
and Loom tests cover direct pair publication, clone fan-out, isolation, late
reads, and incomplete drop. Bombay now uses pairs directly and has deleted its
activation channels and keyed `TerminationCell`, while explicitly completing
every launch and terminal branch.
The owning checkout's exact `bombay-observe-tests --test pair` suite passes all
16 pair-specific lifecycle, cancellation, fan-out, panic-waker, isolation, and
destruction tests against version 0.1.1.

### No upstream change required

- Address already has the complete endpoint-generation contract.
- Timers already has the complete queue contract. Bombay must own the queue in
  the actor environment and select mailbox input against `next_deadline`.
- Behavior/Actors already supplies the pure policies and named effect lanes
  needed for current templates. E5 must consume them directly.

### Entity disposition

Preserve Entity's single-flight activation, bounded admission,
generation-safe commitment/retirement, fencing, drain, cancellation, and stale
fact laws. Do not integrate `LocalDirectory`, `LocalEntityRuntime`,
`EntityRuntime`, or its parallel executors. Entity must first be re-founded as
ordinary Behavior Actors templates emitting standard creation, delivery,
observation, timer, and shutdown effects, with parity tests for every retained
law.

### E5 implementation order

1. Verify the exact new owning-repository APIs and tests; update this record.
2. Finish direct Address-space storage in the private closed topology product.
3. Build one Communication mailbox and one activation/termination Observe pair
   per incarnation.
4. Keep one actor-owned `TimerQueue` behind a minimal shared `LocalTimers` view
   used by `next` and the typed schedule interpreters.
5. Make `LocalEnvironment` the one concrete implementation of the Engine
   spine and retain typed capability products behind `ActionInterpreter`.
6. Remove every duplicate wrapper/channel/task listed above.
7. Prove activation ordering, message delivery, creation, observation, timer
   delivery, recursive shutdown, and terminal ordering through public
   `App::run`.
8. Synchronize all repository artifacts and run the complete workspace and
   owning-repository gates.

### Acceptance invariants

- initialization commits before Address publication and activation success;
- a published activation reference resolves immediately;
- one locally hosted endpoint type has one shared Address space per app;
- one actor has one Communication mailbox and no parallel event channel;
- capability results re-enter only as later typed events;
- action interpretation uses named semantic lanes and returns semantic
  `thiserror` errors without nested positional product errors;
- Address lease retirement precedes terminal Observe publication;
- parent terminal publication implies all owned child tasks settled;
- shutdown races recover every non-linearized payload exactly;
- stale Address leases, Observe generations, timer tokens, and entity facts
  cannot affect replacements;
- the functional API runs the heterogeneous reference scenario without a
  macro;
- full tests, compile tests, doctests, fmt, strict Clippy, model/Loom/fuzz
  owning-repository gates, and repository-wide obsolete-pattern scans pass.

### Current state

E5 is feature-complete and awaits the project-wide release distillation. Direct Address-space storage,
Communication 0.1.2 admission retirement, Observe 0.1.1 activation/termination
pairs, and the actor-owned TimerQueue are integrated. Runtime-stop,
activation, and timer command channels; the timer task; keyed
`TerminationCell`; and the redundant external primitive-reconstruction test
are removed. The updated Behavior Actors proxy shutdown lane is interpreted,
the real DynamicSupervisor -> Proxy -> Worker subtree test passes, and public
`App::run` proves timer delivery without an auxiliary task.
Peer and child Observe futures are now polled by the same active Environment
spine through a typed fact queue; per-observation Tokio tasks, control-lane
reinjection, aborts, joins, and child-observation `JoinHandle` maps are removed.

## Next selection

E9 is active. The current Behavior source confirms that explicit `Behavior`
implementation is the canonical non-macro authoring path; optional concise
authoring is not a prerequisite for proving complex template composition.

## Selected item: E7

### Fresh verification — 2026-08-17

The exact locked source, public API, tests, and relevant documentation for all
neighboring primitives were re-read specifically for E7:

- Behavior / Actors at local patch
  `79b66f0041df956345aee8624504f82df4114f9b` defines `Behavior`, pure
  protocol-indexed `Recipient<B>`, `MailAddr`, typed user envelopes, terminal
  `Exit`/`Crash`, and actor templates. It owns no live endpoint or observation
  handle.
- Address 0.2.0 at local patch
  `7df3bedc5f3177ddbdb617cefe4b6ffcd60ecda3` stores cloneable typed endpoints,
  returns exact resolved-registration snapshots, and retires them through an
  affine generation lease. Its semantics, reclamation, allocation, and Loom
  tests do not define endpoint liveness or terminal results.
- Communication 0.1.2 (locked checksum
  `fc3d06aaf88ef9fe5392506d13e208c2141e6978563e1b802b97b489b1a071e2`)
  supplies non-owning `MailboxRef`, affine user admission, exact rejected
  payload recovery, and clone-counted `ControlSender`. Its retirement,
  lifecycle, teardown, property, allocation, and Loom tests establish that a
  retained control sender prolongs control-lane liveness; therefore public
  actor references must not clone or retain one strongly.
- Observe 0.1.1 (locked checksum
  `7017c773ae142628b6a244cddad0db987cfba22df4c93730844a7d43cc657304`)
  supplies a fresh `pair`, affine consuming `Publisher`, cloneable retained
  `Observation`, `try_get`, blocking wait, and cancellable async wait. Its
  semantic and Loom evidence makes that observation the complete public
  terminal-result capability; Bombay needs no wrapper or query API.
- Timers 0.1.0 (locked checksum
  `5b3fc2dab4a030fd1838d0492a1c62d3c43ece1355125e3fc628da3ca436352d`)
  supplies only actor-owned scheduling/cancellation/deadline extraction. Its
  semantic and Loom tests expose no live-reference responsibility.
- Entity at neighboring patch
  `68d0f503205a569ddda88124f5add8e8a652e18f` was rechecked for boundary
  ownership: its generation/fencing/admission laws do not replace an ordinary
  local actor endpoint and are not integrated into E7.

### E7 ownership decision

`ActorRef<P>` is a non-owning external capability consisting only of the exact
typed address, Communication `MailboxRef`, and the exact retained Observe
termination fact. User admission is guarded by a weak reference to Bombay's
affine owner. Private shutdown routing holds only a weak, erased control-plane
adapter for the concrete behavior; the active environment is its sole strong
lifetime owner. Public addressing remains indexed solely by `P`. The public
terminal method returns the owning Observe type directly. Bombay adds no
terminal cell, waiter, task, or result wrapper.

### E7 current state

E7 is feature-complete. Public references now weakly retain both admission and
private shutdown routing, so only the active environment owns control-lane
liveness. `ActorRef::termination` returns the exact cloned Observe fact. The
Bombay unit, public integration, topology compile, and documentation tests pass
with the narrowed ownership model.

## Selected item: E8

### Fresh verification — 2026-08-17

The exact locked source, APIs, tests, and relevant documentation for every
neighbor were re-read again specifically for the application-entry boundary:

- Behavior / Actors at patch
  `79b66f0041df956345aee8624504f82df4114f9b` owns `Behavior`, `Guardian<A>`,
  all actor templates, typed action products, and the closed child algebra.
  It does not own executors, live topology allocation, or a process entry
  point. Bombay must wrap the supplied root in `Guardian` internally and must
  not add another application actor abstraction.
- Address 0.2.0 at patch
  `7df3bedc5f3177ddbdb617cefe4b6ffcd60ecda3` requires one concrete shared
  `AddressSpace<A, E>` per locally hosted protocol. It cannot infer whether a
  protocol is local, external, or remote, so the closed topology manifest
  remains required independently of entry syntax.
- Communication 0.1.2, Observe 0.1.1, and Timers 0.1.0 at the checksums recorded
  above expose runtime-owned mailbox, terminal-fact, and timer-queue
  capabilities only. Their public APIs and lifecycle/property/Loom tests add
  no user-selectable runtime construction and no entry-macro requirement.
- Entity at patch `68d0f503205a569ddda88124f5add8e8a652e18f`
  remains outside the standard local application composition until its laws
  are expressed through ordinary Behavior Actors templates.

### E8 ownership decision

`App::new(root, actors).run()` is the sole runtime path. `App`
separates the pure root from its named local actor-space product. Bombay owns
no entry or topology macros and must not infer hosting, implement Behavior,
expose Tokio, or construct a user-visible Guardian.

### E8 current state

E8's earlier macro result was superseded by the ordinary actor-system
construction. The prelude, root basic example, README, user API, and
runtime-capability documentation now show one coherent macro-free entry model.

## Selected item: E2

### Fresh verification — 2026-08-17

The exact source, APIs, tests, and relevant documentation of every neighboring
primitive were re-read specifically for static application diagnostics:

- Behavior / Actors at patch
  `79b66f0041df956345aee8624504f82df4114f9b` provides closed
  `Children`/`ChildChoice` dispatch, pure `Recipient<B>`, named send products,
  Guardian, supervisors, pools, routers, shutdown templates, and their algebra,
  runtime-contract, and mutation tests. It deliberately does not classify a
  mentioned protocol as local, remote, or externally supplied. Bombay cannot
  derive a truthful local-hosting closure from `Behavior`.
- Address 0.2.0 at patch
  `7df3bedc5f3177ddbdb617cefe4b6ffcd60ecda3` requires a concrete typed
  `AddressSpace<A, E>` before claim or resolution. Its semantic, reclamation,
  allocation, and Loom tests expose no erased or discoverable registry from
  which Bombay could recover a missing protocol dynamically.
- Communication 0.1.2, Observe 0.1.1, and Timers 0.1.0 at the locked checksums
  recorded above are incarnation-local capabilities. Their API, lifecycle,
  property, cancellation, and Loom evidence contributes no application-wide
  protocol discovery or type inference.
- Entity at patch `68d0f503205a569ddda88124f5add8e8a652e18f`
  remains a separate future Behavior-template integration and cannot be used
  as an endpoint registry for ordinary actors.

### E2 ownership decision

The named local actor-space product is explicit evidence, not inferred
topology. Diagnostics must therefore enforce only facts it can know:

- every listed entry is a Behavior protocol;
- each concrete protocol has exactly one shared Address-space field;
- a protocol required by creation, delivery, observation, reporting, or
  shutdown but lacking `Hosts<P>` reports a focused missing-local-host
  capability error rather than suggesting a runtime registry.

### E2 current state

E2's manifest-specific diagnostics were removed with the manifest. The
remaining useful diagnostic names the absent `Hosts<ChildProtocol>` capability;
ordinary Rust rejects duplicate trait implementations through coherence.

## Selected item: E9

### Fresh verification — 2026-08-19 (complete application intersection)

E9 was re-verified independently against the exact dependency selection used
by this workspace:

| Dependency | Exact source | E9 ownership finding |
|---|---|---|
| Behavior / Behavior Actors | patched checkout commit `643b9f14e4dac5ecd4fd545126c2807097a2c338`, package `bombay-behavior-actors 0.12.0` | owns `Protocol`, `Behavior`, structural effect interpretation, `Children`, Guardian policy, supervision, pools, watches, shutdown coordination, and every actor-template protocol; Bombay must only interpret those effects |
| Address | patched checkout commit `7df3bedc5f3177ddbdb617cefe4b6ffcd60ecda3`, package `bombay-address 0.2.0` | owns typed address-to-endpoint registration, resolution snapshots, lease-exact retirement, and address-reuse isolation; `ActorSpace<P>` is only Bombay's protocol-indexed specialization of this primitive |
| Communication | locked registry package `bombay-communication 0.1.2`, checksum `fc3d06aaf88ef9fe5392506d13e208c2141e6978563e1b802b97b489b1a071e2` | owns the bounded two-lane mailbox, affine user admission, closure, payload recovery, fairness, and wakeup; it owns no actor topology or policy |
| Observe | locked registry package `bombay-observe 0.1.1`, checksum `7017c773ae142628b6a244cddad0db987cfba22df4c93730844a7d43cc657304` | owns retained keyed generations and the unkeyed consuming publisher pair used for exact lifecycle outcomes; it owns no actor protocol routing |
| Timers | locked registry package `bombay-timers 0.1.0`, checksum `5b3fc2dab4a030fd1838d0492a1c62d3c43ece1355125e3fc628da3ca436352d` | owns branded generation-safe scheduling, replacement, cancellation, due ordering, and compaction; timer-backed policy remains in Behavior Actors |

The current public sources, crate documentation, semantic tests, concurrency
tests, runtime-contract manifest, recursive protocol proofs, and relevant
compile-fail contracts were inspected. No neighboring primitive supplies or
requires another Bombay registry, mailbox, lifecycle abstraction, or actor
policy wrapper.

This fresh application-level audit exposes two distinct template-composition
limits which the existing green examples do not cover:

- `WorkerPool` and `KeyedWorkerPool` always create `Proxy<C>` and therefore
  fix both proxy-to-parent report ingresses at `Here`. Unlike
  `DynamicSupervisorWithParent`, neither pool has a parent-path parameter.
  Wrapping a pool in `StopOnShutdown`, `ShutdownCoordinator`, or another
  event-extending template moves the pool-owned `WorkerStopped` and
  `WorkerCreationResolved` lanes under `Inside<Here>`, while its proxies still
  report to `Here`. Bombay cannot recover that parent path from the runtime
  request and must not add a pool-specific positional exception.
- `ShutdownCoordinator<B, C>` deliberately coordinates one homogeneous child
  protocol `C`. The reference root owns heterogeneous supervisor and pool
  children, so this template cannot by itself express the required ordered
  root shutdown. `Guardian` direct shutdown stops the root and Bombay then
  cancels remaining owned tasks; that is a safe ownership fallback, not the
  documented graceful tree traversal.

E9 remains `active`, but its Bombay interpreter layer is not the source of
either gap. The next valid implementation step requires Behavior Actors to
provide path-aware pool parent ingress and a composition for heterogeneous
coordinated child shutdown (or prove an existing template composition that
does so). Bombay must not duplicate either policy. Once those contracts
exist, E9 should need only a public acceptance example and existing generic
interpreters.

### Fresh verification — 2026-08-17

All neighboring contracts were re-read again for the representative complex
application rather than inherited from E2:

- Behavior / Actors patch
  `79b66f0041df956345aee8624504f82df4114f9b` supplies explicit `Behavior`,
  `Children::new().child(...).into_creates()`, closed `ChildChoice`, Guardian,
  DynamicSupervisor and shutdown-capable DynamicProxy, Supervisor,
  BackoffSupervisor, WorkerPool/KeyedWorkerPool, routers, Watch/Link,
  ShutdownCoordinator/TreeShutdown, and timer-backed wrappers. Its current
  constructors, event sums, named sends, algebra tests, runtime-contract tests,
  mutation tests, and compile-fail contracts were inspected. There is no
  current functional Behavior constructor, and none is required to use these
  concrete templates directly.
- Address patch `7df3bedc5f3177ddbdb617cefe4b6ffcd60ecda3`, Communication 0.1.2,
  Observe 0.1.1, and Timers 0.1.0 retain exactly the endpoint, mailbox,
  terminal-fact, and queue ownership recorded above. Their current source and
  semantic/concurrency evidence expose no additional application API.
- Entity patch `68d0f503205a569ddda88124f5add8e8a652e18f` is still not a Behavior
  Actors template and is excluded from this local actor-tree acceptance case.

### Dependency refresh — 2026-08-18 (`MessageAdapter`)

E9 was re-verified against Behavior / Actors patch
`445a2b04008967ce49c914fcfeb39978e6ec16ac` before changing the example.
The new public `MessageAdapter<Input, Destination>` is a pure, concrete
Behavior Actors template: it maps each accepted input exactly once through a
function pointer, emits exactly one typed `Delivery<Destination>`, has empty
initialization, and owns no births, phases, errors, channels, handles, tasks,
or I/O. Its public integration test proves that the concrete type satisfies
both `DynamicSupervisor` reply and `WorkerPool` response positions. The
focused upstream `message_adapter` test passes in the pinned Rust 1.96 shell.

The neighboring capability contracts were re-read for this feature. Address
patch `7df3bedc5f3177ddbdb617cefe4b6ffcd60ecda3` still owns typed resolution and
registration leases; locked Communication 0.1.2 still owns the two-lane
mailbox and admission; locked Observe 0.1.1 (matching pair implementation at
checkout `dea67359dd39296b8b482fc41c08818f6bae7f56`) still owns retained lifecycle
publication; and locked Timers 0.1.0 (matching checkout
`4e515ed176f503bf6a5bd0d736ffa0394cb7f1f2`) still owns generation-safe
scheduling. Protocol adaptation changes none of those ownership boundaries.
Bombay therefore needs no new runtime primitive. Attempting the intended
replacement exposed an upstream type-level blocker not covered by the current
leaf-root integration proof: when the adapter destination is the real
application root, the root sends to a `DynamicSupervisor` whose reply type is
that adapter. Trait selection then recurses through `System::Sends` ->
`DynamicSupervisor<Reply = MessageAdapter<_, Guardian<System>>>` -> the
adapter's `Delivery<Guardian<System>>` -> `System::Sends`, and rustc reports
E0275 while proving `SendEffects`. The compiling handwritten leaf reply actor
is retained only until Behavior Actors supplies a composition that breaks this
recursive protocol proof. E9 must not duplicate that fix inside Bombay.

### Dependency refresh — 2026-08-18 (`Protocol` separation)

Behavior / Actors patch
`a94cae1b683de1e556d906d35d3ed61143d3e236` resolves the preceding blocker by
splitting the static actor signature into `Protocol { Addr, Msg }` and making
`Behavior: Protocol` own only the executable transition algebra. `Recipient`
and `Delivery` now require only `Protocol`; actor templates implement their
protocol signature independently of their Behavior proof. The new concrete
root tests and the 20-family recursive reply matrix prove that
`MessageAdapter<_, Guardian<Root>>` no longer recursively evaluates the root's
sends. This is a source-breaking contract change for every handwritten Bombay
Behavior and for generic code that previously declared `Addr`/`Msg` inside its
`Behavior` implementation, so E9 remains active while the entire repository is
migrated and audited.

The mandatory neighboring audit was repeated against the exact selected
artifacts. Address 0.2.0 at
`7df3bedc5f3177ddbdb617cefe4b6ffcd60ecda3` still owns typed registration,
opaque resolution, and exact lease retirement. Communication 0.1.2 checksum
`fc3d06aaf88ef9fe5392506d13e208c2141e6978563e1b802b97b489b1a071e2`
still owns the two-lane mailbox, admission, fairness, closure, and payload
recovery. Observe 0.1.1 checksum
`7017c773ae142628b6a244cddad0db987cfba22df4c93730844a7d43cc657304`
still owns keyed generations and unkeyed retained lifecycle pairs. Timers
0.1.0 checksum
`5b3fc2dab4a030fd1838d0492a1c62d3c43ece1355125e3fc628da3ca436352d`
still owns generation-safe schedule, replacement, cancellation, and due-value
ordering. Their current public source and semantic/concurrency tests were
re-read; none owns or needs awareness of Behavior's static `Protocol` proof.
Bombay's migration is therefore strictly at its Behavior-facing generic,
implementation, diagnostic, example, and documentation boundaries. It must
not add a duplicate protocol trait or change primitive runtime ownership.

### E9 implementation rule

The reference application must use exact concrete Behavior Actors templates,
explicit Behavior implementations only for application-domain actors,
`Children`, named send products, pure
Recipients, the named local actor-space product, and ordinary `run`. It must not
handwrite runtime protocol interpreters, product routing, mailboxes, tasks,
addresses, observations, timer queues, guardians, or executor setup. Each
successive example layer must execute through public API before adding the
next template.

### E9 current progress

The public `supervision` example now executes a heterogeneous root containing
a DynamicSupervisor, shutdown-capable DynamicProxy/worker subtree, typed reply
actor, and OneShot timer actor through the tiny main API. Running it exposed
and fixed a hidden ownership deadlock: parent retirement previously awaited a
still-live child forever. Behavior templates retain graceful shutdown policy;
Bombay now aborts and joins any remaining owned task when its owner terminates.
A timeout-backed unit invariant fails if that cancellation fallback is removed.

After the `Protocol` separation, all 83 handwritten actors across Engine and
Bombay production tests, integration suites, compile fixtures, fuzz targets,
benchmarks, and public examples were migrated to explicit `Protocol` plus
`Behavior` implementations. Fully-qualified address projections now use
`Protocol`; executable hosting and environment bounds remain `Behavior`
because they require the event/effect algebra. The public supervision example
now replaces its temporary handwritten reply sink with
`MessageAdapter<DynamicSupervisorOutcome<_, _>, Guardian<System>>`; the actual
recursive root compiles and runs through Bombay's unchanged generic delivery
interpreter. Normative API and Driver/capability documents and compile-fail
diagnostics have been synchronized with the split.

### E9 next-slice verification — managed destinations and pool completion

The next acceptance slice was derived from the exact current APIs rather than
simulated with hard-coded addresses. Behavior / Actors patch
`a94cae1b683de1e556d906d35d3ed61143d3e236` was re-read across `Recipient`,
`DynamicSupervisor`, `Proxy`, `MessageAdapter`, `WorkerPool`, its model tests,
and recursive reply proofs. Address 0.2.0, Communication 0.1.2, Observe 0.1.1,
and Timers 0.1.0 were re-read again from the exact selected source. They still
own endpoint lookup, mailbox admission, retained facts, and scheduling only;
none can invent a pure actor destination or repair a template protocol.

Two connected Behavior Actors contracts currently prevent an honest IoT
composition:

- `DynamicSupervisorOutcome` returns a managed child's nonce and phase but no
  reusable typed destination. `Recipient` can represent only an absolute
  address or one child relative to the actor that later emits the delivery.
  A caller outside the supervisor cannot address the nested `DynamicProxy<C>`
  from the nonce without constructing and composing concrete addresses.
- `WorkerPool` delivers `PoolAssignment<J>` through its stable proxy, but the
  assignment contains no pure completion destination. A worker must send
  `PoolMessage::Completed` back to the pool to release its slot, yet it cannot
  obtain the pool recipient from the assignment or from its proxied sender.
  The only current application-level implementation is to preconfigure every
  worker with the pool's absolute `MailAddr` and its stable nonce.

The same issue is visible one layer earlier in the current supervision
example: constructing a reply adapter to the automatically created Guardian
still spells `Recipient::global(MailAddr(0))`. That is exactly the runtime
address plumbing the target API promises to hide.

Bombay must not solve these gaps with a second route algebra, address
arithmetic, runtime handles inside Behavior, special-case channels, or a
handwritten pool protocol. The upstream contract must make a managed child and
a pool completion target usable as pure typed destinations whose identity does
not change according to whichever actor later emits the delivery. E9 remains
blocked until that contract exists; once supplied, Bombay should need only its
existing generic creation and delivery interpreters.

### Dependency refresh — 2026-08-18 (protocol-indexed runtime boundary)

E9 was re-verified against Behavior / Actors patch
`0e8ba09e4ec3372085c90558aa2eb0b658bebd6f` before resuming implementation.
`Behavior` now owns `type Protocol: Protocol` instead of being a `Protocol`
supertrait. `Recipient<P>`, `ChildRecipient<P>`, `Delivery<P>`, and
`DeliveryTarget<P>` are indexed solely by the stable protocol; transparent
wrappers preserve `B::Protocol` and are not new public destinations. Local-child
identity is creator-relative only through `ChildRecipient`; established
recipients carry absolute addresses. `DynamicSupervisorOutcome<A, C>::Started`
now returns `Recipient<DynamicProxy<C>>`, and `PoolAssignment<P>` carries its
worker plus `complete_to: Recipient<P>`. These changes clear both recorded E9
template blockers, so E9 returns to `active`.

The mandatory neighboring audit was repeated against the selected Bombay
artifacts. Address patch `7df3bedc5f3177ddbdb617cefe4b6ffcd60ecda3`
still owns generic address-to-endpoint registration, opaque resolution, and
lease-scoped exact retirement. Communication 0.1.2 still owns the two-lane
`Consumer<C, U>` and its strong-owner/weak-reference admission law; its
independent lane types allow Bombay to keep `B::Event` on the control lane and
put protocol user input on the user lane. Observe 0.1.1 still owns cloneable
retained observations and the consuming unkeyed `Publisher`; Timers 0.1.0 still
owns generation-safe scheduling, replacement, cancellation, and due ordering.
Their exact current public source and tests expose no actor protocol or behavior
composition policy.

The resulting ownership mapping is now explicit. Bombay address spaces and
public actor references are indexed by `B::Protocol`, because protocol identity
must survive transparent behavior wrappers. A running environment, its control
sender, timer/fact injection, and Engine driver remain indexed by concrete `B`,
because only `B` owns the complete event and effect algebra. Communication's
user lane carries `User<BehaviorAddr<B>, BehaviorMessage<B>>`; the environment
injects that value into `B::Event`. Creation selects the one shared namespace
for the child's protocol, while delivery selects the namespace named directly
by `Delivery<P>`. Bombay must delete wrapper-indexed namespaces and must not add
a duplicate protocol registry, erased delivery router, or behavior-owned
channel.

The repository-wide migration now uses Behavior-owned protocol templates at
every Bombay production, example, topology-manifest, and public-documentation
boundary. Ordinary structural inboxes use `MessageProtocol<A, M>`; specialized
actor templates keep their Behavior Actors protocol types. The historical
Engine template-law fixture deliberately retains nominal protocols where it
proves that two equal message signatures remain distinct destinations; those
are test evidence, not Bombay runtime protocols.

The next executable E9 slice is blocked by one verified Behavior Actors
contract. `StopOnShutdown<DynamicSupervisor<...>>` requires
`DynamicSupervisorEvent<...>: RouteInput<ShutdownRequested>`, but the current
event exposes only child-stop, creation-resolution, worker-resolution,
worker-stop, and shutdown-rejection lanes. Without the shutdown lane Bombay
cannot both host the dynamic supervisor and preserve orderly guardian-driven
subtree shutdown. Bombay must not add a duplicate supervisor wrapper or weaken
shutdown to task abortion; Behavior Actors must make the template composable
with `StopOnShutdown` first.

### Dependency refresh — 2026-08-18 (path-indexed ingress algebra)

E9 was re-verified against Behavior / Actors commit
`d5d161f76e743f465c454282fc7891f5e1d6cdab` (workspace version 0.12.0).
Interpreter-originated facts no longer rely on open `RouteInput`/`EventInput`
search. Behavior now owns structural `EventLayer`, path evidence through
`Here`/`Inside<Path>`, `InjectEvent<Input, Path>`, and the zero-sized
`Ingress<Input, Path>` capability retained by effect requests. Actor templates
declare the exact ingress to use when they emit observation, timer, creation,
reporting, and shutdown effects. `StopOnShutdown<B>` now owns an outer
`ShutdownRequested` layer over every `B` without requiring `B::Event` to accept
shutdown, clearing the previous DynamicSupervisor composition blocker.

The neighboring runtime audit was repeated for this feature. The lock selects
Address 0.2.0 at `7df3bedc5f3177ddbdb617cefe4b6ffcd60ecda3`,
Communication 0.1.2, Observe 0.1.1, and Timers 0.1.0. Address still owns typed
registration, resolution, registration identity, and lease-exact retirement.
Communication still owns the two-lane mailbox, strong control sender, weak user
admission, closure, payload recovery, fairness, and wakeup protocol. Observe
still owns retained keyed observations and the consuming unkeyed publication
pair. Timers still owns branded, generation-safe scheduling, replacement,
cancellation, due ordering, and compaction. None owns Behavior ingress paths.

Therefore Bombay's adapter law changes narrowly: every asynchronous runtime
operation must retain the `Ingress` carried by its Behavior request and later
construct the fact with `ingress.event(...)`. Bombay must remove generic
`EventInput`/`RouteInput` bounds and must not rediscover a matching event lane
from the final composed event type. The running behavior remains concrete and
the runtime primitives remain unchanged.

### Complete template audit — 2026-08-18

Before implementing that adapter change, E9 re-read every production template
exported by Behavior Actors at `d5d161f76e743f465c454282fc7891f5e1d6cdab`,
its concrete `Behavior` implementation, event algebra, sends algebra, births,
public tests, `runtime_contracts` manifest, and the upstream foundational
algebra and adapter-contract documents. The complete runtime-facing map is:

- user-only actors with generic delivery/no-service interpretation: `Machine`,
  `MessageAdapter`, `Task`, `Buffer`, `Router`, `PriorityQueue`, `WorkQueue`,
  `Sequencer`, `Acknowledgements`, `Deduplicator`, `RateLimiter`, `Correlator`,
  `OrderGate`, `Topic`, `Registry`, `Resolver`, `PubSub`, `Configuration`,
  `Features`, `Health`, `Readiness`, `Cache`, `Workflow`, `Barrier`, and
  `Latch`;
- concrete actors with private runtime facts: `CircuitBreaker`, `Lease`, and
  `Presence` own `TimerElapsed`; `Proxy` owns child/creation/shutdown/report
  lanes; `DynamicSupervisor` owns dynamic-proxy lifecycle lanes; `WorkerPool`
  and `KeyedWorkerPool` use the complete `SupervisorSends` product;
- named transparent service products: `Deadline`, `OneShot`, `Periodic`, and
  `ReceiveTimeout` pair inner sends with schedules; `Watch` and
  `TerminationMonitor` pair inner sends with observation; `Supervisor` pairs
  inner sends with child lifecycle; `BackoffSupervisor` adds schedules around
  that product; `ShutdownCoordinator` pairs inner sends with child shutdown;
- transparent wrappers with no corresponding sends layer: `Stash` preserves
  both event and sends exactly; `Guardian`, `StopOnShutdown`, and
  `FinalizeOnShutdown` add an outer `EventLayer<ShutdownRequested, _>` while
  preserving the inner sends type exactly.

Aliases (`Link`, `Reaper`, `LifecyclePublisher`, `TreeShutdown`) introduce no
additional interpreter product. `PoolKernel` and `PoolCore` are private pool
implementation layers, not extra public runtime capabilities.

This complete audit exposes an unresolved structural-ingress blocker. The
upstream law says wrapping maps every inner destination through exactly one
`Inside` step. The named products can let Bombay perform that mapping because
their `behavior` field records the wrapper boundary. In contrast,
`Guardian<B>`, `StopOnShutdown<B>`, and `FinalizeOnShutdown<B>` change
`B::Event` to `EventLayer<ShutdownRequested, B::Event>` but return `B::Sends`
unchanged. For example, `StopOnShutdown<Deadline<X>>` emits a
`ScheduleAt` containing `Ingress<TimerElapsed, Here>` even though the deadline
owner is now at `Inside<Here>`. Neither the request value nor its sends product
retains the lost outer depth. Calling `ingress.event::<FinalEvent>` therefore
does not compile; ignoring the ingress and inferring any matching path would
reintroduce payload search and becomes ambiguous when two layers own the same
fact type.

E9 is blocked on Behavior Actors retaining this boundary structurally. A valid
upstream representation may lift every preserved request ingress, or retain a
transparent sends-layer marker that Bombay can traverse; the exact encoding is
Behavior-owned. Bombay must not guess paths from the final event type, add
template-specific runtime exceptions, or silently treat the preserved sends
product as if it belonged at `Here`. The partial `Path` migration in Bombay is
not implementation authority and must not proceed until this contract is
resolved and proven by a nested runtime-facing test such as
`StopOnShutdown<Deadline<Deadline<X>>>` with equal timer payload types.

### Dependency refresh — 2026-08-18 (structural effect interpretation)

Behavior / Actors commit
`011eba1` resolves the preceding blocker. Every event-extending wrapper now
retains the matching effect boundary through `SendLayer`; `NoSends` is the
owned-effect identity. `SendsFor<Event>` proves event/effect compatibility,
and `InterpretSends<Interpreter, RootEvent, Path>` traverses inner effects at
`Inside<Path>` while preserving the absolute root event. The upstream nested
`StopOnShutdown<Deadline<Deadline<_>>>` proof interprets two identical
`ScheduleAt` request types at distinct paths and returns each `TimerElapsed` to
its exact owner. Every public multi-lane actor product owns its structural
`InterpretSends` implementation, so Bombay must delete its duplicate product
walkers and positional `ProductError` tree.

The feature-specific neighboring audit was repeated. Address remains 0.2.0 at
`7df3bedc5f3177ddbdb617cefe4b6ffcd60ecda3`; Communication remains locked
0.1.2 with checksum
`fc3d06aaf88ef9fe5392506d13e208c2141e6978563e1b802b97b489b1a071e2`;
Observe remains locked 0.1.1 with checksum
`7017c773ae142628b6a244cddad0db987cfba22df4c93730844a7d43cc657304`;
Timers remains locked 0.1.0 with checksum
`5b3fc2dab4a030fd1838d0492a1c62d3c43ece1355125e3fc628da3ca436352d`.
Their endpoint, mailbox, retained-fact, and timer-queue ownership is unchanged.

The exact working-tree patch over committed Behavior / Actors `ef2b87e` has
diff identity `15af096f3636ab5f31bd5b25be1d4a2442702a78` and makes
`InterpretSends`, `InterpretRequest`, and `InterpretDelivery` return concrete
`Send` futures. Structural traversal awaits each effect before beginning the
next. Bombay can therefore preserve its existing Communication 0.1.2 policy:
resolve the exact destination, await bounded mailbox admission, retain order,
and recover the exact rejected payload on closure. No `try_send`, blocking
thread, boxed staging queue, or changed pressure semantics is required. The
focused Behavior Actors library build passes against this patch; replace the
patch identity with its commit before feature completion.

Historical resolved blocker: committed dependency `011eba1` failed its own library build before
Bombay is type-checked. `routing/deduplicator.rs`, `routing/rate_limiter.rs`,
and `routing/sequencer.rs` still use `DeduplicatorSends`, `RateLimiterSends`,
and `SequencerSends`, while those product definitions are absent; `routing/mod.rs`
also still re-exports them. The pinned command
`cargo check -p bombay-rs --lib` reports 12 upstream E0422/E0425/E0432 errors.
This was resolved by commit `ef2b87e`, which consolidates those five identical
two-lane products as `DeliveryOutcomes<Target, Reply>`. Bombay must not
recreate the removed actor-owned products locally.

### E9 runtime migration blocker — parent report ingress

Bombay now delegates complete send-product traversal to Behavior, awaits
Communication delivery, and directly interprets timer, peer observation,
creation observation, child observation, shutdown, supervision, and worker
report leaves. The obsolete Bombay product walkers and the duplicate creation
and delivery adapter modules have been removed. The core library and the basic
example compile against the async Behavior working-tree patch.

The full supervision example exposes one remaining Behavior-owned contract.
`DynamicProxy<C>` emits `ReportWorkerStopped<A>` and
`ReportWorkerCreationResolved<A::Nonce>` to its runtime. Bombay must turn those
requests into `WorkerStopped<A>` and `WorkerCreationResolved<A::Nonce>` events
for the proxy's parent. The report request identifies the report payload but
does not retain an `Ingress` for the parent event. Creation also carries no
parent-report ingress. Consequently Bombay can only inject at a hard-coded
path. That works for an unwrapped `DynamicSupervisor`, but fails truthfully for
`StopOnShutdown<DynamicSupervisor<...>>`, whose parent-owned report lanes are
at `Inside<Here>`.

This is not recoverable from the child's request path: that path indexes the
proxy's own event/effect product, not the separately composed parent event.
Bombay must not search the parent event by payload type or add a
template-specific positional exception. Behavior Actors needs to carry the
parent report ingress as part of the created proxy/report capability (or an
equivalent statically typed return-to-parent capability). The required proof
is that a `DynamicProxy` created under
`StopOnShutdown<DynamicSupervisor<...>>` reports both worker facts into the
inner supervisor lanes. E9 remains active and implementation-blocked on this
specific upstream contract; the timer, delivery, observation, creation, and
shutdown paths are no longer blockers.

### Dependency refresh — 2026-08-18 (typed proxy parent ingress)

E9 was re-verified against Behavior / Actors commit `643b9f1`. The committed
API resolves the parent-report blocker without weakening protocol identity:
`ProxyParentIngress<A, ParentPath>` carries correlated typed ingress for both
`WorkerStopped<A>` and `WorkerCreationResolved<A::Nonce>`;
`ReportWorkerStopped<A, ParentPath>` and
`ReportWorkerCreationResolved<N, ParentPath>` retain the appropriate ingress;
and `ProxyWithParent` / `DynamicSupervisorWithParent` preserve the parent path
while their public actor protocol remains stable. The upstream runtime contract
proves both reports at `Inside<Here>` under
`StopOnShutdown<DynamicSupervisorWithParent<...>>`.

The feature-specific neighboring audit was repeated against Bombay's exact
dependency selection. Behavior Actors 0.12.0 is patched to the committed local
checkout above. Address 0.2.0 is patched to local commit `7df3bed` and continues
to own exact claim, resolution snapshots, lease-bound release, and address
reuse isolation. Communication 0.1.2 remains the registry release with checksum
`fc3d06aaf88ef9fe5392506d13e208c2141e6978563e1b802b97b489b1a071e2` and
continues to own the control lane, bounded asynchronous user admission,
payload-preserving rejection, affine admission closure, and two-lane receive
ordering. Observe 0.1.1 remains the registry release with checksum
`7017c773ae142628b6a244cddad0db987cfba22df4c93730844a7d43cc657304` and
continues to own retained observations plus the consuming unkeyed publisher
pair. Timers 0.1.0 remains the registry release with checksum
`5b3fc2dab4a030fd1838d0492a1c62d3c43ece1355125e3fc628da3ca436352d` and
continues to own branded generations, replacement, cancellation, due ordering,
and compaction. None of those primitives owns parent event ingress.

Bombay must now consume the ingress already carried by each proxy report. It
must not retain `LocalParentReports` path assumptions, rediscover the parent
lane, or introduce another report protocol. This clears the E9 implementation
blocker; E9 remains `active` until the adapter migration, examples, tests, and
repository-wide distillation audit pass.

### E9 implementation progress — typed parent reports

Bombay now interprets both proxy report requests generically over their
`ParentPath`. `LocalParentReports` consumes each request's retained `Ingress`
to construct the exact parent event and no longer assumes `Here`. The
supervision example selects
`DynamicSupervisorWithParent<..., Inside<Here>>`, passes a correspondingly
lifted `ProxyParentIngress`, and hosts the stable `Proxy<C>` protocol rather
than the path-indexed concrete proxy behavior. It compiles and runs to orderly
termination. Bombay's crate tests, compile-time topology tests, examples, and
formatting pass.

The repository-wide audit also removed obsolete Bombay-only product walkers,
duplicate creation/delivery adapter modules, and test fixtures coupled to that
deleted architecture. One separate workspace issue remains: Engine's
5,426-line `behavior_actors_scenarios.rs` mirrors the complete, now-changed
Behavior Actors public API and still names removed `EventInput`, `RouteInput`,
and old template generic signatures. This is not a Bombay runtime capability
failure. It must be distilled rather than mechanically preserved: Engine owns
generic `Behavior` execution laws, while Behavior Actors owns each template's
domain laws. Until that test-evidence manifest is reduced to Engine-owned
proofs or migrated to the current upstream API, the workspace-wide test gate
does not pass and E9 cannot become `feature-complete`.

That Engine evidence seam is now distilled. The 5,426-line scenario mirror and
its test entry point were deleted. `docs/driver-template-manifest.json` is now
a boundary assertion: Behavior Actors owns template-specific laws and Engine
mirrors zero templates. Engine retains its generic Driver law, property,
compile-fail, allocation, fuzz, and benchmark evidence, which applies to every
valid `Behavior` implementation without importing upstream template topology.
The repository audit also asserts that neither deleted mirror file can return.
The same audit removed six `D-INC-*` rows from Engine's Driver law and evidence
manifest. Address publication, generation replacement, and incarnation terminal
classification are Bombay runtime integration contracts; keeping them in the
Engine manifest falsely coupled the universal executor to deleted runtime test
modules. Bombay's runtime tests remain responsible for those invariants.
At that historical implementation slice, the full workspace test suite,
Engine's explicit ignored law-completion gate,
workspace Clippy over all targets, formatting check, and `git diff --check` all
passed, so E9 temporarily reached `feature-complete`. The later complete
application-intersection audit above supersedes that state: E9 is `active`
again and blocks M4 until the documented reference topology is executable.
