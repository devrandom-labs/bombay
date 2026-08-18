# Bombay module boundaries

This document maps source ownership. Semantic capability laws are in
[`runtime-capability-interfaces.md`](runtime-capability-interfaces.md); current
implementation blockers are in
[`open-design-ledger.md`](open-design-ledger.md).

## Workspace boundary

```text
bombay-behavior / bombay-behavior-actors
    deterministic Behavior algebra and actor policy templates
                |
                v
crates/bombay-engine
    Driver<B, E>
    Environment<B> -> ActiveEnvironment<B>
                |
                v
crates/bombay
    local capability composition and incarnation ownership
```

### `bombay-engine`

Engine owns only:

- the universal causal sequence from initialization through retirement;
- the affine prepared/live Environment port;
- complete action handoff;
- exact Driver completion and failure vocabulary.

Engine owns no executor, address, mailbox, observation, timer, task,
incarnation, topology, actor template, or runtime adapter. Its semantics and
verification are defined by `driver-law.md` and `driver-test-strategy.md`.

### `bombay`

Bombay owns:

- one concrete standard local Environment composition;
- actor incarnation task ownership and terminal classification;
- Address-space retention and exact lease ordering;
- Communication mailbox construction and event acquisition;
- Observe activation and termination publication;
- actor-local TimerQueue integration with the executor clock;
- static interpretation of named creation, delivery, observation, timer,
  report, and shutdown effect lanes;
- recursive child-task retirement;
- the functional `run(root)` boundary and later entry macros over it.

Bombay does not own actor policy, a second effect algebra, a registry, a
service locator, a public namespace, or a general lifecycle framework.

## Source disposition

The current worktree is mid-E5 migration. These modules express useful laws,
but some still contain transitional types and channels that are explicitly
scheduled for removal:

| Module | Intended responsibility | Transitional content to remove |
|---|---|---|
| `lib.rs` | minimal façade and curated re-exports | implementation details exposed for incomplete bounds |
| `application_runtime.rs` | functional root runner and concrete application capability product | manifest namespace terminology |
| `topology.rs` | closed static storage of locally hosted Address spaces | `Namespace` naming and wrapper products without independent laws |
| `local.rs` | prepared/live local Environment and public boundary reference | final ownership minimization only |
| `launch.rs` | construct one incarnation and place it on Tokio | final activation-publication minimization only |
| `interpret.rs` | statically dispatch complete named action lanes | redundant helper traits exposed by final distillation |
| `creation.rs` | concrete child installation leaf | no application-authored routing algebra |
| `delivery.rs` | resolve one endpoint and admit one typed event | wrapper dependency around AddressSpace |
| `observation.rs` | poll cancellable Observe facts into typed Behavior events | final shared fact-queue minimization only; no auxiliary tasks or channels |
| `time.rs` | adapt the actor-owned TimerQueue into typed events | shared queue view required by the split Environment/interpreter capabilities |
| `reports.rs` | route interpreter-originated typed reports | mutable terminal override plumbing |
| `incarnation.rs` | one Driver execution plus terminal classification | none beyond integration with corrected retirement owner |
| `outcome.rs` | exact terminal vocabulary | keep private unless users must handle it |
| `retirement.rs` | affine terminal handoff | keep minimal and private |
| `products.rs` | static product delegation | remove positional/nested error structure |

This table is a migration map, not a claim that the current worktree builds.
Communication UC1, Observe UO1, and the actor-owned TimerQueue integration are
complete. E5 is active for final repository-wide distillation.

## Static adapter boundary

The Engine Environment pair is the one behavior spine. The concrete local
Environment contains real primitive values, not Bombay wrappers around them.
Named effect leaves implement focused static traits such as send
interpretation and Behavior's existing birth installation. Products delegate
to leaves at compile time.

Do not introduce:

- `dyn Any`, downcasts, or erased effect envelopes;
- a dynamic capability registry;
- one giant trait with a method for every runtime facility;
- user-authored traversal of Bombay's capability products;
- a second Tokio-specific Driver loop.

## Public boundary

Ordinary users should name only the root Behavior composition, Behavior Actors
templates and policy values, pure `Recipient<P>` values, external boundary
`ActorRef<P>` values, `run`, and deliberate run errors. Driver, Environment,
incarnation, primitive capability construction, topology storage, task
ownership, and effect interpreters remain internal or advanced extension
surface.
