# Bombay composition cookbook

This cookbook targets the packages currently named `bombay` and
`bombay-framework`. The project intends to adopt the Bombay name; package
names in commands and source links below describe the current workspace, while
the ownership and outcome recipes are rename-independent.

Every recipe points to an executable example or focused test. The referenced
program is the recipe: this document does not maintain a second, drifting copy
of its code.

## Ownership at a glance

| Desired outcome | Owner |
|---|---|
| Turn an event into typed effects and a next state | Bombay Behavior |
| Preserve priority/control and bounded/user mailbox laws | Bombay Communication |
| Resolve a logical address to the live typed endpoint | Bombay Address |
| Publish and await one exact incarnation's terminal result | Bombay Observe |
| Schedule, replace, cancel, and reject stale keyed timers | Bombay Timers |
| Spawn, interpret effects, retain resources, and retire an incarnation | bombay runtime composition |
| Choose request correlation, retry, admission, or reporting policy | application or optional library |

## Typed heterogeneous effects

Use Behavior's `SendProduct` for multiple effect lanes. Give each emitted
protocol a semantic alias over `Own` or `Inner<Path>` and call typed
`SendAlgebra::send`. Do not reach through `.inner`/`.own`, define an
application send-algebra wrapper, or implement `RouteSends`/
`ObservesCreations`; bombay already interprets the recursive product.
Implement `DeliveryRouter<A, M>` only where application wiring must select the
real endpoint for message `M`.

## Pure fold and typed delivery

Use a Bombay Behavior `Handler` or `Behavior` to map an input to `Actions`.
For explicit initialization plus one ordinary user-message fold,
`Compose::from_fns` can produce a locally inferred `BehaviorFn` with arbitrary
typed sends, births, and errors. When a nominal type is required by `Births`,
`Proxy`, `Supervisor`, endpoints, or routers, use
`#[behavior::behavior(...)]` over inherent `init` and `receive` methods instead
of expanding closure types. Semantic wrappers coordinating wrapper-owned event
protocols remain explicit.
Use `System::spawn` once to install it, retain the affine `Handle` as lifecycle
authority, and clone its typed `ActorRef` for delivery. The behavior performs
no I/O and stores no runtime handle.

Facade applications may construct that same concrete system with
`local_system!(mailbox = ..., routes = ...)`. The macro expands directly to
`System::new`; it does not hide router ownership or add another spawn path.

Executable recipe: [`hello.rs`](../crates/bombay/examples/hello.rs).

```text
nix develop -c cargo run -p bombay-rs --example hello
```

The observable outcome is a typed message fold followed by
`TaskOutcome::Returned(Ok(RunExit::Stopped(Exit::Normal)))`. Behavior owns the
fold and stop decision; Communication owns mailbox admission; Address and
bombay jointly route to the live incarnation; bombay owns execution and
completion normalization.

## Typed reply-to and deadline

Put a typed `Recipient<Addr, Reply>` in the application request. The callee
emits an ordinary `Delivery` to that recipient. If the application needs a
deadline, compose Bombay Behavior's `Deadline`; a late reply remains an
ordinary explicit application event.

Executable recipe: [`reply_to_oracle.rs`](../crates/bombay/tests/reply_to_oracle.rs).

```text
nix develop -c cargo test -p bombay-rs --test reply_to_oracle
```

The four oracles cover a successful typed reply, deadline followed by a late
reply, observation installed before request delivery, and delivery failure to
a retired reply target. They deliberately define no framework request ID,
correlation ID, call registry, or late-reply policy. Applications that need
correlation put their chosen domain value in their own message, or plug in a
separate adapter above bombay.

## Children and transactional creation

Emit Bombay Behavior `Create` values from the behavior's birth lane. bombay
prepares the child, interprets initialization effects, registers the endpoint,
and commits the affine child lease only when all preceding work succeeds.

Executable recipes:

```text
nix develop -c cargo test -p bombay-rs --test creation_oracle
nix develop -c cargo test -p bombay-rs --test lifecycle_oracle parent_retains_created_child_handle_while_parent_is_live
```

The outcomes distinguish recoverable observed rejection from fatal unobserved
failure, prove rollback and nonce reuse, and prove that parent ownership—not an
address-table reference count—keeps the child alive.

## Timers and receive timeout

Compose `Deadline` for an absolute application deadline or `ReceiveTimeout`
for inactivity. Behavior owns the typed timer protocol and rearming decision;
Timers owns the keyed queue; bombay drives the queue and injects the event
into the exact live incarnation.

Executable recipes:

```text
nix develop -c cargo test -p bombay-rs --test lifecycle_oracle typed_behavior_timer_fires_through_the_incarnation
nix develop -c cargo test -p bombay-rs --test lifecycle_oracle successful_user_fold_replaces_the_live_receive_timeout_generation
nix develop -c cargo test -p bombay-rs --test lifecycle_oracle nested_timers_at_the_same_deadline_keep_distinct_identities
```

A receive timeout is rearmed only by a successful continuing user fold.
Service traffic is not receive activity, and stale timer generations are inert.

## Observation

Use Behavior's typed observation intent and let bombay connect it to
Observe's exact-generation terminal publication. Installing observation
before a delivery when causality matters is an application-level ordering
choice expressed by the ordered send algebra.

Executable recipes:

```text
nix develop -c cargo test -p bombay-rs --test lifecycle_oracle child_observation_reports_the_exact_spawned_generation
nix develop -c cargo test -p bombay-rs --test lifecycle_oracle watching_receives_the_exact_peers_normalized_outcome
nix develop -c cargo test -p bombay-rs --test lifecycle_oracle retained_completion_cannot_alias_a_replacement_incarnation
```

Observation does not retain peer liveness. A reused logical address cannot
alias the retained completion of an older registration generation.

## Supervision and restart

Compose Bombay Behavior's `Supervisor` and `Proxy`. Behavior owns restart
strategy, policy, budget, and stable topology. bombay executes the emitted
create, observe, route, and retirement effects without adding supervision
policy.

Executable recipes:

```text
nix develop -c cargo test -p bombay-framework --example local_runtime
nix develop -c cargo test -p bombay-rs --test lifecycle_oracle supervision_escalation_retires_and_releases_the_complete_tree
```

The reference application proves a failed worker is replaced while typed
delivery, timers, observation, and shutdown continue to compose. The focused
oracle proves escalation retires and releases the whole tree.

## Coordinated shutdown

Wrap the root behavior with `StopOnShutdown` or a finalizing shutdown behavior.
The root `Handle` retains shutdown authority. bombay sends the typed priority
request, preserves final effects, requests descendant shutdown, awaits
transitive retirement, and publishes root completion last.

Executable recipes:

```text
nix develop -c cargo test -p bombay-rs --test lifecycle_oracle graceful_shutdown_preempts_user_backlog_and_interprets_final_effects
nix develop -c cargo test -p bombay-rs --test lifecycle_oracle root_shutdown_awaits_transitive_child_retirement
```

Dropping ordinary actor references is not shutdown policy.

## At-least-once job retry

Keep pending and outstanding jobs in application behavior state. On a typed
`WorkerStopped` event, return the outstanding job to the queue and mark its
slot unavailable. Dispatch the retry only after the corresponding typed
`WorkerCreationResolved` confirms that the proxy has installed a routable
replacement; forwards during installation are deliberately inert.

Executable recipe: [`job_queue.rs`](../crates/bombay-framework/examples/job_queue.rs).

The same executable also drives one immediate Behavior deadline through the
production timer interpreter and awaits its normal retirement. Typed routing,
children, supervision observation, timers, retry/backoff, admission refusal,
graceful draining, reply-to/reporting, deadline behavior, and coordinated
shutdown are therefore exercised in one application.

```text
nix develop -c cargo run -p bombay-framework --example job_queue
```

The example proves no-loss accounting across normal completion, terminal
failure, and a worker that fails once. Retry ownership stays in the
application; bombay has no retry manager or job domain.

The same executable now demonstrates graceful draining. The queue composes
Behavior's existing typed shutdown and timer event lanes around its existing
supervision protocol. Once draining begins, accepted pending and outstanding
jobs retain their ordinary accounting, later submissions are returned in the
final report as refused, and grace expiry moves each remaining accepted job to
the abandoned set exactly once. Completed and abandoned sets are disjoint;
bombay adds no drain state, command, task, or timer mechanism.

## Deliberate exclusions

The local runtime does not own registries beyond typed local endpoint
resolution, request/reply policy, correlation identity, dynamic message
erasure, persistence, streams, pub/sub, pools, retry/backoff, remote transport,
durable identity, or authentication. Add one above the runtime only when a
concrete consumer supplies its invariant and lifecycle owner. Do not place
runtime handles, channels, I/O, or clocks inside a pure Behavior merely to
imitate an API from another actor system.
