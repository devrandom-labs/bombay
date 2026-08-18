# Historical Bombay design decisions

This is a distilled historical record. It preserves decisions that still
constrain current work without retaining the former 3,400-line chronological
`open-design-ledger.md`. Exact campaign narratives remain recoverable from Git
history and must not be treated as current API guidance.

## Distilled foundations

- Bombay Behavior and Behavior Actors own the pure deterministic algebra,
  named semantic effect products, creation dispatch, and reusable actor policy
  templates.
- `bombay-engine::Driver` is the one universal direct-Behavior execution path.
  It does not own addresses, mailboxes, clocks, tasks, or runtime policy.
- `Environment<B>` and `ActiveEnvironment<B>` are the affine incarnation
  spine: prepare/activate, acquire events, apply complete actions, retire.
- `Incarnation` adds terminal classification and one affine retirement handoff
  around a Driver execution. It does not manufacture runtime capabilities.
- Address owns exact-generation endpoint claim, resolution, and retirement.
- Communication owns the two-lane mailbox, delivery, backpressure, closure,
  cancellation, and exact rejected-payload recovery.
- Observe owns exact-generation terminal fact publication and waiting.
- Timers owns generation-safe scheduling state; executor clock waiting remains
  an integration responsibility.
- Actor supervision, routing, worker pools, guardians, shutdown coordination,
  retries, leases, presence, and related policies are Behaviors, not Bombay
  runtime policy.

## Rejected designs

- the pre-Driver Bombay runtime, prepared Driver continuation, `RuntimeEffects`,
  and parallel actor execution paths;
- hand-written application `SendEffects`, positional product traversal, nested
  direct lane mutation, and erased `Any`/downcast routing;
- public runtime, mailbox, incarnation, registry, or Driver construction;
- `LocalActors`, `LocalProtocol`, or Bombay-owned “namespace” as an independent
  primitive around Address;
- activation/termination Tokio channels, a timer command task, and a runtime
  stop oneshot when primitive capability contracts can own those laws;
- deriving local hosting from every protocol type mentioned by a Behavior;
- application manifests with unused `outbound` and `provided` declarations;
- a giant dynamic runtime/capability trait or user-selected bundle of the
  standard local primitives;
- treating Entity's legacy directory/runtime/executor façade as the new Bombay
  integration API.

## Accepted user experience

The functional path is authoritative:

```rust,ignore
fn main() -> Result<(), bombay::RunError> {
    bombay::run(IoTSystem::new())
}
```

Users construct application Behaviors and Behavior Actors templates as normal
Rust values. The root value states topology and policy. A future
`#[bombay::main]` may remove only entry-point boilerplate after the functional
path is proven; it must not invent behavior, topology, or protocols.

## Verification history

The removed chronological ledger recorded successful component campaigns for
the direct Driver, incarnation, transactional activation, static named-send
interpretation, recursive creation/delivery, observation/timers/shutdown,
Guardian root launch, the basic example, and one-value child creation. Those
states were component evidence, not permission to skip fresh per-feature
cross-crate verification. Current normative state and blockers live only in
`open-design-ledger.md`.

The actor-system comparison and template inventory remain in:

- `actor-system-comparison.md`;
- `../bombay-behavior/docs/actor-catalogue.md`;
- `../bombay-behavior/docs/runtime-backed-actors.md`.

The current capability ownership and interface audit is
`runtime-capability-interfaces.md`.
