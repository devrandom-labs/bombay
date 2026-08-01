# ADR-0027: Registry lookup is keyed on the handle type, not the actor type

Date: 2026-08-01 · Status: accepted · Card: #289 (wart of #280)

## Context

`Registry::lookup::<A: Actor>` was keyed on the **runtime** actor type. For a
caps actor (the ONE user trait since ADR-0026) the runtime type is the
internal adapter `caps::Shell<A>`, so resolving a caps actor by name forced
the adapter into user code:

```rust
registry.lookup::<caps::Shell<Dispatcher>>(DISPATCHER_NAME)
```

`Shell` is public only because the `caps::Handle<A> = ActorRef<Shell<A>>`
alias names it; users should never have to write it. The alias hides `Shell`
everywhere else (spawn returns a `Handle`, `Ctx::self_ref` returns one) —
lookup was the one hole, because its turbofish takes the *actor* type, a
position no alias can reach. Wart recorded in
`docs/warts/280-registry-lookup-shell.md`; the card offered two candidate
shapes and deferred the choice in-card.

## Decision

Key `lookup` on **the handle type the caller wants back**, through a sealed
trait with a single blanket impl:

```rust
pub trait Resolvable: sealed::Sealed + Sized {
    type Runtime: Actor;
    fn from_ref(actor_ref: ActorRef<Self::Runtime>) -> Self;
}
impl<A: Actor> Resolvable for ActorRef<A> { /* Runtime = A, identity */ }

pub fn lookup<R: Resolvable>(&self, name: &str) -> Result<Option<R>, WrongActorType>
```

Both surfaces now resolve through the one door, and the alias does the
Shell-hiding it already does everywhere else:

- caps: `registry.lookup::<caps::Handle<Dispatcher>>(name)`
- runtime (expert floor): `registry.lookup::<ActorRef<Probe>>(name)`

`register` is unchanged: it receives a handle *value*, so inference already
kept `Shell` out of user code there.

## Why this shape

- **One verb, no split surface.** The caps-side wrapper alternative
  (`lookup` for runtime actors, a second caps-only method) adds a verb and
  makes callers know which trait their type implements — against the #277
  distillation gate (fewer fns, less cognitive load).
- **Layer-clean.** `registry.rs` still never imports `caps`; the blanket
  impl over `ActorRef<A>` is all it knows. `Handle<A>` falls in for free
  because it *is* an `ActorRef`.
- **Coherence-proof.** A single uniform lookup generic over both actor
  traits directly is impossible (blanket impls over `actor::Actor` and
  `caps::Actor` overlap, E0119). Keying on the ref type sidesteps the
  overlap with one impl.
- **Open for the builder arc (#286).** A future handle type joins by
  implementing the sealed trait; the seam is the marker the card's second
  candidate asked for, in its minimal form.

## Alternatives rejected

- **caps-side wrapper delegating to `lookup::<Shell<A>>`** — second verb,
  split surface (above).
- **Annotated-binding inference only** (`let d: caps::Handle<Dispatcher> =
  registry.lookup(..)` compiled even under the old signature) — works only
  when the binding is annotated and both `Result`/`Option` layers are
  unwrapped in place; the idiomatic turbofish form stayed broken, and every
  existing call site used the turbofish.

## Consequences

- `lookup::<Probe>` call sites become `lookup::<ActorRef<Probe>>` — a
  breaking spelling change for the expert floor (internal tests and benches
  updated mechanically; pre-1.0, no external users).
- `WrongActorType` semantics are unchanged: same downcast, now against
  `WeakActorRef<R::Runtime>`.
- The wart file is removed; `examples/job_queue/main.rs` and
  `tests/app_job_queue.rs` resolve via `caps::Handle<Dispatcher>`.
