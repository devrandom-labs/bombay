# ADR-0011: Watch capability guarded by a runtime `Result`, not a typestate handle

**Status:** accepted · card #195 (death-watch, #120 slice 1) · review follow-up

## Context

Only actors spawned via `spawn_linked` own a link channel and can `watch`/`link`.
A `Watch` actor mistakenly spawned via the plain `spawn` path has
`link_tx = None`; its `watch`/`link` calls return `Err(ActorNotLinked)`.

The original justification — "stable Rust has no negative bound to forbid it at
the type level" — is **wrong as stated**. No negative bound is needed: a witness
type makes the mistake unrepresentable at compile time on stable Rust today:

```rust
/// Returned only by spawn_linked; carries a NON-optional link channel.
pub struct LinkedActorRef<A: Watch> {
    inner: ActorRef<A>,
    link_tx: LinkSender,        // always present — no Option, no Err path
}

impl<A: Watch> LinkedActorRef<A> {
    pub async fn watch<B: Actor>(&self, target: &ActorRef<B>) { .. } // infallible
    pub async fn link<B: Watch>(&self, peer: &LinkedActorRef<B>) { .. }
}
// plain `spawn` returns ActorRef<A>, which simply has no watch/link methods.
```

## Decision

Keep the runtime `Result` on `ActorRef<A: Watch>`. The typestate was evaluated
and rejected on cost, not possibility:

- **Handle bifurcation infects every consumer of `ActorRef`.** `Recipient`
  erasure (ADR-0004), the papaya registry (ADR-0009), `WeakActorRef`, and the
  drain-window ref minting (ADR-0010) would each need a second variant or a
  lossy downcast — and `link` taking `&LinkedActorRef<B>` forces the split into
  *callers'* signatures too.
- **#121 (identity-first `ActorId`) freezes the handle surface next.** Two
  handle types double that migration's surface.
- **The failure is a caller mistake with one obvious fix** (spawn with
  `spawn_linked`), surfaced as a typed, non-panicking, single-domain error at
  the first `watch` call — cheap to hit in any test that exercises watching.

## Consequences

- `watch`/`link` keep the `Result<(), ActorNotLinked>` signature; the error is
  terminal for that handle (respawn linked to fix).
- If a later card bifurcates handles anyway (e.g. #121 introduces role-typed
  refs), fold the watch capability into that split and delete `ActorNotLinked`.
- Doc comments must cite this ADR, not "no negative bounds" — the guard is a
  chosen trade-off, not a language limitation.
