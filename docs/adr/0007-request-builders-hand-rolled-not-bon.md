# ADR-0007 — ask/tell request builders: hand-rolled `IntoFuture` structs, not `bon`

**Status:** Accepted (2026-07-18) — implemented under card #118 (absorbs #54)

## Context

Card #118's finalized surface makes the request object itself awaitable:

```rust
actor_ref.tell(msg).await                 // blocking send
actor_ref.tell(msg).try_send()            // non-blocking
actor_ref.tell(msg).timeout(d).await      // bounded blocking send
actor_ref.ask(req).await                  // default timeout (~5s)
actor_ref.ask(req).timeout(d).await       // explicit deadline
```

Card #54 (absorbed here) proposed the `bon` builder crate for this option
matrix. The matrix is small: tell has three terminal forms, ask has one
terminal form with three timeout modes (default / explicit / infinite opt-in).

## Decision

Hand-rolled request structs implementing `IntoFuture`, no `bon` dependency.

- **`bon` cannot express the surface.** A bon builder is always consumed by a
  finishing call — `.call()` for function builders, `.build()` for struct
  builders ([bon-rs.com/guide/overview](https://bon-rs.com/guide/overview)).
  The card's surface awaits the request *directly*, which requires the builder
  type itself to implement `IntoFuture`. With bon every send site becomes
  `tell(msg).call().await` — a mandatory extra token on the hottest ergonomic
  path of the whole public API, to save ~100 lines we'd write once.
- **The reference oracle hand-rolls the same shape.** kameo's
  `src/request/{tell,ask}.rs` are hand-written request structs with
  `IntoFuture` impls; no builder crate appears anywhere in its request layer.
- **Minimal typestate, only where a combination is invalid.**
  `tell(msg)` returns the no-timeout request (has `.try_send()` and
  `IntoFuture`); `.timeout(d)` moves to a timeout-bearing type that is
  `IntoFuture` only — `timeout + try_send` (meaningless: `try_send` is
  instantaneous) does not compile, instead of compiling and silently ignoring
  the timeout. Ask needs no typestate: every timeout mode awaits the same way,
  so a runtime field suffices. We do **not** carry kameo's `Tm`/`Tr` generic
  parameters — bombay has no remote tier or blocking-thread variants yet, so
  the extra generics have no second concrete use (YAGNI rule).
- **Dependency cost.** `bon` is a proc-macro dependency injected into the
  crate's most-used public path — compile time, supply-chain surface
  (cargo-audit/deny), and a third-party abstraction between the API and its
  docs. At this matrix size the trade buys nothing.

## Consequences

- The builders live in `bombay-core/src/request.rs`, one struct per state;
  adding a genuinely new send mode (e.g. the Zenoh remote tier, M1 exit #67)
  adds a struct or a method, not a macro configuration.
- If the option matrix ever grows combinatorial (remote × blocking × timeout ×
  priority…), revisit — that is the scale builder generators earn their keep
  at. Record the reversal against this ADR.
- #54 closes into #118 with this record as its outcome.
