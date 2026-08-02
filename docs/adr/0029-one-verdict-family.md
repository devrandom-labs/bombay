# ADR-0029: One verdict family — `Step<Ph, R>`; `Flow` is its handler corner

Date: 2026-08-02 · Status: accepted · Card: #297 (opened by ADR-0028's "open
doors") · Amends: ADR-0023 (spelling only), ADR-0028 (the `≅` becomes `=`)

## Context

Post-#290, every user-written reaction on the capability surface answers the
same question — "keep going, switch behavior, or stop?" — in one of THREE
dialects:

| dialect | spelling | where | stop reason |
|---|---|---|---|
| `Flow` | `Continue \| Stop` | `Actor::handle` (ADR-0023), `DeadlineHook`, `Admitted::Absorbed` | fixed `Normal` |
| `Step<Ph>` | `Stay \| Goto(Ph) \| Stop` | `DeadlinePolicy<Cx>` seats, `Overflow::Handled` | fixed `Normal` |
| `ControlFlow<ActorStopReason>` | `Continue(()) \| Break(reason)` | `WatchPolicy::on_link_died`, `LinkReact` | carried |

`Step<Never> ≅ Flow` is already law (ADR-0028), enforced by hand-written
adapters at the `Deadlined` and `Phased` hook boundaries. #295 (the machine
algebra: a capability is `Machine → Machine`) needs an actor to be an async
fold of ONE step shape over merged event sources — a composition law over
three verdict types needs adapters at every joint, which is exactly the
bespoke-ness this arc deletes.

## Decision

ONE enum, in `capability::verdict`, both parameters defaulted to the
plain-actor corner:

```rust
pub struct Normal;

pub enum Step<Ph = Never, R = Normal> {
    Continue,
    Goto(Ph),
    Stop(R),
}

pub type Flow = Step; // = Step<Never, Normal>, re-exported at `actor::Flow`
```

The three-corner mapping:

| corner | type | replaces | unconstructible |
|---|---|---|---|
| handler / `DeadlineHook` / `Admitted` | `Flow = Step<Never, Normal>` | `Flow` | `Goto` (phase `Never`), any reason but `Normal` |
| policy seats | `Step<Ph>` = `Step<Ph, Normal>` | `Step<Ph>` | any reason but `Normal` |
| watch (`WatchPolicy`, `LinkReact`) | `Step<Never, ActorStopReason>` | `ControlFlow<ActorStopReason>` | `Goto` |

- **`Normal` is a unit marker, not `ActorStopReason`.** ADR-0023's semantics
  are preserved verbatim: a handler stopping itself has exactly one honest
  reason, and with `R = Normal` no other reason is *spellable* —
  unrepresentable, not convention. The marker is discharged to
  `ActorStopReason::Normal` at the loop's single consumption point
  (`kind.rs::handle_message` and the deadline arm). Only the SPELLING of
  ADR-0023 changes: `Ok(Flow::Stop)` → `Ok(Flow::Stop(Normal))` — the
  handler now *names* the one reason it is allowed.
- **`Stay` is renamed `Continue`.** One word for "keep the current behavior"
  across the whole family; the `gen_statem` `keep_state` reading survives in
  the docs, not in a second variant name.
- **The watch corner carries the reason** exactly as `Break(reason)` did:
  `OtpPropagation` returns `Stop(LinkDied { .. })` for a linked abnormal
  death, `Continue` otherwise — byte-identical (#266 suites are the oracle).
- **`Copy` strategy: derived, so it follows the parameters.**
  `#[derive(Clone, Copy, …)]` bounds on `Ph`/`R`: `Never` and `Normal` are
  `Copy`, so every existing seat verdict stays `Copy`; only the watch corner
  (whose `ActorStopReason` boxes a nested reason) is move-only — precisely
  the corner that was already move-only under `ControlFlow`.
- **`Flow` survives as the corner's alias**, still exported at
  `actor::Flow` (with `Normal` beside it): the ADR-0023 name keeps meaning
  "the handler plane's verdict", README/examples keep their vocabulary, and
  the churn is the mechanical `Stop` payload, not a rename sweep. No alias
  for the watch corner: `Step<Never, ActorStopReason>` at its few impl
  sites spells out which corner the policy occupies.
- **Not folded: `kind.rs`'s internal `ControlFlow<ActorStopReason>`.** The
  run-loop's own helpers (`handle_message`, mailbox polls) break with
  `ControlFlow` — that is loop plumbing at the loop's own break sites, the
  literal purpose of the std type, not a policy verdict crossing a user
  boundary. ADR-0023 option D rejected `ControlFlow` for the *user* plane
  (a unit `Break` beside a reason-carrying `Break` invites the wrong
  generalization); that concern does not apply inside the loop, where every
  `Break` carries the terminal reason.

Sanity check against typed-`become` (Agha 1986: a response designates the
replacement behavior): `Continue` = become(same), `Goto(p)` = become
restricted to the declared phase menu, `Stop(r)` = become(⊥) with an exit
code. The family is `become` with the menu and the exit code as type
parameters — nothing in it is bespoke to a particular capability.

## Alternatives rejected

- **Keep the three dialects + adapters.** Status quo; #295's composition law
  would thread adapters at every joint. The arc exists to delete these.
- **Generalize on `core::ops::ControlFlow`.** No `Goto`; and ADR-0023 D
  already documents the wrong-generalization hazard at the user plane.
- **Uninhabited reason at the handler corner** (`R = Never`). Makes `Stop`
  itself unconstructible — deletes the handler's stop verb, not just the
  fabricated reasons. The corner needs exactly one inhabitant, hence a unit.
- **Payload-less `Stop` kept via a second enum.** That IS the `Flow` dialect;
  two enums is the problem statement.

## Consequences

- The `Step→Flow` adapter in `Deadlined`'s `DeadlineHook` impl (total match,
  `Goto(never) => match never {}`) is DELETED — the policy's
  `Step<Never, Normal>` *is* `Flow`, returned through. `NoTimeout`'s
  unreachable verdict respells `Stay` → `Continue`.
- `Phased::apply` survives — it was never a pure adapter: its `Goto` arm is
  the transition-commit point (D3/D4) and the phase-erasure from
  `Step<P::Phase>` to `Flow`. Its `Stay`/`Stop` arms become
  corner-to-corner identities.
- `WatchPolicy`/`LinkReact` respell `Break`/`Continue(())` as
  `Stop(reason)`/`Continue`; the `core::ops::ControlFlow` import disappears
  from the capability layer, the job-queue app, and every fixture.
- Exhaustive `Flow` matches at the loop gain a total `Goto(never) =>
  match never {}` arm (the one the deleted adapter carried).
- Delete ledger and per-site churn: recorded on card #297's PR (code-only
  lines, docs excluded — the #290 measuring discipline).
