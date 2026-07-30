# ADR-0022 — Bounded stash: `Stashed<S>` composition, in-step replay

**Status:** Accepted (2026-07-30) — implemented under card #224.

## Context

A fixed-interface actor must handle every menu variant in every state or drop
it; multi-phase protocols hand-roll unbounded `Vec<Msg>` buffers per actor.
The literature names the need (conditional synchronization: Briot–Guerraoui–
Löhr 1998; De Koster et al. 2016 §4.2) and the family-correct shape for a
fixed-interface FIFO actor: a separate runtime-owned buffer whose replay
preserves arrival order (SALSA's partial-message mailbox), with guaranteed
delivery — overflow refuses loudly, silent drop is forbidden.

## Decision

- **Composition, not a trait accessor:** `Stashed<S: StashActor>` owns the
  buffer; user state composes in; `handle` receives `&mut Stash`. A stash
  cannot exist outside the wrapper (`Stash::bounded` is crate-private), so
  the v1 field-plus-accessor forget-trap (silent forever-defer) is
  unrepresentable. Compile-time enforcement over runtime discipline
  (ADR-0015 precedent).
- **In-step replay:** `Stashed::handle` runs the user handler, then drains
  the ready queue in the same `handle_message` step — ahead of the mailbox
  backlog, in stash-arrival order, under the step's strong `actor_ref`.
  Zero changes to `kind.rs`/the loops. Top-of-loop drains are unsound in the
  drain window (`WeakActorRef::upgrade` → `None` while queued messages still
  self-pin, ADR-0003/0010).
- **Two queues, snapshot unstash:** `stash()` → `held`; `unstash_all()`
  moves `held` → `ready`; replay pops `ready`. Mid-replay stashes wait for
  the next unstash.
- **Non-pinning:** the stash holds bare messages (no `self_sender`); a
  non-empty stash never keeps an actor alive (`Collected` reachable —
  ADR-0020, timer precedent ADR-0018).
- **Bounded with typed handback:** capacity from `stash_capacity(&args)`
  (constructor input, never `SpawnConfig`); overflow returns
  `StashFull<M>` carrying the message back (`TellError` precedent).

## Stop-fate table

Only `unstash_all` rescues a deferred message. Every terminal path drops the
remainder: Collected, in-band Stop, out-of-band stop(), kill(), panic/`Err`,
and a mid-batch stop/crash. Restart rebuilds from `Args` via
`Stashed::on_start` → structurally fresh stash each incarnation.

## Consequences

- A second trait surface (`StashActor`) mirrors `Actor`'s hooks; use-site
  type is `ActorRef<Stashed<S>>`.
- Stash access is `handle`-only — hook-triggered replay is impossible by
  design (follow-up card if ever needed). `Stashed<S>` as a *watcher*
  (`Watch`/`Supervisor` impls) is deferred to first concrete use; it is
  fully watchable/supervisable as a target today.
- Self-inflicted livelock (re-stash + unstash every replay) remains a user
  bug, documented on `unstash_all`.

Design record: docs/superpowers/specs/2026-07-30-224-bounded-stash-design.md.
