# ADR-0012: Restart accounting is two counters, not a sliding time window

**Status:** accepted · card #196 (restart & supervision, #120 slice 2)

## Context

A supervisor must decide, on each child death, whether to rebuild and — because
unbounded rebuilding of a permanently-broken child is a metastable failure
(retry amplification, Bronson et al. HotOS'21) — when to give up and escalate.

OTP's `supervisor` answers this with a **sliding window**: if more than `MaxR`
restarts occur within `MaxT` seconds, terminate. The window exists in OTP
because OTP has *neither* a backoff delay between restarts *nor* a success
signal — the window is the only thing distinguishing "5 crashes in a burst" from
"5 crashes spread over a year".

Three off-the-shelf mechanisms were evaluated for bombay:

- **`governor` (GCRA).** Models a steady-state *rate* and answers
  `Err(NotUntil { .. })` — backpressure, "permitted again at time *t*". A
  supervisor does not back-pressure; exceeding the limit is *terminal*
  (escalate), not "retry later". GCRA is also a smoothed leaky bucket, so
  "5 restarts / 1000 s" becomes burst-5-then-drip-one-per-200 s — a slow restart
  storm the give-up rule is meant to forbid. Wrong output type, wrong curve.
- **OTP's timestamp ring** (`SmallVec<[Instant; N]>`, prune older than `now - T`).
  Correct, but it re-introduces a second time mechanism that must be sized above
  `max_backoff` or the cap becomes unreachable, and it needs a paused-clock story
  of its own for deterministic tests.
- **Two integer counters.** Chosen — see below.

## Decision

`RestartTracker` carries **two `u32` counters and no time window**:

- `consecutive` — the fast trip, reset to 0 once an incarnation survives
  `reset_after` of healthy uptime. Answers *"did this incarnation work?"* Backoff
  is `min_backoff · 2^(consecutive-1)` capped at `max_backoff`; escalate at
  `consecutive > max_restarts`.
- `total` — the slow trip, **never reset**. Answers *"is this child worth having
  at all?"* Escalate at `total > max_total`.

Both increment with `checked_add`; a counter overflow *trips the limit* (returns
`GiveUp::Yes`) rather than wrapping or saturating — the arithmetic-safety rule
applied to a give-up path.

The window is redundant *given* backoff + a reset rule. `consecutive`'s reset
expresses "it recovered" directly, instead of inferring it from timestamps —
which is why OTP needs the window and bombay does not. The two counters answer
two genuinely different questions: a child that fails once every
`reset_after + ε` resets `consecutive` forever (so a single counter would let it
restart without bound), but `total` still trips. That slow-drip case is the
reason `total` exists and is tested (`slow_drip_exhausts_lifetime_budget`).

## Consequences

- No `governor`, no timestamp ring, no `[dependencies]` for restart accounting —
  the tracker is `restart.rs`, pure and synchronous, and mutation-tests clean.
- Time enters only through backoff and the reset comparison, both on
  `tokio::time::Instant`, so every restart test is deterministic under
  `start_paused`.
- The numeric defaults (`max_restarts = 5`, `max_total = 100`,
  `min_backoff = 100 ms`, `max_backoff = 30 s`, `reset_after = 60 s`) are
  unsourced starting points; `stop_grace = 5 s` is OTP's child-spec `shutdown`
  default, the one sourced value. They are expected to move once #199's DST work
  measures real restart distributions.
- The card's original checkbox wording `restart_frequency_bounded_then_escalates`
  ("N within T, Armstrong") describes the window bombay did **not** ship; it is
  the consecutive+lifetime model instead. This ADR is that record.
