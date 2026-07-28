# Card #245 — supervisor teardown: no orphaned children on any exit path

Status: approved (Joel, 2026-07-28). Companion ADR: `docs/adr/0019-supervisor-subtree-teardown.md`.

## Problem

A supervisor that leaves its message loop with reason `Normal` (or `Killed`,
or a peer-death propagation, or an own-handler panic) does not stop its
remaining supervised children. The only sweep site is the escalation arm
inside `dispatch_death` (`actor/kind.rs:480`); `run_lifecycle_supervised`
(`actor/spawn.rs:593`) goes from loop exit straight to `finish_actor`.
Children keep running until their own refs drop — silent orphans. The
job-queue example (#218) works around it with a manual `stop_child` loop plus
a `FinishStop` self-signal (wart log `docs/warts/218-example-warts.md` #4).

The `kill()` path is structurally worse: `Abortable` wraps the *whole*
lifecycle (`spawn.rs:283`), so an aborted supervisor runs no epilogue at all —
no code site exists where a sweep could go.

## Contract (research-grounded — see ADR-0019 for citations)

**A supervisor's exit, for any reason, structurally prevents its supervised
children from outliving it: each live child is signalled to stop, then
joined — its death confirmed via the already-installed watch edge — bounded
by its per-child grace, after which it is aborted. Never orphaned.**

Grounding, in one line each (full citations in ADR-0019):

- Structured concurrency: parent scope cannot complete before children;
  cancellation is signal-then-still-join (Featherweight X10 PPoPP 2010;
  Kotlin semantics peer-verified in ECOOP 2024; Leijen TyDe 2017).
- Actor-GC theory: an orphaned-but-running actor is only principled with
  automatic collection underneath (Kafura 1990 → LMCS 2022 → CRGC PLDI 2025);
  bombay has none, so orphaning is not an option.
- Empirical: unterminated actors / mis-sequenced teardown are a quantified
  bug class — IncorrectTermination 8.1 % of symptoms, ExplicitLifeCycle
  12.4 % of root causes (Bagherzadeh et al., OOPSLA 2020).
- Oracle: kameo v0.22.2 independently converged to an unconditional
  post-loop children-shutdown + wait-children-closed on every exit path,
  including kill (`spawn.rs:257-261` at tag v0.22.2).

The **bounded grace → abort** step and the **`Abortable` boundary placement**
are engineering choices — the literature is verified-silent on drain
timeouts and shutdown ordering (ADR-0019 labels them as such). The bound is
what keeps the sweep crash-only: Akka's unbounded join admits a wedged
subtree teardown in its own docs.

## Design

### 1. Abortable boundary move — supervised lifecycle only

`run_lifecycle_supervised` restructures so `Abortable` wraps only
`start_actor` + `run_supervised_message_loop`. `kill()` aborts that region;
the abort maps to reason `Killed` and falls through to the epilogue. The
plain and linked lifecycles keep the whole-lifecycle wrap — they have no
children, and their kill semantics do not change.

`PreparedActor::run_supervised` (`spawn.rs:268`) is where the wrap happens
today; the `Abortable` moves inside `run_lifecycle_supervised` around the
prologue+loop, and the function's tail becomes the always-runs epilogue.

### 2. Teardown sweep v2 — one non-skippable site, join semantics

After the loop exits with any `reason`, before `finish_actor`:

1. **Cancel** every live child (`drain_live_handles` — a child in a backoff
   window has no live handle and is already gone).
2. **Join**: drain the supervisor's own `link_rx` for the swept ids' death
   notices. The watch edges installed at spawn are the join signal; no new
   channel, no `JoinHandle` plumbing. Per-child bound: its `stop_grace`,
   graces running concurrently (sweep bounded by the max grace, not the sum).
3. **Abort** any child whose grace expired; its `MailboxReceiver::drop`
   (#195) still emits the death notice.

Non-swept notices read during the drain (peers, late duplicates) are
discarded — the loop is exiting; nothing routes them anymore.

This replaces `stop_surviving_children`'s blind `cancel → sleep(grace) →
abort` (which always burns the full grace and never confirms death). The
call inside `dispatch_death` is removed; escalation paths now `Break` out of
the loop and hit the same epilogue sweep. The #195 peer-death path — which
deliberately left children untouched — now sweeps too: contract change,
recorded in ADR-0019.

### 3. Kill semantics deltas (supervised actors only)

Abort drops the future's locals, so only what lives OUTSIDE the `Abortable`
region survives a kill. The design moves out exactly two things: `children`
and `pending_aborts` (borrowed into the loop, swept by the epilogue).
`state`, `watchers`, and `mailbox_rx` stay inside and drop on abort, which
gives the kill semantics for free:

- Kill skips `on_stop` automatically (state is gone); `finish_actor` needs
  no `Killed` branch and `Killed` never reaches it — the epilogue returns
  `RunResult::Killed` directly after the sweep.
- Watchers of a killed supervisor keep today's notice-on-drop machinery
  (#195, `MailboxReceiver::drop`). On kill the death announcement therefore
  precedes the child sweep (announce-then-sweep); on all graceful paths the
  sweep runs first (children-then-announce). Documented asymmetry — kill is
  brutal.
- `pending_aborts` entries (children already `stop_child`ed, cancelled,
  awaiting their deferred abort deadline) are aborted at sweep end — their
  grace is truncated to the sweep's duration at supervisor exit.
- Kill during a hung `on_stop` no longer instant-drops the hook (the
  registration's region has already completed); the existing
  `ON_STOP_NOTICE_GRACE` timeout abandons it. Bounded, crash-only
  preserved; the affected test's assertion changes with a comment citing
  this spec.
- `run_supervised` no longer wraps the lifecycle in `Abortable`; the
  registration is consumed inside `run_lifecycle_supervised`. Plain and
  linked lifecycles are untouched.

Join bound detail: post-abort death confirmation is itself bounded (reuse
`ON_STOP_NOTICE_GRACE`) — tokio abort is not preemptive, so a child stuck
in non-yielding code can never be collected; the sweep traces the missing
notice and proceeds rather than wedging the supervisor's exit.

### 4. Tests (TDD — failing first)

- **Lifecycle** (card bullet 2): supervisor with live children stops
  `Normal` → every child provably dead (watch notices observed / refs
  report dead) within the grace bound. Same asserted for `kill()`.
- **Escalation regression**: existing escalation-sweep coverage
  (`stop_surviving_children_cancels_and_aborts_live_ones`) reworked to the
  epilogue sweep.
- **Join, not sleep**: a child that stops promptly on cancel completes the
  sweep well before the grace (paused-clock test — no grace-length sleep).
- **Straggler**: a cancel-ignoring child is aborted at its grace bound.
- **Walking skeleton** (card bullet 3): job-queue drain drops the manual
  `stop_child` loop + `FinishStop` self-signal; `app_job_queue.rs` asserts
  workers are actually stopped after drain.

### 5. Docs

- ADR-0019 (contract + citations + engineering-choice labels).
- Wart-log row #4 annotated resolved-by #245.
- README: public-API behavior change (supervisor stop tears down children)
  — one salient-feature line.

## Out of scope

- Plain/linked lifecycle boundary moves (no children).
- Handing-off/re-parenting API (research: no variant permits outliving the
  scope; Orleans-style durable handoff is a nexus/Zenoh-storage concern, M3).
- #244 (rebuild observation hook), #228 (pool teardown consumer).
