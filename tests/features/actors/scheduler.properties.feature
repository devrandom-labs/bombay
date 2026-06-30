# Phase 2 — laws for bombay_actors `Scheduler` (actors/src/scheduler.rs), layered on
# tests/features/actors/scheduler.feature's examples.
#
# Conventions (see docs/testing/properties.md):
#   * @property — ∀ inputs. invariant(SUT(inputs)). Wired with proptest.
#   * @model    — SUT ≡ reference model under any op sequence / interleaving.
#   * Each scenario ALSO carries one Phase-1 category tag (+ @timing if a clock is needed).
#   * # GEN: names the generator strategy AND the boundary values it must include.
#   * # ORACLE: names the reference model / inverse function.
#   * # Generalizes: the Phase-1 example scenario(s) this law subsumes.
#   * Facts only. No step definitions — wiring is Phase 3.
#   * @timing scenarios require tokio::time::pause + advance (a PAUSED clock), not real
#     sleeps, so counts/instants are deterministic.
#
# FACTS (scheduler.rs): SetTimeout::new (:115) captures deadline = Instant::now()+duration
# at construction; the spawned task (:131) does ONE sleep_until then a single tell guarded by
# upgrade() — a gone target ⇒ no send, no panic. SetInterval loop (:185) ticks, upgrades
# (None ⇒ return), tells, and returns on ActorNotRunning|ActorStopped. start_delay (:166)
# rebases via interval_at(now+delay, period).

@actors @scheduler @phase2
Feature: Scheduler — laws over one-shot timeouts, periodic intervals, and clean self-termination
  As an actor needing time-driven messages
  I want fire-once, fire-k-times, abort, and target-gone termination to hold for ALL
  durations and periods
  So that no duration or period shape silently violates the timer contract

  Background:
    Given a running Scheduler actor
    And a target actor that records each message it receives with a timestamp

  # ---------------------------------------------------------------------------
  # @property — universally-quantified laws (paused clock)
  # ---------------------------------------------------------------------------

  @property @sequence @timing
  Scenario: SetTimeout fires exactly once at ~construction+d, and aborting before d means never
    Given a SetTimeout scheduled for any duration d from construction against the target
    When the paused clock is advanced past construction + d and held for further ticks
    Then the target receives the message exactly once, no earlier than d after construction
    And in the separate run where the AbortHandle is aborted before d elapses, the target
      never receives the message however far the clock is then advanced
    # GEN: d ∈ boundary-biased Duration {ZERO, 1 tick, 1ms, 100ms, 1s}; the spawned task does
    #      one sleep_until then a single tell (it never loops), so the count is exactly 1
    #      unless aborted. Abort point ∈ before d.
    # ORACLE: a one-shot model — fired_count == 1 at the first tick ≥ deadline; abort before
    #         the deadline ⇒ fired_count == 0. Deadline = construction-time + d (NOT receipt).
    # Generalizes: scheduler.feature "SetTimeout fires the message exactly once after the
    #   deadline", "SetTimeout does not fire before its deadline", "A SetTimeout deadline is
    #   measured from construction, not from receipt", "A SetTimeout whose deadline is already
    #   in the past fires immediately", "Aborting a SetTimeout before its deadline prevents the
    #   message from firing".

  @property @sequence @timing
  Scenario: SetInterval fires k times by start_delay + k*period under a paused clock
    Given a SetInterval of any period p, optionally rebased by any start_delay s, against the target
    When the paused clock is advanced to start + s + k*p for any k
    Then the number of messages the target has received equals k (the number of ticks whose
      instant has been reached), under MissedTickBehavior::Delay so missed ticks do not replay
    # GEN: p ∈ boundary-biased Duration {1 tick, 1ms, 100ms}; s ∈ {none, 0, 250ms}; k ∈
    #      {0, 1, 5}; advance in p-sized steps so each tick instant is reached exactly. Use
    #      Delay behaviour so the count is the number of reached tick instants, not a burst.
    # ORACLE: a tick-instant model — ticks fire at start + (i*p) for i in 0..; under Delay the
    #         reached-count by time T is the number of tick instants ≤ T. (With start_delay the
    #         first tick is at start+s, not immediate.)
    # Generalizes: scheduler.feature "SetInterval fires repeatedly at its period",
    #   "A SetInterval period is measured from construction, not from receipt",
    #   "start_delay defers the first interval tick by the given delay",
    #   "A SetInterval with MissedTickBehavior Delay does not replay missed ticks".

  @property @sequence
  Scenario: Both handlers return a usable AbortHandle for any duration or period
    Given any SetTimeout duration d or SetInterval period p
    When the message is asked to the Scheduler
    Then the reply is a tokio AbortHandle referencing the spawned task, returned independently
      of whether or when the task fires
    # GEN: d, p ∈ {Duration::ZERO, 1ms, 100ms}; for SetTimeout incl. an already-past deadline.
    # ORACLE: SetTimeout::Reply == SetInterval::Reply == AbortHandle; the handler returns
    #         tasks.spawn(..) before the task runs.
    # Generalizes: scheduler.feature "SetTimeout handler returns an AbortHandle…",
    #   "SetInterval handler returns an AbortHandle…",
    #   "A SetTimeout with Duration::ZERO still returns a usable AbortHandle".

  # ---------------------------------------------------------------------------
  # @model — clean termination when the target is gone, for any period
  # ---------------------------------------------------------------------------

  @model @lifecycle @timing
  Scenario: An interval to a stopped or dropped target terminates cleanly with no panic, for any period
    Given a SetInterval of any period p against the target
    And the target is either dropped (strong refs released) or stopped while a strong ref remains
    When the paused clock is advanced by several further periods
    Then the interval task exits on the next tick without panicking or erroring
    And no further messages are attempted after the target becomes unavailable
    # GEN: p ∈ {1 tick, 1ms, 100ms}; termination mode ∈ {dropped ⇒ upgrade() == None,
    #      stopped-but-referenced ⇒ tell returns ActorNotRunning|ActorStopped}; number of
    #      further periods advanced ∈ {1, 5}.
    # ORACLE: a 2-state model {Running, Terminated}; the task transitions to Terminated on the
    #         first tick after the target is unavailable (None upgrade OR Stopped/NotRunning
    #         tell) and emits nothing thereafter — no observable difference between the two
    #         termination modes beyond the first-tick exit.
    # Generalizes: scheduler.feature "SetInterval stops cleanly once the target actor is
    #   dropped", "SetInterval stops when the target is stopped but not yet dropped",
    #   "SetTimeout to an already-stopped target wakes, fails upgrade, and is silent".

  @model @linearizability @timing
  Scenario: Independent concurrent timers each deliver on their own schedule with no cross-talk
    Given any mix of SetTimeout and SetInterval messages on one Scheduler against the target
    When all are asked concurrently from multiple tasks and the paused clock is advanced
    Then each timeout contributes exactly one message and each interval contributes one message
      per reached tick instant
    And no scheduled message is dropped, duplicated, or attributed to the wrong timer
    # GEN: a multiset of timers — SetTimeout count ∈ {0, 1, 50}, SetInterval count ∈ {0, 1, 2}
    #      with periods ∈ {1 tick, 100ms}; advance the clock in period-sized steps.
    # ORACLE: per-timer independent models (one-shot for each SetTimeout, tick-instant counter
    #         for each SetInterval) summed; each SetTimeout/SetInterval spawns its own JoinSet
    #         task, and next() drains finished tasks without cancelling or disturbing pending
    #         ones, so the totals must equal the sum of the per-timer models.
    # Generalizes: scheduler.feature "Concurrent SetTimeouts on the same Scheduler all deliver
    #   to their targets", "A mix of SetTimeout and SetInterval each fire on their own
    #   schedule", "Draining a finished timeout task does not disturb a still-running interval".
