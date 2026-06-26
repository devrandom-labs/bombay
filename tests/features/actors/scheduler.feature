# Scope: kameo_actors `Scheduler` (actors/src/scheduler.rs) — an actor that owns a
#        tokio JoinSet of background timer tasks. `SetTimeout<A,M>` fires a single
#        `tell(msg)` to a (weak) actor after a deadline; `SetInterval<A,T>` fires
#        `tell(msg.clone())` repeatedly at a period. Both handlers return a tokio
#        `AbortHandle`. The scheduler's `next()` drains finished tasks while it also
#        receives mailbox signals.
#
# Authoring rules (see message_queue.feature — the exemplar — for the full statement):
#   * Every Scenario carries exactly ONE cross-cutting tag:
#       @sequence | @lifecycle | @boundary | @linearizability
#   * Invariant-first Thens grounded in actors/src/scheduler.rs as read.
#   * Timing-dependent scenarios are tagged @timing; at wiring they require
#     tokio::time::pause + advance (a paused clock), NOT real sleeps, so the assertion
#     is deterministic. Where a scenario uses a paused clock this is stated.
#   * No step definitions here; steps are written in the wiring phase.
#
# Source facts that ground the Thens (actors/src/scheduler.rs):
#   * SetTimeout::new (:115) sets deadline = Instant::now() + duration — at
#     CONSTRUCTION of the message, explicitly "not when the Scheduler receives it".
#   * SetTimeout handler (:131) spawns a task: sleep_until(deadline); then
#     `if let Some(target) = actor_ref.upgrade() { _ = target.tell(msg).await }`.
#     A dropped target => no send, no panic. Returns AbortHandle.
#   * SetInterval::new (:157) builds tokio::time::interval(period) at construction;
#     the first tick fires immediately (tokio interval semantics).
#   * SetInterval handler (:185) loops: interval.tick().await; upgrade() else return;
#     tell(msg.clone()); on SendError::ActorNotRunning | ActorStopped return. So the
#     interval task self-terminates when the target is gone.
#   * start_delay (:166) re-bases the interval at Instant::now() + duration via
#     interval_at, preserving the period.
#   * set_missed_tick_behaviour (:172) sets MissedTickBehavior (Burst/Delay/Skip).

@actors @scheduler
Feature: Scheduler — one-shot timeouts and repeating intervals to weak actor refs
  As an actor needing time-driven messages
  I want to schedule a one-shot timeout or a repeating interval against another actor
  So that the message fires after the deadline / every period, stops cleanly when the
  target is gone, and can be cancelled via its abort handle

  Background:
    Given a running Scheduler actor
    And a target actor that records each message it receives with a timestamp

  # ---------------------------------------------------------------------------
  # @sequence — fire-once / fire-repeatedly protocol, abort handle return
  # ---------------------------------------------------------------------------

  @sequence @timing
  Scenario: SetTimeout fires the message exactly once after the deadline
    Given a SetTimeout scheduled for 100ms from now against the target
    When the paused clock is advanced past 100ms and held for several further ticks
    Then the target receives the message exactly once
    # Invariant: the spawned task does a single sleep_until then one tell; it does not
    # loop. Paused clock makes "exactly once, after the deadline" deterministic.

  @sequence @timing
  Scenario: SetTimeout does not fire before its deadline
    Given a SetTimeout scheduled for 100ms from now against the target
    When the paused clock is advanced by only 50ms
    Then the target has received 0 messages
    # Invariant: sleep_until(deadline) has not elapsed.

  @sequence
  Scenario: SetTimeout handler returns an AbortHandle for the spawned task
    Given a SetTimeout scheduled for 100ms from now against the target
    When the SetTimeout message is asked to the Scheduler
    Then the reply is a tokio AbortHandle referencing the spawned timer task
    # Invariant: SetTimeout::Reply = AbortHandle; handler returns tasks.spawn(..).

  @sequence @timing
  Scenario: SetInterval fires repeatedly at its period
    Given a SetInterval with period 100ms against the target
    When the paused clock is advanced by 500ms in 100ms steps
    Then the target receives the message once per elapsed period
    # NOTE @review-semantics: tokio interval fires its FIRST tick immediately, so the
    # count over a window depends on whether t=0 counts. Pin the exact expected count
    # against tokio's documented first-tick-immediate semantics at wiring; assert the
    # specific number then (do not assert a guessed count here).

  @sequence
  Scenario: SetInterval handler returns an AbortHandle for the repeating task
    Given a SetInterval with period 100ms against the target
    When the SetInterval message is asked to the Scheduler
    Then the reply is a tokio AbortHandle referencing the spawned interval task

  # ---------------------------------------------------------------------------
  # @lifecycle — deadline-at-construction, target-gone self-termination, abort
  # ---------------------------------------------------------------------------

  @lifecycle @timing
  Scenario: A SetTimeout deadline is measured from construction, not from receipt
    Given a SetTimeout scheduled for 100ms from now against the target
    And the Scheduler is kept busy so it receives the SetTimeout 80ms after construction
    When the paused clock is advanced so that 100ms total have elapsed since construction
    Then the message fires once at ~100ms after construction, not ~180ms
    # Invariant: deadline = Instant::now() + duration is captured in SetTimeout::new;
    # delivery latency to the Scheduler does not move the deadline.

  @lifecycle @timing
  Scenario: A SetInterval period is measured from construction, not from receipt
    Given a SetInterval with period 100ms against the target
    And construction happens 200ms before the Scheduler receives the message
    When the paused clock is advanced
    Then the interval's tick schedule is anchored to construction time
    # NOTE @review-semantics: tokio::time::interval(period) starts its clock at
    # construction; with MissedTickBehavior::Burst the catch-up ticks fire when the
    # task first runs. Pin the exact catch-up count under the chosen behaviour.

  @lifecycle @timing
  Scenario: SetInterval stops cleanly once the target actor is dropped
    Given a SetInterval with period 100ms against the target
    When the target actor is dropped (its strong refs released)
    And the paused clock is advanced by several further periods
    Then the interval task exits on the next tick without panicking or erroring
    And no further messages are attempted after the target is gone
    # Invariant: each tick calls actor_ref.upgrade(); on None the task returns.

  @lifecycle @timing
  Scenario: SetInterval stops when the target is stopped but not yet dropped
    Given a SetInterval with period 100ms against the target
    When the target actor is stopped while a strong ref to it still exists
    And the paused clock is advanced by another period
    Then the interval task returns on the SendError::ActorNotRunning or ActorStopped result
    And the target receives no further messages
    # Invariant: tell returns ActorNotRunning/ActorStopped on a stopped actor; the
    # handler matches both and returns.

  @lifecycle @timing
  Scenario: SetTimeout to an already-stopped target wakes, fails upgrade, and is silent
    Given the target actor has already been dropped before the deadline elapses
    And a SetTimeout scheduled for 100ms from now against that target
    When the paused clock is advanced past 100ms
    Then the timer task completes without delivering a message and without panicking
    # Invariant: upgrade() returns None, the `if let Some` arm is skipped, task ends.

  @lifecycle
  Scenario: Aborting a SetTimeout before its deadline prevents the message from firing
    Given a SetTimeout scheduled for 100ms from now against the target
    And its AbortHandle has been retained
    When the AbortHandle is aborted before the deadline elapses
    And time then advances past the original deadline
    Then the target never receives the message
    # Invariant: AbortHandle::abort cancels the JoinSet task before sleep_until resolves.

  @lifecycle
  Scenario: Aborting a SetInterval stops further firings
    Given a SetInterval with period 100ms against the target
    And its AbortHandle has been retained
    And the interval has already fired at least once
    When the AbortHandle is aborted
    And time then advances by several further periods
    Then the target receives no messages after the abort
    # Invariant: aborting the interval task ends its loop.

  # ---------------------------------------------------------------------------
  # @boundary — start_delay, missed-tick behaviour, zero/past deadlines
  # ---------------------------------------------------------------------------

  @boundary @timing
  Scenario: start_delay defers the first interval tick by the given delay
    Given a SetInterval with period 100ms and a start_delay of 250ms against the target
    When the paused clock is advanced by 200ms
    Then the target has received 0 messages
    When the paused clock is advanced past 250ms
    Then the target begins receiving messages
    # Invariant: start_delay rebases via interval_at(now + delay, period); first tick
    # is at the delayed instant, not immediately.

  @boundary @timing
  Scenario: A SetTimeout whose deadline is already in the past fires immediately
    Given a SetTimeout constructed with Duration::ZERO against the target
    When the Scheduler receives it and the paused clock is advanced minimally
    Then the target receives the message at the earliest opportunity
    # Invariant: deadline = now + ZERO is already elapsed, sleep_until returns at once.

  @boundary @timing
  Scenario: A SetInterval with a very short period delivers without skipping under Burst
    Given a SetInterval with period equal to one tokio time tick against the target
    And its MissedTickBehavior is set to Burst
    When the paused clock is advanced far in one jump
    Then the target receives one message per missed period (the ticks burst to catch up)
    # NOTE @review-semantics: Burst replays every missed tick back-to-back; pin the
    # exact count against the advance distance and tokio's Burst documentation.

  @boundary @timing
  Scenario: A SetInterval with MissedTickBehavior Delay does not replay missed ticks
    Given a SetInterval with period 100ms against the target
    And its MissedTickBehavior is set to Delay
    When the paused clock jumps forward by 500ms in a single advance
    Then the target receives a single catch-up message, not five
    # Invariant: Delay schedules the next tick one period after the late tick fires,
    # collapsing the missed ticks into one. Contrast with Burst above.
    # NOTE @review-semantics: confirm the exact post-jump count against tokio's Delay
    # documentation when wiring.

  @boundary @timing
  Scenario: A SetInterval with MissedTickBehavior Skip drops missed ticks and realigns
    Given a SetInterval with period 100ms against the target
    And its MissedTickBehavior is set to Skip
    When the paused clock jumps forward by 500ms in a single advance
    Then the target receives a single message and the next tick realigns to the period schedule
    # NOTE (scheduler.rs:172-173): set_missed_tick_behaviour forwards any tokio
    # MissedTickBehavior, including Skip — the third variant the Burst/Delay scenarios omit.
    # @review-semantics: Skip skips missed ticks AND realigns the next deadline to a multiple
    # of the period (unlike Delay, which offsets by one period from the late fire); pin the
    # exact post-jump count and the next deadline against tokio's Skip documentation at wiring.

  @boundary @timing
  Scenario: A SetTimeout with Duration::ZERO still returns a usable AbortHandle
    Given a SetTimeout constructed with Duration::ZERO against the target
    When the SetTimeout is asked to the Scheduler
    Then a tokio AbortHandle is returned even though the deadline is already past
    # Invariant: the reply is produced before/independent of the task firing.

  # ---------------------------------------------------------------------------
  # @linearizability — many concurrent timers on one Scheduler / one target
  # ---------------------------------------------------------------------------

  @linearizability @timing
  Scenario: Concurrent SetTimeouts on the same Scheduler all deliver to their targets
    Given 50 SetTimeout messages each scheduled for 100ms from now against the target
    When all 50 are asked to the Scheduler concurrently from multiple tasks
    And the paused clock is advanced past 100ms
    Then the target receives exactly 50 messages
    And no scheduled timeout is dropped or duplicated
    # Invariant: each SetTimeout spawns its own independent JoinSet task; the
    # Scheduler's next() drains finished tasks without cancelling pending ones.

  @linearizability @timing
  Scenario: A mix of SetTimeout and SetInterval on one Scheduler each fire on their own schedule
    Given a SetTimeout for 100ms and a SetInterval of 100ms against the target
    When both are asked to the Scheduler concurrently
    And the paused clock is advanced by 300ms in 100ms steps
    Then the timeout contributes exactly one message
    And the interval contributes one message per elapsed period
    And messages from the two schedules are never lost or attributed to the wrong source
    # NOTE @review-semantics: pin the exact interval count (first-tick-immediate) when
    # wiring; the invariant under test is independence of the two timer tasks.

  @linearizability @timing
  Scenario: Draining a finished timeout task does not disturb a still-running interval
    Given a SetTimeout for 100ms and a SetInterval of 100ms against the target
    When both are running and the timeout completes and is joined by the Scheduler
    And the paused clock is advanced by several further periods
    Then the interval continues to deliver on schedule after the timeout was drained
    # Invariant: Scheduler::next() join_next()'s only finished tasks; a completed
    # timeout being reaped must not affect the live interval task in the same JoinSet.
