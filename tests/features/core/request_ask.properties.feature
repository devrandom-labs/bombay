# Phase 2: laws (∀ inputs) and model-checks over AskRequest, layered on request_ask.feature.
# Conventions (see docs/testing/properties.md):
#   * @property — ∀ inputs. invariant(SUT(inputs)). Wired with proptest.
#   * @model    — SUT ≡ reference model under any op sequence / interleaving.
#   * Each scenario ALSO carries one Phase-1 category tag (+ @timing if a clock is needed).
#   * # GEN: names the generator strategy AND the boundary values it must include.
#   * # ORACLE: names the reference model / inverse function.
#   * # Generalizes: the Phase-1 example scenario(s) this law subsumes.
#   * Facts only; grounded in src/request/ask.rs as read 2026-06. No step definitions.
#
# Grounding (src/request/ask.rs):
#   * send(): mailbox wait (send / send_timeout) THEN reply wait
#     (rx.await / tokio::time::timeout(reply_timeout, rx)); ask.rs:136-154.
#   * reply_timeout fires ⇒ SendError::Timeout(None) (message already enqueued); ask.rs:151-154.
#   * each ask uses its OWN oneshot channel (ask.rs:125, 317, 381, 421) ⇒ replies cannot cross.

@core @request_ask @phase2
Feature: AskRequest — laws over reply timing and concurrent reply isolation
  As a caller asking actors for replies under generated timeouts and concurrency
  I want the timeout decision and per-caller reply routing to hold for ALL inputs
  So that no delay value or caller count silently breaks request/reply

  Background:
    Given a running actor whose handler can be made to sleep for a given duration

  # ---------------------------------------------------------------------------
  # @property — universally-quantified laws
  # ---------------------------------------------------------------------------

  @property @boundary @timing
  Scenario: reply_timeout resolves Ok iff the handler delay is under the timeout, else Timeout(None)
    Given a bounded mailbox of capacity 100 with spare capacity so the mailbox wait is instant
    And a handler that sleeps for any delay d before replying Ok
    When the caller sends "ask(Sleep(d))" with any reply_timeout t
    Then the call returns Ok(reply) iff d < t, and SendError::Timeout(None) iff d >= t
    And the Timeout case is always None because the message was already enqueued
    # GEN: under tokio paused/auto-advancing time, d, t ∈ boundary-biased Duration
    #      {ZERO, 1ms, t-1, t, t+1, Duration::MAX}; include d == 0, t == 0 (immediate Timeout(None)),
    #      and t == MAX (effectively unbounded ⇒ Ok). Paused clock is REQUIRED to make d/t exact —
    #      note this at wiring (tokio::time::pause + advance), real-clock flake otherwise.
    # ORACLE: the boolean predicate d < t; Ok branch carries the handler's Ok, Err branch is Timeout(None).
    # Generalizes: request_ask.feature "reply_timeout expiring returns Timeout(None)…",
    #              "a reply that arrives just inside reply_timeout returns Ok",
    #              "a reply_timeout of zero fails immediately with Timeout(None)",
    #              "a Duration::MAX reply_timeout behaves as effectively unbounded".

  @property @boundary @timing
  Scenario: with no spare capacity, the mailbox_timeout is awaited first and hands the message back
    Given a bounded mailbox of capacity 1 occupied so it has no spare capacity
    And a handler that would sleep for any delay d
    When the caller sends "ask(Sleep(d))" with any mailbox_timeout tm and any reply_timeout tr
    Then the call returns SendError::Timeout(Some(Sleep(d))) for every tm, tr
    And the reply_timeout clock never starts because capacity was never acquired
    # GEN: tm, tr ∈ boundary-biased Duration {ZERO, 1ms, 50ms, Duration::MAX}; d arbitrary.
    #      send() awaits send_timeout(tm) BEFORE the reply wait (ask.rs:137-154); with the slot
    #      permanently busy the mailbox wait always elapses ⇒ Timeout(Some(msg)). Paused clock at wiring.
    # ORACLE: capacity-unavailable ⇒ always Timeout(Some(msg)); the message is returned (never enqueued).
    # Generalizes: request_ask.feature "mailbox_timeout expiring returns Timeout(Some(msg))…",
    #              "both timeouts set — mailbox capacity is awaited first, then the reply".

  # ---------------------------------------------------------------------------
  # @model — concurrent asks, reply isolation under any interleaving
  # ---------------------------------------------------------------------------

  @model @linearizability
  Scenario: N concurrent asks with distinct payloads each receive their own reply, no cross-talk
    Given a bounded mailbox of any capacity c and an echo handler returning the integer it received
    And any number N of callers each holding a distinct integer payload
    When all N callers concurrently send "ask(n)" with real overlap under a barrier
    Then every caller receives Ok(n) equal to the n it sent, exactly one reply each
    And no reply is delivered to the wrong caller and none is lost, for any N and any c
    # GEN: N ∈ [2, 64] (include N = 2 and a value > c); c ∈ {1, 4, 64}; payloads = distinct ints.
    #      Real overlap via tokio::spawn + Barrier (rule 8), not sequential-then-check.
    # ORACLE: an identity map n -> n; the SUT's received set of (caller, reply) pairs must equal
    #         {(caller_i, n_i)} — a bijection. Per-ask oneshot channel (ask.rs:125) forbids crossing.
    # Generalizes: request_ask.feature "concurrent asks under a bounded capacity are each answered
    #              exactly once with no cross-talk", "blocking_send from worker threads…".

  @model @linearizability @timing
  Scenario: among N concurrent asks, exactly the short-timeout ones fail with Timeout(None)
    Given a bounded mailbox of capacity 100 and a handler that sleeps a fixed delay d before replying
    When N callers concurrently send "ask(Sleep(d))", each with its own reply_timeout t_i, under a barrier
    Then each caller i receives Ok iff d < t_i, else SendError::Timeout(None), independently of the others
    # GEN: N ∈ [2, 16]; d fixed; t_i ∈ boundary-biased {d-1, d, d+1, Duration::MAX} so both
    #      outcomes occur in one run. Paused/auto-advance clock at wiring.
    # ORACLE: per-caller predicate d < t_i; the outcome of caller i is independent of caller j.
    # Generalizes: request_ask.feature "among N concurrent asks one with a short reply_timeout
    #              fails and the rest succeed".
