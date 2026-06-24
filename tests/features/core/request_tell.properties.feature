# Phase 2: laws (∀ inputs) and model-checks over TellRequest, layered on request_tell.feature.
# Conventions (see docs/testing/properties.md):
#   * @property — ∀ inputs. invariant(SUT(inputs)). Wired with proptest.
#   * @model    — SUT ≡ reference model under any op sequence / interleaving.
#   * Each scenario ALSO carries one Phase-1 category tag (+ @timing if a clock is needed).
#   * # GEN: names the generator strategy AND the boundary values it must include.
#   * # ORACLE: names the reference model / inverse function.
#   * # Generalizes: the Phase-1 example scenario(s) this law subsumes.
#   * Facts only; grounded in src/request/tell.rs as read 2026-06. No step definitions.
#
# Grounding (src/request/tell.rs):
#   * tell carries no reply; returns Result<(), SendError<M>>.
#   * try_send → tx.try_send: Ok(()) iff capacity, else MailboxFull(msg) (tell.rs:169-181) — no wait.
#   * send_after spawns sleep-then-send returning JoinHandle; JoinHandle::abort cancels the
#     pending send before it fires (tell.rs:128-160).

@core @request_tell @phase2
Feature: TellRequest — laws over delivery exactly-once and delayed-send cancellation
  As a caller telling actors under generated capacities and delays
  I want every accepted message delivered once and every refused one not delivered
  So that no capacity or delay value silently loses or duplicates a message

  Background:
    Given a running actor whose handler records every integer it receives

  # ---------------------------------------------------------------------------
  # @property — universally-quantified laws
  # ---------------------------------------------------------------------------

  @property @boundary
  Scenario: try_send returns Ok iff there is capacity and MailboxFull otherwise, for any capacity
    Given a bounded mailbox of any capacity c whose actor is parked so it never drains
    And the mailbox already holds any k buffered messages with k in [0, c]
    When one more message is offered with "tell(n).try_send()"
    Then it returns Ok(()) iff k < c and SendError::MailboxFull(n) iff k == c, with no waiting
    # GEN: c ∈ boundary-biased usize {1, 2, 64, 1024}; k ∈ [0, c] including k = 0, c-1, c.
    #      Actor held in a never-returning handler so the slot count is deterministic.
    # ORACLE: the predicate k < c; try_send never blocks (tx.try_send, tell.rs:169-181).
    # Generalizes: request_tell.feature "try_send into a full bounded mailbox returns MailboxFull(msg)",
    #              "try_send never waits for capacity".

  # ---------------------------------------------------------------------------
  # @model — no loss / no duplication under concurrency; send_after lifecycle
  # ---------------------------------------------------------------------------

  @model @linearizability
  Scenario: every Ok try_send is recorded exactly once and every MailboxFull is never recorded
    Given a bounded mailbox of any capacity c whose handler records each integer it receives
    When N callers concurrently invoke "tell(n).try_send()" with distinct integers under a barrier
    Then every call that returned Ok(()) has its integer recorded exactly once
    And every call that returned MailboxFull has its integer never recorded
    And the recorded set equals exactly the set of Ok integers — no loss, no duplicate
    # GEN: N ∈ [2, 128] (include N > c); c ∈ {1, 8, 64}; integers distinct. Real overlap via
    #      tokio::spawn + Barrier (rule 8). Bounded(c) drains as the actor runs, so over time some
    #      offers succeed and some hit MailboxFull — the law is about which the return value claims.
    # ORACLE: partition offers by their own return value; recorded multiset == the Ok set as a SET
    #         (each exactly once). The accepted-then-lost and accept-twice bugs both fail this.
    # Generalizes: request_tell.feature "concurrent try_sends under a bounded capacity deliver every
    #              accepted message exactly once".

  @model @sequence
  Scenario: a bounded send under backpressure eventually delivers every distinct message exactly once
    Given a bounded mailbox of any capacity c whose handler records each integer after a short delay
    When N callers concurrently invoke "tell(n).send()" with distinct integers under a barrier
    Then once all sends complete, every integer is recorded exactly once with none lost or duplicated
    # GEN: N ∈ [1, 64] (include N >> c); c ∈ {1, 2, 8}. send() backpressures on a full bounded
    #      mailbox (tx.send, tell.rs:115-118) rather than dropping, so total delivery is preserved.
    # ORACLE: a multiset of the N distinct integers; the recorded multiset must equal it exactly.
    # Generalizes: request_tell.feature "concurrent bounded sends all eventually deliver as the actor drains".

  @model @lifecycle @timing
  Scenario: send_after either delivers once after the delay or, if aborted before it fires, never delivers
    Given a bounded mailbox of capacity 100 and a handler that records each integer
    When the caller invokes "tell(n).send_after(d)" and then either awaits or aborts the JoinHandle at any point
    Then if the handle is allowed to fire, n is delivered exactly once and the handle resolves Ok(())
    And if the handle is aborted before the delay elapses, n is never delivered and the await reports cancellation
    # GEN: d ∈ boundary-biased Duration {ZERO, 1ms, 50ms, 1s}; abort point ∈ {before fire, after fire,
    #      never}. Paused/auto-advance clock at wiring to make "before the delay elapses" deterministic.
    # ORACLE: a one-shot model — the spawned task fires once or is cancelled once; delivered-count ∈ {0, 1},
    #         and == 1 iff not aborted before fire. JoinHandle::abort cancels the pending send (tell.rs:128-160).
    # Generalizes: request_tell.feature "send_after returns an abortable JoinHandle that delivers after the delay",
    #              "aborting the send_after JoinHandle before the delay prevents delivery",
    #              "send_after with a zero delay sends on the next scheduler tick".
