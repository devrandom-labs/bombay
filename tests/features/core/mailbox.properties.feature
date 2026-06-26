# EXEMPLAR PROPERTY FEATURE — sets the standard for every *.properties.feature.
#
# Phase 2: laws (∀ inputs) and model-checks, layered on mailbox.feature's examples.
# Conventions (see docs/testing/properties.md):
#   * @property — ∀ inputs. invariant(SUT(inputs)). Wired with proptest.
#   * @model    — SUT ≡ reference model under any op sequence / interleaving.
#   * Each scenario ALSO carries one Phase-1 category tag (+ @timing if a clock is needed).
#   * # GEN:    names the generator strategy AND the boundary values it must include.
#   * # ORACLE: names the reference model / inverse function.
#   * # Generalizes: the Phase-1 example scenario(s) this law subsumes.
#   * Facts only; a @bug-exposing property keeps its @bug:<file:line> and fails today.
#   * No step definitions — wiring is Phase 3.

@core @mailbox @phase2
Feature: MessageQueue mailbox — laws over capacity, ordering, and closure
  As the kameo runtime relying on the mailbox
  I want FIFO, backpressure, and closure to hold for ALL capacities and op sequences
  So that no input shape silently violates the queue contract

  # ---------------------------------------------------------------------------
  # @property — universally-quantified laws
  # ---------------------------------------------------------------------------

  @property @sequence
  Scenario: FIFO holds for any bounded capacity and message count
    Given a bounded mailbox of any capacity c
    And any sequence of n distinct tagged messages
    When all n messages are sent, interleaving receives to relieve backpressure
    Then the receiver observes the messages in exactly send order
    # GEN: c ∈ boundary-biased usize {1, 2, 7, 64, 1024, usize::MAX as cap}; n ∈ [0, 4*c]
    #      (include n = 0, 1, c-1, c, c+1); payloads = distinct monotonic tags.
    # ORACLE: a VecDeque<Tag> pushed on send, popped on recv — SUT order == oracle order.
    # Generalizes: mailbox.feature "Bounded backpressure…", "Signal ordering…".

  @property @boundary
  Scenario: try_send returns Full exactly when a bounded mailbox is at capacity
    Given a bounded mailbox of any capacity c
    And the mailbox already holds any k messages with k in [0, c]
    When one more message is offered with try_send
    Then it succeeds iff k < c and returns Full(message) iff k == c
    # GEN: c ∈ {1, 2, 64, 1024}; k ∈ [0, c] including k = 0, c-1, c.
    # Generalizes: mailbox.feature "try_send → Full on bounded…".

  @property @boundary
  Scenario: an unbounded mailbox never reports Full for any message count
    Given an unbounded mailbox
    And any message count n
    When all n messages are sent without receiving
    Then every send succeeds and none returns Full
    # GEN: n ∈ boundary-biased usize {0, 1, 1_000, 100_000}; cap = None always.
    # Generalizes: mailbox.feature "unbounded never blocks…".

  @property @sequence
  Scenario: push_front always drains before the channel for any two batches
    Given any batch F of messages pushed to the front
    And any batch C of messages sent through the channel
    When the receiver drains via any mix of recv / recv_many / try_recv
    Then all of F is observed before any of C, F in push order
    # GEN: |F|, |C| ∈ [0, 32] (include empty F, empty C); recv variant chosen per step.
    # ORACLE: front VecDeque concatenated with channel FIFO.
    # Generalizes: mailbox.feature "push_front drains before channel on restart".

  @property @lifecycle
  Scenario: after the receiver closes, every send returns the un-sent signal
    Given a mailbox (bounded or unbounded) holding any k buffered messages
    When the receiver is closed
    And any further send / try_send is attempted
    Then buffered messages still drain in order, then recv yields None
    And each post-close send returns the signal it failed to deliver
    # GEN: variant ∈ {bounded(c), unbounded}; k ∈ [0, c] or [0, 100]; close point random.
    # Generalizes: mailbox.feature "close → send returns Closed", "is_closed state machine".

  # ---------------------------------------------------------------------------
  # @model — refinement / linearizability against a reference model
  # ---------------------------------------------------------------------------

  @model @linearizability
  Scenario: concurrent senders preserve per-sender FIFO under any interleaving
    Given P concurrent senders, each sending k distinct tagged messages
    And one concurrent draining receiver
    When all senders and the receiver run with real overlap
    Then every message is received exactly once
    And for each sender, its messages appear in send order within the received stream
    And no global cross-sender order is required
    # GEN: P ∈ [2, 8]; k ∈ [1, 50]; tags encode (sender_id, seq).
    # ORACLE: a per-sender VecDeque FIFO model; check the received history is a valid
    #         interleaving (linearization) of the P sender FIFOs. Small cases via loom
    #         for exhaustive interleavings; larger via proptest + randomized scheduling.
    # Generalizes: mailbox.feature "concurrent send/recv ordering",
    #              "bounded backpressure under concurrent senders".

  @model @linearizability
  Scenario: strong-sender count refines a reference counter under concurrent clone/drop
    Given any interleaving of clone and drop operations on the mailbox sender
    When the operations run concurrently
    Then is_closed becomes true exactly when the last strong sender is dropped
    And no weak upgrade succeeds after that point
    # GEN: an op sequence over {clone, drop, downgrade, upgrade} of length [1, 64].
    # ORACLE: an integer strong-count model; closed ⇔ model reaches 0.
    # Generalizes: mailbox.feature "sender strong-count → is_closed when last drops",
    #              "weak upgrade None after close".
