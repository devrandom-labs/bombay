# Phase 2 — laws for bombay_actors `MessageQueue` (actors/src/message_queue.rs), layered on
# tests/features/actors/message_queue.feature's examples.
#
# Conventions (see docs/testing/properties.md):
#   * @property — ∀ inputs. invariant(SUT(inputs)). Wired with proptest.
#   * @model    — SUT ≡ reference model under any op sequence / interleaving.
#   * Each scenario ALSO carries one Phase-1 category tag (+ @timing if a clock is needed).
#   * # GEN: names the generator strategy AND the boundary values it must include.
#   * # ORACLE: names the reference model / inverse function.
#   * # Generalizes: the Phase-1 example scenario(s) this law subsumes.
#   * Facts only; a @bug-exposing property keeps its @bug:<file:line> and fails today.
#   * No step definitions — wiring is Phase 3.
#
# RESOLVED CONTRACT: Topic routing uses the `glob` crate (message_queue.rs:700-704) with
# MatchOptions { case_sensitive:true, require_literal_separator:true,
# require_literal_leading_dot:false } — separator '/', '*' spans '.'. Glob, NOT AMQP.

@actors @message_queue @phase2
Feature: MessageQueue — laws over exchange routing, headers matching, and glob-key validation
  As a producer publishing through a MessageQueue actor
  I want each exchange type's routing rule to hold for ALL keys, bindings, and header sets
  So that no routing-key or header shape silently violates the exchange contract

  Background:
    Given a running MessageQueue actor with delivery strategy "BestEffort"

  # ---------------------------------------------------------------------------
  # @property — universally-quantified laws, one per exchange type
  # ---------------------------------------------------------------------------

  @property @sequence
  Scenario: Direct exchange delivers to a bound queue iff the routing key equals its binding key
    Given a Direct exchange "x" with any set of (queue, binding-key) bindings
    When a message is published to "x" with any routing key r
    Then a bound queue receives the message iff its binding key equals r exactly
    And it receives one copy regardless of how many of its bindings also match (set-deduped per queue)
    # GEN: bindings ∈ size n ∈ {0, 1, 2, 8} over (queue ∈ {q0..q3}, key ∈ {"", "k", "a.b",
    #      "order.new"}); r ∈ the same key alphabet incl. boundaries {"", a key present, a
    #      key absent}.
    # ORACLE: { q : ∃ binding (q, k) with k == r }; target_queues is a HashSet, so a queue
    #         matched by several bindings is delivered once (set semantics).
    # Generalizes: message_queue.feature "Direct exchange routes only on exact routing-key
    #   match", "Default (empty-name) exchange auto-binds a queue to its own name".

  @property @sequence
  Scenario: Fanout exchange delivers to every bound queue for any routing key
    Given a Fanout exchange "x" with any set of bound queues
    When a message is published to "x" with any routing key r
    Then every bound queue receives exactly one copy, independent of r
    # GEN: bound-queue set size n ∈ {0, 1, 2, 16}; r ∈ {"", "anything", "a/b", "a.b.c"}.
    # ORACLE: the set of distinct bound queue names — routing key is ignored for Fanout.
    # Generalizes: message_queue.feature "Fanout exchange routes to every bound queue
    #   regardless of routing key".

  @property @sequence
  Scenario: Topic exchange delivers iff glob(binding key) matches the routing key
    Given a Topic exchange "x" with any set of (queue, compilable-glob-key) bindings
    When a message is published to "x" with any routing key r
    Then a bound queue receives the message iff one of its binding globs matches r under the MessageQueue MatchOptions (separator '/', '*' spans '.')
    # GEN: binding keys ∈ compilable globs incl. boundaries {"", "*", "log.*", "log/*",
    #      "a/*/b", literal "log.warn"}; r ∈ {"", "log.warn", "log.warn.detail", "log/x"}.
    # ORACLE: glob::Pattern::new(key).matches_with(r, MatchOptions) per binding — the glob
    #         crate itself is the reference. "log.*" matches "log.warn.detail" (glob, not AMQP).
    # Generalizes: message_queue.feature "Topic exchange routes a single-segment wildcard
    #   match", "Topic wildcard segment-boundary semantics (glob, not AMQP)".

  @property @sequence
  Scenario: Headers exchange delivers per its x-match all/any law over any header set
    Given a Headers exchange "x" and a queue bound with x-match and any argument map
    When a message is published to "x" with any header map h
    Then with x-match=all the queue receives the message iff every non-"x-" argument is present in h with an equal value
    And with x-match=any the queue receives it iff at least one non-"x-" argument is present in h with an equal value
    # GEN: argument map size ∈ {0, 1, 3}; header map h ∈ subsets/supersets/mismatches of the
    #      arguments incl. boundaries {empty h with HeadersRequired error path, exact match,
    #      one-off match, superset match}; x-match ∈ {all, any}.
    # ORACLE: all ⇒ args ⊆ h with equal values; any ⇒ args ∩ h non-empty with equal values;
    #         empty h ⇒ HeadersRequired error (no delivery).
    # Generalizes: message_queue.feature "Headers exchange with x-match=all requires every
    #   argument to match", "Headers exchange with x-match=any matches when at least one
    #   argument matches", "Publishing to a Headers exchange without headers is rejected".

  @property @boundary @bug:actors/src/message_queue.rs:591
  Scenario: Any non-compilable Topic binding key is rejected at bind time
    Given a Topic exchange "x" and a declared queue "q"
    When "q" is bound to "x" with any routing key that is not a compilable glob
    Then the bind fails with "InvalidRoutingKey"
    # FIXED (:591, card #79): QueueBind validates Topic routing keys (mirroring the Headers
    # x-match check) and returns AmqpError::InvalidRoutingKey for any key glob::Pattern::new
    # rejects.
    # GEN: keys ∈ non-compilable globs incl. boundaries {"[unclosed", "[a-", "[", "a[b"};
    #      oracle compilability via glob::Pattern::new(key).is_err().
    # ORACLE: glob::Pattern::new(key).is_err() ⇒ bind must Err(InvalidRoutingKey).
    # Generalizes: message_queue.feature "A malformed Topic routing key is rejected at
    #   bind time" (@bug:591).

  @property @boundary @bug:actors/src/message_queue.rs:707
  Scenario: Refusing every non-compilable Topic key keeps the actor publish-safe
    Given a Topic exchange "x" and a declared queue "q"
    When "q" is bound to "x" with any routing key that is not a compilable glob
    Then the MessageQueue actor does not panic and publishing to "x" never errors
    # FIXED (:707, card #79): bind-time validation (:591) means a non-compilable key can no
    # longer reach the store through the public API — there is no back door to plant one — so
    # the end-to-end law is: every such bind is refused and a subsequent publish neither
    # errors nor panics the run-loop. The pure publish-side defence (a malformed key already
    # in the store yields AmqpError::InvalidRoutingKey, never a panic, because the publish
    # path compiles globs through topic_matches instead of unwrap) is proven over this exact
    # GEN set by the in-file #[cfg(test)] unit tests in message_queue.rs.
    # GEN: binding keys ∈ non-compilable globs {"[unclosed", "[a-", "[", "a[b"}; r ∈ any
    #      routing key incl. {"", "log.warn", "log.warn.detail"}.
    # ORACLE: glob::Pattern::new(key).is_err() ⇒ bind Err(InvalidRoutingKey); the actor stays
    #         alive and every later publish returns Ok.
    # Generalizes: message_queue.feature "A refused malformed Topic key never panics the actor
    #   when publishing" (@bug:707).

  @property @lifecycle
  Scenario: BasicConsume is idempotent per (queue, type, actor_id) for any repeat count
    Given a declared queue "q" and a consumer with a fixed ActorId
    When that consumer is attached to "q" any number of times k
    And a message of its type is published so "q" is selected
    Then "q" holds exactly one registration for that ActorId and the consumer receives one copy
    # GEN: k ∈ {1, 2, 3, 8}; interleaved with attaching OTHER distinct ActorIds n ∈ {0, 1, 3}
    #      to the same (queue, type) bucket.
    # ORACLE: a HashSet<ActorId> per (queue, TypeId) — push happens iff the id is absent
    #         (message_queue.rs:761), so the registration count == distinct ActorIds and is
    #         independent of k. Dedup-by-id, NOT no-dedup (broker/message_bus) or
    #         overwrite (pubsub).
    # Generalizes: message_queue.feature "Re-consuming the same queue with the same actor
    #   registers it once (dedup-by-id)".

  @property @lifecycle
  Scenario: A consumer is pruned iff delivery surfaces ActorNotRunning, never on full/slow
    Given a declared bound queue "q" with one consumer and any delivery strategy
    When a message is published and the consumer's mailbox is full, slow, or its actor is stopped
    Then the consumer is removed from "q" iff the strategy surfaced ActorNotRunning
    And a MailboxFull (BestEffort) or a Timeout (TimedDelivery) never removes it
    # GEN: strategy ∈ {Guaranteed, BestEffort, TimedDelivery(d) with d ∈ {ZERO, 1ms, 50ms}};
    #      consumer state ∈ {live-and-full, live-and-slow, stopped}.
    # ORACLE: `to_cancel` receives the recipient ONLY in the `Err(ActorNotRunning(_))` arm of
    #         each strategy (message_queue.rs:434, :440, :449); MailboxFull/Timeout fall
    #         through without pushing. Matches broker/message_bus prune semantics.
    # NOTE: Spawned/SpawnedWithTimeout self-prune via a spawned BasicCancel on ActorNotRunning
    #       (message_queue.rs:457-467) — message_queue has NO pubsub-style :125 defect.
    # Generalizes: message_queue.feature "A consumer with a full mailbox is skipped but never
    #   pruned (BestEffort)", "A stopped consumer is pruned and does not block delivery to
    #   live consumers".

  # ---------------------------------------------------------------------------
  # @model — refinement / linearizability against a reference model
  # ---------------------------------------------------------------------------

  @model @linearizability
  Scenario: Concurrent publishes to a fanout exchange refine a per-queue delivery counter
    Given a Fanout exchange "x" with any fixed set of bound queues, each with a consumer
    When N messages are published concurrently from P tasks to "x"
    Then each bound queue's consumer receives exactly N messages — no loss, no duplication
    # GEN: P ∈ [2, 10]; N ∈ {1, 50, 100}; bound-queue set size ∈ {1, 2, 8}.
    # ORACLE: a per-queue integer counter incremented once per publish; MessageQueue
    #         serialises handling on its mailbox, so the observed per-queue count == N.
    # Generalizes: message_queue.feature "Concurrent publishes to a fanout exchange deliver
    #   every message", "A binding added concurrently with a publish is observed atomically".
