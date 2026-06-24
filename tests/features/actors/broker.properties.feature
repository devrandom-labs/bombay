# Phase 2 — laws for kameo_actors `Broker<M>` (actors/src/broker.rs), layered on
# tests/features/actors/broker.feature's examples.
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
# RESOLVED CONTRACT (broker.rs:187-191): topic matching is the `glob` crate with
# MatchOptions { case_sensitive: true, require_literal_separator: true,
# require_literal_leading_dot: false } — separator is '/', '*' spans '.'. Glob, not AMQP.

@actors @broker @phase2
Feature: Broker — laws over glob routing, fan-out multiplicity, and dead-actor pruning
  As a producer publishing to topics through a Broker actor
  I want routing, delivery multiplicity, and pruning to hold for ALL patterns, topics,
  and subscription multisets
  So that no pattern/topic shape silently violates the broker contract

  # ---------------------------------------------------------------------------
  # @property — universally-quantified laws
  # ---------------------------------------------------------------------------

  @property @sequence
  Scenario: A subscriber receives a publish iff its glob pattern matches the topic
    Given a running Broker with delivery strategy "Guaranteed"
    And a single subscriber S subscribed with any compilable glob pattern p
    When a message is published to any topic t
    Then S receives exactly 1 message if glob(p) matches t under the broker MatchOptions
    And S receives 0 messages otherwise
    # GEN: p ∈ compilable glob Patterns incl. boundaries {"", "*", "a", "a/*", "*/b",
    #      "a/*/b", "my-*", literal "a/b/c"}; t ∈ topic Strings incl. boundaries
    #      {"", "a", "a/b", "a/b/c", "my-topic.detail", "a/"} — pair each p with matching
    #      and non-matching t.
    # ORACLE: glob::Pattern::new(p).matches_with(t, broker MatchOptions) — the glob crate
    #         itself is the reference for the boolean routing decision.
    # Generalizes: broker.feature "An exact-literal pattern matches only its exact topic",
    #   "A single-segment glob star…", "A glob star does not cross the '/' separator",
    #   "A prefix glob matches a single trailing segment",
    #   "Topics use '/' as the only glob separator — '.' is an ordinary character",
    #   "A publish whose topic matches no pattern delivers nothing and does not error".

  @property @sequence
  Scenario: The delivery count equals the number of matching (subscriber, pattern) registrations
    Given a running Broker with delivery strategy "Guaranteed"
    And any multiset of (subscriber, pattern) subscriptions, duplicates allowed
    When a message is published to any topic t
    Then each subscriber receives exactly one message per registration whose pattern matches t
    And the total deliveries equal the count of registrations whose pattern matches t
    # GEN: a multiset of size n ∈ {0, 1, 2, 8, 64} over (subscriber_id ∈ {S0..S3},
    #      pattern ∈ {"a/*", "*/x", "a/x", "a/*", literal "topic"}) — MUST include the same
    #      (subscriber, pattern) pair repeated (dup under one pattern) and one subscriber
    #      under two distinct patterns; t chosen to match 0, 1, and ≥2 registrations.
    # ORACLE: count over the registration multiset of patterns matching t (no dedup by
    #         subscriber, no dedup by pattern) — the broker pushes unconditionally.
    # Generalizes: broker.feature "A topic matching two distinct patterns delivers to both",
    #   "One actor subscribed under two patterns is delivered once per matching pattern",
    #   "Subscribing the same actor twice under one pattern stores it twice".

  @property @lifecycle
  Scenario: A dead recipient is pruned iff the strategy surfaces ActorNotRunning to the handler
    Given a running Broker with any delivery strategy d
    And a subscriber S subscribed with pattern "t" whose actor is not running
    When a message is published to topic "t"
    Then S is removed from the subscription iff d delivers inline (Guaranteed, BestEffort,
      TimedDelivery) and surfaces ActorNotRunning, pruned synchronously after the loop
    And for Spawned and SpawnedWithTimeout S is removed asynchronously via a self-sent
      Unsubscribe carrying S's pattern and id
    And the Broker actor never panics for any d
    # GEN: d ∈ all 5 DeliveryStrategy variants incl. boundary timeouts {Duration::ZERO,
    #      50ms} for the timed/spawned-timeout variants; S always reports ActorNotRunning.
    # ORACLE: ActorNotRunning ⇒ prune; every other SendError (MailboxFull, Timeout,
    #         HandlerError) ⇒ keep — a boolean derived from the matched SendError variant.
    # Generalizes: broker.feature "A dead recipient is pruned on the next matching publish
    #   (Guaranteed)", "A dead recipient under Spawned delivery is pruned via a self-sent
    #   Unsubscribe", "A live recipient is never pruned by delivery".

  @property @boundary
  Scenario: Only ActorNotRunning prunes — a full or slow mailbox is never pruned
    Given a running Broker with delivery strategy "BestEffort" or "TimedDelivery"
    And a subscriber S subscribed with pattern "t" whose mailbox is full past any timeout
    When a message is published to topic "t"
    Then S does not receive the message
    And S remains subscribed for any timeout value
    And the Broker actor does not panic
    # GEN: strategy ∈ {BestEffort, TimedDelivery(τ)} with τ ∈ {Duration::ZERO, 1ms, 50ms};
    #      mailbox kept full for the whole publish.
    # ORACLE: pruned-variant set = {ActorNotRunning}; MailboxFull/Timeout ∉ set.
    # Generalizes: broker.feature "BestEffort skips a recipient whose mailbox is full…",
    #   "TimedDelivery bounds each recipient's wait…",
    #   "BestEffort skips a full mailbox without panicking the broker".

  @property @lifecycle
  Scenario: Unsubscribe(None) removes a subscriber from every pattern; Unsubscribe(Some p) only from p
    Given a running Broker with delivery strategy "Guaranteed"
    And a subscriber S subscribed under any set P of patterns
    When S unsubscribes with topic None
    Then S receives 0 messages for any subsequent publish to any topic
    And separately, when S instead unsubscribes from one pattern p in P, S still receives
      deliveries for publishes matching any other pattern in P but none matching only p
    # GEN: P ∈ non-empty pattern sets incl. boundaries {1 pattern, 2 patterns, the same
    #      pattern twice}; pick the unsubscribe target p ∈ P and a publish topic matching p
    #      only vs. matching another pattern.
    # ORACLE: a per-pattern set-of-subscribers model: None empties S from all sets,
    #         Some(p) removes S only from set[p]; an emptied pattern key is dropped.
    # Generalizes: broker.feature "Unsubscribe by topic removes the actor from only that
    #   pattern", "Unsubscribe with topic None removes the actor from every pattern",
    #   "Unsubscribing the last recipient of a pattern drops the pattern entry".

  # ---------------------------------------------------------------------------
  # @model — refinement / linearizability against a reference model
  # ---------------------------------------------------------------------------

  @model @linearizability
  Scenario: Concurrent publishes refine a per-registration delivery counter with no loss or duplication
    Given a running Broker with delivery strategy "Guaranteed"
    And any fixed set of (subscriber, pattern) registrations
    When N messages are published concurrently from P tasks to any topics
    Then for every subscriber the received count equals, per registration matching a published
      topic, the number of publishes to a matching topic — no loss, no duplication
    # GEN: P ∈ [2, 10]; N ∈ {1, 50, 100}; registrations incl. overlapping patterns
    #      ("a/*" and "*/x") and a duplicate registration; topics chosen so some match
    #      multiple patterns.
    # ORACLE: a per-(subscriber,pattern) integer counter incremented once per publish whose
    #         topic matches that pattern; the Broker serialises handling on its mailbox, so
    #         the observed multiset must equal the model counters exactly.
    # Generalizes: broker.feature "Concurrent publishes to one pattern deliver every message
    #   exactly once", "Concurrent publishes to overlapping patterns deliver to each match
    #   independently", "A subscribe concurrent with a publish is observed atomically".
