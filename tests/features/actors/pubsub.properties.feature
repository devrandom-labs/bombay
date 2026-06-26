# Phase 2 — laws for kameo_actors `PubSub<M>` (actors/src/pubsub.rs), layered on
# tests/features/actors/pubsub.feature's examples.
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
# FACTS (pubsub.rs): subscribers keyed by ActorId (re-subscribe overwrites); publish()
# clones+delivers iff filter(&msg) is true; pruning on ActorNotRunning OR ActorStopped
# only; Spawned/SpawnedWithTimeout discard the spawned result so they never prune (the
# :125 leak).

@actors @pubsub @phase2
Feature: PubSub — laws over filter-gated delivery, id-keyed membership, and pruning
  As a producer broadcasting through a PubSub actor
  I want filter selection, subscriber-set membership, and pruning to hold for ALL filters,
  message sets, and subscribe sequences
  So that no filter or re-subscribe shape silently violates the broadcast contract

  # ---------------------------------------------------------------------------
  # @property — universally-quantified laws
  # ---------------------------------------------------------------------------

  @property @sequence
  Scenario: A subscriber receives a message iff its filter accepts it
    Given a running PubSub with delivery strategy "Guaranteed"
    And a single subscriber S subscribed with any filter predicate f
    When any sequence of messages is published
    Then S receives exactly the published messages for which f returns true, in publish order
    And S receives none of the messages for which f returns false
    And S is never pruned by a false filter, because no delivery is attempted
    # GEN: f ∈ {const-true (default), const-false, by-tag "keep", by-prefix "Topic:",
    #      parity-of-payload}; message sequence length n ∈ {0, 1, 2, 64} with payloads
    #      hitting every branch of f (all-accept, all-reject, mixed).
    # ORACLE: the filter predicate itself — expected received == messages.filter(f).
    # Generalizes: pubsub.feature "The default subscribe filter accepts every message",
    #   "A subscriber whose filter rejects a message receives nothing for it",
    #   "A rejecting filter does not affect other accepting subscribers",
    #   "SubscribeFilter predicates select the correct subset across two publishes",
    #   "A rejected-by-filter dead subscriber is NOT pruned…".

  @property @sequence
  Scenario: A published message reaches every subscriber whose filter accepts it
    Given a running PubSub with delivery strategy "Guaranteed"
    And any set of subscribers each with its own filter
    When a single message m is published
    Then exactly the subscribers whose filter accepts m receive one clone each
    And every other subscriber receives nothing
    # GEN: subscriber count k ∈ {0, 1, 3, 16} with a mix of accepting and rejecting filters
    #      for m, incl. the boundaries all-accept and all-reject.
    # ORACLE: the set { s : s.filter(m) } from the per-subscriber predicates.
    # Generalizes: pubsub.feature "A published message is cloned to every subscriber",
    #   "A rejecting filter does not affect other accepting subscribers".

  @property @boundary
  Scenario: Only ActorNotRunning or ActorStopped prunes a subscriber; full or slow never does
    Given a running PubSub with delivery strategy "BestEffort" or "TimedDelivery"
    And a subscriber S whose mailbox is full past any timeout, with an accepting filter
    When a message is published
    Then S does not receive the message
    And S remains in the subscriber set for any timeout value
    And the PubSub actor does not panic
    # GEN: strategy ∈ {BestEffort, TimedDelivery(τ)} with τ ∈ {Duration::ZERO, 1ms, 50ms}.
    # ORACLE: pruned-variant set = {ActorNotRunning, ActorStopped}; MailboxFull / Timeout /
    #         HandlerError ∉ set.
    # Generalizes: pubsub.feature "BestEffort skips a full mailbox without removing the
    #   subscriber", "TimedDelivery bounds the per-subscriber wait…",
    #   "A subscriber reporting ActorStopped is pruned on the next publish".

  @property @lifecycle @bug:actors/src/pubsub.rs:125
  Scenario: For any message, Spawned delivery prunes a stopped subscriber
    Given a running PubSub with delivery strategy "Spawned" or "SpawnedWithTimeout"
    And a subscriber S with any accepting filter whose actor is not running
    When any message accepted by S's filter is published
    Then S is eventually removed from the subscriber set
    # NOTE (pubsub.rs:125-131): today Spawned/SpawnedWithTimeout discard the spawned task
    # result, so ActorNotRunning is never observed and S is never pruned — this leak is a
    # DEFECT, so this property FAILS today for every message. Desired: the spawned task
    # self-sends an Unsubscribe (as broker/message_bus do) and prunes S.
    # GEN: strategy ∈ {Spawned, SpawnedWithTimeout(τ ∈ {Duration::ZERO, 50ms})}; filter ∈
    #      {const-true, by-tag}; published message always passes the filter.
    # ORACLE: ActorNotRunning ⇒ S absent from the subscriber set (same prune rule the inline
    #         strategies satisfy).
    # Generalizes: pubsub.feature "Spawned delivery prunes a dead subscriber" (@bug).

  # ---------------------------------------------------------------------------
  # @model — refinement / linearizability against a reference model
  # ---------------------------------------------------------------------------

  @model @lifecycle
  Scenario: Subscriber-set size equals the number of distinct ActorIds across any subscribe sequence
    Given a running PubSub with delivery strategy "Guaranteed"
    And any sequence of subscribe / subscribe_filter operations over a pool of actors
    When the sequence is applied
    Then the subscriber set holds exactly one entry per distinct ActorId
    And a re-subscribe of an already-present ActorId overwrites its filter, never duplicates it
    And a publish thereafter applies only the most-recently-installed filter for that id
    # GEN: op sequence length ∈ {0, 1, 2, 32} over (actor_id ∈ {A0..A3}, filter ∈
    #      {const-true, by-tag "old", by-tag "new"}); MUST include re-subscribing the same
    #      id twice with different filters and subscribing distinct ids.
    # ORACLE: a HashMap<ActorId, Filter> model — the subscriber set size == number of
    #         distinct ids inserted; the live filter == the last insert for that id.
    # Generalizes: pubsub.feature "Re-subscribing the same actor overwrites its prior filter".

  @model @linearizability
  Scenario: Concurrent publishes refine a per-subscriber accepted-message counter
    Given a running PubSub with delivery strategy "Guaranteed"
    And any fixed set of subscribers with per-subscriber filters
    When N messages are published concurrently from P tasks
    Then each subscriber's received count equals the number of published messages its filter
      accepts — no loss, no duplication
    And each filter is invoked exactly once per published message
    # GEN: P ∈ [2, 10]; N ∈ {1, 50, 100}; subscribers incl. one const-true, one rejecting,
    #      and one consulting a shared counter; messages hit accept and reject branches.
    # ORACLE: a per-subscriber integer counter incremented once per published message the
    #         filter accepts; PubSub serialises publish handling on its mailbox, so each
    #         filter is called once per message with no intra-handler interleaving.
    # Generalizes: pubsub.feature "Concurrent publishes deliver every message with no loss
    #   or duplication", "Concurrent publishes fan out to all subscribers with no cross-talk",
    #   "A filter reading shared state observes a consistent per-message view…".
