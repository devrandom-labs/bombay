# Phase 2 — laws for bombay_actors `MessageBus` (actors/src/message_bus.rs), layered on
# tests/features/actors/message_bus.feature's examples.
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
# FACTS (message_bus.rs): subscriptions keyed by TypeId(M); Register pushes unconditionally
# (no dedup); Publish<M> visits ONLY the TypeId::of::<M>() bucket and downcasts back to
# Recipient<M> (sound because the bucket key IS that TypeId); pruning on ActorNotRunning
# only (Spawned/SpawnedWithTimeout self-send Unregister::<M>).

@actors @message_bus @phase2
Feature: MessageBus — laws over TypeId routing isolation, fan-out multiplicity, and pruning
  As a producer publishing typed values through a MessageBus actor
  I want type-routing isolation and delivery multiplicity to hold for ALL multi-type
  registration sets
  So that no registration shape leaks a value across type boundaries

  # ---------------------------------------------------------------------------
  # @property — universally-quantified laws
  # ---------------------------------------------------------------------------

  @property @sequence
  Scenario: A publish of type M reaches exactly the recipients registered for TypeId(M) and no others
    Given a running MessageBus with delivery strategy "Guaranteed"
    And any set of (recipient, message-type) registrations across several distinct types
    When a value of type M is published
    Then every recipient registered for TypeId(M) receives the value once per such registration
    And no recipient registered only for a type other than M receives it
    # GEN: registrations ∈ multiset of size n ∈ {0, 1, 2, 8, 32} over (recipient_id ∈
    #      {R0..R3}, type ∈ {Ping, Pong, Pang}); MUST include the same recipient under two
    #      distinct types and the same (recipient, type) pair twice; publish each type.
    # ORACLE: a HashMap<TypeId, multiset<recipient_id>> model — expected recipients for a
    #         publish of M == the multiset stored under TypeId(M); all other buckets untouched.
    # Generalizes: message_bus.feature "Publishing a type delivers to every recipient
    #   registered for that type", "A publish is routed only to recipients of the published
    #   type", "One actor registered for two types receives each type independently",
    #   "Registering the same actor for the same type twice delivers twice",
    #   "A publish of one type never reaches recipients of an unrelated type",
    #   "Publishing a type with zero registered recipients is a graceful no-op".

  @property @lifecycle
  Scenario: A dead recipient is pruned iff the strategy surfaces ActorNotRunning, scoped to type M
    Given a running MessageBus with any delivery strategy d
    And a recipient A registered for type "Ping" whose actor is not running
    When a "Ping" message is published
    Then A is removed from the registrations for "Ping" iff d delivers inline (Guaranteed,
      BestEffort, TimedDelivery), pruned synchronously after the loop
    And for Spawned and SpawnedWithTimeout A is removed asynchronously via a self-sent
      Unregister::<Ping>::new(A)
    And A's registrations for any OTHER type are never touched by a Ping publish
    And the MessageBus actor never panics for any d
    # GEN: d ∈ all 5 DeliveryStrategy variants incl. boundary timeouts {Duration::ZERO,
    #      50ms}; A also registered for a second type "Pong" that must survive.
    # ORACLE: ActorNotRunning ⇒ prune A from bucket TypeId(Ping) only; every other SendError
    #         ⇒ keep; the Pong bucket is unaffected.
    # Generalizes: message_bus.feature "A dead recipient is pruned on the next publish of its
    #   type (Guaranteed)", "A dead recipient under Spawned delivery is pruned via a self-sent
    #   Unregister", "A live recipient is never pruned across repeated publishes",
    #   "Unregister for one type leaves the same actor's other-type registration intact".

  @property @boundary
  Scenario: Only ActorNotRunning prunes — a full or slow mailbox is never pruned
    Given a running MessageBus with delivery strategy "BestEffort" or "TimedDelivery"
    And a recipient A registered for "Ping" whose mailbox is full past any timeout
    When a "Ping" message is published
    Then A does not receive the message
    And A remains registered for any timeout value
    And the MessageBus actor does not panic
    # GEN: strategy ∈ {BestEffort, TimedDelivery(τ)} with τ ∈ {Duration::ZERO, 1ms, 50ms}.
    # ORACLE: pruned-variant set = {ActorNotRunning}; MailboxFull / Timeout ∉ set.
    # Generalizes: message_bus.feature "BestEffort skips a full mailbox without removing the
    #   recipient", "TimedDelivery bounds the per-recipient wait…",
    #   "BestEffort skips a full recipient without panicking or blocking others".

  # ---------------------------------------------------------------------------
  # @model — refinement / linearizability against a reference model
  # ---------------------------------------------------------------------------

  @model @linearizability
  Scenario: Concurrent multi-type publishes refine per-(type, recipient) counters with no cross-talk
    Given a running MessageBus with delivery strategy "Guaranteed"
    And any fixed set of registrations across distinct types
    When N values are published concurrently from P tasks across the registered types
    Then each recipient's received count of type M equals the number of M-publishes times its
      registration count for M — no loss, no duplication
    And no recipient ever receives a value of a type it is not registered for
    # GEN: P ∈ [2, 10]; N split across types {Ping, Pong}; registrations incl. a recipient
    #      registered for both types and a recipient registered twice for one type;
    #      per-type counts incl. boundary {0 publishes of one registered type}.
    # ORACLE: a HashMap<TypeId, multiset<recipient_id>> model with a per-(type, recipient)
    #         counter; the MessageBus serialises handling on its mailbox, so the observed
    #         per-type multiset must equal the model counters exactly.
    # Generalizes: message_bus.feature "Concurrent publishes of one type deliver every message
    #   exactly once", "Concurrent publishes of two types route each to its own recipients
    #   only", "A register concurrent with a publish is observed atomically".
