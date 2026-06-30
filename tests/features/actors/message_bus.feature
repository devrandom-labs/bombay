# Scope: bombay_actors `MessageBus` (actors/src/message_bus.rs) — a TYPE-based pub/sub
#        actor. Recipients register a Recipient<M>; the bus stores them keyed by the
#        TypeId of M. Publish(M) routes to every recipient registered for exactly that
#        TypeId, under the configured DeliveryStrategy. Registrations type-erase the
#        recipient via Box<dyn Any> and downcast back to Recipient<M> at publish.
#
# Authoring rules (mirrors tests/features/actors/message_queue.feature):
#   * Every Scenario carries exactly ONE cross-cutting tag:
#       @sequence | @lifecycle | @boundary | @linearizability
#   * Invariant-first; facts only; open questions are @review-semantics. No step definitions.
#
# FACTS pinned from source (message_bus.rs):
#   * subscriptions: HashMap<TypeId, Vec<Registration>>; Registration holds (actor_id,
#     Box<dyn Any> recipient).
#   * Register (:109-122) does `entry(TypeId::of::<M>()).or_default().push(...)` — NO dedupe:
#     registering the same actor for the same type twice stores two Registrations.
#   * Publish (:171-246) looks up only `TypeId::of::<M>()`; a recipient registered for a
#     DIFFERENT type is never delivered M. The downcast_ref::<Recipient<M>>() is sound
#     because the bucket key IS that TypeId.
#   * Pruning fires ONLY on SendError::ActorNotRunning. For Guaranteed / BestEffort /
#     TimedDelivery the actor_id is queued in to_remove and unsubscribed after the loop;
#     for Spawned / SpawnedWithTimeout the spawned task tells the bus Unregister::<M>::new(id).
#   * Unregister (:152-162) removes the actor's Registration for type M only; emptying the
#     bucket drops the TypeId key.
#   * Publish to a TypeId with no bucket is a graceful no-op (the `if let Some` is skipped).

@actors @message_bus
Feature: MessageBus — type-routed broadcast to registered recipients
  As a producer publishing typed values through a MessageBus actor
  I want each value routed to exactly the recipients registered for that value's type
  So that type-based fan-out holds under each delivery strategy and dead recipients are pruned

  # ---------------------------------------------------------------------------
  # @sequence — TypeId routing, multi-type registration, delivery strategies
  # ---------------------------------------------------------------------------

  @sequence
  Scenario: Publishing a type delivers to every recipient registered for that type
    Given a running MessageBus with delivery strategy "Guaranteed"
    And recipients A and B are registered for message type "Ping"
    When a "Ping" message is published
    Then recipient A receives exactly 1 "Ping" message
    And recipient B receives exactly 1 "Ping" message

  @sequence
  Scenario: A publish is routed only to recipients of the published type
    Given a running MessageBus with delivery strategy "Guaranteed"
    And recipient A is registered for message type "Ping"
    And recipient B is registered for message type "Pong"
    When a "Ping" message is published
    Then recipient A receives exactly 1 "Ping" message
    And recipient B receives 0 messages
    # FACT: lookup is by TypeId::of::<Ping>(); the Pong bucket is never visited.

  @sequence
  Scenario: One actor registered for two types receives each type independently
    Given a running MessageBus with delivery strategy "Guaranteed"
    And recipient A is registered for message type "Ping"
    And the same actor A is also registered for message type "Pong"
    When a "Ping" message is published
    And a "Pong" message is published
    Then recipient A receives exactly 1 "Ping" message
    And recipient A receives exactly 1 "Pong" message
    # FACT: the two registrations live in distinct TypeId buckets keyed by Ping and Pong.

  @sequence
  Scenario: Registering the same actor for the same type twice delivers twice
    Given a running MessageBus with delivery strategy "Guaranteed"
    And recipient A is registered for message type "Ping"
    And recipient A is registered for message type "Ping" again
    When a "Ping" message is published
    Then recipient A receives exactly 2 "Ping" messages
    # FACT: Register pushes unconditionally (no id check); the bucket holds two Registrations
    # for the same actor, so a single publish delivers two messages.

  @sequence
  Scenario: Guaranteed delivery blocks until each recipient accepts
    Given a running MessageBus with delivery strategy "Guaranteed"
    And recipient A is registered for message type "Ping"
    When a "Ping" message is published
    Then the publish completes only after A has accepted the message
    And recipient A receives exactly 1 "Ping" message

  @sequence @timing
  Scenario: BestEffort skips a full mailbox without removing the recipient
    Given a running MessageBus with delivery strategy "BestEffort"
    And recipient A whose mailbox is full is registered for message type "Ping"
    When a "Ping" message is published
    Then the publish completes without blocking
    And recipient A does not receive the message
    And recipient A remains registered
    # FACT: try_send yields MailboxFull, which is not a pruned variant.

  @sequence @timing
  Scenario: TimedDelivery bounds the per-recipient wait by the timeout
    Given a running MessageBus with delivery strategy "TimedDelivery" of 50 milliseconds
    And recipient A whose mailbox stays full past the timeout is registered for message type "Ping"
    When a "Ping" message is published
    Then the publish returns within approximately the timeout
    And recipient A does not receive the message
    And recipient A remains registered
    # FACT: mailbox_timeout yields Timeout, which is not a pruned variant.

  @sequence @timing
  Scenario: Spawned delivery returns immediately and retries a full mailbox indefinitely
    Given a running MessageBus with delivery strategy "Spawned"
    And recipient A whose mailbox is full but later drains is registered for message type "Ping"
    When a "Ping" message is published
    Then the publish returns without waiting for A to accept
    And once A's mailbox drains, A eventually receives exactly 1 "Ping" message
    # FACT: Spawned uses tokio::spawn + tell(...).send() with no timeout.

  @sequence @timing
  Scenario: SpawnedWithTimeout returns immediately and abandons delivery after the timeout
    Given a running MessageBus with delivery strategy "SpawnedWithTimeout" of 50 milliseconds
    And recipient A whose mailbox stays full past the timeout is registered for message type "Ping"
    When a "Ping" message is published
    Then the publish returns without waiting for A to accept
    And recipient A does not receive the message after the timeout elapses

  # ---------------------------------------------------------------------------
  # @lifecycle — unregister, dead-recipient pruning
  # ---------------------------------------------------------------------------

  @lifecycle
  Scenario: Unregister by actor id stops delivery of that type
    Given a running MessageBus with delivery strategy "Guaranteed"
    And recipient A is registered for message type "Ping"
    When recipient A is unregistered for message type "Ping"
    And a "Ping" message is published
    Then recipient A receives 0 messages

  @lifecycle
  Scenario: Unregister for one type leaves the same actor's other-type registration intact
    Given a running MessageBus with delivery strategy "Guaranteed"
    And recipient A is registered for message type "Ping"
    And the same actor A is also registered for message type "Pong"
    When recipient A is unregistered for message type "Ping"
    And a "Ping" message is published
    And a "Pong" message is published
    Then recipient A receives 0 "Ping" messages
    And recipient A receives exactly 1 "Pong" message

  @lifecycle
  Scenario: Unregistering the last recipient of a type drops the type bucket
    Given a running MessageBus with delivery strategy "Guaranteed"
    And recipient A is the only recipient registered for message type "Ping"
    When recipient A is unregistered for message type "Ping"
    Then message type "Ping" no longer has any registration
    # FACT: unsubscribe (:91-99) removes the TypeId key once its Vec is empty.

  @lifecycle
  Scenario: A dead recipient is pruned on the next publish of its type (Guaranteed)
    Given a running MessageBus with delivery strategy "Guaranteed"
    And recipient A is registered for message type "Ping"
    And recipient A has stopped so its actor is not running
    When a "Ping" message is published
    Then the publish completes without error
    And recipient A is removed from the registrations for "Ping"
    # FACT: ActorNotRunning queues the actor_id in to_remove, pruned after the loop.

  @lifecycle
  Scenario: A dead recipient under Spawned delivery is pruned via a self-sent Unregister
    Given a running MessageBus with delivery strategy "Spawned"
    And recipient A is registered for message type "Ping"
    And recipient A has stopped so its actor is not running
    When a "Ping" message is published
    Then recipient A is eventually removed from the registrations for "Ping"
    # FACT: the spawned task observes ActorNotRunning and tells the bus
    # Unregister::<Ping>::new(actor_id). Removal is asynchronous.

  @lifecycle
  Scenario: A live recipient is never pruned across repeated publishes
    Given a running MessageBus with delivery strategy "Guaranteed"
    And recipient A is registered for message type "Ping"
    When 4 "Ping" messages are published
    Then recipient A receives exactly 4 "Ping" messages
    And recipient A remains registered

  # ---------------------------------------------------------------------------
  # @boundary — unknown type, no-op publish, skip without panic
  # ---------------------------------------------------------------------------

  @boundary
  Scenario: Publishing a type with zero registered recipients is a graceful no-op
    Given a running MessageBus with delivery strategy "Guaranteed"
    And no recipients are registered for message type "Ping"
    When a "Ping" message is published
    Then the publish completes without error
    And the MessageBus actor does not panic
    # FACT: the `if let Some(registrations)` guard is skipped when the TypeId has no bucket.

  @boundary
  Scenario: A publish of one type never reaches recipients of an unrelated type
    Given a running MessageBus with delivery strategy "Guaranteed"
    And recipient A is registered for message type "Pong"
    When a "Ping" message is published
    Then recipient A receives 0 messages
    And the MessageBus actor does not panic
    # FACT: TypeId routing prevents an unsound downcast; the Pong bucket is never visited
    # during a Ping publish.

  @boundary
  Scenario: Unregistering an actor that was never registered is a no-op
    Given a running MessageBus with delivery strategy "Guaranteed"
    And recipient A is registered for message type "Ping"
    When an unknown actor id is unregistered for message type "Ping"
    Then the MessageBus actor does not panic
    And recipient A remains registered

  @boundary
  Scenario: BestEffort skips a full recipient without panicking or blocking others
    Given a running MessageBus with delivery strategy "BestEffort"
    And recipient FULL whose mailbox is full is registered for message type "Ping"
    And recipient FREE with spare capacity is registered for message type "Ping"
    When a "Ping" message is published
    Then the MessageBus actor does not panic
    And recipient FREE receives exactly 1 "Ping" message
    And recipient FULL receives 0 messages
    And recipient FULL remains registered

  # ---------------------------------------------------------------------------
  # @linearizability — concurrent register / publish, real overlap
  # ---------------------------------------------------------------------------

  @linearizability
  Scenario: Concurrent publishes of one type deliver every message exactly once
    Given a running MessageBus with delivery strategy "Guaranteed"
    And recipient A is registered for message type "Ping"
    When 100 "Ping" messages are published concurrently from 10 tasks
    Then recipient A receives exactly 100 "Ping" messages with no loss or duplication
    # NOTE: the MessageBus actor serialises handling on its mailbox; the overlap is in producers.

  @linearizability
  Scenario: A register concurrent with a publish is observed atomically
    Given a running MessageBus with delivery strategy "Guaranteed"
    And recipient A exists but is not yet registered
    When A registers for message type "Ping" while a "Ping" message is published
    Then A receives the message exactly once or not at all, never a partial delivery
    # NOTE: Register and Publish are distinct mailbox messages on the same actor; each is atomic
    # and their relative order is whatever the mailbox observed.

  @linearizability
  Scenario: Concurrent publishes of two types route each to its own recipients only
    Given a running MessageBus with delivery strategy "Guaranteed"
    And recipient A is registered for message type "Ping"
    And recipient B is registered for message type "Pong"
    When 50 "Ping" and 50 "Pong" messages are published concurrently from 10 tasks
    Then recipient A receives exactly 50 "Ping" messages
    And recipient B receives exactly 50 "Pong" messages
    And neither recipient receives a message of the other type
