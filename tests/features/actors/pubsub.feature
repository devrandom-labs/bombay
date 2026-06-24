# Scope: kameo_actors `PubSub<M>` (actors/src/pubsub.rs) — a broadcast pub/sub actor.
#        Subscribers are keyed by ActorId, each stored with a per-subscriber filter
#        FnMut(&M) -> bool. publish(msg) clones the message to every subscriber whose
#        filter returns true, under the configured DeliveryStrategy. Usable as a plain
#        object (subscribe / publish) or spawned (Subscribe / SubscribeFilter / Publish).
#
# Authoring rules (mirrors tests/features/actors/message_queue.feature):
#   * Every Scenario carries exactly ONE cross-cutting tag:
#       @sequence | @lifecycle | @boundary | @linearizability
#   * Invariant-first; facts only; open questions are @review-semantics. No step definitions.
#
# FACTS pinned from source (pubsub.rs):
#   * subscribers: HashMap<ActorId, (Subscriber<M>, FilterFn<M>)> — keyed by ActorId, so a
#     re-subscribe of the same actor OVERWRITES the prior entry (dedupe by id).
#   * subscribe() installs the default filter `|_| true`; subscribe_filter() installs a custom one.
#   * publish() (:104-148) calls filter(&msg); only on true does it clone+deliver. A false
#     filter SKIPS delivery and the subscriber is NOT removed.
#   * Pruning (:137-147) removes a subscriber ONLY on SendError::ActorNotRunning OR
#     SendError::ActorStopped. MailboxFull / HandlerError / Timeout are kept (no prune).
#   * Spawned / SpawnedWithTimeout deliver via tokio::spawn fire-and-forget — their results
#     are never inspected, so they NEVER prune dead subscribers.
#   * Publish to zero subscribers iterates an empty map: a graceful no-op.

@actors @pubsub
Feature: PubSub — broadcast to all subscribers with per-subscriber filters
  As a producer broadcasting through a PubSub actor
  I want every subscriber whose filter accepts the message to get a clone of it
  So that broadcast and predicate-based filtering both hold under each delivery strategy

  # ---------------------------------------------------------------------------
  # @sequence — broadcast fan-out, filters, delivery strategies
  # ---------------------------------------------------------------------------

  @sequence
  Scenario: A published message is cloned to every subscriber
    Given a running PubSub with delivery strategy "Guaranteed"
    And subscribers A, B and C are subscribed with the default filter
    When a message is published
    Then subscriber A receives exactly 1 message
    And subscriber B receives exactly 1 message
    And subscriber C receives exactly 1 message
    # FACT: publish() clones the message once per accepting subscriber.

  @sequence
  Scenario: The default subscribe filter accepts every message
    Given a running PubSub with delivery strategy "Guaranteed"
    And a subscriber S is subscribed with the default filter
    When 3 distinct messages are published
    Then subscriber S receives exactly 3 messages
    # FACT: subscribe() installs `|_| true`.

  @sequence
  Scenario: A subscriber whose filter rejects a message receives nothing for it
    Given a running PubSub with delivery strategy "Guaranteed"
    And a subscriber S is subscribed with a filter accepting only messages tagged "keep"
    When a message tagged "drop" is published
    Then subscriber S receives 0 messages
    And subscriber S remains subscribed
    # FACT: filter false skips delivery entirely; the subscriber is NOT removed.

  @sequence
  Scenario: A rejecting filter does not affect other accepting subscribers
    Given a running PubSub with delivery strategy "Guaranteed"
    And a subscriber A is subscribed with a filter accepting only "TopicA:" messages
    And a subscriber B is subscribed with a filter accepting only "TopicB:" messages
    And a subscriber C is subscribed with the default filter
    When a message "TopicA: note" is published
    Then subscriber A receives exactly 1 message
    And subscriber B receives 0 messages
    And subscriber C receives exactly 1 message

  @sequence
  Scenario: SubscribeFilter predicates select the correct subset across two publishes
    Given a running PubSub with delivery strategy "Guaranteed"
    And a subscriber A is subscribed via SubscribeFilter accepting only "TopicA:" messages
    And a subscriber B is subscribed via SubscribeFilter accepting only "TopicB:" messages
    When a message "TopicA: x" is published
    And a message "TopicB: y" is published
    Then subscriber A receives exactly 1 message, the "TopicA: x" message
    And subscriber B receives exactly 1 message, the "TopicB: y" message

  @sequence
  Scenario: Guaranteed delivery blocks until each subscriber accepts
    Given a running PubSub with delivery strategy "Guaranteed"
    And a subscriber S is subscribed with the default filter
    When a message is published
    Then the publish completes only after S has accepted the message
    And subscriber S receives exactly 1 message

  @sequence @timing
  Scenario: BestEffort skips a full mailbox without removing the subscriber
    Given a running PubSub with delivery strategy "BestEffort"
    And a subscriber S whose mailbox is full is subscribed with the default filter
    When a message is published
    Then the publish completes without blocking
    And subscriber S does not receive the message
    And subscriber S remains subscribed
    # FACT: try_tell yields MailboxFull, which is not a pruned variant.

  @sequence @timing
  Scenario: TimedDelivery bounds the per-subscriber wait by the timeout
    Given a running PubSub with delivery strategy "TimedDelivery" of 50 milliseconds
    And a subscriber S whose mailbox stays full past the timeout is subscribed with the default filter
    When a message is published
    Then the publish returns within approximately the timeout
    And subscriber S does not receive the message
    And subscriber S remains subscribed
    # FACT: tell_timeout yields Timeout, which is not a pruned variant.

  @sequence @timing
  Scenario: Spawned delivery returns immediately and retries a full mailbox indefinitely
    Given a running PubSub with delivery strategy "Spawned"
    And a subscriber S whose mailbox is full but later drains is subscribed with the default filter
    When a message is published
    Then the publish returns without waiting for S to accept
    And once S's mailbox drains, S eventually receives exactly 1 message
    # FACT: Spawned uses tokio::spawn + subscriber.tell(msg) with no timeout.

  @sequence @timing
  Scenario: SpawnedWithTimeout returns immediately and abandons delivery after the timeout
    Given a running PubSub with delivery strategy "SpawnedWithTimeout" of 50 milliseconds
    And a subscriber S whose mailbox stays full past the timeout is subscribed with the default filter
    When a message is published
    Then the publish returns without waiting for S to accept
    And subscriber S does not receive the message after the timeout elapses

  # ---------------------------------------------------------------------------
  # @lifecycle — re-subscribe overwrite, dead-subscriber pruning
  # ---------------------------------------------------------------------------

  @lifecycle
  Scenario: Re-subscribing the same actor overwrites its prior filter
    Given a running PubSub with delivery strategy "Guaranteed"
    And a subscriber S is subscribed with a filter accepting only "old" messages
    When S is re-subscribed with a filter accepting only "new" messages
    And a message tagged "new" is published
    And a message tagged "old" is published
    Then subscriber S receives exactly 1 message, the "new" message
    # FACT: subscribers is keyed by ActorId; the second insert replaces the first entry —
    # the actor is stored once, never duplicated.

  @lifecycle
  Scenario: A subscriber whose actor is not running is pruned on the next publish
    Given a running PubSub with delivery strategy "Guaranteed"
    And a subscriber S is subscribed with the default filter
    And subscriber S has stopped so its actor is not running
    When a message is published
    Then the publish completes without error
    And subscriber S is removed from the subscriber set
    # FACT: SendError::ActorNotRunning prunes (:140-142).

  @lifecycle
  Scenario: A subscriber reporting ActorStopped is pruned on the next publish
    Given a running PubSub with delivery strategy "Guaranteed"
    And a subscriber S is subscribed with the default filter
    And subscriber S is in the process of stopping so delivery reports ActorStopped
    When a message is published
    Then subscriber S is removed from the subscriber set
    # FACT: ActorStopped is also a pruned variant in PubSub (unlike Broker / MessageBus,
    # which prune only on ActorNotRunning).

  @lifecycle
  Scenario: A rejected-by-filter dead subscriber is NOT pruned because no delivery is attempted
    Given a running PubSub with delivery strategy "Guaranteed"
    And a subscriber S is subscribed with a filter accepting only "keep" messages
    And subscriber S has stopped so its actor is not running
    When a message tagged "drop" is published
    Then subscriber S remains in the subscriber set
    # FACT: a false filter skips the tell entirely, so no SendError is ever observed and no
    # pruning can occur. Pruning requires an attempted delivery.

  @lifecycle
  Scenario: A live subscriber is never pruned across repeated publishes
    Given a running PubSub with delivery strategy "Guaranteed"
    And a subscriber S is subscribed with the default filter
    When 5 messages are published
    Then subscriber S receives exactly 5 messages
    And subscriber S remains subscribed

  @lifecycle @bug:actors/src/pubsub.rs:125
  Scenario: Spawned delivery prunes a dead subscriber
    Given a running PubSub with delivery strategy "Spawned"
    And a subscriber S is subscribed with the default filter
    And subscriber S has stopped so its actor is not running
    When a message is published
    Then subscriber S is removed from the subscriber set
    # NOTE (pubsub.rs:125-131): today Spawned/SpawnedWithTimeout discard the spawned task
    # result, so ActorNotRunning is never seen and S is never pruned — this leak is a
    # DEFECT, so this FAILS today. Desired: Spawned self-sends an Unsubscribe like
    # broker/message_bus do, pruning S.

  # ---------------------------------------------------------------------------
  # @boundary — zero subscribers, no-op publish, skip without panic
  # ---------------------------------------------------------------------------

  @boundary
  Scenario: Publishing with zero subscribers is a graceful no-op
    Given a running PubSub with delivery strategy "Guaranteed"
    And no subscribers are registered
    When a message is published
    Then the publish completes without error
    And the PubSub actor does not panic

  @boundary
  Scenario: BestEffort skips a full subscriber without panicking or blocking others
    Given a running PubSub with delivery strategy "BestEffort"
    And a subscriber FULL whose mailbox is full is subscribed with the default filter
    And a subscriber FREE with spare capacity is subscribed with the default filter
    When a message is published
    Then the PubSub actor does not panic
    And subscriber FREE receives exactly 1 message
    And subscriber FULL receives 0 messages
    And subscriber FULL remains subscribed

  # ---------------------------------------------------------------------------
  # @linearizability — concurrent producers, filters over shared state
  # ---------------------------------------------------------------------------

  @linearizability
  Scenario: Concurrent publishes deliver every message with no loss or duplication
    Given a running PubSub with delivery strategy "Guaranteed"
    And a subscriber S is subscribed with the default filter
    When 100 messages are published concurrently from 10 tasks
    Then subscriber S receives exactly 100 messages with no loss or duplication
    # NOTE: the PubSub actor serialises publish handling on its mailbox; the overlap is in
    # the producers.

  @linearizability
  Scenario: Concurrent publishes fan out to all subscribers with no cross-talk
    Given a running PubSub with delivery strategy "Guaranteed"
    And subscribers A and B are subscribed with the default filter
    When 50 messages are published concurrently from 5 tasks
    Then subscriber A receives exactly 50 messages
    And subscriber B receives exactly 50 messages

  @linearizability
  Scenario: A filter reading shared state observes a consistent per-message view under concurrency
    Given a running PubSub with delivery strategy "Guaranteed"
    And a subscriber S is subscribed with a filter that consults a shared counter
    When 100 messages are published concurrently from 10 tasks
    Then the filter is invoked exactly once per published message
    And subscriber S receives exactly the messages the filter accepted, no more and no fewer
    # NOTE: each filter call happens inside the actor's serialised publish handler, so the
    # FnMut sees one invocation per message with no interleaving inside the handler.
