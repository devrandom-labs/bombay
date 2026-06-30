# Scope: bombay_actors `Broker<M>` (actors/src/broker.rs) — a topic-based pub/sub
#        actor. Subscriptions are keyed by a `glob::Pattern`; a `Publish { topic }`
#        carries a concrete topic String matched against every subscribed pattern with
#        MatchOptions { case_sensitive: true, require_literal_separator: true,
#        require_literal_leading_dot: false }. Delivery obeys the actor's DeliveryStrategy.
#
# Authoring rules (mirrors tests/features/actors/message_queue.feature):
#   * Every Scenario carries exactly ONE cross-cutting tag:
#       @sequence | @lifecycle | @boundary | @linearizability
#   * Invariant-first: the `Then` names the observable guarantee. If the Then cannot be
#     stated without reading impl details, it is a `# NOTE:` + @review-semantics, not a guess.
#   * Facts only — confirmed from source or the `glob` crate docs, never a plausible guess.
#   * No step definitions here. Steps are written in the wiring phase.
#
# FACTS pinned from source (broker.rs):
#   * subscriptions: HashMap<Pattern, Vec<Recipient<M>>> — keyed by the glob Pattern.
#   * Subscribe (:120-130) does `entry(topic).or_default().push(recipient)` — NO dedupe:
#     the same recipient subscribed twice under the same pattern is stored twice.
#   * Subscribe stores the same actor under DIFFERENT patterns as independent entries.
#   * Publish (:179-266) iterates every (pattern, recipients) and delivers when
#     `pattern.matches_with(&topic, options)` is true.
#   * Dead-actor cleanup fires ONLY on SendError::ActorNotRunning. For Guaranteed /
#     BestEffort / TimedDelivery the (pattern, id) is queued in `to_remove` and unsubscribed
#     after the loop; for Spawned / SpawnedWithTimeout the spawned task sends an
#     Unsubscribe { topic: Some(pattern), actor_id } back to the broker. Other SendError
#     variants (MailboxFull, Timeout, HandlerError) never prune.
#   * Unsubscribe (:144-164): Some(topic) prunes that (pattern, actor); None prunes the
#     actor from EVERY pattern. Empty recipient vecs drop their pattern key.

@actors @broker
Feature: Broker — glob-topic publish/subscribe with delivery strategies
  As a producer publishing to topics through a Broker actor
  I want messages routed to every recipient whose glob pattern matches the topic
  So that subscribers receive exactly the messages their patterns select, under the
  reliability contract of the configured delivery strategy

  # ---------------------------------------------------------------------------
  # @sequence — subscribe → publish → observe routing; pattern semantics
  # ---------------------------------------------------------------------------

  @sequence
  Scenario: An exact-literal pattern matches only its exact topic
    Given a running Broker with delivery strategy "Guaranteed"
    And a subscriber S is subscribed with pattern "my-topic"
    When a message is published to topic "my-topic"
    And a message is published to topic "my-other"
    Then subscriber S receives exactly 1 message
    And the received message is the one published to "my-topic"

  @sequence
  Scenario: A single-segment glob star matches within one separator-delimited segment
    Given a running Broker with delivery strategy "Guaranteed"
    And a subscriber S is subscribed with pattern "sensors/kitchen/*"
    When a message is published to topic "sensors/kitchen/temperature"
    Then subscriber S receives exactly 1 message
    # FACT: require_literal_separator:true makes '*' stop at '/', so the single trailing
    # segment "temperature" is matched by the trailing '*'.

  @sequence
  Scenario: A glob star does not cross the '/' separator
    Given a running Broker with delivery strategy "Guaranteed"
    And a subscriber S is subscribed with pattern "sensors/*"
    When a message is published to topic "sensors/kitchen/temperature"
    Then subscriber S receives 0 messages
    # FACT: with require_literal_separator:true, "sensors/*" matches "sensors/kitchen"
    # but NOT "sensors/kitchen/temperature" (the '*' may not span the second '/').

  @sequence
  Scenario: A prefix glob matches a single trailing segment
    Given a running Broker with delivery strategy "Guaranteed"
    And a subscriber S is subscribed with pattern "sensors/*"
    When a message is published to topic "sensors/kitchen"
    Then subscriber S receives exactly 1 message

  @sequence
  Scenario: Topics use '/' as the only glob separator — '.' is an ordinary character
    Given a running Broker with delivery strategy "Guaranteed"
    And a subscriber S is subscribed with pattern "my-*"
    When a message is published to topic "my-topic.detail"
    Then subscriber S receives exactly 1 message
    # NOTE (broker.rs:4-5, glob MatchOptions): glob separator is '/'; '.' is an ordinary
    # char. Resolution: contract IS glob — "my-*" treats "my-topic.detail" as one segment
    # and matches (no '/' stops the '*'). NOT AMQP '.'-segment semantics.

  @sequence
  Scenario: A topic matching two distinct patterns delivers to the recipients of both
    Given a running Broker with delivery strategy "Guaranteed"
    And a subscriber A is subscribed with pattern "sensors/*"
    And a subscriber B is subscribed with pattern "*/kitchen"
    When a message is published to topic "sensors/kitchen"
    Then subscriber A receives exactly 1 message
    And subscriber B receives exactly 1 message

  @sequence
  Scenario: One actor subscribed under two patterns is delivered once per matching pattern
    Given a running Broker with delivery strategy "Guaranteed"
    And a subscriber S is subscribed with pattern "a/*"
    And the same subscriber S is also subscribed with pattern "*/x"
    When a message is published to topic "a/x"
    Then subscriber S receives exactly 2 messages
    # FACT: the two patterns are independent map entries; "a/x" matches both, so the
    # same recipient is told twice. (Broker performs no cross-pattern dedupe.)

  @sequence
  Scenario: Subscribing the same actor twice under one pattern stores it twice
    Given a running Broker with delivery strategy "Guaranteed"
    And a subscriber S is subscribed with pattern "topic"
    And the same subscriber S is subscribed with pattern "topic" again
    When a message is published to topic "topic"
    Then subscriber S receives exactly 2 messages
    # FACT: Subscribe pushes unconditionally (no id check); the recipients Vec holds two
    # copies, so a single publish delivers two messages.

  @sequence
  Scenario: A publish whose topic matches no pattern delivers nothing and does not error
    Given a running Broker with delivery strategy "Guaranteed"
    And a subscriber S is subscribed with pattern "a/*"
    When a message is published to topic "b/1"
    Then subscriber S receives 0 messages
    And the publish completes without error

  # ---------------------------------------------------------------------------
  # @sequence — the five DeliveryStrategy variants
  # ---------------------------------------------------------------------------

  @sequence
  Scenario: Guaranteed delivery blocks the publish until every recipient accepts
    Given a running Broker with delivery strategy "Guaranteed"
    And a subscriber S is subscribed with pattern "t"
    When a message is published to topic "t"
    Then the publish completes only after S has accepted the message
    And S receives exactly 1 message
    # FACT: Guaranteed awaits `recipient.tell(...).await` (no timeout) for each recipient.

  @sequence @timing
  Scenario: BestEffort skips a recipient whose mailbox is full without blocking
    Given a running Broker with delivery strategy "BestEffort"
    And a subscriber S whose mailbox is already full is subscribed with pattern "t"
    When a message is published to topic "t"
    Then the publish completes without blocking
    And S does not receive the message
    And S is NOT pruned from the subscription
    # FACT: BestEffort uses try_send(); a full mailbox yields SendError::MailboxFull which
    # is NOT one of the pruned variants (only ActorNotRunning prunes). No panic occurs.

  @sequence @timing
  Scenario: TimedDelivery bounds each recipient's wait by the configured timeout
    Given a running Broker with delivery strategy "TimedDelivery" of 50 milliseconds
    And a subscriber S whose mailbox stays full past the timeout is subscribed with pattern "t"
    When a message is published to topic "t"
    Then the publish returns within approximately the timeout
    And S does not receive the message
    And S is NOT pruned from the subscription
    # FACT: mailbox_timeout(duration) yields SendError::Timeout, which is not a pruned variant.

  @sequence @timing
  Scenario: Spawned delivery returns immediately and retries a full mailbox indefinitely
    Given a running Broker with delivery strategy "Spawned"
    And a subscriber S whose mailbox is full but later drains is subscribed with pattern "t"
    When a message is published to topic "t"
    Then the publish returns without waiting for S to accept
    And once S's mailbox drains, S eventually receives exactly 1 message
    # FACT: Spawned uses tokio::spawn + tell(...).send() with no timeout — it blocks the
    # spawned task on the mailbox until capacity frees, so delivery is retried indefinitely.

  @sequence @timing
  Scenario: SpawnedWithTimeout returns immediately and abandons delivery after the timeout
    Given a running Broker with delivery strategy "SpawnedWithTimeout" of 50 milliseconds
    And a subscriber S whose mailbox stays full past the timeout is subscribed with pattern "t"
    When a message is published to topic "t"
    Then the publish returns without waiting for S to accept
    And S does not receive the message after the timeout elapses
    # FACT: spawned task uses mailbox_timeout(duration); a Timeout result is not pruned.

  # ---------------------------------------------------------------------------
  # @lifecycle — unsubscribe semantics; dead-actor auto-cleanup
  # ---------------------------------------------------------------------------

  @lifecycle
  Scenario: Unsubscribe by topic removes the actor from only that pattern
    Given a running Broker with delivery strategy "Guaranteed"
    And a subscriber S is subscribed with pattern "a/*"
    And the same subscriber S is also subscribed with pattern "b/*"
    When S unsubscribes from topic "a/*"
    And a message is published to topic "a/1"
    And a message is published to topic "b/1"
    Then subscriber S receives exactly 1 message
    And the received message is the one published to "b/1"

  @lifecycle
  Scenario: Unsubscribe with topic None removes the actor from every pattern
    Given a running Broker with delivery strategy "Guaranteed"
    And a subscriber S is subscribed with pattern "a/*"
    And the same subscriber S is also subscribed with pattern "b/*"
    When S unsubscribes from all topics
    And a message is published to topic "a/1"
    And a message is published to topic "b/1"
    Then subscriber S receives 0 messages

  @lifecycle
  Scenario: Unsubscribing the last recipient of a pattern drops the pattern entry
    Given a running Broker with delivery strategy "Guaranteed"
    And a subscriber S is the only subscriber of pattern "a/*"
    When S unsubscribes from topic "a/*"
    Then pattern "a/*" no longer has any subscription entry
    # FACT: unsubscribe (:98-105) removes the pattern key once its recipients Vec is empty.

  @lifecycle
  Scenario: A dead recipient is pruned on the next matching publish (Guaranteed)
    Given a running Broker with delivery strategy "Guaranteed"
    And a subscriber S is subscribed with pattern "t"
    And subscriber S has stopped so its actor is not running
    When a message is published to topic "t"
    Then the publish completes without error
    And subscriber S is removed from the subscription
    # FACT: ActorNotRunning queues (pattern, id) in to_remove, pruned after the loop.

  @lifecycle
  Scenario: A dead recipient under Spawned delivery is pruned via a self-sent Unsubscribe
    Given a running Broker with delivery strategy "Spawned"
    And a subscriber S is subscribed with pattern "t"
    And subscriber S has stopped so its actor is not running
    When a message is published to topic "t"
    Then subscriber S is eventually removed from the subscription
    # FACT: the spawned task observes ActorNotRunning and tells the broker
    # Unsubscribe { topic: Some("t"), actor_id: S }. Removal is asynchronous.

  @lifecycle
  Scenario: A live recipient is never pruned by delivery
    Given a running Broker with delivery strategy "Guaranteed"
    And a subscriber S is subscribed with pattern "t"
    When 3 messages are published to topic "t"
    Then subscriber S receives exactly 3 messages
    And subscriber S remains subscribed

  # ---------------------------------------------------------------------------
  # @boundary — defensive inputs and skip-without-panic behaviour
  # ---------------------------------------------------------------------------

  @boundary
  Scenario: BestEffort skips a full mailbox without panicking the broker
    Given a running Broker with delivery strategy "BestEffort"
    And a subscriber FULL whose mailbox is full is subscribed with pattern "t"
    And a subscriber FREE with spare capacity is subscribed with pattern "t"
    When a message is published to topic "t"
    Then the Broker actor does not panic
    And subscriber FREE receives exactly 1 message
    And subscriber FULL receives 0 messages
    # FACT: a full mailbox yields MailboxFull (skipped, not pruned); the publish proceeds
    # to the next recipient without interruption.

  @boundary
  Scenario: Unsubscribing an actor that was never subscribed is a no-op
    Given a running Broker with delivery strategy "Guaranteed"
    And a subscriber S is subscribed with pattern "t"
    When an unknown actor id unsubscribes from all topics
    Then the Broker actor does not panic
    And subscriber S remains subscribed

  @boundary
  Scenario: Unsubscribing from a pattern that has no subscriptions is a no-op
    Given a running Broker with delivery strategy "Guaranteed"
    When an actor unsubscribes from topic "never/subscribed"
    Then the Broker actor does not panic

  # ---------------------------------------------------------------------------
  # @linearizability — concurrent producers/subscribers, real overlap
  # ---------------------------------------------------------------------------

  @linearizability
  Scenario: Concurrent publishes to one pattern deliver every message exactly once
    Given a running Broker with delivery strategy "Guaranteed"
    And a subscriber S is subscribed with pattern "t"
    When 100 messages are published concurrently to topic "t" from 10 tasks
    Then subscriber S receives exactly 100 messages with no loss or duplication
    # NOTE: the Broker actor serialises message handling on its mailbox; concurrency is in
    # the producers, not in the handler.

  @linearizability
  Scenario: A subscribe concurrent with a publish is observed atomically
    Given a running Broker with delivery strategy "Guaranteed"
    And a subscriber S exists but is not yet subscribed
    When S subscribes to pattern "t" while a message is published to topic "t"
    Then S receives the message exactly once or not at all, never a partial delivery
    # NOTE: Subscribe and Publish are distinct mailbox messages on the same actor; their
    # relative order is whatever the mailbox observed, but each is atomic.

  @linearizability
  Scenario: Concurrent publishes to overlapping patterns deliver to each match independently
    Given a running Broker with delivery strategy "Guaranteed"
    And a subscriber A is subscribed with pattern "a/*"
    And a subscriber B is subscribed with pattern "*/x"
    When 50 messages are published concurrently to topic "a/x" from 5 tasks
    Then subscriber A receives exactly 50 messages
    And subscriber B receives exactly 50 messages
