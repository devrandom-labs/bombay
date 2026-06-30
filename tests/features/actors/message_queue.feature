# EXEMPLAR FEATURE — sets the authoring standard for every other .feature file.
#
# Scope: bombay_actors `MessageQueue` (actors/src/message_queue.rs) — an in-process
#        AMQP-style broker actor: exchanges (Direct/Topic/Fanout/Headers), queues,
#        bindings, consumers, BasicPublish routing.
#
# Authoring rules (apply to ALL feature files):
#   * Every Scenario carries exactly ONE cross-cutting tag:
#       @sequence | @lifecycle | @boundary | @linearizability
#   * Invariant-first: the `Then` names the observable guarantee. If you cannot state
#     the Then without reading the implementation, the invariant is not yet pinned —
#     write it as a `# NOTE:` and a @review-semantics tag rather than asserting a guess.
#   * @bug:<file:line> marks a scenario that MUST FAIL today (reproduces a real defect).
#   * Facts only: a Then asserts behaviour confirmed from source or AMQP/glob specs,
#     never a plausible guess. Open questions are scenarios tagged @review-semantics.
#   * No step definitions here. Steps are written in the wiring phase.

@actors @message_queue
Feature: MessageQueue — AMQP-style exchange/queue routing
  As a producer publishing through a MessageQueue actor
  I want messages routed to queues by exchange type and binding
  So that consumers receive exactly the messages their bindings select

  Background:
    Given a running MessageQueue actor with delivery strategy "BestEffort"

  # ---------------------------------------------------------------------------
  # @sequence — multi-step protocol: declare → bind → consume → publish → observe
  # ---------------------------------------------------------------------------

  @sequence
  Scenario: Direct exchange routes only on exact routing-key match
    Given a Direct exchange "orders" is declared
    And a queue "new-orders" is declared
    And queue "new-orders" is bound to "orders" with routing key "order.new"
    And a consumer is attached to queue "new-orders"
    When a message is published to "orders" with routing key "order.new"
    And a message is published to "orders" with routing key "order.cancelled"
    Then the consumer receives exactly 1 message
    And the received message is the one published with routing key "order.new"

  @sequence
  Scenario: Fanout exchange routes to every bound queue regardless of routing key
    Given a Fanout exchange "events" is declared
    And queues "audit" and "metrics" are declared and bound to "events"
    And a consumer is attached to each of "audit" and "metrics"
    When a message is published to "events" with routing key "anything"
    Then the "audit" consumer receives exactly 1 message
    And the "metrics" consumer receives exactly 1 message

  @sequence
  Scenario: Topic exchange routes a single-segment wildcard match
    Given a Topic exchange "logs" is declared
    And a queue "warns" is declared and bound to "logs" with routing key "log.*"
    And a consumer is attached to queue "warns"
    When a message is published to "logs" with routing key "log.warn"
    Then the consumer receives exactly 1 message
    # NOTE: matching uses the `glob` crate (Pattern + MatchOptions
    # require_literal_separator:true) whose separator is '/', NOT '.'. Standard AMQP
    # topic semantics ('*' = exactly one dot-segment) do NOT apply. Resolution: the glob
    # contract is authoritative — "log.*" matching "log.warn.detail" is pinned in the
    # "Topic wildcard segment-boundary semantics (glob, not AMQP)" scenario below.

  @sequence
  Scenario: Default (empty-name) exchange auto-binds a queue to its own name
    Given a queue "tasks" is declared
    And a consumer is attached to queue "tasks"
    When a message is published to the default exchange with routing key "tasks"
    Then the consumer receives exactly 1 message

  @sequence
  Scenario: Headers exchange with x-match=all requires every argument to match
    Given a Headers exchange "h" is declared
    And a queue "q" is declared
    And queue "q" is bound to "h" with arguments:
      | x-match | all  |
      | format  | json |
      | level   | high |
    And a consumer is attached to queue "q"
    When a message is published to "h" with headers:
      | format | json |
      | level  | high |
    And a message is published to "h" with headers:
      | format | json |
    Then the consumer receives exactly 1 message
    And the received message is the one whose headers included both "format" and "level"

  @sequence
  Scenario: Headers exchange with x-match=any matches when at least one argument matches
    Given a Headers exchange "h" is declared
    And a queue "q" is declared
    And queue "q" is bound to "h" with arguments:
      | x-match | any  |
      | format  | json |
      | level   | high |
    And a consumer is attached to queue "q"
    When a message is published to "h" with headers:
      | format | json |
    Then the consumer receives exactly 1 message

  @sequence
  Scenario: A publish filter suppresses delivery to a consumer whose tags fail it
    Given a Fanout exchange "events" is declared
    And a queue "q" is declared and bound to "events"
    And a consumer is attached to queue "q" with tags:
      | region | eu |
    When a message is published to "events" with a filter requiring tag "region" = "us"
    Then the consumer receives 0 messages

  # ---------------------------------------------------------------------------
  # @lifecycle — declare/bind/unbind/consume/cancel; auto-delete; dead consumers
  # ---------------------------------------------------------------------------

  @lifecycle
  Scenario: Unbinding the last binding of an auto-delete exchange removes the exchange
    Given a Fanout exchange "ephemeral" is declared with auto-delete enabled
    And a queue "q" is declared and bound to "ephemeral"
    When queue "q" is unbound from "ephemeral"
    Then publishing to "ephemeral" fails with "ExchangeNotFound"

  @lifecycle
  Scenario: A cancelled consumer stops receiving subsequent messages
    Given a Fanout exchange "events" is declared
    And a queue "q" is declared and bound to "events"
    And a consumer is attached to queue "q"
    When the consumer is cancelled
    And a message is published to "events" with routing key "x"
    Then the consumer receives 0 messages

  @lifecycle
  Scenario: A stopped consumer is pruned and does not block delivery to live consumers
    Given a Fanout exchange "events" is declared
    And a queue "q" is declared and bound to "events"
    And a live consumer A and a stopped consumer B are attached to queue "q"
    When a message is published to "events" with routing key "x"
    Then consumer A receives exactly 1 message
    And no error is surfaced for the stopped consumer B

  @lifecycle
  Scenario: Re-consuming the same queue with the same actor registers it once (dedup-by-id)
    Given a Fanout exchange "events" is declared
    And a queue "q" is declared and bound to "events"
    And a consumer C is attached to queue "q"
    When consumer C is attached to queue "q" a second time
    And a message is published to "events" with routing key "x"
    Then consumer C receives exactly 1 message
    # NOTE (:761): BasicConsume pushes a Registration only when no existing registration in
    # the (queue, TypeId) bucket already has the same actor_id
    # (`!recipients.iter().any(|reg| reg.actor_id == actor_id)`). This is dedup-by-id —
    # distinct from broker/message_bus (no-dedup, N registrations = N deliveries) and from
    # pubsub (overwrite-by-id, replaces the filter). See the routing-semantics matrix.

  @lifecycle
  Scenario: A consumer with a full mailbox is skipped but never pruned (BestEffort)
    Given a Fanout exchange "events" is declared
    And a queue "q" is declared and bound to "events"
    And a consumer with a full bounded mailbox is attached to queue "q"
    When a message is published to "events" with routing key "x"
    Then the publish does not error and the actor does not panic
    And the consumer remains registered on queue "q"
    # NOTE (:438-443): BestEffort uses try_send and pushes to `to_cancel` ONLY on
    # SendError::ActorNotRunning — a MailboxFull (and, for TimedDelivery, a Timeout at
    # :444-452) is dropped without pruning. Prune is ActorNotRunning-only, like broker and
    # message_bus.

  @lifecycle
  Scenario: Deleting a queue cascades binding removal and auto-deletes emptied exchanges
    Given a Fanout exchange "ephemeral" is declared with auto-delete enabled
    And a Direct exchange "durable" is declared
    And a queue "q" is declared and bound to both "ephemeral" and "durable" with routing key "k"
    When queue "q" is deleted
    Then publishing to "ephemeral" fails with "ExchangeNotFound"
    And the "durable" exchange still exists with no bindings to "q"
    # NOTE (:370-383): queue_delete removes the queue, retains-out its bindings from the
    # default exchange and every named exchange, then removes any now-empty auto_delete
    # exchange. "durable" (auto_delete=false) survives empty; "ephemeral" (auto_delete=true)
    # is removed.

  @lifecycle
  Scenario: Cancelling the last consumer of an auto-delete queue deletes the queue
    Given a Fanout exchange "events" is declared
    And a queue "q" is declared with auto-delete enabled and bound to "events"
    And a consumer C is attached to queue "q"
    When consumer C is cancelled
    Then queue "q" no longer exists
    # NOTE (:403-408): basic_cancel removes the registration, prunes the empty TypeId bucket,
    # and — because the queue is auto_delete — calls queue_delete(queue, if_unused=true). With
    # no remaining recipients the if_unused check passes and the queue is removed.

  # ---------------------------------------------------------------------------
  # @boundary — defensive: bad inputs, missing entities, the confirmed panic
  # ---------------------------------------------------------------------------

  @boundary
  Scenario: Binding a queue that does not exist is rejected
    Given a Direct exchange "x" is declared
    When queue "ghost" is bound to "x" with routing key "k"
    Then the bind fails with "QueueNotFound"

  @boundary
  Scenario: Binding to an exchange that does not exist is rejected
    Given a queue "q" is declared
    When queue "q" is bound to "no-such-exchange" with routing key "k"
    Then the bind fails with "ExchangeNotFound"

  @boundary
  Scenario: A duplicate (queue, routing-key) binding is rejected
    Given a Direct exchange "x" is declared
    And a queue "q" is declared and bound to "x" with routing key "k"
    When queue "q" is bound to "x" with routing key "k" again
    Then the bind fails with "BindingAlreadyExists"

  @boundary
  Scenario: A Headers binding with an unknown x-match value is rejected
    Given a Headers exchange "h" is declared
    And a queue "q" is declared
    When queue "q" is bound to "h" with arguments:
      | x-match | sometimes |
    Then the bind fails with "InvalidHeaderMatch"

  @boundary
  Scenario: Publishing to a Headers exchange without headers is rejected
    Given a Headers exchange "h" is declared
    And a queue "q" is declared and bound to "h" with arguments:
      | x-match | all  |
      | format  | json |
    When a message is published to "h" with no headers
    Then the publish fails with "HeadersRequired"

  @boundary
  Scenario: Publishing to an unknown exchange is rejected
    When a message is published to "no-such-exchange" with routing key "k"
    Then the publish fails with "ExchangeNotFound"

  @boundary
  Scenario: Declaring an exchange whose name already exists is rejected
    Given a Direct exchange "x" is declared
    When a Direct exchange "x" is declared again
    Then the declare fails with "ExchangeAlreadyExists"
    # NOTE (:507): ExchangeDeclare errors if the name is already present in `exchanges`.

  @boundary
  Scenario: Declaring an empty-name exchange is rejected as already-existing
    When a Direct exchange "" is declared
    Then the declare fails with "ExchangeAlreadyExists"
    # NOTE (:507): the empty name is reserved for the implicit default exchange, so
    # `msg.exchange.is_empty()` is rejected with ExchangeAlreadyExists (not a separate
    # InvalidName variant) — a deliberate quirk worth pinning.

  @boundary
  Scenario: Declaring a queue whose name already exists is rejected
    Given a queue "q" is declared
    When a queue "q" is declared again
    Then the declare fails with "QueueAlreadyExists"
    # NOTE (:557): QueueDeclare errors if the name is already present in `queues`.

  @boundary
  Scenario: Deleting an exchange that still has bindings with if_unused is rejected
    Given a Direct exchange "x" is declared
    And a queue "q" is declared and bound to "x" with routing key "k"
    When exchange "x" is deleted with if_unused set
    Then the delete fails with "ExchangeInUse"
    # NOTE (:534): ExchangeDelete with if_unused returns ExchangeInUse while any binding
    # remains; deleting an unknown exchange returns ExchangeNotFound (:541).

  @boundary
  Scenario: Deleting a queue that still has a consumer with if_unused is rejected
    Given a Fanout exchange "events" is declared
    And a queue "q" is declared and bound to "events"
    And a consumer is attached to queue "q"
    When queue "q" is deleted with if_unused set
    Then the delete fails with "QueueInUse"
    # NOTE (:360-362): queue_delete with if_unused returns QueueInUse while the queue has any
    # recipient; deleting an unknown queue returns QueueNotFound (:366).

  @boundary
  Scenario: Consuming from a queue that does not exist is rejected
    When a consumer is attached to queue "ghost"
    Then the consume fails with "QueueNotFound"
    # NOTE (:757): BasicConsume resolves the queue first and returns QueueNotFound if absent.

  @boundary
  Scenario: Unbinding from an exchange that does not exist is rejected
    Given a queue "q" is declared
    When queue "q" is unbound from "no-such-exchange"
    Then the unbind fails with "ExchangeNotFound"
    # NOTE (:652-654): QueueUnbind matches the exchange entry and returns ExchangeNotFound on
    # a vacant entry before mutating any binding.

  # --- the confirmed defect + the desired behaviour it should become ---------

  @boundary @bug:actors/src/message_queue.rs:707
  Scenario: A refused malformed Topic key never panics the actor when publishing
    Given a Topic exchange "logs" is declared
    And a queue "q" is declared
    When queue "q" is bound to "logs" with routing key "[unclosed"
    Then the bind fails with "InvalidRoutingKey"
    When a message is published to "logs" with routing key "log.warn"
    Then the publish does not error and the actor does not panic
    # FIXED (:707, card #79): bind-time validation (:591) refuses "[unclosed", so a
    # malformed key never reaches the store — there is no back door to plant one
    # through the public API. The publish path no longer `unwrap`-panics (it compiles
    # binding globs through topic_matches, returning AmqpError::InvalidRoutingKey for a
    # bad key). The pure publish-side defence over the GEN boundary set is proven by
    # the in-file #[cfg(test)] unit tests in message_queue.rs; here we assert the
    # end-to-end truth: the bind is refused and a later publish neither errors nor
    # panics the run-loop.

  @boundary @bug:actors/src/message_queue.rs:591
  Scenario: A malformed Topic routing key is rejected at bind time
    Given a Topic exchange "logs" is declared
    And a queue "q" is declared
    When queue "q" is bound to "logs" with routing key "[unclosed"
    Then the bind fails with "InvalidRoutingKey"
    # FIXED (:591, card #79): QueueBind validates Topic routing keys (mirroring the
    # Headers x-match check) and returns AmqpError::InvalidRoutingKey for any key
    # glob::Pattern::new rejects, so the malformed key never reaches the publish path.

  @sequence
  Scenario: Topic wildcard segment-boundary semantics (glob, not AMQP)
    Given a Topic exchange "logs" is declared
    And a queue "q" is declared and bound to "logs" with routing key "log.*"
    And a consumer is attached to queue "q"
    When a message is published to "logs" with routing key "log.warn.detail"
    Then the consumer receives exactly 1 message
    # NOTE (:707, glob): contract IS glob — '*' (separator '/') spans '.', so "log.*"
    # matches "log.warn.detail" and delivers today. NOT AMQP '*' (single-segment) semantics.

  # ---------------------------------------------------------------------------
  # @linearizability — concurrent producers/consumers, real overlap
  # ---------------------------------------------------------------------------

  @linearizability
  Scenario: Concurrent publishes to a fanout exchange deliver every message
    Given a Fanout exchange "events" is declared
    And a queue "q" is declared and bound to "events"
    And a consumer is attached to queue "q"
    When 100 messages are published concurrently to "events" from 10 tasks
    Then the consumer receives exactly 100 messages with no loss or duplication

  @linearizability
  Scenario: A binding added concurrently with a publish is observed atomically
    Given a Direct exchange "x" is declared
    And a queue "q" is declared
    When queue "q" is bound to "x" with routing key "k" while a message with key "k" is published
    Then either the message is delivered to "q" or it is dropped, never partially routed
