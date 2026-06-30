# Scope: bombay core message handling (src/message.rs) — the `Message` trait handler,
#        the `Context<A, R>` passed to handlers (actor_ref, reply channel, stop flag),
#        `reply_sender`/`reply`/`spawn`/`forward`/`try_forward`/`blocking_forward`,
#        `StreamMessage`, and the `DynMessage::handle_dyn` dispatch that routes a
#        handler's reply value or error to the caller (ask) or the panic hook (tell).
#
# Authoring rules (apply to ALL feature files):
#   * Every Scenario carries exactly ONE cross-cutting tag:
#       @sequence | @lifecycle | @boundary | @linearizability
#   * Invariant-first: the `Then` names the observable guarantee. If the Then cannot be
#     stated without reading the implementation, write it as `# NOTE:` + @review-semantics.
#   * @bug:<file:line> marks a scenario that MUST FAIL today.
#   * Facts only: a Then asserts behaviour confirmed from src/message.rs (and the
#     SendError contract in src/error.rs), never a plausible guess.
#   * No step definitions here. Steps are written in the wiring phase.

@core @message
Feature: Message — sequential handler dispatch, context, forwarding, and reply routing
  As an actor processing messages one at a time with exclusive mutable state access
  I want the handler context, forwarding, and reply/error routing to be precise
  So that asks receive replies, tells route errors to the hook, and order is preserved

  Background:
    Given a spawned actor that handles a numbered command message

  # ---------------------------------------------------------------------------
  # @sequence — sequential handling, ctx.stop, deferred reply protocol
  # ---------------------------------------------------------------------------

  @sequence
  Scenario: Messages are handled sequentially in send order with exclusive state access
    When commands 1, 2, 3 are sent in that order
    And the actor records the order in which it handled them
    Then the actor handled them as 1, then 2, then 3
    # Confirmed: doc invariant — "Messages are processed sequentially one at a time, with
    # exclusive mutable access to the actors state" (src/message.rs:36-38). handle_dyn
    # takes &mut A (:399-405).

  @sequence
  Scenario: ctx.stop() stops the actor only after the current message finishes
    When a message whose handler calls ctx.stop() and then returns a reply is sent via ask
    Then the caller still receives that message's reply
    And the actor stops before handling any later-queued message
    # Confirmed: stop() only sets ctx.stop = true (:111-114); handle_dyn copies it back
    # into the loop's stop flag AFTER awaiting the handler and sending the reply
    # (:410-414), so the current reply is delivered before shutdown.

  @sequence
  Scenario: reply_sender takes the reply channel, leaving the context unable to auto-reply
    When a message handler calls ctx.reply_sender() and returns the DelegatedReply marker
    And the handler sends the reply through the taken ReplySender
    Then the caller receives exactly that reply
    And the dispatcher does not also send a second reply
    # Confirmed: reply_sender() does self.reply.take() (:163-165); handle_dyn's
    # ctx.reply.take() is then None, so it does not auto-send (:412-420).

  @sequence
  Scenario: ctx.reply sends an early reply and returns a DelegatedReply marker
    When a handler calls ctx.reply(value) and then continues working
    Then the caller receives value immediately from the early reply
    And no duplicate reply is sent when the handler returns
    # Confirmed: ctx.reply takes the sender and sends, then returns DelegatedReply::new()
    # (:170-175); the taken sender is gone so handle_dyn does not re-send.

  # ---------------------------------------------------------------------------
  # @lifecycle — spawn detached, stream lifecycle, forwarding across actors
  # ---------------------------------------------------------------------------

  @lifecycle
  Scenario: ctx.spawn detached delivers the reply to an ask caller after the actor moves on
    Given a handler that ctx.spawns a task which completes after a delay and returns a value
    When the message is sent via ask
    Then the ask caller receives the spawned task's value
    And the actor was free to handle other messages while the task ran
    # Confirmed: spawn() takes the reply sender and, on Some(tx), sends the awaited reply
    # from the detached tokio task (:229-249); doc: task runs independently of the actor.

  @lifecycle
  Scenario: ctx.spawn detached on a tell whose task errors invokes the global error hook
    Given a handler that ctx.spawns a task returning an Err
    When the message is sent via tell so no reply is expected
    Then the global actor error hook is invoked with that error
    And the actor's on_panic hook is NOT invoked
    # Confirmed: on None reply sender, spawn routes reply.into_any_err() to
    # invoke_actor_error_hook with PanicReason::OnMessage (:236-248); doc explicitly
    # states on_panic is NOT called for the detached task (:189).

  @lifecycle
  Scenario: forward delivers the target actor's reply back to the original ask caller
    Given a router actor and a live target actor
    When the original caller asks the router, whose handler forwards to the target
    Then the original caller receives the target actor's reply
    # Confirmed: forward() on Some(tx) hands the original reply channel to the target's
    # ask via .forward(tx.cast()) (:264-277).

  @lifecycle
  Scenario: forward on a tell sends the message to the target without expecting a reply
    Given a router actor and a live target actor
    When the original message is a tell so the router holds no reply channel
    And the router's handler forwards to the target
    Then the target receives the message
    And the ForwardedReply reflects the send outcome, not a value reply
    # Confirmed: forward()'s None arm does target.tell(message).send() and maps via
    # reset_err_infallible (:278-286).

  @lifecycle
  Scenario: StreamMessage delivers Started, then each Next item, then Finished in order
    Given an actor handling a StreamMessage of items
    When a stream of items [a, b] is attached to the actor
    Then the actor observes Started first
    And then Next(a), then Next(b)
    And finally Finished after the stream ends
    # NOTE @review-semantics: StreamMessage is the data enum (Next/Started/Finished,
    # src/message.rs:67-75). The exact Started-before-items and Finished-after-items
    # ordering is produced by attach_stream (in src/actor or src/request), not by this
    # enum. Pin the emission order against that call site at wiring time.

  # ---------------------------------------------------------------------------
  # @boundary — dead targets, full mailboxes, handler errors routed by ask vs tell
  # ---------------------------------------------------------------------------

  @boundary @bug:error.rs:293
  Scenario: forwarding to a dead target returns a SendError to the original caller
    Given a router actor and a target actor that has been stopped
    When the original caller asks the router, whose handler forwards to the dead target
    Then the original caller receives a SendError indicating the target is not running
    # Confirmed: forward awaits target.ask(...).forward(...); a dead target yields
    # SendError::ActorNotRunning, surfaced through the ForwardedReply (:264-277 +
    # SendError contract in src/error.rs).
    # @bug (MUST FAIL today): desired — the caller receives a graceful
    # SendError::ActorNotRunning. Actual — forward's failure builds the error via
    # `From<mpsc::SendError<Signal>>` (error.rs:293) instantiated with M=(message,
    # ReplySender), whose `signal.downcast_message::<(M, tx)>().unwrap()` is None
    # because the signal holds a BARE message, so .unwrap() PANICS inside the
    # router's handler; the router dies and the caller observes ActorStopped, not
    # ActorNotRunning. Stays red until the error.rs:293 conversion is fixed.

  @boundary @bug:error.rs:305
  Scenario: try_forward returns immediately with a mailbox-full error rather than blocking
    Given a router actor and a target actor whose bounded mailbox is full
    When the router's handler calls try_forward to the target
    Then try_forward returns a ForwardedReply carrying a MailboxFull send error
    And the original reply channel is restored to the router context so it can respond
    # Confirmed: try_forward uses ask(...).try_forward(...); on error map_msg restores
    # self.reply (:300-311), distinguishing a capacity hit from a dead actor.
    # @bug (MUST FAIL today): desired — try_forward returns a graceful
    # SendError::MailboxFull. Actual — same root cause on the try path:
    # `From<mpsc::TrySendError<Signal>>` (error.rs:305 Full arm) does
    # `signal.downcast_message::<(M, tx)>().unwrap()` → None → PANICS inside the
    # router's handler, so the caller sees ActorStopped, not MailboxFull. Stays red
    # until the error.rs:305 conversion is fixed.

  @boundary @bug:ask.rs:461
  Scenario: blocking_forward waits for target capacity instead of returning Full
    Given a router actor and a target actor whose bounded mailbox is momentarily full
    When the router's handler calls blocking_forward and a slot then frees
    Then the message is forwarded once capacity is available
    # Confirmed: blocking_forward uses ask(...).blocking_forward(...) which blocks the
    # thread for capacity (:326-345), unlike try_forward which fails fast.
    # @bug (MUST FAIL today): desired — blocking_forward blocks for capacity and
    # forwards once a slot frees. Actual — AskRequest::blocking_forward calls tokio
    # `tx.blocking_send(signal)` (ask.rs:461), which PANICS ("Cannot block the
    # current thread from within a runtime") whenever called from an async context,
    # and every bombay handler runs on a tokio runtime worker — so calling
    # ctx.blocking_forward from a handler always panics. Stays red until the API
    # guards the misuse-in-async or the scenario is re-specified.

  @boundary
  Scenario: a handler that returns Err on an ask routes the error to the caller as HandlerError
    When a message whose handler returns Err(e) is sent via ask
    Then the caller's ask result is Err with a HandlerError carrying e
    # Confirmed: handle_dyn with Some(tx) sends reply.into_value() through the reply
    # channel; Result's into_value is itself, and ReplySender::send maps Err into
    # BoxSendError::HandlerError (src/reply.rs:174-184, :585-588).

  @boundary
  Scenario: a handler that returns Err on a tell routes the error to the panic hook, not a caller
    When a message whose handler returns Err(e) is sent via tell so no caller awaits
    Then handle_dyn surfaces the error via into_any_err for the run-loop to treat as a panic
    And the actor's on_panic hook is invoked per the Reply doc
    # Confirmed: handle_dyn with None reply sender returns Err(err) from
    # reply.into_any_err() (:415-420); the Reply module doc states an unhandled tell error
    # is treated as a panic, triggering on_panic (src/reply.rs:16-19).

  @boundary
  Scenario: a successful handler reply on a tell produces no error and no hook invocation
    When a message whose handler returns Ok is sent via tell
    Then handle_dyn returns Ok and neither the error hook nor on_panic is invoked
    # Confirmed: None reply sender + into_any_err() == None yields Ok(()) (:415-420).

  # ---------------------------------------------------------------------------
  # @linearizability — concurrent senders observe single-writer serialization
  # ---------------------------------------------------------------------------

  @linearizability
  Scenario: Concurrent asks from many tasks are each answered exactly once
    Given a counter actor that increments and replies with the new count on each ask
    When 50 tasks concurrently ask the actor once each
    Then every task receives a distinct reply
    And the set of replies is exactly the integers 1 through 50 with no gaps or duplicates
    # Confirmed: sequential single-writer processing (:36-38) means increments cannot
    # interleave; concurrency is real (50 spawned tasks) but the actor serializes them.

  @linearizability
  Scenario: Interleaved asks and tells preserve single-writer state consistency
    Given an actor whose state is mutated by both ask and tell commands
    When concurrent tasks send a mix of asks and tells
    Then the final state equals the deterministic result of applying every command once
    And no command is lost or applied twice
