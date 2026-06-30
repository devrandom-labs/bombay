# Scope: bombay core `AskRequest` (src/request/ask.rs + src/request.rs) — the
#        request-with-reply builder: ask(M) → optional mailbox_timeout /
#        reply_timeout → send / try_send / blocking_send / enqueue / try_enqueue /
#        blocking_enqueue / forward / try_forward / blocking_forward / IntoFuture.
#
# Authoring rules (mirror tests/features/actors/message_queue.feature exactly):
#   * Exactly ONE cross-cutting tag per Scenario: @sequence | @lifecycle |
#     @boundary | @linearizability.
#   * Invariant-first: the `Then` names the observable guarantee. If it cannot be
#     stated without reading the impl, write a `# NOTE:` + @review-semantics tag.
#   * @bug:<file:line> marks a scenario that MUST FAIL today.
#   * Facts only: every Then is grounded in src/request/ask.rs, src/request.rs, or
#     src/error.rs (SendError variants). Guesses become @review-semantics.
#   * No step definitions here — steps are written in the wiring phase.
#
# Grounding facts (src/request/ask.rs, src/error.rs):
#   * Two independent timeouts. mailbox_timeout guards waiting for mailbox CAPACITY
#     and, on expiry, surfaces SendError::Timeout(Some(msg)) — the message is
#     returned because it was never enqueued (mailbox tx.send_timeout). reply_timeout
#     guards waiting for the REPLY (tokio::time::timeout over the oneshot rx) and, on
#     expiry, surfaces SendError::Timeout(None) — the message was already enqueued so
#     there is nothing to hand back.  (Confirmed by the two in-file tests
#     bounded_ask_requests_mailbox_timeout → Timeout(Some), *_reply_timeout → Timeout(None).)
#   * send(): mailbox wait then reply wait. try_send(): tx.try_send (no mailbox wait)
#     then reply wait. blocking_send(): tx.blocking_send + rx.blocking_recv, no timeouts.
#   * Closed/absent mailbox → SendError::ActorNotRunning(msg).
#   * Full bounded mailbox via try_send → SendError::MailboxFull(msg).
#   * enqueue/try_enqueue return a PendingReply (a Future); the actor does NOT
#     progress past the handler until that reply is awaited or dropped.

@core @request_ask
Feature: AskRequest — request/reply with mailbox and reply timeouts
  As a caller asking an actor for a reply
  I want independent control over waiting for mailbox capacity and for the reply
  So that a slow or absent actor surfaces a typed error instead of hanging

  Background:
    Given a running actor whose handler can be made to sleep for a given duration

  # ---------------------------------------------------------------------------
  # @sequence — the builder protocol: ask → (timeouts) → terminal send method
  # ---------------------------------------------------------------------------

  @sequence
  Scenario: A plain ask awaits the reply and returns the handler's Ok value
    Given the actor has a bounded mailbox of capacity 100
    When the caller sends "ask(Msg)" and awaits it directly
    Then the caller receives the handler's Ok reply

  @sequence
  Scenario: send, try_send and blocking_send all deliver and return the same reply on an idle actor
    Given the actor has a bounded mailbox of capacity 100 and is idle
    When the caller invokes "ask(Msg).send()"
    And the caller invokes "ask(Msg).try_send()"
    And the caller invokes "ask(Msg).blocking_send()" on a blocking thread
    Then each call returns the handler's Ok reply
    # Formalises the in-file `bounded_ask_requests` / `unbounded_ask_requests` tests.

  @sequence
  Scenario: enqueue returns a pending reply that resolves to the Ok value when awaited
    Given the actor has a bounded mailbox of capacity 100
    When the caller invokes "ask(Msg).enqueue()" and holds the returned pending reply
    And the caller later awaits the pending reply
    Then the awaited pending reply yields the handler's Ok reply

  @sequence
  Scenario: forward routes the reply to a supplied channel instead of the caller
    Given the actor has a bounded mailbox of capacity 100
    And a reply channel is created
    When the caller invokes "ask(Msg).forward(sender)"
    Then the reply is delivered to the channel, not returned to the caller
    And the forward call itself returns Ok(())

  @boundary @bug:error.rs:305
  Scenario: try_forward to a full bounded mailbox returns MailboxFull carrying back the channel
    Given the actor has a bounded mailbox of capacity 1 that is already full
    And a reply channel is created
    When the caller invokes "ask(Msg).try_forward(sender)"
    Then the call returns Err(SendError::MailboxFull) carrying both the message and the reply channel
    And no reply is delivered to the channel
    # NOTE (ask.rs:281-302): try_forward does `tx.try_send(signal)?`; the SendError's message
    # payload is `(M, ReplySender)`, so the caller recovers BOTH the un-sent message and the
    # reply channel to retry. (This differs from message.rs ctx.try_forward, which instead
    # restores ctx.reply in-handler — a separate API.)
    # @bug:error.rs:305 — DESIRED above is ABSENT today. The `?` on `tx.try_send(signal)`
    # converts `TrySendError<Signal>` → `SendError<(M, ReplySender), _>` via
    # `From` (error.rs:305), which calls `signal.downcast_message::<(M, ReplySender)>().unwrap()`.
    # But the signal stores the BARE message `Box::new(self.msg)` (ask.rs:289), NOT the
    # `(M, ReplySender)` tuple, so the downcast is `None` and `.unwrap()` PANICS
    # ("called `Option::unwrap()` on a `None` value") on the caller's thread — no graceful
    # MailboxFull is ever returned. Verified by a throwaway probe (panicked at error.rs:305:71)
    # and pinned by the `bug_try_forward_full_panics` red-on-fix probe in the runner. Same
    # root cause as message.feature's @bug:error.rs:305.

  @boundary @bug:error.rs:293
  Scenario: forward to a stopped actor returns ActorNotRunning carrying back the channel
    Given the actor has been stopped
    And a reply channel is created
    When the caller invokes "ask(Msg).forward(sender)"
    Then the call returns Err(SendError::ActorNotRunning) carrying both the message and the reply channel
    And no reply is delivered to the channel
    # NOTE (ask.rs:239-270): forward awaits `tx.send(signal).await?`; a closed mailbox yields
    # SendError::ActorNotRunning((msg, sender)) — the un-sent message and channel are returned,
    # never silently dropped (rule 3: a dead actor is ActorNotRunning, not a capacity error).
    # @bug:error.rs:293 — DESIRED above is ABSENT today. The `?` on `tx.send(signal).await`
    # converts `SendError<Signal>` → `SendError<(M, ReplySender), _>` via `From` (error.rs:293),
    # calling `err.0.downcast_message::<(M, ReplySender)>().unwrap()`. The signal stores the BARE
    # message (ask.rs:250), so the downcast is `None` and `.unwrap()` PANICS on the caller's
    # thread — no graceful ActorNotRunning is returned. Verified by a throwaway probe (panicked
    # at error.rs:293:66) and pinned by the `bug_forward_stopped_panics` red-on-fix probe in the
    # runner. Same root cause as message.feature's @bug:error.rs:293.

  @sequence
  Scenario: blocking_forward waits for capacity then delivers the reply to the channel
    Given the actor has a bounded mailbox of capacity 1 that is momentarily full
    And a reply channel is created
    When the caller invokes "ask(Msg).blocking_forward(sender)" on a blocking thread
    And the actor frees one mailbox slot
    Then the call returns Ok(()) once capacity is available
    And the reply is delivered to the channel
    # NOTE (ask.rs:443-464): blocking_forward parks on `tx.blocking_send(signal)?` until a slot
    # frees, unlike try_forward which fails fast with MailboxFull.

  @sequence
  Scenario: a handler that returns an Err surfaces as SendError::HandlerError
    Given the actor's handler returns a typed Err for this message
    When the caller sends "ask(Msg)" and awaits it
    Then the caller receives SendError::HandlerError carrying the handler's typed error

  # ---------------------------------------------------------------------------
  # @boundary — timeouts (each kind, both kinds, extremes) and closed/full mailbox
  # ---------------------------------------------------------------------------

  @boundary @timing
  Scenario: mailbox_timeout expiring returns Timeout(Some(msg)) with the message handed back
    Given the actor has a bounded mailbox of capacity 1
    And the mailbox is occupied so it has no spare capacity
    When the caller sends "ask(Msg)" with a mailbox_timeout of 50ms
    Then the caller receives SendError::Timeout(Some(Msg))
    # Grounded: bounded_ask_requests_mailbox_timeout — message returned because never enqueued.

  @boundary @timing
  Scenario: reply_timeout expiring returns Timeout(None) because the message was already enqueued
    Given the actor has a bounded mailbox of capacity 100
    And the handler will sleep 100ms before replying
    When the caller sends "ask(Sleep(100ms))" with a reply_timeout of 90ms
    Then the caller receives SendError::Timeout(None)
    # Grounded: bounded_ask_requests_reply_timeout / unbounded_ask_requests_reply_timeout.

  @boundary @timing
  Scenario: a reply that arrives just inside reply_timeout returns Ok
    Given the actor has a bounded mailbox of capacity 100
    And the handler will sleep 100ms before replying
    When the caller sends "ask(Sleep(100ms))" with a reply_timeout of 120ms
    Then the caller receives the handler's Ok reply
    # Grounded: first assertion of bounded_ask_requests_reply_timeout (Ok(true)).

  @boundary @timing
  Scenario: both timeouts set — mailbox capacity is awaited first, then the reply
    Given the actor has a bounded mailbox of capacity 1
    And the mailbox is occupied so it has no spare capacity
    When the caller sends "ask(Sleep(100ms))" with a mailbox_timeout of 50ms and a reply_timeout of 1s
    Then the caller receives SendError::Timeout(Some(Sleep(100ms)))
    # send() awaits mailbox capacity (send_timeout) BEFORE starting the reply_timeout
    # clock (ask.rs:137-154); with capacity unavailable the mailbox timeout fires
    # first and the message comes back as Some(..).

  @boundary @timing
  Scenario: both timeouts set — once enqueued, a slow reply fails with Timeout(None)
    Given the actor has a bounded mailbox of capacity 100 with spare capacity
    And the handler will sleep 200ms before replying
    When the caller sends "ask(Sleep(200ms))" with a mailbox_timeout of 1s and a reply_timeout of 50ms
    Then the caller receives SendError::Timeout(None)
    # NOTE: capacity is available so the mailbox wait succeeds immediately; the
    # reply_timeout then governs and fires with None (message already enqueued).

  @boundary @timing
  Scenario: a reply_timeout of zero fails immediately with Timeout(None)
    Given the actor has a bounded mailbox of capacity 100
    And the handler sleeps before replying
    When the caller sends "ask(Sleep)" with a reply_timeout of 0ms
    Then the caller receives SendError::Timeout(None)
    # tokio::time::timeout(Duration::ZERO, ..) elapses on first poll; the rx never resolves first.

  @boundary @timing
  Scenario: a Duration::MAX reply_timeout behaves as effectively unbounded
    Given the actor has a bounded mailbox of capacity 100
    And the handler replies promptly
    When the caller sends "ask(Msg)" with a reply_timeout of Duration::MAX
    Then the caller receives the handler's Ok reply without the timeout firing
    # tokio saturates the timer wheel for very large durations rather than panicking
    # (ask.rs:137-154); the reply arrives and the timeout never fires.

  @boundary
  Scenario: asking a stopped actor over a bounded mailbox returns ActorNotRunning(msg)
    Given the actor has a bounded mailbox of capacity 100
    And the actor has been stopped gracefully and shutdown has completed
    When the caller invokes "ask(Msg).send()"
    Then the caller receives SendError::ActorNotRunning(Msg)
    And "ask(Msg).try_send()" also returns SendError::ActorNotRunning(Msg)
    And "ask(Msg).blocking_send()" also returns SendError::ActorNotRunning(Msg)
    # Grounded: bounded_ask_requests_actor_not_running (and the unbounded twin).

  @boundary
  Scenario: try_send into a full bounded mailbox returns MailboxFull(msg) without waiting
    Given the actor has a bounded mailbox of capacity 1
    And the mailbox is filled to capacity while the actor is busy in its handler
    When the caller invokes "ask(Msg).try_send()"
    Then the caller receives SendError::MailboxFull(Msg)
    # Grounded: bounded_ask_requests_mailbox_full. NOTE the in-file comment: a
    # bounded(1) mailbox needs TWO sends to actually occupy the slot (the first is
    # dequeued by the actor), so the test fills to fill_count before asserting —
    # the earlier single-send race is already fixed.

  # ---------------------------------------------------------------------------
  # @lifecycle — actor death mid-flight; pending-reply suspends the actor
  # ---------------------------------------------------------------------------

  @lifecycle
  Scenario: an actor that panics inside the handler fails the in-flight ask rather than hanging
    Given the actor has a bounded mailbox of capacity 100
    And the handler panics when handling this message
    When the caller sends "ask(Msg)" and awaits it
    Then the caller receives a SendError rather than hanging forever
    # NOTE @review-semantics: the oneshot reply sender is dropped when the run-loop
    # unwinds, so rx.await resolves to a RecvError → SendError. Pin the exact variant
    # (ActorStopped vs ActorNotRunning) at wiring; the load-bearing invariant here is
    # NO HANG. This guards the "ask must not deadlock on handler panic" path.

  @lifecycle
  Scenario: killing the actor while an ask is parked on the reply fails the caller
    Given the actor has a bounded mailbox of capacity 100
    And the handler sleeps long enough that the reply is still pending
    When the caller has an outstanding "ask(Sleep)" awaiting the reply
    And the actor is killed
    Then the outstanding ask resolves to a SendError rather than hanging
    # NOTE @review-semantics: pin the exact SendError variant at wiring (reply sender dropped).

  @lifecycle
  Scenario: a bounded(1) mailbox returns MailboxFull only once both the in-flight and queued slots are taken
    Given the actor has a bounded mailbox of capacity 1
    And its handler blocks long enough to stay in-flight (the actor does not progress)
    When a first message is sent and the actor dequeues it, freeing the single slot as it enters the blocked handler
    And a second message is sent and now occupies the one bounded(1) slot
    And the caller then calls "ask(Msg).try_send()" for a third message
    Then the third send fails with exactly Err(SendError::MailboxFull(Msg))
    # Confirmed by the source test request/ask.rs:1265-1289 (and its comment): the FIRST send is
    # dequeued by the actor — which then blocks in the handler — FREEING the single slot, so it
    # takes a SECOND send to actually occupy the mailbox before a THIRD send observes a full one.
    # A single in-flight message races the actor draining it, so an "enqueue one and hold" framing
    # could NOT deterministically yield MailboxFull (this corrects the earlier @review-semantics
    # guess). try_send forwards tokio's TrySendError::Full as SendError::MailboxFull(msg) carrying
    # the rejected message back to the caller (request/ask.rs:311-329).

  # ---------------------------------------------------------------------------
  # @linearizability — concurrent asks with real overlap
  # ---------------------------------------------------------------------------

  @linearizability @timing
  Scenario: among N concurrent asks one with a short reply_timeout fails and the rest succeed
    Given the actor has a bounded mailbox of capacity 100
    And the handler sleeps 100ms before replying
    When 10 callers concurrently send "ask(Sleep(100ms))" and one of them uses a reply_timeout of 10ms
    Then the short-timeout caller receives SendError::Timeout(None)
    And every other caller receives the handler's Ok reply
    # Real overlap (tokio::spawn + Barrier), not sequential-then-check.

  @linearizability
  Scenario: concurrent asks under a bounded capacity are each answered exactly once with no cross-talk
    Given the actor has a bounded mailbox of capacity 4
    And the handler echoes back the integer it received
    When 50 callers concurrently send "ask(n)" each with a distinct integer n
    Then each caller receives the Ok reply equal to the n it sent
    And no reply is delivered to the wrong caller and none is lost
    # Linearizability: distinct-value oracle pins per-caller reply isolation.

  @linearizability @timing
  Scenario: blocking_send from worker threads concurrently each receive their own reply
    Given the actor has a bounded mailbox of capacity 4
    And the handler echoes back the integer it received
    When 8 OS threads each invoke "ask(n).blocking_send()" with a distinct n under a barrier
    Then each thread receives the Ok reply equal to its own n
