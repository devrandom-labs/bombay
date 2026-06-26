# Scope: kameo core `TellRequest` (src/request/tell.rs + src/request.rs) — the
#        fire-and-forget builder: tell(M) → optional mailbox_timeout →
#        send / try_send / blocking_send / send_after / IntoFuture.
#
# Authoring rules (mirror tests/features/actors/message_queue.feature exactly):
#   * Exactly ONE cross-cutting tag per Scenario: @sequence | @lifecycle |
#     @boundary | @linearizability.
#   * Invariant-first Then; unverifiable → `# NOTE:` + @review-semantics.
#   * @bug:<file:line> marks a scenario that MUST FAIL today.
#   * Facts only: every Then is grounded in src/request/tell.rs, src/request.rs,
#     or src/error.rs. Guesses become @review-semantics.
#   * No step definitions here.
#
# Grounding facts (src/request/tell.rs):
#   * Tell carries NO reply (Signal reply: None) and returns Result<(), SendError<M>>.
#   * send(): bounded mailbox → tx.send / tx.send_timeout (WAITS for capacity);
#     unbounded → never waits. mailbox_timeout only matters on a bounded mailbox
#     (an unbounded send never blocks on capacity, so the timeout cannot fire).
#   * try_send(): tx.try_send — no wait; full bounded mailbox → MailboxFull(msg).
#   * blocking_send(): tx.blocking_send — blocks the calling OS thread for capacity.
#   * send_after(duration): spawns a tokio task that sleeps then sends, returning a
#     JoinHandle<Result<(), SendError<M>>>; JoinHandle::abort cancels the pending send.
#   * Closed mailbox → SendError::ActorNotRunning(msg).
#   * mailbox_timeout expiry on a bounded send → SendError::Timeout(Some(msg)).
#   * debug + tracing: a bounded self-tell (actor_ref.is_current() && capacity.is_some())
#     emits a deadlock warning (warn_deadlock); it does NOT change the return value.

@core @request_tell
Feature: TellRequest — fire-and-forget send with optional mailbox backpressure
  As a caller telling an actor something without needing a reply
  I want bounded sends to respect backpressure and try/blocking/delayed variants
  So that delivery either succeeds, is refused with a typed error, or is scheduled

  Background:
    Given a running actor whose handler can be made to sleep for a given duration

  # ---------------------------------------------------------------------------
  # @sequence — builder protocol and the capacity/no-capacity contract
  # ---------------------------------------------------------------------------

  @sequence
  Scenario: send, try_send and blocking_send all deliver to an idle actor and return Ok(())
    Given the actor has a bounded mailbox of capacity 100 and is idle
    When the caller invokes "tell(Msg).send()"
    And the caller invokes "tell(Msg).try_send()"
    And the caller invokes "tell(Msg).blocking_send()" on a blocking thread
    Then each call returns Ok(())
    And the actor eventually handles all three messages
    # Formalises the in-file bounded_tell_requests / unbounded_tell_requests tests.

  @sequence @timing
  Scenario: a bounded send waits for capacity and then succeeds when the actor drains a slot
    Given the actor has a bounded mailbox of capacity 1
    And the single slot is currently occupied by an in-flight message
    When the caller invokes "tell(Msg).send()" with no mailbox_timeout
    Then the send does not return until the actor frees a slot
    And the send then returns Ok(())
    # send() on a bounded mailbox parks on tx.send until room appears (tell.rs:115-118).

  @sequence
  Scenario: try_send never waits for capacity
    Given the actor has a bounded mailbox with spare capacity
    When the caller invokes "tell(Msg).try_send()"
    Then the call returns Ok(()) immediately without awaiting capacity
    # try_send uses tx.try_send — no parking (tell.rs:169-181).

  @sequence
  Scenario: an unbounded send ignores mailbox_timeout because it never waits for capacity
    Given the actor has an unbounded mailbox
    When the caller invokes "tell(Msg).mailbox_timeout(1ms).send()"
    Then the call returns Ok(()) and the mailbox_timeout never fires
    # NOTE: unbounded tx.capacity() is None; send never blocks on capacity, so the
    # timeout branch cannot elapse. Grounded in unbounded mailbox semantics.

  # ---------------------------------------------------------------------------
  # @boundary — timeout, full mailbox, dead actor, self-tell warning, extremes
  # ---------------------------------------------------------------------------

  @boundary @timing
  Scenario: a bounded send whose mailbox_timeout expires returns Timeout(Some(msg))
    Given the actor has a bounded mailbox of capacity 1
    And the mailbox is occupied so it has no spare capacity
    When the caller invokes "tell(Sleep(100ms)).mailbox_timeout(50ms).send()"
    Then the caller receives SendError::Timeout(Some(Sleep(100ms)))
    # Grounded: bounded_tell_requests_mailbox_timeout — message returned (never enqueued).

  @boundary
  Scenario: try_send into a full bounded mailbox returns MailboxFull(msg)
    Given the actor has a bounded mailbox of capacity 1
    And the mailbox is filled to capacity while the actor is busy in its handler
    When the caller invokes "tell(Msg).try_send()"
    Then the caller receives SendError::MailboxFull(Msg)
    # Grounded: bounded_tell_requests_mailbox_full (fill_count handles the bounded(1) drain race).

  @boundary
  Scenario: telling a stopped actor returns ActorNotRunning(msg) for every send variant
    Given the actor has a bounded mailbox of capacity 100
    And the actor has been stopped gracefully and shutdown has completed
    When the caller invokes "tell(Msg).send()"
    Then the caller receives SendError::ActorNotRunning(Msg)
    And "tell(Msg).try_send()" also returns SendError::ActorNotRunning(Msg)
    And "tell(Msg).blocking_send()" also returns SendError::ActorNotRunning(Msg)
    # Grounded: bounded_/unbounded_tell_requests_actor_not_running.

  @boundary @timing
  Scenario: a bounded mailbox_timeout of zero on a full mailbox fails immediately with Timeout(Some)
    Given the actor has a bounded mailbox of capacity 1
    And the mailbox is occupied so it has no spare capacity
    When the caller invokes "tell(Msg).mailbox_timeout(0ms).send()"
    Then the caller receives SendError::Timeout(Some(Msg))
    # send_timeout(.., ZERO) cannot acquire a busy slot, elapses immediately, returns the msg.

  @boundary
  Scenario: a debug-build bounded self-tell emits a deadlock warning but still returns Ok
    Given a tracing-enabled debug build
    And an actor sending a bounded "tell" to itself from within its own handler
    When the self-tell is dispatched with spare capacity available
    Then a deadlock warning is emitted naming the call site
    And the send still returns Ok(()) — the warning does not alter the result
    # warn_deadlock fires only when capacity.is_some() && is_current() (tell.rs:96-102,611-622).
    # @review-semantics: assert the warning via a tracing capture layer at wiring time.

  # ---------------------------------------------------------------------------
  # @lifecycle — on_start buffering, dead actor, send_after scheduling/abort
  # ---------------------------------------------------------------------------

  @lifecycle
  Scenario: a tell issued during on_start is buffered and handled once the actor is running
    Given an actor whose on_start blocks until released
    When the caller invokes "tell(Msg).send()" before on_start has completed
    And on_start is then released
    Then the message is handled after startup rather than rejected
    # The mailbox accepts the message while the actor is starting; on_start buffering
    # delivers it after startup rather than returning ActorNotRunning.

  @lifecycle @timing
  Scenario: send_after returns an abortable JoinHandle that delivers after the delay
    Given the actor has a bounded mailbox of capacity 100
    When the caller invokes "tell(Msg).send_after(50ms)"
    And the caller awaits the returned JoinHandle
    Then the handle resolves to Ok(()) and the message was delivered after the delay
    # send_after spawns sleep-then-send, returning JoinHandle<Result<(),SendError>> (tell.rs:128-160).

  @lifecycle @timing
  Scenario: aborting the send_after JoinHandle before the delay prevents delivery
    Given the actor has a bounded mailbox of capacity 100
    When the caller invokes "tell(Msg).send_after(1s)"
    And the caller aborts the returned JoinHandle before the delay elapses
    Then the message is never delivered to the actor
    And awaiting the aborted handle reports cancellation
    # JoinHandle::abort cancels the spawned task before it sends.

  @lifecycle @timing
  Scenario: send_after with a zero delay sends on the next scheduler tick
    Given the actor has a bounded mailbox of capacity 100
    When the caller invokes "tell(Msg).send_after(0ms)"
    And the caller awaits the returned JoinHandle
    Then the handle resolves to Ok(()) and the message is delivered
    # tokio::time::sleep(ZERO) yields once, then the spawned send runs; the JoinHandle
    # resolves Ok(()) and the message is delivered (tell.rs:128-160).

  @lifecycle @timing
  Scenario: send_after targeting an actor that stops before the delay reports ActorNotRunning
    Given the actor has a bounded mailbox of capacity 100
    When the caller invokes "tell(Msg).send_after(200ms)"
    And the actor is stopped and shutdown completes before the delay elapses
    And the caller awaits the returned JoinHandle
    Then the handle resolves to Err(SendError::ActorNotRunning(Msg))
    # The deferred send hits a closed mailbox.

  # ---------------------------------------------------------------------------
  # @linearizability — concurrent tells with real overlap
  # ---------------------------------------------------------------------------

  @linearizability
  Scenario: concurrent try_sends under a bounded capacity deliver every accepted message exactly once
    Given the actor has a bounded mailbox of capacity 8
    And the handler records each integer it receives
    When 100 callers concurrently invoke "tell(n).try_send()" with distinct integers under a barrier
    Then every call that returned Ok(()) had its integer recorded exactly once
    And every call that returned MailboxFull had its integer NOT recorded
    # Real overlap; no message accepted-then-lost and no duplicate.

  @linearizability @timing
  Scenario: concurrent bounded sends all eventually deliver as the actor drains
    Given the actor has a bounded mailbox of capacity 2
    And the handler records each integer after a short delay
    When 20 callers concurrently invoke "tell(n).send()" with distinct integers under a barrier
    Then every integer is recorded exactly once once all sends complete
    # Bounded send backpressures rather than dropping; total delivery is preserved.
