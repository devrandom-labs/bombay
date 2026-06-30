# Scope: bombay core `Mailbox` (src/mailbox.rs) — a multi-producer single-consumer
#        signal channel between actors. Thin typed wrapper over tokio's
#        `mpsc::channel` (bounded) and `mpsc::unbounded_channel` (unbounded), plus a
#        `front: VecDeque<Signal<A>>` push-back buffer used to re-queue pending signals
#        across an actor restart.
#
# Authoring rules (apply to ALL feature files):
#   * Every Scenario carries exactly ONE cross-cutting tag:
#       @sequence | @lifecycle | @boundary | @linearizability
#   * Invariant-first: the `Then` names the observable guarantee. If the Then cannot be
#     stated without reading the implementation, write it as a `# NOTE:` + @review-semantics
#     rather than asserting a guess.
#   * @bug:<file:line> marks a scenario that MUST FAIL today (reproduces a real defect).
#   * Facts only: a Then asserts behaviour confirmed from src/mailbox.rs or from tokio's
#     documented mpsc contract, never a plausible guess.
#   * No step definitions here. Steps are written in the wiring phase.

@core @mailbox
Feature: Mailbox — bounded/unbounded signal channel with restart push-front
  As the actor runtime delivering messages and lifecycle signals to an actor task
  I want a typed mpsc channel whose backpressure, closure, and ordering are precise
  So that message delivery, restart re-queueing, and shutdown are observable and correct

  # ---------------------------------------------------------------------------
  # @sequence — ordering, batching, push-front protocol on the same channel
  # ---------------------------------------------------------------------------

  @sequence
  Scenario: FIFO delivery — signals are received in the order they were sent
    Given a bounded mailbox with capacity 8
    When signals S1, S2, S3 are sent in that order
    And the receiver calls recv three times
    Then the receiver yields S1, then S2, then S3 in that exact order

  @sequence
  Scenario: push_front drains the re-queued signals before anything still in the channel
    Given a bounded mailbox with capacity 8
    And signals C1, C2 are already queued in the channel
    When the receiver push_fronts the ordered signals F1, F2
    And the receiver calls recv four times
    Then the receiver yields F1, F2, C1, C2 in that exact order
    # Confirmed: recv()/try_recv()/blocking_recv()/poll_recv() all pop self.front first
    # (src/mailbox.rs:574-577, :637-639, :666-668, :785-787); push_front prepends and
    # preserves order (:556-559).

  @sequence
  Scenario: blocking_recv pops the front buffer before the channel
    Given a bounded mailbox with capacity 8
    And signals C1, C2 are already queued in the channel
    When the receiver push_fronts the ordered signals F1, F2
    And the receiver calls blocking_recv four times on a blocking thread
    Then the receiver yields F1, F2, C1, C2 in that exact order
    # Confirmed: blocking_recv pops self.front first (src/mailbox.rs:665-668) — the existing
    # recv scenario only exercises the async recv(); this pins the blocking variant directly.

  @sequence
  Scenario: poll_recv pops the front buffer before the channel
    Given a bounded mailbox with capacity 8
    And signals C1, C2 are already queued in the channel
    When the receiver push_fronts the ordered signals F1, F2
    And the receiver is polled via poll_recv four times
    Then poll_recv yields Ready(F1), Ready(F2), Ready(C1), Ready(C2) in that exact order
    # Confirmed: poll_recv pops self.front first (src/mailbox.rs:784-787) before polling the
    # channel; pins the Future-driving variant the recv scenario does not reach.

  @sequence
  Scenario: blocking_recv_many and poll_recv_many never mix the front buffer with the channel in one call
    Given a bounded mailbox with capacity 8
    And signals C1, C2 are already queued in the channel
    When the receiver push_fronts F1, F2 and calls blocking_recv_many with limit 8
    Then the call returns exactly F1, F2 (count 2) and leaves C1, C2 for the next call
    And poll_recv_many behaves identically when the front buffer is non-empty
    # Confirmed: recv_many variants drain only the front when it is non-empty
    # (src/mailbox.rs:604-606, :694-732, :815-847); a single call never mixes front + channel.

  @sequence
  Scenario: push_front called twice preserves earlier re-queued signals ahead of later ones
    Given a bounded mailbox with capacity 8
    When the receiver push_fronts F1, F2
    And the receiver push_fronts F3, F4
    And the receiver calls recv four times
    Then the receiver yields F3, F4, F1, F2 in that exact order
    # Confirmed: push_front appends the existing front onto the new batch (:556-558),
    # so the most-recently pushed batch is drained first.

  @sequence
  Scenario: recv_many batches all currently-available channel signals up to the limit
    Given a bounded mailbox with capacity 8
    And 5 signals are queued in the channel
    When the receiver calls recv_many with limit 10 into an empty buffer
    Then the returned count is exactly 5
    And exactly those 5 signals are appended to the buffer, so count == buffer.len()
    # Confirmed: front is empty, so recv_many delegates to tokio's rx.recv_many(buffer, limit)
    # (mailbox.rs:603-610). All 5 signals are already queued and there is no concurrent sender,
    # so tokio drains every immediately-available value up to limit (10) in one call — the count
    # is the deterministic 5, not a 1..=5 range (the limit only caps; it does not undercount what
    # is already buffered).

  @sequence
  Scenario: recv_many drains only the front buffer when the front is non-empty
    Given a bounded mailbox with capacity 8
    And 3 signals are queued in the channel
    When the receiver push_fronts F1, F2
    And the receiver calls recv_many with limit 10
    Then exactly 2 signals (F1, F2) are appended and the count is 2
    And the 3 channel signals remain unreceived
    # Confirmed: recv_many returns early via drain_front_into when front is non-empty
    # (:604-606), so a single call never mixes front and channel signals.

  @sequence
  Scenario: recv_many honours the limit when the front holds more than the limit
    Given a bounded mailbox with capacity 8
    When the receiver push_fronts F1, F2, F3
    And the receiver calls recv_many with limit 2
    Then exactly 2 signals (F1, F2) are appended and the count is 2
    And a subsequent recv yields F3
    # Confirmed: drain_front_into takes front.len().min(limit) (:562-565).

  @sequence
  Scenario: len counts both the front buffer and the channel backlog
    Given a bounded mailbox with capacity 8
    And 2 signals are queued in the channel
    When the receiver push_fronts F1, F2, F3
    Then len returns 5
    # Confirmed: len() = self.front.len() + inner.len() (:770-776).

  @sequence
  Scenario: is_empty is false while the front buffer holds signals even if the channel is empty
    Given a bounded mailbox with capacity 8 whose channel is empty
    When the receiver push_fronts F1
    Then is_empty returns false
    # Confirmed: is_empty short-circuits to false when front is non-empty (:753-756).

  # ---------------------------------------------------------------------------
  # @lifecycle — close / drop / weak-upgrade across the channel's lifetime
  # ---------------------------------------------------------------------------

  @lifecycle
  Scenario: After the receiver is dropped, send on a bounded sender returns the closed error carrying the signal
    Given a bounded mailbox with capacity 8
    When the receiver is dropped
    And the sender sends signal S
    Then send returns Err and the returned error carries the original signal S
    # Confirmed: bounded send delegates to tokio mpsc::Sender::send, whose SendError
    # returns the un-sent value when all receivers are gone (:150-157).

  @lifecycle
  Scenario: After the receiver calls close, send returns the closed error
    Given a bounded mailbox with capacity 8
    When the receiver calls close
    And the sender sends signal S
    Then send returns Err carrying signal S
    And the sender's is_closed returns true
    # Confirmed: close() closes the receiving half without dropping it (:727-731);
    # is_closed reflects receiver drop OR close (:265-278).

  @lifecycle
  Scenario: A receiver that closed mid-stream still yields already-buffered signals before ending
    Given a bounded mailbox with capacity 8
    And signals S1, S2 are queued in the channel
    When the receiver calls close
    And the receiver calls recv repeatedly
    Then recv yields S1, then S2, then None
    # NOTE @review-semantics: this is tokio's documented close() behaviour (buffered
    # messages are still received, then the stream ends). Pin against the tokio version
    # in the lockfile at wiring time before asserting the exact drain-then-None order.

  @lifecycle
  Scenario: is_closed on the sender becomes true only when the last strong sender is dropped relative to the receiver
    Given a bounded mailbox with capacity 8
    And the sender is cloned so two strong senders exist
    When one strong sender is dropped
    Then the surviving sender's is_closed returns false
    And strong_count on the surviving sender returns 1

  @lifecycle
  Scenario: Dropping every strong sender lets the receiver observe end-of-stream
    Given a bounded mailbox with capacity 8
    And no signals are queued
    When all strong senders are dropped
    And the receiver calls recv
    Then recv returns None
    # Confirmed: sender_strong_count drives channel closure; tokio mpsc closes the
    # receiver once the last Sender is dropped. recv then yields None (:574-595).

  @lifecycle
  Scenario: A weak sender upgrades to a sender while a strong sender is alive
    Given a bounded mailbox with capacity 8
    And a weak sender is downgraded from the strong sender
    When upgrade is called on the weak sender
    Then upgrade returns Some

  @lifecycle
  Scenario: A weak sender fails to upgrade after every strong sender is dropped
    Given a bounded mailbox with capacity 8
    And a weak sender is downgraded from the strong sender
    When every strong sender is dropped
    And upgrade is called on the weak sender
    Then upgrade returns None
    # Confirmed: WeakMailboxSender::upgrade returns None once no strong senders remain
    # and the channel was not previously closed (:448-469).

  @lifecycle
  Scenario: A weak sender does not keep the channel alive
    Given a bounded mailbox with capacity 8
    And only a weak sender remains after the strong sender is dropped
    When the receiver calls recv
    Then recv returns None
    # Confirmed: WeakMailboxSender does not count toward RAII (:323-332 doc + tokio
    # WeakSender semantics) — a channel with only weak senders is closed.

  # ---------------------------------------------------------------------------
  # @boundary — capacity, try_send/Full, unbounded-never-blocks, kind distinctions
  # ---------------------------------------------------------------------------

  @boundary
  Scenario: try_send on a full bounded channel returns Full carrying the signal
    Given a bounded mailbox with capacity 1
    And the single capacity slot is occupied by an unreceived signal
    When the sender try_sends signal S
    Then try_send returns Err with the Full variant carrying signal S
    # Confirmed: bounded try_send delegates to tokio mpsc::Sender::try_send, which
    # returns TrySendError::Full(value) when at capacity (:175-184).

  @boundary
  Scenario: try_send on an unbounded channel never returns Full
    Given an unbounded mailbox
    And 1000 signals are already queued and unreceived
    When the sender try_sends one more signal
    Then try_send returns Ok
    # Confirmed: the unbounded arm maps only Closed; it has no Full path (:181-184).

  @boundary
  Scenario: try_send on an unbounded channel returns Closed after the receiver is dropped
    Given an unbounded mailbox
    When the receiver is dropped
    And the sender try_sends signal S
    Then try_send returns Err with the Closed variant carrying signal S
    # Confirmed: unbounded try_send maps the un-sent value into TrySendError::Closed
    # (:181-184).

  @boundary
  Scenario: Constructing a bounded mailbox with capacity 0 panics (tokio contract)
    When bounded is called with buffer 0
    Then the constructor panics
    # NOTE (src/mailbox.rs:31-32): bounded(buffer) delegates to tokio mpsc::channel(buffer),
    # which panics when buffer == 0 ("mpsc bounded channel requires buffer > 0"). A
    # zero-capacity mailbox is therefore unconstructible; spawn_with_mailbox(bounded(0))
    # cannot exist. This pins the defensive boundary the other capacity scenarios skip.

  @boundary
  Scenario: blocking_send parks until capacity frees, send_timeout gives up after the timeout
    Given a bounded mailbox with capacity 1 that is currently full
    When the sender calls blocking_send(S) on a blocking thread
    And the receiver later frees one slot
    Then blocking_send returns Ok once the slot is available
    And a send_timeout(S2, d) on a still-full channel returns the timeout error after d elapses
    # Confirmed: blocking_send parks on the bounded sender (src/mailbox.rs:232-250);
    # send_timeout bounds the wait via tokio's bounded send_timeout (:201-210) and surfaces the
    # un-sent signal on expiry. Neither has a scenario today though the doc lists them among
    # the send variants.

  @boundary
  Scenario: capacity reports Some(remaining) for bounded and None for unbounded
    Given a bounded mailbox with capacity 4
    Then the sender's capacity returns Some(4)
    And the sender's max_capacity returns Some(4)
    And the sender's capacity for an unbounded mailbox returns None
    And the sender's max_capacity for an unbounded mailbox returns None
    # Confirmed: capacity/max_capacity return Some for Bounded, None for Unbounded
    # (:303-321).

  @boundary
  Scenario: capacity decreases as the bounded channel fills and recovers after recv
    Given a bounded mailbox with capacity 2
    When 2 signals are sent and left unreceived
    Then the sender's capacity returns Some(0)
    And max_capacity still returns Some(2)
    When the receiver calls recv once
    Then the sender's capacity returns Some(1)

  @boundary
  Scenario: same_channel is true for clones of one sender and false across distinct channels
    Given two bounded senders A and A2 that are clones of the same sender
    And a bounded sender B from a different channel
    Then A.same_channel(A2) returns true
    And A.same_channel(B) returns false

  @boundary
  Scenario: same_channel is always false between a bounded and an unbounded sender
    Given a bounded sender A
    And an unbounded sender U
    Then A.same_channel(U) returns false
    And U.same_channel(A) returns false
    # Confirmed: the cross-kind arms return false unconditionally (:289-290).

  @boundary
  Scenario: signal_startup_finished on a full bounded channel reports MailboxFull, not Closed
    Given a bounded mailbox with capacity 1
    And the single capacity slot is occupied by an unreceived signal
    When signal_startup_finished is invoked on the sender
    Then it returns Err(SendError::MailboxFull)
    # Confirmed: signal_startup_finished maps tokio TrySendError::Full -> MailboxFull and
    # Closed -> ActorNotRunning (:954-967). This keeps a capacity hit distinct from a
    # dead-actor failure, per engineering rule 3 (no overflow-as-Closed).

  @boundary
  Scenario: signal_startup_finished after the receiver is dropped reports ActorNotRunning
    Given a bounded mailbox with capacity 1
    When the receiver is dropped
    And signal_startup_finished is invoked on the sender
    Then it returns Err(SendError::ActorNotRunning)

  @boundary
  Scenario: signalling a stop through a weak sender whose channel is closed reports ActorNotRunning
    Given a bounded mailbox with capacity 8
    And only a weak sender remains after the strong sender is dropped
    When signal_stop is awaited on the weak sender
    Then it returns Err(SendError::ActorNotRunning)
    # Confirmed: WeakMailboxSender SignalMailbox methods upgrade first and return
    # ActorNotRunning when upgrade yields None (:1029-1063).

  # ---------------------------------------------------------------------------
  # @linearizability — concurrent senders/receiver, backpressure under real overlap
  # ---------------------------------------------------------------------------

  @linearizability
  Scenario: A blocked bounded send unblocks exactly when the receiver frees a slot
    Given a bounded mailbox with capacity 1
    And the single slot is occupied by an unreceived signal
    And a task is awaiting send of signal S and is therefore parked
    When the receiver calls recv once, freeing a slot
    Then the parked send completes with Ok
    And a subsequent recv yields signal S
    # Confirmed: bounded send awaits tokio mpsc capacity (:150-157); it cannot resolve
    # until recv frees a slot. Real overlap: the sender must be spawned and the assertion
    # made after the recv, not before.

  @linearizability
  Scenario: Concurrent senders to a bounded channel deliver every signal exactly once with no loss
    Given a bounded mailbox with capacity 4
    When 10 tasks each send 100 signals concurrently via await-send
    And a single receiver drains until the channel closes
    Then the receiver observes exactly 1000 signals with no loss and no duplication

  @linearizability
  Scenario: Under backpressure, total signals received never exceeds total sent at any observation point
    Given a bounded mailbox with capacity 4
    And multiple concurrent senders performing await-send
    And a single receiver draining concurrently
    Then at every observation the count received is less than or equal to the count acknowledged-sent, and capacity never reports more than the configured maximum

  @linearizability
  Scenario: A single receiver preserves per-sender FIFO order under concurrency
    Given a bounded mailbox with capacity 4
    And two concurrent senders A and B each sending an ordered numbered sequence
    When a single receiver drains all signals
    Then the subsequence of signals from sender A is strictly increasing
    And the subsequence of signals from sender B is strictly increasing
    # NOTE @review-semantics: tokio mpsc guarantees per-sender FIFO but NOT a global
    # interleaving order across senders. Assert only the per-sender monotonicity.
