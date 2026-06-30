# Scope: bombay_actors `ActorPool` (actors/src/pool.rs) — a supervisor actor that owns a
#        fixed set of worker actors and load-balances `Dispatch<M>` across them
#        (least-connections by `Arc::weak_count` of a per-worker load counter), fans
#        `Broadcast<M>` to all workers, and replaces dead workers in place on link death.
#
# Authoring rules (see message_queue.feature — the exemplar — for the full statement):
#   * Every Scenario carries exactly ONE cross-cutting tag:
#       @sequence | @lifecycle | @boundary | @linearizability
#   * Invariant-first: the `Then` names the observable guarantee. If the guarantee
#     cannot be stated without reading the implementation, it is written as a
#     `# NOTE:` plus a @review-semantics tag rather than an asserted guess.
#   * Facts only — every `Then` below is grounded in actors/src/pool.rs as read.
#   * No step definitions here; steps are written in the wiring phase.
#
# Source facts that ground the Thens (actors/src/pool.rs):
#   * `next_worker()` (:155) selects the worker with the minimum `Arc::weak_count` of
#     its load counter; an in-flight `Dispatch` holds a `Weak<()>` clone of that
#     counter alive (WorkerMsgWrapper.counter) so a busy worker has a higher weak_count
#     and is deprioritised. Load is "messages currently in flight", not mailbox depth.
#   * `Dispatch` handler (:317) loops at most `workers.len()` times; on a
#     `SendError::ActorNotRunning` it advances to the next worker and retries; if every
#     worker is exhausted it returns `WorkerReply::Err(SendError::ActorNotRunning(msg))`.
#   * `Broadcast` handler (:380) `join_all`s over the *current* `workers` vec, one
#     `tell` per worker, and returns `Vec<Result<(), SendError<..>>>` — one entry per
#     worker, in worker order.
#   * `on_link_died` (:183) locates the dead worker by `ActorId`, rebuilds it via the
#     factory, writes it back to the SAME index (`self.workers[i] = ..`), and re-links
#     it to the pool. A link-death whose id is not in the pool is ignored.
#   * `new` (:93) and `new_async` (:125) both `assert_ne!(size, 0)`.

@actors @pool
Feature: ActorPool — least-connections dispatch, broadcast, and worker replacement
  As a caller distributing work across a fixed set of worker actors
  I want the pool to route each task to the least-loaded live worker, fan broadcasts
  to every worker, and silently replace workers that die
  So that throughput is balanced and the pool self-heals without my intervention

  Background:
    Given an ActorPool spawned with a synchronous factory

  # ---------------------------------------------------------------------------
  # @sequence — multi-step routing/broadcast protocol on one live pool
  # ---------------------------------------------------------------------------

  @sequence
  Scenario: A single Dispatch is forwarded to exactly one worker
    Given a pool of 3 workers each recording the messages they handle
    When 1 message is dispatched to the pool
    Then exactly 1 worker handles the message
    And the total number of messages handled across all workers is 1
    And the pool reply is WorkerReply::Forwarded

  @sequence
  Scenario: Load-balanced Dispatch routes to the least-loaded worker
    Given a pool of 3 workers
    And worker 0 is occupied with an in-flight request that has not yet replied
    When 1 message is dispatched to the pool
    Then the message is handled by a worker other than worker 0
    # Invariant: next_worker picks min Arc::weak_count; the in-flight request on
    # worker 0 holds a Weak counter alive, raising its weak_count above the idle
    # workers, so it is not selected.

  @sequence
  Scenario: Sequential dispatches to an idle pool spread one-per-worker before repeating
    Given a pool of 4 idle workers
    When 4 messages are dispatched to the pool, each awaited to completion
    Then each of the 4 workers has handled exactly 1 message
    # NOTE @review-semantics: this holds only if each ask fully completes (its Weak
    # counter dropped) before the next dispatch is selected, so all idle workers share
    # the minimum weak_count and the tie is broken deterministically by first-min
    # iteration order. Pin at wiring whether the round-robin-like spread is guaranteed
    # or merely emergent from min_by_key tie-breaking.

  @sequence
  Scenario: Broadcast sends the message to every worker and returns one result per worker
    Given a pool of 3 workers each recording the messages they handle
    When a message is broadcast to the pool
    Then every one of the 3 workers handles the message exactly once
    And the pool reply is a Vec of exactly 3 results
    And every result in the Vec is Ok

  @sequence
  Scenario: Dispatch reply is Forwarded, carrying no worker result value
    Given a pool of 2 workers
    When 1 message is dispatched to the pool
    Then the pool reply is WorkerReply::Forwarded and not an error
    # Invariant: Dispatch forwards the reply channel to the worker (forward(tx)); the
    # pool itself answers Forwarded, it does not relay the worker's Ok value back.

  # ---------------------------------------------------------------------------
  # @lifecycle — worker death, replacement at the same index, re-linking
  # ---------------------------------------------------------------------------

  @lifecycle
  Scenario: A worker that stops is replaced and the pool keeps the same size
    Given a pool of 3 workers
    When worker 1 is killed
    And the pool processes the resulting link-death
    Then the pool still has exactly 3 workers
    And the replacement occupies the same index that the dead worker held
    # Invariant: on_link_died writes self.workers[i] = <new worker> at the found index.

  @lifecycle
  Scenario: A replacement worker is re-linked so its later death is also handled
    Given a pool of 2 workers
    When worker 0 is killed and replaced by the pool
    And the replacement worker 0 is then killed
    And the pool processes the resulting link-death
    Then the pool still has exactly 2 workers
    And the second replacement also occupies index 0
    # Invariant: on_link_died calls self.workers[i].0.link(&actor_ref) on the
    # replacement, so a chain of deaths is supervised, not just the first.

  @lifecycle
  Scenario: A link-death from an actor not in the pool is ignored
    Given a pool of 2 workers
    And an unrelated linked actor that is not a pool worker
    When the unrelated actor dies and the pool processes the link-death
    Then the pool still has exactly 2 workers
    And no worker is replaced
    # Invariant: on_link_died returns ControlFlow::Continue without mutation when no
    # worker's id matches the dead actor's id.

  @lifecycle
  Scenario: Dispatch to a pool whose targeted worker is dying retries the next worker
    Given a pool of 2 workers
    And one worker has stopped but has not yet been replaced
    When 1 message is dispatched to the pool
    Then the message is handled by a live worker
    And the pool reply is WorkerReply::Forwarded
    # Invariant: the Dispatch loop catches SendError::ActorNotRunning and advances to
    # the next worker, retrying up to workers.len() times.

  @lifecycle
  Scenario: Dispatch when every worker is unreachable returns an ActorNotRunning error
    Given a pool of 2 workers where both workers have stopped and not been replaced
    When 1 message is dispatched to the pool
    Then the pool reply is WorkerReply::Err with SendError::ActorNotRunning carrying the original message
    # Invariant: after looping workers.len() times without a live worker, the handler
    # returns WorkerReply::Err(SendError::ActorNotRunning(msg)) with the un-sent msg.

  # ---------------------------------------------------------------------------
  # @boundary — construction limits, factory variants, scale
  # ---------------------------------------------------------------------------

  @boundary
  Scenario: Constructing a zero-size pool panics (programmer-error contract)
    When ActorPool::new is called with size 0
    Then the constructor panics
    And ActorPool::new_async called with size 0 also panics
    # NOTE (pool.rs:100, assert_ne!(size, 0)): a zero-worker pool is a programmer bug, so
    # panic is the rule-4-sanctioned contract (capacity LIMITS return Result, but this is
    # not a limit). Resolution: keep the panic; both new(0) and new_async(0) assert.

  @boundary
  Scenario: An async factory builds a working pool equivalent to the sync factory
    Given an ActorPool spawned with an asynchronous factory of 3 workers
    When a message is broadcast to the pool
    Then every one of the 3 workers handles the message exactly once
    And the pool reply is a Vec of exactly 3 Ok results
    # Invariant: new_async produces the same workers/size/dispatch semantics as new.

  @lifecycle @async-factory
  Scenario: An async factory is also used to build replacement workers on death
    Given an ActorPool spawned with an asynchronous factory of 2 workers
    When worker 0 is killed and the pool processes the link-death
    Then the pool still has exactly 2 workers
    And the replacement at index 0 was produced by awaiting the async factory
    # NOTE (on_link_died, Factory::Async f().await): FACT — replacement uses the async
    # factory. Observing "via async factory" needs a factory side-effect installed at
    # wiring time (a wiring detail, not an open semantic).

  @boundary
  Scenario: A large pool spawns without exhausting memory and dispatch still routes
    Given a pool of 1000 workers
    When 1 message is dispatched to the pool
    Then the pool spawns all 1000 workers without panicking or aborting
    And exactly 1 worker handles the dispatched message
    # NOTE @timing: large-pool spawn is allocation-/time-sensitive; bound it generously
    # at wiring and assert no OOM/abort rather than a wall-clock budget.

  @sequence @review-semantics
  Scenario: Broadcast over a healthy pool is all-Ok and never reaches the infallible-reset panic
    Given a pool of 3 workers whose reply type is infallible
    When a message is broadcast to the pool
    Then the result Vec has exactly 3 Ok entries and the pool actor does not panic
    # NOTE (:398-405): the Broadcast map calls `err.map_err(|_| panic!("reset err infallible
    # called on a SendError::HandlerError"))` on each worker's SendError. The send is a
    # `tell` (`worker.tell(..).send()`), which has no reply channel, so it can only yield
    # ActorNotRunning / MailboxFull / Timeout — NEVER HandlerError. The panic arm is
    # therefore a dispatch-bug guard, unreachable in practice (mirrors reply.rs:535
    # `into_value` unreachable! on Forwarded(Ok)). @review-semantics: confirm at wiring that
    # no reachable broadcast input constructs a HandlerError SendError on a tell; if one is
    # found this becomes a real @bug:pool.rs:401 probe (panic → surfaced error in the Vec).

  @lifecycle
  Scenario: A fire-and-forget Dispatch (tell) routes to one worker without a reply value
    Given a pool of 2 workers each recording the messages they handle
    When 1 message is dispatched to the pool via tell rather than ask
    Then exactly 1 worker handles the message
    And the total number of messages handled across all workers is 1
    # NOTE (:346-358): the Dispatch handler's reply_sender is None on a tell — it forwards
    # the message without a reply channel and the retry-on-ActorNotRunning loop still applies.
    # This exercises the None branch that the ask-based Dispatch scenarios never reach.

  # ---------------------------------------------------------------------------
  # @linearizability — concurrent dispatch/broadcast with real overlap
  # ---------------------------------------------------------------------------

  @linearizability
  Scenario: Concurrent dispatches are distributed across workers with no message lost
    Given a pool of 4 workers each recording the messages they handle
    When 100 messages are dispatched concurrently from 10 tasks, each awaited
    Then the total number of messages handled across all workers is exactly 100
    And no message is handled by more than one worker
    And every worker has handled at least one message
    # NOTE @review-semantics: "every worker handled at least one" is expected from the
    # least-connections selection under real overlap but is not a hard guarantee of the
    # code; if it proves flaky at wiring, weaken to a balance bound (e.g. max/min load
    # ratio) rather than dropping the linearizability check on total == 100.

  @linearizability
  Scenario: A Broadcast observes a single consistent worker set even as workers are replaced
    Given a pool of 3 workers
    When a worker is killed and replaced concurrently with a broadcast
    Then the broadcast result Vec has exactly 3 entries
    And the broadcast is delivered to exactly the workers present in the pool at the moment it ran
    # Invariant: the Broadcast handler runs inside the pool actor's single-threaded
    # message loop, so it sees one atomic snapshot of self.workers; a concurrent
    # on_link_died replacement is serialised before or after it, never interleaved.

  @linearizability
  Scenario: Concurrent dispatch and a worker death never duplicate or drop the message
    Given a pool of 3 workers
    When 1 message is dispatched concurrently with the death of the worker it would target
    Then the message is handled by exactly one live worker, or the reply is an ActorNotRunning error
    And the message is never handled twice
    # Invariant: dispatch and on_link_died are serialised by the pool's mailbox; the
    # dispatch retry loop guarantees at-most-once handling with an explicit error on
    # total exhaustion.
