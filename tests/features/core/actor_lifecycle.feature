# Scope: kameo core actor lifecycle (src/actor.rs, src/actor/spawn.rs,
#        src/actor/kind.rs) — the Actor trait's lifecycle hooks (on_start /
#        on_panic / on_link_died / on_stop), the run-loop in
#        run_actor_lifecycle, the startup-buffer replay in ActorBehaviour, and
#        the Spawn extension trait's spawn variants.
#
# Authoring rules (mirrors tests/features/actors/message_queue.feature):
#   * Every Scenario carries exactly ONE cross-cutting tag:
#       @sequence | @lifecycle | @boundary | @linearizability
#   * Invariant-first: the `Then` names the observable guarantee. If the Then
#     cannot be stated without reading internals, it is a `# NOTE:` +
#     @review-semantics, never an asserted guess.
#   * @bug:<file:line> marks a scenario that MUST FAIL today.
#   * Facts only: every Then is confirmed from the source above.

@core @actors @actor_lifecycle
Feature: Actor lifecycle — start, run, panic-recovery, link-death, stop
  As a developer spawning kameo actors
  I want the lifecycle hooks and spawn variants to behave deterministically
  So that initialization, fault recovery, and cleanup are predictable

  Background:
    Given a Tokio multi-threaded runtime is available

  # ---------------------------------------------------------------------------
  # @sequence — startup buffering and replay ordering
  # ---------------------------------------------------------------------------

  @sequence
  Scenario: Messages sent before startup finishes are buffered and replayed in order
    Given an actor whose on_start blocks until released
    And three messages "a", "b", "c" are told to the actor in that order before on_start completes
    When on_start is released and completes successfully
    Then the actor handles "a", then "b", then "c" in that exact order
    # Confirmed: ActorBehaviour buffers external pre-startup messages in a
    # VecDeque (startup_buffer) and drains them front-to-back in
    # handle_startup_finished (kind.rs:77-106, push_back at handle_message:119).

  @sequence
  Scenario: A message sent from within on_start is prioritised over earlier external messages
    Given an actor that, during on_start, tells itself an internal message "init"
    And an external message "ext" was told to the actor before on_start ran
    When on_start completes
    Then the actor handles "init" before "ext"
    # Confirmed: handle_message only buffers when `!sent_within_actor`
    # (kind.rs:117); internally-sent signals bypass the startup buffer.

  @sequence
  Scenario: Startup-finished signal transitions the actor from buffering to direct handling
    Given an actor that has completed on_start and drained its startup buffer
    When a new message "after" is told to the actor
    Then "after" is handled immediately without being buffered
    # Confirmed: finished_startup is set true in handle_startup_finished;
    # subsequent handle_message calls skip the buffer branch.

  # ---------------------------------------------------------------------------
  # @lifecycle — on_panic ControlFlow, on_stop, on_link_died, spawn variants
  # ---------------------------------------------------------------------------

  @lifecycle
  Scenario: Default on_panic stops the actor with Panicked after a handler panic
    Given an actor using the default on_panic implementation
    When a message handler panics
    Then the actor stops with reason Panicked
    # Confirmed: default on_panic returns ControlFlow::Break(Panicked(err))
    # (actor.rs:279-285); on_shutdown routes Panicked through on_panic
    # (kind.rs:401-416).

  @lifecycle
  Scenario: on_panic returning Continue keeps the actor alive after a panic
    Given an actor whose on_panic returns ControlFlow::Continue
    When a message handler panics
    And a follow-up message is sent
    Then the actor processes the follow-up message
    # Confirmed: on_shutdown maps Ok(Ok(Continue)) to ControlFlow::Continue,
    # so the outer loop continues (kind.rs:406, spawn.rs:385-388).

  @lifecycle
  Scenario: on_panic returning Break stops the actor with the chosen reason
    Given an actor whose on_panic returns ControlFlow::Break(Normal)
    When a message handler panics
    Then the actor stops with reason Normal
    # Confirmed: Ok(Ok(Break(reason))) maps to ControlFlow::Break(reason)
    # (kind.rs:407).

  @lifecycle
  Scenario: on_stop is called when the actor is killed
    Given a running actor that records on_stop invocations
    When the actor is killed via ActorRef::kill
    Then on_stop is called exactly once with reason Killed
    # Confirmed: kill aborts the loop, Abortable yields Killed
    # (spawn.rs:236), and on_stop runs unconditionally afterwards
    # (spawn.rs:253-261).

  @lifecycle
  Scenario: on_stop is called on graceful stop
    Given a running actor that records on_stop invocations
    When the actor is stopped gracefully
    Then on_stop is called exactly once with reason Normal
    # Confirmed: Signal::Stop -> handle_stop -> Break(Normal) (kind.rs:383-389),
    # then on_stop runs with that reason (spawn.rs:253).

  @lifecycle
  Scenario: Default on_link_died stops the actor when a linked sibling panics
    Given two linked sibling actors A and B with default on_link_died
    When sibling B stops with reason Panicked
    Then actor A stops with reason LinkDied
    # Confirmed: default on_link_died returns Break(LinkDied{..}) for
    # Killed/Panicked/LinkDied reasons (actor.rs:307-314).

  @lifecycle
  Scenario: Default on_link_died keeps the actor alive when a linked sibling stops Normally
    Given two linked sibling actors A and B with default on_link_died
    When sibling B stops with reason Normal
    Then actor A continues running
    # Confirmed: default on_link_died returns Continue for Normal and
    # SupervisorRestart (actor.rs:303-306).

  @lifecycle
  Scenario: Default on_link_died keeps the actor alive when a linked sibling restarts
    Given two linked sibling actors A and B with default on_link_died
    When sibling B stops with reason SupervisorRestart
    Then actor A continues running
    # Confirmed: SupervisorRestart maps to ControlFlow::Continue
    # (actor.rs:303-306).

  @lifecycle
  Scenario: spawn returns an ActorRef whose actor starts running
    Given an actor type
    When the actor is spawned via spawn
    And the caller waits for startup
    Then the actor is alive and a default bounded mailbox of capacity 64 was used
    # Confirmed: spawn delegates to spawn_with_mailbox with
    # mailbox::bounded(DEFAULT_MAILBOX_CAPACITY=64) (actor.rs:45,499-501).

  @lifecycle
  Scenario: spawn_with_mailbox honours an unbounded mailbox configuration
    Given an actor spawned with an unbounded mailbox
    When more than 64 messages are told without the actor draining them
    Then no send is rejected for a full mailbox
    # Confirmed: spawn_with_mailbox uses the provided mailbox pair verbatim
    # (actor.rs:565-573).

  @lifecycle
  Scenario: spawn_in_thread runs an actor on a dedicated OS thread
    Given an actor that performs a blocking operation in its handler
    When the actor is spawned via spawn_in_thread on a multi-threaded runtime
    And a message is sent with blocking_send
    Then the message is handled without blocking the async runtime
    # Confirmed: spawn_in_thread builds a std::thread and block_on's the
    # lifecycle (spawn.rs:164-180).

  @lifecycle
  Scenario: prepare exposes the ActorRef before the actor runs, then run drains pending mail
    Given a prepared actor created via prepare
    And a message "early" is told to its ActorRef before it runs
    When the prepared actor is run to completion
    Then "early" is handled by the actor
    # Confirmed: PreparedActor::actor_ref is available pre-spawn (spawn.rs:96),
    # and run processes mail already in the mailbox (spawn.rs:131, doc:105-106).

  # ---------------------------------------------------------------------------
  # @boundary — startup failure modes, thread-on-current-thread runtime
  # ---------------------------------------------------------------------------

  @boundary
  Scenario: An actor whose on_start returns Err never enters the run loop
    Given an actor whose on_start returns Err
    When the actor is spawned and startup is awaited
    Then the startup result is an error
    And the actor stops with reason Panicked without handling any message
    # Confirmed: on_start Err is mapped to PanicError(OnStart) and the Err arm
    # sets startup_result=Err and reason=Panicked, skipping the loop
    # (spawn.rs:204-209, 287-319).

  @boundary
  Scenario: An actor whose on_start panics is surfaced as a startup error
    Given an actor whose on_start panics
    When the actor is spawned and startup is awaited
    Then the startup result is an error
    And the actor stops with reason Panicked
    # Confirmed: catch_unwind on on_start turns the panic into
    # PanicError::new_from_panic_any(OnStart) and the same Err arm runs
    # (spawn.rs:204-209, 287).

  @lifecycle
  Scenario: on_stop is not called when on_start fails
    Given an actor whose on_start returns Err and which records on_stop calls
    When the actor is spawned and startup is awaited
    Then on_stop is not called
    # Start-paired contract: the Err arm in run_actor_lifecycle (spawn.rs:287-320)
    # notifies links and unregisters but never builds the actor, and on_stop needs
    # `&mut self` which never existed.

  @boundary
  Scenario: spawn_in_thread on a current-thread runtime panics
    Given a current-thread Tokio runtime
    When an actor is spawned via spawn_in_thread
    Then the spawn call panics with "threaded actors are not supported in a single threaded tokio runtime"
    # Confirmed: spawn_in_thread panics when runtime_flavor() is CurrentThread
    # (spawn.rs:169-171).

  # ---------------------------------------------------------------------------
  # @linearizability — RAII drop-stops, concurrent spawn
  # ---------------------------------------------------------------------------

  @linearizability
  Scenario: Dropping the last strong ActorRef stops the actor
    Given an actor spawned via spawn with no other strong references retained
    When the last ActorRef is dropped while only WeakActorRefs remain
    Then the actor stops
    # Confirmed: ActorRef holds the strong MailboxSender; dropping all strong
    # refs closes the mailbox so MailboxReceiver::recv yields None, which
    # ActorBehaviour::next maps to Break(Normal) (kind.rs:66).

  @linearizability
  Scenario: A retained WeakActorRef does not keep the actor alive
    Given an actor spawned via spawn
    And a WeakActorRef downgraded from its ActorRef
    When every strong ActorRef is dropped
    Then upgrading the WeakActorRef returns None
    # Confirmed: WeakActorRef::upgrade returns None once all strong
    # mailbox senders are gone (actor_ref.rs:2140-2149).

  @linearizability
  Scenario: Spawning many actors concurrently yields distinct live actors
    Given 100 actors are spawned concurrently from 10 tasks
    When each spawn's startup is awaited
    Then all 100 actors are alive and have pairwise-distinct ActorIds
    # Confirmed: each PreparedActor::new calls ActorId::generate (spawn.rs:54),
    # whose atomic fetch_add guarantees distinct sequence_ids (id.rs:72-79).

  @linearizability
  Scenario: spawn_link establishes the link before the child can die
    Given a supervisor actor and a child actor type
    When the child is spawned via spawn_link against the supervisor
    Then the link is in place before the child begins running
    # Confirmed: spawn_link_with_mailbox calls actor_ref.link(link_ref).await
    # BEFORE prepared_actor.spawn(args) (actor.rs:647-653) — race-free by
    # construction.
