# Phase 2: laws (∀ inputs) and model-checks, layered on actor_lifecycle.feature's examples.
# Conventions (see docs/testing/properties.md):
#   * @property — ∀ inputs. invariant(SUT(inputs)). Wired with proptest.
#   * @model    — SUT ≡ reference model under any op sequence / interleaving.
#   * Each scenario ALSO carries one Phase-1 category tag (+ @timing if a clock is needed).
#   * # GEN:    names the generator strategy AND the boundary values it must include.
#   * # ORACLE: names the reference model / inverse function.
#   * # Generalizes: the Phase-1 example scenario(s) this law subsumes.
#   * Facts only; a @bug-exposing property keeps its @bug:<file:line> and fails today.
#   * No step definitions — wiring is Phase 3.
#
# Scope: src/actor.rs (on_panic/on_link_died defaults), src/actor/kind.rs
#        (ActorBehaviour startup buffer + on_shutdown control flow), src/actor/spawn.rs
#        (spawn variants, run_actor_lifecycle).

@core @actors @actor_lifecycle @phase2
Feature: Actor lifecycle — laws over hook decisions, startup replay, and spawn variants
  As a developer spawning bombay actors
  I want the documented stop/continue decisions and startup ordering to hold for ALL inputs
  So that no reason, message sequence, or mailbox shape breaks lifecycle determinism

  Background:
    Given a Tokio multi-threaded runtime is available

  # ---------------------------------------------------------------------------
  # @property — universally-quantified laws
  # ---------------------------------------------------------------------------

  @property @lifecycle
  Scenario: Default on_link_died Break/Continue decision holds for any stop reason
    Given any ActorStopReason r delivered to the default on_link_died
    When the default on_link_died is evaluated for r
    Then it returns Continue iff r is Normal or SupervisorRestart
    And it returns Break(LinkDied{..}) for Killed, Panicked, or LinkDied
    # GEN: r ∈ the full ActorStopReason set {Normal, Killed, Panicked(_),
    #      LinkDied{..}, SupervisorRestart} (every variant — the enum is the boundary);
    #      Panicked/LinkDied payloads generated arbitrarily.
    # ORACLE: a partition function {Normal, SupervisorRestart} -> Continue, else -> Break
    #         (actor.rs:303-314).
    # Generalizes: actor_lifecycle.feature "Default on_link_died stops the actor when a
    #              linked sibling panics", "… keeps the actor alive when a linked sibling
    #              stops Normally", "… restarts".

  @property @lifecycle
  Scenario: Default on_panic always stops with Panicked carrying the original error
    Given any handler panic producing any PanicError reason
    When the default on_panic is evaluated for that error
    Then it returns Break(Panicked) wrapping exactly that error
    # GEN: panic payload ∈ {String panic, &str panic, typed Error value, arbitrary
    #      panic_any payload} (the three documented panic kinds plus a returned error).
    # ORACLE: identity over the error — default on_panic is Break(Panicked(err))
    #         unconditionally (actor.rs:279-285).
    # Generalizes: actor_lifecycle.feature "Default on_panic stops the actor with
    #              Panicked after a handler panic".

  @property @sequence
  Scenario: The startup buffer replays any pre-start external message sequence in send order
    Given an actor whose on_start blocks until released
    And any sequence of n distinct external messages told before on_start completes
    When on_start is released and the startup buffer is drained
    Then the actor handles those n messages in exactly their send order
    # GEN: n ∈ boundary-biased usize {0, 1, 2, 64, 256} (include empty buffer and the
    #      single-message boundary); payloads distinct monotonic tags.
    # ORACLE: a VecDeque<Tag> push_back on each pre-start tell, drained front-to-back —
    #         SUT handle order == oracle drain order (kind.rs:77-106, 117-119).
    # Generalizes: actor_lifecycle.feature "Messages sent before startup finishes are
    #              buffered and replayed in order".

  @property @sequence
  Scenario: Any message sent from within on_start is handled before any buffered external message
    Given an actor that tells itself any number i of internal messages during on_start
    And any number e of external messages were told before on_start ran
    When on_start completes and all messages are handled
    Then every internal message is handled before any external buffered message
    # GEN: i ∈ {1, 2, 8}; e ∈ {0, 1, 8} (include the no-external boundary); tags encode
    #      origin (internal/external) and send order.
    # ORACLE: internal sends bypass the buffer because they are sent_within_actor
    #         (kind.rs:117); model = internal FIFO entirely ahead of the external VecDeque.
    # Generalizes: actor_lifecycle.feature "A message sent from within on_start is
    #              prioritised over earlier external messages".

  # ---------------------------------------------------------------------------
  # @model — spawn-variant equivalence and RAII drop-stop refinement
  # ---------------------------------------------------------------------------

  @model @lifecycle
  Scenario: All spawn variants reach the same running state for any valid mailbox
    Given any mailbox configuration drawn from {bounded(c), unbounded}
    And any spawn variant drawn from {spawn_with_mailbox, prepare-then-run, spawn_in_thread}
    When the actor is spawned via that variant with that mailbox and startup is awaited
    Then the actor is alive after startup
    And it handles the same fixed probe message sequence identically across all variants
    # GEN: c ∈ boundary-biased usize {1, 2, 64, 1024}; variant enumerated exhaustively;
    #      spawn_in_thread requires the multi-threaded runtime (its current-thread panic
    #      is a separate Phase-1 example, not in this law's domain).
    # ORACLE: a single reference run (spawn_with_mailbox on bounded(64)) — every other
    #         variant's observed handle sequence must equal that reference (the spawn
    #         path forks only on mailbox/thread setup, then enters one shared
    #         run_actor_lifecycle, spawn.rs:131,164-180; actor.rs:499-573).
    # Generalizes: actor_lifecycle.feature "spawn returns an ActorRef whose actor starts
    #              running", "spawn_with_mailbox honours an unbounded mailbox",
    #              "spawn_in_thread runs an actor on a dedicated OS thread",
    #              "prepare exposes the ActorRef before the actor runs".

  @model @linearizability
  Scenario: Strong ActorRef presence refines a counter that stops the actor at zero
    Given any interleaving of clone, drop, downgrade, and upgrade on a spawned actor's refs
    When the operations run concurrently
    Then the actor stops (mailbox closes) exactly when the last strong ActorRef is dropped
    And no upgrade of a WeakActorRef succeeds after that point
    # GEN: an op sequence over {clone, drop, downgrade, upgrade} of length [1, 64],
    #      including length 1 and a sequence ending on the last strong drop.
    # ORACLE: an integer strong-count model; actor-stopped ⇔ model reaches 0, since the
    #         strong MailboxSender count drives mailbox closure -> recv None ->
    #         Break(Normal) (kind.rs:66; actor_ref.rs:2140-2149). Small cases via loom.
    # Generalizes: actor_lifecycle.feature "Dropping the last strong ActorRef stops the
    #              actor", "A retained WeakActorRef does not keep the actor alive",
    #              "Spawning many actors concurrently yields distinct live actors".
