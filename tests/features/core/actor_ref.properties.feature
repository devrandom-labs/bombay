# Phase 2: laws (∀ inputs) and model-checks, layered on actor_ref.feature's examples.
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
# Scope: src/actor/actor_ref.rs — id-based Eq/Hash/Ord (actor_ref.rs:1363-1387),
#        strong/weak refcounting + downgrade/upgrade (actor_ref.rs:198-230,1329-1340,
#        2140-2149), and per-ask reply isolation (actor_ref.rs:810-824).

@core @actors @actor_ref @phase2
Feature: ActorRef — laws over identity, reference counting, and ask isolation
  As a holder of an ActorRef
  I want identity, refcounting, and per-ask replies to be exact for ALL inputs
  So that no id pair, clone/drop interleaving, or concurrent ask breaks correctness

  Background:
    Given a running actor spawned with a default bounded mailbox

  # ---------------------------------------------------------------------------
  # @property — universally-quantified laws
  # ---------------------------------------------------------------------------

  @property @boundary
  Scenario: ActorRef Eq, Hash and Ord are id-based and mutually consistent for any pair
    Given any two ActorRefs a, b whose underlying ActorIds are any pair of ids
    When they are compared, ordered, and hashed
    Then a == b iff their ActorIds are equal
    And a == b implies hash(a) == hash(b)
    And the Ord of a, b equals the Ord of their ActorIds
    And clones of the same ActorRef are always equal and hash equally
    # GEN: ids drawn from boundary-biased u64 sequence_ids {0, 1, u64::MAX-1, u64::MAX},
    #      including the equal (clone) case, adjacent ids, and distinct-actor pairs.
    # ORACLE: the ActorId itself — PartialEq/Hash/Ord delegate purely to id
    #         (actor_ref.rs:1363-1387), so this composes the id-level law.
    # Generalizes: actor_ref.feature "Two ActorRefs to the same actor are equal and hash
    #              equally", "ActorRefs to different actors are not equal".

  @property @boundary
  Scenario: Telling any stopped actor fails with ActorNotRunning for any message
    Given an actor that has been stopped and whose shutdown has been awaited
    And any message value of the actor's accepted message type
    When that message is told to the stopped actor
    Then the send fails with SendError::ActorNotRunning
    # GEN: message payload arbitrary (including default/zero and a max-size payload);
    #      the stopped-then-awaited state is the fixed precondition.
    # ORACLE: a closed-mailbox model — every send on a closed mailbox maps to
    #         ActorNotRunning regardless of payload (actor_ref.rs:248-253; error.rs:91).
    # Generalizes: actor_ref.feature "Telling a stopped actor fails with ActorNotRunning".

  # ---------------------------------------------------------------------------
  # @model — refcount refinement and per-ask reply isolation
  # ---------------------------------------------------------------------------

  @model @linearizability
  Scenario: strong/weak counts refine an integer model under any ref op interleaving
    Given any interleaving of clone, downgrade, drop, and upgrade on the ActorRef and its weaks
    When the operations run concurrently
    Then strong_count always equals the model's count of live strong handles
    And weak_count always equals the model's count of live weak handles
    And upgrade returns Some iff the model's strong count is greater than zero
    # GEN: an op sequence over {clone, downgrade, drop-strong, drop-weak, upgrade} of
    #      length [1, 64], including length 1, the last-strong-drop boundary, and an
    #      upgrade attempted after strong reaches 0.
    # ORACLE: a pair of integer counters (strong, weak) mirroring Arc-style semantics —
    #         clone bumps strong, downgrade bumps weak, drops decrement, upgrade succeeds
    #         iff strong > 0 (actor_ref.rs:198-230,1329-1340,2140-2149). Small cases via
    #         loom for exhaustive interleavings.
    # Generalizes: actor_ref.feature "strong_count and weak_count track live handles",
    #              "A WeakActorRef can be upgraded while a strong ActorRef remains",
    #              "A WeakActorRef cannot be upgraded after every strong ref is dropped".

  @model @linearizability
  Scenario: N concurrent asks each receive their own distinct reply with no cross-talk
    Given an actor that echoes back the distinct number it is asked
    And any number N of tasks each asking a distinct number, started at a barrier
    When all N asks run with real overlap
    Then every task receives exactly the number it asked
    And the multiset of received replies equals the multiset of asked numbers
    # GEN: N ∈ [2, 64] (include the smallest concurrent case 2 and a large fan-out);
    #      asked numbers are distinct per task so cross-talk is observable.
    # ORACLE: identity per channel — each AskRequest carries its own reply oneshot, so
    #         reply_i is routed only to caller_i (actor_ref.rs:810-824). Model = a map
    #         task_id -> asked_value; received must equal that map.
    # Generalizes: actor_ref.feature "Concurrent asks each receive their own correct
    #              reply", "Concurrent tells from many tasks are all delivered".

  @model @linearizability
  Scenario: Any number of concurrent startup waiters all observe one completion
    Given an actor whose on_start blocks until released
    And any number W of tasks concurrently awaiting wait_for_startup
    When on_start is then released
    Then all W waiters resolve, and none resolves before on_start completes
    # GEN: W ∈ {1, 2, 10, 64} (include the single-waiter boundary); release point fixed
    #      after all waiters are parked.
    # ORACLE: a one-shot latch fanned out to all observers — a shared SetOnce set exactly
    #         once after on_start (actor_ref.rs:514-517; spawn.rs:413). The same shape
    #         applies to wait_for_shutdown via mailbox closed() (actor_ref.rs:619-622).
    # Generalizes: actor_ref.feature "Many concurrent waiters all observe a single
    #              startup completion", "… shutdown completion".
