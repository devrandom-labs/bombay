# Phase 2 — laws for bombay_actors `ActorPool` (actors/src/pool.rs), layered on
# tests/features/actors/pool.feature's examples.
#
# Conventions (see docs/testing/properties.md):
#   * @property — ∀ inputs. invariant(SUT(inputs)). Wired with proptest.
#   * @model    — SUT ≡ reference model under any op sequence / interleaving.
#   * Each scenario ALSO carries one Phase-1 category tag (+ @timing if a clock is needed).
#   * # GEN: names the generator strategy AND the boundary values it must include.
#   * # ORACLE: names the reference model / inverse function.
#   * # Generalizes: the Phase-1 example scenario(s) this law subsumes.
#   * Facts only. No step definitions — wiring is Phase 3.
#
# FACTS (pool.rs): next_worker() (:155) selects min Arc::weak_count of the per-worker load
# counter; an in-flight Dispatch holds a Weak clone alive, raising that worker's load.
# Broadcast (:380) join_all's one tell per worker and returns one Result per worker in
# worker order. on_link_died (:183) rebuilds the dead worker at the SAME index. new/
# new_async both assert_ne!(size, 0).

@actors @pool @phase2
Feature: ActorPool — laws over least-connections selection, fixed size, and broadcast arity
  As a caller distributing work across a fixed worker set
  I want least-loaded selection, size invariance, and broadcast arity to hold for ALL pool
  sizes and dispatch sequences
  So that no pool size or death sequence silently violates the dispatch contract

  # ---------------------------------------------------------------------------
  # @property — universally-quantified laws
  # ---------------------------------------------------------------------------

  @property @boundary
  Scenario: Constructing a pool with size 0 always panics; any positive size succeeds
    Given any requested pool size n
    When ActorPool::new (and ActorPool::new_async) is called with size n
    Then the constructor panics iff n == 0
    And for any n > 0 it builds a pool of exactly n live workers
    # GEN: n ∈ boundary-biased usize {0, 1, 2, 3, 64, 1000}; both new and new_async tested
    #      at each n.
    # ORACLE: the resolved contract — assert_ne!(size, 0); panic ⇔ n == 0. Size is a
    #         programmer-error contract, not a capacity limit.
    # Generalizes: pool.feature "Constructing a zero-size pool panics",
    #   "A large pool spawns without exhausting memory and dispatch still routes",
    #   "An async factory builds a working pool equivalent to the sync factory".

  @property @sequence
  Scenario: Broadcast returns exactly N results, one per worker in worker order, for any pool size N
    Given a pool of any size N with workers that record what they handle
    When a message is broadcast to the pool
    Then the reply is a Vec of exactly N results, one per worker
    And every worker handles the broadcast message exactly once
    And on a healthy pool every result is Ok
    # GEN: N ∈ boundary-biased usize {1, 2, 4, 64, 1000}.
    # ORACLE: |reply| == |workers| == N (join_all maps one tell per worker, in order).
    # Generalizes: pool.feature "Broadcast sends the message to every worker and returns one
    #   result per worker", "An async factory builds a working pool equivalent to the sync
    #   factory".

  @property @lifecycle
  Scenario: Pool size stays N after any sequence of worker deaths fully processed
    Given a pool of any initial size N
    When any sequence of worker kills is applied and each resulting link-death is processed
    Then the pool still has exactly N workers
    And each replacement occupies the same index its dead predecessor held
    And a replacement is itself re-linked, so a later death of it is also handled
    # GEN: N ∈ {1, 2, 3, 8}; kill sequence length ∈ {0, 1, N, 2*N} incl. killing the same
    #      index repeatedly (death-of-a-replacement) and an unrelated non-worker link-death
    #      (must be ignored, leaving size N).
    # ORACLE: an integer worker-count model that stays == N; on_link_died writes
    #         self.workers[i] = new at the found index and re-links it; an id not in the pool
    #         is a no-op.
    # Generalizes: pool.feature "A worker that stops is replaced and the pool keeps the same
    #   size", "A replacement worker is re-linked…", "A link-death from an actor not in the
    #   pool is ignored", "An async factory is also used to build replacement workers on death".

  # ---------------------------------------------------------------------------
  # @model — dispatch selection refines a least-connections load model
  # ---------------------------------------------------------------------------

  @model @linearizability
  Scenario: Dispatch always selects an argmin(load) worker for any dispatch sequence
    Given a pool of any size N with all workers live
    When any sequence of dispatches runs, each holding its worker busy for a bounded window
    Then each dispatch is routed to a worker whose in-flight load equals the current minimum
      over all live workers at selection time
    And no message is dropped while at least one worker is live; on total exhaustion the reply
      is WorkerReply::Err(SendError::ActorNotRunning(msg)) carrying the original message
    # GEN: N ∈ {1, 2, 4, 8}; dispatch sequence length ∈ {1, N, 4*N} with overlapping in-flight
    #      windows so loads diverge incl. the boundary where one worker is saturated and
    #      another idle; also a run where every worker is stopped (exhaustion path).
    # ORACLE: an integer per-worker load model — load[w] = count of in-flight dispatches
    #         currently holding w's Weak counter alive (Arc::weak_count); selection must be an
    #         argmin over that model, mirroring min_by_key(Arc::weak_count). The min_by_key
    #         first-min tie-break gives idle workers a one-per-worker spread.
    # Generalizes: pool.feature "Load-balanced Dispatch routes to the least-loaded worker",
    #   "Sequential dispatches to an idle pool spread one-per-worker before repeating",
    #   "A single Dispatch is forwarded to exactly one worker",
    #   "Dispatch to a pool whose targeted worker is dying retries the next worker",
    #   "Dispatch when every worker is unreachable returns an ActorNotRunning error".

  @model @linearizability
  Scenario: Concurrent dispatches refine a total-handled counter with at-most-once handling
    Given a pool of any size N with workers recording what they handle
    When M messages are dispatched concurrently from P tasks, each awaited to completion
    Then the total messages handled across all workers equals M
    And no message is handled by more than one worker
    # GEN: N ∈ {2, 4}; M ∈ {1, 50, 100}; P ∈ [2, 10].
    # ORACLE: a single integer counter that must reach exactly M; the retry loop guarantees
    #         at-most-once per message (advance-on-ActorNotRunning), and the pool mailbox
    #         serialises dispatch vs. on_link_died, so concurrent death never duplicates.
    # Generalizes: pool.feature "Concurrent dispatches are distributed across workers with no
    #   message lost", "Concurrent dispatch and a worker death never duplicate or drop the
    #   message", "A Broadcast observes a single consistent worker set even as workers are
    #   replaced".
