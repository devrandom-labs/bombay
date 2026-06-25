# Phase 2 (card #74): laws over the in-tree console server + registry, layered on
# server_wire.feature's examples. See docs/testing/properties.md.
#   * @property — ∀ inputs. invariant(SUT(inputs)). Wired with proptest.
#   * @model    — SUT ≡ reference model under any op sequence / interleaving.
#   * Each scenario ALSO carries one Phase-1 category tag.
#   * # GEN: names the generator strategy AND the boundary values it must include.
#   * # ORACLE: names the reference model.
#   * # Generalizes: the Phase-1 example scenario(s) this law subsumes.
#   * Facts only, grounded in src/console/{server.rs,wire.rs,registry.rs}. No step defs.
#
# Source facts:
#   * Snapshot.seq = SEQ.fetch_add(1, Relaxed) per produced snapshot — a process-global
#     monotonic counter incremented once per snapshot, shared across all clients (registry.rs:447).
#   * snapshot() clones the monitor set under ONE registry lock (registry.rs:423-427), then
#     releases it and renders each actor live afterward — so the LIST (membership) is an atomic
#     snapshot; per-actor counters are read live, not under that lock.
#   * each connection is its own spawned serve_client task; reply = 4-byte BE length + payload.

@console @server_wire @phase2
Feature: Console server + registry — laws over seq monotonicity and membership atomicity
  As an instrumented kameo process serving consoles
  I want seq to advance strictly and the actor membership to be a consistent snapshot
  So that no client ever sees a repeated/decreasing seq or a torn membership list

  # ---------------------------------------------------------------------------
  # @property — universally-quantified laws
  # ---------------------------------------------------------------------------

  @property @sequence
  Scenario: snapshot seq is strictly increasing across any number of rapid sequential polls
    Given a console server with at least one live actor and one open client connection
    And any poll count n
    When the client requests n snapshots back to back on that connection
    Then the seq values observed form a strictly increasing sequence
    And each seq equals the previous one plus exactly one (single producer, no concurrent polls)
    # GEN: n ∈ boundary-biased usize {1, 2, 5, 64, 256}; single connection, no other producers.
    # ORACLE: a monotonic counter model — seq_i == seq_0 + i (SEQ.fetch_add(1) once per
    #         snapshot, registry.rs:447); strictly increasing and +1-stepped with one producer.
    # Generalizes: server_wire.feature "seq strictly increases across rapid sequential polls",
    #              "seq advances by exactly one per produced snapshot".

  @property @sequence
  Scenario: captured_at and uptime are non-decreasing across any sequence of polls
    Given a console server and one open client connection
    And any poll count n
    When the client requests n snapshots in order
    Then each snapshot's captured_at is at or after the previous one's
    And each snapshot's uptime is at or after the previous one's
    # GEN: n ∈ boundary-biased usize {1, 2, 8, 64}; polls issued in order on one connection.
    # ORACLE: monotone clocks — captured_at = SystemTime::now(), uptime = START.elapsed()
    #         (registry.rs:448-449), both non-decreasing when sampled in program order.
    # Generalizes: server_wire.feature "captured_at and uptime advance alongside seq".

  @property @sequence
  Scenario: totals.total_stopped counts every stopped actor exactly once across any reap schedule
    Given any sequence of spawns and stops with reaps interleaved at arbitrary points
    When the client polls after the sequence
    Then totals.total_stopped equals the number of actors that have ever stopped
    And it never double-counts a reaped actor nor loses one mid-reap, for any schedule
    # GEN: op sequences over {spawn, stop, advance-clock-past-ttl-then-poll} of length [0, 64];
    #      include boundaries {0 stops, 1 stop reaped, 1 stop not-yet-reaped, all reaped, none reaped}.
    # ORACLE: an integer model `ever_stopped` incremented once per stop. The SUT computes
    #         total_stopped = REAPED_STOPPED + stopped_now (registry.rs:454); a stop migrates from
    #         stopped_now to REAPED on reap (registry.rs:472), so the sum == ever_stopped at every
    #         poll. NOTE (rule 2): the `+` at registry.rs:454 and `+=` at :442 are unchecked u64
    #         accumulators — the law also asserts no wrap occurs within the tested range; a
    #         realistic actor count cannot overflow u64, but the property pins it rather than assuming.
    # Generalizes: server_wire.feature "total_stopped is conserved across the reap boundary",
    #              "An actor stopped for exactly the grave window is still present, not reaped".

  # ---------------------------------------------------------------------------
  # @model — linearizability of seq and membership under concurrency
  # ---------------------------------------------------------------------------

  @model @linearizability
  Scenario: the membership list under one registry lock is a consistent snapshot for any spawn/stop interleaving
    Given a console server and any concurrent interleaving of spawn and stop operations
    When a client polls while those operations run with real overlap
    Then no two snapshots produced by the process ever share the same seq
    And every actor id appears at most once in the returned snapshot
    And the returned membership equals the registry's contents at one linearization point between the concurrent spawns/stops (no half-applied batch, no torn entry)
    # GEN: an op sequence over {spawn(id), stop(id)} of length [1, 64] run on tokio tasks with a
    #      Barrier for real overlap; ids include reused/duplicate ids and a stop racing its poll;
    #      include the empty-registry and single-actor boundaries; >=8 concurrent pollers.
    # ORACLE: a set model of live actor ids stepped by the op sequence — the snapshot's id set
    #         must equal the model's set at SOME point consistent with the partial order (a valid
    #         linearization), since the monitor set is cloned under one lock (registry.rs:423-427);
    #         and the global SEQ counter model guarantees all produced seqs are distinct.
    # Generalizes: server_wire.feature "Concurrent clients each receive a unique, never-reused seq",
    #              "actor membership of a snapshot is captured atomically under concurrent spawn",
    #              "A snapshot taken during concurrent stops shows each actor once, no torn entry".
