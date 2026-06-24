# Scope: in-tree console server + wire + registry (src/console/server.rs, wire.rs,
#        registry.rs) — the source side that an instrumented kameo app exposes over TCP.
#
# Protocol, confirmed from source (src/console/server.rs):
#   * serve_client loops: read exactly 1 byte (:101), build Snapshot, encode with
#     rmp_serde::to_vec_named (:106), write a 4-byte BIG-ENDIAN u32 length (:115) then the
#     payload (:116). Any read/write/encode error breaks the loop and closes the connection.
#   * The server reads ONLY one request byte per snapshot and never parses a length from the
#     client — there is no client-supplied size to bound, so there is no server-side
#     MAX_FRAME / garbage-length handling (that cap lives entirely in the client poller).
#   * Snapshot.seq is `SEQ.fetch_add(1, Relaxed)` per produced snapshot (registry.rs:447) —
#     a process-global monotonic counter, incremented once per snapshot, across all clients.
#   * snapshot() takes the registry lock once, reaps stopped-past-grave-window monitors,
#     clones the Arc<ActorMonitor> set, releases the lock, then renders each to wire
#     (registry.rs:422-458). The actor LIST is thus a consistent atomic membership; per-actor
#     fields are read live afterward (relaxed atomics / short mutexes), not under one snapshot.
#
# Already covered by tests/console.rs (6 happy-path integration tests):
#   1. serves_live_snapshot_over_tcp        — running actor appears, messages_received>=1, alive>=1
#   2. deadlock_shows_as_a_wait_for_cycle   — mutual ask forms reciprocal waiting_on edges
#   3. handler_activity_is_reported_while_handling — handling.message + elapsed mid-handler
#   4. supervised_restart_increments_restarts — restart/panic counters; no stale handler post-restart
#   5. message_types_and_supervision_are_reported — per-type counts; SupervisionInfo policy/max
#   6. dead_actor_appears_then_is_reaped     — Stopped lingers within grave window, reaped after
#
# GAPS this file captures (NONE of the above exercise these): seq monotonicity across rapid
# polls, atomicity of the actor-membership list under concurrent spawn/stop, error-path
# connection teardown, and the server's (non-)handling of malformed client input.
#
# Authoring rules: one cross-cutting tag per Scenario; invariant-first Then; facts only;
# open questions are @review-semantics. No step definitions (wiring phase).

@console @server_wire
Feature: Console server + wire — snapshot streaming gaps beyond the happy path
  As an instrumented kameo process serving a console
  I want snapshot seq to advance monotonically and the actor set to be captured atomically
  So that a client never sees a stale seq, a torn membership list, or a hung connection

  # ---------------------------------------------------------------------------
  # @sequence — seq monotonicity across rapid polls on one connection
  # ---------------------------------------------------------------------------

  @sequence
  Scenario: seq strictly increases across rapid sequential polls on one connection
    Given a console server with at least one live actor
    And a single open client connection
    When the client requests 5 snapshots back to back
    Then each snapshot's seq is strictly greater than the previous one
    # INVARIANT: SEQ.fetch_add(1) per produced snapshot — never repeated, never decreasing
    # within a single process lifetime (registry.rs:447).

  @sequence
  Scenario: seq advances by exactly one per produced snapshot
    Given a console server and a single open client connection
    When the client requests two snapshots back to back
    Then the second snapshot's seq equals the first snapshot's seq plus one
    # NOTE: fetch_add(1) with no other snapshot producers means a +1 step. With concurrent
    # clients the step may be larger (covered below) — pin "exactly +1" only for the
    # single-connection, no-concurrent-poll case.

  @sequence
  Scenario: captured_at and uptime advance alongside seq
    Given a console server and a single open client connection
    When the client requests two snapshots a short interval apart
    Then the second snapshot's captured_at is at or after the first's
    And the second snapshot's uptime is at or after the first's

  # ---------------------------------------------------------------------------
  # @linearizability — concurrent clients + concurrent spawn/stop during snapshot
  # ---------------------------------------------------------------------------

  @linearizability
  Scenario: Concurrent clients each receive a unique, never-reused seq
    Given a console server with live actors
    And 8 client connections polling concurrently
    When each connection requests several snapshots overlapping in time
    Then no two snapshots produced by the process share the same seq
    # INVARIANT: the global atomic SEQ is shared across all serve_client tasks, so even under
    # concurrent polls every produced snapshot carries a distinct seq.

  @linearizability
  Scenario: The actor membership of a snapshot is captured atomically under concurrent spawn
    Given a console server
    When a client polls while many actors are being spawned concurrently
    Then every actor in the returned snapshot is internally consistent (id present, status set)
    And the snapshot reflects a single registry membership, not a half-applied spawn batch
    # INVARIANT: snapshot() clones the monitor set under one registry lock (registry.rs:423-427),
    # so the LIST of actors is a consistent membership. Per-actor fields are read live after the
    # lock — assert list consistency, not a frozen point-in-time for every counter.

  @linearizability
  Scenario: A snapshot taken during concurrent stops shows each actor once, no torn entry
    Given a console server with actors stopping concurrently with a poll
    When a client polls during the stop storm
    Then each actor id appears at most once in the snapshot
    And a stopping/stopped actor renders a coherent status (never a partially-built entry)

  @linearizability
  Scenario: An actor stopped just before a poll still appears within its grave window
    Given an actor that stops immediately before a poll
    And a grave window longer than the poll latency
    When the client polls
    Then the stopped actor is present with status Stopped carrying its stop reason
    # NOTE: companion to dead_actor_appears_then_is_reaped, but races the stop against the poll
    # rather than sleeping a fixed interval first.

  # ---------------------------------------------------------------------------
  # @lifecycle — connection error paths the 6 happy-path tests never hit
  # ---------------------------------------------------------------------------

  @lifecycle
  Scenario: The server closes the connection when the client disconnects mid-stream
    Given a console server and an open client connection
    When the client closes the socket without sending a request byte
    Then the serve_client loop's read_exact errors and the task ends cleanly
    # INVARIANT: read_exact failure breaks the loop (server.rs:101-103); no panic, no leak.

  @lifecycle
  Scenario: One client's disconnect does not disturb another client's stream
    Given a console server with two open client connections
    When the first client disconnects abruptly
    Then the second client can still request and receive a fresh snapshot
    # INVARIANT: each connection is its own spawned serve_client task (server.rs:58).

  @lifecycle
  Scenario: A client that connects but never requests receives nothing and blocks the server task on read
    Given a console server and a client that connects but sends no byte
    When no request byte is ever written
    Then the server produces no snapshot for that connection
    # INVARIANT: serving is pull-based — an idle (silent) client triggers zero collection work.

  @lifecycle
  Scenario: shutdown aborts the accept loop so new connections are refused
    Given a running console server
    When the handle's shutdown is called
    Then subsequent connection attempts to the bound address are refused
    # NOTE: shutdown aborts the listener task (server.rs:93-95); in-flight serve_client tasks
    # may still drain — pin whether shutdown also aborts active streams at wiring time.

  # ---------------------------------------------------------------------------
  # @boundary — malformed / surplus client input at the server boundary
  # ---------------------------------------------------------------------------

  @boundary
  Scenario: The request byte value is ignored — any byte triggers one snapshot
    Given a console server and an open client connection
    When the client sends the byte 0xFF instead of 0x00
    Then the server still replies with exactly one length-prefixed snapshot frame
    # INVARIANT: serve_client reads one byte into `request` but never inspects its value
    # (server.rs:99-104) — only the act of sending a byte matters, not the byte itself.

  @boundary
  Scenario: Multiple buffered request bytes yield one snapshot each, in order
    Given a console server and an open client connection
    When the client sends 3 request bytes in one write before reading any reply
    Then the server replies with 3 length-prefixed snapshot frames
    And those frames carry strictly increasing seq values
    # INVARIANT: one read_exact(1 byte) ⇒ one snapshot per loop iteration; pipelined requests
    # are answered one-for-one with monotonic seq.

  @boundary
  Scenario: The server applies no frame-size cap because it never reads a client length
    Given a console server
    When a client sends arbitrary surplus bytes after its request byte
    Then the server treats each byte as a fresh request trigger
    And the server never parses or allocates on a client-supplied length
    # NOTE: there is NO MAX_FRAME_BYTES on the server read path — the 64 MiB cap is purely the
    # client poller's. The server allocates nothing on behalf of client-supplied sizes, so it has
    # no oversized-input vulnerability of its own.

  @boundary
  Scenario: A snapshot that fails to encode closes the connection without a partial frame
    Given a console server whose snapshot would fail MessagePack encoding
    When the client requests a snapshot
    Then the server writes no length prefix and closes the connection
    # NOTE: encode error breaks before any write (server.rs:106-113), so the client sees EOF on
    # the length read, never a truncated frame. Constructing an unencodable Snapshot to drive this
    # is a wiring detail, not an open semantic.

  @boundary
  Scenario: An actor stopped for exactly the grave window is still present, not reaped
    Given an actor that has been stopped for exactly the grave window duration
    When the client polls
    Then the actor is still present with status Stopped
    And an actor stopped for strictly longer than the grave window is absent from the snapshot
    # NOTE (registry.rs:470): the reap predicate is `s.since.elapsed() > ttl` — strictly
    # greater-than, so the boundary is INCLUSIVE of the live side: at exactly ttl the actor
    # survives; only once elapsed exceeds ttl is it reaped. Needs a paused clock at wiring.

  @sequence
  Scenario: total_stopped is conserved across the reap boundary — no double-count, no loss
    Given two actors have stopped and been reaped, then a third stops but is not yet reaped
    When the client polls
    Then totals.total_stopped equals 3
    And it equals REAPED_STOPPED (2 already reaped) plus the 1 stopped-but-still-present actor
    # NOTE (registry.rs:454): total_stopped = REAPED_STOPPED.load() + stopped_now. A stopped
    # actor is counted in `stopped_now` while present and migrates to REAPED_STOPPED on reap
    # (registry.rs:472), so the sum is stable across the reap — each stopped actor is counted
    # exactly once whether or not it has been reaped yet.
