# Scope: console crate poller (console/src/poller.rs) — the client-side TCP poller thread
#        that requests snapshots from an instrumented bombay app and decodes them.
#
# Protocol, confirmed from source (console/src/poller.rs):
#   * Request: the client writes exactly ONE byte `[0]` (`Poller::poll`, :108).
#   * Reply:   a 4-byte BIG-ENDIAN u32 length prefix (`u32::from_be_bytes`, :110-112),
#              then that many bytes of a MessagePack-encoded `wire::Message`
#              (`rmp_serde::from_slice`, :123). Only the `Message::Snapshot` variant exists,
#              so decode binds `let Message::Snapshot(snapshot) = …` irrefutably.
#   * Cap:     `MAX_FRAME_BYTES = 64 * 1024 * 1024` (= 67_108_864), const at :19. A length
#              strictly GREATER than the cap is rejected with `io::ErrorKind::InvalidData`
#              BEFORE any allocation (:113-118). `len == MAX_FRAME_BYTES` is allowed.
#   * Backoff: a failed connect sets `ConnectionState::Disconnected { error, since }` and
#              sleeps `Duration::from_secs(5)` before retry (`connect_loop`, :49-55).
#   * Read timeout on the socket = `connection_timeout.max(1s)` (:98-99).
#   * Loop shape: `spawn_poller` = forever { connect_loop (retry until connected) ; poll_loop
#              (poll until first error, then return to reconnect) } (:28-34, :60-84).
#
# Authoring rules: one cross-cutting tag per Scenario; invariant-first Then; facts only;
# open questions are @review-semantics. No step definitions (wiring phase).

@console @poller
Feature: Poller — snapshot request/reply framing over TCP
  As a console client polling an instrumented bombay app
  I want a length-prefixed MessagePack frame protocol with a hard size cap
  So that snapshots decode correctly and a hostile peer cannot exhaust memory

  Background:
    Given a poller connected to a snapshot server over a TCP stream

  # ---------------------------------------------------------------------------
  # @sequence — the request/reply protocol in order, and repeated polls
  # ---------------------------------------------------------------------------

  @sequence
  Scenario: A poll writes exactly one request byte then reads a length-prefixed frame
    When the poller performs one poll
    Then the poller has written exactly the single byte 0x00 to the stream
    And it then read a 4-byte big-endian length prefix
    And it then read exactly that many payload bytes

  @sequence
  Scenario: A Snapshot encodes and decodes back to the same value (round-trip)
    Given a Snapshot with seq 7 and a known set of actors
    When the Snapshot is encoded as a length-prefixed MessagePack frame and the poller decodes it
    Then the decoded Snapshot equals the original Snapshot field-for-field

  @sequence
  Scenario: A successful poll publishes the decoded Snapshot into the shared slot
    Given the server will reply with a Snapshot whose seq is 42
    When the poller performs one poll
    Then the shared snapshot slot holds a Snapshot with seq 42

  @sequence
  Scenario: Sequential polls observe the server's advancing seq in order
    Given the server replies to successive requests with seq 0, then 1, then 2
    When the poller performs three polls in a row
    Then the shared snapshot slot ends holding seq 2
    And each poll observed a seq strictly greater than the previous poll's seq
    # NOTE: monotonicity here is the server's contract (registry SEQ.fetch_add); the poller
    # merely overwrites the slot with whatever it last decoded. The Then asserts the observed
    # order across the three decodes, not a poller-side guarantee.

  # ---------------------------------------------------------------------------
  # @lifecycle — connect / disconnect / retry / mid-poll server death
  # ---------------------------------------------------------------------------

  @lifecycle @timing
  Scenario: A connect timeout records Disconnected with the error and a since-instant
    Given no server is listening at the target address
    When the poller attempts to connect with a 50ms connection timeout
    Then the connection state becomes Disconnected carrying a non-empty error string
    And the Disconnected state carries a since instant captured at the failure

  @lifecycle @timing
  Scenario: After a failed connect the poller waits 5 seconds before retrying
    Given no server is listening at the target address
    When the poller's connect loop fails to connect once
    Then it sleeps for 5 seconds before the next connect attempt
    # NOTE: the backoff is a fixed Duration::from_secs(5) (:54); not exponential, not jittered.

  @lifecycle
  Scenario: The connection state transitions Connecting then Connected on a successful connect
    Given a server is listening at the target address
    When the poller's connect loop runs
    Then the connection state passed through Connecting
    And the connection state ends at Connected before the first poll

  @lifecycle
  Scenario: A server that shuts down mid-poll drops the poller back to reconnect
    Given a poller mid poll-loop against a live server
    When the server closes the connection before sending a full frame
    Then the poll returns an error
    And the connection state becomes Disconnected with that error
    And the poll loop returns so the outer loop reconnects

  @lifecycle
  Scenario: After a mid-poll failure the next connect re-establishes and resumes polling
    Given a poller whose poll loop just failed on a closed connection
    When a server becomes available again at the target address
    Then the poller reconnects and a subsequent poll publishes a fresh Snapshot

  # ---------------------------------------------------------------------------
  # @boundary — the MAX_FRAME_BYTES cap, garbage lengths, malformed payloads
  # ---------------------------------------------------------------------------

  @boundary
  Scenario: A frame whose length equals MAX_FRAME_BYTES is accepted
    Given the server replies with a length prefix equal to 67108864 (64 MiB)
    When the poller reads the length prefix
    Then the poller does not reject the frame on size
    # NOTE: the guard is `len > MAX_FRAME_BYTES` (:113), so the boundary value is inclusive.
    # (The payload that follows must still decode; this scenario only pins the size gate.)

  @boundary
  Scenario: A frame one byte larger than MAX_FRAME_BYTES is rejected as InvalidData
    Given the server replies with a length prefix equal to 67108865 (64 MiB + 1)
    When the poller reads the length prefix
    Then the poller returns an InvalidData error naming the frame size
    And the poller allocates no payload buffer for that frame

  @boundary
  Scenario: A garbage maximal length 0xFFFFFFFF is rejected before allocation
    Given the server replies with a length prefix of 0xFFFFFFFF
    When the poller reads the length prefix
    Then the poller returns an InvalidData error
    And the poller does not attempt to allocate 4 GiB

  @boundary
  Scenario: A zero-length frame decodes as an empty payload and fails MessagePack decode
    Given the server replies with a length prefix of 0 and no payload bytes
    When the poller reads the frame and attempts to decode it
    Then the decode fails with InvalidData
    And the poll returns an error so the poller reconnects
    # NOTE: len == 0 passes the size gate; read_exact of 0 bytes succeeds; rmp_serde on an
    # empty slice cannot yield a Message::Snapshot, so the map_err to InvalidData (:124) fires.

  @boundary
  Scenario: A well-sized frame carrying invalid MessagePack triggers reconnect
    Given the server replies with a valid length prefix but a non-MessagePack payload
    When the poller reads the payload and attempts to decode it
    Then the decode fails with InvalidData
    And the shared snapshot slot is left unchanged
    And the poll loop returns so the outer loop reconnects

  @boundary
  Scenario: A truncated payload (fewer bytes than the prefix promised) errors the poll
    Given the server sends a length prefix of N but only N-1 payload bytes then closes
    When the poller reads the payload
    Then read_exact returns an UnexpectedEof error
    And the poll returns that error so the poller reconnects
    # NOTE: io::Read::read_exact yields ErrorKind::UnexpectedEof on short reads.

  @boundary
  Scenario: A truncated length prefix (fewer than 4 bytes) errors the poll
    Given the server sends only 2 bytes of the 4-byte length prefix then closes
    When the poller reads the length prefix
    Then read_exact returns an UnexpectedEof error
    And the poll returns that error so the poller reconnects

  @boundary
  Scenario: A length prefix exactly at MAX with a payload that under-delivers
    Given the server replies with a length prefix of 67108864 but never sends that many bytes
    When the poller blocks reading the payload
    Then the socket read timeout (connection_timeout.max(1s)) eventually errors the poll
    And the poll returns that error so the poller reconnects
    # NOTE: the per-socket read timeout is set at connect (connection_timeout.max(1s), :98-99),
    # so a stalled max-size payload read times out rather than blocking forever.
