# Scope: kameo core ActorId (src/actor/id.rs) — generate() uniqueness via an
#        atomic counter, byte serialization round-trips, the too-short decode
#        error, equality/hash/ordering. Behaviour differs by the `remote`
#        feature: without it, ActorId is just a u64 sequence_id; with it, a
#        PeerIdKind is folded into eq/hash/ord and the byte layout.
#
# Authoring rules (mirrors tests/features/actors/message_queue.feature):
#   * Every Scenario carries exactly ONE cross-cutting tag.
#   * Invariant-first; unverified guarantees are `# NOTE:` + @review-semantics.
#   * Facts only: every Then is confirmed from the source above.

@core @actors @actor_id
Feature: ActorId — generation, byte round-trips, identity
  As the addressing layer for actors
  I want ActorId generation and serialization to be unique and lossless
  So that actors can be identified and referenced reliably

  # ---------------------------------------------------------------------------
  # @sequence — sequential generation order
  # ---------------------------------------------------------------------------

  @sequence
  Scenario: Sequential generate() calls produce strictly increasing sequence ids
    Given two ActorIds are generated one after another on a single thread
    When their sequence ids are compared
    Then the second sequence id is exactly the first plus one
    # Confirmed: generate() returns the pre-increment value of a single
    # AtomicUsize via fetch_add(1, Relaxed) (id.rs:72-79).

  @sequence
  Scenario: A batch of sequential generate() calls yields no duplicates
    Given 1000 ActorIds are generated sequentially
    When their sequence ids are collected
    Then all 1000 sequence ids are distinct
    # Confirmed: each generate() consumes a unique counter slot (id.rs:74).

  # ---------------------------------------------------------------------------
  # @lifecycle — byte round-trips (write -> read -> verify)
  # ---------------------------------------------------------------------------

  @lifecycle
  Scenario: to_bytes then from_bytes round-trips an ActorId
    Given an ActorId generated locally
    When it is encoded with to_bytes and decoded with from_bytes
    Then the decoded ActorId equals the original
    # Confirmed (no `remote`): to_bytes writes the 8 LE bytes of sequence_id
    # (id.rs:110-127) and from_bytes reads them back (id.rs:138-159).

  @lifecycle
  Scenario: A locally-built ActorId encodes to exactly eight bytes without the remote feature
    Given an ActorId created via new(7)
    When it is encoded with to_bytes
    Then the encoding is 8 bytes long
    # Confirmed: without `remote`, to_bytes only extends with
    # sequence_id.to_le_bytes() (id.rs:111-112, 126).

  @lifecycle
  Scenario: sequence_id is preserved across a byte round-trip
    Given an ActorId created via new(123456789)
    When it is encoded and decoded
    Then the decoded sequence_id is 123456789
    # Confirmed: u64 little-endian round-trip is lossless (id.rs:112, 140-144).

  # ---------------------------------------------------------------------------
  # @boundary — malformed decode input, ordering edges
  # ---------------------------------------------------------------------------

  @boundary
  Scenario: Decoding fewer than eight bytes fails with MissingSequenceID
    Given a 4-byte slice
    When from_bytes is called on it
    Then it returns Err(ActorIdFromBytesError::MissingSequenceID)
    # Confirmed: bytes[0..8].try_into() fails for short input and maps to
    # MissingSequenceID (id.rs:140-143).

  @boundary
  Scenario: Decoding an empty slice fails with MissingSequenceID
    Given an empty byte slice
    When from_bytes is called on it
    Then it returns Err(ActorIdFromBytesError::MissingSequenceID)
    # Confirmed: same too-short path (id.rs:140-143).

  @boundary
  Scenario: Decoding exactly eight bytes succeeds without the remote feature
    Given the 8 little-endian bytes of the value 1
    When from_bytes is called on them
    Then it returns an ActorId with sequence_id 1
    # Confirmed (no `remote`): len is not > 8, so no peer id is parsed
    # (id.rs:147-152 is gated out) and decode succeeds.

  @boundary
  Scenario: Ordering follows sequence id for locally-generated ActorIds
    Given an earlier-generated ActorId and a later-generated ActorId
    When they are ordered
    Then the earlier id sorts before the later id
    # Confirmed: ActorId derives Ord; without `remote` the only field is
    # sequence_id, and generate() assigns increasing values (id.rs:19, 72-79).
    # NOTE @review-semantics: WITH `remote`, Ord compares PeerIdKind first then
    # sequence_id (id.rs:19, 304-308), so cross-peer ordering is peer-then-seq;
    # pin that intended total order before asserting cross-peer cases.

  @boundary
  Scenario: generate() never panics on any reachable call and every id is unique
    Given generate() is called repeatedly
    When each call assigns the next sequence id
    Then no reachable call panics and every assigned id is unique
    # generate() does atomic fetch_add(1, Relaxed).try_into() (id.rs:74-78); on
    # 64-bit usize==u64 so try_into never fails. True u64 counter exhaustion is
    # unreachable/untestable and documented, not wired.

  # ---------------------------------------------------------------------------
  # @linearizability — concurrent generation under the Relaxed counter
  # ---------------------------------------------------------------------------

  @linearizability
  Scenario: Concurrent generate() calls produce no duplicate sequence ids
    Given 1000 ActorIds are generated across 10 tasks started at a barrier
    When all generated sequence ids are collected
    Then all 1000 sequence ids are pairwise distinct
    # Confirmed: fetch_add is atomic, so each concurrent caller gets a unique
    # slot even under Relaxed ordering — uniqueness needs only atomicity, not
    # ordering (id.rs:74).

  @linearizability
  Scenario: Concurrent generate() calls collectively cover a contiguous id range
    Given the counter value before spawning is recorded as N
    And 100 ActorIds are generated concurrently from several tasks
    When the generated sequence ids are collected into a set
    Then the set equals exactly the integers N through N+99
    # Confirmed: fetch_add hands out every integer in [N, N+100) exactly once
    # with no gaps and no repeats (id.rs:74).

  @linearizability
  Scenario: Equal ActorIds hash equally and unequal ones do not, without the remote feature
    Given two ActorIds with the same sequence_id and two with different ones
    When each pair is compared for equality and hashed
    Then the same-sequence pair is equal with equal hashes
    And the different-sequence pair is unequal
    # Confirmed (no `remote`): Eq/Hash derive over the sole sequence_id field
    # (id.rs:19); see existing unit tests id.rs:340-365, 440-467.
