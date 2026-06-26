# Phase 2: laws (∀ inputs) and model-checks, layered on actor_id.feature's examples.
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
# Scope: src/actor/id.rs without the `remote` feature — ActorId is a single u64
#        sequence_id; to_bytes/from_bytes are an 8-byte LE round-trip; generate()
#        is fetch_add(1, Relaxed) over one process-wide AtomicUsize.

@core @actors @actor_id @phase2
Feature: ActorId — laws over byte round-trips, decode rejection, and generation
  As the addressing layer for actors
  I want serialization to be lossless and generation to be unique for ALL inputs
  So that no id value or interleaving silently breaks identity or addressing

  # ---------------------------------------------------------------------------
  # @property — universally-quantified laws
  # ---------------------------------------------------------------------------

  @property @lifecycle
  Scenario: from_bytes ∘ to_bytes is the identity for any ActorId
    Given any ActorId built from any sequence_id via new
    When it is encoded with to_bytes and decoded with from_bytes
    Then the decoded ActorId equals the original
    And the decoded sequence_id equals the original sequence_id
    # GEN: sequence_id ∈ boundary-biased u64 {0, 1, u64::MAX-1, u64::MAX, plus uniform}.
    # ORACLE: inverse-function round-trip — from_bytes is the left inverse of to_bytes
    #         (no `remote`: to_bytes is sequence_id.to_le_bytes(), id.rs:111-126;
    #          from_bytes reads bytes[0..8] LE, id.rs:140-144).
    # Generalizes: actor_id.feature "to_bytes then from_bytes round-trips an ActorId",
    #              "sequence_id is preserved across a byte round-trip".

  @property @boundary
  Scenario: from_bytes rejects any byte string shorter than eight bytes
    Given any byte slice of length n with n in [0, 7]
    When from_bytes is called on it
    Then it returns Err(ActorIdFromBytesError::MissingSequenceID)
    # GEN: n ∈ {0, 1, 7} (include empty and the largest too-short length); bytes uniform.
    # ORACLE: bytes[0..8].try_into() fails for any len < 8 -> MissingSequenceID
    #         (id.rs:140-143).
    # Generalizes: actor_id.feature "Decoding fewer than eight bytes fails…",
    #              "Decoding an empty slice fails…".

  @property @boundary
  Scenario: from_bytes accepts any eight-byte string and recovers its LE value (no remote)
    Given any u64 value v encoded as its 8 little-endian bytes
    When from_bytes is called on exactly those 8 bytes
    Then it returns Ok(ActorId) whose sequence_id equals v
    # GEN: v ∈ boundary-biased u64 {0, 1, u64::MAX-1, u64::MAX, plus uniform};
    #      input length is always exactly 8 (no `remote`: len is never > 8 so no peer
    #      id is parsed, id.rs:147-152 gated out).
    # ORACLE: u64::from_le_bytes is the inverse of v.to_le_bytes().
    # Generalizes: actor_id.feature "Decoding exactly eight bytes succeeds…".

  @property @sequence
  Scenario: sequential generate() is strictly increasing by exactly one per call
    Given any count n of generate() calls made back-to-back on a single thread
    When their sequence ids are read in call order
    Then each id is exactly one greater than the id of the previous call
    And all n ids are pairwise distinct
    # GEN: n ∈ boundary-biased usize {1, 2, 1000} (include the single-call and
    #      adjacent-pair boundaries); single thread so no interleaving.
    # ORACLE: a u64 counter incremented by 1 per call — SUT delta == oracle delta
    #         (generate() returns the pre-increment fetch_add(1, Relaxed), id.rs:72-79).
    # Generalizes: actor_id.feature "Sequential generate() calls produce strictly
    #              increasing sequence ids", "A batch … yields no duplicates".

  @property @boundary
  Scenario: Eq, Hash and Ord agree for any pair of ActorIds (no remote)
    Given any pair of ActorIds a, b built from any two sequence_ids
    When they are compared and hashed
    Then a == b iff their sequence_ids are equal
    And a == b implies hash(a) == hash(b)
    And the Ord of a, b equals the Ord of their sequence_ids
    # GEN: sequence_ids drawn from boundary-biased u64 {0, 1, u64::MAX-1, u64::MAX},
    #      including the equal-pair case and adjacent pairs.
    # ORACLE: the integer sequence_id itself — Eq/Hash/Ord derive over the sole field
    #         without `remote` (id.rs:19).
    # Generalizes: actor_id.feature "Equal ActorIds hash equally and unequal ones do
    #              not…", "Ordering follows sequence id…".

  # ---------------------------------------------------------------------------
  # @model — uniqueness under concurrent interleaving
  # ---------------------------------------------------------------------------

  @model @linearizability
  Scenario: N concurrent generate() calls yield N distinct, gap-free ids for any N
    Given the counter value before spawning is recorded as N0
    And any number P of tasks each calling generate() any number of times, k total calls
    When all tasks run with real overlap, started at a barrier
    Then the k assigned sequence ids are pairwise distinct
    And the set of assigned ids equals exactly the integers N0 through N0+k-1
    # GEN: P ∈ [2, 16]; k ∈ {1, 2, 100, 1000} (include the single-id and large-batch
    #      boundaries); each task's call count chosen so the totals sum to k.
    # ORACLE: an integer counter handing out [N0, N0+k) exactly once — the set of SUT
    #         ids must equal that contiguous range with no gaps or repeats. Atomicity
    #         of fetch_add suffices under Relaxed (id.rs:74). Small cases via loom for
    #         exhaustive interleavings; larger via proptest + randomized scheduling.
    # Generalizes: actor_id.feature "Concurrent generate() calls produce no duplicate
    #              sequence ids", "… collectively cover a contiguous id range".
