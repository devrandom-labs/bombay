# Phase 2 (card #74): laws over the console poller's frame protocol, layered on
# poller.feature's examples. See docs/testing/properties.md.
#   * @property — ∀ inputs. invariant(SUT(inputs)). Wired with proptest.
#   * Each scenario ALSO carries one Phase-1 category tag.
#   * # GEN: names the generator strategy AND the boundary values it must include.
#   * # ORACLE: names the reference model / inverse function.
#   * # Generalizes: the Phase-1 example scenario(s) this law subsumes.
#   * Facts only, grounded in console/src/poller.rs. No step definitions (wiring is Phase 3).
#
# Source facts (console/src/poller.rs):
#   * MAX_FRAME_BYTES = 64*1024*1024 = 67_108_864 (:19); the gate is `len > MAX_FRAME_BYTES`
#     (:113), so MAX is INCLUSIVE (accepted) and MAX+1 is rejected as InvalidData.
#   * length prefix is a 4-byte BIG-ENDIAN u32 (:110-112); decode is rmp_serde::from_slice
#     of a `wire::Message`, the only variant being Message::Snapshot (:123).
#   * wire::Snapshot derives Serialize/Deserialize but NOT PartialEq (wire.rs:21), so the
#     round-trip oracle compares re-encoded bytes / field-for-field, never `==`.

@console @poller @phase2
Feature: Poller framing — laws over msgpack round-trip and the frame-size cap
  As a console client decoding snapshots from an instrumented kameo app
  I want round-trip fidelity and the size cap to hold for ALL snapshots and ALL lengths
  So that no snapshot shape and no hostile length silently breaks decode or exhausts memory

  # ---------------------------------------------------------------------------
  # @property — universally-quantified laws
  # ---------------------------------------------------------------------------

  @property @sequence
  Scenario: decode(encode(Snapshot)) reproduces the Snapshot for any Snapshot
    Given any Snapshot value s
    When s is encoded as a MessagePack Message::Snapshot frame and the poller decodes it back
    Then the decoded Snapshot equals s field-for-field
    # GEN: arbitrary Snapshot — seq ∈ boundary-biased u64 {0, 1, u64::MAX}; actors vec of
    #      length {0, 1, many} with arbitrary ids/names (incl. empty name, unicode, "::" and
    #      generics), every ActorStatus/Flag/mailbox capacity {None, Some(0), Some(MAX)} variant.
    # ORACLE: the inverse function — rmp_serde::from_slice ∘ rmp_serde::to_vec_named == identity.
    #         Snapshot has no PartialEq (wire.rs:21), so compare by re-encoding both and asserting
    #         equal bytes (or assert each wire field equal), NOT `decoded == s`.
    # Generalizes: poller.feature "A Snapshot encodes and decodes back to the same value".

  @property @boundary
  Scenario: a length prefix is accepted iff it is <= MAX_FRAME_BYTES, rejected as InvalidData iff above
    Given any 4-byte big-endian length prefix L
    When the poller reads the length prefix and applies the size gate
    Then the frame passes the size gate iff L <= 67108864
    And it is rejected with an io::ErrorKind::InvalidData error naming the size iff L > 67108864
    # GEN: L ∈ boundary-biased u32 including {0, 1, MAX-1=67108863, MAX=67108864,
    #      MAX+1=67108865, u32::MAX=0xFFFFFFFF}.
    # ORACLE: the predicate `L <= MAX_FRAME_BYTES` (the gate is `len > MAX_FRAME_BYTES`, :113).
    # Generalizes: poller.feature "frame length equals MAX_FRAME_BYTES is accepted",
    #              "one byte larger than MAX_FRAME_BYTES is rejected as InvalidData",
    #              "garbage maximal length 0xFFFFFFFF is rejected before allocation".

  @property @boundary
  Scenario: an oversized length prefix is rejected before any payload buffer is allocated
    Given any length prefix L strictly greater than MAX_FRAME_BYTES
    When the poller applies the size gate
    Then it returns the InvalidData error without allocating a buffer of L bytes
    # GEN: L ∈ (67108864, u32::MAX] including {67108865, 1<<30, 0xFFFFFFFF}.
    # ORACLE: the gate returns before the `vec![0u8; len as usize]` allocation (:113-120) for
    #         every such L — assert rejection, and that no L-sized allocation is attempted.
    # Generalizes: poller.feature "garbage maximal length 0xFFFFFFFF is rejected before allocation",
    #              "one byte larger than MAX_FRAME_BYTES is rejected as InvalidData".

  @property @boundary
  Scenario: any byte string that is not valid MessagePack decodes to an error, never a panic
    Given any byte string B whose length is <= MAX_FRAME_BYTES
    When the poller passes B through rmp_serde decode
    Then it returns either a decoded Snapshot or an InvalidData error, and never panics
    And a B that is not a valid MessagePack Message::Snapshot yields the InvalidData error
    And on that error the shared snapshot slot is left unchanged
    # GEN: B ∈ {empty slice, random bytes, valid-msgpack-of-wrong-type, a truncated prefix of a
    #      valid frame, all-0xFF}; |B| ∈ {0, 1, small, up to MAX_FRAME_BYTES}.
    # ORACLE: decode is total over &[u8] — for all B it returns Result, mapping any rmp_serde
    #         error to ErrorKind::InvalidData (:123-124); never Ok unless B is a real Snapshot.
    # Generalizes: poller.feature "zero-length frame fails MessagePack decode",
    #              "well-sized frame carrying invalid MessagePack triggers reconnect".
