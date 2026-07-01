//! Shared `ActorIdWorld` + step definitions for the core `ActorId` scenarios.
//!
//! Wired by two runners that `#[path]`-include this module:
//!   * `core_actor_id_bdd.rs`        — the example feature (actor_id.feature)
//!   * `core_actor_id_props_bdd.rs`  — the property laws (actor_id.properties.feature)
//!
//! Every assertion is the SPECIFIC value confirmed in the scenario's
//! `# Confirmed:` / `# ORACLE:` note (facts only — no vague `contains`).
//!
//! The `@boundary` decode-rejection scenarios (a slice shorter than 8 bytes,
//! and a truncated buffer fed through serde `Deserialize`) assert the
//! defensive-boundary contract: `from_bytes` returns `MissingSequenceID` and
//! the serde visitor maps it to `invalid_length`, WITHOUT panicking. These
//! pinned the `@bug:id.rs:140-143` / `:218-221` defect fixed under card #80
//! (bounds-check before slicing `bytes[0..8]`); before the fix they panicked.
//!
//! `ActorId::generate()` increments one PROCESS-GLOBAL `AtomicUsize`
//! (`fetch_add(1, Relaxed)`, id.rs:72-79), shared across every scenario in the
//! process. The generation scenarios therefore assert RELATIVE facts (delta of
//! +1, pairwise-distinctness, contiguity of the OBSERVED range) that hold from
//! any counter start point — never an absolute id value.

use std::{
    collections::{HashSet, hash_map::DefaultHasher},
    hash::{Hash, Hasher},
    sync::Arc,
};

use bombay::actor::{ActorId, ActorIdFromBytesError};
use cucumber::{World, given, then, when};
use proptest::prelude::*;
use tokio::sync::Barrier;

#[derive(Debug, Default, World)]
pub struct ActorIdWorld {
    /// Ids generated in a scenario (sequence / linearizability scenarios).
    ids: Vec<ActorId>,
    /// First / second operand for pair scenarios (ordering, equality).
    a: Option<ActorId>,
    b: Option<ActorId>,
    /// Same-sequence pair (equality scenario).
    eq_pair: Option<(ActorId, ActorId)>,
    /// Different-sequence pair (equality scenario).
    ne_pair: Option<(ActorId, ActorId)>,
    /// Encoded bytes captured by a When step, asserted by a Then step.
    bytes: Vec<u8>,
    /// Decoded ActorId from a byte round-trip.
    decoded: Option<ActorId>,
    /// Result of a `from_bytes` decode attempt on a (possibly truncated) slice
    /// (the @boundary decode-rejection scenarios).
    decode_result: Option<Result<ActorId, ActorIdFromBytesError>>,
    /// Result of deserializing a (possibly truncated) buffer through the real
    /// serde `Deserialize` path; the Err arm holds the serde error's `Display`.
    serde_result: Option<Result<ActorId, String>>,
    /// Last formatted (Display/Debug) string.
    last_string: String,
}

fn hash_of(id: &ActorId) -> u64 {
    let mut h = DefaultHasher::new();
    id.hash(&mut h);
    h.finish()
}

// ===========================================================================
// @sequence — sequential generation order
// ===========================================================================

#[given(regex = r"^two ActorIds are generated one after another on a single thread$")]
async fn given_two_generated(world: &mut ActorIdWorld) {
    world.ids.push(ActorId::generate());
    world.ids.push(ActorId::generate());
}

#[when(regex = r"^their sequence ids are compared$")]
async fn when_seq_ids_compared(_world: &mut ActorIdWorld) {
    // The comparison itself is the Then assertion; nothing to do here.
}

#[then(regex = r"^the second sequence id is exactly the first plus one$")]
async fn then_second_is_first_plus_one(world: &mut ActorIdWorld) {
    assert_eq!(world.ids.len(), 2, "expected two ids, got {:?}", world.ids);
    let first = world.ids[0].sequence_id();
    let second = world.ids[1].sequence_id();
    assert_eq!(
        second,
        first + 1,
        "second sequence id must be first + 1, got first={first} second={second}"
    );
}

#[given(regex = r"^1000 ActorIds are generated sequentially$")]
async fn given_1000_generated(world: &mut ActorIdWorld) {
    for _ in 0..1000 {
        world.ids.push(ActorId::generate());
    }
}

#[when(regex = r"^their sequence ids are collected$")]
async fn when_seq_ids_collected(_world: &mut ActorIdWorld) {}

#[then(regex = r"^all 1000 sequence ids are distinct$")]
async fn then_1000_distinct(world: &mut ActorIdWorld) {
    assert_eq!(world.ids.len(), 1000, "expected 1000 ids");
    let set: HashSet<u64> = world.ids.iter().map(ActorId::sequence_id).collect();
    assert_eq!(
        set.len(),
        1000,
        "all 1000 sequence ids must be distinct, found {} unique",
        set.len()
    );
}

// ===========================================================================
// @lifecycle — byte round-trips
// ===========================================================================

#[given(regex = r"^an ActorId generated locally$")]
async fn given_one_generated(world: &mut ActorIdWorld) {
    world.a = Some(ActorId::generate());
}

#[when(regex = r"^it is encoded with to_bytes and decoded with from_bytes$")]
async fn when_encode_decode(world: &mut ActorIdWorld) {
    let id = world.a.expect("an ActorId");
    world.bytes = id.to_bytes();
    world.decoded = Some(ActorId::from_bytes(&world.bytes).expect("8-byte decode succeeds"));
}

#[then(regex = r"^the decoded ActorId equals the original$")]
async fn then_decoded_equals_original(world: &mut ActorIdWorld) {
    let original = world.a.expect("an original ActorId");
    let decoded = world.decoded.expect("a decoded ActorId");
    assert_eq!(decoded, original, "decoded ActorId must equal the original");
}

#[given(regex = r"^an ActorId created via new\((\d+)\)$")]
async fn given_new(world: &mut ActorIdWorld, seq: u64) {
    world.a = Some(ActorId::new(seq));
}

#[when(regex = r"^it is encoded with to_bytes$")]
async fn when_encode(world: &mut ActorIdWorld) {
    world.bytes = world.a.expect("an ActorId").to_bytes();
}

#[then(regex = r"^the encoding is 8 bytes long$")]
async fn then_encoding_is_8_bytes(world: &mut ActorIdWorld) {
    assert_eq!(
        world.bytes.len(),
        8,
        "without the remote feature, to_bytes must be exactly 8 bytes"
    );
}

#[when(regex = r"^it is encoded and decoded$")]
async fn when_encoded_and_decoded(world: &mut ActorIdWorld) {
    let id = world.a.expect("an ActorId");
    world.bytes = id.to_bytes();
    world.decoded = Some(ActorId::from_bytes(&world.bytes).expect("8-byte decode succeeds"));
}

#[then(regex = r"^the decoded sequence_id is (\d+)$")]
async fn then_decoded_seq_is(world: &mut ActorIdWorld, expected: u64) {
    let decoded = world.decoded.expect("a decoded ActorId");
    assert_eq!(
        decoded.sequence_id(),
        expected,
        "decoded sequence_id must be {expected}"
    );
}

#[when(regex = r"^it is serialized with serde and then deserialized$")]
async fn when_serde_roundtrip(world: &mut ActorIdWorld) {
    let id = world.a.expect("an ActorId");
    let buf = rmp_serde::to_vec(&id).expect("serialize");
    world.decoded = Some(rmp_serde::from_slice(&buf).expect("deserialize a well-formed buffer"));
}

#[then(regex = r"^the deserialized ActorId equals the original$")]
async fn then_deserialized_equals_original(world: &mut ActorIdWorld) {
    let original = world.a.expect("an original ActorId");
    let decoded = world.decoded.expect("a deserialized ActorId");
    assert_eq!(
        decoded, original,
        "deserialized ActorId must equal original"
    );
}

// ===========================================================================
// @boundary — Display / Debug formatting, decode success, ordering, generate()
// ===========================================================================

#[when(regex = r#"^it is formatted with Display \("\{\}"\)$"#)]
async fn when_format_display(world: &mut ActorIdWorld) {
    world.last_string = format!("{}", world.a.expect("an ActorId"));
}

#[when(regex = r#"^it is formatted with Debug \("\{:\?\}"\)$"#)]
async fn when_format_debug(world: &mut ActorIdWorld) {
    world.last_string = format!("{:?}", world.a.expect("an ActorId"));
}

#[then(regex = r#"^the output is "(.+)"$"#)]
async fn then_output_is(world: &mut ActorIdWorld, expected: String) {
    assert_eq!(
        world.last_string, expected,
        "formatted output must be exactly {expected:?}"
    );
}

#[given(regex = r"^the 8 little-endian bytes of the value 1$")]
async fn given_8_le_bytes_of_one(world: &mut ActorIdWorld) {
    world.bytes = 1u64.to_le_bytes().to_vec();
}

#[when(regex = r"^from_bytes is called on them$")]
async fn when_from_bytes_on_them(world: &mut ActorIdWorld) {
    world.decoded = Some(ActorId::from_bytes(&world.bytes).expect("exactly 8 bytes decode"));
}

#[then(regex = r"^it returns an ActorId with sequence_id (\d+)$")]
async fn then_returns_actorid_with_seq(world: &mut ActorIdWorld, expected: u64) {
    let decoded = world.decoded.expect("a decoded ActorId");
    assert_eq!(
        decoded.sequence_id(),
        expected,
        "decoded sequence_id must be {expected}"
    );
}

// ---------------------------------------------------------------------------
// @boundary — truncated decode input must error, not panic (card #80)
// ---------------------------------------------------------------------------

#[given(regex = r"^a 4-byte slice$")]
async fn given_4_byte_slice(world: &mut ActorIdWorld) {
    world.bytes = vec![0u8; 4];
}

#[given(regex = r"^an empty byte slice$")]
async fn given_empty_slice(world: &mut ActorIdWorld) {
    world.bytes = Vec::new();
}

#[when(regex = r"^from_bytes is called on it$")]
async fn when_from_bytes_on_it(world: &mut ActorIdWorld) {
    // No unwrap: capture the Result so the Then can assert the clean error.
    // Before the bounds-check fix this call panicked on `bytes[0..8]`.
    world.decode_result = Some(ActorId::from_bytes(&world.bytes));
}

#[then(regex = r"^it returns Err\(ActorIdFromBytesError::MissingSequenceID\) without panicking$")]
async fn then_err_missing_sequence_id(world: &mut ActorIdWorld) {
    let result = world
        .decode_result
        .as_ref()
        .expect("from_bytes must have been called");
    assert!(
        matches!(result, Err(ActorIdFromBytesError::MissingSequenceID)),
        "from_bytes on a slice shorter than 8 bytes must return \
         Err(MissingSequenceID) without panicking, got {result:?}"
    );
}

#[given(regex = r"^a serialized byte buffer of only 4 bytes fed to ActorId's Deserialize$")]
async fn given_serialized_4_byte_buffer(world: &mut ActorIdWorld) {
    // A MessagePack `bin` payload of 4 bytes; rmp_serde routes it to
    // ActorIdVisitor::visit_bytes — the real Deserialize path a truncated
    // wire buffer would take.
    world.bytes = rmp_serde::to_vec(&serde_bytes::Bytes::new(&[0u8; 4]))
        .expect("serialize 4-byte bin payload");
}

#[when(regex = r"^the ActorIdVisitor's visit_bytes runs from_bytes on it$")]
async fn when_visit_bytes_runs(world: &mut ActorIdWorld) {
    let result: Result<ActorId, rmp_serde::decode::Error> = rmp_serde::from_slice(&world.bytes);
    world.serde_result = Some(result.map_err(|err| err.to_string()));
}

#[then(
    regex = r#"^deserialization fails with serde invalid_length\(4, "sequence ID"\), no panic$"#
)]
async fn then_deserialize_invalid_length(world: &mut ActorIdWorld) {
    let result = world
        .serde_result
        .as_ref()
        .expect("deserialization must have been attempted");
    let message = result
        .as_ref()
        .err()
        .expect("deserializing a 4-byte buffer must fail, not succeed");
    assert!(
        message.contains("invalid length 4, expected sequence ID"),
        "serde must reject the truncated buffer with invalid_length(4, \"sequence ID\"), \
         got: {message}"
    );
}

#[given(regex = r"^an earlier-generated ActorId and a later-generated ActorId$")]
async fn given_earlier_later(world: &mut ActorIdWorld) {
    world.a = Some(ActorId::generate()); // earlier
    world.b = Some(ActorId::generate()); // later
}

#[when(regex = r"^they are ordered$")]
async fn when_they_are_ordered(_world: &mut ActorIdWorld) {}

#[then(regex = r"^the earlier id sorts before the later id$")]
async fn then_earlier_before_later(world: &mut ActorIdWorld) {
    let earlier = world.a.expect("earlier id");
    let later = world.b.expect("later id");
    assert!(
        earlier < later,
        "earlier id {earlier:?} must sort before later id {later:?}"
    );
}

#[given(regex = r"^generate\(\) is called repeatedly$")]
async fn given_generate_repeatedly(world: &mut ActorIdWorld) {
    for _ in 0..1000 {
        world.ids.push(ActorId::generate());
    }
}

#[when(regex = r"^each call assigns the next sequence id$")]
async fn when_each_call_assigns(_world: &mut ActorIdWorld) {}

#[then(regex = r"^no reachable call panics and every assigned id is unique$")]
async fn then_no_panic_all_unique(world: &mut ActorIdWorld) {
    // Reaching this Then proves no call panicked (a panic would have aborted
    // the Given). Assert the substantive guarantee: pairwise-distinct ids.
    let n = world.ids.len();
    assert_eq!(n, 1000, "expected 1000 generated ids");
    let set: HashSet<u64> = world.ids.iter().map(ActorId::sequence_id).collect();
    assert_eq!(set.len(), n, "every assigned id must be unique");
}

// ===========================================================================
// @linearizability — concurrent generation under the Relaxed counter
// ===========================================================================

#[given(regex = r"^1000 ActorIds are generated across 10 tasks started at a barrier$")]
async fn given_1000_across_10_tasks(world: &mut ActorIdWorld) {
    let tasks = 10usize;
    let per_task = 100usize; // 10 * 100 == 1000
    let barrier = Arc::new(Barrier::new(tasks));
    let handles: Vec<_> = (0..tasks)
        .map(|_| {
            let barrier = Arc::clone(&barrier);
            tokio::spawn(async move {
                barrier.wait().await;
                (0..per_task)
                    .map(|_| ActorId::generate())
                    .collect::<Vec<_>>()
            })
        })
        .collect();
    for h in handles {
        world
            .ids
            .extend(h.await.expect("generator task must not panic"));
    }
}

#[when(regex = r"^all generated sequence ids are collected$")]
async fn when_all_collected(_world: &mut ActorIdWorld) {}

#[then(regex = r"^all 1000 sequence ids are pairwise distinct$")]
async fn then_1000_pairwise_distinct(world: &mut ActorIdWorld) {
    assert_eq!(world.ids.len(), 1000, "expected 1000 ids across the tasks");
    let set: HashSet<u64> = world.ids.iter().map(ActorId::sequence_id).collect();
    assert_eq!(
        set.len(),
        1000,
        "concurrent fetch_add must hand each task a unique slot — found {} unique",
        set.len()
    );
}

#[given(regex = r"^the counter value before spawning is recorded as N$")]
async fn given_counter_recorded_as_n(_world: &mut ActorIdWorld) {
    // The process-global counter is private; N is derived from the OBSERVED
    // minimum of the contiguous range in the Then step (the oracle is integer
    // contiguity, not an absolute counter read). Nothing to record here.
}

#[given(regex = r"^100 ActorIds are generated concurrently from several tasks$")]
async fn given_100_concurrent(world: &mut ActorIdWorld) {
    let tasks = 4usize;
    let per_task = 25usize; // 4 * 25 == 100
    let barrier = Arc::new(Barrier::new(tasks));
    let handles: Vec<_> = (0..tasks)
        .map(|_| {
            let barrier = Arc::clone(&barrier);
            tokio::spawn(async move {
                barrier.wait().await;
                (0..per_task)
                    .map(|_| ActorId::generate())
                    .collect::<Vec<_>>()
            })
        })
        .collect();
    for h in handles {
        world
            .ids
            .extend(h.await.expect("generator task must not panic"));
    }
}

#[when(regex = r"^the generated sequence ids are collected into a set$")]
async fn when_collected_into_set(_world: &mut ActorIdWorld) {}

#[then(regex = r"^the set equals exactly the integers N through N\+99$")]
async fn then_set_is_contiguous_100(world: &mut ActorIdWorld) {
    let seqs: Vec<u64> = world.ids.iter().map(ActorId::sequence_id).collect();
    assert_eq!(seqs.len(), 100, "expected 100 generated ids");
    let set: HashSet<u64> = seqs.iter().copied().collect();
    assert_eq!(set.len(), 100, "the 100 ids must be pairwise distinct");
    // N = observed minimum; the oracle is the contiguous integer range
    // [N, N+100) handed out by fetch_add with no gaps or repeats. The oracle
    // is built from integers ONLY — it does not call the SUT again.
    let n = *set.iter().min().expect("non-empty set");
    let expected: HashSet<u64> = (n..n + 100).collect();
    assert_eq!(
        set, expected,
        "the assigned ids must equal exactly the contiguous range [N, N+100)"
    );
}

#[given(regex = r"^two ActorIds with the same sequence_id and two with different ones$")]
async fn given_eq_and_ne_pairs(world: &mut ActorIdWorld) {
    world.eq_pair = Some((ActorId::new(42), ActorId::new(42)));
    world.ne_pair = Some((ActorId::new(1), ActorId::new(2)));
}

#[when(regex = r"^each pair is compared for equality and hashed$")]
async fn when_pairs_compared(_world: &mut ActorIdWorld) {}

#[then(regex = r"^the same-sequence pair is equal with equal hashes$")]
async fn then_same_pair_equal(world: &mut ActorIdWorld) {
    let (a, b) = world.eq_pair.expect("equal pair");
    assert_eq!(a, b, "same-sequence ActorIds must be equal");
    assert_eq!(hash_of(&a), hash_of(&b), "equal ActorIds must hash equally");
}

#[then(regex = r"^the different-sequence pair is unequal$")]
async fn then_diff_pair_unequal(world: &mut ActorIdWorld) {
    let (a, b) = world.ne_pair.expect("unequal pair");
    assert_ne!(a, b, "different-sequence ActorIds must be unequal");
}

// ===========================================================================
// @property laws (actor_id.properties.feature) — proptest with boundary-biased
// generators that hit the `# GEN:` values, asserting the `# ORACLE:`.
// ===========================================================================

#[given(regex = r"^any ActorId built from any sequence_id via new$")]
async fn given_any_actorid(world: &mut ActorIdWorld) {
    // A representative built-via-new ActorId so the SHARED encode/decode When
    // and the `the decoded ActorId equals the original` Then (reused from the
    // example feature) operate on real state. The universal ∀-inputs law is
    // carried by the `And the decoded sequence_id equals the original
    // sequence_id` line (`law_roundtrip_seq_identity`), which runs the full
    // boundary-biased proptest.
    world.a = Some(ActorId::new(123_456_789));
}

#[then(regex = r"^the decoded sequence_id equals the original sequence_id$")]
async fn law_roundtrip_seq_identity(_world: &mut ActorIdWorld) {
    for v in [0u64, 1, u64::MAX - 1, u64::MAX] {
        let id = ActorId::new(v);
        assert_eq!(
            ActorId::from_bytes(&id.to_bytes()).unwrap().sequence_id(),
            v
        );
    }
    proptest!(|(v in any::<u64>())| {
        let id = ActorId::new(v);
        prop_assert_eq!(ActorId::from_bytes(&id.to_bytes()).unwrap().sequence_id(), v);
    });
}

// The property round-trip law shares the example feature's
// "the decoded ActorId equals the original" Then phrasing. The example runner
// reaches `then_decoded_equals_original` (asserting `world.a`); for the
// property feature there is no `world.a`, so this step ALSO exercises the law
// over the boundary-biased generator. To keep one step per phrase, the
// equality Then is the example one; the universal law is carried by the
// dedicated `# GEN`-driven steps below (seq identity + accept/recover), which
// have distinct phrasings. The property "decoded ActorId equals the original"
// And-line is `the decoded sequence_id equals the original sequence_id` above,
// which runs the full proptest. (See the feature: the @property lifecycle
// scenario asserts BOTH lines; the second is the law driver.)

#[given(regex = r"^any byte slice of length n with n in \[0, 7\]$")]
async fn given_short_slice(world: &mut ActorIdWorld) {
    // A representative too-short slice so the shared `from_bytes is called on
    // it` When runs; the exhaustive n ∈ {0, 1, 7} + proptest sweep lives in the
    // Then below (this file's law-driver-in-the-Then convention).
    world.bytes = vec![0u8; 3];
}

#[then(regex = r"^it returns Err\(ActorIdFromBytesError::MissingSequenceID\)$")]
async fn law_reject_short_slice(_world: &mut ActorIdWorld) {
    // GEN: n ∈ {0, 1, 7} — empty, the smallest non-empty, and the largest
    // too-short length — plus a uniform proptest over every length in [0, 8).
    // ORACLE: any slice shorter than 8 bytes -> Err(MissingSequenceID), never a
    // panic (id.rs bounds-checks before slicing `bytes[0..8]`).
    for n in [0usize, 1, 7] {
        let bytes = vec![0u8; n];
        assert!(
            matches!(
                ActorId::from_bytes(&bytes),
                Err(ActorIdFromBytesError::MissingSequenceID)
            ),
            "a {n}-byte slice must decode to Err(MissingSequenceID), not panic"
        );
    }
    proptest!(|(bytes in prop::collection::vec(any::<u8>(), 0..8))| {
        prop_assert!(matches!(
            ActorId::from_bytes(&bytes),
            Err(ActorIdFromBytesError::MissingSequenceID)
        ));
    });
}

#[given(regex = r"^any u64 value v encoded as its 8 little-endian bytes$")]
async fn given_any_u64_le(_world: &mut ActorIdWorld) {}

#[when(regex = r"^from_bytes is called on exactly those 8 bytes$")]
async fn when_from_bytes_on_8(_world: &mut ActorIdWorld) {}

#[then(regex = r"^it returns Ok\(ActorId\) whose sequence_id equals v$")]
async fn law_accept_recover_8_bytes(_world: &mut ActorIdWorld) {
    for v in [0u64, 1, u64::MAX - 1, u64::MAX] {
        let bytes = v.to_le_bytes();
        let id = ActorId::from_bytes(&bytes).expect("exactly 8 bytes must decode");
        assert_eq!(id.sequence_id(), v, "from_bytes must recover the LE value");
    }
    proptest!(|(v in any::<u64>())| {
        let id = ActorId::from_bytes(&v.to_le_bytes()).unwrap();
        prop_assert_eq!(id.sequence_id(), v);
    });
}

#[given(regex = r"^any count n of generate\(\) calls made back-to-back on a single thread$")]
async fn given_any_count_generate(_world: &mut ActorIdWorld) {}

#[when(regex = r"^their sequence ids are read in call order$")]
async fn when_read_in_call_order(_world: &mut ActorIdWorld) {}

#[then(regex = r"^each id is exactly one greater than the id of the previous call$")]
async fn law_sequential_plus_one(_world: &mut ActorIdWorld) {
    // n ∈ {1, 2, 1000}: the single-call, adjacent-pair and large-batch
    // boundaries. Single thread => no interleaving; the counter is global so we
    // assert the DELTA (each id == previous + 1), valid from any start point.
    for n in [1usize, 2, 1000] {
        let ids: Vec<u64> = (0..n).map(|_| ActorId::generate().sequence_id()).collect();
        for w in ids.windows(2) {
            assert_eq!(
                w[1],
                w[0] + 1,
                "each generate() id must be exactly one greater than the previous, got {ids:?}"
            );
        }
    }
}

#[then(regex = r"^all n ids are pairwise distinct$")]
async fn law_sequential_distinct(_world: &mut ActorIdWorld) {
    for n in [1usize, 2, 1000] {
        let ids: Vec<u64> = (0..n).map(|_| ActorId::generate().sequence_id()).collect();
        let set: HashSet<u64> = ids.iter().copied().collect();
        assert_eq!(
            set.len(),
            n,
            "all {n} generated ids must be pairwise distinct"
        );
    }
}

#[given(regex = r"^any pair of ActorIds a, b built from any two sequence_ids$")]
async fn given_any_pair(_world: &mut ActorIdWorld) {}

#[when(regex = r"^they are compared and hashed$")]
async fn when_compared_and_hashed(_world: &mut ActorIdWorld) {}

#[then(regex = r"^a == b iff their sequence_ids are equal$")]
async fn law_eq_iff_seq(_world: &mut ActorIdWorld) {
    let boundary = [0u64, 1, u64::MAX - 1, u64::MAX];
    for &x in &boundary {
        for &y in &boundary {
            let a = ActorId::new(x);
            let b = ActorId::new(y);
            assert_eq!(
                a == b,
                x == y,
                "ActorId eq must agree with integer eq for ({x}, {y})"
            );
        }
    }
    proptest!(|(x in any::<u64>(), y in any::<u64>())| {
        prop_assert_eq!(ActorId::new(x) == ActorId::new(y), x == y);
    });
}

#[then(regex = r"^a == b implies hash\(a\) == hash\(b\)$")]
async fn law_eq_implies_hash(_world: &mut ActorIdWorld) {
    for v in [0u64, 1, u64::MAX - 1, u64::MAX] {
        let a = ActorId::new(v);
        let b = ActorId::new(v);
        assert_eq!(a, b);
        assert_eq!(hash_of(&a), hash_of(&b), "equal ActorIds must hash equally");
    }
    proptest!(|(v in any::<u64>())| {
        prop_assert_eq!(hash_of(&ActorId::new(v)), hash_of(&ActorId::new(v)));
    });
}

#[then(regex = r"^the Ord of a, b equals the Ord of their sequence_ids$")]
async fn law_ord_agrees(_world: &mut ActorIdWorld) {
    let boundary = [0u64, 1, u64::MAX - 1, u64::MAX];
    for &x in &boundary {
        for &y in &boundary {
            assert_eq!(
                ActorId::new(x).cmp(&ActorId::new(y)),
                x.cmp(&y),
                "ActorId Ord must agree with integer Ord for ({x}, {y})"
            );
        }
    }
    proptest!(|(x in any::<u64>(), y in any::<u64>())| {
        prop_assert_eq!(ActorId::new(x).cmp(&ActorId::new(y)), x.cmp(&y));
    });
}

// ---------------------------------------------------------------------------
// @model concurrent-contiguity law. Async + a process-global counter, so it is
// a DOCUMENTED DETERMINISTIC LOOP over (P, k) boundary pairs (NOT inside
// proptest! — proptest cannot drive tokio tasks, and the counter is global so
// repeated proptest cases would not reset it). Real overlap via tokio::spawn +
// Arc<Barrier>. The oracle is integer contiguity of the OBSERVED range
// [N0, N0+k): N0 is the observed minimum (the private counter cannot be read),
// and the assigned set must equal that contiguous range with no gaps/repeats.
// ---------------------------------------------------------------------------

#[given(regex = r"^the counter value before spawning is recorded as N0$")]
async fn given_counter_recorded_as_n0(_world: &mut ActorIdWorld) {}

#[given(
    regex = r"^any number P of tasks each calling generate\(\) any number of times, k total calls$"
)]
async fn given_p_tasks_k_calls(_world: &mut ActorIdWorld) {}

#[when(regex = r"^all tasks run with real overlap, started at a barrier$")]
async fn when_tasks_run_overlap(_world: &mut ActorIdWorld) {}

#[then(regex = r"^the k assigned sequence ids are pairwise distinct$")]
async fn law_model_distinct(_world: &mut ActorIdWorld) {
    for (p, k) in [(2usize, 1usize), (2, 2), (16, 100), (16, 1000)] {
        let seqs = run_concurrent_generate(p, k).await;
        let set: HashSet<u64> = seqs.iter().copied().collect();
        assert_eq!(
            set.len(),
            k,
            "P={p} k={k}: the {k} assigned ids must be pairwise distinct"
        );
    }
}

#[then(regex = r"^the set of assigned ids equals exactly the integers N0 through N0\+k-1$")]
async fn law_model_contiguous(_world: &mut ActorIdWorld) {
    for (p, k) in [(2usize, 1usize), (2, 2), (16, 100), (16, 1000)] {
        let seqs = run_concurrent_generate(p, k).await;
        let set: HashSet<u64> = seqs.iter().copied().collect();
        assert_eq!(set.len(), k, "P={p} k={k}: ids must be distinct");
        let n0 = *set.iter().min().expect("non-empty set");
        // Oracle: integer contiguity only — never calls the SUT.
        let expected: HashSet<u64> = (n0..n0 + k as u64).collect();
        assert_eq!(
            set, expected,
            "P={p} k={k}: assigned ids must equal the contiguous range [N0, N0+k)"
        );
    }
}

/// Runs `k` total `generate()` calls spread across `p` tasks that all release
/// from one shared `Barrier`, so the calls genuinely overlap. Returns every
/// assigned sequence id. Each task's call count is chosen so the totals sum to
/// exactly `k` (the remainder is folded into the last task).
async fn run_concurrent_generate(p: usize, k: usize) -> Vec<u64> {
    let barrier = Arc::new(Barrier::new(p));
    let base = k / p;
    let rem = k % p;
    let handles: Vec<_> = (0..p)
        .map(|i| {
            let barrier = Arc::clone(&barrier);
            // Distribute the remainder so the per-task counts sum to exactly k.
            let calls = base + usize::from(i < rem);
            tokio::spawn(async move {
                barrier.wait().await;
                (0..calls)
                    .map(|_| ActorId::generate().sequence_id())
                    .collect::<Vec<_>>()
            })
        })
        .collect();
    let mut seqs = Vec::with_capacity(k);
    for h in handles {
        seqs.extend(h.await.expect("generator task must not panic"));
    }
    seqs
}
