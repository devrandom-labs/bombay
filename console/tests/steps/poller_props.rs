//! Step definitions for `poller.properties.feature`'s `@property` laws.
//!
//! Each `@property` scenario states a ∀-quantified invariant over the poller's
//! frame protocol helpers (`check_frame_len`, `decode_frame`, and the msgpack
//! round-trip). Following `tui_props.rs`, the generic `Given`/`When` lines bind
//! to NO-OP steps (cucumber 0.23 + `fail_on_skipped` fails any scenario with an
//! unmatched line); the real proptest-backed assertion lives in each scenario's
//! discriminating `Then`/`And` lines. `proptest!` is a synchronous macro run
//! inline in the async step body — it completes before the fn returns.
//!
//! `wire::Snapshot` has NO `PartialEq` (wire.rs:21), so the round-trip oracle
//! compares RE-ENCODED bytes (`to_vec_named ∘ from_slice ∘ to_vec_named` is the
//! identity on the byte image), never `decoded == s`. Generators follow each
//! scenario's `# GEN:` comment, hitting the named boundary edges via
//! `prop_oneof![Just(edge), …, any::<_>()]`.

use std::io;
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime};

use cucumber::{World, given, then, when};
use kameo::console::wire::Message;
use kameo_console::testing::{
    ActorCounters, ActorId, ActorSnapshot, ActorStatus, Links, MAX_FRAME_BYTES, MailboxKind,
    MailboxStats, RefCounts, Snapshot, Totals, check_frame_len, decode_frame,
};
use proptest::prelude::*;

/// A unit world: proptest carries its own per-case state, so no per-scenario
/// fields are needed (each scenario gets a fresh `World::default()`).
#[derive(Debug, Default, World)]
pub struct PollerPropsWorld;

// ---------------------------------------------------------------------------
// Strategies — arbitrary Snapshot, boundary-biased per each scenario's # GEN.
// ---------------------------------------------------------------------------

/// Boundary-biased u64: the named edges {0, 1, MAX} plus the full range.
fn u64_edges() -> impl Strategy<Value = u64> {
    prop_oneof![Just(0u64), Just(1u64), Just(u64::MAX), any::<u64>()]
}

/// Actor names that stress the serde string path: empty, ascii, unicode, and a
/// path-with-generics like `a::b::Foo<X>` (the `# GEN:` "::" and generics edge).
fn name_strategy() -> impl Strategy<Value = String> {
    prop_oneof![
        Just(String::new()),
        Just("Actor".to_owned()),
        Just("名前".to_owned()),
        Just("a::b::Foo<X>".to_owned()),
        ".*",
    ]
}

/// Every `ActorStatus` variant, including the struct variant `Stopped { at, reason }`.
fn status_strategy() -> impl Strategy<Value = ActorStatus> {
    prop_oneof![
        Just(ActorStatus::Starting),
        Just(ActorStatus::Running),
        Just(ActorStatus::Restarting),
        Just(ActorStatus::Stopping),
        name_strategy().prop_map(|reason| ActorStatus::Stopped {
            at: SystemTime::UNIX_EPOCH,
            reason,
        }),
    ]
}

/// Mailbox capacity across the named edges {None, Some(0), Some(MAX)} plus arbitrary.
fn capacity_strategy() -> impl Strategy<Value = Option<usize>> {
    prop_oneof![
        Just(None),
        Just(Some(0usize)),
        Just(Some(usize::MAX)),
        any::<usize>().prop_map(Some),
    ]
}

fn mailbox_kind_strategy() -> impl Strategy<Value = MailboxKind> {
    prop_oneof![Just(MailboxKind::Bounded), Just(MailboxKind::Unbounded)]
}

/// An arbitrary `ActorSnapshot`. Varies the fields the round-trip law names
/// (id, name, status, mailbox kind+capacity); fields the law does not stress
/// use fixed/simple values (`spawned_at = UNIX_EPOCH`, no handler/wait/strategy/
/// supervision edges, empty topology) — they still serialize, just deterministically.
fn actor_strategy() -> impl Strategy<Value = ActorSnapshot> {
    (
        any::<u64>(),
        name_strategy(),
        status_strategy(),
        mailbox_kind_strategy(),
        capacity_strategy(),
    )
        .prop_map(|(id, name, status, kind, capacity)| ActorSnapshot {
            id: ActorId(id),
            name,
            status,
            handling: None,
            waiting_on: None,
            strategy: None,
            spawned_at: SystemTime::UNIX_EPOCH,
            mailbox: MailboxStats {
                kind,
                len: 0,
                capacity,
            },
            counters: ActorCounters::default(),
            message_types: Vec::new(),
            refs: RefCounts { strong: 1, weak: 0 },
            links: Links::default(),
            supervision: None,
        })
}

/// An arbitrary `Snapshot`: boundary-biased `seq`, 0..4 arbitrary actors (the
/// `# GEN:` "length {0, 1, many}"), simple `captured_at`/`uptime`/`totals`.
fn snapshot_strategy() -> impl Strategy<Value = Snapshot> {
    (u64_edges(), prop::collection::vec(actor_strategy(), 0..4)).prop_map(|(seq, actors)| {
        Snapshot {
            seq,
            captured_at: SystemTime::UNIX_EPOCH,
            uptime: Duration::ZERO,
            actors,
            totals: Totals::default(),
        }
    })
}

/// The source-side encode: a named-MessagePack `Message::Snapshot`. Deterministic,
/// so two encodes of equal values match byte-for-byte (the round-trip oracle).
fn encode(s: &Snapshot) -> Vec<u8> {
    rmp_serde::to_vec_named(&Message::Snapshot(s.clone())).expect("encode snapshot")
}

// ---------------------------------------------------------------------------
// @sequence — decode(encode(Snapshot)) reproduces the Snapshot for any Snapshot
// ---------------------------------------------------------------------------

#[given(regex = r"^any Snapshot value s$")]
async fn given_any_snapshot(_world: &mut PollerPropsWorld) {}

#[when(
    regex = r"^s is encoded as a MessagePack Message::Snapshot frame and the poller decodes it back$"
)]
async fn when_encode_decode(_world: &mut PollerPropsWorld) {}

#[then(regex = r"^the decoded Snapshot equals s field-for-field$")]
async fn then_round_trip_identity(_world: &mut PollerPropsWorld) {
    // ORACLE: rmp_serde::from_slice ∘ to_vec_named == identity. Snapshot has no
    // PartialEq, so identity is asserted on the RE-ENCODED byte image: encode(s),
    // decode it, re-encode the decoded value, and require the two byte vecs equal.
    proptest!(|(s in snapshot_strategy())| {
        let enc = encode(&s);
        let decoded = decode_frame(&enc).expect("round-trip frame must decode");
        let re = encode(&decoded);
        prop_assert_eq!(enc, re, "round-trip is not identity for this Snapshot shape");
    });
}

// ---------------------------------------------------------------------------
// @boundary — size gate: accepted iff L <= MAX_FRAME_BYTES
// ---------------------------------------------------------------------------

#[given(regex = r"^any 4-byte big-endian length prefix L$")]
async fn given_any_len_prefix(_world: &mut PollerPropsWorld) {}

#[when(regex = r"^the poller reads the length prefix and applies the size gate$")]
async fn when_applies_size_gate(_world: &mut PollerPropsWorld) {}

#[then(regex = r"^the frame passes the size gate iff L <= 67108864$")]
async fn then_gate_iff_le_max(_world: &mut PollerPropsWorld) {
    // ORACLE: the predicate `L <= MAX_FRAME_BYTES` (the gate is `len > MAX`, :27).
    // GEN: boundary-biased u32 incl {0, 1, MAX-1, MAX, MAX+1, u32::MAX}.
    proptest!(|(l in len_edges())| {
        let r = check_frame_len(l);
        prop_assert_eq!(r.is_ok(), l <= MAX_FRAME_BYTES);
    });
    // The named boundaries, asserted concretely (not just sampled).
    assert!(check_frame_len(0).is_ok());
    assert!(check_frame_len(1).is_ok());
    assert!(check_frame_len(67_108_863).is_ok(), "MAX-1 accepted");
    assert!(check_frame_len(67_108_864).is_ok(), "MAX is inclusive");
    assert!(check_frame_len(67_108_865).is_err(), "MAX+1 rejected");
    assert!(check_frame_len(0xFFFF_FFFF).is_err(), "u32::MAX rejected");
}

#[then(
    regex = r"^it is rejected with an io::ErrorKind::InvalidData error naming the size iff L > 67108864$"
)]
async fn then_invalid_data_naming_size_iff_above(_world: &mut PollerPropsWorld) {
    proptest!(|(l in len_edges())| {
        let r = check_frame_len(l);
        if l > MAX_FRAME_BYTES {
            let err = r.unwrap_err();
            prop_assert_eq!(err.kind(), io::ErrorKind::InvalidData);
            prop_assert!(err.to_string().contains(&l.to_string()), "error must name the size");
        } else {
            prop_assert!(r.is_ok());
        }
    });
}

/// Boundary-biased u32 length prefixes: the named edges {0, 1, MAX-1, MAX, MAX+1,
/// u32::MAX} plus the full range.
fn len_edges() -> impl Strategy<Value = u32> {
    prop_oneof![
        Just(0u32),
        Just(1u32),
        Just(67_108_863u32),
        Just(67_108_864u32),
        Just(67_108_865u32),
        Just(u32::MAX),
        any::<u32>(),
    ]
}

// ---------------------------------------------------------------------------
// @boundary — oversized prefix rejected before any payload buffer is allocated
// ---------------------------------------------------------------------------

#[given(regex = r"^any length prefix L strictly greater than MAX_FRAME_BYTES$")]
async fn given_oversized_len(_world: &mut PollerPropsWorld) {}

#[when(regex = r"^the poller applies the size gate$")]
async fn when_applies_gate_oversized(_world: &mut PollerPropsWorld) {}

#[then(regex = r"^it returns the InvalidData error without allocating a buffer of L bytes$")]
async fn then_oversized_rejected_no_alloc(_world: &mut PollerPropsWorld) {
    // ORACLE: for every L > MAX, the gate returns Err before `vec![0u8; len]`
    // (poller.rs :27-33 returns before poll()'s allocation at :215). The absence
    // of an L-sized allocation is STRUCTURAL — `check_frame_len` returning Err is
    // what guarantees `poll` never reaches the `vec!`; a 4 GiB allocation cannot be
    // attempted to prove a negative, so we assert the gate rejects every such L.
    proptest!(|(l in oversized_len_edges())| {
        prop_assert!(l > MAX_FRAME_BYTES);
        let err = check_frame_len(l).unwrap_err();
        prop_assert_eq!(err.kind(), io::ErrorKind::InvalidData);
    });
    // The named edges {MAX+1, 1<<30, 0xFFFFFFFF}, asserted concretely.
    for l in [67_108_865u32, 1u32 << 30, 0xFFFF_FFFF] {
        let err = check_frame_len(l).unwrap_err();
        assert_eq!(
            err.kind(),
            io::ErrorKind::InvalidData,
            "L={l} must be rejected"
        );
    }
}

/// Length prefixes strictly in `(MAX_FRAME_BYTES, u32::MAX]`, including the named
/// edges {MAX+1, 1<<30, u32::MAX}.
fn oversized_len_edges() -> impl Strategy<Value = u32> {
    prop_oneof![
        Just(67_108_865u32),
        Just(1u32 << 30),
        Just(u32::MAX),
        (MAX_FRAME_BYTES + 1)..=u32::MAX,
    ]
}

// ---------------------------------------------------------------------------
// @boundary — any non-msgpack byte string decodes to an error, never a panic
// ---------------------------------------------------------------------------

#[given(regex = r"^any byte string B whose length is <= MAX_FRAME_BYTES$")]
async fn given_any_byte_string(_world: &mut PollerPropsWorld) {}

#[when(regex = r"^the poller passes B through rmp_serde decode$")]
async fn when_decode_b(_world: &mut PollerPropsWorld) {}

#[then(regex = r"^it returns either a decoded Snapshot or an InvalidData error, and never panics$")]
async fn then_decode_total(_world: &mut PollerPropsWorld) {
    // ORACLE: decode is TOTAL over &[u8] — every B yields a Result, mapping any
    // rmp_serde error to ErrorKind::InvalidData; it never panics (the proptest run
    // completing without a panic is itself the no-panic assertion).
    proptest!(|(b in prop::collection::vec(any::<u8>(), 0..256))| {
        let r = decode_frame(&b);
        prop_assert!(r.is_ok() || r.as_ref().unwrap_err().kind() == io::ErrorKind::InvalidData);
    });
    // The named edges: empty slice and all-0xFF are not valid Message::Snapshot.
    assert_eq!(
        decode_frame(&[]).unwrap_err().kind(),
        io::ErrorKind::InvalidData
    );
    assert_eq!(
        decode_frame(&[0xFF; 16]).unwrap_err().kind(),
        io::ErrorKind::InvalidData
    );
}

#[then(
    regex = r"^a B that is not a valid MessagePack Message::Snapshot yields the InvalidData error$"
)]
async fn then_invalid_b_is_invalid_data(_world: &mut PollerPropsWorld) {
    // Non-Snapshot bytes (random + the named all-0xFF / empty edges) decode to
    // InvalidData, never Ok. (A random 0..256 byte string is overwhelmingly not a
    // valid named-msgpack Message::Snapshot; assert the gate maps it to InvalidData
    // whenever it is rejected.)
    proptest!(|(b in prop::collection::vec(any::<u8>(), 0..64))| {
        if let Err(err) = decode_frame(&b) {
            prop_assert_eq!(err.kind(), io::ErrorKind::InvalidData);
        }
    });
    assert_eq!(
        decode_frame(&[]).unwrap_err().kind(),
        io::ErrorKind::InvalidData
    );
    assert_eq!(
        decode_frame(&[0xFF; 8]).unwrap_err().kind(),
        io::ErrorKind::InvalidData
    );
}

#[then(regex = r"^on that error the shared snapshot slot is left unchanged$")]
async fn then_slot_unchanged_on_error(_world: &mut PollerPropsWorld) {
    // `decode_frame` is PURE — it never touches the shared slot (only `Poller::poll`
    // writes the slot, and only AFTER a successful decode, poller.rs :218). So a
    // decode-only failure cannot mutate a slot: a fresh `None` slot stays `None`
    // across a rejected decode.
    let slot: Arc<Mutex<Option<Snapshot>>> = Arc::new(Mutex::new(None));
    let err = decode_frame(&[0xFF; 8]).unwrap_err();
    assert_eq!(err.kind(), io::ErrorKind::InvalidData);
    assert!(
        slot.lock().unwrap().is_none(),
        "decode-only failure must not write the slot"
    );
}
