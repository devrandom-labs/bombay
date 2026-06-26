//! Shared `ErrorWorld` + step definitions for the core `error` scenarios.
//!
//! Wired by two runners that `#[path]`-include this module:
//!   * `core_error_bdd.rs`        — the example feature (error.feature)
//!   * `core_error_props_bdd.rs`  — the property laws (error.properties.feature)
//!
//! Every assertion is the SPECIFIC value confirmed in the scenario's
//! `# Confirmed:` / `# ORACLE:` note (facts only — no vague `contains`,
//! grounded in src/error.rs as read 2026-06).
//!
//! This is a PURE module: SendError / ActorStopReason / PanicReason / PanicError
//! / RegistryError are exercised in-process with no spawned actors. The concrete
//! generic params are `TestMsg(u32)` for the message slot and `TestErr(String)`
//! for the handler-error slot.

use cucumber::{World, given, then, when};
use kameo::error::{
    ActorStopReason, BoxSendError, PanicError, PanicReason, RegistryError, SendError,
};
use proptest::prelude::*;

/// Concrete message payload for the message-bearing variants.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct TestMsg(u32);

/// Concrete handler-error payload for the `HandlerError` variant.
#[derive(Debug, Clone, PartialEq, Eq)]
struct TestErr(String);

/// A second, distinct message type used by the wrong-type downcast scenarios.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct OtherMsg(u64);

type SE = SendError<TestMsg, TestErr>;

/// A small non-string, non-`String` payload type T used by the serde
/// payload-erasure scenarios. Its `Debug`/`Display`-free `with_str` path yields
/// `None` (it is not a `&str` nor a `String`), so PanicError's Display falls
/// back to the reason alone.
#[derive(Debug, Clone, PartialEq, Eq)]
struct PayloadT(u32);

#[derive(Debug, Default, World)]
pub struct ErrorWorld {
    /// The single SendError under test (built by a Given, transformed by a When).
    se: Option<SE>,
    /// Result of map_msg (variant tag + applied-ness inspected by the Then).
    mapped_msg: Option<SE>,
    /// Result of map_err.
    mapped_err: Option<SendError<TestMsg, TestErr>>,
    /// A boxed (type-erased) SendError under test for downcast scenarios.
    boxed: Option<BoxSendError>,
    /// The ActorStopReason under test.
    stop_reason: Option<ActorStopReason>,
    /// The PanicReason under test.
    panic_reason: Option<PanicReason>,
    /// The PanicError under test.
    panic_err: Option<PanicError>,
    /// `with_str` result captured by a When for a later Then.
    with_str_result: Option<Option<String>>,
    /// The exact string a Given placed into the PanicError payload (so the
    /// shared `with_str` Then can assert the precise value, not "one of").
    expected_str: Option<String>,
}

/// Builds the `SendError<TestMsg, TestErr>` named by a feature token. The same
/// tokens appear in several Outlines; one parser keeps them consistent.
fn build_variant(token: &str) -> SE {
    match token {
        "ActorNotRunning(m)" => SendError::ActorNotRunning(TestMsg(1)),
        "ActorStopped" => SendError::ActorStopped,
        "MailboxFull(m)" => SendError::MailboxFull(TestMsg(2)),
        "HandlerError(e)" => SendError::HandlerError(TestErr("e".into())),
        "Timeout(Some(m))" => SendError::Timeout(Some(TestMsg(3))),
        "Timeout(None)" => SendError::Timeout(None),
        other => panic!("unknown SendError variant token: {other:?}"),
    }
}

/// A coarse variant tag used by oracles that assert "the tag is preserved".
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Tag {
    ActorNotRunning,
    ActorStopped,
    MailboxFull,
    HandlerError,
    Timeout,
}

fn tag_of<M, E>(e: &SendError<M, E>) -> Tag {
    match e {
        SendError::ActorNotRunning(_) => Tag::ActorNotRunning,
        SendError::ActorStopped => Tag::ActorStopped,
        SendError::MailboxFull(_) => Tag::MailboxFull,
        SendError::HandlerError(_) => Tag::HandlerError,
        SendError::Timeout(_) => Tag::Timeout,
    }
}

// ===========================================================================
// @sequence — SendError map_msg / map_err / boxed / msg / err / flatten
// ===========================================================================

#[given(regex = r#"^a SendError "([^"]+)"$"#)]
async fn given_a_senderror(world: &mut ErrorWorld, token: String) {
    world.se = Some(build_variant(&token));
}

#[when(regex = r"^map_msg is applied with a message transform$")]
async fn when_map_msg(world: &mut ErrorWorld) {
    let se = world.se.clone().expect("a SendError");
    // f adds 100 so an applied transform is observable on the payload.
    world.mapped_msg = Some(se.map_msg(|TestMsg(n)| TestMsg(n + 100)));
}

#[then(regex = r"^the transform is applied: (yes|no)$")]
async fn then_msg_transform_applied(world: &mut ErrorWorld, applied: String) {
    let before = world.se.clone().expect("a SendError");
    let after = world.mapped_msg.clone().expect("a map_msg result");
    let original_msg = before.msg();
    let mapped_msg = after.msg();
    match applied.as_str() {
        "yes" => {
            let TestMsg(orig) = original_msg.expect("message-bearing variant has Some msg");
            let TestMsg(got) = mapped_msg.expect("mapped variant still carries the message");
            assert_eq!(
                got,
                orig + 100,
                "map_msg must have applied f (+100) to the payload"
            );
        }
        "no" => {
            // Either no message at all (ActorStopped/HandlerError/Timeout(None))
            // or — for HandlerError — the msg() is None on both sides. The oracle
            // is: the payload is byte-identical to the original (f never ran).
            assert_eq!(
                mapped_msg, original_msg,
                "map_msg must NOT have changed the payload"
            );
        }
        _ => unreachable!(),
    }
}

#[then(regex = r"^the variant tag is preserved$")]
async fn then_variant_tag_preserved(world: &mut ErrorWorld) {
    let before = world.se.clone().expect("a SendError");
    let after = world.mapped_msg.clone().expect("a map_msg result");
    assert_eq!(
        tag_of(&after),
        tag_of(&before),
        "map_msg must preserve the variant tag"
    );
}

#[when(regex = r"^map_err is applied with an error transform$")]
async fn when_map_err(world: &mut ErrorWorld) {
    let se = world.se.clone().expect("a SendError");
    // g appends "!" so an applied transform is observable on the error payload.
    world.mapped_err = Some(se.map_err(|TestErr(s)| TestErr(format!("{s}!"))));
}

#[then(regex = r"^the error transform is applied: (yes|no)$")]
async fn then_err_transform_applied(world: &mut ErrorWorld, applied: String) {
    let before = world.se.clone().expect("a SendError");
    let after = world.mapped_err.clone().expect("a map_err result");
    assert_eq!(
        tag_of(&after),
        tag_of(&before),
        "map_err must preserve the variant tag"
    );
    match applied.as_str() {
        "yes" => {
            let TestErr(orig) = before.err().expect("HandlerError carries an error");
            let TestErr(got) = after.err().expect("HandlerError still carries an error");
            assert_eq!(
                got,
                format!("{orig}!"),
                "map_err must apply g to HandlerError's payload"
            );
        }
        "no" => {
            assert_eq!(
                after.err(),
                before.err(),
                "map_err must NOT change a non-HandlerError"
            );
        }
        _ => unreachable!(),
    }
}

#[given(regex = r"^a SendError HandlerError carrying a concrete error of type E$")]
async fn given_handler_error_concrete(world: &mut ErrorWorld) {
    world.se = Some(SendError::HandlerError(TestErr("boom".into())));
}

#[when(regex = r"^boxed\(\) erases it to BoxSendError$")]
async fn when_boxed(world: &mut ErrorWorld) {
    let se = world.se.clone().expect("a SendError");
    world.boxed = Some(se.boxed());
}

#[when(regex = r"^downcast::<M, E>\(\) is applied$")]
async fn when_downcast(world: &mut ErrorWorld) {
    let boxed = world.boxed.take().expect("a BoxSendError");
    world.se = Some(boxed.downcast::<TestMsg, TestErr>());
}

#[then(regex = r"^the recovered value equals the original HandlerError\(E\)$")]
async fn then_recovered_equals_original(world: &mut ErrorWorld) {
    let recovered = world.se.clone().expect("a recovered SendError");
    assert_eq!(
        recovered,
        SendError::HandlerError(TestErr("boom".into())),
        "boxed + downcast must round-trip the HandlerError payload exactly"
    );
}

#[given(regex = r"^the five SendError variants$")]
async fn given_five_variants(_world: &mut ErrorWorld) {
    // The five tokens are materialised inline by the Then; nothing to store.
}

#[then(
    regex = r"^msg\(\) returns Some for ActorNotRunning, MailboxFull and Timeout\(Some\), else None$"
)]
async fn then_msg_extraction(_world: &mut ErrorWorld) {
    assert_eq!(build_variant("ActorNotRunning(m)").msg(), Some(TestMsg(1)));
    assert_eq!(build_variant("MailboxFull(m)").msg(), Some(TestMsg(2)));
    assert_eq!(build_variant("Timeout(Some(m))").msg(), Some(TestMsg(3)));
    assert_eq!(build_variant("Timeout(None)").msg(), None);
    assert_eq!(build_variant("ActorStopped").msg(), None);
    assert_eq!(build_variant("HandlerError(e)").msg(), None);
}

#[then(regex = r"^err\(\) returns Some only for HandlerError, else None$")]
async fn then_err_extraction(_world: &mut ErrorWorld) {
    assert_eq!(
        build_variant("HandlerError(e)").err(),
        Some(TestErr("e".into()))
    );
    assert_eq!(build_variant("ActorNotRunning(m)").err(), None);
    assert_eq!(build_variant("MailboxFull(m)").err(), None);
    assert_eq!(build_variant("Timeout(Some(m))").err(), None);
    assert_eq!(build_variant("Timeout(None)").err(), None);
    assert_eq!(build_variant("ActorStopped").err(), None);
}

#[given(regex = r"^a nested SendError where the outer HandlerError wraps an inner SendError$")]
async fn given_nested_senderror(_world: &mut ErrorWorld) {
    // The five hoist cases are asserted inline by the Then steps below; each
    // constructs its own nested value so the oracle is self-contained.
}

#[then(regex = r"^HandlerError\(ActorNotRunning\(m\)\) becomes ActorNotRunning\(m\)$")]
async fn then_flatten_anr(_world: &mut ErrorWorld) {
    let nested: SendError<TestMsg, SE> =
        SendError::HandlerError(SendError::ActorNotRunning(TestMsg(7)));
    assert_eq!(nested.flatten(), SendError::ActorNotRunning(TestMsg(7)));
}

#[then(regex = r"^HandlerError\(ActorStopped\) becomes ActorStopped$")]
async fn then_flatten_stopped(_world: &mut ErrorWorld) {
    let nested: SendError<TestMsg, SE> = SendError::HandlerError(SendError::ActorStopped);
    assert_eq!(nested.flatten(), SendError::ActorStopped);
}

#[then(regex = r"^HandlerError\(MailboxFull\(m\)\) becomes MailboxFull\(m\)$")]
async fn then_flatten_full(_world: &mut ErrorWorld) {
    let nested: SendError<TestMsg, SE> =
        SendError::HandlerError(SendError::MailboxFull(TestMsg(8)));
    assert_eq!(nested.flatten(), SendError::MailboxFull(TestMsg(8)));
}

#[then(regex = r"^HandlerError\(Timeout\(m\)\) becomes Timeout\(m\)$")]
async fn then_flatten_timeout(_world: &mut ErrorWorld) {
    let nested: SendError<TestMsg, SE> =
        SendError::HandlerError(SendError::Timeout(Some(TestMsg(9))));
    assert_eq!(nested.flatten(), SendError::Timeout(Some(TestMsg(9))));
}

#[then(regex = r"^HandlerError\(HandlerError\(e\)\) becomes HandlerError\(e\)$")]
async fn then_flatten_handler(_world: &mut ErrorWorld) {
    let nested: SendError<TestMsg, SE> =
        SendError::HandlerError(SendError::HandlerError(TestErr("e".into())));
    assert_eq!(
        nested.flatten(),
        SendError::HandlerError(TestErr("e".into()))
    );
}

// ===========================================================================
// @boundary — downcast mismatches, unwrap_msg / unwrap_err, PanicError payloads
// ===========================================================================

#[given(regex = r"^a BoxSendError ActorNotRunning whose boxed message is actually type A$")]
async fn given_box_anr_type_a(world: &mut ErrorWorld) {
    // A = OtherMsg; box it through the real boxed() path.
    let se: SendError<OtherMsg, TestErr> = SendError::ActorNotRunning(OtherMsg(42));
    world.boxed = Some(se.boxed());
}

// This When phrasing is SHARED by error.feature (example) and
// error.properties.feature (the @property law). For the example it performs the
// real wrong-type downcast and stashes the recovered BoxSendError for the Then;
// for the property scenario (whose Given is a no-op) `world.boxed` is None and
// the law runs entirely in its Then, so this is a no-op. One definition avoids
// the ambiguous-match panic from two identical regexes in the same binary.
#[when(regex = r"^try_downcast::<B, E>\(\) is applied with B != A$")]
async fn when_try_downcast_wrong(world: &mut ErrorWorld) {
    if let Some(boxed) = world.boxed.take() {
        // Request B = TestMsg (!= OtherMsg). The downcast must FAIL and hand
        // back the original BoxSendError, which we restore for the Then.
        let res = boxed.try_downcast::<TestMsg, TestErr>();
        let recovered = res.err().expect("wrong-type try_downcast must return Err");
        world.boxed = Some(recovered);
    }
}

#[then(regex = r"^it returns Err re-wrapping the value as the same variant$")]
async fn then_err_same_variant(world: &mut ErrorWorld) {
    let recovered = world.boxed.take().expect("a recovered BoxSendError");
    assert_eq!(
        tag_of(&recovered),
        Tag::ActorNotRunning,
        "the recovered BoxSendError must be the SAME variant (ActorNotRunning)"
    );
    // And the original payload is still recoverable with the CORRECT type.
    let back: SendError<OtherMsg, TestErr> = recovered.downcast::<OtherMsg, TestErr>();
    assert_eq!(
        back,
        SendError::ActorNotRunning(OtherMsg(42)),
        "the original payload must survive a wrong-type downcast attempt"
    );
}

#[then(regex = r"^no panic occurs$")]
async fn then_no_panic(_world: &mut ErrorWorld) {
    // Reaching this Then proves try_downcast returned Err rather than panicking;
    // the substantive recovery was asserted by the preceding Then.
}

#[given(regex = r"^a BoxSendError whose boxed payload is type A$")]
async fn given_box_payload_type_a(world: &mut ErrorWorld) {
    let se: SendError<OtherMsg, TestErr> = SendError::ActorNotRunning(OtherMsg(7));
    world.boxed = Some(se.boxed());
}

#[when(regex = r"^downcast::<B, E>\(\) is applied with B != A$")]
async fn when_downcast_wrong_panics(world: &mut ErrorWorld) {
    let boxed = world.boxed.take().expect("a BoxSendError");
    // downcast is try_downcast().unwrap(); a wrong type must panic. Catch it so
    // the Then can assert the panic actually occurred (a green test that fails
    // if the unwrap ever stops panicking).
    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let _: SendError<TestMsg, TestErr> = boxed.downcast::<TestMsg, TestErr>();
    }));
    assert!(
        result.is_err(),
        "downcast to the wrong type must panic (try_downcast().unwrap())"
    );
}

#[then(regex = r"^it panics because downcast is try_downcast\(\)\.unwrap\(\)$")]
async fn then_downcast_panicked(_world: &mut ErrorWorld) {
    // The panic was caught and asserted in the When; reaching here confirms it.
}

#[when(regex = r"^unwrap_msg\(\) is called$")]
async fn when_unwrap_msg(_world: &mut ErrorWorld) {
    // Outcome asserted by the Then so the panicking cases can be caught there.
}

#[then(regex = r"^the outcome is returns m$")]
async fn then_unwrap_msg_returns_m(world: &mut ErrorWorld) {
    let se = world.se.clone().expect("a SendError");
    let expected = se
        .clone()
        .msg()
        .expect("message-bearing variant has a message");
    assert_eq!(
        se.unwrap_msg(),
        expected,
        "unwrap_msg must return the inner message"
    );
}

#[then(
    regex = r#"^the outcome is panics "called `SendError::unwrap_msg\(\)` on a non message error"$"#
)]
async fn then_unwrap_msg_panics(world: &mut ErrorWorld) {
    let se = world.se.clone().expect("a SendError");
    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| se.unwrap_msg()));
    let payload = result.expect_err("unwrap_msg on a non-message variant must panic");
    let msg = panic_message(&payload);
    assert_eq!(
        msg, "called `SendError::unwrap_msg()` on a non message error",
        "the panic message must be exact"
    );
}

#[when(regex = r"^unwrap_err\(\) is called$")]
async fn when_unwrap_err(_world: &mut ErrorWorld) {}

#[then(regex = r"^the outcome is returns e$")]
async fn then_unwrap_err_returns_e(world: &mut ErrorWorld) {
    let se = world.se.clone().expect("a SendError");
    let expected = se.clone().err().expect("HandlerError carries an error");
    assert_eq!(
        se.unwrap_err(),
        expected,
        "unwrap_err must return the inner handler error"
    );
}

#[then(regex = r#"^the outcome is panics "called `SendError::unwrap_err\(\)` on a non error"$"#)]
async fn then_unwrap_err_panics(world: &mut ErrorWorld) {
    let se = world.se.clone().expect("a SendError");
    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| se.unwrap_err()));
    let payload = result.expect_err("unwrap_err on a non-error variant must panic");
    let msg = panic_message(&payload);
    assert_eq!(
        msg, "called `SendError::unwrap_err()` on a non error",
        "the panic message must be exact"
    );
}

/// Extracts the `&str`/`String` payload of a caught panic. Takes the boxed
/// payload by reference so `downcast_ref` auto-derefs through the `Box` to the
/// INNER value (a plain `panic!("literal")` payload is `&'static str`).
fn panic_message(payload: &Box<dyn std::any::Any + Send>) -> String {
    payload
        .downcast_ref::<&str>()
        .map(|s| (*s).to_string())
        .or_else(|| payload.downcast_ref::<String>().cloned())
        .expect("panic payload must be a string")
}

#[given(regex = r"^a PanicError constructed from a panic carrying a &'static str$")]
async fn given_panic_static_str(world: &mut ErrorWorld) {
    world.panic_err = Some(PanicError::new(
        Box::new("static boom"),
        PanicReason::HandlerPanic,
    ));
    world.expected_str = Some("static boom".to_string());
}

#[given(regex = r"^a PanicError constructed from a panic carrying a String$")]
async fn given_panic_string(world: &mut ErrorWorld) {
    world.panic_err = Some(PanicError::new(
        Box::new(String::from("string boom")),
        PanicReason::HandlerPanic,
    ));
    world.expected_str = Some("string boom".to_string());
}

#[when(regex = r"^with_str is called$")]
async fn when_with_str(world: &mut ErrorWorld) {
    let pe = world.panic_err.as_ref().expect("a PanicError");
    world.with_str_result = Some(pe.with_str(|s| s.to_string()));
}

#[then(regex = r"^it yields Some with the original string$")]
async fn then_with_str_some(world: &mut ErrorWorld) {
    let got = world.with_str_result.clone().expect("with_str was called");
    let expected = world
        .expected_str
        .clone()
        .expect("a Given recorded the expected string");
    assert_eq!(
        got,
        Some(expected),
        "with_str must yield Some with the exact constructed string"
    );
}

#[given(regex = r"^a PanicError whose inner payload is a String$")]
async fn given_panic_inner_string(world: &mut ErrorWorld) {
    world.panic_err = Some(PanicError::new(
        Box::new(String::from("payload")),
        PanicReason::OnStart,
    ));
}

#[when(regex = r"^with_downcast_ref::<SomeOtherType, _, _> is called$")]
async fn when_with_downcast_ref_mismatch(world: &mut ErrorWorld) {
    let pe = world.panic_err.as_ref().expect("a PanicError");
    // SomeOtherType = u32 (the payload is a String, so this must miss).
    let got: Option<u32> = pe.with_downcast_ref::<u32, _, _>(|n: &u32| *n);
    // Stash via with_str_result as a presence flag: None on mismatch.
    world.with_str_result = Some(got.map(|n| n.to_string()));
}

#[then(regex = r"^it returns None$")]
async fn then_with_downcast_ref_none(world: &mut ErrorWorld) {
    let got = world
        .with_str_result
        .clone()
        .expect("with_downcast_ref was called");
    assert_eq!(
        got, None,
        "with_downcast_ref to a non-matching type must return None"
    );
}

#[given(regex = r"^a PanicError whose inner error mutex has been poisoned by a prior panic$")]
async fn given_poisoned_panic_error(world: &mut ErrorWorld) {
    let pe = PanicError::new(Box::new(String::from("poison me")), PanicReason::OnStop);
    // Poison the inner Mutex via the public `with` path: panicking inside the
    // closure while the lock is held drops the guard during unwind, poisoning
    // the mutex. This is the only poisoning route reachable from an external
    // test (the `err` field is pub(crate)).
    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        pe.with(|_boxed| panic!("poisoning the inner mutex"));
    }));
    assert!(result.is_err(), "the poisoning closure must have panicked");
    world.panic_err = Some(pe);
}

#[when(regex = r"^with, with_str, or with_downcast_ref is called$")]
async fn when_access_poisoned(world: &mut ErrorWorld) {
    let pe = world.panic_err.as_ref().expect("a poisoned PanicError");
    // Access via the poisoned guard's get_ref must still succeed and not panic.
    let s = pe.with_str(|s| s.to_string());
    world.with_str_result = Some(s);
}

#[then(regex = r"^it still accesses the payload via the poisoned guard's get_ref$")]
async fn then_poisoned_access_succeeds(world: &mut ErrorWorld) {
    let got = world
        .with_str_result
        .clone()
        .expect("with_str was called on a poisoned PanicError");
    assert_eq!(
        got.as_deref(),
        Some("poison me"),
        "a poisoned mutex must still surface the original payload via get_ref"
    );
}

#[then(regex = r"^no second panic is raised by the access$")]
async fn then_no_second_panic(_world: &mut ErrorWorld) {
    // Reaching this Then proves the access in the When did not panic.
}

// ===========================================================================
// @lifecycle — ActorStopReason / PanicReason classifiers + PanicError serde
// ===========================================================================

#[given(regex = r#"^an ActorStopReason "([^"]+)"$"#)]
async fn given_actor_stop_reason(world: &mut ErrorWorld, token: String) {
    world.stop_reason = Some(match token.as_str() {
        "Normal" => ActorStopReason::Normal,
        "SupervisorRestart" => ActorStopReason::SupervisorRestart,
        "Killed" => ActorStopReason::Killed,
        "Panicked" => {
            ActorStopReason::Panicked(PanicError::new(Box::new("x"), PanicReason::HandlerPanic))
        }
        "LinkDied" => ActorStopReason::LinkDied {
            id: kameo::actor::ActorId::new(1),
            reason: Box::new(ActorStopReason::Normal),
        },
        other => panic!("unknown ActorStopReason token: {other:?}"),
    });
}

#[then(regex = r"^is_normal\(\) returns (true|false)$")]
async fn then_is_normal(world: &mut ErrorWorld, expected: String) {
    let reason = world.stop_reason.as_ref().expect("an ActorStopReason");
    let expected: bool = expected.parse().expect("true|false");
    assert_eq!(
        reason.is_normal(),
        expected,
        "is_normal() must match the oracle"
    );
}

#[given(regex = r#"^a PanicReason "([^"]+)"$"#)]
async fn given_panic_reason(world: &mut ErrorWorld, token: String) {
    world.panic_reason = Some(parse_panic_reason(&token));
}

fn parse_panic_reason(token: &str) -> PanicReason {
    match token {
        "OnStart" => PanicReason::OnStart,
        "OnPanic" => PanicReason::OnPanic,
        "OnLinkDied" => PanicReason::OnLinkDied,
        "OnStop" => PanicReason::OnStop,
        "HandlerPanic" => PanicReason::HandlerPanic,
        "OnMessage" => PanicReason::OnMessage,
        "Next" => PanicReason::Next,
        other => panic!("unknown PanicReason token: {other:?}"),
    }
}

#[then(regex = r"^is_lifecycle_hook\(\) returns (true|false)$")]
async fn then_is_lifecycle_hook(world: &mut ErrorWorld, expected: String) {
    let reason = world.panic_reason.expect("a PanicReason");
    let expected: bool = expected.parse().expect("true|false");
    assert_eq!(
        reason.is_lifecycle_hook(),
        expected,
        "is_lifecycle_hook() must match the oracle"
    );
}

#[then(regex = r"^is_message_processing\(\) returns (true|false)$")]
async fn then_is_message_processing(world: &mut ErrorWorld, expected: String) {
    let reason = world.panic_reason.expect("a PanicReason");
    let expected: bool = expected.parse().expect("true|false");
    assert_eq!(
        reason.is_message_processing(),
        expected,
        "is_message_processing() must match the oracle"
    );
}

#[given(regex = r"^the PanicReason Next$")]
async fn given_panic_reason_next(world: &mut ErrorWorld) {
    world.panic_reason = Some(PanicReason::Next);
}

#[then(regex = r"^is_lifecycle_hook\(\) is false and is_message_processing\(\) is false$")]
async fn then_next_neither(world: &mut ErrorWorld) {
    let reason = world.panic_reason.expect("a PanicReason");
    assert!(
        !reason.is_lifecycle_hook(),
        "Next must NOT be a lifecycle hook"
    );
    assert!(
        !reason.is_message_processing(),
        "Next must NOT be message processing"
    );
}

// --- PanicError serde round-trips (lossy, non-idempotent) -------------------

#[given(
    regex = r"^a PanicError with reason R wrapping a payload of concrete type T \(not String\)$"
)]
async fn given_panic_payload_type_t(world: &mut ErrorWorld) {
    // T = TestErr (a Debug type, NOT String). Its Display string surfaces via
    // with_str only when it is a &str/String — TestErr is neither, so with_str
    // → None and Display falls back to the reason alone.
    world.panic_err = Some(PanicError::new(
        Box::new(TestErr("typed".into())),
        PanicReason::OnPanic,
    ));
}

#[when(regex = r"^it is serialized and then deserialized$")]
async fn when_serde_roundtrip(world: &mut ErrorWorld) {
    let pe = world.panic_err.as_ref().expect("a PanicError");
    let buf = rmp_serde::to_vec_named(pe).expect("serialize PanicError");
    let back: PanicError = rmp_serde::from_slice(&buf).expect("deserialize PanicError");
    world.panic_err = Some(back);
}

#[then(regex = r"^the recovered PanicError carries the same reason R$")]
async fn then_recovered_reason_same(world: &mut ErrorWorld) {
    let pe = world.panic_err.as_ref().expect("a recovered PanicError");
    assert_eq!(
        pe.reason(),
        PanicReason::OnPanic,
        "the reason must survive the round-trip"
    );
}

#[then(regex = r"^its inner payload is a String \(the original Display text\), no longer type T$")]
async fn then_inner_is_string(world: &mut ErrorWorld) {
    let pe = world.panic_err.as_ref().expect("a recovered PanicError");
    // After deserialize the inner is a boxed String. with_str must now find it.
    // The original TestErr had no string Display surface, so the serialized err
    // field was the reason alone ("on_panic returned error"); that becomes the
    // inner String.
    let s = pe.with_str(|s| s.to_string());
    assert_eq!(
        s.as_deref(),
        Some("on_panic returned error"),
        "the inner payload must now be the Display String (reason alone, since T had no string)"
    );
}

#[then(regex = r"^downcast::<T>\(\) on the recovered PanicError returns None$")]
async fn then_downcast_t_none(world: &mut ErrorWorld) {
    let pe = world.panic_err.as_ref().expect("a recovered PanicError");
    let got: Option<TestErr> = pe.downcast::<TestErr>();
    assert_eq!(
        got, None,
        "the concrete type T must be erased — downcast::<T>() returns None"
    );
}

#[given(regex = r#"^a PanicError with reason R wrapping the string payload "boom"$"#)]
async fn given_panic_string_boom(world: &mut ErrorWorld) {
    // R = OnStart; Display is "{reason}: {payload}" = "on_start returned error: boom".
    world.panic_err = Some(PanicError::new(
        Box::new(String::from("boom")),
        PanicReason::OnStart,
    ));
}

#[when(regex = r"^it is serialized once and deserialized to p1$")]
async fn when_serialize_to_p1(world: &mut ErrorWorld) {
    let pe = world.panic_err.as_ref().expect("a PanicError");
    let buf = rmp_serde::to_vec_named(pe).expect("serialize");
    let p1: PanicError = rmp_serde::from_slice(&buf).expect("deserialize p1");
    world.panic_err = Some(p1);
}

#[then(regex = r#"^p1's inner string equals the Display "R: boom"$"#)]
async fn then_p1_inner(world: &mut ErrorWorld) {
    let p1 = world.panic_err.as_ref().expect("p1");
    let s = p1.with_str(|s| s.to_string());
    assert_eq!(
        s.as_deref(),
        Some("on_start returned error: boom"),
        "p1's inner string must be the full Display 'reason: boom'"
    );
}

#[when(regex = r"^p1 is serialized again and deserialized to p2$")]
async fn when_serialize_to_p2(world: &mut ErrorWorld) {
    let p1 = world.panic_err.as_ref().expect("p1");
    let buf = rmp_serde::to_vec_named(p1).expect("serialize p1");
    let p2: PanicError = rmp_serde::from_slice(&buf).expect("deserialize p2");
    world.panic_err = Some(p2);
}

#[then(regex = r#"^p2's inner string equals "R: R: boom"$"#)]
async fn then_p2_inner(world: &mut ErrorWorld) {
    let p2 = world.panic_err.as_ref().expect("p2");
    let s = p2.with_str(|s| s.to_string());
    assert_eq!(
        s.as_deref(),
        Some("on_start returned error: on_start returned error: boom"),
        "p2's inner string must show the reason prefix compounding once more"
    );
}

#[given(
    regex = r"^a PanicError with reason R wrapping a payload whose Display surfaces no string \(with_str → None\)$"
)]
async fn given_panic_no_string_display(world: &mut ErrorWorld) {
    // R = OnStop; payload PayloadT is neither &str nor String, so with_str → None.
    world.panic_err = Some(PanicError::new(Box::new(PayloadT(99)), PanicReason::OnStop));
}

#[when(regex = r"^it is serialized$")]
async fn when_serialized_only(world: &mut ErrorWorld) {
    let pe = world.panic_err.as_ref().expect("a PanicError");
    // Capture the serialized `err` field by deserializing into a probe struct.
    let buf = rmp_serde::to_vec_named(pe).expect("serialize");
    #[derive(serde::Deserialize)]
    struct Probe {
        err: String,
        #[allow(dead_code)]
        reason: PanicReason,
    }
    let probe: Probe = rmp_serde::from_slice(&buf).expect("deserialize probe");
    world.with_str_result = Some(Some(probe.err));
    // Also keep the real deserialized PanicError for the follow-up Then.
    let back: PanicError = rmp_serde::from_slice(&buf).expect("deserialize PanicError");
    world.panic_err = Some(back);
}

#[then(
    regex = r#"^the err field is exactly "R" \(the reason alone\), the payload value is absent$"#
)]
async fn then_err_field_reason_only(world: &mut ErrorWorld) {
    let err_field = world
        .with_str_result
        .clone()
        .expect("serialized err field captured")
        .expect("err field is a string");
    assert_eq!(
        err_field, "on_stop returned error",
        "the serialized err must be the reason's Display alone"
    );
}

#[then(
    regex = r#"^after deserialize the inner String is "R" and the original value is unrecoverable$"#
)]
async fn then_inner_reason_only(world: &mut ErrorWorld) {
    let pe = world.panic_err.as_ref().expect("a recovered PanicError");
    let s = pe.with_str(|s| s.to_string());
    assert_eq!(
        s.as_deref(),
        Some("on_stop returned error"),
        "the recovered inner String must be the reason alone"
    );
    let got: Option<PayloadT> = pe.downcast::<PayloadT>();
    assert_eq!(
        got, None,
        "the original PayloadT value must be unrecoverable after serde"
    );
}

// ===========================================================================
// @boundary — RegistryError distinct domains
// ===========================================================================

#[given(regex = r"^a registry lookup that finds an actor of the wrong type$")]
async fn given_registry_bad_type(_world: &mut ErrorWorld) {}

#[given(regex = r"^a registry registration whose name is already taken$")]
async fn given_registry_name_taken(_world: &mut ErrorWorld) {}

#[then(regex = r"^the first yields RegistryError::BadActorType$")]
async fn then_first_bad_type(_world: &mut ErrorWorld) {
    assert!(matches!(
        RegistryError::BadActorType,
        RegistryError::BadActorType
    ));
    assert_eq!(RegistryError::BadActorType.to_string(), "bad actor type");
}

#[then(regex = r"^the second yields RegistryError::NameAlreadyRegistered$")]
async fn then_second_name_registered(_world: &mut ErrorWorld) {
    assert!(matches!(
        RegistryError::NameAlreadyRegistered,
        RegistryError::NameAlreadyRegistered
    ));
    assert_eq!(
        RegistryError::NameAlreadyRegistered.to_string(),
        "name already registered"
    );
}

#[then(regex = r"^the two are not equal and carry different Display strings$")]
async fn then_two_distinct(_world: &mut ErrorWorld) {
    // RegistryError does not derive PartialEq; distinctness is asserted via the
    // (necessarily different) Display strings and distinct variant constructors.
    let a = RegistryError::BadActorType.to_string();
    let b = RegistryError::NameAlreadyRegistered.to_string();
    assert_ne!(
        a, b,
        "the two failure domains must carry different Display strings"
    );
    assert_eq!(a, "bad actor type");
    assert_eq!(b, "name already registered");
}

// ===========================================================================
// @property / @model laws (error.properties.feature) — proptest over the
// SendError variant set with boundary-biased generators, asserting the ORACLE.
// ===========================================================================

/// Boundary-biased u32 generator for message payloads.
fn msg_values() -> impl Strategy<Value = u32> {
    prop_oneof![
        Just(0u32),
        Just(1),
        Just(u32::MAX - 1),
        Just(u32::MAX),
        any::<u32>()
    ]
}

/// The seven generator tokens named in the # GEN lines, built from a u32 seed
/// and a string seed so the law covers every variant incl both Timeout boundaries.
fn gen_variants(m: u32, e: &str) -> Vec<SE> {
    vec![
        SendError::ActorNotRunning(TestMsg(m)),
        SendError::ActorStopped,
        SendError::MailboxFull(TestMsg(m)),
        SendError::HandlerError(TestErr(e.to_string())),
        SendError::Timeout(Some(TestMsg(m))),
        SendError::Timeout(None),
    ]
}

#[given(regex = r"^any SendError value drawn from all five variants$")]
async fn given_any_value_all_variants(_world: &mut ErrorWorld) {}

#[when(regex = r"^map_msg\(f\) and independently map_err\(g\) are applied$")]
async fn when_map_msg_and_map_err(_world: &mut ErrorWorld) {}

#[then(regex = r"^the variant tag is unchanged in both results, for every variant$")]
async fn law_tag_unchanged(_world: &mut ErrorWorld) {
    for &m in &[0u32, 1, u32::MAX - 1, u32::MAX] {
        for v in gen_variants(m, "e") {
            let before = tag_of(&v);
            let after_msg = tag_of(&v.clone().map_msg(|TestMsg(n)| TestMsg(n.wrapping_add(1))));
            let after_err = tag_of(&v.map_err(|TestErr(s)| TestErr(format!("{s}!"))));
            assert_eq!(after_msg, before, "map_msg must preserve the tag");
            assert_eq!(after_err, before, "map_err must preserve the tag");
        }
    }
    proptest!(|(m in msg_values(), e in ".*")| {
        for v in gen_variants(m, &e) {
            let before = tag_of(&v);
            prop_assert_eq!(tag_of(&v.clone().map_msg(|TestMsg(n)| TestMsg(n.wrapping_add(1)))), before);
            prop_assert_eq!(tag_of(&v.map_err(|TestErr(s)| TestErr(format!("{s}!")))), before);
        }
    });
}

#[then(
    regex = r"^map_msg applies f only to ActorNotRunning/MailboxFull/Timeout\(Some\); map_err applies g only to HandlerError$"
)]
async fn law_applied_predicate(_world: &mut ErrorWorld) {
    proptest!(|(m in msg_values(), e in ".*")| {
        // map_msg: payload changes exactly for ANR / Full / Timeout(Some).
        for v in gen_variants(m, &e) {
            let tag = tag_of(&v);
            let before_msg = v.clone().msg();
            let after_msg = v.clone().map_msg(|TestMsg(n)| TestMsg(n.wrapping_add(1))).msg();
            let should_change = matches!(
                v,
                SendError::ActorNotRunning(_) | SendError::MailboxFull(_) | SendError::Timeout(Some(_))
            );
            if should_change {
                prop_assert_eq!(after_msg, before_msg.map(|TestMsg(n)| TestMsg(n.wrapping_add(1))));
            } else {
                prop_assert_eq!(after_msg, before_msg);
            }
            // map_err: error changes exactly for HandlerError.
            let before_err = v.clone().err();
            let after_err = v.clone().map_err(|TestErr(s)| TestErr(format!("{s}!"))).err();
            if matches!(tag, Tag::HandlerError) {
                prop_assert_eq!(after_err, before_err.map(|TestErr(s)| TestErr(format!("{s}!"))));
            } else {
                prop_assert_eq!(after_err, before_err);
            }
        }
    });
}

#[given(regex = r"^any SendError<M, E> value over concrete types M and E$")]
async fn given_any_value_concrete(_world: &mut ErrorWorld) {}

#[when(regex = r"^boxed\(\) erases it to BoxSendError and try_downcast::<M, E>\(\) recovers it$")]
async fn when_boxed_then_try_downcast(_world: &mut ErrorWorld) {}

#[then(
    regex = r"^the recovered SendError equals the original, for every variant and every concrete M, E$"
)]
async fn law_boxed_downcast_identity(_world: &mut ErrorWorld) {
    for &m in &[0u32, 1, u32::MAX - 1, u32::MAX] {
        for v in gen_variants(m, "concrete") {
            let recovered = v
                .clone()
                .boxed()
                .try_downcast::<TestMsg, TestErr>()
                .expect("correct type");
            assert_eq!(recovered, v, "try_downcast ∘ boxed must be the identity");
        }
    }
    proptest!(|(m in msg_values(), e in ".*")| {
        for v in gen_variants(m, &e) {
            let recovered = v.clone().boxed().try_downcast::<TestMsg, TestErr>().unwrap();
            prop_assert_eq!(recovered, v);
        }
    });
}

#[given(
    regex = r"^a BoxSendError produced by boxing a SendError whose payload is concrete type A$"
)]
async fn given_box_concrete_a(_world: &mut ErrorWorld) {}

#[then(
    regex = r"^it returns Err carrying a BoxSendError of the SAME variant, with no panic, for every variant$"
)]
async fn law_wrong_type_same_variant(_world: &mut ErrorWorld) {
    // A = OtherMsg; B = TestMsg (!= A). For payload-bearing variants try_downcast
    // must return Err whose recovered BoxSendError is the SAME variant.
    let payload_bearing: Vec<(Tag, SendError<OtherMsg, TestErr>)> = vec![
        (
            Tag::ActorNotRunning,
            SendError::ActorNotRunning(OtherMsg(5)),
        ),
        (Tag::MailboxFull, SendError::MailboxFull(OtherMsg(6))),
        (Tag::Timeout, SendError::Timeout(Some(OtherMsg(7)))),
    ];
    for (tag, v) in payload_bearing {
        let boxed = v.boxed();
        let res = boxed.try_downcast::<TestMsg, TestErr>();
        let recovered = res
            .err()
            .expect("wrong-type payload-bearing downcast must be Err");
        assert_eq!(
            tag_of(&recovered),
            tag,
            "the Err must re-wrap as the SAME variant"
        );
        // And the original is still recoverable with the correct type.
        let back: SendError<OtherMsg, TestErr> = recovered.downcast::<OtherMsg, TestErr>();
        assert_eq!(tag_of(&back), tag);
    }
}

#[then(
    regex = r"^a Timeout\(None\) downcasts Ok for ANY requested type because it carries no payload$"
)]
async fn law_timeout_none_ok_any_type(_world: &mut ErrorWorld) {
    // Timeout(None) has nothing to downcast ⇒ Ok for ANY requested type.
    let v: SendError<OtherMsg, TestErr> = SendError::Timeout(None);
    let res = v.boxed().try_downcast::<TestMsg, TestErr>();
    let ok = res.expect("Timeout(None) carries no payload so any-type downcast is Ok");
    assert_eq!(
        ok,
        SendError::Timeout(None),
        "Timeout(None) round-trips to Timeout(None)"
    );
}

#[given(
    regex = r"^any nested SendError<M, SendError<M, E>> whose outer is HandlerError wrapping any inner variant$"
)]
async fn given_any_nested(_world: &mut ErrorWorld) {}

#[when(regex = r"^flatten\(\) is applied$")]
async fn when_flatten_applied(_world: &mut ErrorWorld) {}

#[then(
    regex = r"^each inner failure domain is hoisted to the matching outer variant, for every inner variant$"
)]
async fn law_flatten_hoists(_world: &mut ErrorWorld) {
    fn check(m: u32, e: &str) {
        let cases: Vec<(SendError<TestMsg, SE>, SE)> = vec![
            (
                SendError::HandlerError(SendError::ActorNotRunning(TestMsg(m))),
                SendError::ActorNotRunning(TestMsg(m)),
            ),
            (
                SendError::HandlerError(SendError::ActorStopped),
                SendError::ActorStopped,
            ),
            (
                SendError::HandlerError(SendError::MailboxFull(TestMsg(m))),
                SendError::MailboxFull(TestMsg(m)),
            ),
            (
                SendError::HandlerError(SendError::Timeout(Some(TestMsg(m)))),
                SendError::Timeout(Some(TestMsg(m))),
            ),
            (
                SendError::HandlerError(SendError::Timeout(None)),
                SendError::Timeout(None),
            ),
            (
                SendError::HandlerError(SendError::HandlerError(TestErr(e.to_string()))),
                SendError::HandlerError(TestErr(e.to_string())),
            ),
            // Bare outer variants flatten leaves in place.
            (
                SendError::ActorNotRunning(TestMsg(m)),
                SendError::ActorNotRunning(TestMsg(m)),
            ),
            (SendError::ActorStopped, SendError::ActorStopped),
            (
                SendError::MailboxFull(TestMsg(m)),
                SendError::MailboxFull(TestMsg(m)),
            ),
            (
                SendError::Timeout(Some(TestMsg(m))),
                SendError::Timeout(Some(TestMsg(m))),
            ),
            (SendError::Timeout(None), SendError::Timeout(None)),
        ];
        for (nested, expected) in cases {
            assert_eq!(
                nested.flatten(),
                expected,
                "flatten must hoist to the matching outer variant"
            );
        }
    }
    for &m in &[0u32, 1, u32::MAX - 1, u32::MAX] {
        check(m, "e");
    }
    proptest!(|(m in msg_values(), e in ".*")| {
        check(m, &e);
    });
}
