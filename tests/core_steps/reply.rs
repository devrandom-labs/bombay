//! Shared `ReplyWorld` + step definitions for the core `reply` scenarios.
//!
//! Wired by two runners that `#[path]`-include this module:
//!   * `core_reply_bdd.rs`        — the example feature (reply.feature)
//!   * `core_reply_props_bdd.rs`  — the property laws (reply.properties.feature)
//!
//! Scope (src/reply.rs): the `Reply` trait (`to_result`/`into_any_err`/
//! `into_value`/`downcast_ok`/`downcast_err`), the `Result<T,E>` and
//! `impl_infallible_reply!` blanket impls, the `DelegatedReply` marker, the
//! single-use `ReplySender`, and `ForwardedReply` (Forwarded vs Direct;
//! `from_ok`/`from_err`/`from_result`; downcast paths).
//!
//! Most of the surface is exercised as PURE VALUES (no spawned actors). Three
//! cases need real actor machinery, reached through the public API:
//!   * `ReplySender` single-use — obtained inside a handler via
//!     `ctx.reply_sender()` (its `new` is `pub(crate)`); `send(self, …)` consumes
//!     it, so the single-use guarantee is the move itself.
//!   * `DelegatedReply` marker — also obtained inside a handler via
//!     `ctx.reply_sender()` (it is `Copy`), so the boundary `to_result`/
//!     `into_value` panics are caught on a copy.
//!   * `@linearizability` concurrent forwarded asks — real `ctx.forward` routing
//!     across spawned router/target actors with `tokio::spawn` + `Barrier`.
//!
//! The bare `Forwarded(Ok(()))` / `Forwarded(Err(e))` STATES are produced in
//! production only by the `pub(crate)` `ForwardedReply::new` (consumed by the
//! dispatcher), so they are reached via the gated `bombay::reply::testing::forwarded`
//! constructor (added under the `testing` feature for exactly this).
//!
//! Every assertion is the SPECIFIC value confirmed in the scenario's
//! `# Confirmed:` / `# ORACLE:` note (facts only — no vague `contains`).

use std::{
    any::Any,
    num::{NonZeroI128, NonZeroU8, NonZeroUsize},
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicUsize},
    },
};

use bombay::{
    error::{BoxSendError, Infallible, SendError},
    prelude::*,
    reply::{DelegatedReply, ForwardedReply, Reply, testing::forwarded},
};
use cucumber::{World, given, then, when};
use proptest::prelude::*;
use tokio::sync::Barrier;

// ===========================================================================
// Concrete reply / value / error test types
// ===========================================================================

/// A concrete handler-error type carried by `Result` / `ForwardedReply` replies.
/// `ReplyError` is `Debug + Send + 'static`, which this satisfies.
#[derive(Debug, Clone, PartialEq, Eq)]
struct TestErr(u32);

/// A distinct message type used as the `M` slot in `ForwardedReply<M, R>` and the
/// downcast scenarios (so the outer/inner downcast paths can be told apart).
#[derive(Debug, Clone, PartialEq, Eq)]
struct TestMsg(u64);

/// A second message type used to FORCE the inner `try_downcast::<N, R::Error>` to
/// FAIL so the outer-SendError fallback (and its `map_msg` unreachable guard) is
/// reached on a message-bearing outer variant.
#[derive(Debug, Clone, PartialEq, Eq)]
struct OtherN(i8);

// ===========================================================================
// World
// ===========================================================================

#[derive(Debug, Default, World)]
pub struct ReplyWorld {
    /// A captured `Result` reply value's `to_result` outcome.
    result_ok: Option<Result<u32, TestErr>>,
    /// Captured `into_any_err` presence + recovered error.
    any_err_recovered: Option<Option<TestErr>>,
    /// Captured `into_value` outcome for a `Result` reply.
    value_out: Option<Result<u32, TestErr>>,

    /// Captured `to_result` of an infallible reply (a `String`).
    infallible_to_result: Option<Result<String, Infallible>>,
    /// Captured `into_any_err` of an infallible reply.
    infallible_any_err_none: Option<bool>,

    /// The single reply an ask delegated through a taken `ReplySender` produced.
    sender_reply: Option<u32>,
    /// The boxed wire value a `ReplySender::send(Ok(..))` placed on the channel.
    wire_ok: Option<u32>,
    /// The boxed wire error a `ReplySender::send(Err(..))` placed on the channel.
    wire_err: Option<TestErr>,

    /// `ForwardedReply::from_*` `to_result` / `into_value` outcomes.
    fwd_to_result: Option<Result<u32, SendError<TestMsg, TestErr>>>,
    fwd_into_value: Option<Result<u32, SendError<TestMsg, TestErr>>>,
    /// `ForwardedReply` `into_any_err` recovered (as a presence + variant probe).
    fwd_any_err: Option<Option<SendError<TestMsg, TestErr>>>,
    /// from_result(Ok) and from_result(Err) both-arm captures.
    fwd_result_ok: Option<Result<u32, SendError<TestMsg, TestErr>>>,
    fwd_result_err: Option<Result<u32, SendError<TestMsg, TestErr>>>,

    /// `Forwarded(Ok(()))` into_any_err presence.
    forwarded_ok_any_err_none: Option<bool>,
    /// `Forwarded(Err(send_error))` into_any_err recovered SendError.
    forwarded_err_any_err: Option<Option<SendError<TestMsg, Infallible>>>,

    /// DelegatedReply marker probe results.
    delegated_to_result_panicked: Option<bool>,
    delegated_into_value_panicked: Option<bool>,
    delegated_any_err_none: Option<bool>,

    /// downcast_ok recovered inner Ok value.
    downcast_ok_value: Option<u32>,
    /// downcast_err (inner path) recovered SendError (nested).
    downcast_err_inner: Option<SendError<TestMsg, SendError<TestMsg, TestErr>>>,
    /// downcast_err (outer message-less path) recovered SendError.
    downcast_err_outer: Option<SendError<OtherN, SendError<TestMsg, TestErr>>>,
    /// downcast_err outer map_msg unreachable-guard hit (panic caught).
    downcast_err_map_msg_panicked: Option<bool>,

    /// default Reply::downcast_ok wrong-type panic.
    reply_downcast_ok_panicked: Option<bool>,
    /// Forwarded(Ok) into_value unreachable panic.
    forwarded_ok_into_value_panicked: Option<bool>,

    /// Concurrent forwarded-ask results: task_id -> recovered value.
    concurrent: Vec<(u64, u64)>,
}

// ===========================================================================
// Test actors (ReplySender single-use; DelegatedReply marker; concurrency)
// ===========================================================================

/// An actor that, on each ask, takes the reply channel via `ctx.reply_sender()`
/// and sends a value through it manually — the single-use `ReplySender` SUT.
#[derive(Clone)]
struct Delegator;

impl Actor for Delegator {
    type Args = Self;
    type Error = Infallible;

    async fn on_start(state: Self::Args, _: ActorRef<Self>) -> Result<Self, Self::Error> {
        Ok(state)
    }
}

/// Asks the delegator to reply `value` via a taken `ReplySender`.
struct SendVia(u32);

impl Message<SendVia> for Delegator {
    type Reply = DelegatedReply<u32>;

    async fn handle(&mut self, msg: SendVia, ctx: &mut Context<Self, Self::Reply>) -> Self::Reply {
        let (delegated, sender) = ctx.reply_sender();
        if let Some(tx) = sender {
            // `send(self, ..)` consumes the ReplySender — the single-use move.
            tx.send(msg.0);
        }
        delegated
    }
}

/// A target actor for the concurrent forwarding model: echoes the asked value.
#[derive(Clone)]
struct EchoTarget;

impl Actor for EchoTarget {
    type Args = Self;
    type Error = Infallible;

    async fn on_start(state: Self::Args, _: ActorRef<Self>) -> Result<Self, Self::Error> {
        Ok(state)
    }
}

struct Echo(u64);

impl Message<Echo> for EchoTarget {
    type Reply = u64;

    async fn handle(&mut self, msg: Echo, _ctx: &mut Context<Self, Self::Reply>) -> Self::Reply {
        msg.0
    }
}

/// A router whose handler `ctx.forward`s each ask to the target, yielding a
/// `ForwardedReply` whose `downcast_ok` recovers the per-reply value.
#[derive(Clone)]
struct ForwardRouter {
    target: ActorRef<EchoTarget>,
}

impl Actor for ForwardRouter {
    type Args = Self;
    type Error = Infallible;

    async fn on_start(state: Self::Args, _: ActorRef<Self>) -> Result<Self, Self::Error> {
        Ok(state)
    }
}

struct RouteEcho(u64);

impl Message<RouteEcho> for ForwardRouter {
    type Reply = ForwardedReply<Echo, <EchoTarget as Message<Echo>>::Reply>;

    async fn handle(
        &mut self,
        msg: RouteEcho,
        ctx: &mut Context<Self, Self::Reply>,
    ) -> Self::Reply {
        ctx.forward(&self.target, Echo(msg.0)).await
    }
}

// ===========================================================================
// @sequence — Reply conversion protocol on Result and infallible types
// ===========================================================================

#[given(regex = r"^a reply value Ok\(value\) of type Result<T, E>$")]
async fn given_result_ok(world: &mut ReplyWorld) {
    let v: Result<u32, TestErr> = Ok(7);
    world.result_ok = Some(v.clone().to_result());
    world.any_err_recovered = Some(
        v.clone()
            .into_any_err()
            .map(|b| *b.downcast::<TestErr>().expect("E")),
    );
    world.value_out = Some(Reply::into_value(v));
}

#[when(regex = r"^to_result is called$")]
async fn when_to_result(_world: &mut ReplyWorld) {}

// SHARED Then: `it returns Ok(value)` is used by BOTH the Result-Ok scenario
// (asserting to_result on a `Result`) and the ForwardedReply::from_ok scenario
// (asserting to_result on a Direct(Ok)). Each Given populates a different slot.
#[then(regex = r"^it returns Ok\(value\)$")]
async fn then_returns_ok_value(world: &mut ReplyWorld) {
    if let Some(r) = &world.result_ok {
        assert_eq!(
            *r,
            Ok(7),
            "to_result on a Result is the identity: Ok(value) stays Ok(value)"
        );
    } else if let Some(fwd) = &world.fwd_to_result {
        assert_eq!(
            *fwd,
            Ok(5),
            "from_ok builds Direct(Ok(value)); to_result maps Ok through unchanged"
        );
    } else {
        panic!("no to_result Ok value was captured by a preceding step");
    }
}

// SHARED Then: `into_any_err returns None` is used by the Result-Ok scenario,
// the example infallible scenario, AND the ForwardedReply::from_ok scenario.
// Each Given populates a DIFFERENT World slot; assert whichever was set so the
// one definition serves all three (cucumber matches by regex — one fn per regex).
#[then(regex = r"^into_any_err returns None$")]
async fn then_any_err_none(world: &mut ReplyWorld) {
    if let Some(recovered) = &world.any_err_recovered {
        assert_eq!(*recovered, None, "into_any_err on an Ok value yields None");
    } else if let Some(none) = world.infallible_any_err_none {
        assert!(none, "an infallible type's into_any_err yields None");
    } else if let Some(fwd) = &world.fwd_any_err {
        assert_eq!(*fwd, None, "from_ok's into_any_err yields None");
    } else {
        panic!("no into_any_err result was captured by a preceding step");
    }
}

#[then(regex = r"^into_value returns the same Ok\(value\)$")]
async fn then_into_value_same_ok(world: &mut ReplyWorld) {
    assert_eq!(
        world.value_out,
        Some(Ok(7)),
        "into_value on a Result is the identity"
    );
}

#[given(regex = r"^a reply value Err\(e\) of type Result<T, E>$")]
async fn given_result_err(world: &mut ReplyWorld) {
    let v: Result<u32, TestErr> = Err(TestErr(9));
    world.any_err_recovered = Some(
        v.clone()
            .into_any_err()
            .map(|b| *b.downcast::<TestErr>().expect("E")),
    );
    world.result_ok = Some(v.to_result());
}

#[when(regex = r"^into_any_err is called$")]
async fn when_into_any_err(_world: &mut ReplyWorld) {}

#[then(regex = r"^it returns Some boxing e as a dyn ReplyError$")]
async fn then_some_boxing_e(world: &mut ReplyWorld) {
    assert_eq!(
        world.any_err_recovered,
        Some(Some(TestErr(9))),
        "into_any_err on Err boxes e (recovered by downcast to E)"
    );
}

#[then(regex = r"^to_result returns Err\(e\)$")]
async fn then_to_result_err_e(world: &mut ReplyWorld) {
    assert_eq!(
        world.result_ok,
        Some(Err(TestErr(9))),
        "to_result on an Err is the identity: Err(e) stays Err(e)"
    );
}

#[given(regex = r"^a reply value of an impl_infallible_reply type such as String$")]
async fn given_infallible_string(world: &mut ReplyWorld) {
    let s = String::from("hi");
    world.infallible_to_result = Some(s.clone().to_result());
    world.infallible_any_err_none = Some(s.into_any_err().is_none());
}

#[then(regex = r"^it returns Ok\(self\)$")]
async fn then_infallible_ok_self(world: &mut ReplyWorld) {
    assert_eq!(
        world.infallible_to_result,
        Some(Ok(String::from("hi"))),
        "an infallible type's to_result is Ok(self)"
    );
}

#[then(regex = r"^the associated Error type is Infallible$")]
async fn then_error_is_infallible(_world: &mut ReplyWorld) {
    // Type-level guarantee: the `infallible_to_result` field is typed
    // `Result<String, Infallible>`, which only compiles because
    // `<String as Reply>::Error == Infallible`. Reaching here proves it.
    fn assert_infallible<R: Reply<Error = Infallible>>() {}
    assert_infallible::<String>();
    assert_infallible::<()>();
}

// --- Scenario Outline: every infallible-reply family obeys the identity ------

#[given(regex = r"^a reply value of type (.+)$")]
async fn given_infallible_typed(world: &mut ReplyWorld, ty: String) {
    // The Outline's `And into_any_err returns None` step routes to the shared
    // `then_any_err_none` handler, which reads a World slot. The per-type identity
    // (incl. into_any_err == None) is asserted inline below; record the flag so the
    // shared Then confirms it for this row.
    world.infallible_any_err_none = Some(true);
    // Each row asserts the SAME identity contract for one family member. The
    // Outline placeholder `<type>` is substituted into the step text. We assert
    // the contract per type inline (so the Then steps below only confirm flags),
    // because `Reply` is not object-safe and each member has a distinct `Ok`/
    // `Value`. A helper checks to_result==Ok(self), into_value==self, into_any_err
    // None, and Error=Infallible for any infallible type with a comparable value.
    fn check<R>(v: R)
    where
        R: Reply<Ok = R, Error = Infallible, Value = R> + Clone + PartialEq + std::fmt::Debug,
    {
        assert_eq!(
            v.clone().to_result(),
            Ok(v.clone()),
            "to_result must be Ok(self)"
        );
        assert!(
            v.clone().into_any_err().is_none(),
            "into_any_err must be None"
        );
        assert_eq!(Reply::into_value(v.clone()), v, "into_value must be self");
    }
    match ty.as_str() {
        "()" => check(()),
        "u8" => check(7u8),
        "bool" => check(true),
        "String" => check(String::from("x")),
        "Vec<u8>" => check(vec![1u8, 2, 3]),
        "Arc<u8>" => {
            // Arc<u8>: Reply Ok=Self, but Arc is not PartialEq-by-value identity;
            // compare by deref. Assert the three identities by hand.
            let a: Arc<u8> = Arc::new(5);
            assert_eq!(*a.clone().to_result().expect("ok"), 5);
            assert!(a.clone().into_any_err().is_none());
            assert_eq!(*Reply::into_value(a), 5);
        }
        "NonZeroU8" => check(NonZeroU8::new(1).expect("nz")),
        "NonZeroUsize" => check(NonZeroUsize::new(1).expect("nz")),
        "NonZeroI128" => check(NonZeroI128::new(1).expect("nz")),
        "AtomicBool" => {
            // Atomics are not Clone/PartialEq; assert the three identities by hand.
            let to_res = AtomicBool::new(true).to_result().expect("ok");
            assert!(to_res.load(std::sync::atomic::Ordering::SeqCst));
            assert!(AtomicBool::new(true).into_any_err().is_none());
            assert!(
                Reply::into_value(AtomicBool::new(true)).load(std::sync::atomic::Ordering::SeqCst)
            );
        }
        "AtomicUsize" => {
            let to_res = AtomicUsize::new(9).to_result().expect("ok");
            assert_eq!(to_res.load(std::sync::atomic::Ordering::SeqCst), 9);
            assert!(AtomicUsize::new(9).into_any_err().is_none());
            assert_eq!(
                Reply::into_value(AtomicUsize::new(9)).load(std::sync::atomic::Ordering::SeqCst),
                9
            );
        }
        "(u8,)" => check((1u8,)),
        "(u8, u16)" => check((1u8, 2u16)),
        // tuple arity 26 (macro maximum): build the A..=Z tuple of u8s. std caps
        // its Debug/PartialEq derives at arity 12, so the generic `check` (which
        // needs both) can't be used — assert the three identities by hand,
        // probing a representative field (the last, index .25) on each result.
        "(A, …, Z)" => {
            fn mk() -> (
                u8,
                u8,
                u8,
                u8,
                u8,
                u8,
                u8,
                u8,
                u8,
                u8,
                u8,
                u8,
                u8,
                u8,
                u8,
                u8,
                u8,
                u8,
                u8,
                u8,
                u8,
                u8,
                u8,
                u8,
                u8,
                u8,
            ) {
                (
                    0u8, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21,
                    22, 23, 24, 25,
                )
            }
            assert_eq!(
                mk().to_result().expect("Ok(self)").25,
                25,
                "to_result Ok(self)"
            );
            assert!(mk().into_any_err().is_none(), "into_any_err None");
            assert_eq!(Reply::into_value(mk()).25, 25, "into_value self");
        }
        other => panic!("unhandled infallible Outline type: {other:?}"),
    }
}

#[when(regex = r"^the Reply conversions are applied$")]
async fn when_reply_conversions_applied(_world: &mut ReplyWorld) {}

#[then(regex = r"^to_result returns Ok\(self\)$")]
async fn then_outline_to_result_ok_self(_world: &mut ReplyWorld) {
    // Asserted inline in the Given (per concrete type). Reaching here means the
    // identity held for this row.
}

#[then(regex = r"^into_value returns self unchanged$")]
async fn then_outline_into_value_self(_world: &mut ReplyWorld) {
    // Asserted inline in the Given.
}

// --- ReplySender::send maps Ok / Err onto the wire --------------------------

#[given(regex = r"^a ReplySender for a Result reply$")]
async fn given_reply_sender_for_result(_world: &mut ReplyWorld) {
    // The ReplySender is taken inside the Delegator handler (its `new` is
    // pub(crate)); the When spawns the actor and drives the send so the wire
    // outcome (Ok box / HandlerError) can be observed by the caller.
}

#[when(regex = r"^send is called with Ok\(value\)$")]
async fn when_send_ok(world: &mut ReplyWorld) {
    // A handler that takes the channel and sends Ok(value): the wire receives a
    // boxed success which the ask caller downcasts to the value. Build a
    // dedicated actor whose Reply is `Result<u32, TestErr>` and whose handler
    // sends Ok via a taken ReplySender.
    world.wire_ok = Some(run_sender_ok(55).await);
}

#[then(regex = r"^the channel receives Ok\(Box value\) — the boxed success$")]
async fn then_channel_ok_box(world: &mut ReplyWorld) {
    assert_eq!(
        world.wire_ok,
        Some(55),
        "ReplySender::send(Ok(v)) wires Box::new(v); the caller recovers v"
    );
}

#[when(regex = r"^send is called with Err\(e\)$")]
async fn when_send_err(world: &mut ReplyWorld) {
    world.wire_err = Some(run_sender_err(TestErr(66)).await);
}

#[then(regex = r"^the channel receives Err\(BoxSendError::HandlerError boxing e\)$")]
async fn then_channel_err_handler(world: &mut ReplyWorld) {
    assert_eq!(
        world.wire_err,
        Some(TestErr(66)),
        "ReplySender::send(Err(e)) wires BoxSendError::HandlerError(Box::new(e))"
    );
}

// ===========================================================================
// @lifecycle — single-use sender; ForwardedReply ctors; Forwarded arm
// ===========================================================================

#[given(regex = r"^a ReplySender obtained for one received message$")]
async fn given_reply_sender_single_use(_world: &mut ReplyWorld) {}

#[when(regex = r"^send is called once$")]
async fn when_send_once(world: &mut ReplyWorld) {
    let actor = Delegator::spawn(Delegator);
    actor.wait_for_startup().await;
    world.sender_reply = Some(actor.ask(SendVia(11)).await.expect("ask succeeds"));
    actor.stop_gracefully().await.unwrap();
}

#[then(regex = r"^the ReplySender is consumed by value and cannot be used again$")]
async fn then_sender_consumed(world: &mut ReplyWorld) {
    // `send(self, ..)` takes the sender BY VALUE — a second send is a compile
    // error (use-after-move), so single-use is enforced by the type system, not
    // a runtime check. The OBSERVABLE consequence is that exactly one reply was
    // delivered for the one received message.
    assert_eq!(
        world.sender_reply,
        Some(11),
        "the single-use ReplySender delivered exactly the one sent reply"
    );
}

#[given(regex = r"^a ForwardedReply built via from_ok\(value\)$")]
async fn given_forwarded_from_ok(world: &mut ReplyWorld) {
    let r: ForwardedReply<TestMsg, Result<u32, TestErr>> = ForwardedReply::from_ok(5);
    world.fwd_to_result = Some(r.to_result());
    let r2: ForwardedReply<TestMsg, Result<u32, TestErr>> = ForwardedReply::from_ok(5);
    world.fwd_any_err = Some(
        r2.into_any_err()
            .map(|b| *b.downcast::<SendError<TestMsg, TestErr>>().expect("se")),
    );
}

// `it returns Ok(value)` (from_ok's to_result) and `into_any_err returns None`
// (from_ok's into_any_err) are served by the SHARED handlers above
// (`then_returns_ok_value` / `then_any_err_none`), which route on the populated
// World slot. No separate from_ok-specific handlers are needed.

#[given(regex = r"^a ForwardedReply built via from_err\(error\)$")]
async fn given_forwarded_from_err(world: &mut ReplyWorld) {
    let r: ForwardedReply<TestMsg, Result<u32, TestErr>> = ForwardedReply::from_err(TestErr(3));
    world.fwd_into_value = Some(r.into_value());
    let r2: ForwardedReply<TestMsg, Result<u32, TestErr>> = ForwardedReply::from_err(TestErr(3));
    world.fwd_any_err = Some(
        r2.into_any_err()
            .map(|b| *b.downcast::<SendError<TestMsg, TestErr>>().expect("se")),
    );
}

#[when(regex = r"^into_value is called$")]
async fn when_into_value(_world: &mut ReplyWorld) {}

#[then(regex = r"^it returns Err\(SendError::HandlerError\(error\)\)$")]
async fn then_forwarded_into_value_err(world: &mut ReplyWorld) {
    assert_eq!(
        world.fwd_into_value,
        Some(Err(SendError::HandlerError(TestErr(3)))),
        "from_err's into_value maps the Err via SendError::HandlerError"
    );
}

#[then(regex = r"^into_any_err returns Some boxing that HandlerError$")]
async fn then_forwarded_any_err_some_handler(world: &mut ReplyWorld) {
    assert_eq!(
        world.fwd_any_err,
        Some(Some(SendError::HandlerError(TestErr(3)))),
        "from_err's into_any_err wraps the error as SendError::HandlerError"
    );
}

#[given(
    regex = r"^two ForwardedReplies, one from from_result\(Ok\(v\)\) and one from from_result\(Err\(e\)\)$"
)]
async fn given_two_forwarded_from_result(world: &mut ReplyWorld) {
    let ok: ForwardedReply<TestMsg, Result<u32, TestErr>> = ForwardedReply::from_result(Ok(17));
    world.fwd_result_ok = Some(ok.into_value());
    let err: ForwardedReply<TestMsg, Result<u32, TestErr>> =
        ForwardedReply::from_result(Err(TestErr(19)));
    world.fwd_result_err = Some(err.into_value());
}

#[when(regex = r"^each is converted via into_value$")]
async fn when_each_into_value(_world: &mut ReplyWorld) {}

#[then(
    regex = r"^the Ok one yields Ok\(v\) and the Err one yields Err\(SendError::HandlerError\(e\)\)$"
)]
async fn then_from_result_both_arms(world: &mut ReplyWorld) {
    assert_eq!(
        world.fwd_result_ok,
        Some(Ok(17)),
        "from_result(Ok(v)) stores Direct(Ok(v)); into_value yields Ok(v)"
    );
    assert_eq!(
        world.fwd_result_err,
        Some(Err(SendError::HandlerError(TestErr(19)))),
        "from_result(Err(e)) stores Direct(Err(e)); into_value maps to HandlerError(e)"
    );
}

#[given(
    regex = r"^a ForwardedReply representing a forward that succeeded \(Forwarded\(Ok\(\(\)\)\)\)$"
)]
async fn given_forwarded_ok(world: &mut ReplyWorld) {
    let r: ForwardedReply<TestMsg, Result<u32, TestErr>> = forwarded(Ok(()));
    world.forwarded_ok_any_err_none = Some(r.into_any_err().is_none());
}

// SHARED Then: `it returns None` is used by the Forwarded(Ok) into_any_err
// scenario AND the DelegatedReply into_any_err scenario. Each Given sets a
// different World slot; assert whichever was populated.
#[then(regex = r"^it returns None$")]
async fn then_forwarded_ok_none(world: &mut ReplyWorld) {
    if let Some(ok) = world.forwarded_ok_any_err_none {
        assert!(ok, "Forwarded(Ok) into_any_err returns res.err() == None");
    } else if let Some(deleg) = world.delegated_any_err_none {
        assert!(
            deleg,
            "DelegatedReply::into_any_err returns None unconditionally"
        );
    } else {
        panic!("no into_any_err None result was captured by a preceding step");
    }
}

#[given(
    regex = r"^a ForwardedReply representing a forward that failed \(Forwarded\(Err\(send_error\)\)\)$"
)]
async fn given_forwarded_err(world: &mut ReplyWorld) {
    // The forwarding SendError is over the OUTER `SendError<M, R::Error>`; a
    // message-less ActorStopped is the canonical "failed to forward" outcome.
    let r: ForwardedReply<TestMsg, Result<u32, Infallible>> =
        forwarded(Err(SendError::ActorStopped));
    world.forwarded_err_any_err = Some(
        r.into_any_err()
            .map(|b| *b.downcast::<SendError<TestMsg, Infallible>>().expect("se")),
    );
}

#[then(regex = r"^it returns Some boxing that SendError$")]
async fn then_forwarded_err_some(world: &mut ReplyWorld) {
    let got = world
        .forwarded_err_any_err
        .clone()
        .expect("into_any_err captured");
    assert!(
        matches!(got, Some(SendError::ActorStopped)),
        "Forwarded(Err) into_any_err boxes the forwarding SendError, got {got:?}"
    );
}

// ===========================================================================
// @boundary — DelegatedReply misuse, downcast paths, wrong-type downcast
// ===========================================================================

#[given(regex = r"^a DelegatedReply marker value$")]
async fn given_delegated_marker(world: &mut ReplyWorld) {
    // A DelegatedReply<R> has no public constructor (its `new` is pub(crate)),
    // so it is obtained inside a real handler via `ctx.reply_sender()`. It is
    // `Copy`, so the to_result / into_value panics and into_any_err==None are all
    // probed on a copy inside the handler; the flags are recorded into the World.
    let (to_res_panic, into_val_panic, any_err_none) = run_delegated_probe().await;
    world.delegated_to_result_panicked = Some(to_res_panic);
    world.delegated_into_value_panicked = Some(into_val_panic);
    world.delegated_any_err_none = Some(any_err_none);
}

#[when(regex = r"^to_result is called on it directly$")]
async fn when_delegated_to_result(_world: &mut ReplyWorld) {}

// SHARED Then: `the call panics with an unimplemented marker message` is used
// by BOTH the DelegatedReply::to_result scenario and the ::into_value scenario.
// The Given (`given_delegated_marker`) probed both markers; assert both panic so
// the one definition pins both unimplemented! markers (reply.rs:238, :246).
#[then(regex = r"^the call panics with an unimplemented marker message$")]
async fn then_delegated_marker_panics(world: &mut ReplyWorld) {
    assert_eq!(
        world.delegated_to_result_panicked,
        Some(true),
        "DelegatedReply::to_result must panic (unimplemented marker, reply.rs:238)"
    );
    assert_eq!(
        world.delegated_into_value_panicked,
        Some(true),
        "DelegatedReply::into_value must panic (unimplemented marker, reply.rs:246)"
    );
}

#[when(regex = r"^into_value is called on it directly$")]
async fn when_delegated_into_value(_world: &mut ReplyWorld) {}

#[when(regex = r"^into_any_err is called on it$")]
async fn when_delegated_any_err(_world: &mut ReplyWorld) {}

// The DelegatedReply into_any_err scenario's `Then it returns None` is served by
// the SHARED `then_forwarded_ok_none` handler above (routes on the World slot).

#[given(regex = r"^a wire reply whose boxed value is the inner R::Ok of a successful forward$")]
async fn given_wire_inner_ok(world: &mut ReplyWorld) {
    let boxed: Box<dyn Any> = Box::new(42u32);
    world.downcast_ok_value =
        Some(<ForwardedReply<TestMsg, Result<u32, TestErr>> as Reply>::downcast_ok(boxed));
}

#[when(regex = r"^ForwardedReply::downcast_ok is called on that boxed value$")]
async fn when_forwarded_downcast_ok(_world: &mut ReplyWorld) {}

#[then(regex = r"^it returns the inner Ok value$")]
async fn then_downcast_ok_inner(world: &mut ReplyWorld) {
    assert_eq!(
        world.downcast_ok_value,
        Some(42),
        "downcast_ok recovers the inner R::Ok from the boxed wire value"
    );
}

#[given(regex = r"^a wire BoxSendError that originated from the inner R::Error of a forward$")]
async fn given_wire_inner_err(world: &mut ReplyWorld) {
    // Inner R::Error = TestErr boxed as SendError<TestMsg, TestErr>::HandlerError.
    let inner: SendError<TestMsg, TestErr> = SendError::HandlerError(TestErr(11));
    let boxed: BoxSendError = inner.boxed();
    world.downcast_err_inner = Some(
        <ForwardedReply<TestMsg, Result<u32, TestErr>> as Reply>::downcast_err::<TestMsg>(boxed),
    );
}

#[when(regex = r"^ForwardedReply::downcast_err is called$")]
async fn when_forwarded_downcast_err(_world: &mut ReplyWorld) {}

#[then(regex = r"^it recovers the inner error mapped through SendError::HandlerError$")]
async fn then_downcast_err_inner(world: &mut ReplyWorld) {
    let got = world
        .downcast_err_inner
        .clone()
        .expect("downcast_err captured");
    assert_eq!(
        got,
        SendError::HandlerError(SendError::HandlerError(TestErr(11))),
        "downcast_err tries the inner R::Error first, mapping it through HandlerError"
    );
}

#[given(
    regex = r"^a wire BoxSendError that is the OUTER forwarding error \(e\.g\. ActorNotRunning\), not the inner R::Error$"
)]
async fn given_wire_outer_err(world: &mut ReplyWorld) {
    // A MESSAGE-LESS outer variant (ActorStopped) is the clean "failed to
    // forward" outcome: the inner-then-outer downcast recovers it as the outer
    // SendError with no payload to mis-map (so the map_msg guard is never hit).
    let outer: SendError<OtherN, SendError<TestMsg, TestErr>> = SendError::ActorStopped;
    let boxed: BoxSendError = outer.boxed();
    world.downcast_err_outer = Some(
        <ForwardedReply<TestMsg, Result<u32, TestErr>> as Reply>::downcast_err::<OtherN>(boxed),
    );
}

#[then(
    regex = r"^try_downcast::<N, R::Error> fails and it recovers the outer SendError<N, Self::Error>$"
)]
async fn then_downcast_err_outer(world: &mut ReplyWorld) {
    let got = world
        .downcast_err_outer
        .clone()
        .expect("downcast_err captured");
    assert!(
        matches!(got, SendError::ActorStopped),
        "the message-less outer SendError is recovered cleanly, got {got:?}"
    );
}

#[given(
    regex = r"^the outer forwarding SendError still carried the original message \(e\.g\. MailboxFull\(msg\) or Timeout\(Some\(msg\)\)\)$"
)]
async fn given_outer_with_message(world: &mut ReplyWorld) {
    // Force the inner `try_downcast::<N, R::Error>` to FAIL by requesting N=OtherN
    // on an ActorNotRunning whose boxed payload is TestMsg (!= OtherN). The outer
    // fallback then downcasts to SendError<TestMsg, ..> and applies map_msg to the
    // recovered TestMsg payload — hitting the unreachable! guard (reply.rs:559).
    let outer: SendError<TestMsg, SendError<TestMsg, TestErr>> =
        SendError::ActorNotRunning(TestMsg(77));
    let boxed: BoxSendError = outer.boxed();
    let res = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        <ForwardedReply<TestMsg, Result<u32, TestErr>> as Reply>::downcast_err::<OtherN>(boxed)
    }));
    world.downcast_err_map_msg_panicked = Some(res.is_err());
}

#[when(
    regex = r"^downcast_err reaches the outer-SendError fallback and map_msg is applied to that message$"
)]
async fn when_outer_map_msg(_world: &mut ReplyWorld) {}

#[then(regex = r"^the unreachable!\(\.\.\.\) wrong-type guard is hit$")]
async fn then_map_msg_unreachable(world: &mut ReplyWorld) {
    assert_eq!(
        world.downcast_err_map_msg_panicked,
        Some(true),
        "a message-bearing outer SendError reaching map_msg hits the unreachable! guard"
    );
}

#[given(regex = r"^a wire reply whose boxed value is NOT the expected Ok type$")]
async fn given_wire_wrong_ok_type(world: &mut ReplyWorld) {
    let boxed: Box<dyn Any> = Box::new("not a u32");
    let res = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let _ = <Result<u32, TestErr> as Reply>::downcast_ok(boxed);
    }));
    world.reply_downcast_ok_panicked = Some(res.is_err());
}

#[when(regex = r"^the default Reply::downcast_ok is called$")]
async fn when_default_downcast_ok(_world: &mut ReplyWorld) {}

#[then(regex = r"^it panics on the failed downcast$")]
async fn then_default_downcast_ok_panics(world: &mut ReplyWorld) {
    assert_eq!(
        world.reply_downcast_ok_panicked,
        Some(true),
        "default downcast_ok is *ok.downcast().unwrap(); a wrong type panics"
    );
}

#[given(regex = r"^a ForwardedReply in the Forwarded\(Ok\(\(\)\)\) state$")]
async fn given_forwarded_ok_for_into_value(world: &mut ReplyWorld) {
    let r: ForwardedReply<TestMsg, Result<u32, TestErr>> = forwarded(Ok(()));
    let res = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let _ = r.into_value();
    }));
    world.forwarded_ok_into_value_panicked = Some(res.is_err());
}

#[then(regex = r"^the unreachable forwarded-success branch is hit and the call panics$")]
async fn then_forwarded_ok_into_value_panics(world: &mut ReplyWorld) {
    assert_eq!(
        world.forwarded_ok_into_value_panicked,
        Some(true),
        "Forwarded(Ok) into_value hits the unreachable! forwarded-success branch"
    );
}

// ===========================================================================
// @linearizability — concurrent forwarded replies stay correctly typed
// ===========================================================================

#[given(
    regex = r"^a router whose ForwardedReply forwards each ask to a target returning a distinct value$"
)]
async fn given_concurrent_router(_world: &mut ReplyWorld) {}

#[when(regex = r"^many tasks concurrently ask the router and await typed replies$")]
async fn when_concurrent_asks(world: &mut ReplyWorld) {
    world.concurrent = run_concurrent_forwards(32).await;
}

#[then(regex = r"^every task receives its own correct value with no cross-talk between replies$")]
async fn then_no_cross_talk(world: &mut ReplyWorld) {
    for (id, value) in &world.concurrent {
        assert_eq!(
            id, value,
            "task {id} received {value} — a forwarded reply crossed channels"
        );
    }
    assert_eq!(
        world.concurrent.len(),
        32,
        "all 32 forwarded asks completed"
    );
}

#[then(regex = r"^no reply downcast panics$")]
async fn then_no_downcast_panic(world: &mut ReplyWorld) {
    // Each task's `ask().await` ran the ForwardedReply downcast_ok path; if any
    // had panicked, its join handle would have surfaced an Err and the When would
    // have panicked before populating `concurrent`. A full result set proves no
    // downcast panicked.
    assert_eq!(
        world.concurrent.len(),
        32,
        "every forwarded ask resolved through downcast_ok without panicking"
    );
}

// ===========================================================================
// @property / @model laws (reply.properties.feature)
// ===========================================================================

/// Boundary-biased u32 generator for value/error payloads.
fn u32_values() -> impl Strategy<Value = u32> {
    prop_oneof![
        Just(0u32),
        Just(1),
        Just(u32::MAX - 1),
        Just(u32::MAX),
        any::<u32>()
    ]
}

// -- @property @sequence: Result Ok/Err round-trips for any v and any e -------

#[given(regex = r"^any Result<T, E> value that is either Ok\(v\) or Err\(e\)$")]
async fn given_any_result(_world: &mut ReplyWorld) {}

#[when(regex = r"^to_result, into_value, and into_any_err are evaluated on a clone of it$")]
async fn when_eval_result_conversions(_world: &mut ReplyWorld) {}

#[then(regex = r"^to_result returns the same Ok\(v\) or Err\(e\)$")]
async fn law_result_to_result_identity(_world: &mut ReplyWorld) {
    proptest!(|(v in u32_values(), e in u32_values())| {
        let ok: Result<u32, TestErr> = Ok(v);
        prop_assert_eq!(ok.clone().to_result(), Ok(v));
        let err: Result<u32, TestErr> = Err(TestErr(e));
        prop_assert_eq!(err.clone().to_result(), Err(TestErr(e)));
    });
}

#[then(regex = r"^into_value returns the same Ok\(v\) or Err\(e\)$")]
async fn law_result_into_value_identity(_world: &mut ReplyWorld) {
    proptest!(|(v in u32_values(), e in u32_values())| {
        let ok: Result<u32, TestErr> = Ok(v);
        prop_assert_eq!(Reply::into_value(ok), Ok(v));
        let err: Result<u32, TestErr> = Err(TestErr(e));
        prop_assert_eq!(Reply::into_value(err), Err(TestErr(e)));
    });
}

#[then(regex = r"^into_any_err returns None for Ok\(v\) and Some boxing e for Err\(e\)$")]
async fn law_result_into_any_err(_world: &mut ReplyWorld) {
    proptest!(|(v in u32_values(), e in u32_values())| {
        let ok: Result<u32, TestErr> = Ok(v);
        prop_assert!(ok.into_any_err().is_none());
        let err: Result<u32, TestErr> = Err(TestErr(e));
        let recovered = err.into_any_err().expect("Some for Err");
        let got = *recovered.downcast::<TestErr>().expect("E");
        prop_assert_eq!(got, TestErr(e));
    });
}

// -- @property @sequence: ReplySender::send wires Ok / Err for any v / e ------

#[given(regex = r"^a ReplySender for a Result reply and any Result value Ok\(v\) or Err\(e\)$")]
async fn given_sender_any_result(_world: &mut ReplyWorld) {}

#[when(regex = r"^send is called with that value$")]
async fn when_send_that_value(_world: &mut ReplyWorld) {}

#[then(regex = r"^on Ok\(v\) the channel receives Ok\(Box\) recovering v$")]
async fn law_sender_ok_recovers_v(_world: &mut ReplyWorld) {
    // v ∈ boundary-biased {0, 1, MAX-1, MAX}. Each send is exercised on a fresh
    // sender (single-use). proptest cannot drive async actors, so a documented
    // deterministic boundary loop seeds the values; the oracle is the identity
    // (the caller recovers exactly v).
    for v in [0u32, 1, u32::MAX - 1, u32::MAX] {
        assert_eq!(
            run_sender_ok(v).await,
            v,
            "send(Ok(v)) must wire v={v} verbatim"
        );
    }
}

#[then(
    regex = r"^on Err\(e\) the channel receives Err\(BoxSendError::HandlerError\) recovering e$"
)]
async fn law_sender_err_recovers_e(_world: &mut ReplyWorld) {
    for e in [0u32, 1, u32::MAX - 1, u32::MAX] {
        assert_eq!(
            run_sender_err(TestErr(e)).await,
            TestErr(e),
            "send(Err(e)) must wire HandlerError(e) recovering e={e}"
        );
    }
}

// -- @property @sequence: any impl_infallible_reply type yields Ok(self) ------

#[given(regex = r"^any value of an impl_infallible_reply type$")]
async fn given_any_infallible(_world: &mut ReplyWorld) {}

#[when(regex = r"^to_result and into_any_err are evaluated$")]
async fn when_eval_infallible(_world: &mut ReplyWorld) {}

#[then(regex = r"^to_result returns Ok\(self\) and into_any_err returns None$")]
async fn law_infallible_ok_self_none(_world: &mut ReplyWorld) {
    // Sample several infallible types across boundaries; each must yield Ok(self)
    // and into_any_err None. () and bool are constant; integers hit {0,1,MAX-1,MAX};
    // String hits empty + a max-ish string.
    assert_eq!(().to_result(), Ok::<(), Infallible>(()));
    assert!(().into_any_err().is_none());
    for b in [false, true] {
        assert_eq!(b.to_result(), Ok::<bool, Infallible>(b));
        assert!(b.into_any_err().is_none());
    }
    proptest!(|(n in u32_values())| {
        prop_assert_eq!(n.to_result(), Ok::<u32, Infallible>(n));
        prop_assert!(n.into_any_err().is_none());
    });
    for s in ["".to_string(), "x".repeat(1024)] {
        assert_eq!(s.clone().to_result(), Ok::<String, Infallible>(s.clone()));
        assert!(s.into_any_err().is_none());
    }
}

// The infallible property scenario's `And the associated Error type is
// Infallible` is served by the SHARED `then_error_is_infallible` handler above.

// -- @property @lifecycle: from_ok/from_err/from_result preserve any v/e ------

#[given(
    regex = r"^any Result<T, E> value built into a ForwardedReply via from_ok/from_err/from_result$"
)]
async fn given_any_forwarded(_world: &mut ReplyWorld) {}

#[when(regex = r"^into_value, to_result, and into_any_err are evaluated on it$")]
async fn when_eval_forwarded(_world: &mut ReplyWorld) {}

#[then(regex = r"^a from_ok\(v\) / from_result\(Ok\(v\)\) yields Ok\(v\) and into_any_err None$")]
async fn law_forwarded_ok_arm(_world: &mut ReplyWorld) {
    proptest!(|(v in u32_values())| {
        let a: ForwardedReply<TestMsg, Result<u32, TestErr>> = ForwardedReply::from_ok(v);
        prop_assert_eq!(a.into_value(), Ok(v));
        let b: ForwardedReply<TestMsg, Result<u32, TestErr>> = ForwardedReply::from_ok(v);
        prop_assert_eq!(b.to_result(), Ok(v));
        let c: ForwardedReply<TestMsg, Result<u32, TestErr>> = ForwardedReply::from_ok(v);
        prop_assert!(c.into_any_err().is_none());
        let d: ForwardedReply<TestMsg, Result<u32, TestErr>> = ForwardedReply::from_result(Ok(v));
        prop_assert_eq!(d.into_value(), Ok(v));
    });
}

#[then(
    regex = r"^a from_err\(e\) / from_result\(Err\(e\)\) yields Err\(SendError::HandlerError\(e\)\)$"
)]
async fn law_forwarded_err_arm(_world: &mut ReplyWorld) {
    proptest!(|(e in u32_values())| {
        let a: ForwardedReply<TestMsg, Result<u32, TestErr>> = ForwardedReply::from_err(TestErr(e));
        prop_assert_eq!(a.into_value(), Err(SendError::HandlerError(TestErr(e))));
        let b: ForwardedReply<TestMsg, Result<u32, TestErr>> =
            ForwardedReply::from_result(Err(TestErr(e)));
        prop_assert_eq!(b.into_value(), Err(SendError::HandlerError(TestErr(e))));
    });
}

#[then(regex = r"^the Err case's into_any_err is Some boxing that HandlerError$")]
async fn law_forwarded_err_any_err(_world: &mut ReplyWorld) {
    proptest!(|(e in u32_values())| {
        let r: ForwardedReply<TestMsg, Result<u32, TestErr>> = ForwardedReply::from_err(TestErr(e));
        let recovered = r.into_any_err().expect("Some for Err");
        let se = *recovered.downcast::<SendError<TestMsg, TestErr>>().expect("se");
        prop_assert_eq!(se, SendError::HandlerError(TestErr(e)));
    });
}

// -- @model @linearizability: N concurrent forwarded asks, no cross-talk ------

// The @model router Given shares its text with the @linearizability scenario's
// Given (`given_concurrent_router` above) — one handler serves both features.

#[given(
    regex = r"^any number N of tasks concurrently asking the router and awaiting typed replies$"
)]
async fn given_n_tasks(_world: &mut ReplyWorld) {}

#[when(regex = r"^all N asks run with real overlap, started at a barrier$")]
async fn when_n_overlap(_world: &mut ReplyWorld) {}

#[then(regex = r"^every task recovers its own correct value via downcast_ok$")]
async fn law_model_recovers_own_value(_world: &mut ReplyWorld) {
    // N ∈ {2, 8, 64}: the smallest concurrent case plus a large fan-out. Each task
    // forwards a DISTINCT value through the router; the oracle is task_id == value.
    for n in [2u64, 8, 64] {
        let got = run_concurrent_forwards(n).await;
        assert_eq!(got.len() as u64, n, "N={n}: all forwarded asks completed");
        for (id, value) in &got {
            assert_eq!(id, value, "N={n}: task {id} got {value} (cross-talk)");
        }
    }
}

#[then(regex = r"^no reply downcast panics and no value is delivered to the wrong caller$")]
async fn law_model_no_panic_no_misdelivery(_world: &mut ReplyWorld) {
    // Re-run a representative large case so this Then is a real assertion: a full
    // result set with every id==value proves no downcast panicked and no value
    // crossed channels.
    let got = run_concurrent_forwards(64).await;
    assert_eq!(got.len(), 64, "all 64 forwarded asks resolved");
    let mut ids: Vec<u64> = got.iter().map(|(id, _)| *id).collect();
    ids.sort_unstable();
    assert_eq!(
        ids,
        (1..=64).collect::<Vec<_>>(),
        "exactly tasks 1..=64, no dupes"
    );
    for (id, value) in &got {
        assert_eq!(id, value, "task {id} recovered {value} (misdelivery)");
    }
}

// ===========================================================================
// Helpers — real-actor probes reached through the public API
// ===========================================================================

/// An actor whose `Result`-reply handler takes the channel via `reply_sender()`
/// and sends `Ok(v)` (the wire-Ok path). Used by the ReplySender::send scenarios.
#[derive(Clone)]
struct WireOk(u32);
#[derive(Clone)]
struct WireErr(TestErr);

#[derive(Clone)]
struct Wirer;

impl Actor for Wirer {
    type Args = Self;
    type Error = Infallible;

    async fn on_start(state: Self::Args, _: ActorRef<Self>) -> Result<Self, Self::Error> {
        Ok(state)
    }
}

impl Message<WireOk> for Wirer {
    type Reply = DelegatedReply<Result<u32, TestErr>>;

    async fn handle(&mut self, msg: WireOk, ctx: &mut Context<Self, Self::Reply>) -> Self::Reply {
        let (delegated, sender) = ctx.reply_sender();
        if let Some(tx) = sender {
            tx.send(Ok(msg.0));
        }
        delegated
    }
}

impl Message<WireErr> for Wirer {
    type Reply = DelegatedReply<Result<u32, TestErr>>;

    async fn handle(&mut self, msg: WireErr, ctx: &mut Context<Self, Self::Reply>) -> Self::Reply {
        let (delegated, sender) = ctx.reply_sender();
        if let Some(tx) = sender {
            tx.send(Err(msg.0));
        }
        delegated
    }
}

/// Drives a `ReplySender::send(Ok(v))` through a real handler and recovers `v`
/// from the ask caller's reply (the wire-Ok box downcast back to v).
async fn run_sender_ok(v: u32) -> u32 {
    let actor = Wirer::spawn(Wirer);
    actor.wait_for_startup().await;
    let got = actor.ask(WireOk(v)).await.expect("ask succeeds");
    actor.stop_gracefully().await.unwrap();
    got
}

/// Drives `ReplySender::send(Err(e))` and recovers `e` from the caller's
/// `SendError::HandlerError(e)`.
async fn run_sender_err(e: TestErr) -> TestErr {
    let actor = Wirer::spawn(Wirer);
    actor.wait_for_startup().await;
    let result = actor.ask(WireErr(e)).await;
    actor.stop_gracefully().await.unwrap();
    match result {
        Err(SendError::HandlerError(boom)) => boom,
        other => panic!("expected HandlerError on the wire, got {other:?}"),
    }
}

/// Obtains a `DelegatedReply` inside a real handler and probes the three trait
/// methods on a COPY: returns (to_result panicked, into_value panicked,
/// into_any_err is None).
async fn run_delegated_probe() -> (bool, bool, bool) {
    let probe: Arc<Mutex<Option<(bool, bool, bool)>>> = Arc::new(Mutex::new(None));

    #[derive(Clone)]
    struct Probe {
        out: Arc<Mutex<Option<(bool, bool, bool)>>>,
    }

    impl Actor for Probe {
        type Args = Self;
        type Error = Infallible;

        async fn on_start(state: Self::Args, _: ActorRef<Self>) -> Result<Self, Self::Error> {
            Ok(state)
        }
    }

    struct Run;

    impl Message<Run> for Probe {
        type Reply = DelegatedReply<u32>;

        async fn handle(&mut self, _msg: Run, ctx: &mut Context<Self, Self::Reply>) -> Self::Reply {
            let (delegated, sender) = ctx.reply_sender();
            // DelegatedReply is Copy — probe the marker methods on copies, then
            // still send the real reply + return the marker so the ask resolves.
            let c1 = delegated;
            let c2 = delegated;
            let c3 = delegated;
            let to_res = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                let _ = Reply::to_result(c1);
            }))
            .is_err();
            let into_val = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                let _ = Reply::into_value(c2);
            }))
            .is_err();
            let any_err_none = Reply::into_any_err(c3).is_none();
            *self.out.lock().unwrap() = Some((to_res, into_val, any_err_none));
            if let Some(tx) = sender {
                tx.send(0);
            }
            delegated
        }
    }

    let actor = Probe::spawn(Probe {
        out: Arc::clone(&probe),
    });
    actor.wait_for_startup().await;
    let _ = actor.ask(Run).await.expect("ask succeeds");
    actor.stop_gracefully().await.unwrap();
    let out = probe.lock().unwrap().take().expect("probe recorded");
    out
}

/// Spawns one router/target pair and runs `n` concurrent forwarded asks, each
/// carrying a DISTINCT value (the task id 1..=n). Returns (task_id, recovered
/// value) pairs. Real overlap via `tokio::spawn` + `Barrier`.
async fn run_concurrent_forwards(n: u64) -> Vec<(u64, u64)> {
    let target = EchoTarget::spawn(EchoTarget);
    target.wait_for_startup().await;
    let router = ForwardRouter::spawn(ForwardRouter {
        target: target.clone(),
    });
    router.wait_for_startup().await;

    let barrier = Arc::new(Barrier::new(n as usize));
    let handles: Vec<_> = (1..=n)
        .map(|id| {
            let router = router.clone();
            let barrier = Arc::clone(&barrier);
            tokio::spawn(async move {
                barrier.wait().await;
                // The router forwards to the target; the ForwardedReply's
                // downcast_ok recovers this task's own value from its channel.
                let value = router.ask(RouteEcho(id)).await.expect("forward ask");
                (id, value)
            })
        })
        .collect();

    let mut out = Vec::new();
    for h in handles {
        out.push(h.await.expect("forward task must not panic"));
    }
    router.stop_gracefully().await.unwrap();
    target.stop_gracefully().await.unwrap();
    out
}
