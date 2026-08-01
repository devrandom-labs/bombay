//! Card #278 stage-1 integration tests: the caps surface on the Shell
//! adapter — spike proofs O1/O4 ported in-repo, the W1 walkthrough
//! (plain actor: ONE trait impl + derive(Msg)), and adapter forwarding
//! (on_stop reason, panic → on_panic domain).

use std::time::Duration;

use bombay::{
    actor::Flow,
    caps::{Actor, CapSet, Ctx, Handle, Provide, Replay, Shell, spawn},
    error::ActorStopReason,
    mailbox::Mailboxed,
    reply::ReplySender,
};
use tokio::sync::mpsc;

// ----------------------------------------------------- third-party cap --

/// O1: a capability wholly foreign to bombay-core — defined here, joined
/// to a cap set through the public seam only.
mod third_party {
    pub struct RateLimited {
        pub tokens: u32,
    }

    pub trait RatePolicy {
        fn burst() -> u32;
    }

    impl RateLimited {
        pub fn new<RP: RatePolicy>() -> Self {
            Self {
                tokens: RP::burst(),
            }
        }
        pub fn try_take(&mut self) -> bool {
            if self.tokens == 0 {
                return false;
            }
            self.tokens -= 1;
            true
        }
    }
}

use third_party::{RateLimited, RatePolicy};

struct Burst2;
impl RatePolicy for Burst2 {
    fn burst() -> u32 {
        2
    }
}

// -------------------------------------------------- W1 + O1/O4 actor --

#[derive(Debug, bombay_macros::Msg)]
enum GateMsg {
    Submit { id: u64, reply: ReplySender<bool> },
    Stop,
}

/// Cap set: one third-party capability, hand-written impls standing where
/// `#[derive(bombay_macros::Provide)]` emits the same (the derive's own
/// doctests cover the emission; here the seam is exercised end to end).
struct GateCaps {
    rate: RateLimited,
}

impl CapSet<Gatekeeper> for GateCaps {
    fn build(_: &<Gatekeeper as Actor>::Args) -> Self {
        Self {
            rate: RateLimited::new::<Burst2>(),
        }
    }
}

impl Provide<RateLimited> for GateCaps {
    fn provide(&mut self) -> &mut RateLimited {
        &mut self.rate
    }
}

// A hand-written cap set supplies its own loop-participation hook; with no
// stash field it yields `None` (the derive emits exactly this for you).
impl<M> Replay<M> for GateCaps {
    fn next_replay(&mut self) -> Option<M> {
        None
    }
}

struct Gatekeeper {
    admitted: u64,
    probe: mpsc::UnboundedSender<ActorStopReason>,
}

impl Mailboxed for Gatekeeper {
    type Msg = GateMsg;
}

impl Actor for Gatekeeper {
    type Msg = GateMsg;
    type Args = mpsc::UnboundedSender<ActorStopReason>;
    type Error = core::convert::Infallible;
    type Caps = GateCaps;

    async fn init(probe: Self::Args, _: Ctx<'_, Self>) -> Result<Self, Self::Error> {
        Ok(Self { admitted: 0, probe })
    }

    async fn handle(&mut self, msg: GateMsg, mut cx: Ctx<'_, Self>) -> Result<Flow, Self::Error> {
        match msg {
            GateMsg::Submit { id, reply } => {
                // O4: the third-party policy (Burst2) drives behavior,
                // reached through the ONE gated accessor.
                let admitted = cx.cap::<RateLimited>().try_take();
                if admitted {
                    self.admitted = self.admitted.wrapping_add(id);
                }
                let _ = reply.send(admitted);
                Ok(Flow::Continue)
            }
            GateMsg::Stop => Ok(Flow::Stop),
        }
    }

    async fn on_stop(
        &mut self,
        _: bombay::actor::WeakActorRef<Shell<Self>>,
        reason: ActorStopReason,
    ) -> Result<(), Self::Error> {
        let _ = self.probe.send(reason);
        Ok(())
    }
}

/// O1 + O4 + W1: ONE trait impl (plus derive(Msg) and the cap-set pair),
/// one spawn call, ask round-trips, third-party policy enforced.
#[tokio::test]
async fn caps_actor_round_trip_with_third_party_cap() {
    let (tx, mut rx) = mpsc::unbounded_channel();
    let gate: Handle<Gatekeeper> = spawn::<Gatekeeper>(tx);

    let ask = |id: u64| gate.ask(move |reply| GateMsg::Submit { id, reply });
    assert!(ask(1).await.expect("ask 1"), "burst slot 1 admitted");
    assert!(ask(2).await.expect("ask 2"), "burst slot 2 admitted");
    assert!(
        !ask(3).await.expect("ask 3"),
        "third-party Burst2 policy refuses the third submit"
    );

    // Flow::Stop path + on_stop forwarding through the Shell.
    gate.tell(GateMsg::Stop).await.expect("tell stop");
    let reason = tokio::time::timeout(Duration::from_secs(5), rx.recv())
        .await
        .expect("on_stop must run within the grace")
        .expect("probe open");
    assert!(
        matches!(reason, ActorStopReason::Normal),
        "Flow::Stop maps to Normal through the adapter; got {reason:?}"
    );
}

// ------------------------------------------- panic-domain forwarding --

#[derive(Debug, bombay_macros::Msg)]
enum BoomMsg {
    Boom,
}

struct Boomer {
    probe: mpsc::UnboundedSender<&'static str>,
}

impl Mailboxed for Boomer {
    type Msg = BoomMsg;
}

impl Actor for Boomer {
    type Msg = BoomMsg;
    type Args = mpsc::UnboundedSender<&'static str>;
    type Error = core::convert::Infallible;
    type Caps = ();

    async fn init(probe: Self::Args, _: Ctx<'_, Self>) -> Result<Self, Self::Error> {
        Ok(Self { probe })
    }

    async fn handle(&mut self, msg: BoomMsg, _: Ctx<'_, Self>) -> Result<Flow, Self::Error> {
        match msg {
            BoomMsg::Boom => panic!("deliberate test panic"),
        }
    }

    async fn on_panic(
        &mut self,
        _: bombay::actor::WeakActorRef<Shell<Self>>,
        err: bombay::error::PanicError,
    ) -> ActorStopReason {
        let _ = self.probe.send("on_panic");
        ActorStopReason::Panicked(err)
    }

    async fn on_stop(
        &mut self,
        _: bombay::actor::WeakActorRef<Shell<Self>>,
        _: ActorStopReason,
    ) -> Result<(), Self::Error> {
        let _ = self.probe.send("on_stop");
        Ok(())
    }
}

/// A handler panic reaches the caps-level `on_panic`, then `on_stop`,
/// in that order — the poisoning pipeline is inherited, not re-built.
#[tokio::test]
async fn handler_panic_forwards_to_caps_hooks_in_order() {
    let (tx, mut rx) = mpsc::unbounded_channel();
    let boomer = spawn::<Boomer>(tx);
    boomer.tell(BoomMsg::Boom).await.expect("tell boom");

    let first = tokio::time::timeout(Duration::from_secs(5), rx.recv())
        .await
        .expect("hooks run promptly")
        .expect("probe open");
    let second = tokio::time::timeout(Duration::from_secs(5), rx.recv())
        .await
        .expect("on_stop follows")
        .expect("probe open");
    assert_eq!(
        (first, second),
        ("on_panic", "on_stop"),
        "panic pipeline order through the Shell"
    );
}
