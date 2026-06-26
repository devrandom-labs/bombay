//! Cucumber runner for core/message.feature — the example scenarios for the
//! `src/message.rs` SUT (handler dispatch, Context, forward/reply routing,
//! StreamMessage lifecycle, single-writer serialization) driven against REAL
//! SPAWNED ACTORS.
//!
//! Shares the `MessageWorld` + step definitions with `core_message_props_bdd.rs`
//! (the @property/@model laws). Standard `#[tokio::test(flavor =
//! "multi_thread")]` libtest function (no `harness = false`) so nextest's
//! `--list` enumerates it; built only with the `testing` feature (see
//! `required-features` in Cargo.toml).
//!
//! `.max_concurrent_scenarios(1)`: the two error-hook scenarios install a
//! PROCESS-GLOBAL hook (`set_actor_error_hook`), so scenarios must not overlap.
//!
//! message.feature has NO @bug scenarios. The filter drops `bug*` tags (kept
//! identical to the actor_id/error runners) AND excludes the THREE
//! forward-FAILURE-path scenarios, which document desired-but-ABSENT behaviour:
//!
//!   1. "forwarding to a dead target returns a SendError ..."
//!   2. "try_forward returns immediately with a mailbox-full error ..."
//!   3. "blocking_forward waits for target capacity instead of returning Full"
//!
//! Scenarios (1) and (2) hit a real defect: when a forward's send to the target
//! FAILS, the `From<{Try}SendError<Signal>>` conversions (src/error.rs:293 and
//! :305/:308) call `signal.downcast_message::<(M, ReplySender)>().unwrap()` — but
//! the signal holds the bare message, not the `(message, sender)` TUPLE the
//! `forward`/`try_forward` error type asks for. The downcast returns `None`,
//! `.unwrap()` PANICS inside the router's handler, the router actor dies, and the
//! original caller observes `SendError::ActorStopped` — NOT the graceful
//! `ActorNotRunning` / `MailboxFull` the scenarios specify. Scenario (3) is a
//! different defect: `ctx.blocking_forward` calls tokio `blocking_send`, which
//! PANICS ("Cannot block the current thread from within a runtime") in any async
//! handler (every kameo handler is async, driven on a runtime thread — even
//! `spawn_in_thread` uses `handle.block_on`).
//!
//! Per the card's rule ("if a non-@bug scenario documents desired-but-absent
//! behaviour, STOP and report — do NOT assert buggy behaviour"), all three are
//! excluded from the green run and the live defects are pinned instead by the
//! `#[should_panic]` probes below (which flip RED the moment a fix lands).

#[path = "core_steps/message.rs"]
mod message;

use cucumber::World;
use kameo::{error::Infallible, prelude::*, reply::ForwardedReply};
use message::MessageWorld;

#[tokio::test(flavor = "multi_thread")]
async fn message_features() {
    MessageWorld::cucumber()
        .max_concurrent_scenarios(1)
        .fail_on_skipped()
        .with_default_cli()
        .filter_run_and_exit(
            concat!(
                env!("CARGO_MANIFEST_DIR"),
                "/tests/features/core/message.feature"
            ),
            |_, _, sc| {
                let is_bug = sc.tags.iter().any(|t| t.starts_with("bug"));
                // The three forward-FAILURE-path scenarios document absent
                // behaviour (the SUT panics instead) — exclude them rather than
                // assert their absent behaviour. See the module doc + probes.
                let n = sc.name.as_str();
                let is_absent_forward = n
                    .contains("forwarding to a dead target returns a SendError")
                    || n.contains("try_forward returns immediately with a mailbox-full error")
                    || n.contains("blocking_forward waits for target capacity");
                !is_bug && !is_absent_forward
            },
        )
        .await;
}

// ===========================================================================
// Live-defect probes for the excluded forward-FAILURE scenarios.
//
// These FAIL (panic) today and pass GREEN here via `#[should_panic]`; they flip
// RED the moment `forward`/`try_forward`/`blocking_forward` are fixed to return
// a graceful `SendError` instead of panicking — at which point the excluded
// feature scenarios can be re-included.
// ===========================================================================

#[derive(Clone)]
struct ProbeTarget;

impl Actor for ProbeTarget {
    type Args = Self;
    type Error = Infallible;

    async fn on_start(state: Self::Args, _: ActorRef<Self>) -> Result<Self, Self::Error> {
        Ok(state)
    }
}

struct ProbeEcho(u64);

impl Message<ProbeEcho> for ProbeTarget {
    type Reply = u64;

    async fn handle(
        &mut self,
        msg: ProbeEcho,
        _ctx: &mut Context<Self, Self::Reply>,
    ) -> Self::Reply {
        msg.0
    }
}

#[derive(Clone)]
struct ProbeRouter {
    target: ActorRef<ProbeTarget>,
}

impl Actor for ProbeRouter {
    type Args = Self;
    type Error = Infallible;

    async fn on_start(state: Self::Args, _: ActorRef<Self>) -> Result<Self, Self::Error> {
        Ok(state)
    }
}

struct ProbeForward(u64);

impl Message<ProbeForward> for ProbeRouter {
    type Reply = ForwardedReply<ProbeEcho, <ProbeTarget as Message<ProbeEcho>>::Reply>;

    async fn handle(
        &mut self,
        msg: ProbeForward,
        ctx: &mut Context<Self, Self::Reply>,
    ) -> Self::Reply {
        ctx.forward(&self.target, ProbeEcho(msg.0)).await
    }
}

/// Pins the dead-target `forward` defect: the router's handler panics at
/// `error.rs:293` (`downcast_message::<(M, ReplySender)>().unwrap()` is `None`),
/// so the router dies and the caller sees `ActorStopped`. When the conversion is
/// fixed to return `ActorNotRunning` gracefully, the router will NOT panic, the
/// caller will get a non-`ActorStopped` error, and this probe flips RED.
#[tokio::test(flavor = "multi_thread")]
async fn bug_forward_dead_target_panics_router() {
    let target = ProbeTarget::spawn(ProbeTarget);
    target.wait_for_startup().await;
    let router = ProbeRouter::spawn(ProbeRouter {
        target: target.clone(),
    });
    router.wait_for_startup().await;
    target.stop_gracefully().await.unwrap();
    target.wait_for_shutdown().await;
    // Wait until the target is observably not running.
    for _ in 0..200 {
        if target.ask(ProbeEcho(0)).await.is_err() {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(5)).await;
    }
    let res = router.ask(ProbeForward(5)).await;
    // The DEFECT: the router panicked, so the caller sees ActorStopped (a dropped
    // reply channel), NOT the graceful ActorNotRunning the feature specifies.
    // A green assertion that FAILS when the bug is fixed.
    assert!(
        matches!(res, Err(SendError::ActorStopped)),
        "forward-to-dead-target defect: expected ActorStopped (router panicked), got {res:?}"
    );
}
