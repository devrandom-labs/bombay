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
//! THREE forward-FAILURE-path scenarios document desired-but-ABSENT behaviour
//! and are tagged `@bug:<file:line>` in the feature file, so the standard
//! `bug*`-tag filter below drops them from the green run (kept identical to the
//! actor_id/error runners). The live defects are pinned instead by the three
//! red-on-fix probes below:
//!
//!   1. `@bug:error.rs:293` — "forwarding to a dead target returns a SendError ..."
//!   2. `@bug:error.rs:305` — "try_forward returns immediately with a mailbox-full error ..."
//!   3. `@bug:ask.rs:461`   — "blocking_forward waits for target capacity instead of returning Full"
//!
//! Scenarios (1) and (2): when a forward's send to the target FAILS, the
//! `From<{Try}SendError<Signal>>` conversions (src/error.rs:293 and :305) call
//! `signal.downcast_message::<(M, ReplySender)>().unwrap()` — but the signal
//! holds the bare message, not the `(message, sender)` TUPLE the
//! `forward`/`try_forward` error type asks for. The downcast returns `None`,
//! `.unwrap()` PANICS inside the router's handler, the router actor dies, and the
//! original caller observes `SendError::ActorStopped` — NOT the graceful
//! `ActorNotRunning` / `MailboxFull` the scenarios specify. Scenario (3) is a
//! different defect: `ctx.blocking_forward` → `AskRequest::blocking_forward`
//! (src/request/ask.rs:461) calls tokio `blocking_send`, which PANICS ("Cannot
//! block the current thread from within a runtime") in any async handler (every
//! kameo handler is async, driven on a runtime worker).
//!
//! ## Probe mechanism: the router's own `on_panic` hook (not the global hook)
//!
//! All three panics happen INSIDE the router's message handler. kameo's actor
//! loop catches the unwind (`kind.rs:176/181`) and turns it into
//! `ActorStopReason::Panicked(PanicError { reason: HandlerPanic, .. })`, which is
//! delivered to the actor's OWN `on_panic` hook (`kind.rs:401-414`). The
//! PROCESS-GLOBAL `set_actor_error_hook` is verified NOT to fire for a
//! `HandlerPanic` — `invoke_actor_error_hook` is only called when `on_stop`
//! itself panics (`spawn.rs:268-282`, `PanicReason::OnStop`). So the most
//! faithful in-process observer of these defects is a custom `on_panic` on the
//! router that records the `PanicError` message into a per-probe
//! `Arc<Mutex<Vec<String>>>`.
//!
//! Each probe drives the defect, then asserts `on_panic` fired with a message
//! matching the defect (the `.unwrap()`/`None` panic for 1 & 2; "Cannot block the
//! current thread" for 3). When a defect is FIXED, no panic fires → `on_panic` is
//! never called → the recorder stays empty → the probe FAILS (red-on-fix), at
//! which point the corresponding @bug scenario can be re-included in the green run.
//! A plain `#[should_panic]` test would NOT observe these (the panic never reaches
//! the test thread — it is caught actor-internally), which is why the `on_panic`
//! recorder is used.

#[path = "core_steps/message.rs"]
mod message;

use std::{
    ops::ControlFlow,
    sync::{Arc, Mutex},
    time::Duration,
};

use cucumber::World;
use kameo::{
    error::{ActorStopReason, Infallible, PanicError},
    mailbox,
    prelude::*,
    reply::ForwardedReply,
};
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
            // Standard predicate (identical to the actor_id/error runners): the
            // three forward-FAILURE-path scenarios are @bug-tagged in the feature
            // file (they document desired-but-absent behaviour the SUT panics on),
            // and the live defects are pinned by the probes below instead.
            |_, _, sc| !sc.tags.iter().any(|t| t.starts_with("bug")),
        )
        .await;
}

// ===========================================================================
// Red-on-fix probes for the three @bug forward-FAILURE scenarios.
//
// Each probe spawns a router whose custom `on_panic` records the PanicError
// message, drives the defect, and asserts the recorder captured a panic matching
// that defect. When a defect is FIXED, no panic fires, `on_panic` is never
// called, the recorder stays empty, and the probe FAILS (red-on-fix).
// ===========================================================================

/// Polls a recorder until it captures a message (bounded; no wall-clock
/// assertion), returning the captured messages.
async fn await_recorded(recorder: &Arc<Mutex<Vec<String>>>) -> Vec<String> {
    for _ in 0..400 {
        {
            let log = recorder.lock().unwrap();
            if !log.is_empty() {
                return log.clone();
            }
        }
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    Vec::new()
}

// --- Probe actors ----------------------------------------------------------

#[derive(Clone)]
struct ProbeTarget {
    log: Arc<Mutex<Vec<u64>>>,
}

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
        self.log.lock().unwrap().push(msg.0);
        msg.0
    }
}

/// Parks the handler (occupying a bounded-capacity-1 mailbox slot) until a shared
/// `watch` flips to `true` — used to drive the mailbox-full path deterministically.
struct ProbeHold(tokio::sync::watch::Receiver<bool>);

impl Message<ProbeHold> for ProbeTarget {
    type Reply = ();

    async fn handle(
        &mut self,
        msg: ProbeHold,
        _ctx: &mut Context<Self, Self::Reply>,
    ) -> Self::Reply {
        let mut rx = msg.0;
        while !*rx.borrow() {
            if rx.changed().await.is_err() {
                break;
            }
        }
    }
}

/// The router whose handler forwards to the target. Its custom `on_panic`
/// records each PanicError's message (the faithful in-process observer of the
/// forward-failure defects, which panic INSIDE the handler).
#[derive(Clone)]
struct ProbeRouter {
    target: ActorRef<ProbeTarget>,
    /// Records "<reason>: <message>" for each `on_panic` invocation.
    panics: Arc<Mutex<Vec<String>>>,
}

impl Actor for ProbeRouter {
    type Args = Self;
    type Error = Infallible;

    async fn on_start(state: Self::Args, _: ActorRef<Self>) -> Result<Self, Self::Error> {
        Ok(state)
    }

    async fn on_panic(
        &mut self,
        _actor_ref: WeakActorRef<Self>,
        err: PanicError,
    ) -> Result<ControlFlow<ActorStopReason>, Self::Error> {
        let msg = err
            .with_str(|s| s.to_string())
            .unwrap_or_else(|| format!("{err}"));
        self.panics
            .lock()
            .unwrap()
            .push(format!("{:?}: {msg}", err.reason()));
        // Default behaviour: stop on panic.
        Ok(ControlFlow::Break(ActorStopReason::Panicked(err)))
    }
}

/// `ctx.forward` (await) — defect 1 (error.rs:293) on a dead target.
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

/// `ctx.try_forward` — defect 2 (error.rs:305) on a full target.
struct ProbeTryForward(u64);

impl Message<ProbeTryForward> for ProbeRouter {
    type Reply = ForwardedReply<ProbeEcho, <ProbeTarget as Message<ProbeEcho>>::Reply>;

    async fn handle(
        &mut self,
        msg: ProbeTryForward,
        ctx: &mut Context<Self, Self::Reply>,
    ) -> Self::Reply {
        ctx.try_forward(&self.target, ProbeEcho(msg.0))
    }
}

/// `ctx.blocking_forward` — defect 3 (ask.rs:461): panics from any async handler.
struct ProbeBlockingForward(u64);

impl Message<ProbeBlockingForward> for ProbeRouter {
    type Reply = ForwardedReply<ProbeEcho, <ProbeTarget as Message<ProbeEcho>>::Reply>;

    async fn handle(
        &mut self,
        msg: ProbeBlockingForward,
        ctx: &mut Context<Self, Self::Reply>,
    ) -> Self::Reply {
        ctx.blocking_forward(&self.target, ProbeEcho(msg.0))
    }
}

fn spawn_probe_router(
    target: ActorRef<ProbeTarget>,
) -> (ActorRef<ProbeRouter>, Arc<Mutex<Vec<String>>>) {
    let panics: Arc<Mutex<Vec<String>>> = Arc::new(Mutex::new(Vec::new()));
    let router = ProbeRouter::spawn(ProbeRouter {
        target,
        panics: Arc::clone(&panics),
    });
    (router, panics)
}

/// Defect 1 (error.rs:293) — forwarding to a DEAD target. The router's handler
/// panics on `downcast_message::<(M, ReplySender)>().unwrap()` returning `None`,
/// which kameo's actor loop catches and routes to the router's `on_panic`. GREEN
/// today: `on_panic` records the unwrap/`None` panic. RED on fix: a graceful
/// `ActorNotRunning` conversion means NO panic → `on_panic` never fires → the
/// recorder is empty → the `assert!(!recorded.is_empty())` fails.
#[tokio::test(flavor = "multi_thread")]
async fn bug_forward_dead_target_panics_router() {
    let target = ProbeTarget::spawn(ProbeTarget {
        log: Arc::new(Mutex::new(Vec::new())),
    });
    target.wait_for_startup().await;
    let (router, panics) = spawn_probe_router(target.clone());
    router.wait_for_startup().await;

    target.stop_gracefully().await.unwrap();
    target.wait_for_shutdown().await;
    // Wait until the target is observably not running before forwarding.
    for _ in 0..200 {
        if target.ask(ProbeEcho(0)).await.is_err() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(5)).await;
    }

    // Drive the forward-to-dead-target; the router's handler will panic.
    let _ = router.ask(ProbeForward(5)).await;
    let recorded = await_recorded(&panics).await;

    assert!(
        !recorded.is_empty(),
        "defect error.rs:293 must panic the router on forward-to-dead-target \
         (on_panic never fired — has the conversion been fixed to ActorNotRunning?)"
    );
    assert!(
        recorded
            .iter()
            .any(|m| m.contains("HandlerPanic") && (m.contains("unwrap()") || m.contains("None"))),
        "expected a HandlerPanic from downcast_message().unwrap()==None, got {recorded:?}"
    );
}

/// Defect 2 (error.rs:305) — `try_forward` to a FULL bounded mailbox. Same root
/// cause on the Full arm: `downcast_message::<(M, ReplySender)>().unwrap()` is
/// `None` and the router's handler panics. GREEN today: `on_panic` records the
/// panic. RED on fix: a graceful `MailboxFull` means no panic → `on_panic` never
/// fires → empty recorder → assertion fails.
#[tokio::test(flavor = "multi_thread")]
async fn bug_try_forward_full_target_panics_router() {
    let target = ProbeTarget::spawn_with_mailbox(
        ProbeTarget {
            log: Arc::new(Mutex::new(Vec::new())),
        },
        mailbox::bounded(1),
    );
    target.wait_for_startup().await;

    // Fill the bounded-capacity-1 mailbox: first Hold is dequeued into the
    // (parked) handler, the second fills the one buffer slot — a third send is
    // MailboxFull. (Same arithmetic as the steps module's `setup_full_target`.)
    let (release_tx, release_rx) = tokio::sync::watch::channel(false);
    target
        .tell(ProbeHold(release_rx.clone()))
        .send()
        .await
        .expect("first hold enqueued and dequeued");
    tokio::time::sleep(Duration::from_millis(20)).await;
    target
        .tell(ProbeHold(release_rx))
        .try_send()
        .expect("second hold fills the buffer slot");

    let (router, panics) = spawn_probe_router(target.clone());
    router.wait_for_startup().await;

    // Drive try_forward to the full target; the router's handler will panic.
    let _ = router.ask(ProbeTryForward(61)).await;
    let recorded = await_recorded(&panics).await;

    // Release the parked handlers so the target can drain on drop.
    let _ = release_tx.send(true);

    assert!(
        !recorded.is_empty(),
        "defect error.rs:305 must panic the router on try_forward-to-full-target \
         (on_panic never fired — has the conversion been fixed to MailboxFull?)"
    );
    assert!(
        recorded
            .iter()
            .any(|m| m.contains("HandlerPanic") && (m.contains("unwrap()") || m.contains("None"))),
        "expected a HandlerPanic from downcast_message().unwrap()==None, got {recorded:?}"
    );
}

/// Defect 3 (ask.rs:461) — `ctx.blocking_forward` from an async handler. tokio's
/// `blocking_send` panics ("Cannot block the current thread from within a
/// runtime") because every kameo handler runs on a runtime worker; the panic is
/// caught and routed to the router's `on_panic`. GREEN today: `on_panic` records
/// the "Cannot block the current thread" panic. RED on fix: an API guard (or a
/// non-blocking re-spec) means no panic → `on_panic` never fires → empty recorder
/// → assertion fails.
#[tokio::test(flavor = "multi_thread")]
async fn bug_blocking_forward_from_handler_panics_router() {
    let target = ProbeTarget::spawn(ProbeTarget {
        log: Arc::new(Mutex::new(Vec::new())),
    });
    target.wait_for_startup().await;
    let (router, panics) = spawn_probe_router(target.clone());
    router.wait_for_startup().await;

    // Calling blocking_forward from the (async, runtime-driven) handler panics.
    let _ = router.ask(ProbeBlockingForward(71)).await;
    let recorded = await_recorded(&panics).await;

    assert!(
        !recorded.is_empty(),
        "defect ask.rs:461 must panic the router when blocking_forward is called \
         from an async handler (on_panic never fired — has the API been guarded?)"
    );
    assert!(
        recorded
            .iter()
            .any(|m| m.contains("Cannot block the current thread")),
        "expected the tokio blocking_send 'Cannot block the current thread' panic, \
         got {recorded:?}"
    );
}
