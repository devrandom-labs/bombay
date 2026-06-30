//! Cucumber runner for core/request_ask.feature — the example scenarios for the
//! `src/request/ask.rs` SUT (the `AskRequest` builder: mailbox/reply timeouts,
//! send/try_send/blocking_send, enqueue, forward/try_forward/blocking_forward),
//! driven against REAL SPAWNED ACTORS.
//!
//! Shares the `AskWorld` + step definitions with `core_request_ask_props_bdd.rs`
//! (the @property/@model laws). Standard `#[tokio::test(flavor =
//! "multi_thread")]` libtest function (no `harness = false`) so nextest's
//! `--list` enumerates it; built only with the `testing` feature (see
//! `required-features` in Cargo.toml — the forward scenarios use the
//! `bombay::reply::testing::reply_channel` constructor gated by that feature).
//!
//! `.max_concurrent_scenarios(1)`: several @timing scenarios stand up a
//! dedicated paused current-thread runtime on a blocking thread, and the
//! full-mailbox / kill-mid-flight scenarios park real handlers; serializing
//! scenarios keeps the bounded settle/poll deterministic and avoids starving the
//! blocking-thread pool. request_ask.feature has TWO @bug forward-FAILURE
//! scenarios (@bug:error.rs:293 forward-to-stopped, @bug:error.rs:305
//! try_forward-to-full); the standard `bug*`-tag filter drops them from the
//! green run and they are pinned by the red-on-fix probes below.

#[path = "core_steps/request_ask.rs"]
mod request_ask;

use std::time::Duration;

use bombay::{
    error::{Infallible, SendError},
    mailbox,
    prelude::*,
    reply::testing::reply_channel,
};
use cucumber::World;
use request_ask::AskWorld;
use tokio::sync::watch;

#[tokio::test(flavor = "multi_thread")]
async fn request_ask_features() {
    AskWorld::cucumber()
        .max_concurrent_scenarios(1)
        .fail_on_skipped()
        .with_default_cli()
        .filter_run_and_exit(
            concat!(
                env!("CARGO_MANIFEST_DIR"),
                "/tests/features/core/request_ask.feature"
            ),
            // Standard predicate: the two forward-FAILURE scenarios are
            // @bug-tagged (they document desired-but-absent behaviour the SUT
            // PANICS on); the live defects are pinned by the probes below.
            |_, _, sc| !sc.tags.iter().any(|t| t.starts_with("bug")),
        )
        .await;
}

// ===========================================================================
// Red-on-fix probes for the two @bug forward-FAILURE scenarios.
//
// `AskRequest::forward` / `try_forward` build a signal carrying the BARE message
// (ask.rs:250 / :289), but on a send failure the `?` converts the inner
// `{Try}SendError<Signal>` into `SendError<(M, ReplySender), _>` via the `From`
// impls at error.rs:293 / :305, which call
// `signal.downcast_message::<(M, ReplySender)>().unwrap()`. The bare-message
// signal does NOT downcast to the tuple, so `.unwrap()` is `None.unwrap()` and
// PANICS on the CALLER's thread (these calls are NOT inside an actor handler, so
// the panic propagates to the caller, not to an `on_panic` hook). Each probe
// drives the defect on its own thread and asserts the thread PANICKED with the
// unwrap/None message. When a defect is FIXED (graceful ActorNotRunning /
// MailboxFull carrying the channel), no panic fires and the probe FAILS
// (red-on-fix), at which point the @bug scenario can rejoin the green run.
// ===========================================================================

#[derive(Clone)]
struct Probe;

impl Actor for Probe {
    type Args = Self;
    type Error = Infallible;

    async fn on_start(state: Self::Args, _: ActorRef<Self>) -> Result<Self, Self::Error> {
        Ok(state)
    }
}

struct ProbeMsg;

impl Message<ProbeMsg> for Probe {
    type Reply = bool;

    async fn handle(
        &mut self,
        _msg: ProbeMsg,
        _ctx: &mut Context<Self, Self::Reply>,
    ) -> Self::Reply {
        true
    }
}

/// Parks the handler on a `watch` gate so a bounded(1) mailbox stays full.
struct ProbeHold(watch::Receiver<bool>);

impl Message<ProbeHold> for Probe {
    type Reply = bool;

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
        true
    }
}

/// Defect error.rs:293 — `forward` to a STOPPED actor. The conversion's
/// `downcast_message::<(M, ReplySender)>().unwrap()` is `None.unwrap()` → PANIC.
/// GREEN today: the spawned forward task panics. RED on fix: a graceful
/// `ActorNotRunning` means no panic → `is_panic()` is false → the assertion fails.
#[tokio::test(flavor = "multi_thread")]
async fn bug_forward_stopped_panics() {
    let actor = Probe::spawn(Probe);
    actor.wait_for_startup().await;
    actor.stop_gracefully().await.unwrap();
    actor.wait_for_shutdown().await;

    let (sender, _rx) = reply_channel::<bool>();
    let join = tokio::spawn(async move { actor.ask(ProbeMsg).forward(sender).await.map(|_| ()) });
    let outcome = join.await;
    assert!(
        outcome.is_err() && outcome.unwrap_err().is_panic(),
        "defect error.rs:293 must PANIC forward-to-stopped \
         (no panic — has the conversion been fixed to ActorNotRunning?)"
    );
}

/// Defect error.rs:305 — `try_forward` to a FULL bounded mailbox. Same root cause
/// on the Full arm → `None.unwrap()` PANIC. GREEN today: `try_forward` panics on
/// the caller thread (caught here). RED on fix: a graceful `MailboxFull` means no
/// panic → the assertion fails.
#[tokio::test(flavor = "multi_thread")]
async fn bug_try_forward_full_panics() {
    let actor = Probe::spawn_with_mailbox(Probe, mailbox::bounded(1));
    actor.wait_for_startup().await;

    // Fill the bounded(1) mailbox: first Hold dequeued into the parked handler
    // (freeing the slot), second occupies the one buffer slot.
    let (release_tx, release_rx) = watch::channel(false);
    actor
        .tell(ProbeHold(release_rx.clone()))
        .send()
        .await
        .expect("first hold enqueued and dequeued");
    tokio::time::sleep(Duration::from_millis(20)).await;
    actor
        .tell(ProbeHold(release_rx))
        .try_send()
        .expect("second hold fills the buffer slot");
    // Confirm observably full.
    for _ in 0..200 {
        if matches!(
            actor.ask(ProbeMsg).try_send().await,
            Err(SendError::MailboxFull(_))
        ) {
            break;
        }
        tokio::time::sleep(Duration::from_millis(5)).await;
    }

    let (sender, _rx) = reply_channel::<bool>();
    let probe_actor = actor.clone();
    let panicked = tokio::task::spawn_blocking(move || {
        std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let _ = probe_actor.ask(ProbeMsg).try_forward(sender);
        }))
        .is_err()
    })
    .await
    .expect("probe thread join");

    let _ = release_tx.send(true);
    actor.kill();

    assert!(
        panicked,
        "defect error.rs:305 must PANIC try_forward-to-full \
         (no panic — has the conversion been fixed to MailboxFull?)"
    );
}
