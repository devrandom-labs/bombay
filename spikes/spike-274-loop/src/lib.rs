//! Executable model for the framework-event plane (ADR-0025 pre-check):
//! a run-loop-owned deadline arm in a biased select, with the actor
//! declaring its deadline as a pure function of state (`next_deadline`)
//! and receiving expiry through a hook (`on_deadline`).
//!
//! This models the LOOP SHAPE only (kind.rs surgery de-risk). No bombay
//! dep — the properties under test are structural:
//!
//!   P1  arm ABOVE the mailbox arm: a due deadline fires promptly even
//!       against a saturated mailbox (and the counter-model — arm BELOW —
//!       starves, proving the ordering is forced, not stylistic).
//!   P2  `next_deadline() == None` disables the arm: no fire, no spin.
//!   P3  fires-once-per-value guard: a hook that leaves its deadline
//!       unchanged (even in the past) fires exactly once — no busy loop.
//!   P4  sliding deadline (receive-timeout emulation): every handled
//!       message defers the fire; idle for T fires at last_activity + T.
//!   P5  deadline never fires mid-handler (run-to-completion): expiry
//!       during a long handler is observed only after the handler returns.

use tokio::sync::mpsc;
use tokio::time::{Instant, sleep_until};

/// The model actor: state + the two plane hooks.
pub trait ModelActor {
    fn handle(&mut self, msg: u64) -> impl Future<Output = ()> + Send;
    /// Declarative deadline — a pure function of current state.
    fn next_deadline(&self) -> Option<Instant>;
    /// Expiry delivery. Returns `false` to stop the loop.
    fn on_deadline(&mut self) -> impl Future<Output = bool> + Send;
}

/// Where the deadline arm sits relative to the mailbox arm.
#[derive(Clone, Copy, PartialEq, Eq)]
pub enum ArmOrder {
    /// The designed placement: deadline served before queued messages.
    AboveMailbox,
    /// The counter-model: biased select starves this under saturation.
    BelowMailbox,
}

/// The modeled run loop. Mirrors `kind.rs` shape: biased select, guarded
/// arms, one handler step at a time (run-to-completion).
///
/// The fires-once guard: after firing for deadline value `d`, the arm is
/// disabled until `next_deadline()` reports a value different from `d`
/// (`last_fired`). This is what makes a hook that leaves its deadline
/// unchanged spin-proof BY CONSTRUCTION.
pub async fn run_loop<A: ModelActor>(actor: &mut A, rx: &mut mpsc::Receiver<u64>, order: ArmOrder) {
    let mut last_fired: Option<Instant> = None;
    loop {
        let deadline = actor.next_deadline();
        // The arm is armed only when a deadline exists AND it is not the
        // one already fired (P3).
        let armed = deadline.is_some() && deadline != last_fired;
        let due = deadline.unwrap_or_else(Instant::now);

        match order {
            ArmOrder::AboveMailbox => {
                tokio::select! {
                    biased;
                    () = sleep_until(due), if armed => {
                        last_fired = deadline;
                        if !actor.on_deadline().await {
                            return;
                        }
                    }
                    msg = rx.recv() => {
                        let Some(msg) = msg else { return };
                        actor.handle(msg).await;
                    }
                }
            }
            ArmOrder::BelowMailbox => {
                tokio::select! {
                    biased;
                    msg = rx.recv() => {
                        let Some(msg) = msg else { return };
                        actor.handle(msg).await;
                    }
                    () = sleep_until(due), if armed => {
                        last_fired = deadline;
                        if !actor.on_deadline().await {
                            return;
                        }
                    }
                }
            }
        }
    }
}
