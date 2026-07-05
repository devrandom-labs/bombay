//! Spawn entry points (card #116): prepare an actor, then run it in the current
//! task or a background tokio task. Kill is uniform across both via
//! `futures::Abortable` wrapping the whole lifecycle (so a hard kill skips
//! `on_stop`).

use std::{
    fmt,
    panic::AssertUnwindSafe,
    sync::atomic::{AtomicU64, Ordering},
};

use futures::{
    FutureExt,
    stream::{AbortHandle, AbortRegistration, Abortable},
};
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

use crate::{
    actor::{Actor, ActorRef, kind::run_message_loop},
    error::{ActorStopReason, PanicError, PanicReason},
    mailbox::{ActorId, Capacity, Mailbox, MailboxReceiver},
};

/// The default mailbox capacity for the ergonomic spawn path (4 cache-lines'
/// worth of slots is a sane starting point; tune with `spawn_with_capacity`).
pub const DEFAULT_MAILBOX_CAPACITY: usize = 64;

/// The default capacity as a validated [`Capacity`]. Infallible for the fixed
/// constant 64 (in `1..=Capacity::MAX`); the `expect` is proven by
/// `default_capacity_is_64` and can never trip at runtime.
pub(super) fn default_capacity() -> Capacity {
    #[expect(
        clippy::expect_used,
        reason = "DEFAULT_MAILBOX_CAPACITY (64) is a compile-time-valid capacity; \
                  the conversion is infallible and pinned by a unit test"
    )]
    Capacity::try_from(DEFAULT_MAILBOX_CAPACITY).expect("64 is a valid capacity")
}

/// Monotonic scaffold id source (#121 replaces this with the AID).
static NEXT_ACTOR_ID: AtomicU64 = AtomicU64::new(1);

fn next_actor_id() -> ActorId {
    // Relaxed is sufficient: correctness needs only that each `fetch_add` returns
    // a distinct value. Uniqueness is a property of atomic increment alone and
    // requires no happens-before with any other memory (CLAUDE rule #5).
    ActorId::new(NEXT_ACTOR_ID.fetch_add(1, Ordering::Relaxed))
}

/// The total outcome of running an actor to completion in the current task.
pub enum RunResult<A: Actor> {
    /// Ran and stopped. If `reason` is [`ActorStopReason::Panicked`], `actor` is
    /// **poisoned** (torn state): resource-release only, never read domain fields.
    Stopped {
        /// The final actor state.
        actor: A,
        /// Why it stopped.
        reason: ActorStopReason,
    },
    /// `on_start` returned `Err` or panicked — no actor was produced.
    StartupFailed(PanicError),
    /// Hard-killed via [`ActorRef::kill`] — `on_stop` was skipped, state dropped.
    Killed,
}

impl<A: Actor> fmt::Debug for RunResult<A> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Stopped { reason, .. } => f
                .debug_struct("Stopped")
                .field("reason", reason)
                .finish_non_exhaustive(),
            Self::StartupFailed(err) => f.debug_tuple("StartupFailed").field(err).finish(),
            Self::Killed => f.write_str("Killed"),
        }
    }
}

/// An actor initialized and ready to run, with its [`ActorRef`] available before
/// the loop starts (so callers can pre-send messages).
#[must_use = "a prepared actor must be run or spawned"]
pub struct PreparedActor<A: Actor> {
    actor_ref: ActorRef<A>,
    mailbox_rx: MailboxReceiver<A>,
    abort_registration: AbortRegistration,
}

impl<A: Actor> fmt::Debug for PreparedActor<A> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("PreparedActor")
            .field("actor_ref", &self.actor_ref)
            .finish_non_exhaustive()
    }
}

impl<A: Actor> PreparedActor<A> {
    /// Prepares an actor with a mailbox of the given `capacity`.
    pub fn new(capacity: Capacity) -> Self {
        let (mailbox_tx, mailbox_rx) = Mailbox::<A>::bounded(capacity);
        let (abort_handle, abort_registration) = AbortHandle::new_pair();
        let actor_ref = ActorRef::new(
            next_actor_id(),
            mailbox_tx,
            CancellationToken::new(),
            abort_handle,
        );
        Self {
            actor_ref,
            mailbox_rx,
            abort_registration,
        }
    }

    /// The handle to the actor, usable before the loop starts.
    #[must_use]
    pub const fn actor_ref(&self) -> &ActorRef<A> {
        &self.actor_ref
    }

    /// Runs the actor in the current task until it stops. Aborts (hard kill)
    /// short-circuit to [`RunResult::Killed`], skipping `on_stop`.
    pub async fn run(self, args: A::Args) -> RunResult<A> {
        let lifecycle = run_lifecycle(args, self.actor_ref, self.mailbox_rx);
        Abortable::new(lifecycle, self.abort_registration)
            .await
            .unwrap_or(RunResult::Killed)
    }

    /// Spawns the actor in a background tokio task.
    pub fn spawn(self, args: A::Args) -> JoinHandle<RunResult<A>> {
        tokio::spawn(self.run(args))
    }
}

/// `on_start` (catch) → message loop → `on_stop` (catch; Err logged, reason
/// preserved). Returns `StartupFailed` if `on_start` fails, else `Stopped`.
async fn run_lifecycle<A: Actor>(
    args: A::Args,
    actor_ref: ActorRef<A>,
    mut mailbox_rx: MailboxReceiver<A>,
) -> RunResult<A> {
    let started = AssertUnwindSafe(A::on_start(args, actor_ref.clone()))
        .catch_unwind()
        .await;
    let mut state = match started {
        Ok(Ok(actor)) => actor,
        Ok(Err(err)) => {
            return RunResult::StartupFailed(PanicError::new(Box::new(err), PanicReason::OnStart));
        }
        Err(payload) => {
            return RunResult::StartupFailed(PanicError::from_panic_any(
                payload,
                PanicReason::OnStart,
            ));
        }
    };

    let reason = run_message_loop(&mut state, &actor_ref, &mut mailbox_rx).await;

    let weak = actor_ref.downgrade();
    let stop_result = AssertUnwindSafe(state.on_stop(weak, reason.clone()))
        .catch_unwind()
        .await;
    log_on_stop_outcome::<A>(&reason, stop_result);

    RunResult::Stopped {
        actor: state,
        reason,
    }
}

/// Logs a failed/panicked `on_stop` without altering the preserved stop reason
/// and without unwrapping (a double-panic on the shutdown path can abort the
/// process — std `Drop` docs).
#[expect(
    clippy::print_stderr,
    reason = "diagnostic-only surface until the tracing feature lands (#66); \
              an on_stop failure must be surfaced, never swallowed"
)]
fn log_on_stop_outcome<A: Actor>(
    reason: &ActorStopReason,
    stop_result: Result<Result<(), A::Error>, Box<dyn std::any::Any + Send>>,
) {
    match stop_result {
        Ok(Ok(())) => {}
        Ok(Err(err)) => {
            eprintln!(
                "[bombay] on_stop for {} returned an error: {err:?} (stop reason: {reason})",
                A::name()
            );
        }
        Err(_payload) => {
            eprintln!(
                "[bombay] on_stop for {} panicked (stop reason: {reason})",
                A::name()
            );
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::{
        Arc,
        atomic::{AtomicU32, Ordering},
    };

    use super::DEFAULT_MAILBOX_CAPACITY;
    use crate::{
        actor::{ActorRef, PreparedActor, RunResult, WeakActorRef},
        error::ActorStopReason,
        mailbox::{Capacity, Mailboxed, Signal},
        message::Msg,
    };

    /// Counts handled messages and records whether `on_stop` ran, via shared
    /// atomics the test inspects — the SUT is the real loop, not a reimpl.
    struct Counter {
        handled: Arc<AtomicU32>,
        stopped: Arc<AtomicU32>,
    }
    struct Tick;
    impl Msg for Tick {}
    impl Mailboxed for Counter {
        type Msg = Tick;
    }
    impl crate::actor::Actor for Counter {
        type Args = (Arc<AtomicU32>, Arc<AtomicU32>);
        type Error = core::convert::Infallible;

        async fn on_start(
            (handled, stopped): Self::Args,
            _: ActorRef<Self>,
        ) -> Result<Self, Self::Error> {
            Ok(Self { handled, stopped })
        }

        async fn handle(
            &mut self,
            _: Tick,
            _: ActorRef<Self>,
            _: &mut bool,
        ) -> Result<(), Self::Error> {
            self.handled.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }

        async fn on_stop(
            &mut self,
            _: WeakActorRef<Self>,
            _: ActorStopReason,
        ) -> Result<(), Self::Error> {
            self.stopped.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }
    }

    fn cap(n: usize) -> Capacity {
        Capacity::try_from(n).expect("valid test capacity")
    }

    /// Sequence: two messages then a `Stop` — both are handled (FIFO, before the
    /// stop), `on_stop` runs exactly once, and the outcome is a normal stop.
    #[tokio::test]
    async fn handles_queued_messages_then_stops_normally() {
        let handled = Arc::new(AtomicU32::new(0));
        let stopped = Arc::new(AtomicU32::new(0));

        let prepared = PreparedActor::<Counter>::new(cap(8));
        let actor_ref = prepared.actor_ref().clone();
        actor_ref
            .mailbox_sender()
            .send(Signal::Message(Tick))
            .await
            .expect("send 1");
        actor_ref
            .mailbox_sender()
            .send(Signal::Message(Tick))
            .await
            .expect("send 2");
        actor_ref
            .mailbox_sender()
            .send(Signal::Stop)
            .await
            .expect("stop");

        let outcome = prepared
            .run((Arc::clone(&handled), Arc::clone(&stopped)))
            .await;

        assert_eq!(
            handled.load(Ordering::SeqCst),
            2,
            "both messages handled before stop"
        );
        assert_eq!(stopped.load(Ordering::SeqCst), 1, "on_stop ran once");
        assert!(
            matches!(
                outcome,
                RunResult::Stopped {
                    reason: ActorStopReason::Normal,
                    ..
                }
            ),
            "clean normal stop",
        );
    }

    /// Lifecycle: `stop()` (out-of-band cancel) while a handler is mid-flight lets
    /// that handler finish, then stops and runs `on_stop`. The queued-behind message
    /// is abandoned (finish-current-then-stop, no drain).
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn cancel_finishes_in_flight_then_stops() {
        use tokio::sync::oneshot;

        struct Slow {
            entered: Option<oneshot::Sender<()>>,
            release: Option<oneshot::Receiver<()>>,
            handled: Arc<AtomicU32>,
        }
        struct Work;
        impl Msg for Work {}
        impl Mailboxed for Slow {
            type Msg = Work;
        }
        impl crate::actor::Actor for Slow {
            type Args = (oneshot::Sender<()>, oneshot::Receiver<()>, Arc<AtomicU32>);
            type Error = core::convert::Infallible;
            async fn on_start(
                (entered, release, handled): Self::Args,
                _: ActorRef<Self>,
            ) -> Result<Self, Self::Error> {
                Ok(Self {
                    entered: Some(entered),
                    release: Some(release),
                    handled,
                })
            }
            async fn handle(
                &mut self,
                _: Work,
                _: ActorRef<Self>,
                _: &mut bool,
            ) -> Result<(), Self::Error> {
                if let Some(entered) = self.entered.take() {
                    let _ = entered.send(());
                }
                if let Some(release) = self.release.take() {
                    let _ = release.await;
                }
                self.handled.fetch_add(1, Ordering::SeqCst);
                Ok(())
            }
        }

        let (entered_tx, entered_rx) = oneshot::channel();
        let (release_tx, release_rx) = oneshot::channel();
        let handled = Arc::new(AtomicU32::new(0));

        let prepared = PreparedActor::<Slow>::new(cap(8));
        let actor_ref = prepared.actor_ref().clone();
        // Two messages: the first blocks until released; the second must be abandoned.
        actor_ref
            .mailbox_sender()
            .send(Signal::Message(Work))
            .await
            .expect("send 1");
        actor_ref
            .mailbox_sender()
            .send(Signal::Message(Work))
            .await
            .expect("send 2");

        let run = tokio::spawn(prepared.run((entered_tx, release_rx, Arc::clone(&handled))));

        entered_rx.await.expect("handler entered"); // handler #1 is mid-flight
        actor_ref.stop(); // cancel while in-flight
        release_tx.send(()).expect("release handler"); // let handler #1 finish

        let outcome = run.await.expect("run task");
        assert_eq!(
            handled.load(Ordering::SeqCst),
            1,
            "only the in-flight message finished; the queued one was abandoned"
        );
        assert!(matches!(
            outcome,
            RunResult::Stopped {
                reason: ActorStopReason::Normal,
                ..
            }
        ));
    }

    /// Sequence: a handler that sets `*stop = true` stops the actor cleanly after it
    /// returns `Ok` — a following queued message is never handled.
    #[tokio::test]
    async fn stop_flag_stops_after_current_handler() {
        struct Once {
            handled: Arc<AtomicU32>,
        }
        struct Go;
        impl Msg for Go {}
        impl Mailboxed for Once {
            type Msg = Go;
        }
        impl crate::actor::Actor for Once {
            type Args = Arc<AtomicU32>;
            type Error = core::convert::Infallible;
            async fn on_start(handled: Self::Args, _: ActorRef<Self>) -> Result<Self, Self::Error> {
                Ok(Self { handled })
            }
            async fn handle(
                &mut self,
                _: Go,
                _: ActorRef<Self>,
                stop: &mut bool,
            ) -> Result<(), Self::Error> {
                self.handled.fetch_add(1, Ordering::SeqCst);
                *stop = true;
                Ok(())
            }
        }

        let handled = Arc::new(AtomicU32::new(0));
        let prepared = PreparedActor::<Once>::new(cap(8));
        let actor_ref = prepared.actor_ref().clone();
        actor_ref
            .mailbox_sender()
            .send(Signal::Message(Go))
            .await
            .expect("send 1");
        actor_ref
            .mailbox_sender()
            .send(Signal::Message(Go))
            .await
            .expect("send 2");

        let outcome = prepared.run(Arc::clone(&handled)).await;
        assert_eq!(
            handled.load(Ordering::SeqCst),
            1,
            "stopped after the first handler; second never ran"
        );
        assert!(matches!(
            outcome,
            RunResult::Stopped {
                reason: ActorStopReason::Normal,
                ..
            }
        ));
    }

    /// Sequence (no startup buffer): messages that arrive while `on_start` is still
    /// running wait in the bounded mailbox and are handled *after* start, in FIFO
    /// order — the ordering guarantee comes from the flume channel, not a buffer.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn messages_during_on_start_are_handled_after_in_order() {
        use std::sync::Mutex;
        use tokio::sync::oneshot;

        struct Recorder {
            seen: Arc<Mutex<Vec<u32>>>,
        }
        struct N(u32);
        impl Msg for N {}
        impl Mailboxed for Recorder {
            type Msg = N;
        }
        impl crate::actor::Actor for Recorder {
            type Args = (oneshot::Receiver<()>, Arc<Mutex<Vec<u32>>>);
            type Error = core::convert::Infallible;
            async fn on_start(
                (gate, seen): Self::Args,
                _: ActorRef<Self>,
            ) -> Result<Self, Self::Error> {
                let _ = gate.await; // block startup until the test has enqueued messages
                Ok(Self { seen })
            }
            async fn handle(
                &mut self,
                N(n): N,
                _: ActorRef<Self>,
                stop: &mut bool,
            ) -> Result<(), Self::Error> {
                self.seen.lock().expect("lock").push(n);
                if n == 2 {
                    *stop = true;
                }
                Ok(())
            }
        }

        let (gate_tx, gate_rx) = oneshot::channel();
        let seen = Arc::new(Mutex::new(Vec::new()));

        let prepared = PreparedActor::<Recorder>::new(cap(8));
        let actor_ref = prepared.actor_ref().clone();
        let run = tokio::spawn(prepared.run((gate_rx, Arc::clone(&seen))));

        // Enqueue BEFORE releasing on_start — these must be buffered by the mailbox.
        actor_ref
            .mailbox_sender()
            .send(Signal::Message(N(0)))
            .await
            .expect("send 0");
        actor_ref
            .mailbox_sender()
            .send(Signal::Message(N(1)))
            .await
            .expect("send 1");
        actor_ref
            .mailbox_sender()
            .send(Signal::Message(N(2)))
            .await
            .expect("send 2");
        gate_tx.send(()).expect("release on_start");

        run.await.expect("run task");
        assert_eq!(
            *seen.lock().expect("lock"),
            vec![0, 1, 2],
            "handled after start, in FIFO order"
        );
    }

    /// Lifecycle: `on_start` returning `Err` produces `StartupFailed` (no actor, no
    /// message ever handled) — tagged as an `OnStart`-phase panic reason.
    #[tokio::test]
    async fn on_start_error_yields_startup_failed() {
        #[derive(Debug)]
        struct Boom;
        struct NeverStarts;
        struct Never;
        impl Msg for Never {}
        impl Mailboxed for NeverStarts {
            type Msg = Never;
        }
        impl crate::actor::Actor for NeverStarts {
            type Args = ();
            type Error = Boom;
            async fn on_start(_: (), _: ActorRef<Self>) -> Result<Self, Self::Error> {
                Err(Boom)
            }
            async fn handle(
                &mut self,
                _: Never,
                _: ActorRef<Self>,
                _: &mut bool,
            ) -> Result<(), Self::Error> {
                Ok(())
            }
        }

        let outcome = PreparedActor::<NeverStarts>::new(cap(4)).run(()).await;
        let RunResult::StartupFailed(err) = outcome else {
            panic!("expected StartupFailed, got {outcome:?}");
        };
        assert_eq!(err.reason(), crate::error::PanicReason::OnStart);
    }

    /// Defensive: a panic in `on_start` is CAUGHT (not a process abort) and becomes
    /// `StartupFailed` with the `OnStart` reason and the recoverable message.
    ///
    /// This is the card's `panic = "unwind"` pin: under `panic = "abort"` the
    /// process aborts here instead, and the test cannot pass.
    #[tokio::test]
    async fn on_start_panic_is_caught_as_startup_failed() {
        struct PanicsOnStart;
        struct Never;
        impl Msg for Never {}
        impl Mailboxed for PanicsOnStart {
            type Msg = Never;
        }
        impl crate::actor::Actor for PanicsOnStart {
            type Args = ();
            type Error = core::convert::Infallible;
            async fn on_start(_: (), _: ActorRef<Self>) -> Result<Self, Self::Error> {
                panic!("startup boom")
            }
            async fn handle(
                &mut self,
                _: Never,
                _: ActorRef<Self>,
                _: &mut bool,
            ) -> Result<(), Self::Error> {
                Ok(())
            }
        }

        let outcome = PreparedActor::<PanicsOnStart>::new(cap(4)).run(()).await;
        let RunResult::StartupFailed(err) = outcome else {
            panic!("expected StartupFailed, got {outcome:?}");
        };
        assert_eq!(err.reason(), crate::error::PanicReason::OnStart);
        assert_eq!(
            err.with_str(str::to_owned),
            Some(String::from("startup boom"))
        );
    }

    /// A handler that panics mid-mutation, with an `on_stop` spy that records the
    /// reason it received and whether it observed torn state. Shared across the
    /// three panic guarantees below.
    mod panic_probe {
        use super::*;
        use std::sync::Mutex;

        pub(super) struct Torn {
            pub(super) counter: u32,
            pub(super) stop_reason: Arc<Mutex<Option<ActorStopReason>>>,
            pub(super) counter_at_stop: Arc<Mutex<Option<u32>>>,
        }
        pub(super) struct Explode;
        impl Msg for Explode {}
        impl Mailboxed for Torn {
            type Msg = Explode;
        }
        impl crate::actor::Actor for Torn {
            type Args = (Arc<Mutex<Option<ActorStopReason>>>, Arc<Mutex<Option<u32>>>);
            type Error = core::convert::Infallible;
            async fn on_start(
                (stop_reason, counter_at_stop): Self::Args,
                _: ActorRef<Self>,
            ) -> Result<Self, Self::Error> {
                Ok(Self {
                    counter: 0,
                    stop_reason,
                    counter_at_stop,
                })
            }
            async fn handle(
                &mut self,
                _: Explode,
                _: ActorRef<Self>,
                _: &mut bool,
            ) -> Result<(), Self::Error> {
                self.counter = 99; // torn write BEFORE the panic
                panic!("handler boom");
            }
            async fn on_stop(
                &mut self,
                _: WeakActorRef<Self>,
                reason: ActorStopReason,
            ) -> Result<(), Self::Error> {
                // Records the reason and the poisoned field value — a real on_stop
                // must NOT persist `self.counter` (torn); this spy only records it so
                // the test can assert the loop DID run on_stop with the torn state
                // present (the contract is "don't flush", enforced by review + this
                // documented probe).
                *self.stop_reason.lock().expect("lock") = Some(reason);
                *self.counter_at_stop.lock().expect("lock") = Some(self.counter);
                Ok(())
            }
        }
    }

    /// `@bug` Lifecycle: after a handler panic, `on_stop` STILL runs and receives
    /// `ActorStopReason::Panicked` (OTP `terminate` precedent). Fails if the loop
    /// skips `on_stop` on the panic path.
    #[tokio::test]
    async fn on_stop_runs_after_panic_with_panicked_reason() {
        use panic_probe::*;
        use std::sync::Mutex;

        let stop_reason: Arc<Mutex<Option<ActorStopReason>>> = Arc::new(Mutex::new(None));
        let counter_at_stop = Arc::new(Mutex::new(None));

        let prepared = PreparedActor::<Torn>::new(cap(4));
        let actor_ref = prepared.actor_ref().clone();
        actor_ref
            .mailbox_sender()
            .send(Signal::Message(Explode))
            .await
            .expect("send");

        let outcome = prepared
            .run((Arc::clone(&stop_reason), Arc::clone(&counter_at_stop)))
            .await;

        assert!(
            matches!(
                &outcome,
                RunResult::Stopped {
                    reason: ActorStopReason::Panicked(_),
                    ..
                }
            ),
            "panic → Stopped with Panicked, got {outcome:?}",
        );
        let recorded = stop_reason.lock().expect("lock").clone();
        assert!(
            matches!(recorded, Some(ActorStopReason::Panicked(_))),
            "on_stop ran and saw Panicked, got {recorded:?}",
        );
    }

    /// `@bug` Defensive (poison contract): the field mutated just before the panic
    /// (`counter = 99`) IS still visible to `on_stop` (proving the state is torn, not
    /// rolled back) — which is exactly why a real `on_stop` must NOT flush it. This
    /// pins that the loop surfaces torn state to `on_stop` rather than silently
    /// discarding before cleanup, so the "don't flush" contract is meaningful.
    #[tokio::test]
    async fn on_stop_after_panic_observes_torn_state() {
        use panic_probe::*;
        use std::sync::Mutex;

        let stop_reason = Arc::new(Mutex::new(None));
        let counter_at_stop: Arc<Mutex<Option<u32>>> = Arc::new(Mutex::new(None));

        let prepared = PreparedActor::<Torn>::new(cap(4));
        let actor_ref = prepared.actor_ref().clone();
        actor_ref
            .mailbox_sender()
            .send(Signal::Message(Explode))
            .await
            .expect("send");
        let _ = prepared
            .run((Arc::clone(&stop_reason), Arc::clone(&counter_at_stop)))
            .await;

        assert_eq!(
            *counter_at_stop.lock().expect("lock"),
            Some(99),
            "on_stop sees the torn (pre-panic-mutated) field — hence must not flush it",
        );
    }

    /// `@bug` Lifecycle: once a handler panic stops the actor, its mailbox receiver
    /// is dropped, so a later `send` fails (the actor is gone). Fails if teardown
    /// leaves the receiver alive on the panic path.
    #[tokio::test]
    async fn send_after_handler_panic_fails() {
        struct Bomb;
        struct Trigger;
        impl Msg for Trigger {}
        impl Mailboxed for Bomb {
            type Msg = Trigger;
        }
        impl crate::actor::Actor for Bomb {
            type Args = ();
            type Error = core::convert::Infallible;
            async fn on_start(_: (), _: ActorRef<Self>) -> Result<Self, Self::Error> {
                Ok(Bomb)
            }
            async fn handle(
                &mut self,
                _: Trigger,
                _: ActorRef<Self>,
                _: &mut bool,
            ) -> Result<(), Self::Error> {
                panic!("boom")
            }
        }

        let prepared = PreparedActor::<Bomb>::new(cap(4));
        let actor_ref = prepared.actor_ref().clone();
        let handle = prepared.spawn(());
        actor_ref
            .mailbox_sender()
            .send(Signal::Message(Trigger))
            .await
            .expect("send trigger");

        let outcome = handle.await.expect("run task");
        assert!(matches!(
            outcome,
            RunResult::Stopped {
                reason: ActorStopReason::Panicked(_),
                ..
            }
        ));

        let resend = actor_ref
            .mailbox_sender()
            .send(Signal::Message(Trigger))
            .await;
        assert!(
            resend.is_err(),
            "the actor's mailbox is closed after the panic-stop"
        );
    }

    /// Lifecycle: a handler that RETURNS `Err` (not a panic) is a controlled crash —
    /// it stops the actor with `Panicked(HandlerPanic)` and runs `on_stop`. This is
    /// the only test that exercises the `Ok(Err(_))` arm of the loop's dispatch.
    #[tokio::test]
    async fn handle_returning_err_stops_as_panicked() {
        #[derive(Debug)]
        struct Nope;
        struct Failer;
        struct Do;
        impl Msg for Do {}
        impl Mailboxed for Failer {
            type Msg = Do;
        }
        impl crate::actor::Actor for Failer {
            type Args = ();
            type Error = Nope;
            async fn on_start(_: (), _: ActorRef<Self>) -> Result<Self, Self::Error> {
                Ok(Failer)
            }
            async fn handle(
                &mut self,
                _: Do,
                _: ActorRef<Self>,
                _: &mut bool,
            ) -> Result<(), Self::Error> {
                Err(Nope)
            }
        }

        let prepared = PreparedActor::<Failer>::new(cap(4));
        let actor_ref = prepared.actor_ref().clone();
        actor_ref
            .mailbox_sender()
            .send(Signal::Message(Do))
            .await
            .expect("send");
        let outcome = prepared.run(()).await;

        let RunResult::Stopped {
            reason: ActorStopReason::Panicked(err),
            ..
        } = outcome
        else {
            panic!("expected Stopped/Panicked, got {outcome:?}");
        };
        assert_eq!(err.reason(), crate::error::PanicReason::HandlerPanic);
    }

    /// Lifecycle: `kill()` while a handler is mid-flight aborts the task at its next
    /// await point — the handler never completes, `on_stop` does NOT run, and the
    /// outcome is `Killed`.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn kill_skips_on_stop_and_drops_in_flight() {
        use tokio::sync::oneshot;

        struct Blocker {
            entered: Option<oneshot::Sender<()>>,
            finished: Arc<AtomicU32>,
            stopped: Arc<AtomicU32>,
        }
        struct Block;
        impl Msg for Block {}
        impl Mailboxed for Blocker {
            type Msg = Block;
        }
        impl crate::actor::Actor for Blocker {
            type Args = (oneshot::Sender<()>, Arc<AtomicU32>, Arc<AtomicU32>);
            type Error = core::convert::Infallible;
            async fn on_start(
                (entered, finished, stopped): Self::Args,
                _: ActorRef<Self>,
            ) -> Result<Self, Self::Error> {
                Ok(Self {
                    entered: Some(entered),
                    finished,
                    stopped,
                })
            }
            async fn handle(
                &mut self,
                _: Block,
                _: ActorRef<Self>,
                _: &mut bool,
            ) -> Result<(), Self::Error> {
                if let Some(entered) = self.entered.take() {
                    let _ = entered.send(());
                }
                std::future::pending::<()>().await; // never completes until aborted
                self.finished.fetch_add(1, Ordering::SeqCst);
                Ok(())
            }
            async fn on_stop(
                &mut self,
                _: WeakActorRef<Self>,
                _: ActorStopReason,
            ) -> Result<(), Self::Error> {
                self.stopped.fetch_add(1, Ordering::SeqCst);
                Ok(())
            }
        }

        let (entered_tx, entered_rx) = oneshot::channel();
        let finished = Arc::new(AtomicU32::new(0));
        let stopped = Arc::new(AtomicU32::new(0));

        let prepared = PreparedActor::<Blocker>::new(cap(4));
        let actor_ref = prepared.actor_ref().clone();
        actor_ref
            .mailbox_sender()
            .send(Signal::Message(Block))
            .await
            .expect("send");
        let handle = prepared.spawn((entered_tx, Arc::clone(&finished), Arc::clone(&stopped)));

        entered_rx.await.expect("handler entered"); // handler is now parked forever
        actor_ref.kill(); // hard abort

        let outcome = handle.await.expect("join");
        assert!(
            matches!(outcome, RunResult::Killed),
            "kill → Killed, got {outcome:?}"
        );
        assert_eq!(
            finished.load(Ordering::SeqCst),
            0,
            "in-flight handler dropped, never finished"
        );
        assert_eq!(
            stopped.load(Ordering::SeqCst),
            0,
            "on_stop skipped on hard kill"
        );
    }

    /// The ergonomic spawn path uses the default mailbox capacity; pin the constant
    /// and that `default_capacity()` yields exactly it (guards a wrong default).
    #[test]
    fn default_capacity_is_64() {
        assert_eq!(DEFAULT_MAILBOX_CAPACITY, 64);
        assert_eq!(super::default_capacity().get(), 64);
    }

    /// Linearizability / single-writer: many senders race messages at one actor from
    /// the same instant; the actor handles them sequentially, so the total count is
    /// exact (none lost or double-counted) despite real concurrency.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn concurrent_senders_single_writer_exact_count() {
        use crate::actor::Spawn;
        use tokio::sync::{Barrier, oneshot};

        const SENDERS: u32 = 8;
        const PER_SENDER: u32 = 50;

        struct Sink {
            count: u32,
            done_at: u32,
            done: Option<oneshot::Sender<u32>>,
        }
        struct Bump;
        impl Msg for Bump {}
        impl Mailboxed for Sink {
            type Msg = Bump;
        }
        impl crate::actor::Actor for Sink {
            type Args = (u32, oneshot::Sender<u32>);
            type Error = core::convert::Infallible;
            async fn on_start(
                (done_at, done): Self::Args,
                _: ActorRef<Self>,
            ) -> Result<Self, Self::Error> {
                Ok(Self {
                    count: 0,
                    done_at,
                    done: Some(done),
                })
            }
            async fn handle(
                &mut self,
                _: Bump,
                _: ActorRef<Self>,
                stop: &mut bool,
            ) -> Result<(), Self::Error> {
                self.count += 1;
                if self.count == self.done_at {
                    if let Some(done) = self.done.take() {
                        let _ = done.send(self.count);
                    }
                    *stop = true;
                }
                Ok(())
            }
        }

        let (done_tx, done_rx) = oneshot::channel();
        let total = SENDERS * PER_SENDER;
        let actor_ref = Sink::spawn_with_capacity(cap(4), (total, done_tx));

        let start = Arc::new(Barrier::new(SENDERS as usize));
        let mut tasks = Vec::new();
        for _ in 0..SENDERS {
            let sender = actor_ref.mailbox_sender().clone();
            let start = Arc::clone(&start);
            tasks.push(tokio::spawn(async move {
                start.wait().await;
                for _ in 0..PER_SENDER {
                    sender.send(Signal::Message(Bump)).await.expect("send");
                }
            }));
        }

        let final_count = done_rx.await.expect("actor finished");
        assert_eq!(
            final_count, total,
            "single writer counted every message exactly once"
        );
        for task in tasks {
            task.await.expect("sender task");
        }
    }
}
