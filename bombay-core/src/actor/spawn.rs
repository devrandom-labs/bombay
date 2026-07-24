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
    actor::{
        Actor, ActorRef, Watch, WeakActorRef,
        kind::{LinkedChannels, LoopHandles, run_linked_message_loop, run_message_loop},
    },
    error::{ActorStopReason, PanicError, PanicReason},
    mailbox::{ActorId, Capacity, Mailbox, MailboxReceiver, Signal},
    watch::{LinkReceiver, Watchers},
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
        let id = next_actor_id();
        let (mailbox_tx, mailbox_rx) = Mailbox::<A>::bounded(capacity, id);
        let (abort_handle, abort_registration) = AbortHandle::new_pair();
        let actor_ref = ActorRef::new(id, mailbox_tx, CancellationToken::new(), abort_handle, None);
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

impl<A: Watch> PreparedActor<A> {
    /// Prepares a **linked** actor: like [`new`](Self::new) but also creates the
    /// actor's UNBOUNDED link channel (so it can watch others), storing the sender
    /// in the [`ActorRef`] (`Some(link_tx)`) and returning the receiver for the
    /// run-loop to drain. A plain [`new`](Self::new) leaves `link_tx` `None`, so a
    /// plain-spawned `Watch` actor cannot watch (it has no channel).
    #[must_use = "a prepared actor and its link receiver must be run"]
    pub fn new_linked(capacity: Capacity) -> (Self, LinkReceiver) {
        let id = next_actor_id();
        let (mailbox_tx, mailbox_rx) = Mailbox::<A>::bounded(capacity, id);
        let (abort_handle, abort_registration) = AbortHandle::new_pair();
        let (link_tx, link_rx) = flume::unbounded();
        let actor_ref = ActorRef::new(
            id,
            mailbox_tx,
            CancellationToken::new(),
            abort_handle,
            Some(link_tx),
        );
        (
            Self {
                actor_ref,
                mailbox_rx,
                abort_registration,
            },
            link_rx,
        )
    }

    /// Runs the linked actor in the current task until it stops, draining death
    /// notices off `link_rx` alongside messages. Aborts (hard kill) short-circuit
    /// to [`RunResult::Killed`], skipping `on_stop`.
    pub async fn run_linked(self, args: A::Args, link_rx: LinkReceiver) -> RunResult<A> {
        let lifecycle = run_lifecycle_linked(args, self.actor_ref, self.mailbox_rx, link_rx);
        Abortable::new(lifecycle, self.abort_registration)
            .await
            .unwrap_or(RunResult::Killed)
    }

    /// Spawns the linked actor in a background tokio task.
    pub fn spawn_linked_task(
        self,
        args: A::Args,
        link_rx: LinkReceiver,
    ) -> JoinHandle<RunResult<A>> {
        tokio::spawn(self.run_linked(args, link_rx))
    }
}

/// The pieces [`start_actor`] hands the loop driver: the built `state`, the
/// task-owned watcher guard, the loop's cold handle copies, and the weak self-ref.
/// Grouped so the prologue can return them as one and the two lifecycles differ
/// only in which loop they drive.
struct StartedActor<A: Actor> {
    state: A,
    watchers: Watchers,
    handles: LoopHandles,
    weak: WeakActorRef<A>,
}

/// Lifecycle prologue shared by the plain and linked loops: run `on_start` under
/// `catch_unwind`, and on success stand up the ref-count-driven-stop scaffolding,
/// dropping the strong `actor_ref`. Returns the loop-driver inputs, or the
/// `StartupFailed` [`RunResult`] for the caller to early-return.
async fn start_actor<A: Actor>(
    args: A::Args,
    actor_ref: ActorRef<A>,
) -> Result<StartedActor<A>, RunResult<A>> {
    let started = AssertUnwindSafe(A::on_start(args, actor_ref.clone()))
        .catch_unwind()
        .await;
    let state = match started {
        Ok(Ok(actor)) => actor,
        Ok(Err(err)) => {
            return Err(RunResult::StartupFailed(PanicError::new(
                Box::new(err),
                PanicReason::OnStart,
            )));
        }
        Err(payload) => {
            return Err(RunResult::StartupFailed(PanicError::from_panic_any(
                payload,
                PanicReason::OnStart,
            )));
        }
    };

    // Ref-count-driven stop goes live (#117): the loop must not hold a strong
    // self-ref, or the mailbox never closes and the "all-senders-gone" arm stays
    // unreachable (kameo issue #171: a leaked strong self-ref pins the count and
    // the actor never stops). Keep only a weak self-ref plus the loop's own
    // copies of the cold lifecycle handles (for drain-window minting, ADR-0010).
    // The task-owned watcher set: its `Drop` fires the death notices, so a
    // watched actor is notified on EVERY exit path (normal return, panic unwind,
    // `Abortable` kill — `Drop` runs on all three), card #195.
    let watchers = Watchers::new(actor_ref.id());
    let handles = LoopHandles {
        cancel: actor_ref.cancel_token().clone(),
        abort: actor_ref.abort_handle().clone(),
    };
    let weak = actor_ref.downgrade();
    drop(actor_ref);

    Ok(StartedActor {
        state,
        watchers,
        handles,
        weak,
    })
}

/// Startup-failure teardown shared by both lifecycles (card #196): answer the
/// watch registrations that were already queued when `on_start` failed with the
/// TRUE reason, `Panicked(OnStart)`.
///
/// No [`Watchers`] guard exists yet on this path (it is minted only after a
/// successful start), so the backlog is the only record of those watchers, and
/// `MailboxReceiver`'s `Drop` would otherwise answer them with the synthetic
/// [`AlreadyDead`](ActorStopReason::AlreadyDead) — which a supervisor reads as
/// restart-worthy, crash-looping a child that can never start.
fn reject_watchers_on_startup_failure<A: Actor>(
    failed: RunResult<A>,
    mailbox_rx: &MailboxReceiver<A>,
) -> RunResult<A> {
    if let RunResult::StartupFailed(err) = &failed {
        mailbox_rx.reject_queued_watchers(&ActorStopReason::Panicked(err.clone()));
    }
    failed
}

/// Lifecycle epilogue shared by the plain and linked loops: apply any `Watch`/
/// `Unwatch` that raced the stop (FIFO — a late `Watch` is otherwise a silently
/// missed death, a late `Unwatch` would otherwise spuriously notify a former
/// watcher), fire the death notices by dropping the guard, then run `on_stop` under
/// `catch_unwind` (Err logged, `reason` preserved). On a hard kill this never runs
/// (the lifecycle future is dropped by `Abortable`) — the guard's `Drop` still
/// fires `Killed` for whatever was already registered.
async fn finish_actor<A: Actor>(
    mut state: A,
    weak: WeakActorRef<A>,
    mut mailbox_rx: MailboxReceiver<A>,
    mut watchers: Watchers,
    reason: ActorStopReason,
) -> RunResult<A> {
    for signal in mailbox_rx.drain() {
        match signal {
            Signal::Watch(reg) => watchers.apply(*reg),
            Signal::Unwatch(id) => watchers.remove(id),
            Signal::Message { .. } | Signal::Stop => {}
        }
    }
    watchers.set_reason(reason.clone());
    drop(watchers); // fires the graceful-path notifications before on_stop

    let stop_result = AssertUnwindSafe(state.on_stop(weak.clone(), reason.clone()))
        .catch_unwind()
        .await;
    log_on_stop_outcome::<A>(&reason, stop_result);

    RunResult::Stopped {
        actor: state,
        reason,
    }
}

/// `on_start` (catch) → message loop → `on_stop` (catch; Err logged, reason
/// preserved). Returns `StartupFailed` if `on_start` fails, else `Stopped`. The
/// prologue/epilogue are shared with [`run_lifecycle_linked`] via [`start_actor`]/
/// [`finish_actor`]; this differs only in driving the one-arm [`run_message_loop`].
async fn run_lifecycle<A: Actor>(
    args: A::Args,
    actor_ref: ActorRef<A>,
    mut mailbox_rx: MailboxReceiver<A>,
) -> RunResult<A> {
    let StartedActor {
        mut state,
        mut watchers,
        handles,
        weak,
    } = match start_actor(args, actor_ref).await {
        Ok(started) => started,
        Err(failed) => return reject_watchers_on_startup_failure(failed, &mailbox_rx),
    };

    let reason =
        run_message_loop(&mut state, &weak, &handles, &mut mailbox_rx, &mut watchers).await;

    finish_actor(state, weak, mailbox_rx, watchers, reason).await
}

/// The linked-actor lifecycle (#195): identical to [`run_lifecycle`] but drives the
/// two-arm [`run_linked_message_loop`] so the actor also drains its link channel and
/// reacts to deaths via `on_link_died`. Prologue and teardown are the shared
/// [`start_actor`]/[`finish_actor`] — a linked actor is watchable too.
async fn run_lifecycle_linked<A: Watch>(
    args: A::Args,
    actor_ref: ActorRef<A>,
    mut mailbox_rx: MailboxReceiver<A>,
    link_rx: LinkReceiver,
) -> RunResult<A> {
    let StartedActor {
        mut state,
        mut watchers,
        handles,
        weak,
    } = match start_actor(args, actor_ref).await {
        Ok(started) => started,
        Err(failed) => return reject_watchers_on_startup_failure(failed, &mailbox_rx),
    };

    let reason = run_linked_message_loop(
        &mut state,
        &weak,
        &handles,
        &mut watchers,
        LinkedChannels {
            mailbox_rx: &mut mailbox_rx,
            link_rx: &link_rx,
        },
    )
    .await;

    finish_actor(state, weak, mailbox_rx, watchers, reason).await
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
        error::{ActorNotLinked, ActorStopReason, PanicReason},
        mailbox::{ActorId, Capacity, Mailboxed, Signal},
        message::Msg,
        test_support::terminate_bound,
        watch::{LinkDied, WatchReg},
    };

    /// Counts handled messages and records whether `on_stop` ran, via shared
    /// atomics the test inspects — the SUT is the real loop, not a reimpl.
    struct Counter {
        handled: Arc<AtomicU32>,
        stopped: Arc<AtomicU32>,
    }
    #[derive(Debug)]
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
    // `Watch` with the default OTP hook, so `Counter` can be `spawn_linked` in the
    // link tests (it is used as a linked peer that stops normally).
    impl crate::actor::Watch for Counter {}

    fn cap(n: usize) -> Capacity {
        Capacity::try_from(n).expect("valid test capacity")
    }

    /// Bounds an actor-lifecycle await under the MIRI-aware fail-fast bound so a
    /// regression that stalls the loop FAILS instead of hanging (card #179).
    ///
    /// The assertions here are correct; the gap was that a bare `.run(..).await`
    /// (or a pre-run `tell`/`send` into a broken mailbox) has no upper bound, so
    /// under mutation a vanished message or a never-arriving `Stop` deadlocks the
    /// whole test binary — reported as a 20 s **timeout** rather than a caught
    /// mutant. Mirrors the inline `timeout(terminate_bound(), …)` already used
    /// across this module, extracted so the fix reads uniformly.
    async fn bounded<F: IntoFuture>(fut: F) -> F::Output {
        tokio::time::timeout(terminate_bound(), fut)
            .await
            .expect("actor lifecycle op must terminate, not hang")
    }

    /// Sequence: two messages then a `Stop` — both are handled (FIFO, before the
    /// stop), `on_stop` runs exactly once, and the outcome is a normal stop.
    #[tokio::test]
    async fn handles_queued_messages_then_stops_normally() {
        let handled = Arc::new(AtomicU32::new(0));
        let stopped = Arc::new(AtomicU32::new(0));

        let prepared = PreparedActor::<Counter>::new(cap(8));
        let actor_ref = prepared.actor_ref().clone();
        bounded(actor_ref.tell(Tick)).await.expect("send 1");
        bounded(actor_ref.tell(Tick)).await.expect("send 2");
        bounded(actor_ref.mailbox_sender().send(Signal::Stop))
            .await
            .expect("stop");

        let outcome = bounded(prepared.run((Arc::clone(&handled), Arc::clone(&stopped)))).await;

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

    /// Ref-count-driven stop (card #117): with no explicit stop and no messages,
    /// dropping the **last strong `ActorRef`** closes the mailbox, so the loop's
    /// `recv` returns `None` and the actor stops normally. In #116 the loop held a
    /// strong self-ref, so this arm was unreachable and the actor would hang here.
    #[tokio::test]
    async fn dropping_last_actor_ref_stops_the_actor() {
        let handled = Arc::new(AtomicU32::new(0));
        let stopped = Arc::new(AtomicU32::new(0));

        let prepared = PreparedActor::<Counter>::new(cap(8));
        let actor_ref = prepared.actor_ref().clone();
        let join = prepared.spawn((Arc::clone(&handled), Arc::clone(&stopped)));

        // The only strong ref is `actor_ref`; dropping it must stop the actor.
        drop(actor_ref);

        let outcome = tokio::time::timeout(terminate_bound(), join)
            .await
            .expect("actor stops promptly after the last ref drops")
            .expect("join");

        assert_eq!(
            handled.load(Ordering::SeqCst),
            0,
            "no messages were sent before the ref dropped"
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
            "ref-count stop is a clean normal stop",
        );
    }

    /// @bug (card #117) The everyday `tell` then release-the-handle pattern: a
    /// message enqueued while a strong ref existed must still be handled even if
    /// the last strong ref drops before the loop dequeues it. The queued message
    /// must **pin the actor alive** (ref-count stop drains the backlog). Here the
    /// `Tick` is enqueued before spawning while no external ref is held, so once
    /// the loop downgrades its own ref after `on_start` the sender count hits 0
    /// with `Tick` still queued. FAILS while the loop merely upgrades a weak
    /// self-ref (upgrade returns `None`, the message is abandoned) — Design E
    /// embeds the sender in the signal so the message keeps itself deliverable.
    #[tokio::test]
    async fn queued_message_is_handled_even_if_last_ref_drops_first() {
        let handled = Arc::new(AtomicU32::new(0));
        let stopped = Arc::new(AtomicU32::new(0));

        let prepared = PreparedActor::<Counter>::new(cap(8));
        bounded(prepared.actor_ref().tell(Tick))
            .await
            .expect("enqueue before spawn");

        let join = prepared.spawn((Arc::clone(&handled), Arc::clone(&stopped)));

        let outcome = tokio::time::timeout(terminate_bound(), join)
            .await
            .expect("actor stops")
            .expect("join");

        assert_eq!(
            handled.load(Ordering::SeqCst),
            1,
            "the queued message is handled before the ref-count stop"
        );
        assert_eq!(stopped.load(Ordering::SeqCst), 1, "on_stop ran once");
        assert!(matches!(
            outcome,
            RunResult::Stopped {
                reason: ActorStopReason::Normal,
                ..
            }
        ));
    }

    /// #186 / ADR-0010 guard: the `ActorRef` a handler receives in the DRAIN
    /// WINDOW (no external strong ref exists; the loop lifts/mints it from the
    /// queued message's `self_sender`) is wired to the REAL loop — `stop()`
    /// through it cancels the actual token, so the rest of the backlog is
    /// abandoned. Fails if the drain-window ref carries a fresh token.
    #[tokio::test]
    async fn drain_window_handler_ref_stops_the_actor() {
        struct Stopper {
            handled: Arc<AtomicU32>,
        }
        #[derive(Debug)]
        struct Halt;
        impl Msg for Halt {}
        impl Mailboxed for Stopper {
            type Msg = Halt;
        }
        impl crate::actor::Actor for Stopper {
            type Args = Arc<AtomicU32>;
            type Error = core::convert::Infallible;
            async fn on_start(handled: Self::Args, _: ActorRef<Self>) -> Result<Self, Self::Error> {
                Ok(Self { handled })
            }
            async fn handle(
                &mut self,
                _: Halt,
                actor_ref: ActorRef<Self>,
                _: &mut bool,
            ) -> Result<(), Self::Error> {
                self.handled.fetch_add(1, Ordering::SeqCst);
                actor_ref.stop(); // out-of-band cancel via the drain-window ref
                Ok(())
            }
        }

        let handled = Arc::new(AtomicU32::new(0));
        let prepared = PreparedActor::<Stopper>::new(cap(8));
        // Enqueue BEFORE spawning and hold no external ref: once the loop
        // downgrades its own ref after on_start, only the queued self_senders
        // keep the actor alive — the drain window.
        bounded(prepared.actor_ref().tell(Halt))
            .await
            .expect("send 1");
        bounded(prepared.actor_ref().tell(Halt))
            .await
            .expect("send 2");

        let outcome = bounded(prepared.run(Arc::clone(&handled))).await;

        assert_eq!(
            handled.load(Ordering::SeqCst),
            1,
            "stop() through the drain-window ref abandons the second message"
        );
        assert!(matches!(
            outcome,
            RunResult::Stopped {
                reason: ActorStopReason::Normal,
                ..
            }
        ));
    }

    /// #186 / ADR-0010 guard, the abort sibling: `kill()` through a
    /// drain-window handler ref aborts the REAL task at its next await point —
    /// the parked handler never finishes and the outcome is `Killed`. Fails if
    /// the drain-window ref carries a fresh abort handle.
    #[tokio::test]
    async fn drain_window_handler_ref_kills_the_actor() {
        struct Berserker {
            finished: Arc<AtomicU32>,
        }
        #[derive(Debug)]
        struct Rampage;
        impl Msg for Rampage {}
        impl Mailboxed for Berserker {
            type Msg = Rampage;
        }
        impl crate::actor::Actor for Berserker {
            type Args = Arc<AtomicU32>;
            type Error = core::convert::Infallible;
            async fn on_start(
                finished: Self::Args,
                _: ActorRef<Self>,
            ) -> Result<Self, Self::Error> {
                Ok(Self { finished })
            }
            async fn handle(
                &mut self,
                _: Rampage,
                actor_ref: ActorRef<Self>,
                _: &mut bool,
            ) -> Result<(), Self::Error> {
                actor_ref.kill();
                std::future::pending::<()>().await; // aborted here, never below
                self.finished.fetch_add(1, Ordering::SeqCst);
                Ok(())
            }
        }

        let finished = Arc::new(AtomicU32::new(0));
        let prepared = PreparedActor::<Berserker>::new(cap(4));
        bounded(prepared.actor_ref().tell(Rampage))
            .await
            .expect("send");

        let join = prepared.spawn(Arc::clone(&finished));
        let outcome = tokio::time::timeout(terminate_bound(), join)
            .await
            .expect("kill() through the drain-window ref must abort the task")
            .expect("join");

        assert!(
            matches!(outcome, RunResult::Killed),
            "kill -> Killed, got {outcome:?}"
        );
        assert_eq!(
            finished.load(Ordering::SeqCst),
            0,
            "the parked handler was aborted, never finished"
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
        #[derive(Debug)]
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
        bounded(actor_ref.tell(Work)).await.expect("send 1");
        bounded(actor_ref.tell(Work)).await.expect("send 2");

        let run = tokio::spawn(prepared.run((entered_tx, release_rx, Arc::clone(&handled))));

        // Bounded: if the send is a no-op the handler never enters, so fail fast
        // here rather than hanging until the harness timeout.
        tokio::time::timeout(terminate_bound(), entered_rx)
            .await
            .expect("the sent Work must reach the handler, not hang")
            .expect("handler entered"); // handler #1 is mid-flight
        actor_ref.stop(); // cancel while in-flight
        release_tx.send(()).expect("release handler"); // let handler #1 finish

        // Bounded so that if `stop` is a no-op the loop never ends (it would go on
        // to handle the queued message and park on `recv`), FAILING FAST here
        // rather than hanging until the harness timeout.
        let outcome = tokio::time::timeout(terminate_bound(), run)
            .await
            .expect("stop() must terminate the actor after the in-flight handler")
            .expect("run task");
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

    /// Lifecycle: out-of-band `stop()` fired **before the loop ever polls `recv`**
    /// abandons the whole backlog, not just the tail behind an in-flight handler.
    /// Distinct from `cancel_finishes_in_flight_then_stops` (which cancels mid-loop,
    /// after the first message is already dequeued and handled): here the token is
    /// already cancelled when `run_message_loop`'s first
    /// `cancel.run_until_cancelled(mailbox_rx.recv())` races, so `run_until_cancelled`
    /// must observe the pre-fired cancellation rather than the pending `recv` even
    /// though messages are sitting in the mailbox — zero messages are handled.
    #[tokio::test]
    async fn cancel_token_stop_abandons_the_backlog() {
        let handled = Arc::new(AtomicU32::new(0));
        let stopped = Arc::new(AtomicU32::new(0));

        let prepared = PreparedActor::<Counter>::new(cap(8));
        let actor_ref = prepared.actor_ref().clone();
        bounded(actor_ref.tell(Tick)).await.expect("enqueue 1");
        bounded(actor_ref.tell(Tick)).await.expect("enqueue 2");
        actor_ref.stop(); // cancel BEFORE run() ever drains anything

        let outcome = bounded(prepared.run((Arc::clone(&handled), Arc::clone(&stopped)))).await;

        assert_eq!(
            handled.load(Ordering::SeqCst),
            0,
            "cancel-before-drain abandons the whole backlog"
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
            "clean normal stop, got {outcome:?}",
        );
    }

    /// Sequence: a handler that sets `*stop = true` stops the actor cleanly after it
    /// returns `Ok` — a following queued message is never handled.
    #[tokio::test]
    async fn stop_flag_stops_after_current_handler() {
        struct Once {
            handled: Arc<AtomicU32>,
        }
        #[derive(Debug)]
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
        bounded(actor_ref.tell(Go)).await.expect("send 1");
        bounded(actor_ref.tell(Go)).await.expect("send 2");

        // Bounded so that if the `stop` flag is ignored (the loop keeps running
        // and parks on `recv`, since this test still holds a strong sender), the
        // test FAILS FAST here rather than hanging until the harness timeout.
        let outcome = tokio::time::timeout(terminate_bound(), prepared.run(Arc::clone(&handled)))
            .await
            .expect("the stop flag must terminate the actor");
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
        #[derive(Debug)]
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
        bounded(actor_ref.tell(N(0))).await.expect("send 0");
        bounded(actor_ref.tell(N(1))).await.expect("send 1");
        bounded(actor_ref.tell(N(2))).await.expect("send 2");
        gate_tx.send(()).expect("release on_start");

        // Bounded so that if the `stop` flag is ignored the loop parks on `recv`
        // after handling all three, FAILING FAST here rather than hanging until
        // the harness timeout.
        tokio::time::timeout(terminate_bound(), run)
            .await
            .expect("the stop flag must terminate the actor")
            .expect("run task");
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
    /// This asserts containment, but it canNOT pin `panic = "unwind"` — cargo
    /// ignores the `panic` setting for tests, so this passes even when the
    /// release profile is `abort` and real binaries die on this exact panic.
    /// The pin is the `cfg(panic = "abort")` compile_error in `lib.rs` (#169).
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

    /// @bug (card #196) A child whose `on_start` fails must deliver its TRUE
    /// reason — `Panicked(OnStart)` — to watchers whose registration was still
    /// queued when the mailbox died. FAILS while only `MailboxReceiver::drop`
    /// answers them, because its synthetic `AlreadyDead` is restart-worthy: a
    /// supervisor would burn its whole restart budget crash-looping a child that
    /// can never start, instead of escalating on the first failure.
    #[tokio::test]
    async fn startup_failure_answers_queued_watchers_with_on_start_reason() {
        #[derive(Debug)]
        struct Refuses;
        struct FailingStart;
        struct Never;
        impl Msg for Never {}
        impl Mailboxed for FailingStart {
            type Msg = Never;
        }
        impl crate::actor::Actor for FailingStart {
            type Args = ();
            type Error = Refuses;
            async fn on_start(_: (), _: ActorRef<Self>) -> Result<Self, Self::Error> {
                Err(Refuses)
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

        let prepared = PreparedActor::<FailingStart>::new(cap(4));
        let (link_tx, link_rx) = flume::unbounded::<LinkDied>();
        prepared
            .actor_ref()
            .mailbox_sender()
            .try_send(Signal::Watch(Box::new(WatchReg {
                watcher: ActorId::new(1),
                link_tx,
                linked: false,
            })))
            .expect("fresh mailbox has capacity");

        let outcome = bounded(prepared.run(())).await;
        assert!(
            matches!(outcome, RunResult::StartupFailed(_)),
            "expected StartupFailed, got {outcome:?}",
        );

        let notice = link_rx
            .try_recv()
            .expect("a queued watch reg must be notified, never silently dropped");
        match notice.reason {
            ActorStopReason::Panicked(err) => {
                assert_eq!(err.reason(), PanicReason::OnStart);
            }
            other => panic!("expected Panicked(OnStart), got {other:?}"),
        }
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
        #[derive(Debug)]
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
        bounded(actor_ref.tell(Explode)).await.expect("send");

        // Bounded: if the send is a no-op the actor never panics and the loop
        // never ends, so fail fast rather than hanging until the harness timeout.
        let outcome = tokio::time::timeout(
            terminate_bound(),
            prepared.run((Arc::clone(&stop_reason), Arc::clone(&counter_at_stop))),
        )
        .await
        .expect("the sent Explode must panic the actor and stop the loop, not hang");

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
        bounded(actor_ref.tell(Explode)).await.expect("send");
        // Bounded: if the send is a no-op the actor never panics and the loop
        // never ends, so fail fast rather than hanging until the harness timeout.
        let _ = tokio::time::timeout(
            terminate_bound(),
            prepared.run((Arc::clone(&stop_reason), Arc::clone(&counter_at_stop))),
        )
        .await
        .expect("the sent Explode must panic the actor and stop the loop, not hang");

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
        #[derive(Debug)]
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
        bounded(actor_ref.tell(Trigger))
            .await
            .expect("send trigger");

        // Bounded: if the send is a no-op the actor never panics and the loop
        // never ends, so fail fast rather than hanging until the harness timeout.
        let outcome = tokio::time::timeout(terminate_bound(), handle)
            .await
            .expect("the sent Trigger must panic the actor and stop the loop, not hang")
            .expect("run task");
        assert!(matches!(
            outcome,
            RunResult::Stopped {
                reason: ActorStopReason::Panicked(_),
                ..
            }
        ));

        let resend = bounded(actor_ref.tell(Trigger)).await;
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
        #[derive(Debug)]
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
        bounded(actor_ref.tell(Do)).await.expect("send");
        // Bounded: if the send is a no-op the handler never returns Err and the
        // loop never ends, so fail fast rather than hanging until the harness timeout.
        let outcome = tokio::time::timeout(terminate_bound(), prepared.run(()))
            .await
            .expect("the sent Do must return Err and stop the loop, not hang");

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
        #[derive(Debug)]
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
        bounded(actor_ref.tell(Block)).await.expect("send");
        let handle = prepared.spawn((entered_tx, Arc::clone(&finished), Arc::clone(&stopped)));

        // Bounded: if the send is a no-op the handler never enters, so fail fast
        // here rather than hanging until the harness timeout.
        tokio::time::timeout(terminate_bound(), entered_rx)
            .await
            .expect("the sent Block must reach the handler, not hang")
            .expect("handler entered"); // handler is now parked forever
        actor_ref.kill(); // hard abort

        // Bounded so that if `kill` is a no-op, the parked handler never aborts
        // and this FAILS FAST rather than hanging until the harness timeout.
        let outcome = tokio::time::timeout(terminate_bound(), handle)
            .await
            .expect("kill() must abort the parked actor")
            .expect("join");
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

    /// Lifecycle: `stop()` on an otherwise-idle actor (empty mailbox, loop parked
    /// on `recv`) wakes the loop and stops it normally, running `on_stop`. Bounded
    /// so that if `stop` is a no-op the loop parks forever and this FAILS FAST
    /// instead of hanging until the harness timeout.
    #[tokio::test]
    async fn stop_terminates_idle_actor() {
        let handled = Arc::new(AtomicU32::new(0));
        let stopped = Arc::new(AtomicU32::new(0));

        let prepared = PreparedActor::<Counter>::new(cap(4));
        let actor_ref = prepared.actor_ref().clone();
        let run = tokio::spawn(prepared.run((Arc::clone(&handled), Arc::clone(&stopped))));

        // No messages are ever sent: the loop is parked on `recv`. The cancel must
        // wake it. (`actor_ref` still holds a strong sender, so `recv` will NOT
        // return `None` on its own — only the cancel can end the loop.)
        actor_ref.stop();

        let outcome = tokio::time::timeout(terminate_bound(), run)
            .await
            .expect("stop() must terminate the idle actor")
            .expect("run task");
        assert!(
            matches!(
                outcome,
                RunResult::Stopped {
                    reason: ActorStopReason::Normal,
                    ..
                }
            ),
            "clean normal stop, got {outcome:?}",
        );
        assert_eq!(handled.load(Ordering::SeqCst), 0, "no message was handled");
        assert_eq!(stopped.load(Ordering::SeqCst), 1, "on_stop ran once");
    }

    /// The `RunResult` debug view distinguishes each variant by name — guards the
    /// hand-written `Debug` impl against being stubbed to an empty formatter.
    #[test]
    fn run_result_debug_distinguishes_variants() {
        let killed: RunResult<Counter> = RunResult::Killed;
        assert_eq!(format!("{killed:?}"), "Killed", "Killed prints its name");

        let stopped: RunResult<Counter> = RunResult::Stopped {
            actor: Counter {
                handled: Arc::new(AtomicU32::new(0)),
                stopped: Arc::new(AtomicU32::new(0)),
            },
            reason: ActorStopReason::Normal,
        };
        let shown = format!("{stopped:?}");
        assert!(shown.contains("Stopped"), "names the variant: {shown}");
        assert!(
            shown.contains("reason"),
            "surfaces the reason field: {shown}"
        );

        let failed: RunResult<Counter> =
            RunResult::StartupFailed(crate::error::PanicError::from_panic_any(
                Box::new("boom"),
                crate::error::PanicReason::OnStart,
            ));
        assert!(
            format!("{failed:?}").contains("StartupFailed"),
            "names the variant: {failed:?}",
        );
    }

    /// The `PreparedActor` debug view names the struct — guards its hand-written
    /// `Debug` impl against being stubbed to an empty formatter.
    #[test]
    fn prepared_actor_debug_names_struct() {
        let prepared = PreparedActor::<Counter>::new(cap(4));
        let shown = format!("{prepared:?}");
        assert!(
            shown.contains("PreparedActor"),
            "debug names the struct: {shown}"
        );
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
        #[derive(Debug)]
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
                    sender.send_message(Bump).await.expect("send");
                }
            }));
        }

        // Bounded: if a send is a no-op the actor never reaches `total`, so fail
        // fast here rather than hanging until the harness timeout.
        let final_count = tokio::time::timeout(terminate_bound(), done_rx)
            .await
            .expect("every sent Bump must be handled, not hang")
            .expect("actor finished");
        assert_eq!(
            final_count, total,
            "single writer counted every message exactly once"
        );
        for task in tasks {
            task.await.expect("sender task");
        }
    }

    /// Linearizability: `tell` racing the last-strong-ref drop (#117). Each task
    /// sends once then drops its ref, so the enqueues race the sender-drops that
    /// close the mailbox. The single-writer invariant must hold under every
    /// interleaving: an *accepted* message is always handled before the actor
    /// stops (drain-before-close, ADR-0003), and the actor stops exactly once,
    /// Normal. Distinct from `concurrent_senders_single_writer_exact_count`,
    /// where no task ever drops the last ref.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn tell_racing_last_ref_drop_never_loses_an_accepted_message() {
        use tokio::sync::Barrier;

        const REFS: usize = 8;
        let handled = Arc::new(AtomicU32::new(0));
        let stopped = Arc::new(AtomicU32::new(0));
        let prepared = PreparedActor::<Counter>::new(cap(REFS));
        let refs: Vec<_> = (0..REFS).map(|_| prepared.actor_ref().clone()).collect();
        let join = prepared.spawn((Arc::clone(&handled), Arc::clone(&stopped)));

        let barrier = Arc::new(Barrier::new(REFS));
        let tasks: Vec<_> = refs
            .into_iter()
            .map(|r| {
                let barrier = Arc::clone(&barrier);
                tokio::spawn(async move {
                    barrier.wait().await;
                    let accepted = r.tell(Tick).await.is_ok();
                    drop(r);
                    accepted
                })
            })
            .collect();

        let mut accepted = 0;
        for task in tasks {
            if task.await.expect("tell task") {
                accepted += 1;
            }
        }

        let outcome = tokio::time::timeout(terminate_bound(), join)
            .await
            .expect("actor stops after the last ref drops")
            .expect("join");

        assert_eq!(
            handled.load(Ordering::SeqCst),
            accepted,
            "every accepted message is handled before the mailbox closes",
        );
        assert_eq!(
            stopped.load(Ordering::SeqCst),
            1,
            "on_stop runs exactly once"
        );
        assert!(matches!(
            outcome,
            RunResult::Stopped {
                reason: ActorStopReason::Normal,
                ..
            }
        ));
    }

    /// DST: `WeakActorRef::upgrade` racing the last-strong-ref drop (#117).
    /// `upgrade` (`fetch_update` on flume's `sender_count`) races the strong
    /// ref's drop (`count 1→0`). Every upgrade must yield either a valid ref
    /// with the actor's identity or `None` — never a torn/dangling handle — and
    /// once the last strong sender is gone `upgrade` is `None`. This is the
    /// concurrent `upgrade` probe #150's DST leg lacked.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn upgrade_racing_last_ref_drop_is_some_or_none_never_torn() {
        use tokio::sync::Barrier;

        let handled = Arc::new(AtomicU32::new(0));
        let stopped = Arc::new(AtomicU32::new(0));
        let prepared = PreparedActor::<Counter>::new(cap(8));
        let strong = prepared.actor_ref().clone();
        let weak = strong.downgrade();
        let id = strong.id();
        let join = prepared.spawn((Arc::clone(&handled), Arc::clone(&stopped)));

        let barrier = Arc::new(Barrier::new(2));
        let upgrader = {
            let barrier = Arc::clone(&barrier);
            let weak = weak.clone();
            tokio::spawn(async move {
                barrier.wait().await;
                for _ in 0..1_000 {
                    if let Some(strong) = weak.upgrade() {
                        assert_eq!(
                            strong.id(),
                            id,
                            "an upgraded ref keeps the actor's identity"
                        );
                    }
                }
            })
        };
        let dropper = {
            let barrier = Arc::clone(&barrier);
            tokio::spawn(async move {
                barrier.wait().await;
                drop(strong);
            })
        };

        upgrader.await.expect("upgrade task");
        dropper.await.expect("drop task");

        let outcome = tokio::time::timeout(terminate_bound(), join)
            .await
            .expect("actor stops after the last ref drops")
            .expect("join");

        assert!(
            weak.upgrade().is_none(),
            "no strong sender remains, so upgrade is None",
        );
        assert!(matches!(
            outcome,
            RunResult::Stopped {
                reason: ActorStopReason::Normal,
                ..
            }
        ));
    }

    /// A minimal actor whose `handle` unwinds — used to drive the panic exit path
    /// for the death-watch teardown tests (card #195).
    struct Panicker;
    #[derive(Debug)]
    struct Boom;
    impl Msg for Boom {}
    impl Mailboxed for Panicker {
        type Msg = Boom;
    }
    impl crate::actor::Actor for Panicker {
        type Args = ();
        type Error = core::convert::Infallible;
        async fn on_start(_: (), _: ActorRef<Self>) -> Result<Self, Self::Error> {
            Ok(Panicker)
        }
        async fn handle(
            &mut self,
            _: Boom,
            _: ActorRef<Self>,
            _: &mut bool,
        ) -> Result<(), Self::Error> {
            panic!("boom")
        }
    }
    // `Watch` with the default OTP hook: a linked abnormal death `Break`s, so a
    // `Panicker` linked to a peer that dies abnormally propagates (used by the
    // `link_*` tests). Empty impl = the trait's default `on_link_died`.
    impl crate::actor::Watch for Panicker {}

    /// A `Watch` actor that TRAPS every death — its `on_link_died` returns
    /// `Continue` even for a linked abnormal death, so it survives a linked peer's
    /// crash (the `trap_exit` override, card #195).
    struct Trapper;
    impl Mailboxed for Trapper {
        type Msg = Never;
    }
    impl crate::actor::Actor for Trapper {
        type Args = ();
        type Error = core::convert::Infallible;
        async fn on_start(_: (), _: ActorRef<Self>) -> Result<Self, Self::Error> {
            Ok(Trapper)
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
    impl crate::actor::Watch for Trapper {
        async fn on_link_died(
            &mut self,
            _: ActorId,
            _: ActorStopReason,
            _: bool,
        ) -> Result<core::ops::ControlFlow<ActorStopReason>, Self::Error> {
            Ok(core::ops::ControlFlow::Continue(())) // trap: never propagate
        }
    }

    /// A `Watch` actor giving BOTH positive signals the trap/no-propagate/dead-target
    /// tests need (card #195):
    ///
    /// - `deaths` — bumped once per `on_link_died` invocation (after `last` is
    ///   recorded), the proof a death was actually DELIVERED and the hook RAN. A
    ///   broken loop that never fires the death leaves `deaths == 0`, so a
    ///   bounded-wait on it times out instead of passing on a fixed-time window.
    /// - `handled` — bumped per `Ping` message handled. Its hook always returns
    ///   `Continue` (never propagates), so a POST-death `Ping` round-trip
    ///   (`deaths == 1` then send `Ping`, wait `handled == 1`) is the robust proof
    ///   the actor SURVIVED — the loop is still dequeuing messages after the death.
    ///   A racy `is_alive()` check would instead pass under an always-`Break`
    ///   mutation, since the mailbox closes only lazily during async teardown.
    /// - `already_dead` — bumped when a delivered death's reason is the synthetic
    ///   [`AlreadyDead`](ActorStopReason::AlreadyDead), bumped BEFORE `deaths`.
    ///   This is the signature of the backpressure bug: a `try_send` that returns
    ///   `Full` for a momentarily-full-but-alive target would synthesize a
    ///   spurious synthetic death. The correct `send().await` waits, so a watcher
    ///   only ever sees the target's real reason — `already_dead` stays 0 across
    ///   a backpressured registration. It doubles as the positive probe for the
    ///   genuine link-to-dead path (the dead-target test asserts exactly one).
    struct Recorder {
        deaths: Arc<AtomicU32>,
        already_dead: Arc<AtomicU32>,
        handled: Arc<AtomicU32>,
        last: Arc<std::sync::Mutex<Option<(ActorId, bool)>>>,
    }
    #[derive(Debug)]
    struct Ping;
    impl Msg for Ping {}
    impl Mailboxed for Recorder {
        type Msg = Ping;
    }
    impl crate::actor::Actor for Recorder {
        type Args = RecorderSlots;
        type Error = core::convert::Infallible;
        async fn on_start(slots: Self::Args, _: ActorRef<Self>) -> Result<Self, Self::Error> {
            Ok(Self {
                deaths: slots.deaths,
                already_dead: slots.already_dead,
                handled: slots.handled,
                last: slots.last,
            })
        }
        async fn handle(
            &mut self,
            _: Ping,
            _: ActorRef<Self>,
            _: &mut bool,
        ) -> Result<(), Self::Error> {
            self.handled.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }
    }
    impl crate::actor::Watch for Recorder {
        async fn on_link_died(
            &mut self,
            id: ActorId,
            reason: ActorStopReason,
            linked: bool,
        ) -> Result<core::ops::ControlFlow<ActorStopReason>, Self::Error> {
            *self.last.lock().expect("lock") = Some((id, linked));
            if matches!(reason, ActorStopReason::AlreadyDead) {
                // Bump BEFORE `deaths`, so a reader observing `deaths == 1` (SeqCst)
                // also sees this write — the spurious-synthetic check is race-free.
                self.already_dead.fetch_add(1, Ordering::SeqCst);
            }
            self.deaths.fetch_add(1, Ordering::SeqCst);
            Ok(core::ops::ControlFlow::Continue(())) // trap: never propagate
        }
    }

    /// The shared observation slots handed to a [`Recorder`] at spawn.
    struct RecorderSlots {
        deaths: Arc<AtomicU32>,
        already_dead: Arc<AtomicU32>,
        handled: Arc<AtomicU32>,
        last: Arc<std::sync::Mutex<Option<(ActorId, bool)>>>,
    }

    /// A spawned linked [`Recorder`] plus the slots a test asserts on: the death,
    /// spurious-`Killed`, and message-handled counters, and the last-seen `(id, linked)`.
    struct RecorderProbe {
        handle: ActorRef<Recorder>,
        deaths: Arc<AtomicU32>,
        already_dead: Arc<AtomicU32>,
        handled: Arc<AtomicU32>,
        last: Arc<std::sync::Mutex<Option<(ActorId, bool)>>>,
    }

    /// Spawns a linked [`Recorder`] and returns it with its observation slots.
    fn spawn_recorder() -> RecorderProbe {
        use crate::actor::SpawnLinked;
        let deaths = Arc::new(AtomicU32::new(0));
        let already_dead = Arc::new(AtomicU32::new(0));
        let handled = Arc::new(AtomicU32::new(0));
        let last = Arc::new(std::sync::Mutex::new(None));
        let handle = Recorder::spawn_linked(RecorderSlots {
            deaths: Arc::clone(&deaths),
            already_dead: Arc::clone(&already_dead),
            handled: Arc::clone(&handled),
            last: Arc::clone(&last),
        });
        RecorderProbe {
            handle,
            deaths,
            already_dead,
            handled,
            last,
        }
    }

    /// Proves a [`Recorder`] SURVIVED (did not propagate) after processing a death:
    /// bounded-waits `deaths == 1` (the death was delivered + hook ran), then sends a
    /// `Ping` and bounded-waits `handled == 1` (the loop is still dequeuing messages
    /// AFTER the death). Both waits are bounded, so an always-`Break` loop — which
    /// stops the actor before the `Ping` is handled — makes the second wait time out
    /// and FAILS the test, where a racy post-`deaths` `is_alive()` check would not.
    async fn assert_survived_one_death(probe: &RecorderProbe) {
        bounded(async {
            while probe.deaths.load(Ordering::SeqCst) == 0 {
                tokio::task::yield_now().await;
            }
        })
        .await;
        probe
            .handle
            .tell(Ping)
            .try_send()
            .expect("actor still alive to ping");
        bounded(async {
            while probe.handled.load(Ordering::SeqCst) == 0 {
                tokio::task::yield_now().await;
            }
        })
        .await;
        assert_eq!(
            probe.deaths.load(Ordering::SeqCst),
            1,
            "exactly one death delivered (a duplicate-delivery regression must fail)",
        );
    }

    /// Lifecycle (card #195): a registered watcher is notified on the NORMAL stop
    /// path with the actor's id, a normal reason, and `linked == false` (a `watch`
    /// edge). The notification is the `Watchers` guard's `Drop` on the graceful
    /// teardown, after the loop returns `Normal`.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn watch_notified_on_normal_stop() {
        use crate::actor::Spawn;

        let handled = Arc::new(AtomicU32::new(0));
        let stopped = Arc::new(AtomicU32::new(0));
        let target = Counter::spawn((Arc::clone(&handled), Arc::clone(&stopped)));
        let (watch_tx, watch_rx) = flume::unbounded::<LinkDied>();

        // Register a watcher directly via the mailbox (`ActorRef::watch` is Task 9).
        target
            .mailbox_sender()
            .send(Signal::Watch(Box::new(WatchReg {
                watcher: ActorId::new(999),
                link_tx: watch_tx,
                linked: false,
            })))
            .await
            .expect("registration delivered");

        target.stop(); // graceful
        let notice = bounded(watch_rx.recv_async()).await.expect("watch fired");
        assert_eq!(notice.id, target.id());
        assert!(notice.reason.is_normal(), "normal stop => normal reason");
        assert!(!notice.linked, "a watch edge carries linked == false");
    }

    /// Lifecycle (card #195): a registered watcher is notified on the PANIC path.
    /// The handler panic is caught by `handle_message`'s `catch_unwind` and the loop
    /// returns `Panicked`, so the teardown `set_reason(Panicked) → drop(watchers)`
    /// path fires the notice — not a true unwind through the guard.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn watch_notified_on_panic() {
        use crate::actor::Spawn;

        let target = Panicker::spawn(());
        let (tx, rx) = flume::unbounded::<LinkDied>();
        target
            .mailbox_sender()
            .send(Signal::Watch(Box::new(WatchReg {
                watcher: ActorId::new(1),
                link_tx: tx,
                linked: false,
            })))
            .await
            .expect("registration delivered");
        target.tell(Boom).try_send().expect("provoke the panic");
        let notice = bounded(rx.recv_async())
            .await
            .expect("watch fired on the panic path");
        assert!(matches!(notice.reason, ActorStopReason::Panicked(_)));
    }

    /// Applies queued Watch registrations to `target`'s guard, deterministic via
    /// FIFO: a follow-up `Tick` is enqueued behind them, so once `handled` reaches
    /// 1 the loop has provably dequeued every prior signal (the regs) and pushed
    /// them to the guard. Returns once the barrier is crossed.
    ///
    /// The KILL tests use it to pin the APPLIED-then-killed path (`Watchers::drop`
    /// reports `Killed`). The complementary still-QUEUED-at-kill path is delivered
    /// by `MailboxReceiver::drop` as `AlreadyDead` — see
    /// `watch_in_flight_at_kill_still_notified`.
    async fn watch_and_await_applied(target: &ActorRef<Counter>, handled: &AtomicU32) {
        bounded(target.tell(Tick)).await.expect("barrier tick sent");
        bounded(async {
            while handled.load(Ordering::SeqCst) == 0 {
                tokio::task::yield_now().await;
            }
        })
        .await;
    }

    /// Lifecycle (card #195): a registered watcher is notified on the KILL path.
    /// `Abortable` drops the whole lifecycle future, but the `Watchers` guard's
    /// `Drop` still runs (no graceful reason set) and reports `Killed`.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn watch_notified_on_kill() {
        use crate::actor::Spawn;

        let handled = Arc::new(AtomicU32::new(0));
        let stopped = Arc::new(AtomicU32::new(0));
        let target = Counter::spawn((Arc::clone(&handled), stopped));
        let (tx, rx) = flume::unbounded::<LinkDied>();
        target
            .mailbox_sender()
            .send(Signal::Watch(Box::new(WatchReg {
                watcher: ActorId::new(1),
                link_tx: tx,
                linked: false,
            })))
            .await
            .expect("registration delivered");
        // The reg must reach the guard before the abort (see helper doc).
        watch_and_await_applied(&target, &handled).await;

        target.kill(); // Abortable drops the loop future — no on_stop
        let notice = bounded(rx.recv_async()).await.expect("watch fired on kill");
        assert!(matches!(notice.reason, ActorStopReason::Killed));
    }

    /// `@bug` Sequence (card #195): a `Signal::Watch` QUEUED but not yet applied
    /// when the target is hard-killed is still notified — the missed-death race
    /// the card exists to kill. The `Abortable` drops the lifecycle future with
    /// the reg still in the channel, so the notice comes from
    /// `MailboxReceiver::drop`, with reason
    /// [`AlreadyDead`](ActorStopReason::AlreadyDead) (the receiver cannot know
    /// the true reason; Erlang's `noproc`). The `Gate` handler is parked
    /// mid-`handle`, so the loop provably never dequeues the reg before the
    /// abort. FAILS while the receiver's drop drain silently discards it.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn watch_in_flight_at_kill_still_notified() {
        use crate::actor::Spawn;
        use tokio::sync::oneshot;

        let (entered_tx, entered_rx) = oneshot::channel();
        let (_release_tx, release_rx) = oneshot::channel();
        let target = Gate::spawn_with_capacity(cap(1), (entered_tx, release_rx));

        // Park the single handler so the loop cannot dequeue anything further.
        bounded(target.tell(Enter)).await.expect("enqueue Enter");
        bounded(entered_rx).await.expect("handler parked");

        let (tx, rx) = flume::unbounded::<LinkDied>();
        target
            .mailbox_sender()
            .try_send(Signal::Watch(Box::new(WatchReg {
                watcher: ActorId::new(1),
                link_tx: tx,
                linked: false,
            })))
            .expect("reg queued behind the parked handler");

        target.kill(); // Abortable drops the loop with the reg still queued

        let notice = bounded(rx.recv_async())
            .await
            .expect("a queued-at-kill watch must still be notified");
        assert_eq!(notice.id, target.id());
        assert!(
            matches!(notice.reason, ActorStopReason::AlreadyDead),
            "true reason unknowable at the receiver => AlreadyDead, got {:?}",
            notice.reason,
        );
    }

    /// `@bug` Lifecycle (card #195): a `Signal::Watch` ACCEPTED (send returned
    /// `Ok`) during the graceful teardown window — after `finish_actor`'s drain
    /// snapshot, while `on_stop` is still running — is still notified. The
    /// mailbox stays open across `on_stop`, so the send succeeds; the reg is
    /// then only reachable by `MailboxReceiver::drop`, which must deliver the
    /// synthetic [`AlreadyDead`](ActorStopReason::AlreadyDead) notice. FAILS
    /// while the drop drain silently discards it (watcher waits forever for a
    /// death that already happened — the #100-class hang).
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn watch_accepted_during_on_stop_still_notified() {
        use crate::actor::Spawn;
        use tokio::sync::oneshot;

        struct SlowStop {
            entered: Option<oneshot::Sender<()>>,
            release: Option<oneshot::Receiver<()>>,
        }
        impl Mailboxed for SlowStop {
            type Msg = Never;
        }
        impl crate::actor::Actor for SlowStop {
            type Args = (oneshot::Sender<()>, oneshot::Receiver<()>);
            type Error = core::convert::Infallible;
            async fn on_start(
                (entered, release): Self::Args,
                _: ActorRef<Self>,
            ) -> Result<Self, Self::Error> {
                Ok(Self {
                    entered: Some(entered),
                    release: Some(release),
                })
            }
            async fn handle(
                &mut self,
                _: Never,
                _: ActorRef<Self>,
                _: &mut bool,
            ) -> Result<(), Self::Error> {
                Ok(())
            }
            async fn on_stop(
                &mut self,
                _: WeakActorRef<Self>,
                _: ActorStopReason,
            ) -> Result<(), Self::Error> {
                if let Some(entered) = self.entered.take() {
                    let _ = entered.send(());
                }
                if let Some(release) = self.release.take() {
                    let _ = release.await; // park teardown INSIDE the window
                }
                Ok(())
            }
        }

        let (entered_tx, entered_rx) = oneshot::channel();
        let (release_tx, release_rx) = oneshot::channel();
        let target = SlowStop::spawn((entered_tx, release_rx));

        target.stop(); // loop exits; finish_actor drains, then parks in on_stop
        bounded(entered_rx).await.expect("on_stop entered");

        // The teardown drain already ran; this send still succeeds (receiver
        // alive) — the acceptance that must not become a silent missed death.
        let (tx, rx) = flume::unbounded::<LinkDied>();
        bounded(
            target
                .mailbox_sender()
                .send(Signal::Watch(Box::new(WatchReg {
                    watcher: ActorId::new(1),
                    link_tx: tx,
                    linked: false,
                }))),
        )
        .await
        .expect("mailbox is still open during on_stop");

        release_tx.send(()).expect("release on_stop");

        let notice = bounded(rx.recv_async())
            .await
            .expect("a watch accepted during on_stop must still be notified");
        assert_eq!(notice.id, target.id());
        assert!(
            matches!(notice.reason, ActorStopReason::AlreadyDead),
            "window regs carry AlreadyDead, got {:?}",
            notice.reason,
        );
    }

    /// An actor that WATCHES others and records the id of the last death it saw
    /// into a shared slot — the SUT for `spawn_linked` + the two-arm linked loop
    /// (card #195). Its overridden `on_link_died` returns `Continue`, so it merely
    /// observes (never propagates), which is what lets the test read the slot.
    struct Observer {
        seen: Arc<std::sync::Mutex<Option<ActorId>>>,
    }
    #[derive(Debug)]
    struct Never;
    impl Msg for Never {}
    impl Mailboxed for Observer {
        type Msg = Never;
    }
    impl crate::actor::Actor for Observer {
        type Args = Arc<std::sync::Mutex<Option<ActorId>>>;
        type Error = core::convert::Infallible;
        async fn on_start(seen: Self::Args, _: ActorRef<Self>) -> Result<Self, Self::Error> {
            Ok(Self { seen })
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
    impl crate::actor::Watch for Observer {
        async fn on_link_died(
            &mut self,
            id: ActorId,
            _reason: ActorStopReason,
            _linked: bool,
        ) -> Result<core::ops::ControlFlow<ActorStopReason>, Self::Error> {
            *self.seen.lock().expect("lock") = Some(id);
            Ok(core::ops::ControlFlow::Continue(()))
        }
    }

    /// Sequence (card #195): a `spawn_linked` actor actually RECEIVES a death on its
    /// link channel and its `on_link_died` runs. The watcher is spawned linked (so it
    /// has a link channel), its `link_tx` is registered on a plain `Counter` target,
    /// the target stops, and the watcher's overridden hook records the target id.
    /// FAILS without the two-arm linked loop (the link channel is never drained).
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn linked_actor_receives_death_of_watched_target() {
        use crate::actor::{Spawn, SpawnLinked};

        let seen = Arc::new(std::sync::Mutex::new(None::<ActorId>));
        let watcher = Observer::spawn_linked(Arc::clone(&seen));

        let handled = Arc::new(AtomicU32::new(0));
        let stopped = Arc::new(AtomicU32::new(0));
        let target = Counter::spawn((handled, stopped));

        // Register the watcher's link channel on the target directly (`ActorRef::watch`
        // is Task 9). A `watch` edge => `linked == false`.
        let link_tx = watcher
            .link_tx()
            .expect("a linked actor has a link channel")
            .clone();
        target
            .mailbox_sender()
            .send(Signal::Watch(Box::new(WatchReg {
                watcher: watcher.id(),
                link_tx,
                linked: false,
            })))
            .await
            .expect("registration delivered");

        target.stop();

        // Poll the shared slot under the fail-fast bound: if the linked loop never
        // drains the death, this bound FAILS FAST rather than hanging.
        bounded(async {
            while seen.lock().expect("lock").is_none() {
                tokio::task::yield_now().await;
            }
        })
        .await;
        assert_eq!(*seen.lock().expect("lock"), Some(target.id()));
    }

    /// Lifecycle (card #195): an `Unwatch` queued FIFO-behind its `Watch` and left
    /// in the mailbox at stop must be honored by the teardown drain — a former
    /// watcher receives NO death notice. Deterministic via cancel-before-drain
    /// (mirrors `cancel_token_stop_abandons_the_backlog`): the token is cancelled
    /// before `run` drains anything, so the loop handles neither signal and the
    /// whole `[Watch(1), Unwatch(1)]` backlog reaches the drain. FAILS while the
    /// drain applies `Watch` but ignores `Unwatch` (the removed watcher is spuriously
    /// notified).
    #[tokio::test]
    async fn unwatch_queued_before_stop_suppresses_notice() {
        let handled = Arc::new(AtomicU32::new(0));
        let stopped = Arc::new(AtomicU32::new(0));
        let prepared = PreparedActor::<Counter>::new(cap(8));
        let actor_ref = prepared.actor_ref().clone();
        let (tx, rx) = flume::unbounded::<LinkDied>();

        bounded(
            actor_ref
                .mailbox_sender()
                .send(Signal::Watch(Box::new(WatchReg {
                    watcher: ActorId::new(1),
                    link_tx: tx,
                    linked: false,
                }))),
        )
        .await
        .expect("watch enqueued");
        bounded(
            actor_ref
                .mailbox_sender()
                .send(Signal::Unwatch(ActorId::new(1))),
        )
        .await
        .expect("unwatch enqueued");
        actor_ref.stop(); // cancel BEFORE run() drains anything

        let outcome = bounded(prepared.run((handled, stopped))).await;
        assert!(matches!(
            outcome,
            RunResult::Stopped {
                reason: ActorStopReason::Normal,
                ..
            }
        ));
        assert!(
            rx.try_recv().is_err(),
            "an unwatched former watcher must NOT be notified at stop",
        );
    }

    /// Defensive (card #195): a `Watch` actor spawned via the plain [`Spawn`] path
    /// has no link channel, so `watch` returns [`ActorNotLinked`] rather than
    /// panicking — the runtime guard chosen over a typestate handle (ADR-0011).
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn plain_spawned_watch_actor_watch_errs() {
        use crate::actor::Spawn;

        let a = Panicker::spawn(()); // a `Watch` actor, but plain-spawned
        let b = Panicker::spawn(());
        assert_eq!(a.watch(&b).await, Err(ActorNotLinked));
    }

    /// Sequence (card #195): `a.link(&b)`; `b` dies abnormally (handler panic); `a`'s
    /// default `on_link_died` returns `Break`, so `a` stops too — the link
    /// propagated. FAILS if the linked loop never reacts to the death or the default
    /// hook does not propagate a linked abnormal exit.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn link_propagates_on_abnormal() {
        use crate::actor::SpawnLinked;

        let a = Panicker::spawn_linked(()); // default hook: Break on linked abnormal
        let b = Panicker::spawn_linked(());
        a.link(&b).await.expect("both linked, both can watch");

        b.tell(Boom).try_send().expect("provoke b's panic");

        // If the link propagates, `a` stops; poll under the fail-fast bound so a
        // broken propagation FAILS FAST here rather than hanging.
        bounded(async {
            while a.is_alive() {
                tokio::task::yield_now().await;
            }
        })
        .await;
        assert!(!a.is_alive(), "a linked abnormal death propagated to a");
    }

    /// Sequence (card #195): a linked peer that stops NORMALLY does not propagate —
    /// the survivor keeps running AFTER it has actually processed the death. The
    /// [`Recorder`] hook bumps `count` when the normal-death notice is delivered, so
    /// the bounded-wait on `count == 1` is a POSITIVE signal the loop reacted; only
    /// then is `is_alive()` asserted. Paired with Task 6's
    /// `default_hook_breaks_on_linked_abnormal_and_continues_otherwise` (which unit-
    /// tests the normal→`Continue` decision), this pins that the loop HONORS
    /// `Continue` on a delivered normal death. FAILS if the loop propagates it.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn link_does_not_propagate_on_normal() {
        use crate::actor::SpawnLinked;

        let probe = spawn_recorder();
        let handled = Arc::new(AtomicU32::new(0));
        let stopped = Arc::new(AtomicU32::new(0));
        let b = Counter::spawn_linked((handled, stopped));
        probe.handle.link(&b).await.expect("both linked");

        b.stop(); // normal stop

        // Robust positive signal: the normal-death notice was delivered AND the
        // recorder survived it (still handles a post-death Ping).
        assert_survived_one_death(&probe).await;
    }

    /// Sequence (card #195): a `Watch` actor overriding `on_link_died` to `Continue`
    /// (the `trap_exit` override) survives a linked ABNORMAL death. The [`Recorder`]
    /// hook bumps `count` when the abnormal-death notice is delivered; the bounded-
    /// wait on `count == 1` proves the death was delivered and the hook ran, so
    /// asserting `is_alive()` afterwards pins that the loop HONORED the `Continue`
    /// rather than not-yet-having-fired. FAILS if the loop ignores the hook's
    /// `ControlFlow` and propagates anyway.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn trap_exit_via_override_keeps_running() {
        use crate::actor::SpawnLinked;

        let probe = spawn_recorder(); // hook always Continues
        let b = Panicker::spawn_linked(());
        probe.handle.link(&b).await.expect("both linked");

        b.tell(Boom).try_send().expect("provoke b's panic");

        // Robust positive signal: the abnormal-death notice was delivered AND the
        // recorder survived it (still handles a post-death Ping) — the loop honored
        // the hook's Continue rather than propagating.
        assert_survived_one_death(&probe).await;
    }

    /// Defensive (card #195): watching an already-dead target delivers a `LinkDied`
    /// at once (Erlang's link-to-dead rule). The bounded-wait on the [`Recorder`]'s
    /// `count == 1` is the POSITIVE proof the synthetic notice was actually delivered
    /// to a's own channel and its hook ran (not merely that `watch` returned `Ok`);
    /// the recorded `(id, linked)` then pins the notice carries the dead target's id
    /// and `linked == false` (a `watch` edge). FAILS if `register_on` drops the
    /// dead-target branch — the counter never reaches 1 and the wait times out.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn dead_target_watch_immediate_linkdied() {
        use crate::actor::SpawnLinked;

        let probe = spawn_recorder();
        let b = Panicker::spawn_linked(());
        let b_id = b.id();

        b.kill();
        bounded(async {
            while b.is_alive() {
                tokio::task::yield_now().await;
            }
        })
        .await;

        // Watching a dead b must synthesize an immediate notice on a's channel.
        assert!(
            probe.handle.watch(&b).await.is_ok(),
            "watching a dead target still succeeds",
        );

        // Robust positive signal: the synthetic notice actually reached a's channel
        // and its hook ran (deaths == 1), and a survived it (post-death Ping).
        assert_survived_one_death(&probe).await;
        assert_eq!(
            *probe.last.lock().expect("lock"),
            Some((b_id, false)),
            "the synthetic dead-target notice carries b's id and a watch edge (linked == false)",
        );
        assert_eq!(
            probe.already_dead.load(Ordering::SeqCst),
            1,
            "link-to-dead carries AlreadyDead (Erlang noproc), never the fabricated \
             real reason of a target whose death was not observed",
        );
    }

    /// Sequence (card #195): [`ActorRef::unwatch`] actually removes the edge — after a
    /// watch followed by an unwatch, the target's death delivers NO notice. The
    /// recorder's biased loop drains any pending death BEFORE the post-stop `Ping`, so
    /// waiting `handled == 1` then asserting `deaths == 0` is a robust negative proof
    /// (not a race). FAILS if `unwatch` is a no-op — the edge survives and the notice
    /// fires (`deaths == 1`).
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn unwatch_removes_edge_so_death_delivers_no_notice() {
        use crate::actor::Spawn;

        let probe = spawn_recorder();
        let handled = Arc::new(AtomicU32::new(0));
        let stopped = Arc::new(AtomicU32::new(0));
        let target = Counter::spawn((handled, Arc::clone(&stopped)));

        probe
            .handle
            .watch(&target)
            .await
            .expect("watcher is linked");
        probe.handle.unwatch(&target).await;

        target.stop();
        bounded(async {
            while stopped.load(Ordering::SeqCst) == 0 {
                tokio::task::yield_now().await;
            }
        })
        .await;

        // The recorder's biased loop processes any pending death BEFORE this Ping,
        // so once the Ping is handled, `deaths` reflects reality.
        probe.handle.tell(Ping).try_send().expect("recorder alive");
        bounded(async {
            while probe.handled.load(Ordering::SeqCst) == 0 {
                tokio::task::yield_now().await;
            }
        })
        .await;
        assert_eq!(
            probe.deaths.load(Ordering::SeqCst),
            0,
            "an unwatched watcher receives no death notice",
        );
    }

    /// Defensive (card #195): `link`'s both-channels pre-check prevents a HALF-link —
    /// linking to a plain-spawned (unlinked) peer returns `Err` and installs NO edge in
    /// EITHER direction. If the pre-check is weakened (`||` → `&&`), the first
    /// `register_on` still lands on the peer before the second fails, leaving the peer
    /// watching `self`; the recorder would then receive the peer's death. Waiting the
    /// recorder's post-kill `Ping` (biased loop drains any death first) then asserting
    /// `deaths == 0` proves no half-edge survived.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn link_to_plain_peer_errs_without_half_link() {
        use crate::actor::Spawn;

        let probe = spawn_recorder(); // linked self, hook Continues + bumps `deaths`
        let peer = Panicker::spawn(()); // a `Watch` actor, but plain-spawned => link_tx None

        assert_eq!(
            probe.handle.link(&peer).await,
            Err(ActorNotLinked),
            "linking to an unlinked peer is rejected",
        );

        // A raw "fence" watcher, registered on `peer` AFTER `link` — so in `peer`'s
        // watcher list it drains AFTER any half-edge the mutant installed. Receiving
        // the fence's notice therefore deterministically proves `peer`'s
        // `Watchers::drop` already fired the (earlier) half-edge, if one exists. This
        // replaces a racy `peer.is_alive()` poll, which flips before the drop's
        // notifications actually run.
        let (fence_tx, fence_rx) = flume::unbounded::<LinkDied>();
        peer.mailbox_sender()
            .send(Signal::Watch(Box::new(WatchReg {
                watcher: ActorId::new(0xF),
                link_tx: fence_tx,
                linked: false,
            })))
            .await
            .expect("fence registered on peer");

        // Graceful stop (NOT kill): the teardown drain applies every pending
        // `Signal::Watch` (the fence, and any mutant half-edge) before notifying, so
        // the fence is guaranteed installed and fired — a `kill` would abort with the
        // fence still queued, dropping it unsent.
        peer.stop();
        bounded(fence_rx.recv_async())
            .await
            .expect("fence observed peer's death (teardown drained all edges)");

        // If a half-edge survived, the recorder's death is now on its link channel
        // (drained before the fence); its biased loop drains it before this Ping.
        probe.handle.tell(Ping).try_send().expect("recorder alive");
        bounded(async {
            while probe.handled.load(Ordering::SeqCst) == 0 {
                tokio::task::yield_now().await;
            }
        })
        .await;
        assert_eq!(
            probe.deaths.load(Ordering::SeqCst),
            0,
            "a rejected link leaves NO half-edge (the peer never watched self)",
        );
    }

    /// Defensive (card #195, ADR-0003): watching holds NO strong `ActorRef` to the
    /// target — the watcher list stores the watcher's own channel, not the target's.
    /// So dropping the target's last external strong ref still stops it. FAILS if a
    /// watch edge pins the target alive.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn watch_does_not_pin_target() {
        use crate::actor::SpawnLinked;

        let a = Trapper::spawn_linked(());
        let handled = Arc::new(AtomicU32::new(0));
        let stopped = Arc::new(AtomicU32::new(0));
        let b = Counter::spawn_linked((handled, Arc::clone(&stopped)));
        a.watch(&b).await.expect("a is linked, can watch");

        drop(b); // the last external strong ref to b

        // b must stop via ref-count-driven stop despite being watched.
        bounded(async {
            while stopped.load(Ordering::SeqCst) == 0 {
                tokio::task::yield_now().await;
            }
        })
        .await;
        assert_eq!(
            stopped.load(Ordering::SeqCst),
            1,
            "the watched target still stops when its last external ref drops",
        );
    }

    /// Linearizability (card #195): N watchers register on one target from the same
    /// instant (real overlap via a `Barrier`); each receives exactly one `LinkDied`
    /// when the target stops. Exercises the `SmallVec` spill past its inline slot.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn many_watchers_all_notified() {
        use crate::actor::Spawn;
        use tokio::sync::Barrier;

        let handled = Arc::new(AtomicU32::new(0));
        let stopped = Arc::new(AtomicU32::new(0));
        let target = Counter::spawn((handled, stopped));
        let n = 8usize;
        let barrier = Arc::new(Barrier::new(n));
        let mut receivers = Vec::new();
        let mut tasks = Vec::new();
        for i in 0..n {
            let (tx, rx) = flume::unbounded::<LinkDied>();
            receivers.push(rx);
            let sender = target.mailbox_sender().clone();
            let b = Arc::clone(&barrier);
            let watcher_id = ActorId::new(u64::try_from(i).expect("fits u64") + 1);
            tasks.push(tokio::spawn(async move {
                b.wait().await; // real overlap: all registrations race
                // Bounded: a broken run-loop that never drains the mailbox would
                // otherwise leave this send parked forever (the #179 pattern).
                bounded(sender.send(Signal::Watch(Box::new(WatchReg {
                    watcher: watcher_id,
                    link_tx: tx,
                    linked: false,
                }))))
                .await
                .expect("registration delivered");
            }));
        }
        for t in tasks {
            bounded(t).await.expect("registration task");
        }

        target.stop();
        for rx in receivers {
            let notice = bounded(rx.recv_async())
                .await
                .expect("each watcher is notified exactly once");
            assert_eq!(notice.id, target.id());
            // Exactly once: after the target is gone every link sender is dropped,
            // so a second recv on a correctly-single-notified channel is a clean
            // Disconnected — a duplicate-apply mutant delivers a second notice
            // here instead and FAILS.
            assert!(
                bounded(rx.recv_async()).await.is_err(),
                "a watcher must receive exactly one notice, not duplicates",
            );
        }
    }

    /// Defensive (card #195): a stale watcher edge self-prunes. A watcher registers,
    /// then drops its link-channel receiver (the watcher "dies"). When the target
    /// later stops, `Watchers::drop` `try_send`s onto the now-disconnected channel;
    /// that send fails and is silently skipped (`let _ = try_send`). The target must
    /// still stop cleanly — no panic, no leak. FAILS if the dead-edge send is
    /// unwrapped rather than dropped.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn stale_watcher_edge_self_prunes() {
        use crate::actor::Spawn;

        let handled = Arc::new(AtomicU32::new(0));
        let stopped = Arc::new(AtomicU32::new(0));
        let target = Counter::spawn((handled, Arc::clone(&stopped)));

        let (tx, rx) = flume::unbounded::<LinkDied>();
        target
            .mailbox_sender()
            .send(Signal::Watch(Box::new(WatchReg {
                watcher: ActorId::new(1),
                link_tx: tx,
                linked: false,
            })))
            .await
            .expect("registration delivered");

        drop(rx); // the watcher's receiver is gone => the edge is now stale

        target.stop();

        // The target must reach `on_stop` despite the dead edge; a bounded poll on the
        // shared `stopped` atomic fails fast rather than hanging if it never does.
        bounded(async {
            while stopped.load(Ordering::SeqCst) == 0 {
                tokio::task::yield_now().await;
            }
        })
        .await;
        assert_eq!(
            stopped.load(Ordering::SeqCst),
            1,
            "the target stops cleanly; the stale edge was skipped, not fatal",
        );
    }

    /// A backpressure fixture: its single handler blocks on `release` until the test
    /// lets it proceed, so its bounded (cap-1) mailbox can be deliberately saturated
    /// while the actor stays ALIVE.
    struct Gate {
        entered: Option<tokio::sync::oneshot::Sender<()>>,
        release: Option<tokio::sync::oneshot::Receiver<()>>,
    }
    #[derive(Debug)]
    struct Enter;
    impl Msg for Enter {}
    impl Mailboxed for Gate {
        type Msg = Enter;
    }
    impl crate::actor::Actor for Gate {
        type Args = (
            tokio::sync::oneshot::Sender<()>,
            tokio::sync::oneshot::Receiver<()>,
        );
        type Error = core::convert::Infallible;
        async fn on_start(
            (entered, release): Self::Args,
            _: ActorRef<Self>,
        ) -> Result<Self, Self::Error> {
            Ok(Self {
                entered: Some(entered),
                release: Some(release),
            })
        }
        async fn handle(
            &mut self,
            _: Enter,
            _: ActorRef<Self>,
            _: &mut bool,
        ) -> Result<(), Self::Error> {
            if let Some(entered) = self.entered.take() {
                let _ = entered.send(());
            }
            if let Some(release) = self.release.take() {
                let _ = release.await; // park here, holding the mailbox saturated
            }
            Ok(())
        }
    }

    /// `@bug` Defensive (card #195): the regression test for the Full/Closed
    /// conflation bug. Registration rides the target's bounded message mailbox, so a
    /// momentarily-full-but-ALIVE target must apply BACKPRESSURE — `send().await`
    /// waits for a slot — never be mistaken for dead. The buggy `try_send` returned
    /// `Full` for exactly this case and its `is_err()` synthesized a spurious
    /// `LinkDied { reason: AlreadyDead }`, which (for a `link` edge) self-terminates
    /// the watcher from ordinary backpressure. Here `a` watches a saturated target;
    /// the slot later frees, the edge installs, and `a` sees ONLY the target's real
    /// (Normal) death — `already_dead == 0`. FAILS with `try_send` (the spurious
    /// synthetic death bumps `already_dead`).
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn watch_full_but_alive_target_backpressures_no_spurious_death() {
        use crate::actor::Spawn;
        use tokio::sync::oneshot;

        let (entered_tx, entered_rx) = oneshot::channel();
        let (release_tx, release_rx) = oneshot::channel();
        let target = Gate::spawn_with_capacity(cap(1), (entered_tx, release_rx));

        // Saturate target while it stays alive: msg #1 enters the blocking handler
        // (dequeued), msg #2 then fills the single mailbox slot.
        bounded(target.tell(Enter)).await.expect("enqueue #1");
        bounded(entered_rx).await.expect("handler entered"); // #1 dequeued + parked
        bounded(target.tell(Enter))
            .await
            .expect("enqueue #2 fills the 1-slot mailbox");

        // a watches the full-but-alive target. Buggy `try_send` returns at once after
        // synthesizing a spurious AlreadyDead death; correct `send().await` PARKS
        // under backpressure until the slot frees.
        let a = spawn_recorder();
        let watch_task = {
            let watcher = a.handle.clone();
            let target = target.clone();
            tokio::spawn(async move { watcher.watch(&target).await })
        };

        // Free the mailbox: the handler returns, target drains msg #2, capacity opens
        // and the parked registration completes.
        release_tx.send(()).expect("release the gate");
        bounded(watch_task)
            .await
            .expect("watch task joins")
            .expect("watch succeeds");

        // Stop the target normally; the correctly-installed edge delivers a's ONLY death.
        target.stop();

        // Positive signal: a receives a death (real Normal stop with send().await; the
        // spurious AlreadyDead with the buggy try_send).
        bounded(async {
            while a.deaths.load(Ordering::SeqCst) == 0 {
                tokio::task::yield_now().await;
            }
        })
        .await;
        assert_eq!(
            a.already_dead.load(Ordering::SeqCst),
            0,
            "a full-but-alive target must backpressure, NOT synthesize a spurious death",
        );
        assert_eq!(
            a.deaths.load(Ordering::SeqCst),
            1,
            "exactly one death — the target's real stop, not a duplicate",
        );
        assert_eq!(
            *a.last.lock().expect("lock"),
            Some((target.id(), false)),
            "the death carries the target's id and a watch edge (linked == false)",
        );
    }
}
