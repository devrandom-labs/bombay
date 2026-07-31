//! `pipe_to_self` / `pipe_ask` (card #226): the sanctioned non-blocking
//! ask-from-handler. Fire a future, END the turn, take the result later as an
//! ordinary message — the ITP-liveness escape hatch the `request.rs` ask ban
//! points at. Design record:
//! `docs/superpowers/specs/2026-07-28-226-pipe-to-self-design.md`,
//! ADR-0017.

use core::future::Future;
use std::panic::AssertUnwindSafe;

use futures::FutureExt as _;

use crate::{
    actor::{Actor, ActorRef, WeakActorRef},
    error::{AskError, PanicError, PanicReason, PipeAskError, TellError},
    reply::ReplySender,
    trace,
};

impl<E> PipeAskError<E> {
    /// Flattens the generic pipe's nested outcome into the single-match shape.
    fn flatten<R, M>(out: Result<Result<R, AskError<M, E>>, PanicError>) -> Result<R, Self> {
        match out {
            Ok(Ok(r)) => Ok(r),
            Ok(Err(AskError::Deliver(TellError::ActorNotAlive(_)))) => Err(Self::TargetDead),
            Ok(Err(AskError::Deliver(TellError::MailboxFull(_)))) => Err(Self::MailboxFull),
            Ok(Err(AskError::Deliver(TellError::SendTimeout(_)))) => Err(Self::SendTimeout),
            Ok(Err(AskError::Timeout)) => Err(Self::ReplyTimeout),
            Ok(Err(AskError::Interrupted)) => Err(Self::Interrupted),
            Ok(Err(AskError::Handler(e))) => Err(Self::Handler(e)),
            Err(panic) => Err(Self::Panicked(panic)),
        }
    }
}

impl<A: Actor> ActorRef<A> {
    /// Asks `target` without blocking the turn: [`pipe_to_self`] specialized
    /// for the ask case, with the error union flattened once (here) instead of
    /// at every call site, and the target cloned internally (no caller-side
    /// clone dance). Uses the ask builder's default deadline, so an accidental
    /// cycle self-resolves as a timeout instead of deadlocking.
    ///
    /// [`pipe_to_self`]: ActorRef::pipe_to_self
    pub fn pipe_ask<B, R, E, F, M>(&self, target: &ActorRef<B>, make_msg: F, mapper: M)
    where
        B: Actor,
        R: Send + 'static,
        E: Send + 'static,
        F: FnOnce(ReplySender<R, E>) -> B::Msg + Send + 'static,
        M: FnOnce(Result<R, PipeAskError<E>>) -> A::Msg + Send + 'static,
    {
        let owned = target.clone();
        spawn_pipe(
            self.downgrade(),
            async move { owned.ask(make_msg).await },
            move |out| mapper(PipeAskError::flatten(out)),
        );
    }
}

impl<A: Actor> ActorRef<A> {
    /// Runs `future` on a detached task and delivers its outcome to this
    /// actor as an ordinary message — the sanctioned non-blocking alternative
    /// to `ask(..).await` inside a handler (which is the bounded-mailbox
    /// cycle deadlock; see the ban in [`crate::request`]).
    ///
    /// The task holds only a [`WeakActorRef`], so an in-flight pipe never
    /// keeps the actor alive (ADR-0003 / ADR-0017). If the actor is gone when
    /// the future resolves, the result is dropped. A panic in `future`
    /// reaches `mapper` as an `Err` carrying [`PanicError`] — the actor decides; the
    /// panic never touches the actor itself. Delivery waits for mailbox
    /// capacity like any sender (backpressure, not a failure). No ordering is
    /// guaranteed relative to other senders.
    ///
    /// `mapper` must not panic: a mapper panic kills only the pipe task and
    /// the result is dropped (a `tracing` error event records it).
    pub fn pipe_to_self<T, F, M>(&self, future: F, mapper: M)
    where
        T: Send + 'static,
        F: Future<Output = T> + Send + 'static,
        M: FnOnce(Result<T, PanicError>) -> A::Msg + Send + 'static,
    {
        spawn_pipe(self.downgrade(), future, mapper);
    }
}

/// The shared non-pinning delayed-self-send primitive (#223/#230 reuse seam):
/// weak-hold while pending, upgrade-or-drop at resolution, backpressured
/// send, closed-mailbox failure swallowed.
pub fn spawn_pipe<A, T, F, M>(weak: WeakActorRef<A>, future: F, mapper: M)
where
    A: Actor,
    T: Send + 'static,
    F: Future<Output = T> + Send + 'static,
    M: FnOnce(Result<T, PanicError>) -> A::Msg + Send + 'static,
{
    let _join = tokio::spawn(async move {
        let out = AssertUnwindSafe(future)
            .catch_unwind()
            .await
            .map_err(|payload| PanicError::from_panic_any(payload, PanicReason::PipedFuture));
        // Dead or in the drain window: no external handle may reach the actor
        // (ADR-0010), the result is dropped — the spec'd fate.
        let Some(strong) = weak.upgrade() else {
            trace::pipe_result_dropped::<A>();
            return;
        };
        let Ok(msg) = std::panic::catch_unwind(AssertUnwindSafe(|| mapper(out))) else {
            trace::pipe_mapper_panicked::<A>();
            return;
        };
        // Race with stop/kill: the mailbox closed between upgrade and enqueue.
        // Nothing to hand the message back to — swallow, per the fate table.
        if strong.tell(msg).await.is_err() {
            trace::pipe_result_dropped::<A>();
        }
    });
}

#[cfg(test)]
mod tests {
    use core::time::Duration;
    use std::sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    };

    use crate::{
        actor::{Actor, ActorRef, Flow, Spawn as _},
        error::{PanicError, PanicReason, PipeAskError},
        mailbox::Mailboxed,
        message::Msg,
        reply::ReplySender,
        test_support::terminate_bound,
    };

    /// Accumulates piped outcomes so tests can ask for what arrived.
    struct Sink {
        seen: Vec<Result<u32, String>>,
    }

    #[derive(Debug)]
    enum SinkMsg {
        /// The mapped pipe result re-entering the menu.
        Piped(Result<u32, PanicError>),
        /// The mapped pipe_ask result re-entering the menu.
        PipedAsk(Result<u32, String>),
        /// Read back everything that arrived.
        Read(ReplySender<Vec<Result<u32, String>>>),
    }
    impl Msg for SinkMsg {}
    impl Mailboxed for Sink {
        type Msg = SinkMsg;
    }
    impl Actor for Sink {
        type Args = ();
        type Error = core::convert::Infallible;
        async fn on_start(_: (), _: ActorRef<Self>) -> Result<Self, Self::Error> {
            Ok(Self { seen: Vec::new() })
        }
        async fn handle(&mut self, msg: SinkMsg, _: ActorRef<Self>) -> Result<Flow, Self::Error> {
            match msg {
                SinkMsg::Piped(res) => {
                    self.seen
                        .push(res.map_err(|e| e.with_str(|s| s.to_owned()).unwrap_or_default()));
                }
                SinkMsg::PipedAsk(res) => self.seen.push(res),
                SinkMsg::Read(reply) => drop(reply.send(self.seen.clone())),
            }
            Ok(Flow::Continue)
        }
    }

    /// Round-trip: a real future's value re-enters through the mailbox as the
    /// mapped menu variant and mutates state (card invariant 1).
    #[tokio::test]
    async fn piped_future_result_arrives_as_mapped_message() {
        let actor_ref = Sink::spawn(());
        actor_ref.pipe_to_self(async { 6u32 * 7 }, SinkMsg::Piped);

        let seen = tokio::time::timeout(terminate_bound(), wait_for_seen(&actor_ref, 1))
            .await
            .expect("piped result must arrive within the bound");
        assert_eq!(seen, vec![Ok(42)], "the exact mapped value round-trips");
    }

    /// A panic inside the piped future arrives at the mapper as
    /// `Err(PanicError)` with `PanicReason::PipedFuture`, and the actor keeps
    /// running (card invariant 5; the panic is not the actor's turn).
    #[tokio::test]
    async fn piped_panic_reaches_mapper_typed_and_actor_survives() {
        let actor_ref = Sink::spawn(());
        actor_ref.pipe_to_self(
            async { panic!("boom in the pipe") },
            |res: Result<u32, PanicError>| {
                let err = res.expect_err("the piped panic must surface as Err");
                assert_eq!(
                    err.reason(),
                    PanicReason::PipedFuture,
                    "the panic is attributed to the pipe, not a hook or turn",
                );
                SinkMsg::Piped(Err(err))
            },
        );

        let seen = tokio::time::timeout(terminate_bound(), wait_for_seen(&actor_ref, 1))
            .await
            .expect("the mapped panic must arrive within the bound");
        assert_eq!(seen.len(), 1);
        let e = seen[0].as_ref().expect_err("stored as the Err arm");
        assert!(
            e.contains("boom in the pipe"),
            "the payload string survives into PanicError, got: {e}",
        );
        assert!(
            actor_ref.is_alive(),
            "a piped-future panic must never kill the actor",
        );
    }

    /// An in-flight pipe holds only a weak ref: an actor whose ONLY remaining
    /// tie is a never-resolving pipe still ref-count-stops when the last
    /// external strong ref drops (card invariant 2, ADR-0003/ADR-0017).
    #[tokio::test]
    async fn in_flight_pipe_does_not_pin_refcount_stop() {
        let actor_ref = Sink::spawn(());
        actor_ref.pipe_to_self(
            async {
                core::future::pending::<()>().await;
                0u32
            },
            SinkMsg::Piped,
        );
        let weak = actor_ref.downgrade();
        drop(actor_ref);

        // The actor must die: the weak handle stops upgrading within the bound.
        tokio::time::timeout(terminate_bound(), async {
            loop {
                if weak.upgrade().is_none() {
                    return;
                }
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .expect("with only a pending pipe left, the actor must ref-count-stop");
    }

    /// Fires a oneshot when dropped — how the tests observe "the detached
    /// pipe task exited", whichever path it took.
    struct ExitGuard(Option<tokio::sync::oneshot::Sender<()>>);
    impl Drop for ExitGuard {
        fn drop(&mut self) {
            if let Some(tx) = self.0.take() {
                let _sent = tx.send(());
            }
        }
    }

    /// Actor dead before the pipe resolves: the result is dropped, the mapper
    /// never runs, and the detached task exits cleanly — no panic against the
    /// dead handle (card invariant 3).
    #[tokio::test]
    async fn actor_dead_before_resolution_drops_result_cleanly() {
        let (gate_tx, gate_rx) = tokio::sync::oneshot::channel::<()>();
        let (exit_tx, exit_rx) = tokio::sync::oneshot::channel::<()>();
        let mapper_ran = Arc::new(AtomicBool::new(false));

        let actor_ref = Sink::spawn(());
        let guard = ExitGuard(Some(exit_tx));
        let ran = Arc::clone(&mapper_ran);
        actor_ref.pipe_to_self(
            async move {
                let _ = gate_rx.await;
                7u32
            },
            move |res| {
                let _hold = guard;
                ran.store(true, Ordering::SeqCst);
                SinkMsg::Piped(res)
            },
        );

        // Kill the actor first, then let the pipe resolve.
        let weak = actor_ref.downgrade();
        drop(actor_ref);
        tokio::time::timeout(terminate_bound(), async {
            while weak.upgrade().is_some() {
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .expect("actor stops once its last strong ref drops");

        gate_tx
            .send(())
            .expect("pipe future is waiting on the gate");
        tokio::time::timeout(terminate_bound(), exit_rx)
            .await
            .expect("the detached task must exit within the bound")
            .expect("guard dropped, not leaked");
        assert!(
            !mapper_ran.load(Ordering::SeqCst),
            "a dead actor's mapper must never run — the result is dropped whole",
        );
    }

    /// Kill race (card invariant 6): external strong refs still alive, mailbox
    /// already closed by kill. The upgrade succeeds, the tell fails — and the
    /// failure is swallowed, no panic in the detached task.
    #[tokio::test]
    async fn pipe_resolving_into_killed_mailbox_is_swallowed() {
        let (gate_tx, gate_rx) = tokio::sync::oneshot::channel::<()>();
        let (exit_tx, exit_rx) = tokio::sync::oneshot::channel::<()>();

        let actor_ref = Sink::spawn(());
        let guard = ExitGuard(Some(exit_tx));
        actor_ref.pipe_to_self(
            async move {
                let _ = gate_rx.await;
                9u32
            },
            move |res| {
                let _hold = guard;
                SinkMsg::Piped(res)
            },
        );

        actor_ref.kill();
        // Keep `actor_ref` (strong) alive across the resolution on purpose.
        gate_tx
            .send(())
            .expect("pipe future is waiting on the gate");
        tokio::time::timeout(terminate_bound(), exit_rx)
            .await
            .expect("the detached task must exit despite the closed mailbox")
            .expect("guard dropped, not leaked");
        drop(actor_ref);
    }

    /// Polls the sink until `n` outcomes arrived (each poll is itself a
    /// bounded ask; the outer timeout bounds the whole wait).
    async fn wait_for_seen(actor_ref: &ActorRef<Sink>, n: usize) -> Vec<Result<u32, String>> {
        loop {
            let seen = actor_ref
                .ask(|reply| SinkMsg::Read(reply))
                .await
                .expect("sink replies while alive");
            if seen.len() >= n {
                return seen;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    }

    /// A responder whose reply waits on an external gate.
    struct GatedB {
        gate: Option<tokio::sync::oneshot::Receiver<()>>,
    }
    #[derive(Debug)]
    enum GatedBMsg {
        Get(ReplySender<u32>),
    }
    impl Msg for GatedBMsg {}
    impl Mailboxed for GatedB {
        type Msg = GatedBMsg;
    }
    impl Actor for GatedB {
        type Args = tokio::sync::oneshot::Receiver<()>;
        type Error = core::convert::Infallible;
        async fn on_start(gate: Self::Args, _: ActorRef<Self>) -> Result<Self, Self::Error> {
            Ok(Self { gate: Some(gate) })
        }
        async fn handle(
            &mut self,
            GatedBMsg::Get(reply): GatedBMsg,
            _: ActorRef<Self>,
        ) -> Result<Flow, Self::Error> {
            if let Some(gate) = self.gate.take() {
                let _opened = gate.await;
            }
            let _ = reply.send(99);
            Ok(Flow::Continue)
        }
    }

    /// ITP liveness (card invariant 4): A pipes an ask to B and KEEPS
    /// PROCESSING while B's reply is pending. Order proved by construction:
    /// B's gate opens only after A answered a second message, so if the pipe
    /// blocked A's turn, this test would deadlock (and the bound would trip).
    #[tokio::test]
    async fn actor_keeps_processing_while_piped_ask_is_pending() {
        let (gate_tx, gate_rx) = tokio::sync::oneshot::channel::<()>();
        let b_ref = GatedB::spawn(gate_rx);
        let a_ref = Sink::spawn(());

        // Fire the pipe from outside a handler for setup simplicity — the
        // mechanism is identical (same detached task); the in-handler shape
        // is exercised by the doc example test in Task 8.
        let b = b_ref.clone();
        a_ref.pipe_to_self(
            async move {
                b.ask(|reply| GatedBMsg::Get(reply))
                    .no_timeout()
                    .await
                    .expect("B replies once the gate opens")
            },
            |res: Result<u32, PanicError>| SinkMsg::Piped(res),
        );

        // OVERLAP PROOF: while B is gated (reply pending), A answers an ask.
        let seen = tokio::time::timeout(terminate_bound(), a_ref.ask(|reply| SinkMsg::Read(reply)))
            .await
            .expect("A must answer while the piped ask is still pending")
            .expect("A replies");
        assert!(
            seen.is_empty(),
            "the piped result cannot have arrived yet — B is gated",
        );

        // Only now open the gate; the piped reply then lands in A.
        gate_tx.send(()).expect("B is waiting on the gate");
        let seen = tokio::time::timeout(terminate_bound(), wait_for_seen(&a_ref, 1))
            .await
            .expect("the piped reply arrives after the gate opens");
        assert_eq!(seen, vec![Ok(99)]);
    }

    /// Sugar round-trip (widened card scope): `pipe_ask` borrows the target
    /// (no caller-side clone — the `&b_ref` at the call site IS the
    /// assertion) and the mapper receives ONE flat Result.
    #[tokio::test]
    async fn pipe_ask_delivers_flat_ok() {
        let (gate_tx, gate_rx) = tokio::sync::oneshot::channel::<()>();
        gate_tx.send(()).expect("open the gate up front");
        let b_ref = GatedB::spawn(gate_rx);
        let a_ref = Sink::spawn(());

        a_ref.pipe_ask(
            &b_ref,
            |reply| GatedBMsg::Get(reply),
            |res: Result<u32, PipeAskError>| SinkMsg::PipedAsk(res.map_err(|e| e.to_string())),
        );

        let seen = tokio::time::timeout(terminate_bound(), wait_for_seen(&a_ref, 1))
            .await
            .expect("flat Ok arrives");
        assert_eq!(seen, vec![Ok(99)]);
    }

    /// Every source variant maps to a DISTINCT flat variant (lossless by
    /// variant; widened card invariant). Pure-fn test: exact, no timing.
    #[test]
    fn flatten_maps_every_variant_distinctly() {
        use crate::error::{AskError, PipeAskError, TellError};
        let f = PipeAskError::<&'static str>::flatten::<u32, ()>;

        assert!(matches!(f(Ok(Ok(1))), Ok(1)));
        assert!(matches!(
            f(Ok(Err(AskError::Deliver(TellError::ActorNotAlive(()))))),
            Err(PipeAskError::TargetDead)
        ));
        assert!(matches!(
            f(Ok(Err(AskError::Deliver(TellError::MailboxFull(()))))),
            Err(PipeAskError::MailboxFull)
        ));
        assert!(matches!(
            f(Ok(Err(AskError::Deliver(TellError::SendTimeout(()))))),
            Err(PipeAskError::SendTimeout)
        ));
        assert!(matches!(
            f(Ok(Err(AskError::Timeout))),
            Err(PipeAskError::ReplyTimeout)
        ));
        assert!(matches!(
            f(Ok(Err(AskError::Interrupted))),
            Err(PipeAskError::Interrupted)
        ));
        assert!(matches!(
            f(Ok(Err(AskError::Handler("conflict")))),
            Err(PipeAskError::Handler("conflict"))
        ));
        let panic_err = PanicError::from_panic_any(Box::new("boom"), PanicReason::PipedFuture);
        assert!(matches!(f(Err(panic_err)), Err(PipeAskError::Panicked(_))));
    }

    /// Dead target end-to-end: piping an ask at a dead actor lands
    /// `PipeAskError::TargetDead` in the mapper.
    #[tokio::test]
    async fn pipe_ask_dead_target_flattens_to_target_dead() {
        let (_gate_tx, gate_rx) = tokio::sync::oneshot::channel::<()>();
        let b_ref = GatedB::spawn(gate_rx);
        b_ref.kill();
        tokio::time::timeout(terminate_bound(), async {
            while b_ref.is_alive() {
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .expect("killed B closes its mailbox within the bound");

        let a_ref = Sink::spawn(());
        a_ref.pipe_ask(
            &b_ref,
            |reply| GatedBMsg::Get(reply),
            |res: Result<u32, PipeAskError>| {
                assert!(
                    matches!(res, Err(PipeAskError::TargetDead)),
                    "dead target must flatten to TargetDead, got {res:?}",
                );
                SinkMsg::PipedAsk(Ok(0)) // sentinel: mapper ran with the right arm
            },
        );
        let seen = tokio::time::timeout(terminate_bound(), wait_for_seen(&a_ref, 1))
            .await
            .expect("mapped sentinel arrives");
        assert_eq!(seen, vec![Ok(0)]);
    }

    /// A small domain error for the handler-error arm.
    #[derive(Debug, Clone, PartialEq, Eq)]
    struct Conflict;

    /// A responder whose handler replies with a domain error.
    struct FailingB;
    #[derive(Debug)]
    enum FailingBMsg {
        Get(ReplySender<u32, Conflict>),
    }
    impl Msg for FailingBMsg {}
    impl Mailboxed for FailingB {
        type Msg = FailingBMsg;
    }
    impl Actor for FailingB {
        type Args = ();
        type Error = core::convert::Infallible;
        async fn on_start(_: (), _: ActorRef<Self>) -> Result<Self, Self::Error> {
            Ok(Self)
        }
        async fn handle(
            &mut self,
            FailingBMsg::Get(reply): FailingBMsg,
            _: ActorRef<Self>,
        ) -> Result<Flow, Self::Error> {
            let _ = reply.send_err(Conflict);
            Ok(Flow::Continue)
        }
    }

    /// Handler domain error end-to-end: flattens to `Handler(E)` un-erased.
    #[tokio::test]
    async fn pipe_ask_handler_error_flattens_unerased() {
        let b_ref = FailingB::spawn(());
        let a_ref = Sink::spawn(());

        a_ref.pipe_ask(
            &b_ref,
            |reply| FailingBMsg::Get(reply),
            |res: Result<u32, PipeAskError<Conflict>>| {
                assert!(
                    matches!(res, Err(PipeAskError::Handler(Conflict))),
                    "handler error must flatten un-erased, got {res:?}",
                );
                SinkMsg::PipedAsk(Ok(0))
            },
        );
        let seen = tokio::time::timeout(terminate_bound(), wait_for_seen(&a_ref, 1))
            .await
            .expect("mapped sentinel arrives");
        assert_eq!(seen, vec![Ok(0)]);
    }

    /// Widened invariant 4: the sugar inherits non-pinning by delegation — an
    /// in-flight pipe_ask holds no strong ref to A.
    #[tokio::test]
    async fn in_flight_pipe_ask_does_not_pin_refcount_stop() {
        let (_gate_tx, gate_rx) = tokio::sync::oneshot::channel::<()>();
        let b_ref = GatedB::spawn(gate_rx); // gate never opens; B never replies
        let a_ref = Sink::spawn(());
        a_ref.pipe_ask(
            &b_ref,
            |reply| GatedBMsg::Get(reply),
            |res: Result<u32, PipeAskError>| SinkMsg::PipedAsk(res.map_err(|e| e.to_string())),
        );
        let weak = a_ref.downgrade();
        drop(a_ref);
        tokio::time::timeout(terminate_bound(), async {
            while weak.upgrade().is_some() {
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .expect("A must ref-count-stop while its pipe_ask is still pending");
        drop(b_ref);
    }
}
