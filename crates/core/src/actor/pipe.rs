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
    error::{PanicError, PanicReason},
    trace,
};

impl<A: Actor> ActorRef<A> {
    /// Runs `future` on a detached task and delivers its outcome to this
    /// actor as an ordinary message — the sanctioned non-blocking alternative
    /// to `ask(..).await` inside a handler (which is the bounded-mailbox
    /// cycle deadlock; see the ban in [`crate::request`]).
    ///
    /// The task holds only a [`WeakActorRef`], so an in-flight pipe never
    /// keeps the actor alive (ADR-0003 / ADR-0017). If the actor is gone when
    /// the future resolves, the result is dropped. A panic in `future`
    /// reaches `mapper` as `Err(`[`PanicError`]`)` — the actor decides; the
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
pub(crate) fn spawn_pipe<A, T, F, M>(weak: WeakActorRef<A>, future: F, mapper: M)
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
        let msg = match std::panic::catch_unwind(AssertUnwindSafe(|| mapper(out))) {
            Ok(msg) => msg,
            Err(_) => {
                trace::pipe_mapper_panicked::<A>();
                return;
            }
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

    use crate::{
        actor::{Actor, ActorRef, Spawn as _},
        error::{PanicError, PanicReason},
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
        async fn handle(
            &mut self,
            msg: SinkMsg,
            _: ActorRef<Self>,
            _: &mut bool,
        ) -> Result<(), Self::Error> {
            match msg {
                SinkMsg::Piped(res) => {
                    self.seen
                        .push(res.map_err(|e| e.with_str(|s| s.to_owned()).unwrap_or_default()));
                }
                SinkMsg::Read(reply) => drop(reply.send(self.seen.clone())),
            }
            Ok(())
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
}
