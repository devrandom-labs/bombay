//! The minimal handle to a running actor (card #116 scaffold).
//!
//! Each field is independently cheap to clone and shares state, so no outer
//! `Arc` is needed here — the Arc/Weak ref-count semantics (last strong drop
//! stops the actor), `Recipient` erasure, and the `tell`/`ask` builders are
//! #117/#118. #116 exposes only what the hooks, spawn, and loop need.

use core::fmt;

use futures::stream::AbortHandle;
use tokio_util::sync::CancellationToken;

use crate::{
    actor::Actor,
    mailbox::{ActorId, MailboxSender, WeakMailboxSender},
    reply::ReplySender,
    request::{AskRequest, TellRequest},
};

/// A cloneable handle to a running actor: enqueue signals, stop it gracefully,
/// or kill it. Does **not** (yet) drive ref-count shutdown — see the module doc.
pub struct ActorRef<A: Actor> {
    id: ActorId,
    mailbox: MailboxSender<A>,
    cancel: CancellationToken,
    abort: AbortHandle,
}

impl<A: Actor> Clone for ActorRef<A> {
    fn clone(&self) -> Self {
        Self {
            id: self.id,
            mailbox: self.mailbox.clone(),
            cancel: self.cancel.clone(),
            abort: self.abort.clone(),
        }
    }
}

impl<A: Actor> fmt::Debug for ActorRef<A> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ActorRef")
            .field("id", &self.id)
            .field("actor", &A::name())
            .finish_non_exhaustive()
    }
}

impl<A: Actor> ActorRef<A> {
    pub(crate) const fn new(
        id: ActorId,
        mailbox: MailboxSender<A>,
        cancel: CancellationToken,
        abort: AbortHandle,
    ) -> Self {
        Self {
            id,
            mailbox,
            cancel,
            abort,
        }
    }

    /// The actor's scaffold identity (replaced by the AID in #121).
    #[must_use]
    pub const fn id(&self) -> ActorId {
        self.id
    }

    /// Whether the actor is still running (its mailbox is still open). A
    /// send-and-observe backup — never a pre-send gate (a send races the stop
    /// regardless), so prefer acting on the [`TellError`] a [`tell`](Self::tell)
    /// returns.
    #[must_use]
    pub fn is_alive(&self) -> bool {
        !self.mailbox.is_closed()
    }

    /// Prepares a fire-and-forget send of `msg` (card #118). The returned
    /// [`TellRequest`] does nothing until consumed:
    ///
    /// - `.await` — waits for mailbox capacity (backpressure), resolving to
    ///   [`TellError::ActorNotAlive`](crate::error::TellError::ActorNotAlive)
    ///   with `msg` handed back if the actor has stopped.
    /// - [`.try_send()`](TellRequest::try_send) — non-blocking; a full mailbox
    ///   is [`TellError::MailboxFull`](crate::error::TellError::MailboxFull).
    pub const fn tell(&self, msg: A::Msg) -> TellRequest<'_, A> {
        TellRequest::new(&self.mailbox, msg)
    }

    /// Prepares a request/reply `ask` (card #118): builds the message around a
    /// fresh typed reply port and returns an [`AskRequest`] that delivers it
    /// and awaits the reply on `.await`.
    ///
    /// ```ignore
    /// let count = actor_ref.ask(|reply| CounterMsg::Get { reply }).await?;
    /// ```
    ///
    /// One deadline (default [`DEFAULT_ASK_TIMEOUT`](crate::request::DEFAULT_ASK_TIMEOUT),
    /// override via [`timeout`](AskRequest::timeout), opt out via
    /// [`no_timeout`](AskRequest::no_timeout)) budgets delivery *and* reply.
    /// Handlers must never `ask(..).await` another actor (#122-#4) — that is
    /// the bounded-mailbox cycle deadlock; the deadline is the backstop, not
    /// the license.
    pub fn ask<R, E>(
        &self,
        make_msg: impl FnOnce(ReplySender<R, E>) -> A::Msg,
    ) -> AskRequest<'_, A, R, E> {
        AskRequest::new(&self.mailbox, make_msg)
    }

    /// The sender half of the actor's mailbox — used to enqueue `Signal`s. The
    /// ergonomic `tell`/`ask` builders wrap this in #118.
    #[must_use]
    pub const fn mailbox_sender(&self) -> &MailboxSender<A> {
        &self.mailbox
    }

    /// The loop's graceful-cancellation token (loop-internal).
    pub(crate) const fn cancel_token(&self) -> &CancellationToken {
        &self.cancel
    }

    /// Requests a graceful, out-of-band stop: the in-flight message finishes,
    /// then the actor stops and `on_stop` runs. Queued messages are abandoned.
    pub fn stop(&self) {
        self.cancel.cancel();
    }

    /// Hard-kills the actor: the task is aborted at its next await point,
    /// `on_stop` does **not** run, and any in-flight message is dropped.
    pub fn kill(&self) {
        self.abort.abort();
    }

    /// Downgrades to a non-pinning [`WeakActorRef`].
    #[must_use]
    pub fn downgrade(&self) -> WeakActorRef<A> {
        WeakActorRef {
            id: self.id,
            mailbox: self.mailbox.downgrade(),
            cancel: self.cancel.clone(),
            abort: self.abort.clone(),
        }
    }
}

/// A non-pinning handle to an actor. [`upgrade`](WeakActorRef::upgrade) yields a
/// strong [`ActorRef`] only while the actor's mailbox is still open.
pub struct WeakActorRef<A: Actor> {
    id: ActorId,
    mailbox: WeakMailboxSender<A>,
    cancel: CancellationToken,
    abort: AbortHandle,
}

impl<A: Actor> Clone for WeakActorRef<A> {
    fn clone(&self) -> Self {
        Self {
            id: self.id,
            mailbox: self.mailbox.clone(),
            cancel: self.cancel.clone(),
            abort: self.abort.clone(),
        }
    }
}

impl<A: Actor> fmt::Debug for WeakActorRef<A> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("WeakActorRef")
            .field("id", &self.id)
            .field("actor", &A::name())
            .finish_non_exhaustive()
    }
}

impl<A: Actor> WeakActorRef<A> {
    /// The actor's scaffold identity.
    #[must_use]
    pub const fn id(&self) -> ActorId {
        self.id
    }

    /// Upgrades to a strong [`ActorRef`], or `None` if the actor's mailbox has
    /// closed (every strong sender dropped).
    #[must_use]
    pub fn upgrade(&self) -> Option<ActorRef<A>> {
        self.mailbox
            .upgrade()
            .map(|mailbox| self.with_sender(mailbox))
    }

    /// Reassembles a strong [`ActorRef`] from this handle's cold fields
    /// (`id`/`cancel`/`abort`) plus a provided strong mailbox `sender`
    /// (ADR-0003). The run-loop uses it to lift the self-ref out of a
    /// `Signal::Message` — the message carries the only liveness-bearing handle
    /// (the sender), so the loop never holds a strong self-ref of its own.
    pub(crate) fn with_sender(&self, sender: MailboxSender<A>) -> ActorRef<A> {
        ActorRef {
            id: self.id,
            mailbox: sender,
            cancel: self.cancel.clone(),
            abort: self.abort.clone(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use futures::stream::AbortHandle;
    use tokio_util::sync::CancellationToken;

    use crate::{
        error::TellError,
        mailbox::{ActorId, Capacity, Mailbox, MailboxReceiver, Mailboxed},
        message::Msg,
    };

    // A minimal Actor purely to key the mailbox/ref. `on_start`/`handle` are
    // never called in this task's tests (no loop yet) — they exist so the type
    // satisfies `Actor`. `ProbeMsg` carries a `u64` so a delivery-failure test
    // can prove the *exact* undelivered message is handed back, not just the
    // variant (a ZST would make the handback unfalsifiable).
    struct Probe;
    #[derive(Debug)]
    struct ProbeMsg(u64);
    impl Msg for ProbeMsg {}
    impl Mailboxed for Probe {
        type Msg = ProbeMsg;
    }
    impl Actor for Probe {
        type Args = ();
        type Error = core::convert::Infallible;
        async fn on_start(_: (), _: ActorRef<Self>) -> Result<Self, Self::Error> {
            Ok(Probe)
        }
        async fn handle(
            &mut self,
            _: ProbeMsg,
            _: ActorRef<Self>,
            _: &mut bool,
        ) -> Result<(), Self::Error> {
            Ok(())
        }
    }

    // Keeps the `MailboxReceiver` alive so a test can *reap* the actor on its own
    // terms (dropping the receiver is exactly what the run-loop does on stop —
    // see the `mailbox` module doc). Dropping the receiver early would leave the
    // channel already disconnected before the test even begins.
    fn build_ref_with_rx() -> (ActorRef<Probe>, WeakActorRef<Probe>, MailboxReceiver<Probe>) {
        let cap = Capacity::try_from(4usize).expect("valid capacity");
        let (tx, rx) = Mailbox::<Probe>::bounded(cap);
        let (abort, _reg) = AbortHandle::new_pair();
        let actor_ref = ActorRef::new(ActorId::new(7), tx, CancellationToken::new(), abort);
        let weak = actor_ref.downgrade();
        (actor_ref, weak, rx)
    }

    fn build_ref() -> (ActorRef<Probe>, WeakActorRef<Probe>) {
        let (actor_ref, weak, _rx) = build_ref_with_rx();
        (actor_ref, weak)
    }

    /// The `ActorRef` debug view names the struct and surfaces its id and actor
    /// name — guards the hand-written `Debug` impl against being stubbed to an
    /// empty formatter (`Ok(Default::default())`).
    #[test]
    fn actor_ref_debug_names_struct_id_and_actor() {
        let (actor_ref, _weak) = build_ref();
        let shown = format!("{actor_ref:?}");
        assert!(
            shown.contains("ActorRef"),
            "debug names the struct: {shown}"
        );
        assert!(shown.contains('7'), "debug surfaces the id: {shown}");
        assert!(
            shown.contains("Probe"),
            "debug surfaces the actor name: {shown}"
        );
    }

    /// Same guard for the weak handle's `Debug` impl.
    #[test]
    fn weak_actor_ref_debug_names_struct_and_id() {
        let (_actor_ref, weak) = build_ref();
        let shown = format!("{weak:?}");
        assert!(
            shown.contains("WeakActorRef"),
            "debug names the struct: {shown}"
        );
        assert!(shown.contains('7'), "debug surfaces the id: {shown}");
        assert!(
            shown.contains("Probe"),
            "debug surfaces the actor name: {shown}"
        );
    }

    /// `Actor::name` defaults to the concrete type name — guards the trait
    /// default against being stubbed to a constant/empty string.
    #[test]
    fn actor_name_defaults_to_type_name() {
        assert!(
            Probe::name().contains("Probe"),
            "name() returns the type name, got {:?}",
            Probe::name(),
        );
    }

    /// Lifecycle: a weak ref upgrades while the mailbox is open, and returns
    /// `None` once every strong sender (incl. the one inside `ActorRef`) drops.
    #[tokio::test]
    async fn weak_upgrades_while_open_then_none_after_drop() {
        let (actor_ref, weak) = build_ref();
        assert_eq!(weak.id(), ActorId::new(7));
        assert!(weak.upgrade().is_some(), "mailbox open -> upgradable");

        drop(actor_ref);
        assert!(
            weak.upgrade().is_none(),
            "all strong senders dropped -> not upgradable",
        );
    }

    /// A `WeakActorRef` — even several clones of one — carries no pinning power:
    /// once the sole strong `ActorRef` drops, the mailbox channel is gone. Proven
    /// from both ends: the weak handle cannot re-`upgrade` to a strong sender, and
    /// the receiver observes the channel as disconnected (`recv` yields `None`).
    #[tokio::test]
    async fn weak_actor_ref_does_not_pin_channel() {
        let (actor_ref, weak, mut rx) = build_ref_with_rx();
        let weak_clone = weak.clone();

        drop(actor_ref); // only weak handles remain

        assert!(
            weak.upgrade().is_none(),
            "a WeakActorRef must not resurrect a strong sender",
        );
        assert!(
            weak_clone.upgrade().is_none(),
            "cloning the weak handle adds no pinning power",
        );
        assert!(
            rx.recv().await.is_none(),
            "a weak handle must not keep the mailbox channel open",
        );
    }

    /// `@bug` — a `WeakActorRef` captured while the actor was alive must never be
    /// a back door to resurrect it after the actor is reaped (every strong sender
    /// dropped *and* the run-loop's receiver gone). `upgrade` stays `None`, and
    /// re-cloning the stale handle is not a resurrection path either. The `id`
    /// survives as a tombstone (useful for logging a dead link) but must not
    /// imply liveness. FAILS if `upgrade` ever hands back a sender for a
    /// disconnected channel.
    #[tokio::test]
    async fn stale_ref_cannot_resurrect_reaped_actor() {
        let (actor_ref, _weak, rx) = build_ref_with_rx();
        let stale = actor_ref.downgrade();

        drop(actor_ref);
        drop(rx); // full reap: no senders, no receiver

        assert!(
            stale.upgrade().is_none(),
            "a reaped actor cannot be upgraded from a stale weak ref",
        );
        assert!(
            stale.clone().upgrade().is_none(),
            "cloning a stale weak ref does not resurrect the actor",
        );
        assert_eq!(
            stale.id(),
            ActorId::new(7),
            "the id survives as a tombstone, but that is not liveness",
        );
    }

    /// A `tell` to a reaped actor fails terminally with
    /// [`TellError::ActorNotAlive`], handing the *bare, undelivered* message
    /// straight back — nothing is lost into the void, and a retry loop can see
    /// (via `is_terminal`) that re-sending would only spin.
    #[tokio::test]
    async fn send_to_reaped_actor_returns_actor_not_alive() {
        let (actor_ref, _weak, rx) = build_ref_with_rx();

        drop(rx); // reap: the run-loop's receiver is gone

        let err = actor_ref
            .tell(ProbeMsg(42))
            .await
            .expect_err("tell to a reaped actor must fail");

        assert!(
            err.is_terminal(),
            "a reaped actor is terminal, never retryable",
        );
        let TellError::ActorNotAlive(ProbeMsg(returned)) = err else {
            panic!("expected ActorNotAlive carrying the message, got {err:?}");
        };
        assert_eq!(returned, 42, "the exact undelivered message is handed back");
    }

    /// `with_sender` cannot be reached by cargo-mutants' whole-body
    /// replacement (it returns `ActorRef<A>`, which has no `Default`, so no
    /// mutant compiles — `known_zero_viable` in `mutants-baseline.json`,
    /// card #165). This is the hand-written compensating control: it proves
    /// `with_sender` copies the WEAK ref's `id`/`cancel`/`abort` VERBATIM
    /// (not fresh values) by observing shared state through each field.
    ///
    /// - `id`: a plain `Copy` value — `assert_eq!` is a direct, exact check.
    /// - `cancel`: `CancellationToken::clone` "will get cancelled whenever
    ///   the current token gets cancelled, and vice versa" (tokio-util
    ///   `cancellation_token.rs` doc on `impl Clone`) — so cancelling
    ///   through the rebuilt ref must flip the ORIGINAL's token too, which a
    ///   fresh `CancellationToken::new()` could never do.
    /// - `abort`: `AbortHandle` clones share one `Arc<AbortInner>` (futures
    ///   `abortable.rs`) — `is_aborted` reads that shared `AtomicBool`. Both
    ///   fields are private but observable here: `tests` is a descendant
    ///   module of the struct's defining module, so plain field access
    ///   (`.cancel`, `.abort`) is in scope without needing a public
    ///   liveness-identity accessor that would otherwise leak the field.
    #[tokio::test]
    async fn with_sender_preserves_id_cancel_and_abort() {
        let (actor_ref, weak, _rx) = build_ref_with_rx();
        let self_sender = actor_ref.mailbox_sender().clone();

        let rebuilt = weak.with_sender(self_sender);

        assert_eq!(
            rebuilt.id(),
            ActorId::new(7),
            "id must be copied verbatim from the weak ref",
        );

        assert!(
            !actor_ref.cancel.is_cancelled(),
            "precondition: nothing cancelled yet",
        );
        rebuilt.stop();
        assert!(
            actor_ref.cancel.is_cancelled(),
            "with_sender must copy the SAME cancellation token as the \
             original — a fresh token would leave the original uncancelled",
        );

        assert!(
            !actor_ref.abort.is_aborted(),
            "precondition: nothing aborted yet",
        );
        rebuilt.kill();
        assert!(
            actor_ref.abort.is_aborted(),
            "with_sender must copy the SAME abort handle as the original — \
             a fresh handle would leave the original's abort unset",
        );
    }

    /// Liveness is a property of the shared channel, not of any one handle:
    /// `is_alive`/`is_closed` read identically across cloned senders. A surviving
    /// clone keeps the actor alive after the original drops; reaping the actor
    /// (receiver gone) flips *every* clone to closed at once.
    #[tokio::test]
    async fn cloned_sender_liveness_via_is_closed() {
        let (actor_ref, _weak, rx) = build_ref_with_rx();
        let clone = actor_ref.clone();

        assert!(actor_ref.is_alive(), "original sees a live actor");
        assert!(clone.is_alive(), "clone sees the same live actor");
        assert!(
            !clone.mailbox_sender().is_closed(),
            "an open channel is not closed",
        );

        // Dropping the original strong handle does not close the channel: the
        // clone is still a strong sender and the receiver is still up.
        drop(actor_ref);
        assert!(clone.is_alive(), "a surviving clone keeps liveness true",);

        // Reaping the actor flips liveness for the clone too — is_closed reflects
        // the shared channel, not the individual handle.
        drop(rx);
        assert!(!clone.is_alive(), "the clone observes the reap");
        assert!(
            clone.mailbox_sender().is_closed(),
            "and reports the channel as closed",
        );
    }
}
