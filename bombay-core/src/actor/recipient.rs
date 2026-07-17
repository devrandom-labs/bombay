//! Type-erased, zero-box fan-in handles (card #145).
//!
//! A [`Recipient<M>`] erases the actor type behind a uniform interface so a
//! `Vec<Recipient<M>>` can address **heterogeneous** actors and broadcast one
//! `M` to all of them. It targets any actor whose closed menu satisfies
//! `A::Msg: From<M>`: the send converts `M -> A::Msg` **by value** (never boxing
//! the message) and enqueues it — only the handle sits behind an `Arc<dyn …>`.
//! See ADR-0004 and `docs/superpowers/specs/2026-07-13-145-recipient-design.md`.

use std::sync::Arc;

use core::{any::type_name, fmt};

use futures::future::BoxFuture;

use crate::{
    actor::{Actor, ActorRef, WeakActorRef},
    error::TellError,
    mailbox::{ActorId, TrySendError},
};

/// The erased operations a [`Recipient<M>`] needs from a concrete actor whose
/// menu accepts `M`. `M` is the trait's parameter (not a method generic), so
/// `dyn ErasedRecipient<M>` is object-safe.
trait ErasedRecipient<M>: Send + Sync {
    /// Awaiting send: convert `M -> A::Msg` and enqueue, waiting for capacity.
    /// Boxes the future (the cost of `dyn` async dispatch) — never the message.
    fn tell(&self, msg: M) -> BoxFuture<'_, Result<(), TellError<M>>>;
    /// Non-blocking send: convert `M -> A::Msg` and enqueue, or hand `M` back.
    fn try_tell(&self, msg: M) -> Result<(), TellError<M>>;
    /// The target actor's identity (preserved through erasure).
    fn id(&self) -> ActorId;
    /// Whether the target's mailbox is still open.
    fn is_alive(&self) -> bool;
    /// Downgrades to a non-pinning erased handle.
    fn downgrade(&self) -> WeakRecipient<M>;
}

impl<A, M> ErasedRecipient<M> for ActorRef<A>
where
    A: Actor,
    A::Msg: From<M>,
    M: Clone + Send + 'static,
{
    fn tell(&self, msg: M) -> BoxFuture<'_, Result<(), TellError<M>>> {
        Box::pin(async move {
            let converted = A::Msg::from(msg.clone());
            match self.mailbox_sender().send_message(converted).await {
                Ok(()) => Ok(()),
                // The awaiting path only fails when the mailbox is closed.
                Err(_undelivered) => Err(TellError::ActorNotAlive(msg)),
            }
        })
    }

    fn try_tell(&self, msg: M) -> Result<(), TellError<M>> {
        // Clone `M` before converting: erasure leaves no `A::Msg -> M` path, so
        // the retained original is the only way to hand a typed `M` back on
        // failure (ADR-0004). The converted `A::Msg` lives inline in the queued
        // signal — the message never hits the heap.
        let converted = A::Msg::from(msg.clone());
        match self.mailbox_sender().try_send_message(converted) {
            Ok(()) => Ok(()),
            Err(TrySendError::Full(_)) => Err(TellError::MailboxFull(msg)),
            Err(TrySendError::Closed(_)) => Err(TellError::ActorNotAlive(msg)),
        }
    }

    fn id(&self) -> ActorId {
        Self::id(self)
    }

    fn is_alive(&self) -> bool {
        Self::is_alive(self)
    }

    fn downgrade(&self) -> WeakRecipient<M> {
        WeakRecipient {
            inner: Arc::new(Self::downgrade(self)),
        }
    }
}

/// The erased operations a [`WeakRecipient<M>`] needs.
///
/// Non-pinning: upgrading yields a [`Recipient<M>`] only while a strong sender
/// still exists.
trait ErasedWeakRecipient<M>: Send + Sync {
    fn upgrade(&self) -> Option<Recipient<M>>;
    fn id(&self) -> ActorId;
}

impl<A, M> ErasedWeakRecipient<M> for WeakActorRef<A>
where
    A: Actor,
    A::Msg: From<M>,
    M: Clone + Send + 'static,
{
    fn upgrade(&self) -> Option<Recipient<M>> {
        Self::upgrade(self).map(Recipient::from)
    }

    fn id(&self) -> ActorId {
        Self::id(self)
    }
}

/// A cloneable, type-erased handle that delivers `M` to some actor whose menu
/// satisfies `A::Msg: From<M>`.
///
/// Exposes only the messaging surface — **not** `stop`/`kill` (a recipient is a
/// messaging handle, not a lifecycle handle).
pub struct Recipient<M> {
    inner: Arc<dyn ErasedRecipient<M>>,
}

impl<M> Clone for Recipient<M> {
    fn clone(&self) -> Self {
        Self {
            inner: Arc::clone(&self.inner),
        }
    }
}

impl<M> fmt::Debug for Recipient<M> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Recipient")
            .field("id", &self.inner.id())
            .field("msg", &type_name::<M>())
            .finish_non_exhaustive()
    }
}

impl<M> Recipient<M> {
    /// Awaiting send: delivers `M` converted to the target's menu, waiting for
    /// capacity. Parity with [`ActorRef::tell`].
    ///
    /// # Errors
    ///
    /// [`TellError::ActorNotAlive`] (terminal) carrying `M` back if the actor has
    /// stopped. The awaiting path never returns `MailboxFull`.
    pub async fn tell(&self, msg: M) -> Result<(), TellError<M>> {
        self.inner.tell(msg).await
    }

    /// Non-blocking send: delivers `M` converted to the target's menu, or hands
    /// `M` back.
    ///
    /// # Errors
    ///
    /// [`TellError::MailboxFull`] (retryable backpressure) or
    /// [`TellError::ActorNotAlive`] (terminal) — both carry `M` back.
    pub fn try_tell(&self, msg: M) -> Result<(), TellError<M>> {
        self.inner.try_tell(msg)
    }

    /// The target actor's identity, preserved through erasure.
    #[must_use]
    pub fn id(&self) -> ActorId {
        self.inner.id()
    }

    /// Whether the target's mailbox is still open (send-and-observe, not a
    /// pre-send gate — mirrors [`ActorRef::is_alive`]).
    #[must_use]
    pub fn is_alive(&self) -> bool {
        self.inner.is_alive()
    }

    /// Downgrades to a non-pinning [`WeakRecipient`].
    #[must_use]
    pub fn downgrade(&self) -> WeakRecipient<M> {
        self.inner.downgrade()
    }
}

/// A non-pinning, type-erased handle.
///
/// [`upgrade`](WeakRecipient::upgrade) yields a strong [`Recipient`] only while
/// the target's mailbox is still open.
pub struct WeakRecipient<M> {
    inner: Arc<dyn ErasedWeakRecipient<M>>,
}

impl<M> Clone for WeakRecipient<M> {
    fn clone(&self) -> Self {
        Self {
            inner: Arc::clone(&self.inner),
        }
    }
}

impl<M> fmt::Debug for WeakRecipient<M> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("WeakRecipient")
            .field("id", &self.inner.id())
            .field("msg", &type_name::<M>())
            .finish_non_exhaustive()
    }
}

impl<M> WeakRecipient<M> {
    /// Upgrades to a strong [`Recipient`], or `None` if every strong sender has
    /// dropped (the actor is gone).
    #[must_use]
    pub fn upgrade(&self) -> Option<Recipient<M>> {
        self.inner.upgrade()
    }

    /// The target actor's identity, preserved through erasure.
    #[must_use]
    pub fn id(&self) -> ActorId {
        self.inner.id()
    }
}

impl<A, M> From<ActorRef<A>> for Recipient<M>
where
    A: Actor,
    A::Msg: From<M>,
    M: Clone + Send + 'static,
{
    fn from(actor_ref: ActorRef<A>) -> Self {
        Self {
            inner: Arc::new(actor_ref),
        }
    }
}

impl<A: Actor> ActorRef<A> {
    /// Builds a type-erased [`Recipient<M>`] for this actor: any `M` its closed
    /// menu can be built from (`A::Msg: From<M>`). Enables `Vec<Recipient<M>>`
    /// fan-in across heterogeneous actors.
    #[must_use]
    pub fn recipient<M>(&self) -> Recipient<M>
    where
        A::Msg: From<M>,
        M: Clone + Send + 'static,
    {
        Recipient::from(self.clone())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use core::time::Duration;

    use futures::stream::AbortHandle;
    use tokio::time::timeout;
    use tokio_util::sync::CancellationToken;

    use crate::{
        actor::{Actor, ActorRef},
        error::TellError,
        mailbox::{ActorId, Capacity, Mailbox, MailboxReceiver, Mailboxed, Signal},
        message::Msg,
        test_support::terminate_bound,
    };

    /// Upper bound on how long a delivered message may take to surface on the
    /// receiver. A live round-trip is instant; this only fires if a send/tell
    /// silently fails to enqueue — turning what would be an unbounded hang into a
    /// fast, legible assertion failure (so a stubbed-out `tell`/`try_tell` is
    /// *caught*, not merely a mutation-run timeout). Scaled under MIRI — see
    /// `terminate_bound`.
    const DELIVERY: Duration = terminate_bound();

    /// The shared broadcast signal. `Clone` because `Recipient<M>` requires it
    /// (the typed-handback consequence, ADR-0004).
    #[derive(Clone, PartialEq, Eq, Debug)]
    struct Tick;
    impl Msg for Tick {}

    /// Actor 1 — its own closed menu; builds `Post` from a `Tick`.
    #[derive(PartialEq, Eq, Debug)]
    enum LedgerCmd {
        #[expect(
            dead_code,
            reason = "a second variant so From<Tick> = Post is a real choice, not the sole variant"
        )]
        Credit(u64),
        Post,
    }
    impl Msg for LedgerCmd {}
    impl From<Tick> for LedgerCmd {
        fn from(_: Tick) -> Self {
            Self::Post
        }
    }
    struct Ledger;
    impl Mailboxed for Ledger {
        type Msg = LedgerCmd;
    }
    impl Actor for Ledger {
        type Args = ();
        type Error = core::convert::Infallible;
        async fn on_start(_: (), _: ActorRef<Self>) -> Result<Self, Self::Error> {
            Ok(Self)
        }
        async fn handle(
            &mut self,
            _: LedgerCmd,
            _: ActorRef<Self>,
            _: &mut bool,
        ) -> Result<(), Self::Error> {
            Ok(())
        }
    }

    /// Actor 2 — a DIFFERENT menu; builds `Record` from a `Tick`.
    #[derive(PartialEq, Eq, Debug)]
    enum AuditCmd {
        Record,
        #[expect(
            dead_code,
            reason = "a second variant so From<Tick> = Record is a real choice, not the sole variant"
        )]
        Flush,
    }
    impl Msg for AuditCmd {}
    impl From<Tick> for AuditCmd {
        fn from(_: Tick) -> Self {
            Self::Record
        }
    }
    struct Audit;
    impl Mailboxed for Audit {
        type Msg = AuditCmd;
    }
    impl Actor for Audit {
        type Args = ();
        type Error = core::convert::Infallible;
        async fn on_start(_: (), _: ActorRef<Self>) -> Result<Self, Self::Error> {
            Ok(Self)
        }
        async fn handle(
            &mut self,
            _: AuditCmd,
            _: ActorRef<Self>,
            _: &mut bool,
        ) -> Result<(), Self::Error> {
            Ok(())
        }
    }

    /// Builds an `ActorRef<A>` plus the receiver we retain to inspect what the
    /// erased send delivered — no run-loop needed (the `actor_ref.rs` idiom).
    fn build<A: Actor>(id: u64, capacity: usize) -> (ActorRef<A>, MailboxReceiver<A>) {
        let cap = Capacity::try_from(capacity).expect("valid capacity");
        let (tx, rx) = Mailbox::<A>::bounded(cap);
        let (abort, _reg) = AbortHandle::new_pair();
        (
            ActorRef::new(ActorId::new(id), tx, CancellationToken::new(), abort),
            rx,
        )
    }

    /// The erased `try_tell` converts `M -> A::Msg` by value and delivers the
    /// correct variant — the single-actor proof that erasure routes to the real
    /// `From` impl (never a default).
    #[tokio::test]
    async fn try_tell_delivers_the_converted_variant() {
        let (ledger, mut rx) = build::<Ledger>(1, 4);
        let recipient: Recipient<Tick> = ledger.recipient();

        recipient
            .try_tell(Tick)
            .expect("delivered into an open mailbox");

        let delivered = timeout(DELIVERY, rx.recv())
            .await
            .expect("the converted variant must arrive, not hang");
        assert!(matches!(
            delivered,
            Some(Signal::Message {
                msg: LedgerCmd::Post,
                ..
            })
        ));
    }

    /// THE headline: one `Vec<Recipient<Tick>>` addresses two actors with
    /// DIFFERENT menus; broadcasting one `Tick` reaches each as its OWN
    /// converted variant. Proves type erasure + heterogeneous dispatch.
    #[tokio::test]
    async fn broadcast_reaches_heterogeneous_actors_as_their_own_variant() {
        let (ledger, mut ledger_rx) = build::<Ledger>(1, 4);
        let (audit, mut audit_rx) = build::<Audit>(2, 4);

        let group: Vec<Recipient<Tick>> = vec![ledger.recipient(), audit.recipient()];
        for recipient in &group {
            recipient.try_tell(Tick).expect("delivered");
        }

        let to_ledger = timeout(DELIVERY, ledger_rx.recv())
            .await
            .expect("the ledger's own variant must arrive, not hang");
        assert!(matches!(
            to_ledger,
            Some(Signal::Message {
                msg: LedgerCmd::Post,
                ..
            })
        ));
        let to_audit = timeout(DELIVERY, audit_rx.recv())
            .await
            .expect("the audit's own variant must arrive, not hang");
        assert!(matches!(
            to_audit,
            Some(Signal::Message {
                msg: AuditCmd::Record,
                ..
            })
        ));
    }

    /// Handback: a full mailbox bounces `try_tell` as retryable backpressure,
    /// carrying the EXACT original `M` back (not the converted `A::Msg`).
    #[tokio::test]
    async fn try_tell_to_full_mailbox_hands_the_message_back() {
        let (ledger, _rx) = build::<Ledger>(1, 1);
        let recipient: Recipient<Tick> = ledger.recipient();

        recipient.try_tell(Tick).expect("first fits");
        // Capacity 1, now full: backpressure with the original Tick returned.
        assert!(matches!(
            recipient.try_tell(Tick),
            Err(TellError::MailboxFull(Tick))
        ));
    }

    /// Handback: a stopped actor (receiver dropped) is the terminal
    /// `ActorNotAlive`, again carrying the original `M`.
    #[tokio::test]
    async fn try_tell_to_stopped_actor_reports_not_alive() {
        let (ledger, rx) = build::<Ledger>(1, 4);
        let recipient: Recipient<Tick> = ledger.recipient();
        drop(rx); // receiver gone -> mailbox closed

        assert!(matches!(
            recipient.try_tell(Tick),
            Err(TellError::ActorNotAlive(Tick))
        ));
    }

    /// The async `tell` awaits capacity and delivers the converted variant.
    #[tokio::test]
    async fn tell_awaits_then_delivers_the_converted_variant() {
        let (ledger, mut rx) = build::<Ledger>(1, 4);
        let recipient: Recipient<Tick> = ledger.recipient();

        timeout(DELIVERY, recipient.tell(Tick))
            .await
            .expect("the awaited tell must send within the bound, not hang")
            .expect("delivered");

        let delivered = timeout(DELIVERY, rx.recv())
            .await
            .expect("the awaited tell must deliver, not hang");
        assert!(matches!(
            delivered,
            Some(Signal::Message {
                msg: LedgerCmd::Post,
                ..
            })
        ));
    }

    /// The async `tell` to a stopped actor is terminal `ActorNotAlive` with the
    /// original `M` back (the awaiting path has no `Full`).
    #[tokio::test]
    async fn tell_to_stopped_actor_reports_not_alive() {
        let (ledger, rx) = build::<Ledger>(1, 4);
        let recipient: Recipient<Tick> = ledger.recipient();
        drop(rx);

        assert!(matches!(
            recipient.tell(Tick).await,
            Err(TellError::ActorNotAlive(Tick))
        ));
    }

    /// A weak recipient upgrades while a strong sender lives and returns `None`
    /// once every strong sender drops — and preserves `id` throughout.
    #[tokio::test]
    async fn weak_recipient_upgrades_while_alive_then_none_after_drop() {
        let (ledger, _rx) = build::<Ledger>(7, 4);
        let recipient: Recipient<Tick> = ledger.recipient();
        let weak = recipient.downgrade();

        assert_eq!(
            weak.id(),
            ActorId::new(7),
            "id survives erasure + downgrade"
        );
        assert!(weak.upgrade().is_some(), "alive -> upgradable");

        drop(recipient);
        drop(ledger); // every strong sender now gone (receiver `_rx` still held)
        assert!(
            weak.upgrade().is_none(),
            "all strong senders dropped -> not upgradable"
        );
    }

    /// `id` is preserved through the strong erasure and the downgrade.
    #[test]
    fn recipient_preserves_actor_id_through_erasure() {
        let (ledger, _rx) = build::<Ledger>(42, 4);
        let recipient: Recipient<Tick> = ledger.recipient();
        assert_eq!(recipient.id(), ActorId::new(42));
        assert_eq!(recipient.downgrade().id(), ActorId::new(42));
    }

    /// The `Recipient`/`WeakRecipient` debug views name the struct and surface
    /// the id — guards the hand-written impls against an empty formatter.
    #[test]
    fn recipient_debug_names_struct_and_id() {
        let (ledger, _rx) = build::<Ledger>(7, 4);
        let recipient: Recipient<Tick> = ledger.recipient();
        let shown = format!("{recipient:?}");
        assert!(shown.contains("Recipient"), "names the struct: {shown}");
        assert!(shown.contains('7'), "surfaces the id: {shown}");

        let weak = recipient.downgrade();
        let weak_shown = format!("{weak:?}");
        assert!(
            weak_shown.contains("WeakRecipient"),
            "names the weak struct: {weak_shown}"
        );
        assert!(weak_shown.contains('7'), "surfaces the id: {weak_shown}");
    }

    /// `is_alive` tracks the mailbox: true while open, false once the receiver
    /// (the run-loop) is dropped.
    #[tokio::test]
    async fn recipient_is_alive_tracks_the_mailbox() {
        let (ledger, rx) = build::<Ledger>(1, 4);
        let recipient: Recipient<Tick> = ledger.recipient();
        assert!(recipient.is_alive(), "open mailbox -> alive");
        drop(rx);
        assert!(!recipient.is_alive(), "receiver dropped -> not alive");
    }
}
