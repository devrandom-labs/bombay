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
        self.mailbox.upgrade().map(|mailbox| ActorRef {
            id: self.id,
            mailbox,
            cancel: self.cancel.clone(),
            abort: self.abort.clone(),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use futures::stream::AbortHandle;
    use tokio_util::sync::CancellationToken;

    use crate::{
        mailbox::{ActorId, Capacity, Mailbox, Mailboxed},
        message::Msg,
    };

    // A minimal Actor purely to key the mailbox/ref. `on_start`/`handle` are
    // never called in this task's tests (no loop yet) — they exist so the type
    // satisfies `Actor`.
    struct Probe;
    struct ProbeMsg;
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

    fn build_ref() -> (ActorRef<Probe>, WeakActorRef<Probe>) {
        let cap = Capacity::try_from(4usize).expect("valid capacity");
        let (tx, _rx) = Mailbox::<Probe>::bounded(cap);
        let (abort, _reg) = AbortHandle::new_pair();
        let actor_ref = ActorRef::new(ActorId::new(7), tx, CancellationToken::new(), abort);
        let weak = actor_ref.downgrade();
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
}
