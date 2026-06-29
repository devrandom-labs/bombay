use std::{
    any::Any,
    collections::HashMap,
    fmt, mem,
    ops::{self, ControlFlow},
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::{Duration, Instant},
};

use futures::{
    FutureExt, StreamExt,
    future::{BoxFuture, join_all},
    stream::FuturesUnordered,
};
use tokio::sync::Mutex;

use crate::{
    actor::{Actor, ActorId},
    error::ActorStopReason,
    mailbox::{MailboxReceiver, SignalMailbox},
    supervision::RestartPolicy,
};

pub type BoxMailboxReceiver = Box<dyn Any + Send>;
pub type BoxActorRef = Box<dyn Any + Send>;
pub type SpawnFactory =
    Box<dyn Fn(BoxMailboxReceiver) -> BoxFuture<'static, BoxActorRef> + Send + Sync>;
pub type ShutdownFn = Box<dyn Fn() -> BoxFuture<'static, ()> + Send + Sync>;

/// A collection of links to other actors that are notified when the actor dies.
///
/// Links are used for parent-child or sibling relationships, allowing actors to observe each other's lifecycle.
#[derive(Clone, Default)]
#[allow(missing_debug_implementations)]
pub struct Links(Arc<Mutex<LinksInner>>);

impl Links {
    /// Sets the `parent_shutdown` flag on all supervised children.
    ///
    /// This must be called before the parent stops processing its mailbox so that any child
    /// exiting independently after this point will drop its `mailbox_rx` immediately in
    /// `notify_links`, preventing it from being queued in the parent's mailbox where it would
    /// deadlock the `shutdown_children` wait.
    pub async fn set_children_parent_shutdown(&self) {
        let inner = self.lock().await;
        for spec in inner.children.values() {
            spec.parent_shutdown.store(true, Ordering::Release);
        }
    }

    pub async fn send_children_shutdown(&self) {
        let shutdowns: Vec<_> = {
            let inner = self.lock().await;
            inner
                .children
                .values()
                .map(|c| c.shutdown.clone())
                .collect()
        };

        join_all(shutdowns.iter().map(|s| s())).await;
    }

    pub async fn wait_children_closed(&self) {
        let mailboxes: Vec<_> = {
            let inner = self.lock().await;
            inner
                .children
                .values()
                .map(|c| c.signal_mailbox.clone())
                .collect()
        };

        join_all(mailboxes.iter().map(|m| m.closed())).await;
    }
}

#[derive(Default)]
#[allow(missing_debug_implementations)]
pub struct LinksInner {
    /// Parent actor supervising us.
    pub parent: Option<(ActorId, Link)>,
    /// Sibbling linked actors.
    pub sibblings: HashMap<ActorId, Link>,
    /// Child actors we are supervising.
    pub children: HashMap<ActorId, ErasedChildSpec>,
    /// Set by the parent supervisor (via `ErasedChildSpec`) before sending `SupervisorRestart`
    /// during final shutdown. When `true`, `notify_links` drops `mailbox_rx` immediately
    /// instead of queuing it in the parent's mailbox, which would deadlock the parent's
    /// `shutdown_children` wait.
    pub parent_shutdown: Arc<AtomicBool>,
}

impl LinksInner {
    /// Notify parent or sibblings, depending on supervision status.
    pub fn notify_links<A: Actor>(
        &mut self,
        id: ActorId,
        reason: ActorStopReason,
        mailbox_rx: MailboxReceiver<A>,
    ) {
        match self.parent.clone() {
            Some((parent_id, parent_link)) => {
                let sibblings = mem::take(&mut self.sibblings);
                if self.parent_shutdown.load(Ordering::Acquire) {
                    // Parent is in shutdown_children — drop mailbox_rx immediately so the
                    // child's channel closes and the parent's mailbox.closed() wait resolves.
                    // Passing it to the parent's queue would deadlock since the parent is not
                    // processing its mailbox while blocked in shutdown_children.
                    tokio::spawn(parent_link.notify(parent_id, id, reason, None, None));
                } else {
                    // Supervised normal path — pass mailbox_rx so the parent can restart us.
                    tokio::spawn(parent_link.notify(
                        parent_id,
                        id,
                        reason,
                        Some(Box::new(mailbox_rx)),
                        Some(sibblings),
                    ));
                }
            }
            None => {
                // Unsupervised, notify sibblings directly
                self.notify_sibblings(id, &reason);
            }
        }
    }

    /// Notify sibbling links.
    pub fn notify_sibblings(&mut self, id: ActorId, reason: &ActorStopReason) {
        let mut notify_futs: FuturesUnordered<_> = self
            .sibblings
            .drain()
            .map(|(sibbling_actor_id, link)| {
                link.notify(sibbling_actor_id, id, reason.clone(), None, None)
                    .boxed()
            })
            .collect();
        tokio::spawn(async move { while let Some(()) = notify_futs.next().await {} });
    }
}

impl ops::Deref for Links {
    type Target = Mutex<LinksInner>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

#[derive(Clone)]
#[allow(missing_debug_implementations)]
pub enum Link {
    Local(Box<dyn SignalMailbox>),
    #[cfg(feature = "remote")]
    Remote(std::borrow::Cow<'static, str>),
}

impl Link {
    #[cfg_attr(not(feature = "remote"), allow(unused_variables))]
    pub async fn notify(
        self,
        link_actor_id: ActorId,
        dead_actor_id: ActorId,
        reason: ActorStopReason,
        mailbox_rx: Option<BoxMailboxReceiver>,
        dead_actor_sibblings: Option<HashMap<ActorId, Link>>,
    ) {
        match self {
            Link::Local(mailbox) => {
                #[cfg_attr(not(feature = "tracing"), allow(unused_variables))]
                if let Err(err) = mailbox
                    .signal_link_died(dead_actor_id, reason, mailbox_rx, dead_actor_sibblings)
                    .await
                {
                    #[cfg(feature = "tracing")]
                    match err {
                        crate::error::SendError::ActorNotRunning(_) => {}
                        _ => {
                            tracing::error!("failed to notify actor a link died: {err}");
                        }
                    }
                }
            }
            #[cfg(feature = "remote")]
            Link::Remote(notified_actor_remote_id) => {
                if let Some(swarm) = crate::remote::ActorSwarm::get() {
                    let res = swarm
                        .signal_link_died(
                            dead_actor_id,
                            link_actor_id,
                            notified_actor_remote_id,
                            reason,
                        )
                        .await;
                    #[cfg_attr(not(feature = "tracing"), allow(unused_variables))]
                    if let Err(err) = res {
                        #[cfg(feature = "tracing")]
                        tracing::error!("failed to notify actor a link died: {err}");
                    }
                }
            }
        }
    }
}

#[derive(Clone)]
#[allow(missing_debug_implementations)]
pub struct ErasedChildSpec {
    pub factory: Arc<SpawnFactory>,
    pub shutdown: Arc<ShutdownFn>,
    pub signal_mailbox: Box<dyn SignalMailbox>,
    /// Shared with the child's `LinksInner::parent_shutdown`. Set to `true` by
    /// `shutdown_children` before sending `SupervisorRestart`, so the child knows to drop
    /// its `mailbox_rx` immediately rather than forwarding it to the parent's queue.
    pub parent_shutdown: Arc<AtomicBool>,
    pub restart_policy: RestartPolicy,
    pub restart_count: u32,
    pub last_restart: Instant,
    pub max_restarts: u32,
    pub restart_window: Duration,
}

impl ErasedChildSpec {
    pub fn should_restart(&mut self, reason: &ActorStopReason) -> ControlFlow<NoRestartReason> {
        // Never policy takes precedence over everything, including coordinator-initiated restarts
        if matches!(self.restart_policy, RestartPolicy::Never) {
            return ControlFlow::Break(NoRestartReason::NeverPolicy);
        }

        // Always restart if supervisor initiated the shutdown for coordination
        if matches!(reason, ActorStopReason::SupervisorRestart) {
            return ControlFlow::Continue(());
        }

        // Policy check
        match self.restart_policy {
            RestartPolicy::Permanent => {}
            RestartPolicy::Transient if reason.is_normal() => {
                return ControlFlow::Break(NoRestartReason::NormalExitUnderTransientPolicy);
            }
            RestartPolicy::Transient => {}
            RestartPolicy::Never => unreachable!(),
        }

        // Reset count if outside time window
        let now = Instant::now();
        if now.duration_since(self.last_restart) > self.restart_window {
            self.restart_count = 0;
        }

        // Intensity check
        if self.restart_count >= self.max_restarts {
            return ControlFlow::Break(NoRestartReason::MaxRestartsExceeded {
                restart_count: self.restart_count,
                max_restarts: self.max_restarts,
            });
        }

        self.restart_count += 1;
        self.last_restart = now;

        ControlFlow::Continue(())
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum NoRestartReason {
    /// Transient policy but stop was normal
    NormalExitUnderTransientPolicy,
    /// Restart intensity threshold exceeded
    MaxRestartsExceeded {
        restart_count: u32,
        max_restarts: u32,
    },
    /// Never policy — this child is configured to never restart
    NeverPolicy,
}

impl fmt::Display for NoRestartReason {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            NoRestartReason::NormalExitUnderTransientPolicy => {
                write!(f, "normal exit under transient policy")
            }
            NoRestartReason::MaxRestartsExceeded {
                restart_count,
                max_restarts,
            } => {
                write!(
                    f,
                    "max restarts exceeded ({restart_count} >= {max_restarts})"
                )
            }
            NoRestartReason::NeverPolicy => write!(f, "never restart policy"),
        }
    }
}

/// Test-only surface for the `links` cucumber scenarios (card #77).
///
/// Gated behind the `testing` feature. The scenarios pin the *link/notification
/// machinery directly* — which sibling/parent is notified, with or without the
/// dying actor's `mailbox_rx`, and the `parent_shutdown` Release/Acquire
/// ordering. Driving that requires constructing a raw [`Links`], wiring its
/// `parent`/`sibblings`/`children` maps, invoking `notify_links` /
/// `notify_sibblings`, and observing the resulting [`Signal::LinkDied`] a
/// notified actor's mailbox receives. In production a [`Links`] is built only by
/// the spawn machinery and a notified [`Link::Local`] holds an internal
/// `MailboxSender` whose receiver is owned by the actor's run-loop, so none of
/// this is observable out-of-crate. Every helper here builds those structures
/// over a caller-provided `MailboxSender`/`MailboxReceiver` pair so the test
/// keeps the receiver and reads back exactly what was delivered. None of this is
/// part of the production API.
#[cfg(any(test, feature = "testing"))]
pub mod testing {
    use std::{
        sync::{
            Arc,
            atomic::{AtomicBool, Ordering},
        },
        time::{Duration, Instant},
    };

    use super::{ErasedChildSpec, Link, Links, ShutdownFn, SpawnFactory};
    use crate::{
        actor::{Actor, ActorId},
        error::ActorStopReason,
        mailbox::{MailboxReceiver, MailboxSender, Signal},
        supervision::RestartPolicy,
    };

    /// Builds a fresh, empty [`Links`] (no parent, no siblings, no children).
    pub fn empty_links() -> Links {
        Links::default()
    }

    /// Wires `links`'s `parent` to a [`Link::Local`] over `sender`, so a
    /// subsequent `notify_links` delivers the death notification to `sender`'s
    /// channel (the supervised path). `parent_id` is the id stored in the link.
    pub async fn set_parent<A: Actor>(
        links: &Links,
        parent_id: ActorId,
        sender: &MailboxSender<A>,
    ) {
        let mut inner = links.lock().await;
        inner.parent = Some((parent_id, Link::Local(Box::new(sender.clone()))));
    }

    /// Inserts a sibling [`Link::Local`] over `sender` keyed by `sibling_id`, so
    /// `notify_sibblings` / the unsupervised `notify_links` path delivers to it.
    pub async fn add_sibbling<A: Actor>(
        links: &Links,
        sibling_id: ActorId,
        sender: &MailboxSender<A>,
    ) {
        let mut inner = links.lock().await;
        inner.sibblings.insert(sibling_id, Link::Local(Box::new(sender.clone())));
    }

    /// Returns the shared `parent_shutdown` flag of `links` — the exact
    /// `AtomicBool` `notify_links` loads (Acquire) and `set_children_parent_shutdown`
    /// stores (Release) on.
    pub async fn parent_shutdown_flag(links: &Links) -> Arc<AtomicBool> {
        links.lock().await.parent_shutdown.clone()
    }

    /// A child `ErasedChildSpec` built over `sender`, plus an observable
    /// `shutdown`-invocation counter. The spec shares `parent_flag` as its
    /// `parent_shutdown` (so a test can assert `set_children_parent_shutdown`
    /// stored `true` on it). The returned counter is incremented once each time
    /// the spec's `shutdown` closure is invoked (by `send_children_shutdown`).
    pub fn child_spec<A: Actor>(
        sender: &MailboxSender<A>,
        parent_flag: Arc<AtomicBool>,
    ) -> (ErasedChildSpec, Arc<std::sync::atomic::AtomicU32>) {
        let shutdown_calls = Arc::new(std::sync::atomic::AtomicU32::new(0));
        let factory: SpawnFactory = Box::new(|rx| Box::pin(async move { rx }));
        let shutdown: ShutdownFn = Box::new({
            let calls = shutdown_calls.clone();
            move || {
                let calls = calls.clone();
                Box::pin(async move {
                    calls.fetch_add(1, Ordering::SeqCst);
                })
            }
        });
        let spec = ErasedChildSpec {
            factory: Arc::new(factory),
            shutdown: Arc::new(shutdown),
            signal_mailbox: Box::new(sender.clone()),
            parent_shutdown: parent_flag,
            restart_policy: RestartPolicy::Permanent,
            restart_count: 0,
            last_restart: Instant::now(),
            max_restarts: 3,
            restart_window: Duration::from_secs(3),
        };
        (spec, shutdown_calls)
    }

    /// Registers `spec` as a supervised child of `links` keyed by `child_id`.
    pub async fn add_child(links: &Links, child_id: ActorId, spec: ErasedChildSpec) {
        let mut inner = links.lock().await;
        inner.children.insert(child_id, spec);
    }

    /// Drives `LinksInner::notify_links::<A>` on `links` for a dying actor `id`,
    /// passing `mailbox_rx` (the supervised-restart hand-off the parent receives
    /// when the `parent_shutdown` flag is false).
    pub async fn notify_links<A: Actor>(
        links: &Links,
        id: ActorId,
        reason: ActorStopReason,
        mailbox_rx: MailboxReceiver<A>,
    ) {
        let mut inner = links.lock().await;
        inner.notify_links::<A>(id, reason, mailbox_rx);
    }

    /// What a notified actor's mailbox received: the dying actor's id, and
    /// whether the notification carried `mailbox_rx` / `dead_actor_sibblings`.
    /// Reading these presence bits is how the scenarios distinguish the
    /// supervised-restart hand-off (both `Some`) from the sibling / shutdown
    /// path (both `None`).
    #[derive(Debug, Clone, PartialEq, Eq)]
    pub struct LinkDiedParts {
        /// The dying actor's id carried in the signal.
        pub id: ActorId,
        /// Whether the signal carried the dying actor's `mailbox_rx`.
        pub has_mailbox_rx: bool,
        /// Whether the signal carried the dying actor's sibling links.
        pub has_sibblings: bool,
    }

    /// Reads the next [`Signal::LinkDied`] from `rx`, bounded by `timeout`.
    /// Returns `None` if no signal arrives in time (so a test can assert a
    /// non-notification without blocking forever). Panics if a non-LinkDied
    /// signal is yielded.
    pub async fn recv_link_died<A: Actor>(
        rx: &mut MailboxReceiver<A>,
        timeout: Duration,
    ) -> Option<LinkDiedParts> {
        match tokio::time::timeout(timeout, rx.recv()).await {
            Ok(Some(Signal::LinkDied {
                id,
                mailbox_rx,
                dead_actor_sibblings,
                ..
            })) => Some(LinkDiedParts {
                id,
                has_mailbox_rx: mailbox_rx.is_some(),
                has_sibblings: dead_actor_sibblings.is_some(),
            }),
            Ok(Some(_)) => panic!("expected a Signal::LinkDied"),
            Ok(None) | Err(_) => None,
        }
    }
}
