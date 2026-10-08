use crate::observe::Publisher;
use behavior::{Behavior, BehaviorAddr, BehaviorMessage, User, UserEvent};
use communication::{Consumer, Drained, MailboxOwner, MailboxRef, Received};
use core::marker::PhantomData;
use std::sync::{Mutex, PoisonError, Weak};

impl<A, M> Clone for EndpointMailbox<A, M> {
    fn clone(&self) -> Self {
        match self {
            Self::Standard { mailbox, admission } => Self::Standard {
                mailbox: mailbox.clone(),
                admission: admission.clone(),
            },
            Self::Entity { mailbox, admission } => Self::Entity {
                mailbox: mailbox.clone(),
                admission: admission.clone(),
            },
        }
    }
}

impl<A, M> EndpointMailbox<A, M> {
    pub(in crate::local) fn close_admission(&self) -> AdmissionClosure {
        match self {
            Self::Standard { admission, .. } => Admission::close_from_endpoint(admission),
            Self::Entity { admission, .. } => Admission::close_from_endpoint(admission),
        }
    }
}

pub(crate) trait IngressMode<B: Behavior> {
    type Item: Send;
    type Retired;

    fn endpoint(
        mailbox: MailboxRef<Self::Item>,
        admission: Weak<Admission<Self::Item>>,
    ) -> EndpointMailbox<BehaviorAddr<B>, BehaviorMessage<B>>;

    fn into_event(item: Self::Item) -> Option<B::Event>;

    fn retain_at_retirement(item: Self::Item) -> Option<Self::Retired>;
}

impl<B> IngressMode<B> for StandardIngress
where
    B: Behavior,
    B::Event: UserEvent<Addr = BehaviorAddr<B>, Message = BehaviorMessage<B>>,
    User<BehaviorAddr<B>, BehaviorMessage<B>>: Send,
{
    type Item = User<BehaviorAddr<B>, BehaviorMessage<B>>;
    type Retired = Self::Item;

    fn endpoint(
        mailbox: MailboxRef<Self::Item>,
        admission: Weak<Admission<Self::Item>>,
    ) -> EndpointMailbox<BehaviorAddr<B>, BehaviorMessage<B>> {
        EndpointMailbox::Standard { mailbox, admission }
    }

    fn into_event(user: Self::Item) -> Option<B::Event> {
        Some(B::Event::user(user.from, user.message))
    }

    fn retain_at_retirement(item: Self::Item) -> Option<Self::Retired> {
        Some(item)
    }
}

impl<B> IngressMode<B> for EntityIngress
where
    B: Behavior,
    B::Event: UserEvent<Addr = BehaviorAddr<B>, Message = BehaviorMessage<B>>,
    LocalIngress<BehaviorAddr<B>, BehaviorMessage<B>>: Send,
{
    type Item = LocalIngress<BehaviorAddr<B>, BehaviorMessage<B>>;
    type Retired = User<BehaviorAddr<B>, BehaviorMessage<B>>;

    fn endpoint(
        mailbox: MailboxRef<Self::Item>,
        admission: Weak<Admission<Self::Item>>,
    ) -> EndpointMailbox<BehaviorAddr<B>, BehaviorMessage<B>> {
        EndpointMailbox::Entity { mailbox, admission }
    }

    fn into_event(item: Self::Item) -> Option<B::Event> {
        match item {
            LocalIngress::Message(user) => Some(B::Event::user(user.from, user.message)),
            LocalIngress::Fence(publisher) => {
                publisher.complete(Ok(()));
                None
            }
        }
    }

    fn retain_at_retirement(item: Self::Item) -> Option<Self::Retired> {
        match item {
            LocalIngress::Message(user) => Some(user),
            LocalIngress::Fence(publisher) => {
                publisher.complete(Err(crate::entity::FenceFailure::Acknowledgement));
                None
            }
        }
    }
}

impl<B, M> LocalInbox<B, M>
where
    B: Behavior,
    M: IngressMode<B>,
{
    pub(in crate::local) fn new(consumer: Consumer<B::Event, M::Item>) -> Self {
        Self {
            consumer: Some(consumer),
            mode: PhantomData,
        }
    }

    pub(in crate::local) async fn recv(&mut self) -> Option<Received<B::Event, M::Item>> {
        self.consumer
            .as_mut()
            .expect("active local inbox retains its consumer")
            .recv()
            .await
    }

    pub(in crate::local) async fn recv_source(&mut self) -> Option<B::Event> {
        self.consumer
            .as_mut()
            .expect("active local inbox retains its consumer")
            .recv_control()
            .await
    }

    pub(in crate::local) fn drain(&mut self) -> Option<Drained<B::Event, M::Retired>> {
        self.consumer.take().map(collect_retired_ingress::<B, M>)
    }
}

impl<B, M> Drop for LocalInbox<B, M>
where
    B: Behavior,
    M: IngressMode<B>,
{
    fn drop(&mut self) {
        drop(self.drain());
    }
}

impl<U> Admission<U> {
    pub(crate) fn new(owner: MailboxOwner<U>) -> Self {
        Self {
            owner: Mutex::new(Some(owner)),
        }
    }

    pub(in crate::local) fn close_from_endpoint(admission: &Weak<Self>) -> AdmissionClosure {
        match admission.upgrade() {
            Some(admission) => admission.close(),
            None => AdmissionClosure::AlreadyClosed,
        }
    }

    pub(crate) fn close(&self) -> AdmissionClosure {
        self.owner
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .take()
            .map_or(AdmissionClosure::AlreadyClosed, |owner| {
                owner.close_admission();
                AdmissionClosure::Closed
            })
    }
}

pub(crate) const DEFAULT_USER_CAPACITY: usize = 1_024;

/// Shared access to one affine Communication admission owner.
///
/// The active environment holds the only strong reference. Actor references
/// hold a weak reference so they cannot keep admission open. Closing takes the
/// owner exactly once; dropping the final strong reference closes it through
/// Communication's `MailboxOwner` drop law.
pub(crate) struct Admission<U> {
    owner: Mutex<Option<MailboxOwner<U>>>,
}

#[must_use = "admission closure reports whether this call won the transition"]
pub(crate) enum AdmissionClosure {
    Closed,
    AlreadyClosed,
}

pub(crate) enum LocalIngress<A, M> {
    Message(User<A, M>),
    Fence(Publisher<Result<(), crate::entity::FenceFailure>>),
}

pub(crate) struct StandardIngress;

pub(crate) struct EntityIngress;

pub(crate) enum EndpointMailbox<A, M> {
    Standard {
        mailbox: MailboxRef<User<A, M>>,
        admission: Weak<Admission<User<A, M>>>,
    },
    Entity {
        mailbox: MailboxRef<LocalIngress<A, M>>,
        admission: Weak<Admission<LocalIngress<A, M>>>,
    },
}

pub(in crate::local) fn collect_retired_ingress<B, M>(
    consumer: Consumer<B::Event, M::Item>,
) -> Drained<B::Event, M::Retired>
where
    B: Behavior,
    M: IngressMode<B>,
{
    let Drained { control, user } = consumer.drain();
    let user = user
        .into_iter()
        .filter_map(M::retain_at_retirement)
        .collect();
    Drained { control, user }
}

pub(in crate::local) struct LocalInbox<B, M>
where
    B: Behavior,
    M: IngressMode<B>,
{
    consumer: Option<Consumer<B::Event, M::Item>>,
    mode: PhantomData<fn() -> M>,
}
