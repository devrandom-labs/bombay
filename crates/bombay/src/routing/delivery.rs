//! Shared outbound delivery routing.

use core::future::Future;
use core::hash::Hash;

use behavior::{
    Address, Behavior, Crash, DeadlineSends, Delivery, Exit, ObserveChild, ObserveCreation,
    ObservePeer, ProxySends, ReceiveTimeoutSends, ReportWorkerCreationResolved,
    ReportWorkerStopped, SendProduct, ServiceSends, SupervisorSends, WatchSends,
};
pub use bombay_address::AddressInUse;
use bombay_address::{AddressSpace, Lease};
use observe::{Observation, ObservationSpace};

/// Normalized terminal outcome exposed to Bombay Behavior's peer protocol.
pub(crate) type PeerOutcome<A> = Result<Exit<A>, Crash>;

/// A registered delivery endpoint paired with its exact incarnation outcome.
#[doc(hidden)]
pub struct IncarnationEndpoint<A: Address, D> {
    delivery: D,
    completion: ObservationSpace<(), PeerOutcome<A>>,
}

impl<A: Address, D> IncarnationEndpoint<A, D> {
    pub(crate) const fn new(delivery: D, completion: ObservationSpace<(), PeerOutcome<A>>) -> Self {
        Self {
            delivery,
            completion,
        }
    }

    fn observation(&self) -> Observation<PeerOutcome<A>> {
        self.completion
            .observe(&())
            .expect("a registered incarnation must retain its completion subject")
    }
}

impl<A: Address, D: Clone> Clone for IncarnationEndpoint<A, D> {
    fn clone(&self) -> Self {
        Self::new(self.delivery.clone(), self.completion.clone())
    }
}

impl<A, M, D> DeliveryEndpoint<A, M> for IncarnationEndpoint<A, D>
where
    A: Address + Send + Sync,
    M: Send,
    D: DeliveryEndpoint<A, M> + Sync,
{
    type Error = D::Error;

    async fn deliver(&self, from: A, message: M) -> Result<(), RejectedDelivery<M, Self::Error>> {
        self.delivery.deliver(from, message).await
    }
}

/// Failure to capture one currently registered peer incarnation.
#[doc(hidden)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum PeerObservationError<A> {
    /// No live incarnation is registered at the observed address.
    #[error("no live incarnation is registered at address {0:?}")]
    Unknown(A),
}

/// Resolves and captures exact-generation peer completion without liveness.
#[doc(hidden)]
pub trait PeerObserver<A: Address> {
    fn observe_peer(&self, peer: A)
    -> Result<Observation<PeerOutcome<A>>, PeerObservationError<A>>;
}

/// A resolved destination for one statically typed message protocol.
///
/// This port performs no route interpretation or address lookup; those belong
/// to [`DeliveryRouter`].
#[doc(hidden)]
pub trait DeliveryEndpoint<A, M> {
    /// Delivery failure.
    type Error;

    /// Deliver `message` on behalf of `from`.
    fn deliver(
        &self,
        from: A,
        message: M,
    ) -> impl Future<Output = Result<(), RejectedDelivery<M, Self::Error>>> + Send;
}

/// One endpoint rejection with ownership of the unchanged message.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RejectedDelivery<M, E> {
    /// The message the endpoint did not accept.
    pub message: M,
    /// The endpoint-specific rejection reason.
    pub error: E,
}

impl<M, E> RejectedDelivery<M, E> {
    /// Preserve one rejected message with its typed reason.
    #[must_use]
    pub const fn new(message: M, error: E) -> Self {
        Self { message, error }
    }
}

/// Registers resolved typed endpoints for later address routing.
pub trait EndpointRegistry<B: Behavior, D> {
    /// Registration failure.
    type Error;
    /// Ownership token that keeps the exact registration generation live.
    type Registration;

    /// Register `endpoint` at `address`.
    ///
    /// # Errors
    ///
    /// Returns the registry's collision or storage failure.
    fn register(&self, address: B::Addr, endpoint: D) -> Result<Self::Registration, Self::Error>;
}

/// Failure while resolving or invoking a registered endpoint.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum RoutingError<A: Address, M, E> {
    /// No endpoint exists for this resolved address.
    #[error("no endpoint exists for the resolved address {address:?}")]
    UnknownAddress {
        /// The concrete address obtained from the typed route.
        address: A,
        /// The unchanged message that was not accepted.
        message: M,
    },
    /// The resolved endpoint rejected delivery.
    #[error("the endpoint at {address:?} rejected delivery: {rejected:?}")]
    Endpoint {
        /// The concrete endpoint address selected by the route.
        address: A,
        /// The unchanged rejected message and typed endpoint reason.
        rejected: RejectedDelivery<M, E>,
    },
}

/// A simple shared routing table for one typed message protocol.
pub struct AddressRouter<A, D> {
    entries: AddressSpace<A, D>,
}

impl<A, D> Clone for AddressRouter<A, D> {
    fn clone(&self) -> Self {
        Self {
            entries: self.entries.clone(),
        }
    }
}

impl<A, D> Default for AddressRouter<A, D> {
    fn default() -> Self {
        Self {
            entries: AddressSpace::new(),
        }
    }
}

impl<B, D> EndpointRegistry<B, D> for AddressRouter<B::Addr, D>
where
    B: Behavior,
    B::Addr: Hash,
{
    type Error = AddressInUse<B::Addr>;
    type Registration = Lease<B::Addr, D>;

    fn register(&self, address: B::Addr, endpoint: D) -> Result<Self::Registration, Self::Error> {
        self.entries.claim(address, endpoint)
    }
}

impl<B, D> DeliveryRouter<B> for AddressRouter<B::Addr, D>
where
    B: Behavior,
    B::Addr: Hash + Send + Sync,
    <B::Addr as Address>::Nonce: Send,
    B::Msg: Send,
    D: DeliveryEndpoint<B::Addr, B::Msg> + Clone + Send + Sync,
    D::Error: Send,
{
    type Error = RoutingError<B::Addr, B::Msg, D::Error>;

    async fn deliver(&self, from: B::Addr, delivery: Delivery<B>) -> Result<(), Self::Error> {
        let address = delivery.to.resolve(from);
        let Some(endpoint) = self.entries.resolve(&address) else {
            return Err(RoutingError::UnknownAddress {
                address,
                message: delivery.message,
            });
        };
        endpoint
            .deliver(from, delivery.message)
            .await
            .map_err(|rejected| RoutingError::Endpoint { address, rejected })
    }
}

impl<A, D> PeerObserver<A> for AddressRouter<A, IncarnationEndpoint<A, D>>
where
    A: Address + Hash,
    D: Clone,
{
    fn observe_peer(
        &self,
        peer: A,
    ) -> Result<Observation<PeerOutcome<A>>, PeerObservationError<A>> {
        self.entries
            .resolve(&peer)
            .map(|endpoint| endpoint.observation())
            .ok_or(PeerObservationError::Unknown(peer))
    }
}

/// Shared capability that resolves and delivers one typed behavior send.
///
/// Resolution of global, child-relative, and service routes is distinct from
/// invoking the resulting [`DeliveryEndpoint`].
pub trait DeliveryRouter<B: Behavior> {
    /// Delivery failure.
    type Error;

    /// Deliver `delivery` on behalf of `from`.
    fn deliver(
        &self,
        from: B::Addr,
        delivery: Delivery<B>,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send;
}

/// Statically reveals whether a send algebra observes one creation nonce.
///
/// The interpreter consults this probe before routing a transition's sends:
/// a creation failure at an observed nonce becomes a committed
/// [`behavior::CreationResolved`] rejection, while an unobserved failure
/// retains its exact runtime error. Custom send algebras must implement this
/// alongside [`RouteSends`] so the two agree.
pub trait ObservesCreations<N> {
    /// Whether this algebra contains an `ObserveCreation` for `nonce`.
    fn observes_creation(&self, nonce: N) -> bool;
}

impl<B: Behavior, N> ObservesCreations<N> for Vec<Delivery<B>> {
    fn observes_creation(&self, _nonce: N) -> bool {
        false
    }
}

impl<N> ObservesCreations<N> for Vec<behavior::Never> {
    fn observes_creation(&self, _nonce: N) -> bool {
        false
    }
}

impl<N> ObservesCreations<N> for ServiceSends<ObserveCreation<N>>
where
    N: Copy + Eq,
{
    fn observes_creation(&self, nonce: N) -> bool {
        self.iter().any(|request| request.nonce == nonce)
    }
}

macro_rules! observes_no_creations {
    ($($lane:ty),+ $(,)?) => {
        $(
            impl<N> ObservesCreations<N> for $lane {
                fn observes_creation(&self, _nonce: N) -> bool {
                    false
                }
            }
        )+
    };
}

observes_no_creations!(
    ServiceSends<behavior::ScheduleAt>,
    ServiceSends<behavior::ScheduleAfter>,
);

impl<M, N> ObservesCreations<N> for ServiceSends<ObserveChild<M>> {
    fn observes_creation(&self, _nonce: N) -> bool {
        false
    }
}

impl<A: Address, N> ObservesCreations<N> for ServiceSends<ObservePeer<A>> {
    fn observes_creation(&self, _nonce: N) -> bool {
        false
    }
}

impl<A, N> ObservesCreations<N> for ServiceSends<behavior::UnwatchPeer<A>> {
    fn observes_creation(&self, _nonce: N) -> bool {
        false
    }
}

impl<A: Address, N> ObservesCreations<N> for ServiceSends<ReportWorkerStopped<A>> {
    fn observes_creation(&self, _nonce: N) -> bool {
        false
    }
}

impl<M, N> ObservesCreations<N> for ServiceSends<ReportWorkerCreationResolved<M>> {
    fn observes_creation(&self, _nonce: N) -> bool {
        false
    }
}

impl<L, R, N> ObservesCreations<N> for SendProduct<L, R>
where
    L: ObservesCreations<N>,
    R: ObservesCreations<N>,
    N: Copy,
{
    fn observes_creation(&self, nonce: N) -> bool {
        self.inner.observes_creation(nonce) || self.own.observes_creation(nonce)
    }
}

impl<A: Address, Sends> ObservesCreations<A::Nonce> for WatchSends<A, Sends>
where
    Sends: ObservesCreations<A::Nonce>,
    A::Nonce: Copy,
{
    fn observes_creation(&self, nonce: A::Nonce) -> bool {
        self.behavior.observes_creation(nonce)
    }
}

impl<Sends, N> ObservesCreations<N> for DeadlineSends<Sends>
where
    Sends: ObservesCreations<N>,
    N: Copy,
{
    fn observes_creation(&self, nonce: N) -> bool {
        self.behavior.observes_creation(nonce)
    }
}

impl<Sends, N> ObservesCreations<N> for ReceiveTimeoutSends<Sends>
where
    Sends: ObservesCreations<N>,
    N: Copy,
{
    fn observes_creation(&self, nonce: N) -> bool {
        self.behavior.observes_creation(nonce)
    }
}

impl<A: Address, Sends, C> ObservesCreations<A::Nonce> for SupervisorSends<A, Sends, C>
where
    Sends: ObservesCreations<A::Nonce>,
    C: Behavior<Addr = A, Ph = behavior::Never>,
    A::Nonce: Copy + From<u64>,
{
    fn observes_creation(&self, nonce: A::Nonce) -> bool {
        self.behavior.observes_creation(nonce)
    }
}

impl<C: Behavior> ObservesCreations<<C::Addr as Address>::Nonce> for ProxySends<C>
where
    <C::Addr as Address>::Nonce: Copy + Eq,
{
    fn observes_creation(&self, nonce: <C::Addr as Address>::Nonce) -> bool {
        self.creation_observations.observes_creation(nonce)
    }
}

/// Interprets a composed behavior send algebra through shared routing.
///
/// [`behavior::SendAlgebra`] only supplies pure `empty` and `append`
/// composition. This runtime port consumes that accumulated value in fold
/// order and reports the exact interpreter leg that failed.
pub trait RouteSends<A: Address, R>: Sized {
    /// Delivery failure.
    type Error;

    /// Route every send in fold order.
    fn route(self, from: A, router: &mut R)
    -> impl Future<Output = Result<(), Self::Error>> + Send;
}

impl<A, B, R> RouteSends<A, R> for Vec<Delivery<B>>
where
    A: Address + Send,
    B: Behavior<Addr = A>,
    B::Msg: Send,
    R: DeliveryRouter<B> + Send + Sync,
    A::Nonce: Send + From<u64>,
{
    type Error = R::Error;

    async fn route(self, from: A, router: &mut R) -> Result<(), Self::Error> {
        for delivery in self {
            <R as DeliveryRouter<B>>::deliver(router, from, delivery).await?;
        }
        Ok(())
    }
}

impl<A, R> RouteSends<A, R> for Vec<behavior::Never>
where
    A: Address + Send,
    R: Send,
{
    type Error = core::convert::Infallible;

    async fn route(self, _from: A, _router: &mut R) -> Result<(), Self::Error> {
        if let Some(never) = self.into_iter().next() {
            match never {}
        }
        Ok(())
    }
}

/// Failure from one statically selected leg of a composed send algebra.
#[doc(hidden)]
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum SendProductError<L, R> {
    /// The inner protocol leg failed.
    #[error("inner protocol leg failed: {0:?}")]
    Inner(L),
    /// The wrapper-owned protocol leg failed.
    #[error("wrapper-owned protocol leg failed: {0:?}")]
    Own(R),
}

impl<A, R, L, Own> RouteSends<A, R> for SendProduct<L, Own>
where
    A: Address + Send,
    L: RouteSends<A, R> + Send,
    Own: RouteSends<A, R> + Send,
    R: Send,
{
    type Error = SendProductError<L::Error, Own::Error>;

    async fn route(self, from: A, router: &mut R) -> Result<(), Self::Error> {
        self.inner
            .route(from, router)
            .await
            .map_err(SendProductError::Inner)?;
        self.own
            .route(from, router)
            .await
            .map_err(SendProductError::Own)
    }
}

/// Failure from one named lane of a [`WatchSends`] composition.
#[doc(hidden)]
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum WatchSendsError<B, O> {
    /// The wrapped behavior's own sends failed.
    #[error("watch behavior sends failed: {0:?}")]
    Behavior(B),
    /// The watch observation lane failed.
    #[error("watch observation lane failed: {0:?}")]
    Observations(O),
}

impl<A, R, Sends> RouteSends<A, R> for WatchSends<A, Sends>
where
    A: Address + Send,
    Sends: RouteSends<A, R> + Send,
    ServiceSends<ObservePeer<A>>: RouteSends<A, R> + Send,
    R: Send,
{
    type Error = WatchSendsError<
        <Sends as RouteSends<A, R>>::Error,
        <ServiceSends<ObservePeer<A>> as RouteSends<A, R>>::Error,
    >;

    async fn route(self, from: A, router: &mut R) -> Result<(), Self::Error> {
        self.behavior
            .route(from, router)
            .await
            .map_err(WatchSendsError::Behavior)?;
        self.observations
            .route(from, router)
            .await
            .map_err(WatchSendsError::Observations)
    }
}

/// Failure from one named lane of a [`DeadlineSends`] composition.
#[doc(hidden)]
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum DeadlineSendsError<B, S> {
    /// The wrapped behavior's own sends failed.
    #[error("deadline behavior sends failed: {0:?}")]
    Behavior(B),
    /// The absolute-schedule lane failed.
    #[error("deadline schedule lane failed: {0:?}")]
    Schedules(S),
}

impl<A, R, Sends> RouteSends<A, R> for DeadlineSends<Sends>
where
    A: Address + Send,
    Sends: RouteSends<A, R> + Send,
    ServiceSends<behavior::ScheduleAt>: RouteSends<A, R> + Send,
    R: Send,
{
    type Error = DeadlineSendsError<
        <Sends as RouteSends<A, R>>::Error,
        <ServiceSends<behavior::ScheduleAt> as RouteSends<A, R>>::Error,
    >;

    async fn route(self, from: A, router: &mut R) -> Result<(), Self::Error> {
        self.behavior
            .route(from, router)
            .await
            .map_err(DeadlineSendsError::Behavior)?;
        self.schedules
            .route(from, router)
            .await
            .map_err(DeadlineSendsError::Schedules)
    }
}

/// Failure from one named lane of a [`ReceiveTimeoutSends`] composition.
#[doc(hidden)]
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum ReceiveTimeoutSendsError<B, S> {
    /// The wrapped behavior's own sends failed.
    #[error("receive-timeout behavior sends failed: {0:?}")]
    Behavior(B),
    /// The relative-schedule lane failed.
    #[error("receive-timeout schedule lane failed: {0:?}")]
    Schedules(S),
}

impl<A, R, Sends> RouteSends<A, R> for ReceiveTimeoutSends<Sends>
where
    A: Address + Send,
    Sends: RouteSends<A, R> + Send,
    ServiceSends<behavior::ScheduleAfter>: RouteSends<A, R> + Send,
    R: Send,
{
    type Error = ReceiveTimeoutSendsError<
        <Sends as RouteSends<A, R>>::Error,
        <ServiceSends<behavior::ScheduleAfter> as RouteSends<A, R>>::Error,
    >;

    async fn route(self, from: A, router: &mut R) -> Result<(), Self::Error> {
        self.behavior
            .route(from, router)
            .await
            .map_err(ReceiveTimeoutSendsError::Behavior)?;
        self.schedules
            .route(from, router)
            .await
            .map_err(ReceiveTimeoutSendsError::Schedules)
    }
}

/// Failure from one named lane of a [`SupervisorSends`] composition.
#[doc(hidden)]
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum SupervisorSendsError<B, O, C> {
    /// The supervised behavior's own sends failed.
    #[error("supervised behavior sends failed: {0:?}")]
    Behavior(B),
    /// The child-observation lane failed.
    #[error("supervisor child-observation lane failed: {0:?}")]
    ChildObservations(O),
    /// The replacement-command lane failed.
    #[error("supervisor replacement-command lane failed: {0:?}")]
    ReplacementCommands(C),
}

impl<A, R, Sends, C> RouteSends<A, R> for SupervisorSends<A, Sends, C>
where
    A: Address + Send,
    A::Nonce: Send + From<u64>,
    Sends: RouteSends<A, R> + Send,
    ServiceSends<ObserveChild<A::Nonce>>: RouteSends<A, R> + Send,
    Vec<Delivery<behavior::Proxy<C>>>: RouteSends<A, R> + Send,
    C: Behavior<Addr = A, Ph = behavior::Never> + Send,
    R: Send,
{
    type Error = SupervisorSendsError<
        <Sends as RouteSends<A, R>>::Error,
        <ServiceSends<ObserveChild<A::Nonce>> as RouteSends<A, R>>::Error,
        <Vec<Delivery<behavior::Proxy<C>>> as RouteSends<A, R>>::Error,
    >;

    async fn route(self, from: A, router: &mut R) -> Result<(), Self::Error> {
        self.behavior
            .route(from, router)
            .await
            .map_err(SupervisorSendsError::Behavior)?;
        self.child_observations
            .route(from, router)
            .await
            .map_err(SupervisorSendsError::ChildObservations)?;
        self.replacement_commands
            .route(from, router)
            .await
            .map_err(SupervisorSendsError::ReplacementCommands)
    }
}

/// Failure from one named lane of a [`ProxySends`] composition.
#[doc(hidden)]
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum ProxySendsError<D, C, CR, S, RP> {
    /// The proxied user-delivery lane failed.
    #[error("proxy delivery lane failed: {0:?}")]
    Deliveries(D),
    /// The child-observation lane failed.
    #[error("proxy child-observation lane failed: {0:?}")]
    ChildObservations(C),
    /// The creation-observation lane failed.
    #[error("proxy creation-observation lane failed: {0:?}")]
    CreationObservations(CR),
    /// The worker-stopped report lane failed.
    #[error("proxy worker-stopped report lane failed: {0:?}")]
    StoppedReports(S),
    /// The worker-creation report lane failed.
    #[error("proxy worker-creation report lane failed: {0:?}")]
    CreationReports(RP),
}

impl<A, C, R> RouteSends<A, R> for ProxySends<C>
where
    A: Address + Send,
    A::Nonce: Send,
    C: Behavior<Addr = A>,
    Vec<Delivery<C>>: RouteSends<A, R> + Send,
    ServiceSends<ObserveChild<A::Nonce>>: RouteSends<A, R> + Send,
    ServiceSends<ObserveCreation<A::Nonce>>: RouteSends<A, R> + Send,
    ServiceSends<ReportWorkerStopped<A>>: RouteSends<A, R> + Send,
    ServiceSends<ReportWorkerCreationResolved<A::Nonce>>: RouteSends<A, R> + Send,
    R: Send,
{
    type Error = ProxySendsError<
        <Vec<Delivery<C>> as RouteSends<A, R>>::Error,
        <ServiceSends<ObserveChild<A::Nonce>> as RouteSends<A, R>>::Error,
        <ServiceSends<ObserveCreation<A::Nonce>> as RouteSends<A, R>>::Error,
        <ServiceSends<ReportWorkerStopped<A>> as RouteSends<A, R>>::Error,
        <ServiceSends<ReportWorkerCreationResolved<A::Nonce>> as RouteSends<A, R>>::Error,
    >;

    async fn route(self, from: A, router: &mut R) -> Result<(), Self::Error> {
        self.deliveries
            .route(from, router)
            .await
            .map_err(ProxySendsError::Deliveries)?;
        self.child_observations
            .route(from, router)
            .await
            .map_err(ProxySendsError::ChildObservations)?;
        self.creation_observations
            .route(from, router)
            .await
            .map_err(ProxySendsError::CreationObservations)?;
        self.stopped_reports
            .route(from, router)
            .await
            .map_err(ProxySendsError::StoppedReports)?;
        self.creation_reports
            .route(from, router)
            .await
            .map_err(ProxySendsError::CreationReports)
    }
}

#[cfg(test)]
mod tests {
    use core::convert::Infallible;
    use std::sync::{Arc, Mutex};

    use behavior::{
        Address, Delivery, Exit, MailAddr, ObserveChild, ObserveCreation, ObservePeer, Recipient,
        ReportWorkerCreationResolved, ReportWorkerStopped, ScheduleAfter, ScheduleAt, SendProduct,
        ServiceSends, UnwatchPeer, WatchSends,
    };
    use observe::ObservationSpace;

    use super::{
        AddressRouter, DeliveryEndpoint, DeliveryRouter, EndpointRegistry, IncarnationEndpoint,
        ObservesCreations, PeerObserver, RejectedDelivery, RouteSends, RoutingError,
        SendProductError,
    };

    #[test]
    fn probe_truth_table_distinguishes_creation_lanes_and_nonces() {
        let creations = ServiceSends::one(ObserveCreation::new(7u64));
        assert!(creations.observes_creation(7));
        assert!(
            !creations.observes_creation(9),
            "the creation lane matches only its staged nonce"
        );

        assert!(
            !Vec::<Delivery<AssertBehavior>>::new().observes_creation(7u64),
            "plain deliveries never observe creations"
        );
        assert!(!ServiceSends::<ObserveChild<u64>>::new(Vec::new()).observes_creation(7u64));
        assert!(!ServiceSends::<ObservePeer<MailAddr>>::new(Vec::new()).observes_creation(7u64));
        assert!(
            !ServiceSends::<ReportWorkerStopped<MailAddr>>::new(Vec::new()).observes_creation(7u64)
        );
        assert!(
            !ServiceSends::<ReportWorkerCreationResolved<u64>>::new(Vec::new())
                .observes_creation(7u64)
        );
        assert!(!ServiceSends::<ScheduleAt>::new(Vec::new()).observes_creation(7u64));
        assert!(!ServiceSends::<ScheduleAfter>::new(Vec::new()).observes_creation(7u64));

        let product = SendProduct {
            inner: Vec::<Delivery<AssertBehavior>>::new(),
            own: creations.clone(),
        };
        assert!(product.observes_creation(7));
        assert!(
            !SendProduct {
                inner: Vec::<Delivery<AssertBehavior>>::new(),
                own: ServiceSends::<ObserveChild<u64>>::new(Vec::new()),
            }
            .observes_creation(7u64),
            "a product observes only through a real creation lane"
        );

        let wrapped = WatchSends {
            behavior: creations.clone(),
            observations: ServiceSends::one(ObservePeer::new(MailAddr(3))),
        };
        assert!(wrapped.observes_creation(7));
        assert!(!wrapped.observes_creation(9));
        let unwrapped = WatchSends {
            behavior: Vec::<Delivery<AssertBehavior>>::new(),
            observations: ServiceSends::one(ObservePeer::new(MailAddr(3))),
        };
        assert!(
            !unwrapped.observes_creation(7u64),
            "wrappers delegate to their behavior lane only"
        );

        let deadline = behavior::DeadlineSends {
            behavior: creations.clone(),
            schedules: ServiceSends::<ScheduleAt>::new(Vec::new()),
        };
        assert!(deadline.observes_creation(7));
        let deadline_empty = behavior::DeadlineSends {
            behavior: Vec::<Delivery<AssertBehavior>>::new(),
            schedules: ServiceSends::<ScheduleAt>::new(Vec::new()),
        };
        assert!(!deadline_empty.observes_creation(7u64));

        let timeout = behavior::ReceiveTimeoutSends {
            behavior: creations.clone(),
            schedules: ServiceSends::<ScheduleAfter>::new(Vec::new()),
        };
        assert!(timeout.observes_creation(7));
        let timeout_empty = behavior::ReceiveTimeoutSends {
            behavior: Vec::<Delivery<AssertBehavior>>::new(),
            schedules: ServiceSends::<ScheduleAfter>::new(Vec::new()),
        };
        assert!(!timeout_empty.observes_creation(7u64));

        let supervised = behavior::SupervisorSends::<MailAddr, _, AssertBehavior> {
            behavior: creations.clone(),
            child_observations: ServiceSends::<ObserveChild<u64>>::new(Vec::new()),
            replacement_commands: Vec::new(),
        };
        assert!(supervised.observes_creation(7));
        let supervised_empty = behavior::SupervisorSends::<MailAddr, _, AssertBehavior> {
            behavior: Vec::<Delivery<AssertBehavior>>::new(),
            child_observations: ServiceSends::<ObserveChild<u64>>::new(Vec::new()),
            replacement_commands: Vec::new(),
        };
        assert!(!supervised_empty.observes_creation(7u64));

        let proxy = behavior::ProxySends::<AssertBehavior> {
            deliveries: Vec::new(),
            child_observations: ServiceSends::<ObserveChild<u64>>::new(Vec::new()),
            creation_observations: creations,
            stopped_reports: ServiceSends::<ReportWorkerStopped<MailAddr>>::new(Vec::new()),
            creation_reports: ServiceSends::<ReportWorkerCreationResolved<u64>>::new(Vec::new()),
        };
        assert!(proxy.observes_creation(7));
        assert!(!proxy.observes_creation(9));
        let proxy_empty = behavior::ProxySends::<AssertBehavior> {
            deliveries: Vec::new(),
            child_observations: ServiceSends::<ObserveChild<u64>>::new(Vec::new()),
            creation_observations: ServiceSends::<ObserveCreation<u64>>::new(Vec::new()),
            stopped_reports: ServiceSends::<ReportWorkerStopped<MailAddr>>::new(Vec::new()),
            creation_reports: ServiceSends::<ReportWorkerCreationResolved<u64>>::new(Vec::new()),
        };
        assert!(!proxy_empty.observes_creation(7u64));
    }

    #[test]
    fn peer_observation_cancellation_never_claims_creation_observation() {
        assert!(!ServiceSends::<UnwatchPeer<MailAddr>>::new(Vec::new()).observes_creation(7u64));
    }

    struct AssertChild;

    type AssertBehavior = behavior::Pure<AssertChild>;

    impl behavior::Handler for AssertChild {
        type Addr = MailAddr;
        type Msg = u8;

        fn receive(
            &mut self,
            _from: MailAddr,
            _message: u8,
        ) -> behavior::Acted<
            MailAddr,
            behavior::Never,
            Vec<behavior::Never>,
            behavior::NoBirths,
            behavior::Never,
        > {
            Ok(behavior::Actions::cont())
        }
    }

    #[derive(Clone, Default)]
    struct RecordingEndpoint(Arc<Mutex<Vec<(MailAddr, u8)>>>);

    impl DeliveryEndpoint<MailAddr, u8> for RecordingEndpoint {
        type Error = Infallible;

        async fn deliver(
            &self,
            from: MailAddr,
            message: u8,
        ) -> Result<(), RejectedDelivery<u8, Self::Error>> {
            self.0.lock().expect("endpoint lock").push((from, message));
            Ok(())
        }
    }

    #[derive(Clone)]
    struct FailingEndpoint;

    impl DeliveryEndpoint<MailAddr, u8> for FailingEndpoint {
        type Error = &'static str;

        async fn deliver(
            &self,
            _from: MailAddr,
            message: u8,
        ) -> Result<(), RejectedDelivery<u8, Self::Error>> {
            Err(RejectedDelivery::new(message, "endpoint rejected delivery"))
        }
    }

    #[derive(Default)]
    struct RecordingRouter(Mutex<Vec<(MailAddr, Delivery<AssertBehavior>)>>);

    impl DeliveryRouter<AssertBehavior> for RecordingRouter {
        type Error = &'static str;

        async fn deliver(
            &self,
            from: MailAddr,
            delivery: Delivery<AssertBehavior>,
        ) -> Result<(), Self::Error> {
            if delivery.message == 2 {
                return Err("delivery failed");
            }
            self.0.lock().expect("delivery lock").push((from, delivery));
            Ok(())
        }
    }

    #[tokio::test]
    async fn router_resolves_global_and_child_routes_before_endpoint_delivery() {
        let router = AddressRouter::default();
        let from = MailAddr(7);
        let global = RecordingEndpoint::default();
        let child = RecordingEndpoint::default();
        let _global_lease =
            EndpointRegistry::<AssertBehavior, _>::register(&router, MailAddr(9), global.clone())
                .unwrap();
        let _child_lease =
            EndpointRegistry::<AssertBehavior, _>::register(&router, from.birth(3), child.clone())
                .unwrap();

        <AddressRouter<MailAddr, RecordingEndpoint> as DeliveryRouter<AssertBehavior>>::deliver(
            &router,
            from,
            Delivery::new(Recipient::<AssertBehavior>::global(MailAddr(9)), 11),
        )
        .await
        .unwrap();
        <AddressRouter<MailAddr, RecordingEndpoint> as DeliveryRouter<AssertBehavior>>::deliver(
            &router,
            from,
            Delivery::new(Recipient::<AssertBehavior>::child(3), 12),
        )
        .await
        .unwrap();

        assert_eq!(
            *global.0.lock().expect("global endpoint lock"),
            [(from, 11)]
        );
        assert_eq!(*child.0.lock().expect("child endpoint lock"), [(from, 12)]);
    }

    #[tokio::test]
    async fn router_distinguishes_lookup_from_endpoint_failure() {
        let router = AddressRouter::<MailAddr, FailingEndpoint>::default();
        let from = MailAddr(7);

        assert_eq!(
            router
                .deliver(
                    from,
                    Delivery::new(Recipient::<AssertBehavior>::global(MailAddr(8)), 1),
                )
                .await,
            Err(RoutingError::UnknownAddress {
                address: MailAddr(8),
                message: 1,
            })
        );

        let _lease =
            EndpointRegistry::<AssertBehavior, _>::register(&router, MailAddr(9), FailingEndpoint)
                .unwrap();
        assert_eq!(
            router
                .deliver(
                    from,
                    Delivery::new(Recipient::<AssertBehavior>::global(MailAddr(9)), 2),
                )
                .await,
            Err(RoutingError::Endpoint {
                address: MailAddr(9),
                rejected: RejectedDelivery::new(2, "endpoint rejected delivery"),
            })
        );
    }

    #[tokio::test]
    async fn incarnation_endpoint_preserves_the_inner_rejection() {
        let completion = ObservationSpace::new();
        let _subject = completion.subject(()).unwrap();
        let endpoint = IncarnationEndpoint::new(FailingEndpoint, completion);

        assert_eq!(
            endpoint.deliver(MailAddr(7), 23).await,
            Err(RejectedDelivery::new(23, "endpoint rejected delivery"))
        );
    }

    #[test]
    fn peer_observation_captures_the_resolved_address_generation() {
        let router = AddressRouter::default();
        let old_space = ObservationSpace::new();
        let mut old_subject = old_space.subject(()).unwrap();
        let old_endpoint = IncarnationEndpoint::new(RecordingEndpoint::default(), old_space);
        let old_lease =
            EndpointRegistry::<AssertBehavior, _>::register(&router, MailAddr(9), old_endpoint)
                .unwrap();
        let old_observation = router.observe_peer(MailAddr(9)).unwrap();

        drop(old_lease);
        let new_space = ObservationSpace::new();
        let mut new_subject = new_space.subject(()).unwrap();
        let new_endpoint = IncarnationEndpoint::new(RecordingEndpoint::default(), new_space);
        let _new_lease =
            EndpointRegistry::<AssertBehavior, _>::register(&router, MailAddr(9), new_endpoint)
                .unwrap();
        let new_observation = router.observe_peer(MailAddr(9)).unwrap();

        old_subject.complete(Ok(Exit::Normal));
        new_subject.complete(Ok(Exit::Collected));
        assert_eq!(old_observation.try_get(), Some(Ok(Exit::Normal)));
        assert_eq!(new_observation.try_get(), Some(Ok(Exit::Collected)));
    }

    #[tokio::test]
    async fn routes_composed_products_in_algebra_order() {
        let recipient = Recipient::global(MailAddr(9));
        let sends = SendProduct {
            inner: vec![Delivery::new(recipient, 3), Delivery::new(recipient, 4)],
            own: vec![Delivery::new(recipient, 5)],
        };
        let mut router = RecordingRouter::default();

        sends.route(MailAddr(7), &mut router).await.unwrap();

        assert_eq!(
            router
                .0
                .lock()
                .expect("delivery lock")
                .iter()
                .map(|entry| entry.1.message)
                .collect::<Vec<_>>(),
            [3, 4, 5]
        );
    }

    #[tokio::test]
    async fn stops_at_the_first_delivery_failure() {
        let recipient = Recipient::global(MailAddr(9));
        let sends = vec![
            Delivery::new(recipient, 1),
            Delivery::new(recipient, 2),
            Delivery::new(recipient, 3),
        ];
        let mut router = RecordingRouter::default();

        assert_eq!(
            sends.route(MailAddr(7), &mut router).await,
            Err("delivery failed")
        );
        assert_eq!(
            router
                .0
                .lock()
                .expect("delivery lock")
                .iter()
                .map(|entry| entry.1.message)
                .collect::<Vec<_>>(),
            [1]
        );
    }

    #[tokio::test]
    async fn product_failure_preserves_the_exact_interpreter_leg() {
        let recipient = Recipient::global(MailAddr(9));
        let mut router = RecordingRouter::default();
        let inner_failure = SendProduct {
            inner: vec![Delivery::new(recipient, 2)],
            own: vec![Delivery::new(recipient, 3)],
        };
        assert_eq!(
            inner_failure.route(MailAddr(7), &mut router).await,
            Err(SendProductError::Inner("delivery failed"))
        );

        let own_failure = SendProduct {
            inner: vec![Delivery::new(recipient, 1)],
            own: vec![Delivery::new(recipient, 2)],
        };
        assert_eq!(
            own_failure.route(MailAddr(7), &mut router).await,
            Err(SendProductError::Own("delivery failed"))
        );
    }
}
