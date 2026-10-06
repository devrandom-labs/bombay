//! Affine runtime port: activate once, run turns, then retire once.

use core::future::Future;
use core::ops::ControlFlow;

use behavior::{Behavior, ClassifySettlement, Interpretation, Never, SourceCustody};

use crate::ActionsOf;

/// A prepared runtime environment that has not exposed its event source.
///
/// Activation loans the original environment and complete initialization action
/// owner while receiving the exact result outside the disposable operation.
/// Only successful activation yields an [`ActiveEnvironment`], making
/// initialize-before-ingress ordering structural rather than conventional.
pub trait Environment<B: Behavior<Ph = Never>> {
    /// The only environment value capable of running ordinary turns.
    type Active: ActiveEnvironment<
            B,
            Residual = Self::Residual,
            Settlement = Self::Settlement,
            RetirementRequest = Self::RetirementRequest,
        >;

    /// Exact total settlement selected by this closed Behavior/runtime pair.
    type Settlement: ClassifySettlement;

    /// Exact environment-owned request selecting retirement without another event fold.
    type RetirementRequest;

    /// Failure while committing initialization and making the incarnation live.
    type Error;

    /// Exact runtime-owned state returned through the retirement barrier.
    type Residual;

    /// Install the incarnation and interpret its complete initialization actions.
    ///
    /// Successful installation acquires an unpublished active environment and
    /// exact interpretation in `received` before producer disposal. The original
    /// input slots become empty only after this structural transfer. A unit return
    /// with no result or remaining input is incomplete, not successful cleanup.
    /// The Driver resolves initialization settlement
    /// custody before calling [`ActiveEnvironment::publish`].
    #[allow(
        clippy::type_complexity,
        reason = "the exact active environment, settlement, activation error, and residual form one public ownership contract"
    )]
    fn activate(
        environment: &mut Option<Self>,
        actions: &mut Option<ActionsOf<B>>,
        received: &mut Option<
            Result<(Self::Active, Interpretation<Self::Settlement>), (Self::Error, Self::Residual)>,
        >,
    ) -> impl Future<Output = ()>
    where
        Self: Sized;

    /// Retire a prepared environment when Behavior initialization is rejected.
    ///
    /// The original owner and every acquired intermediate remain outside work.
    /// Only an actual complete residual in `received` with exhausted original
    /// input marks completed retirement. None does not fabricate a residual.
    fn retire(
        environment: &mut Option<Self>,
        actions: &mut Option<ActionsOf<B>>,
        received: &mut Option<Self::Residual>,
    ) -> impl Future<Output = ()>
    where
        Self: Sized;
}

/// The live runtime half of one exact closed Behavior.
pub trait ActiveEnvironment<B: Behavior<Ph = Never>> {
    /// Exact total settlement selected by this closed Behavior/runtime pair.
    type Settlement: ClassifySettlement;

    /// Exact environment-owned request selecting retirement without another event fold.
    type RetirementRequest;

    /// Exact runtime-owned state returned through the retirement barrier.
    type Residual;

    /// Acquire an ordinary event or transfer the exact retirement request.
    ///
    /// `Continue(Some(event))` moves one event into its next Behavior fold.
    /// `Continue(None)` reports permanent event-source exhaustion. `Break(request)`
    /// transfers that request once to the Driver without claiming exhaustion or
    /// completed cleanup. Already admitted events remain owned by the environment
    /// and must be returned through its retirement residual.
    fn next(
        &mut self,
    ) -> impl Future<Output = ControlFlow<Self::RetirementRequest, Option<B::Event>>>;

    /// Obtain the next control event after one source result was admitted.
    ///
    /// This path cannot expose ordinary user ingress. `Continue(Some(event))`
    /// moves one admitted control event into its next Behavior fold; its complete
    /// transitive action chain precedes older settlement products while acquisition
    /// continues. `Continue(None)` reports source closure with retained settlement
    /// custody, rather than ordinary event-source exhaustion.
    ///
    /// `Break(request)` transfers that exact request once to the Driver. The
    /// environment retains admitted events, and the Driver retains every unoffered
    /// settlement remainder before calling the retirement barrier. No further
    /// event fold or source offer follows the selected request.
    fn next_source(
        &mut self,
    ) -> impl Future<Output = ControlFlow<Self::RetirementRequest, Option<B::Event>>>;

    /// Apply one successful decision's complete action value.
    ///
    /// Interpretation is ordered but not transactional. The returned value
    /// retains every accepted, rejected, corrupt, and unattempted item; the
    /// Driver neither retries nor rolls back a factual prefix.
    fn apply(
        &mut self,
        actions: &mut Option<ActionsOf<B>>,
        received: &mut Option<Interpretation<Self::Settlement>>,
    ) -> impl Future<Output = ()>;

    /// Offer at most one ordered source result from a complete settlement.
    fn offer_next(
        &mut self,
        settlement: &mut Option<Self::Settlement>,
        received: &mut Option<SourceCustody<Self::Settlement>>,
    ) -> impl Future<Output = ()>;

    /// Publish the installed incarnation after initialization custody resolves.
    ///
    /// `Continue(())` reports publication and permits ordinary input acquisition.
    /// `Break(request)` transfers the original retirement request without publication.
    /// The Driver retains the complete settlement suffix, retires the environment,
    /// and never calls publication or acquires another event after that request.
    fn publish(&mut self) -> ControlFlow<Self::RetirementRequest, ()>;

    /// Finish retiring resources owned by this execution before ordinary return.
    ///
    /// Every acquired original child/task row enters an outside receiving owner
    /// before the next operation. The actual environment and settlement input
    /// remain available until a complete original residual is acquired outside
    /// this future. Simultaneous input and result remain factual incomplete custody.
    ///
    /// This is a completion barrier, not a fallible action interpreter.
    /// Uncommitted actions, acquired interpretation/source replies, and acquired
    /// ingress are independent original facts. The host receives them without
    /// changing their typed provenance, together with its own partial progress.
    /// `source_index` is the original current source position among the remaining
    /// settlements when a lower owner acquired input before producing its reply.
    /// Preserve that position with the partial source; never infer it after the
    /// Driver has removed its original ordered row.
    /// Once retirement begins there is no next Driver turn and no retry,
    /// rollback, or alternate terminal result. `settlements` is ordered from
    /// the next product that would have progressed to the oldest residual.
    /// Native construction, poll or disposal failure does not imply completed
    /// retirement. Dropping this borrowing future retains surviving input and
    /// received facts; dropping their whole owner is a separate discharge.
    /// User values destroyed inside consuming user work cannot be reconstructed.
    #[expect(
        clippy::too_many_arguments,
        reason = "each loan preserves an independently acquired original value and its ownership"
    )]
    fn retire(
        environment: &mut Option<Self>,
        actions: &mut Option<ActionsOf<B>>,
        interpretation: &mut Option<Interpretation<Self::Settlement>>,
        source: &mut Option<SourceCustody<Self::Settlement>>,
        source_index: &mut Option<usize>,
        ingress: &mut Option<ControlFlow<Self::RetirementRequest, Option<B::Event>>>,
        settlements: &mut Option<Vec<Self::Settlement>>,
        received: &mut Option<Self::Residual>,
    ) -> impl Future<Output = ()>
    where
        Self: Sized;
}
