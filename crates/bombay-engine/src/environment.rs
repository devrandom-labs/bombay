//! Affine runtime port: activate once, run turns, then retire once.

use core::future::Future;
use core::ops::ControlFlow;

use behavior::{Behavior, ClassifySettlement, Interpretation, Never, SourceCustody};

use crate::ActionsOf;

/// A prepared runtime environment that has not exposed its event source.
///
/// Activation consumes the environment and the complete initialization action
/// value. Only successful activation yields an [`ActiveEnvironment`], making
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
    /// Successful installation returns an unpublished active environment and
    /// the exact interpretation. The Driver resolves initialization settlement
    /// custody before calling [`ActiveEnvironment::publish`].
    #[allow(
        clippy::type_complexity,
        reason = "the exact active environment, settlement, activation error, and residual form one public ownership contract"
    )]
    fn activate(
        self,
        actions: ActionsOf<B>,
    ) -> impl Future<
        Output = Result<
            (Self::Active, Interpretation<Self::Settlement>),
            (Self::Error, Self::Residual),
        >,
    >;

    /// Retire a prepared environment when Behavior initialization is rejected.
    fn retire(self) -> impl Future<Output = Self::Residual>;
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
        actions: ActionsOf<B>,
    ) -> impl Future<Output = Interpretation<Self::Settlement>>;

    /// Offer at most one ordered source result from a complete settlement.
    fn offer_next(
        &mut self,
        settlement: Self::Settlement,
    ) -> impl Future<Output = SourceCustody<Self::Settlement>>;

    /// Publish the installed incarnation after initialization custody resolves.
    ///
    /// `Continue(())` reports publication and permits ordinary input acquisition.
    /// `Break(request)` transfers the original retirement request without publication.
    /// The Driver retains the complete settlement suffix, retires the environment,
    /// and never calls publication or acquires another event after that request.
    fn publish(&mut self) -> ControlFlow<Self::RetirementRequest, ()>;

    /// Finish retiring resources owned by this execution before ordinary return.
    ///
    /// This is a completion barrier, not a fallible action interpreter.
    /// Once retirement begins there is no next Driver turn and no retry,
    /// rollback, or alternate terminal result. `settlements` is ordered from
    /// the next product that would have progressed to the oldest residual.
    /// Panics remain panics; cancellation may drop this future before the
    /// barrier completes.
    fn retire(self, settlements: Vec<Self::Settlement>) -> impl Future<Output = Self::Residual>;
}
