//! Affine runtime port: activate once, run turns, then retire once.

use core::future::Future;

use behavior::{Behavior, ClassifySettlement, Interpretation, Never, SourceCustody};

use crate::ActionsOf;

/// A prepared runtime environment that has not exposed its event source.
///
/// Activation consumes the environment and the complete initialization action
/// value. Only successful activation yields an [`ActiveEnvironment`], making
/// initialize-before-ingress ordering structural rather than conventional.
pub trait Environment<B: Behavior<Ph = Never>> {
    /// The only environment value capable of running ordinary turns.
    type Active: ActiveEnvironment<B, Residual = Self::Residual, Settlement = Self::Settlement>;

    /// Exact total settlement selected by this closed Behavior/runtime pair.
    type Settlement: ClassifySettlement;

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

    /// Exact runtime-owned state returned through the retirement barrier.
    type Residual;

    /// Produce the next event, or `None` when the source is closed.
    fn next(&mut self) -> impl Future<Output = Option<B::Event>>;

    /// Obtain the next control event after one source result was admitted.
    ///
    /// This path cannot expose ordinary user ingress. It lets the Driver settle
    /// the admitted event and its complete transitive action chain before
    /// returning to an older settlement product.
    fn next_source(&mut self) -> impl Future<Output = Option<B::Event>>;

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
    fn publish(&mut self);

    /// Finish retiring resources owned by this execution before ordinary return.
    ///
    /// This is a completion barrier, not a fallible action interpreter. An
    /// Once retirement begins there is no next Driver turn and no retry,
    /// rollback, or alternate terminal result. `settlements` is ordered from
    /// the next product that would have progressed to the oldest residual.
    /// Panics remain panics; cancellation may drop this future before the
    /// barrier completes.
    fn retire(self, settlements: Vec<Self::Settlement>) -> impl Future<Output = Self::Residual>;
}
