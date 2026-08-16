//! Affine runtime port: activate once, run turns, then retire once.

use core::future::Future;

use behavior::{Behavior, Never};

use crate::ActionsOf;

/// A prepared runtime environment that has not exposed its event source.
///
/// Activation consumes the environment and the complete initialization action
/// value. Only successful activation yields an [`ActiveEnvironment`], making
/// initialize-before-ingress ordering structural rather than conventional.
pub trait Environment<B: Behavior<Ph = Never>> {
    /// The only environment value capable of running ordinary turns.
    type Active: ActiveEnvironment<B>;

    /// Failure while committing initialization and making the incarnation live.
    type Error;

    /// Commit initialization and produce the active runtime environment.
    fn activate(
        self,
        actions: ActionsOf<B>,
    ) -> impl Future<Output = Result<Self::Active, Self::Error>>;
}

/// The live runtime half of one exact closed Behavior.
pub trait ActiveEnvironment<B: Behavior<Ph = Never>> {
    /// Failure while committing one ordinary Behavior decision.
    type Error;

    /// Produce the next event, or `None` when the source is closed.
    fn next(&mut self) -> impl Future<Output = Option<B::Event>>;

    /// Apply one successful decision's complete action value.
    ///
    /// Application is ordered but not transactional: an error may report a
    /// factual partial prefix. The Driver neither retries nor rolls it back.
    fn apply(&mut self, actions: ActionsOf<B>) -> impl Future<Output = Result<(), Self::Error>>;

    /// Finish retiring resources owned by this execution before ordinary return.
    ///
    /// This is a completion barrier, not a fallible action interpreter. An
    /// environment must encode any recoverable rejection in [`Self::Error`]
    /// while applying the terminal actions. Once retirement begins there is no
    /// next Driver turn and no retry, rollback, or alternate terminal result.
    /// Panics remain panics; cancellation may drop this future before the
    /// barrier completes.
    fn retire(self) -> impl Future<Output = ()>;
}
