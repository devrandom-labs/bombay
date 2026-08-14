//! Runtime construction of children emitted by behavior transitions.

use core::future::Future;

use behavior::{Address, BirthMode, Births, Create, Never, NoBirths};

pub(crate) trait SealedChildRuntime {}
pub(crate) trait SealedBirthMode {}

impl SealedBirthMode for NoBirths {}
impl<C> SealedBirthMode for Births<C> {}

/// Constructs one child generation and returns its affine ownership lease.
#[allow(
    private_bounds,
    reason = "sealing supertrait is deliberately crate-private"
)]
#[doc(hidden)]
pub trait ChildRuntime<A: Address, B, S>: SealedChildRuntime {
    /// Fully typed ownership retained by the parent generation.
    type Lease: super::CoordinatedChild;
    /// Creation failure.
    type Error;

    /// Birth `child` beneath `parent` at the emitted nonce.
    fn birth(
        &self,
        parent: A,
        child: Create<A, B>,
        response: S,
    ) -> impl Future<Output = Result<Self::Lease, Self::Error>> + Send;
}

/// Runtime used when the behavior's birth type is uninhabited.
#[derive(Debug, Clone, Copy)]
#[doc(hidden)]
pub struct NoChildren(());

impl SealedChildRuntime for NoChildren {}

impl NoChildren {
    pub(crate) const fn new() -> Self {
        Self(())
    }
}

impl<A, S> ChildRuntime<A, Never, S> for NoChildren
where
    A: Address + Send,
    A::Nonce: Send,
    S: Send,
{
    type Lease = Never;
    type Error = Never;

    async fn birth(
        &self,
        _parent: A,
        child: Create<A, Never>,
        _response: S,
    ) -> Result<Self::Lease, Self::Error> {
        match child.child {}
    }
}

/// Derives a generation-local child runtime from a behavior's birth mode.
#[allow(
    private_bounds,
    reason = "sealed birth-mode supertrait is deliberately crate-private"
)]
#[doc(hidden)]
pub trait RuntimeBirthMode<A: Address, Y, S>: BirthMode + SealedBirthMode {
    /// Runtime constructed for one parent generation.
    type Runtime: ChildRuntime<A, Self::Child, S>;

    /// Construct an empty runtime for a new parent.
    fn runtime(system: Y) -> Self::Runtime;
}

impl<A, Y, S> RuntimeBirthMode<A, Y, S> for NoBirths
where
    A: Address + Send,
    A::Nonce: Send,
    S: Send,
{
    type Runtime = NoChildren;

    fn runtime(_system: Y) -> Self::Runtime {
        NoChildren::new()
    }
}

/// System-backed child runtime derived for `Births<C>`.
#[doc(hidden)]
pub struct SystemChildren<Y> {
    pub(crate) system: Y,
}

impl<Y> SealedChildRuntime for SystemChildren<Y> {}

impl<A: Address, Y, C, S> RuntimeBirthMode<A, Y, S> for Births<C>
where
    SystemChildren<Y>: ChildRuntime<A, C, S>,
{
    type Runtime = SystemChildren<Y>;

    fn runtime(system: Y) -> Self::Runtime {
        SystemChildren { system }
    }
}

/// Failure while interpreting a behavior effect.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[doc(hidden)]
pub enum RuntimeEffectError<C, D> {
    /// Child birth failed.
    #[error("child birth failed")]
    Birth(#[source] C),
    /// A behavior reused a child identity within one parent incarnation.
    #[error("a behavior reused a child identity within one parent incarnation")]
    DuplicateChild,
    /// Outbound delivery failed.
    #[error("outbound delivery failed")]
    Delivery(#[source] D),
}

/// Classifies a child-creation failure for same-action rejection delivery.
///
/// The interpreter keeps the exact typed error for unobserved creations; this
/// classification only selects the [`behavior::CreationRejection`] reported to
/// a behavior that staged an `ObserveCreation` for the failed nonce.
pub trait CreationFailure {
    /// The closed semantic classification of this creation failure.
    fn rejection(&self) -> behavior::CreationRejection;
}

impl CreationFailure for Never {
    fn rejection(&self) -> behavior::CreationRejection {
        match *self {}
    }
}
