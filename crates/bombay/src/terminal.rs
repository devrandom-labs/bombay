//! Exact application-owned projection of local actor retirement.

use core::marker::PhantomData;

use behavior::{Behavior, BehaviorAddr, BehaviorMessage, ChildRole, Here, Protocol, User};
use bombay_address::ClaimError;
use bombay_engine::{ActionsOf, Completion};

use crate::IncarnationOutcome;
use crate::address::MailAddr;
use crate::interpret::EffectInterpretationError;
use crate::local::{LocalActivationError, LocalResidual};

pub(crate) type LocalOutcome<B, Descendants, CommitError> = IncarnationOutcome<
    B,
    LocalResidual<
        ActionsOf<B>,
        <B as Behavior>::Event,
        User<BehaviorAddr<B>, BehaviorMessage<B>>,
        Descendants,
    >,
    <B as Behavior>::Error,
    LocalActivationError<CommitError, ClaimError<BehaviorAddr<B>>>,
    CommitError,
>;

/// Exact runtime identity of one actor retirement owned by a semantic role.
///
/// Bombay constructs origins only after allocating the concrete actor. The
/// owner and role parameters preserve application meaning without exposing a
/// structural birth position.
pub struct ActorOrigin<Owner, Role = Owner> {
    address: MailAddr,
    nonce: Option<u64>,
    declaration: PhantomData<fn() -> (Owner, Role)>,
}

impl<Owner, Role> Copy for ActorOrigin<Owner, Role> {}

impl<Owner, Role> Clone for ActorOrigin<Owner, Role> {
    fn clone(&self) -> Self {
        *self
    }
}

impl<Owner, Role> ActorOrigin<Owner, Role> {
    pub(crate) const fn root(address: MailAddr) -> Self {
        Self {
            address,
            nonce: None,
            declaration: PhantomData,
        }
    }

    pub(crate) const fn child(address: MailAddr, nonce: u64) -> Self {
        Self {
            address,
            nonce: Some(nonce),
            declaration: PhantomData,
        }
    }

    /// Return the exact allocated address of this actor incarnation.
    #[must_use]
    pub const fn address(&self) -> MailAddr {
        self.address
    }

    /// Return the creator-local nonce, or absence for the application root.
    #[must_use]
    pub const fn nonce(&self) -> Option<u64> {
        self.nonce
    }
}

impl<Owner> ActorOrigin<Owner, Here> {
    /// Convert the runtime root occurrence into its application role.
    #[doc(hidden)]
    #[must_use]
    pub const fn into_declared_root(self) -> ActorOrigin<Owner> {
        ActorOrigin {
            address: self.address,
            nonce: self.nonce,
            declaration: PhantomData,
        }
    }
}

impl<Owner, Position> ActorOrigin<Owner, Position>
where
    Owner: Behavior,
{
    /// Convert a structural occurrence into its statically proven child role.
    #[doc(hidden)]
    #[must_use]
    pub const fn into_declared_child<Role>(self) -> ActorOrigin<Owner, Role>
    where
        Role: ChildRole<Owner, Position = Position>,
    {
        ActorOrigin {
            address: self.address,
            nonce: self.nonce,
            declaration: PhantomData,
        }
    }
}

impl<Owner, Role> core::fmt::Debug for ActorOrigin<Owner, Role> {
    fn fmt(&self, formatter: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        formatter
            .debug_struct("ActorOrigin")
            .field("address", &self.address)
            .field("nonce", &self.nonce)
            .finish_non_exhaustive()
    }
}

impl<Owner, Role> PartialEq for ActorOrigin<Owner, Role> {
    fn eq(&self, other: &Self) -> bool {
        self.address == other.address && self.nonce == other.nonce
    }
}

impl<Owner, Role> Eq for ActorOrigin<Owner, Role> {}

/// Total static lift from one exact terminal source into an application sum.
pub trait ProjectTerminal<Origin, Terminal> {
    /// Preserve the supplied origin and terminal value in the application sum.
    fn project(origin: Origin, terminal: Terminal) -> Self;
}

/// Complete retirement of one local actor.
///
/// Every variant either retains final owned state or names the executor event
/// that made such custody unavailable.
pub enum ActorRetirement<BehaviorState, Root, EffectError = EffectInterpretationError>
where
    BehaviorState: Behavior<Protocol: Protocol<Addr = MailAddr>, Ph = behavior::Never>,
{
    AllocationRejected {
        behavior: BehaviorState,
        reason: behavior::AllocationRejection,
    },
    InitializationRejected {
        behavior: BehaviorState,
        error: BehaviorState::Error,
        control: Vec<BehaviorState::Event>,
        user: Vec<User<MailAddr, BehaviorMessage<BehaviorState>>>,
        descendants: Vec<Root>,
    },
    HostRejected {
        behavior: BehaviorState,
        initialization: ActionsOf<BehaviorState>,
        error: ClaimError<MailAddr>,
        control: Vec<BehaviorState::Event>,
        user: Vec<User<MailAddr, BehaviorMessage<BehaviorState>>>,
        descendants: Vec<Root>,
    },
    InitializationEffectsFailed {
        behavior: BehaviorState,
        error: EffectError,
        control: Vec<BehaviorState::Event>,
        user: Vec<User<MailAddr, BehaviorMessage<BehaviorState>>>,
        descendants: Vec<Root>,
    },
    EndedBeforeActivation {
        completion: Completion,
    },
    Completed {
        behavior: BehaviorState,
        control: Vec<BehaviorState::Event>,
        user: Vec<User<MailAddr, BehaviorMessage<BehaviorState>>>,
        descendants: Vec<Root>,
        completion: Completion,
    },
    BehaviorFailed {
        behavior: BehaviorState,
        control: Vec<BehaviorState::Event>,
        user: Vec<User<MailAddr, BehaviorMessage<BehaviorState>>>,
        descendants: Vec<Root>,
        error: BehaviorState::Error,
    },
    EffectsFailed {
        behavior: BehaviorState,
        error: EffectError,
        control: Vec<BehaviorState::Event>,
        user: Vec<User<MailAddr, BehaviorMessage<BehaviorState>>>,
        descendants: Vec<Root>,
    },
    OwnerCancelled {
        behavior: BehaviorState,
        control: Vec<BehaviorState::Event>,
        user: Vec<User<MailAddr, BehaviorMessage<BehaviorState>>>,
        descendants: Vec<Root>,
    },
    Panicked,
    Cancelled,
}

impl<BehaviorState, Root, EffectError> ActorRetirement<BehaviorState, Root, EffectError>
where
    BehaviorState: Behavior<Protocol: Protocol<Addr = MailAddr>, Ph = behavior::Never>,
{
    fn from_completed(outcome: LocalOutcome<BehaviorState, Vec<Root>, EffectError>) -> Self {
        let IncarnationOutcome::Completed {
            behavior,
            residual,
            completion,
        } = outcome
        else {
            unreachable!("the completed terminal projection received another outcome")
        };
        let LocalResidual::Retired {
            ingress,
            activation_tasks,
            descendants,
            owner_cancellation,
        } = residual
        else {
            unreachable!("a completed local Environment cannot remain uncommitted")
        };
        assert!(
            activation_tasks.is_empty(),
            "terminal projection follows activation-task settlement"
        );
        match owner_cancellation {
            Some(_) => {
                assert_eq!(
                    completion,
                    Completion::Exhausted,
                    "owner cancellation exhausts the active Environment"
                );
                Self::OwnerCancelled {
                    behavior,
                    control: ingress.control,
                    user: ingress.user,
                    descendants,
                }
            }
            None => Self::Completed {
                behavior,
                control: ingress.control,
                user: ingress.user,
                descendants,
                completion,
            },
        }
    }

    pub(crate) fn from_local(outcome: LocalOutcome<BehaviorState, Vec<Root>, EffectError>) -> Self {
        match outcome {
            completed @ IncarnationOutcome::Completed { .. } => Self::from_completed(completed),
            IncarnationOutcome::BehaviorFailed {
                behavior,
                residual:
                    LocalResidual::Retired {
                        ingress,
                        descendants,
                        ..
                    },
                error,
            } => Self::BehaviorFailed {
                behavior,
                control: ingress.control,
                user: ingress.user,
                descendants,
                error,
            },
            IncarnationOutcome::ActivationFailed {
                behavior,
                residual:
                    LocalResidual::Uncommitted {
                        initialization,
                        ingress,
                        descendants,
                        ..
                    },
                error: LocalActivationError::Address(error),
            } => Self::HostRejected {
                behavior,
                initialization,
                error,
                control: ingress.control,
                user: ingress.user,
                descendants,
            },
            IncarnationOutcome::ActivationFailed {
                behavior,
                residual:
                    LocalResidual::Retired {
                        ingress,
                        descendants,
                        ..
                    },
                error: LocalActivationError::Commit(error),
            } => Self::InitializationEffectsFailed {
                behavior,
                error,
                control: ingress.control,
                user: ingress.user,
                descendants,
            },
            IncarnationOutcome::EnvironmentFailed {
                behavior,
                residual:
                    LocalResidual::Retired {
                        ingress,
                        descendants,
                        ..
                    },
                error,
            } => Self::EffectsFailed {
                behavior,
                error,
                control: ingress.control,
                user: ingress.user,
                descendants,
            },
            IncarnationOutcome::Panicked => Self::Panicked,
            IncarnationOutcome::Cancelled => Self::Cancelled,
            IncarnationOutcome::BehaviorFailed {
                residual: LocalResidual::Uncommitted { .. },
                ..
            }
            | IncarnationOutcome::ActivationFailed {
                residual: LocalResidual::Uncommitted { .. },
                error: LocalActivationError::Commit(_),
                ..
            }
            | IncarnationOutcome::ActivationFailed {
                residual: LocalResidual::Retired { .. },
                error: LocalActivationError::Address(_),
                ..
            }
            | IncarnationOutcome::EnvironmentFailed {
                residual: LocalResidual::Uncommitted { .. },
                ..
            } => unreachable!("the local Environment returned an impossible residual phase"),
        }
    }
}
