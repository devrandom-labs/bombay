//! Exact application-owned projection of local actor retirement.

use core::marker::PhantomData;

use behavior::{
    Behavior, BehaviorAddr, BehaviorMessage, BehaviorSettlements, ChildRole, Protocol, User,
};
use bombay_address::ClaimError;
use bombay_engine::{ActionsOf, Completion, SettlementFailure};

use crate::ActorExecutionOutcome;
use crate::address::MailAddr;
use crate::interpret::ActionSettlementOf;
use crate::local::{LocalActivationRejection, LocalResidual};

pub(crate) type LocalOutcome<B, Descendants> = ActorExecutionOutcome<
    B,
    LocalResidual<
        ActionsOf<B>,
        ActionSettlementOf<B>,
        <B as Behavior>::Event,
        User<BehaviorAddr<B>, BehaviorMessage<B>>,
        Descendants,
    >,
    <B as Behavior>::Error,
    LocalActivationRejection<BehaviorAddr<B>>,
>;

/// Exact allocated address of the application's root actor retirement.
pub struct RootOrigin<Owner> {
    address: MailAddr,
    owner: PhantomData<fn() -> Owner>,
}

impl<Owner> Copy for RootOrigin<Owner> {}

impl<Owner> Clone for RootOrigin<Owner> {
    fn clone(&self) -> Self {
        *self
    }
}

impl<Owner> RootOrigin<Owner> {
    pub(crate) const fn new(address: MailAddr) -> Self {
        Self {
            address,
            owner: PhantomData,
        }
    }

    /// Return the exact allocated address of the root incarnation.
    #[must_use]
    pub const fn address(&self) -> MailAddr {
        self.address
    }
}

impl<Owner> core::fmt::Debug for RootOrigin<Owner> {
    fn fmt(&self, formatter: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        formatter
            .debug_struct("RootOrigin")
            .field("address", &self.address)
            .finish_non_exhaustive()
    }
}

impl<Owner> PartialEq for RootOrigin<Owner> {
    fn eq(&self, other: &Self) -> bool {
        self.address == other.address
    }
}

impl<Owner> Eq for RootOrigin<Owner> {}

/// Exact allocated address and creator-local nonce of a child retirement.
/// The owner and role preserve the statically declared application meaning.
pub struct ChildOrigin<Owner, Role> {
    address: MailAddr,
    nonce: u64,
    declaration: PhantomData<fn() -> (Owner, Role)>,
}

impl<Owner, Role> Copy for ChildOrigin<Owner, Role> {}

impl<Owner, Role> Clone for ChildOrigin<Owner, Role> {
    fn clone(&self) -> Self {
        *self
    }
}

impl<Owner, Role> ChildOrigin<Owner, Role> {
    pub(crate) const fn new(address: MailAddr, nonce: u64) -> Self {
        Self {
            address,
            nonce,
            declaration: PhantomData,
        }
    }

    /// Return the exact allocated address of the child incarnation.
    #[must_use]
    pub const fn address(&self) -> MailAddr {
        self.address
    }

    /// Return the creator-local nonce of this child occurrence.
    #[must_use]
    pub const fn nonce(&self) -> u64 {
        self.nonce
    }
}

impl<Owner, Position> ChildOrigin<Owner, Position>
where
    Owner: Behavior,
{
    /// Convert a structural occurrence into its statically proven child role.
    #[doc(hidden)]
    #[must_use]
    pub const fn into_declared_child<Role>(self) -> ChildOrigin<Owner, Role>
    where
        Role: ChildRole<Owner, Position = Position>,
    {
        ChildOrigin {
            address: self.address,
            nonce: self.nonce,
            declaration: PhantomData,
        }
    }
}

impl<Owner, Role> core::fmt::Debug for ChildOrigin<Owner, Role> {
    fn fmt(&self, formatter: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        formatter
            .debug_struct("ChildOrigin")
            .field("address", &self.address)
            .field("nonce", &self.nonce)
            .finish_non_exhaustive()
    }
}

impl<Owner, Role> PartialEq for ChildOrigin<Owner, Role> {
    fn eq(&self, other: &Self) -> bool {
        self.address == other.address && self.nonce == other.nonce
    }
}

impl<Owner, Role> Eq for ChildOrigin<Owner, Role> {}

/// Total static lift from one exact terminal source into an application sum.
pub trait ProjectTerminal<Origin, Terminal> {
    /// Preserve the supplied origin and terminal value in the application sum.
    fn project(origin: Origin, terminal: Terminal) -> Self;
}

/// Complete retirement of one local actor.
///
/// Every variant either retains final owned state or names the executor event
/// that made such custody unavailable.
#[must_use = "the exact actor retirement must be inspected or explicitly discharged"]
pub enum ActorRetirement<BehaviorState, Root>
where
    BehaviorState: BehaviorSettlements<Protocol: Protocol<Addr = MailAddr>, Ph = behavior::Never>,
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
    InitializationPanicked {
        behavior: BehaviorState,
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
    BindingAbandoned {
        behavior: BehaviorState,
        initialization: ActionsOf<BehaviorState>,
        control: Vec<BehaviorState::Event>,
        user: Vec<User<MailAddr, BehaviorMessage<BehaviorState>>>,
        descendants: Vec<Root>,
    },
    Completed {
        behavior: BehaviorState,
        settlements: Vec<ActionSettlementOf<BehaviorState>>,
        control: Vec<BehaviorState::Event>,
        user: Vec<User<MailAddr, BehaviorMessage<BehaviorState>>>,
        descendants: Vec<Root>,
        completion: Completion,
    },
    BehaviorFailed {
        behavior: BehaviorState,
        settlements: Vec<ActionSettlementOf<BehaviorState>>,
        control: Vec<BehaviorState::Event>,
        user: Vec<User<MailAddr, BehaviorMessage<BehaviorState>>>,
        descendants: Vec<Root>,
        error: BehaviorState::Error,
    },
    EffectsFailed {
        behavior: BehaviorState,
        settlements: Vec<ActionSettlementOf<BehaviorState>>,
        error: SettlementFailure,
        control: Vec<BehaviorState::Event>,
        user: Vec<User<MailAddr, BehaviorMessage<BehaviorState>>>,
        descendants: Vec<Root>,
    },
    OwnerCancelled {
        behavior: BehaviorState,
        settlements: Vec<ActionSettlementOf<BehaviorState>>,
        control: Vec<BehaviorState::Event>,
        user: Vec<User<MailAddr, BehaviorMessage<BehaviorState>>>,
        descendants: Vec<Root>,
    },
    Panicked,
    Cancelled,
}

impl<BehaviorState, Root> ActorRetirement<BehaviorState, Root>
where
    BehaviorState: BehaviorSettlements<Protocol: Protocol<Addr = MailAddr>, Ph = behavior::Never>,
{
    fn from_completed(outcome: LocalOutcome<BehaviorState, Vec<Root>>) -> Self {
        let ActorExecutionOutcome::Completed {
            behavior,
            residual,
            completion,
        } = outcome
        else {
            unreachable!("the completed terminal projection received another outcome")
        };
        match residual {
            LocalResidual::OwnerCancelled {
                settlements,
                ingress,
                activation_tasks,
                descendants,
                cancellation: _,
            } => {
                assert!(
                    activation_tasks.is_empty(),
                    "terminal projection follows activation-task settlement"
                );
                assert_eq!(
                    completion,
                    Completion::Exhausted,
                    "owner cancellation exhausts the active Environment"
                );
                Self::OwnerCancelled {
                    behavior,
                    settlements,
                    control: ingress.control,
                    user: ingress.user,
                    descendants,
                }
            }
            LocalResidual::Retired {
                settlements,
                ingress,
                activation_tasks,
                descendants,
            } => {
                assert!(
                    activation_tasks.is_empty(),
                    "terminal projection follows activation-task settlement"
                );
                Self::Completed {
                    behavior,
                    settlements,
                    control: ingress.control,
                    user: ingress.user,
                    descendants,
                    completion,
                }
            }
            LocalResidual::Prepared { .. } | LocalResidual::Uncommitted { .. } => {
                unreachable!("a completed local Environment cannot remain uncommitted")
            }
        }
    }

    #[allow(
        clippy::too_many_lines,
        reason = "one exhaustive terminal projection preserves each distinct residual phase and exact custody"
    )]
    pub(crate) fn from_local(outcome: LocalOutcome<BehaviorState, Vec<Root>>) -> Self {
        match outcome {
            completed @ ActorExecutionOutcome::Completed { .. } => Self::from_completed(completed),
            ActorExecutionOutcome::BehaviorFailed {
                behavior,
                residual:
                    LocalResidual::Prepared {
                        ingress,
                        descendants,
                        ..
                    },
                error,
            } => Self::InitializationRejected {
                behavior,
                error,
                control: ingress.control,
                user: ingress.user,
                descendants,
            },
            ActorExecutionOutcome::BehaviorFailed {
                behavior,
                residual:
                    LocalResidual::Retired {
                        settlements,
                        ingress,
                        descendants,
                        ..
                    },
                error,
            } => Self::BehaviorFailed {
                behavior,
                settlements,
                control: ingress.control,
                user: ingress.user,
                descendants,
                error,
            },
            ActorExecutionOutcome::InitializationPanicked {
                behavior,
                residual:
                    LocalResidual::Prepared {
                        ingress,
                        descendants,
                        ..
                    },
            } => Self::InitializationPanicked {
                behavior,
                control: ingress.control,
                user: ingress.user,
                descendants,
            },
            ActorExecutionOutcome::ActivationFailed {
                behavior,
                residual:
                    LocalResidual::Uncommitted {
                        initialization,
                        ingress,
                        descendants,
                        ..
                    },
                error,
            } => match error {
                LocalActivationRejection::Address(error) => Self::HostRejected {
                    behavior,
                    initialization,
                    error,
                    control: ingress.control,
                    user: ingress.user,
                    descendants,
                },
                LocalActivationRejection::BindingAbandoned => Self::BindingAbandoned {
                    behavior,
                    initialization,
                    control: ingress.control,
                    user: ingress.user,
                    descendants,
                },
            },
            ActorExecutionOutcome::SettlementFailed {
                behavior,
                residual:
                    LocalResidual::Retired {
                        settlements,
                        ingress,
                        descendants,
                        ..
                    },
                error,
            } => Self::EffectsFailed {
                behavior,
                settlements,
                error,
                control: ingress.control,
                user: ingress.user,
                descendants,
            },
            ActorExecutionOutcome::Panicked => Self::Panicked,
            ActorExecutionOutcome::Cancelled => Self::Cancelled,
            ActorExecutionOutcome::BehaviorFailed {
                residual: LocalResidual::Uncommitted { .. },
                ..
            }
            | ActorExecutionOutcome::BehaviorFailed {
                residual: LocalResidual::OwnerCancelled { .. },
                ..
            }
            | ActorExecutionOutcome::ActivationFailed {
                residual:
                    LocalResidual::Retired { .. }
                    | LocalResidual::OwnerCancelled { .. }
                    | LocalResidual::Prepared { .. },
                ..
            }
            | ActorExecutionOutcome::SettlementFailed {
                residual:
                    LocalResidual::Uncommitted { .. }
                    | LocalResidual::Prepared { .. }
                    | LocalResidual::OwnerCancelled { .. },
                ..
            }
            | ActorExecutionOutcome::InitializationPanicked {
                residual:
                    LocalResidual::Uncommitted { .. }
                    | LocalResidual::Retired { .. }
                    | LocalResidual::OwnerCancelled { .. },
                ..
            } => unreachable!("the local Environment returned an impossible residual phase"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    struct Root;
    struct ChildRole;

    #[test]
    fn root_and_child_origins_keep_distinct_exact_payloads() {
        let root = RootOrigin::<Root>::new(MailAddr(1));
        let child = ChildOrigin::<Root, ChildRole>::new(MailAddr(7), 13);
        assert_eq!(root.address(), MailAddr(1));
        assert_eq!(child.address(), MailAddr(7));
        assert_eq!(child.nonce(), 13);
    }

    #[test]
    fn root_origin_identity_and_diagnostic_preserve_its_address() {
        let first = RootOrigin::<Root>::new(MailAddr(1));
        let same = RootOrigin::<Root>::new(MailAddr(1));
        let other = RootOrigin::<Root>::new(MailAddr(2));

        assert_eq!(first, same);
        assert_ne!(first, other);
        let diagnostic = format!("{first:?}");
        assert!(diagnostic.contains("RootOrigin"));
        assert!(diagnostic.contains("MailAddr(1)"));
    }

    #[test]
    fn child_origin_identity_and_diagnostic_preserve_address_and_nonce() {
        let first = ChildOrigin::<Root, ChildRole>::new(MailAddr(7), 13);
        let same = ChildOrigin::<Root, ChildRole>::new(MailAddr(7), 13);
        let other_address = ChildOrigin::<Root, ChildRole>::new(MailAddr(8), 13);
        let other_nonce = ChildOrigin::<Root, ChildRole>::new(MailAddr(7), 14);

        assert_eq!(first, same);
        assert_ne!(first, other_address);
        assert_ne!(first, other_nonce);
        let diagnostic = format!("{first:?}");
        assert!(diagnostic.contains("ChildOrigin"));
        assert!(diagnostic.contains("MailAddr(7)"));
        assert!(diagnostic.contains("nonce: 13"));
    }
}
