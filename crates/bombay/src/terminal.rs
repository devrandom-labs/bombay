//! Exact application-owned projection of local actor retirement.

use core::marker::PhantomData;

use behavior::{
    Behavior, BehaviorAddr, BehaviorMessage, BehaviorSettlements, ChildRole, Protocol, User,
};
use bombay_address::ClaimError;
use bombay_engine::{ActionsOf, Completion, SettlementFailure};

use tokio::task::JoinError;

use crate::ActorExecutionOutcome;
use crate::address::MailAddr;
use crate::interpret::ActionSettlementOf;
use crate::local::{
    LocalActivationRejection, LocalResidual, LocalRetirementRequest, OwnerCancellation,
};

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
    LocalRetirementRequest,
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
/// that made such custody unavailable. `CapabilityFailed` owns the first task
/// failure acquired as the primary cause. `capability_failures` retains other
/// task failures joined during cleanup without changing that cause.
/// `unread_owner_cancellation` records an accepted owner request not acquired by
/// execution. Its unit carries every field of the private zero-field request;
/// the report conveys no further cancellation authority.
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
        capability_failures: Vec<JoinError>,
        unread_owner_cancellation: Option<()>,
    },
    InitializationPanicked {
        behavior: BehaviorState,
        control: Vec<BehaviorState::Event>,
        user: Vec<User<MailAddr, BehaviorMessage<BehaviorState>>>,
        descendants: Vec<Root>,
        capability_failures: Vec<JoinError>,
        unread_owner_cancellation: Option<()>,
    },
    HostRejected {
        behavior: BehaviorState,
        initialization: ActionsOf<BehaviorState>,
        error: ClaimError<MailAddr>,
        control: Vec<BehaviorState::Event>,
        user: Vec<User<MailAddr, BehaviorMessage<BehaviorState>>>,
        descendants: Vec<Root>,
        capability_failures: Vec<JoinError>,
        unread_owner_cancellation: Option<()>,
    },
    BindingAbandoned {
        behavior: BehaviorState,
        initialization: ActionsOf<BehaviorState>,
        control: Vec<BehaviorState::Event>,
        user: Vec<User<MailAddr, BehaviorMessage<BehaviorState>>>,
        descendants: Vec<Root>,
        capability_failures: Vec<JoinError>,
        unread_owner_cancellation: Option<()>,
    },
    Completed {
        behavior: BehaviorState,
        settlements: Vec<ActionSettlementOf<BehaviorState>>,
        control: Vec<BehaviorState::Event>,
        user: Vec<User<MailAddr, BehaviorMessage<BehaviorState>>>,
        descendants: Vec<Root>,
        capability_failures: Vec<JoinError>,
        unread_owner_cancellation: Option<()>,
        completion: Completion,
    },
    BehaviorFailed {
        behavior: BehaviorState,
        settlements: Vec<ActionSettlementOf<BehaviorState>>,
        control: Vec<BehaviorState::Event>,
        user: Vec<User<MailAddr, BehaviorMessage<BehaviorState>>>,
        descendants: Vec<Root>,
        capability_failures: Vec<JoinError>,
        unread_owner_cancellation: Option<()>,
        error: BehaviorState::Error,
    },
    EffectsFailed {
        behavior: BehaviorState,
        settlements: Vec<ActionSettlementOf<BehaviorState>>,
        error: SettlementFailure,
        control: Vec<BehaviorState::Event>,
        user: Vec<User<MailAddr, BehaviorMessage<BehaviorState>>>,
        descendants: Vec<Root>,
        capability_failures: Vec<JoinError>,
        unread_owner_cancellation: Option<()>,
    },
    OwnerCancelled {
        behavior: BehaviorState,
        settlements: Vec<ActionSettlementOf<BehaviorState>>,
        control: Vec<BehaviorState::Event>,
        user: Vec<User<MailAddr, BehaviorMessage<BehaviorState>>>,
        descendants: Vec<Root>,
        capability_failures: Vec<JoinError>,
        unread_owner_cancellation: Option<()>,
    },
    /// Live execution acquired this exact capability task failure as its primary cause.
    CapabilityFailed {
        behavior: BehaviorState,
        settlements: Vec<ActionSettlementOf<BehaviorState>>,
        control: Vec<BehaviorState::Event>,
        user: Vec<User<MailAddr, BehaviorMessage<BehaviorState>>>,
        descendants: Vec<Root>,
        error: JoinError,
        capability_failures: Vec<JoinError>,
        unread_owner_cancellation: Option<()>,
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
        let LocalResidual::Retired {
            settlements,
            ingress,
            activation_tasks,
            descendants,
            capability_failures,
            unread_owner_cancellation,
        } = residual
        else {
            unreachable!("a completed local Environment cannot remain uncommitted");
        };
        assert!(
            activation_tasks.is_empty(),
            "terminal projection follows activation-task settlement"
        );
        match completion {
            Completion::Stopped => Self::Completed {
                behavior,
                settlements,
                control: ingress.control,
                user: ingress.user,
                descendants,
                capability_failures,
                unread_owner_cancellation,
                completion: Completion::Stopped,
            },
            Completion::Exhausted => Self::Completed {
                behavior,
                settlements,
                control: ingress.control,
                user: ingress.user,
                descendants,
                capability_failures,
                unread_owner_cancellation,
                completion: Completion::Exhausted,
            },
            Completion::RetirementRequested(LocalRetirementRequest::OwnerCancellation(
                OwnerCancellation,
            )) => Self::OwnerCancelled {
                behavior,
                settlements,
                control: ingress.control,
                user: ingress.user,
                descendants,
                capability_failures,
                unread_owner_cancellation,
            },
            Completion::RetirementRequested(LocalRetirementRequest::CapabilityFailed(error)) => {
                Self::CapabilityFailed {
                    behavior,
                    settlements,
                    control: ingress.control,
                    user: ingress.user,
                    descendants,
                    error,
                    capability_failures,
                    unread_owner_cancellation,
                }
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
                        capability_failures,
                        unread_owner_cancellation,
                        ..
                    },
                error,
            } => Self::InitializationRejected {
                behavior,
                error,
                control: ingress.control,
                user: ingress.user,
                descendants,
                capability_failures,
                unread_owner_cancellation,
            },
            ActorExecutionOutcome::BehaviorFailed {
                behavior,
                residual:
                    LocalResidual::Retired {
                        settlements,
                        ingress,
                        descendants,
                        capability_failures,
                        unread_owner_cancellation,
                        ..
                    },
                error,
            } => Self::BehaviorFailed {
                behavior,
                settlements,
                control: ingress.control,
                user: ingress.user,
                descendants,
                capability_failures,
                unread_owner_cancellation,
                error,
            },
            ActorExecutionOutcome::InitializationPanicked {
                behavior,
                residual:
                    LocalResidual::Prepared {
                        ingress,
                        descendants,
                        capability_failures,
                        unread_owner_cancellation,
                        ..
                    },
            } => Self::InitializationPanicked {
                behavior,
                control: ingress.control,
                user: ingress.user,
                descendants,
                capability_failures,
                unread_owner_cancellation,
            },
            ActorExecutionOutcome::ActivationFailed {
                behavior,
                residual:
                    LocalResidual::Uncommitted {
                        initialization,
                        ingress,
                        descendants,
                        capability_failures,
                        unread_owner_cancellation,
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
                    capability_failures,
                    unread_owner_cancellation,
                },
                LocalActivationRejection::BindingAbandoned => Self::BindingAbandoned {
                    behavior,
                    initialization,
                    control: ingress.control,
                    user: ingress.user,
                    descendants,
                    capability_failures,
                    unread_owner_cancellation,
                },
            },
            ActorExecutionOutcome::SettlementFailed {
                behavior,
                residual:
                    LocalResidual::Retired {
                        settlements,
                        ingress,
                        descendants,
                        capability_failures,
                        unread_owner_cancellation,
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
                capability_failures,
                unread_owner_cancellation,
            },
            ActorExecutionOutcome::Panicked => Self::Panicked,
            ActorExecutionOutcome::Cancelled => Self::Cancelled,
            ActorExecutionOutcome::BehaviorFailed {
                residual: LocalResidual::Uncommitted { .. },
                ..
            }
            | ActorExecutionOutcome::ActivationFailed {
                residual: LocalResidual::Retired { .. } | LocalResidual::Prepared { .. },
                ..
            }
            | ActorExecutionOutcome::SettlementFailed {
                residual: LocalResidual::Uncommitted { .. } | LocalResidual::Prepared { .. },
                ..
            }
            | ActorExecutionOutcome::InitializationPanicked {
                residual: LocalResidual::Uncommitted { .. } | LocalResidual::Retired { .. },
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

#[cfg(test)]
mod capability_retirement_projection {
    use std::panic::panic_any;

    use behavior::{
        Actions, ActiveTurn, BehaviorActed, MessageProtocol, Never, NoBirths, NoSends, Step, User,
    };
    use communication::Drained;

    use super::{ActorRetirement, LocalOutcome};
    use crate::ActorExecutionOutcome;
    use crate::MailAddr;
    use crate::local::{
        ActivationTasks, LocalActivationRejection, LocalResidual, LocalRetirementRequest,
        OwnerCancellation,
    };
    use bombay_engine::Completion;

    struct RetiringActor {
        values: Vec<u64>,
    }

    impl behavior::Behavior for RetiringActor {
        type Protocol = MessageProtocol<MailAddr, Never>;
        type Event = User<MailAddr, Never>;
        type Sends = NoSends;
        type Ph = Never;
        type Error = Never;
        type Birth = NoBirths;

        fn transition(&mut self, _: ActiveTurn, event: Self::Event) -> BehaviorActed<Self> {
            match event.message {}
        }
    }

    #[derive(Debug, PartialEq, Eq)]
    enum RetirementCause {
        InitializationPanicked,
        BindingAbandoned,
        Stopped,
        OwnerCancellation,
        CapabilityFailed,
    }

    #[tokio::test]
    #[allow(
        clippy::too_many_lines,
        reason = "one complete typed projection trace observes all three residual phases and their separate primary causes"
    )]
    async fn every_residual_phase_preserves_later_failure_and_unread_owner_in_public_retirement() {
        for cause in [
            RetirementCause::InitializationPanicked,
            RetirementCause::BindingAbandoned,
            RetirementCause::Stopped,
            RetirementCause::OwnerCancellation,
            RetirementCause::CapabilityFailed,
        ] {
            let values = vec![77, 177];
            let allocation = values.as_ptr();
            let panic_values = Box::new(vec![88_u64, 188]);
            let panic_allocation = panic_values.as_ptr();
            let mut tasks = ActivationTasks::new();
            tasks.spawn(async move { panic_any(panic_values) });
            let behavior = RetiringActor { values };
            let ingress = Drained {
                control: Vec::new(),
                user: Vec::new(),
            };
            let residual = match cause {
                RetirementCause::InitializationPanicked => LocalResidual::Prepared {
                    ingress,
                    activation_tasks: tasks,
                    descendants: vec![99],
                    capability_failures: Vec::new(),
                    unread_owner_cancellation: Some(()),
                },
                RetirementCause::BindingAbandoned => LocalResidual::Uncommitted {
                    initialization: Actions::cont(),
                    ingress,
                    activation_tasks: tasks,
                    descendants: vec![99],
                    capability_failures: Vec::new(),
                    unread_owner_cancellation: Some(()),
                },
                RetirementCause::Stopped
                | RetirementCause::OwnerCancellation
                | RetirementCause::CapabilityFailed => LocalResidual::Retired {
                    settlements: Vec::new(),
                    ingress,
                    activation_tasks: tasks,
                    descendants: vec![99],
                    capability_failures: Vec::new(),
                    unread_owner_cancellation: match cause {
                        RetirementCause::OwnerCancellation => None,
                        _ => Some(()),
                    },
                },
            };
            let residual = residual.settle_activation_tasks().await;
            let outcome: LocalOutcome<RetiringActor, Vec<u64>> = match cause {
                RetirementCause::InitializationPanicked => {
                    ActorExecutionOutcome::InitializationPanicked { behavior, residual }
                }
                RetirementCause::BindingAbandoned => ActorExecutionOutcome::ActivationFailed {
                    behavior,
                    residual,
                    error: LocalActivationRejection::BindingAbandoned,
                },
                RetirementCause::Stopped => ActorExecutionOutcome::Completed {
                    behavior,
                    residual,
                    completion: Completion::Stopped,
                },
                RetirementCause::OwnerCancellation => ActorExecutionOutcome::Completed {
                    behavior,
                    residual,
                    completion: Completion::RetirementRequested(
                        LocalRetirementRequest::OwnerCancellation(OwnerCancellation),
                    ),
                },
                RetirementCause::CapabilityFailed => {
                    let primary = tokio::spawn(async { panic_any(Box::new(vec![66_u64, 166])) })
                        .await
                        .expect_err("the primary task failure is retained");
                    ActorExecutionOutcome::Completed {
                        behavior,
                        residual,
                        completion: Completion::RetirementRequested(
                            LocalRetirementRequest::CapabilityFailed(primary),
                        ),
                    }
                }
            };
            let (behavior, control, user, descendants, mut failures, unread) =
                match ActorRetirement::from_local(outcome) {
                    ActorRetirement::InitializationPanicked {
                        behavior,
                        control,
                        user,
                        descendants,
                        capability_failures,
                        unread_owner_cancellation,
                    } => {
                        assert_eq!(cause, RetirementCause::InitializationPanicked);
                        (
                            behavior,
                            control,
                            user,
                            descendants,
                            capability_failures,
                            unread_owner_cancellation,
                        )
                    }
                    ActorRetirement::BindingAbandoned {
                        behavior,
                        initialization,
                        control,
                        user,
                        descendants,
                        capability_failures,
                        unread_owner_cancellation,
                    } => {
                        assert_eq!(cause, RetirementCause::BindingAbandoned);
                        assert_eq!(initialization.sends, NoSends);
                        assert!(initialization.creates.is_empty());
                        assert!(matches!(initialization.become_, Step::Continue));
                        (
                            behavior,
                            control,
                            user,
                            descendants,
                            capability_failures,
                            unread_owner_cancellation,
                        )
                    }
                    ActorRetirement::Completed {
                        behavior,
                        settlements,
                        control,
                        user,
                        descendants,
                        completion,
                        capability_failures,
                        unread_owner_cancellation,
                    } => {
                        assert_eq!(cause, RetirementCause::Stopped);
                        assert_eq!(completion, Completion::Stopped);
                        assert_eq!(settlements.len(), 0);
                        (
                            behavior,
                            control,
                            user,
                            descendants,
                            capability_failures,
                            unread_owner_cancellation,
                        )
                    }
                    ActorRetirement::OwnerCancelled {
                        behavior,
                        settlements,
                        control,
                        user,
                        descendants,
                        capability_failures,
                        unread_owner_cancellation,
                    } => {
                        assert_eq!(cause, RetirementCause::OwnerCancellation);
                        assert_eq!(settlements.len(), 0);
                        (
                            behavior,
                            control,
                            user,
                            descendants,
                            capability_failures,
                            unread_owner_cancellation,
                        )
                    }
                    ActorRetirement::CapabilityFailed {
                        behavior,
                        settlements,
                        control,
                        user,
                        descendants,
                        error,
                        capability_failures,
                        unread_owner_cancellation,
                    } => {
                        assert_eq!(cause, RetirementCause::CapabilityFailed);
                        assert_eq!(settlements.len(), 0);
                        let primary = error
                            .into_panic()
                            .downcast::<Box<Vec<u64>>>()
                            .expect("the exact acquired primary panic value remains separate");
                        assert_eq!(**primary, [66, 166]);
                        (
                            behavior,
                            control,
                            user,
                            descendants,
                            capability_failures,
                            unread_owner_cancellation,
                        )
                    }
                    _ => panic!(
                        "the selected primary cause was changed during exact public projection"
                    ),
                };
            assert_eq!(behavior.values, [77, 177]);
            assert_eq!(behavior.values.as_ptr(), allocation);
            assert_eq!(control.len(), 0);
            assert_eq!(user.len(), 0);
            assert_eq!(descendants, [99]);
            assert_eq!(
                unread,
                match cause {
                    RetirementCause::OwnerCancellation => None,
                    _ => Some(()),
                }
            );
            assert_eq!(failures.len(), 1);
            let failure = failures
                .pop()
                .expect("one exact late failure survives every projection");
            let payload = failure
                .into_panic()
                .downcast::<Box<Vec<u64>>>()
                .expect("the late original panic value remains owned");
            assert_eq!(payload.as_ptr(), panic_allocation);
            assert_eq!(**payload, [88, 188]);
        }
    }
}
