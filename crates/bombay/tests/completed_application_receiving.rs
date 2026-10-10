use bombay::ActorNotificationReceipts;
use core::future::{Future, poll_fn};
use core::mem;
use core::num::NonZeroUsize;
use core::pin::{Pin, pin};
use core::task::{Context, Poll};
use std::any::Any;
use std::cell::Cell;
use std::panic::{AssertUnwindSafe, catch_unwind, panic_any, resume_unwind};
use std::ptr;
use std::rc::Rc;
use std::sync::{Arc, Mutex, PoisonError, Weak};

use behavior_actors::{Exit, StopOnShutdown};
use bombay::behavior::{
    ActionSettlement, Actions, BehaviorActed, BehaviorBase, ChildChoice, ChildCons,
    ChildCreationOutcome, CreationKind, CreationSettlement, EstablishedDelivery,
    EstablishedRecipient, EventLayer, ItemSettlement, Never, NoChildren, NoSends, Protocol,
    SettledItem, Step, User,
};
use bombay_engine::Completion;
use tokio::runtime::{Builder, Handle, Id};
use tokio::sync::oneshot::{self, error::RecvError};
use tokio::task::JoinError;

use bombay::actors::ActorExt;
use bombay::entity::{
    ActivationId, AdmissionFailure, DirectoryConfig, DrainFailure, EntityActivationError,
    EntityCapacity, EntityDefinition, EntityId, EntityMetrics, EntityShutdown,
};
use bombay::{
    ActorFailureAssessment, ActorRetirement, ActorRetirementReport, ApplicationBehavior,
    ApplicationCleanupError, ApplicationDefinitionError, ApplicationOutcome,
    ApplicationStagingError, ChildFailure, ChildOrigin, ProjectTerminal, RetirementAssessment,
    RootOrigin, TerminalProjection,
};
use bombay::{ActorSpace, ActorSpaces, App, Application, Hosts, MailAddr, actor};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum AccountAdmission {
    Ready,
    Reject,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum AccountRefusal {
    Initialization,
}

struct ReceivingAccount {
    admission: AccountAdmission,
    original: Arc<Vec<u8>>,
}

#[actor(message = Never, error = AccountRefusal)]
impl ReceivingAccount {
    fn init(&mut self) -> BehaviorActed<Self> {
        match self.admission {
            AccountAdmission::Ready => Ok(Actions::cont()),
            AccountAdmission::Reject => Err(AccountRefusal::Initialization),
        }
    }
}

#[derive(ActorSpaces)]
struct AccountSpaces {
    #[actor_space(ReceivingAccount)]
    accounts: ActorSpace<ReceivingAccount>,
}

enum AccountConclusion {
    Root {
        origin: RootOrigin<StopOnShutdown<ReceivingAccount>>,
        terminal: ActorRetirement<StopOnShutdown<ReceivingAccount>, Self, ()>,
    },
}

impl
    ProjectTerminal<
        RootOrigin<StopOnShutdown<ReceivingAccount>>,
        ActorRetirement<StopOnShutdown<ReceivingAccount>, Self, ()>,
    > for AccountConclusion
{
    fn project(
        origin: RootOrigin<StopOnShutdown<ReceivingAccount>>,
        terminal: ActorRetirement<StopOnShutdown<ReceivingAccount>, Self, ()>,
    ) -> Self {
        match terminal {
            completed @ ActorRetirement::Completed { .. } => {
                // This caller consumes only its own joined actor retirement.
                drop(completed);
                panic!("the application terminal conversion failed");
            }
            terminal => Self::Root { origin, terminal },
        }
    }
}

#[derive(Clone)]
struct AccountsRole(Arc<Vec<u8>>);

struct Accounts;

impl EntityDefinition for Accounts {
    type Id = u64;
    type Behavior = StopOnShutdown<ReceivingAccount>;
    type Hosts = AccountSpaces;
    type HydrationError = Never;
    type Terminal = Never;
    type ChildFailures = ();

    #[expect(
        clippy::unused_async_trait_impl,
        reason = "construct the hydration result when polled, preserving the owning async contract"
    )]
    async fn hydrate(&self, _: EntityId<u64>) -> Result<Self::Behavior, Never> {
        Ok(ReceivingAccount {
            admission: AccountAdmission::Ready,
            original: Arc::new(vec![77]),
        }
        .stop_on_shutdown())
    }

    fn activation_failed(
        &self,
        _: EntityId<u64>,
        _: ActivationId,
        failure: EntityActivationError<Never, Self::Behavior, Never, ()>,
    ) {
        drop(failure);
        panic!("no incarnation is requested by this startup controller");
    }

    fn admission_refused(&self, _: EntityId<u64>, failure: AdmissionFailure<Never>) {
        match failure {
            AdmissionFailure::Refused { command, .. }
            | AdmissionFailure::ActivationIdsExhausted(command)
            | AdmissionFailure::DispatchIdsExhausted(command) => match command {},
        }
    }

    fn forced_retirement(&self, _: &EntityId<u64>, _: ActivationId, _: DrainFailure) {
        panic!("no represented incarnation requires a fence");
    }

    fn retired(
        &self,
        _: &EntityId<u64>,
        _: ActivationId,
        retirement: Result<ActorRetirement<Self::Behavior, Never, ()>, JoinError>,
        retirement_report: ActorRetirementReport,
    ) {
        // This startup-only controller expects no incarnation receipt.
        drop((retirement, retirement_report));
        panic!("no incarnation is requested by this startup controller");
    }
}

fn assert_rejected_root(outcome: &AccountConclusion) {
    let AccountConclusion::Root { origin, terminal } = outcome;
    let ActorRetirement::InitializationRejected {
        behavior,
        error,
        control,
        user,
        descendants,
        child_failures: (),
        capability_failures,
        unread_owner_cancellation,
        additional_failures,
        received_interpretation,
        received_source,
        source_index,
        acquired_ingress,
        retirement_failures,
        terminal_report,
    } = terminal
    else {
        panic!("the complete actual initialization refusal remains owned");
    };
    assert!(matches!(
        (
            received_interpretation,
            received_source,
            source_index,
            acquired_ingress
        ),
        (None, None, None, None)
    ));
    assert!(additional_failures.is_empty());
    assert!(retirement_failures.is_empty());
    assert!(terminal_report.is_none());
    assert_eq!(origin.address(), MailAddr::APPLICATION_ROOT);
    assert_eq!(behavior.base().admission, AccountAdmission::Reject);
    assert_eq!(behavior.base().original.as_slice(), &[31, 37]);
    assert_eq!(*error, AccountRefusal::Initialization);
    assert_eq!(control.len(), 0);
    assert_eq!(user.len(), 0);
    assert_eq!(descendants.len(), 0);
    assert_eq!(capability_failures.len(), 0);
    assert!(unread_owner_cancellation.is_none());
}

#[tokio::test(flavor = "current_thread")]
#[expect(
    clippy::too_many_lines,
    reason = "one controller checks every native lane before consuming projection and then proves original output custody"
)]
async fn completed_output_survives_actual_root_terminal_conversion_failure() {
    let output = Arc::new(vec![11, 19]);
    let original_output = Arc::downgrade(&output);
    let state = Arc::new(vec![31, 37]);
    let original_state = Arc::downgrade(&state);
    let application = App::new(
        ReceivingAccount {
            admission: AccountAdmission::Ready,
            original: state,
        }
        .stop_on_shutdown(),
        AccountSpaces {
            accounts: ActorSpace::new(),
        },
    );
    let outcome = application
        .run_with::<AccountConclusion, _, _, _>(move |application| async move {
            let requested = application.lifecycle().request_shutdown();
            requested.expect("the actual live root accepts shutdown");
            output
        })
        .await
        .unwrap_or_else(|_| panic!("the caller's actual runtime is entered"));
    let (
        ApplicationOutcome::Completed {
            output,
            cleanup: Ok(()),
        },
        Ok((origin, joined)),
        Ok(ActorNotificationReceipts {
            termination: Ok(()),
            retirement: Ok(()),
        }),
    ) = outcome
    else {
        panic!("completed original output and both actual cleanup boundaries are acquired");
    };
    let retirement = joined;
    let ActorRetirement::Completed {
        behavior,
        settlements,
        control,
        user,
        descendants,
        child_failures: (),
        capability_failures,
        unread_owner_cancellation,
        completion,
        interpretation,
        source,
        additional_failures,
        received_interpretation,
        received_source,
        source_index,
        acquired_ingress,
        retirement_failures,
        terminal_report,
    } = &retirement
    else {
        panic!("same standard root completed normally");
    };
    assert!(matches!(
        (
            interpretation,
            source,
            received_interpretation,
            received_source,
            source_index,
            acquired_ingress
        ),
        (None, None, None, None, None, None)
    ));
    assert!(additional_failures.is_empty());
    assert!(retirement_failures.is_empty());
    assert!(terminal_report.is_none());
    assert_eq!(behavior.base().admission, AccountAdmission::Ready);
    assert_eq!(behavior.base().original.as_slice(), &[31, 37]);
    assert_eq!(settlements.len(), 1);
    assert_eq!(settlements[0].creations.len(), 0);
    assert_eq!(settlements[0].sends.owned, NoSends);
    assert_eq!(settlements[0].sends.inner, NoSends);
    assert!(matches!(settlements[0].become_, Step::Stop(_)));
    assert_eq!(control.len(), 0);
    assert_eq!(user.len(), 0);
    assert_eq!(descendants.len(), 0);
    assert_eq!(capability_failures.len(), 0);
    assert!(unread_owner_cancellation.is_none());
    assert!(matches!(completion, Completion::Stopped));
    let projected = catch_unwind(AssertUnwindSafe(|| {
        AccountConclusion::project(origin, retirement)
    }));
    let result = (output, projected);
    assert!(result.1.is_err());
    let retained = original_output.strong_count();
    let exact = original_output
        .upgrade()
        .is_some_and(|original| Arc::ptr_eq(&original, &result.0));
    let contents = result.0.as_slice().to_vec();
    let root_released = original_state.strong_count();
    drop(result);
    let output_released = original_output.strong_count();
    assert_eq!(retained, 1);
    assert!(exact);
    assert_eq!(contents, vec![11, 19]);
    assert_eq!(root_released, 0);
    assert_eq!(output_released, 0);
}

#[test]
#[expect(
    clippy::too_many_lines,
    reason = "one actual startup-refusal controller proves native, family shutdown and original allocation custody together"
)]
fn actual_family_shutdown_survives_pure_root_startup_refusal() {
    let role = Arc::new(vec![41, 43]);
    let original_role = Arc::downgrade(&role);
    let state = Arc::new(vec![31, 37]);
    let original_state = Arc::downgrade(&state);
    let capacity = EntityCapacity::new(
        NonZeroUsize::new(1).expect("one resident"),
        NonZeroUsize::new(1).expect("one hydration"),
    );
    let application = App::new(
        ReceivingAccount {
            admission: AccountAdmission::Reject,
            original: state,
        }
        .stop_on_shutdown(),
        AccountSpaces {
            accounts: ActorSpace::new(),
        },
    )
    .entity_family(
        AccountsRole(role),
        Accounts,
        DirectoryConfig::default(),
        capacity,
    )
    .unwrap_or_else(|_| panic!("the real directory configuration is valid"));
    let application_host = Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("the synchronous refusal controller owns one enabled application host");
    let (
        outcome,
        root_receiving,
        Ok(ActorNotificationReceipts {
            termination: Ok(()),
            retirement: Ok(()),
        }),
        shutdowns,
    ) = application_host
        .block_on(
            application.run_with_entities::<AccountConclusion, _, _, _>(|_| async {
                panic!("startup refusal leaves original work uninvoked");
            }),
        )
        .unwrap_or_else(|(application, work, error)| {
            drop((application, work));
            panic!("the explicit application host must be entered: {error}");
        })
    else {
        panic!("the joined startup refusal retains both successful notifications");
    };
    drop(application_host);
    let ApplicationOutcome::NotInvoked {
        work,
        startup_error: Some(startup_error),
        cleanup: Ok(()),
    } = outcome
    else {
        panic!("actual root startup refuses with its original uninvoked work and joined cleanup");
    };
    let (origin, retirement) =
        root_receiving.expect("the actual rejected root retirement is independently acquired");
    let retirement = match retirement {
        ActorRetirement::ActorTaskFailed(failure) => {
            panic!("ordinary initialization refusal retains its actor: {failure}")
        }
        retirement => retirement,
    };
    let outcome = AccountConclusion::project(origin, retirement);
    assert_rejected_root(&outcome);
    let (head_receiving, ()) = shutdowns;
    let (returned_role, (shutdown, metrics, family_disposal_failure)) =
        head_receiving.expect("the original zero-incarnation family retirement is acquired");
    assert!(family_disposal_failure.is_none());
    let EntityShutdown::Settled {
        represented,
        entities,
    } = shutdown
    else {
        panic!("the actual zero-incarnation family shutdown joins its complete owner");
    };
    let exact_role = original_role
        .upgrade()
        .is_some_and(|original| Arc::ptr_eq(&original, &returned_role.0));
    let role_contents = returned_role.0.as_slice().to_vec();
    let role_retained = original_role.strong_count();
    let state_retained = original_state.strong_count();
    let entities_len = entities.len();
    drop((work, startup_error, outcome, returned_role, entities));
    let role_released = original_role.strong_count();
    let state_released = original_state.strong_count();
    assert_eq!(represented, 0);
    assert_eq!(entities_len, 0);
    assert_eq!(
        metrics,
        EntityMetrics {
            activations: 0,
            hydration_failures: 0,
            launch_failures: 0,
            capacity_refusals: 0,
            forced_retirements: 0,
            peak_hydrations: 0,
            residents: 0
        }
    );
    assert!(exact_role);
    assert_eq!(role_contents, vec![41, 43]);
    assert_eq!(role_retained, 1);
    assert_eq!(state_retained, 1);
    assert_eq!(role_released, 0);
    assert_eq!(state_released, 0);
}

#[tokio::test(flavor = "current_thread")]
async fn application_startup_refusal_returns_whole_unprojected_root() {
    let original = Arc::new(vec![31, 37]);
    let retained = Arc::downgrade(&original);
    let outcome = Application::new(
        ReceivingAccount {
            admission: AccountAdmission::Reject,
            original,
        }
        .stop_on_shutdown(),
    )
    .run_with::<AccountConclusion, _, _, _, _, _>(|_| async {
        panic!("actual initialization refusal leaves this work uninvoked");
    })
    .await
    .unwrap_or_else(|_| panic!("the caller's actual runtime is entered"));
    let outcome = match outcome {
        (
            ApplicationOutcome::NotInvoked {
                work,
                startup_error: Some(startup_error),
                cleanup: Ok(()),
            },
            Ok((origin, retirement)),
            Ok(ActorNotificationReceipts {
                termination: Ok(()),
                retirement: Ok(()),
            }),
        ) => {
            drop((work, startup_error));
            AccountConclusion::project(origin, retirement)
        }
        outcome => {
            drop(outcome);
            panic!("the actual Application root refused initialization");
        }
    };
    assert_rejected_root(&outcome);
    let owned = retained.strong_count();
    drop(outcome);
    let released = retained.strong_count();
    assert_eq!(owned, 1);
    assert_eq!(released, 0);
}

#[expect(
    clippy::type_complexity,
    reason = "show the exact composed actor and child failure product without another wrapper"
)]
#[derive(TerminalProjection)]
enum DeclaredAccountConclusion {
    Root {
        origin: RootOrigin<StopOnShutdown<ReceivingAccount>>,
        terminal: ActorRetirement<
            ApplicationBehavior<
                StopOnShutdown<ReceivingAccount>,
                ChildCons<MailAddr, StopOnShutdown<ReceivingAccount>, NoChildren>,
            >,
            Self,
            (
                Vec<
                    ChildFailure<
                        ChildOrigin<StopOnShutdown<ReceivingAccount>, AccountsRole>,
                        StopOnShutdown<ReceivingAccount>,
                    >,
                >,
                (),
            ),
        >,
    },
    #[application_actor]
    Account {
        origin: ChildOrigin<StopOnShutdown<ReceivingAccount>, AccountsRole>,
        terminal: ActorRetirement<StopOnShutdown<ReceivingAccount>, Self, ()>,
    },
}

#[tokio::test(flavor = "current_thread")]
#[expect(
    clippy::too_many_lines,
    reason = "observe complete original root and declared unstarted child custody, full rejection lanes and final disposal in one controller"
)]
async fn declared_application_refusal_retains_unstarted_original_child() {
    let original_root = Arc::new(vec![31, 37]);
    let root_retained = Arc::downgrade(&original_root);
    let original_child = Arc::new(vec![53, 59]);
    let child_retained = Arc::downgrade(&original_child);
    let application = Application::new(
        ReceivingAccount {
            admission: AccountAdmission::Reject,
            original: original_root,
        }
        .stop_on_shutdown(),
    )
    .child(
        AccountsRole(Arc::new(vec![61, 67])),
        ReceivingAccount {
            admission: AccountAdmission::Ready,
            original: original_child,
        }
        .stop_on_shutdown(),
    );
    let outcome = application
        .run_with::<DeclaredAccountConclusion, _, _, _, _, _>(|_| async {
            panic!("the refused root never invokes application work");
        })
        .await
        .unwrap_or_else(|_| panic!("the caller's actual runtime is entered"));
    let (origin, retirement) = match outcome {
        (
            ApplicationOutcome::NotInvoked {
                work,
                startup_error: Some(startup_error),
                cleanup: Ok(()),
            },
            Ok((origin, retirement)),
            Ok(ActorNotificationReceipts {
                termination: Ok(()),
                retirement: Ok(()),
            }),
        ) => {
            drop((work, startup_error));
            (origin, retirement)
        }
        other => {
            drop(other);
            panic!("declared application initialization refuses before publication");
        }
    };
    let returned = DeclaredAccountConclusion::project(origin, retirement);
    let returned = match returned {
        DeclaredAccountConclusion::Root { origin, terminal } => (origin, terminal),
        DeclaredAccountConclusion::Account { origin, terminal } => {
            drop((origin, terminal));
            panic!("refusal returns the root, not a child terminal");
        }
    };
    let (
        origin,
        ActorRetirement::InitializationRejected {
            behavior,
            error,
            control,
            user,
            descendants,
            child_failures: (child_failures, ()),
            capability_failures,
            unread_owner_cancellation,
            additional_failures,
            received_interpretation,
            received_source,
            source_index,
            acquired_ingress,
            retirement_failures,
            terminal_report,
        },
    ) = &returned
    else {
        panic!("the actual composed actor retains complete initialization rejection");
    };
    assert!(matches!(
        (
            received_interpretation,
            received_source,
            source_index,
            acquired_ingress
        ),
        (None, None, None, None)
    ));
    assert!(additional_failures.is_empty());
    assert!(retirement_failures.is_empty());
    assert!(terminal_report.is_none());
    assert_eq!(origin.address(), MailAddr::APPLICATION_ROOT);
    assert_eq!(behavior.base().admission, AccountAdmission::Reject);
    assert_eq!(behavior.base().original.as_slice(), &[31, 37]);
    let refusal = match error {
        ApplicationDefinitionError::Root(reason) => ApplicationDefinitionError::Root(*reason),
        ApplicationDefinitionError::InitializedTwice => {
            ApplicationDefinitionError::InitializedTwice
        }
    };
    assert_eq!(control.len(), 0);
    assert_eq!(user.len(), 0);
    assert_eq!(descendants.len(), 0);
    assert_eq!(child_failures.len(), 0);
    assert_eq!(capability_failures.len(), 0);
    assert!(unread_owner_cancellation.is_none());
    let root_owned = root_retained.strong_count();
    let child_owned = child_retained.strong_count();
    let child_contents = child_retained
        .upgrade()
        .map(|child| child.as_slice().to_vec());
    drop(returned);
    let root_released = root_retained.strong_count();
    let child_released = child_retained.strong_count();
    assert_eq!(root_owned, 1);
    assert_eq!(child_owned, 1);
    assert_eq!(child_contents, Some(vec![53, 59]));
    assert_eq!(root_released, 0);
    assert_eq!(child_released, 0);
    match refusal {
        ApplicationDefinitionError::Root(AccountRefusal::Initialization) => {}
        ApplicationDefinitionError::InitializedTwice => {
            panic!("this original composed application initializes exactly once");
        }
    }
}

fn paired_account(original: Arc<Vec<u8>>) -> App<StopOnShutdown<ReceivingAccount>, AccountSpaces> {
    App::new(
        ReceivingAccount {
            admission: AccountAdmission::Ready,
            original,
        }
        .stop_on_shutdown(),
        AccountSpaces {
            accounts: ActorSpace::new(),
        },
    )
}

#[expect(
    clippy::type_complexity,
    reason = "the oracle reads both independent real join boundaries and the complete concrete actor retirement"
)]
fn assert_completed_account(
    root_receipt: &Result<
        (
            RootOrigin<StopOnShutdown<ReceivingAccount>>,
            ActorRetirement<StopOnShutdown<ReceivingAccount>, AccountConclusion, ()>,
        ),
        RecvError,
    >,
) {
    let Ok((
        origin,
        ActorRetirement::Completed {
            behavior,
            settlements,
            control,
            user,
            descendants,
            child_failures: (),
            capability_failures,
            unread_owner_cancellation,
            completion,
            interpretation,
            source,
            additional_failures,
            received_interpretation,
            received_source,
            source_index,
            acquired_ingress,
            retirement_failures,
            terminal_report,
        },
    )) = root_receipt
    else {
        panic!("the standard account and root_receipt join normally");
    };
    assert!(matches!(
        (
            interpretation,
            source,
            received_interpretation,
            received_source,
            source_index,
            acquired_ingress
        ),
        (None, None, None, None, None, None)
    ));
    assert!(additional_failures.is_empty());
    assert!(retirement_failures.is_empty());
    assert!(terminal_report.is_none());
    assert_eq!(origin.address(), MailAddr::APPLICATION_ROOT);
    assert_eq!(behavior.base().admission, AccountAdmission::Ready);
    assert_eq!(behavior.base().original.as_slice(), &[31, 37]);
    assert_eq!(settlements.len(), 1);
    assert_eq!(settlements[0].creations.len(), 0);
    assert_eq!(settlements[0].sends.owned, NoSends);
    assert_eq!(settlements[0].sends.inner, NoSends);
    assert!(matches!(settlements[0].become_, Step::Stop(_)));
    assert_eq!(control.len(), 0);
    assert_eq!(user.len(), 0);
    assert_eq!(descendants.len(), 0);
    assert_eq!(capability_failures.len(), 0);
    assert!(unread_owner_cancellation.is_none());
    assert!(matches!(completion, Completion::Stopped));
}

#[expect(
    clippy::type_complexity,
    reason = "the oracle reads both independent real join boundaries and the complete concrete actor retirement"
)]
fn assert_cancelled_account(
    root_receipt: &Result<
        (
            RootOrigin<StopOnShutdown<ReceivingAccount>>,
            ActorRetirement<StopOnShutdown<ReceivingAccount>, AccountConclusion, ()>,
        ),
        RecvError,
    >,
) {
    let Ok((
        origin,
        ActorRetirement::OwnerCancelled {
            behavior,
            settlements,
            control,
            user,
            descendants,
            child_failures: (),
            capability_failures,
            unread_owner_cancellation,
            interpretation,
            source,
            additional_failures,
            received_interpretation,
            received_source,
            source_index,
            acquired_ingress,
            retirement_failures,
            terminal_report,
        },
    )) = root_receipt
    else {
        panic!("the original authority cancels the still-live account");
    };
    assert!(matches!(
        (
            interpretation,
            source,
            received_interpretation,
            received_source,
            source_index,
            acquired_ingress
        ),
        (None, None, None, None, None, None)
    ));
    assert!(additional_failures.is_empty());
    assert!(retirement_failures.is_empty());
    assert!(terminal_report.is_none());
    assert_eq!(origin.address(), MailAddr::APPLICATION_ROOT);
    assert_eq!(behavior.base().admission, AccountAdmission::Ready);
    assert_eq!(behavior.base().original.as_slice(), &[31, 37]);
    assert_eq!(settlements.len(), 0);
    assert_eq!(control.len(), 0);
    assert_eq!(user.len(), 0);
    assert_eq!(descendants.len(), 0);
    assert_eq!(capability_failures.len(), 0);
    assert!(unread_owner_cancellation.is_none());
}

#[test]
fn borrowed_local_work_returns_non_send_output_after_root_join() {
    let runtime = Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("the actual runtime builds");
    let borrowed = vec![71, 73];
    let output = Rc::new(vec![79, 83]);
    let original = Rc::downgrade(&output);
    runtime.block_on(async {
        let borrowed = &borrowed;
        let application = paired_account(Arc::new(vec![31, 37]));
        let pair = application.execute_with::<AccountConclusion, (), _, _>(
            move |application| async move {
                let requested = application.lifecycle().request_shutdown();
                requested.expect("the live root accepts shutdown");
                (output, borrowed)
            },
        );
        let (execution, result) =
            pair.unwrap_or_else(|_| panic!("the original entered executor is available"));
        let ((), result) = tokio::join!(execution, result);
        let (
            ApplicationOutcome::Completed {
                output,
                cleanup: Ok(()),
            },
            root_receipt,
            Ok(ActorNotificationReceipts {
                termination: Ok(()),
                retirement: Ok(()),
            }),
        ) = result
        else {
            panic!("the original caller-local work completes");
        };
        assert_completed_account(&root_receipt);
        let exact = original
            .upgrade()
            .is_some_and(|original| Rc::ptr_eq(&original, &output.0));
        let contents = output.0.as_slice().to_vec();
        let borrowed_contents = output.1.as_slice().to_vec();
        let retained = original.strong_count();
        drop((output, root_receipt));
        let released = original.strong_count();
        assert!(exact);
        assert_eq!(contents, vec![79, 83]);
        assert_eq!(borrowed_contents, vec![71, 73]);
        assert_eq!(retained, 1);
        assert_eq!(released, 0);
    });
}

#[test]
fn unpolled_execution_returns_original_application_and_callable_for_real_retry() {
    let runtime = Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("the actual runtime builds");
    let original = Rc::new(vec![89, 97]);
    let retained = Rc::downgrade(&original);
    let state = Arc::new(vec![31, 37]);
    let state_retained = Arc::downgrade(&state);
    runtime.block_on(async {
        let application = paired_account(state);
        let (execution, result) = application
            .execute_with::<AccountConclusion, (), _, _>(move |application| async move {
                let requested = application.lifecycle().request_shutdown();
                requested.expect("the retried live root accepts shutdown");
                original
            })
            .unwrap_or_else(|_| panic!("the original entered executor is available"));
        drop(execution);
        let (ApplicationOutcome::Unstarted { application, work }, Err(_), Err(_)) = result.await
        else {
            panic!("before-first-poll drop returns the whole original inputs");
        };
        let owned_before_retry = retained.strong_count();
        let (execution, result) = application
            .execute_with::<AccountConclusion, (), _, _>(work)
            .unwrap_or_else(|_| panic!("the exact original inputs retry in the live executor"));
        let ((), result) = tokio::join!(execution, result);
        let (
            ApplicationOutcome::Completed {
                output,
                cleanup: Ok(()),
            },
            root_receipt,
            Ok(ActorNotificationReceipts {
                termination: Ok(()),
                retirement: Ok(()),
            }),
        ) = result
        else {
            panic!("the recovered original callable runs on the recovered original application");
        };
        assert_completed_account(&root_receipt);
        let Ok((_, ActorRetirement::Completed { behavior, .. })) = &root_receipt else {
            panic!("the recovered application owns its original root");
        };
        let exact_root = state_retained
            .upgrade()
            .is_some_and(|original| Arc::ptr_eq(&original, &behavior.base().original));
        let exact = retained
            .upgrade()
            .is_some_and(|original| Rc::ptr_eq(&original, &output));
        let contents = output.as_slice().to_vec();
        drop((output, root_receipt));
        let released = retained.strong_count();
        let state_released = state_retained.strong_count();
        assert_eq!(owned_before_retry, 1);
        assert!(exact);
        assert!(exact_root);
        assert_eq!(contents, vec![89, 97]);
        assert_eq!(released, 0);
        assert_eq!(state_released, 0);
    });
}

#[test]
fn startup_pending_drop_preserves_uninvoked_callable_and_exact_old_root() {
    let runtime = Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("the actual runtime builds");
    let callable = Rc::new(vec![101, 103]);
    let retained = Rc::downgrade(&callable);
    runtime.block_on(async {
        let application = paired_account(Arc::new(vec![31, 37]));
        let (execution, result) = application
            .execute_with::<AccountConclusion, (), _, _>(move |application| async move {
                let requested = application.lifecycle().request_shutdown();
                requested.expect("the new live root accepts shutdown");
                callable
            })
            .unwrap_or_else(|_| panic!("the original entered executor is available"));
        let mut execution = Box::pin(execution);
        let first_poll = poll_fn(|context| Poll::Ready(execution.as_mut().poll(context))).await;
        drop(execution);
        let (
            ApplicationOutcome::NotInvoked {
                work,
                startup_error,
                cleanup: Ok(()),
            },
            root_receipt,
            Ok(ActorNotificationReceipts {
                termination: Ok(()),
                retirement: Ok(()),
            }),
        ) = result.await
        else {
            panic!("the current-thread first poll starts the task but cannot yet acquire startup");
        };
        assert_cancelled_account(&root_receipt);
        let callable_retained = retained.strong_count();
        drop(root_receipt);
        let (execution, result) = paired_account(Arc::new(vec![31, 37]))
            .execute_with::<AccountConclusion, (), _, _>(work)
            .unwrap_or_else(|_| panic!("the original callable runs against a genuine new handle"));
        let ((), result) = tokio::join!(execution, result);
        let (
            ApplicationOutcome::Completed {
                output,
                cleanup: Ok(()),
            },
            root_receipt,
            Ok(ActorNotificationReceipts {
                termination: Ok(()),
                retirement: Ok(()),
            }),
        ) = result
        else {
            panic!("the recovered original callable completes on the new actor");
        };
        assert_completed_account(&root_receipt);
        let exact = retained
            .upgrade()
            .is_some_and(|original| Rc::ptr_eq(&original, &output));
        let contents = output.as_slice().to_vec();
        drop((output, root_receipt));
        let released = retained.strong_count();
        assert!(first_poll.is_pending());
        assert!(startup_error.is_none());
        assert_eq!(callable_retained, 1);
        assert!(exact);
        assert_eq!(contents, vec![101, 103]);
        assert_eq!(released, 0);
    });
}

#[test]
fn owning_execution_drop_releases_pending_local_work_before_join() {
    let runtime = Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("the actual runtime builds");
    let work_value = Rc::new(vec![107, 109]);
    let retained = Rc::downgrade(&work_value);
    runtime.block_on(async {
        let (ready, ready_result) = oneshot::channel();
        let (continuation, continued) = oneshot::channel();
        let (execution, result) = paired_account(Arc::new(vec![31, 37]))
            .execute_with::<AccountConclusion, (), _, _>(move |_| async move {
                ready.send(()).expect("the caller owns the ready receiver");
                let completed = continued.await;
                match completed {
                    Ok(()) | Err(_) => {}
                }
                work_value
            })
            .unwrap_or_else(|_| panic!("the original entered executor is available"));
        let mut execution = Box::pin(execution);
        let ready = tokio::select! {
            ready = ready_result => ready,
            () = &mut execution => panic!("the local work remains gated"),
        };
        drop(execution);
        let work_released_before_join = retained.strong_count();
        match continuation.send(()) {
            Ok(()) | Err(()) => {}
        }
        let outcome = result.await;
        let (
            ApplicationOutcome::Interrupted { cleanup: Ok(()) },
            root_receipt,
            Ok(ActorNotificationReceipts {
                termination: Ok(()),
                retirement: Ok(()),
            }),
        ) = outcome
        else {
            panic!("invoked unfinished work is surrendered, not reconstructed");
        };
        assert_cancelled_account(&root_receipt);
        drop(root_receipt);
        ready.expect("the original work really started before owning drop");
        assert_eq!(work_released_before_join, 0);
    });
}

#[test]
fn result_surrender_releases_completed_output_without_cancelling_live_execution() {
    let runtime = Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("the actual runtime builds");
    let output = Rc::new(vec![113, 127]);
    let retained = Rc::downgrade(&output);
    runtime.block_on(async {
        let (ready, ready_result) = oneshot::channel();
        let (execution, result) = paired_account(Arc::new(vec![31, 37]))
            .execute_with::<AccountConclusion, (), _, _>(move |application| async move {
                ready
                    .send(application.lifecycle())
                    .expect("the caller owns the live-root receiver");
                output
            })
            .unwrap_or_else(|_| panic!("the original entered executor is available"));
        let mut execution = Box::pin(execution);
        let lifecycle = tokio::select! {
            ready = ready_result => ready.expect("the original work acquired the real handle"),
            () = &mut execution => panic!("the live root has not completed"),
        };
        let held_output = retained.strong_count();
        let before_surrender = {
            let mut observed = pin!(lifecycle.termination());
            poll_fn(|context| Poll::Ready(observed.as_mut().poll(context))).await
        };
        drop(result);
        let released_at_surrender = retained.strong_count();
        let requested = lifecycle.request_shutdown();
        execution.await;
        requested.expect("the surviving execution keeps the root live until this explicit request");
        assert!(before_surrender.is_pending());
        assert_eq!(held_output, 1);
        assert_eq!(released_at_surrender, 0);
    });
}

#[test]
fn completed_root_does_not_surrender_pending_application_work() {
    let runtime = Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("the actual runtime builds");
    let output = Rc::new(vec![131, 137]);
    let retained = Rc::downgrade(&output);
    runtime.block_on(async {
        let (ready, ready_result) = oneshot::channel();
        let (continuation, continued) = oneshot::channel();
        let (execution, result) = paired_account(Arc::new(vec![31, 37]))
            .execute_with::<AccountConclusion, (), _, _>(move |application| async move {
                let lifecycle = application.lifecycle();
                let requested = lifecycle.request_shutdown();
                ready
                    .send((lifecycle, requested))
                    .expect("the caller owns the real shutdown request observation");
                let continued = continued.await;
                continued.expect("the host releases this original work gate");
                output
            })
            .unwrap_or_else(|_| panic!("the original entered executor is available"));
        let mut execution = Box::pin(execution);
        let (lifecycle, requested) = tokio::select! {
            ready = ready_result => ready.expect("the original work acquired the real handle"),
            () = &mut execution => panic!("the original work remains pending"),
        };
        let stopped = lifecycle.termination().await;
        let held_after_root_stop = retained.strong_count();
        let released = continuation.send(());
        let ((), outcome) = tokio::join!(&mut execution, result);
        let (
            ApplicationOutcome::Completed {
                output,
                cleanup: Ok(()),
            },
            root_receipt,
            Ok(ActorNotificationReceipts {
                termination: Ok(()),
                retirement: Ok(()),
            }),
        ) = outcome
        else {
            panic!("the original work survives root completion and then completes");
        };
        assert_completed_account(&root_receipt);
        let exact = retained
            .upgrade()
            .is_some_and(|original| Rc::ptr_eq(&original, &output));
        let contents = output.as_slice().to_vec();
        drop((output, root_receipt));
        let surrendered = retained.strong_count();
        requested.expect("the original work admitted its typed shutdown request");
        released.expect("root completion did not destroy the original gated work");
        assert_eq!(stopped, Ok(Exit::Normal));
        assert_eq!(held_after_root_stop, 1);
        assert!(exact);
        assert_eq!(contents, vec![131, 137]);
        assert_eq!(surrendered, 0);
    });
}

#[test]
fn missing_executor_returns_original_application_and_callable() {
    let output = Rc::new(vec![139, 149]);
    let retained = Rc::downgrade(&output);
    let original_state = Arc::new(vec![31, 37]);
    let state_retained = Arc::downgrade(&original_state);
    let application = paired_account(original_state);
    let attempted =
        application.execute_with::<AccountConclusion, (), _, _>(move |application| async move {
            let requested = application.lifecycle().request_shutdown();
            requested.expect("the actual retried root accepts shutdown");
            output
        });
    let (application, work, context_error) = match attempted {
        Err(original) => original,
        Ok(pair) => {
            drop(pair);
            panic!("this controller has not entered a Tokio executor");
        }
    };
    let original_inputs_owned = retained.strong_count();
    let original_state_owned = state_retained.strong_count();
    let runtime = Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("the actual retry runtime builds");
    runtime.block_on(async {
        let (execution, result) = application
            .execute_with::<AccountConclusion, (), _, _>(work)
            .unwrap_or_else(|_| {
                panic!("the exact original input product retries in the entered executor")
            });
        let ((), outcome) = tokio::join!(execution, result);
        let (
            ApplicationOutcome::Completed {
                output,
                cleanup: Ok(()),
            },
            root_receipt,
            Ok(ActorNotificationReceipts {
                termination: Ok(()),
                retirement: Ok(()),
            }),
        ) = outcome
        else {
            panic!("the recovered original callable completes");
        };
        assert_completed_account(&root_receipt);
        let exact = retained
            .upgrade()
            .is_some_and(|original| Rc::ptr_eq(&original, &output));
        let contents = output.as_slice().to_vec();
        drop((output, root_receipt));
        let output_released = retained.strong_count();
        let state_released = state_retained.strong_count();
        assert!(context_error.is_missing_context());
        assert_eq!(original_inputs_owned, 1);
        assert_eq!(original_state_owned, 1);
        assert!(exact);
        assert_eq!(contents, vec![139, 149]);
        assert_eq!(output_released, 0);
        assert_eq!(state_released, 0);
    });
}

#[test]
#[expect(
    clippy::too_many_lines,
    reason = "observe full startup refusal, original callable/native receiving error and real retry with complete joined root custody"
)]
fn startup_refusal_returns_exact_uninvoked_callable_and_full_rejected_root() {
    let runtime = Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("the actual runtime builds");
    let callable = Rc::new(vec![151, 157]);
    let retained = Rc::downgrade(&callable);
    let root = Arc::new(vec![31, 37]);
    let root_retained = Arc::downgrade(&root);
    runtime.block_on(async {
        let application = App::new(
            ReceivingAccount {
                admission: AccountAdmission::Reject,
                original: root,
            }
            .stop_on_shutdown(),
            AccountSpaces {
                accounts: ActorSpace::new(),
            },
        );
        let (execution, result) = application
            .execute_with::<AccountConclusion, (), _, _>(move |application| async move {
                let requested = application.lifecycle().request_shutdown();
                requested.expect("the real retry root accepts shutdown");
                callable
            })
            .unwrap_or_else(|_| panic!("the entered executor is available"));
        let ((), outcome) = tokio::join!(execution, result);
        let (
            ApplicationOutcome::NotInvoked {
                work,
                startup_error,
                cleanup: Ok(()),
            },
            root_receipt,
            Ok(ActorNotificationReceipts {
                termination: Ok(()),
                retirement: Ok(()),
            }),
        ) = outcome
        else {
            panic!("actual initialization refusal preserves the uninvoked callable");
        };
        let Ok((
            origin,
            ActorRetirement::InitializationRejected {
                behavior,
                error,
                control,
                user,
                descendants,
                child_failures: (),
                capability_failures,
                unread_owner_cancellation,
                additional_failures,
                received_interpretation,
                received_source,
                source_index,
                acquired_ingress,
                retirement_failures,
                terminal_report,
            },
        )) = &root_receipt
        else {
            panic!("the complete original initialization refusal remains available");
        };
        assert!(matches!(
            (
                received_interpretation,
                received_source,
                source_index,
                acquired_ingress
            ),
            (None, None, None, None)
        ));
        assert!(additional_failures.is_empty());
        assert!(retirement_failures.is_empty());
        assert!(terminal_report.is_none());
        let exact_root = root_retained
            .upgrade()
            .is_some_and(|original| Arc::ptr_eq(&original, &behavior.base().original));
        assert_eq!(origin.address(), MailAddr::APPLICATION_ROOT);
        assert_eq!(behavior.base().admission, AccountAdmission::Reject);
        assert_eq!(behavior.base().original.as_slice(), &[31, 37]);
        assert_eq!(*error, AccountRefusal::Initialization);
        assert_eq!(control.len(), 0);
        assert_eq!(user.len(), 0);
        assert_eq!(descendants.len(), 0);
        assert_eq!(capability_failures.len(), 0);
        assert!(unread_owner_cancellation.is_none());
        let callable_owned = retained.strong_count();
        drop(root_receipt);
        let root_released = root_retained.strong_count();
        let (execution, result) = paired_account(Arc::new(vec![31, 37]))
            .execute_with::<AccountConclusion, (), _, _>(work)
            .unwrap_or_else(|_| {
                panic!("the exact recovered callable retries on an actual new root")
            });
        let ((), outcome) = tokio::join!(execution, result);
        let (
            ApplicationOutcome::Completed {
                output,
                cleanup: Ok(()),
            },
            root_receipt,
            Ok(ActorNotificationReceipts {
                termination: Ok(()),
                retirement: Ok(()),
            }),
        ) = outcome
        else {
            panic!("the original callable completes on its real retry");
        };
        assert_completed_account(&root_receipt);
        let exact_callable = retained
            .upgrade()
            .is_some_and(|original| Rc::ptr_eq(&original, &output));
        let contents = output.as_slice().to_vec();
        drop((output, root_receipt));
        let callable_released = retained.strong_count();
        assert!(startup_error.is_some());
        assert!(exact_root);
        assert_eq!(callable_owned, 1);
        assert_eq!(root_released, 0);
        assert!(exact_callable);
        assert_eq!(contents, vec![151, 157]);
        assert_eq!(callable_released, 0);
    });
}

#[test]
fn cancelled_borrowed_result_wait_preserves_same_receiver_and_completed_output() {
    let runtime = Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("the actual runtime builds");
    let output = Rc::new(vec![163, 167]);
    let retained = Rc::downgrade(&output);
    runtime.block_on(async {
        let (ready, ready_result) = oneshot::channel();
        let (execution, result) = paired_account(Arc::new(vec![31, 37]))
            .execute_with::<AccountConclusion, (), _, _>(move |application| async move {
                ready
                    .send(application.lifecycle())
                    .expect("the caller owns the live-root receiver");
                output
            })
            .unwrap_or_else(|_| panic!("the entered executor is available"));
        let mut execution = Box::pin(execution);
        let lifecycle = tokio::select! {
            ready = ready_result => ready.expect("work acquired its actual application handle"),
            () = &mut execution => panic!("the original root remains live"),
        };
        let mut result = Box::pin(result);
        let waiting = {
            let mut borrowed_wait = pin!(result.as_mut());
            poll_fn(|context| Poll::Ready(borrowed_wait.as_mut().poll(context))).await
        };
        let output_owned = retained.strong_count();
        let requested = lifecycle.request_shutdown();
        let ((), outcome) = tokio::join!(&mut execution, result.as_mut());
        let (
            ApplicationOutcome::Completed {
                output,
                cleanup: Ok(()),
            },
            root_receipt,
            Ok(ActorNotificationReceipts {
                termination: Ok(()),
                retirement: Ok(()),
            }),
        ) = outcome
        else {
            panic!("the same original receiver retains completed caller-local output");
        };
        assert_completed_account(&root_receipt);
        let exact = retained
            .upgrade()
            .is_some_and(|original| Rc::ptr_eq(&original, &output));
        let contents = output.as_slice().to_vec();
        drop((output, root_receipt));
        let released = retained.strong_count();
        requested.expect("the live root accepts the actual root_receipt request");
        assert!(waiting.is_pending());
        assert_eq!(output_owned, 1);
        assert!(exact);
        assert_eq!(contents, vec![163, 167]);
        assert_eq!(released, 0);
    });
}

struct ReadyAccountWork {
    output: Option<Rc<Vec<u8>>>,
    panic_payload: Option<Arc<Vec<u8>>>,
}

impl Future for ReadyAccountWork {
    type Output = Rc<Vec<u8>>;

    fn poll(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Self::Output> {
        let output = self
            .get_mut()
            .output
            .take()
            .expect("the original work returns Ready exactly once");
        Poll::Ready(output)
    }
}

impl Drop for ReadyAccountWork {
    fn drop(&mut self) {
        if let Some(payload) = self.panic_payload.take() {
            panic_any(payload);
        }
    }
}

#[test]
fn ready_output_survives_work_destructor_panic_and_actual_root_join() {
    let runtime = Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("the actual runtime builds");
    let output = Rc::new(vec![173, 179]);
    let original_output = Rc::downgrade(&output);
    let payload = Arc::new(vec![181, 191]);
    let original_payload = Arc::downgrade(&payload);
    runtime.block_on(async {
        let (execution, result) = paired_account(Arc::new(vec![31, 37]))
            .execute_with::<AccountConclusion, (), _, _>(move |_| ReadyAccountWork {
                output: Some(output),
                panic_payload: Some(payload),
            })
            .unwrap_or_else(|_| panic!("the entered executor is available"));
        let mut execution = Box::pin(execution);
        let execution_fault = poll_fn(|context| {
            match catch_unwind(AssertUnwindSafe(|| execution.as_mut().poll(context))) {
                Ok(Poll::Pending) => Poll::Pending,
                Ok(Poll::Ready(())) => Poll::Ready(Ok(())),
                Err(payload) => Poll::Ready(Err(payload)),
            }
        })
        .await;
        drop(execution);
        let result = result.await;
        let (
            ApplicationOutcome::Completed {
                output,
                cleanup: Ok(()),
            },
            root_receipt,
            Ok(ActorNotificationReceipts {
                termination: Ok(()),
                retirement: Ok(()),
            }),
        ) = result
        else {
            panic!(
                "the already acquired Ready output survives W disposal and real actor root_receipt"
            );
        };
        assert_cancelled_account(&root_receipt);
        let Err(payload) = execution_fault else {
            panic!("the caller receives the original work destructor fault independently");
        };
        let exact_output = original_output
            .upgrade()
            .is_some_and(|original| Rc::ptr_eq(&original, &output));
        let contents = output.as_slice().to_vec();
        let output_owned = original_output.strong_count();
        let cause_owned = original_payload.strong_count();
        drop((output, root_receipt));
        let output_released = original_output.strong_count();
        let cause_still_owned = original_payload.strong_count();
        drop(payload);
        let cause_released = original_payload.strong_count();
        assert!(exact_output);
        assert_eq!(contents, vec![173, 179]);
        assert_eq!(output_owned, 1);
        assert_eq!(cause_owned, 1);
        assert_eq!(output_released, 0);
        assert_eq!(cause_still_owned, 1);
        assert_eq!(cause_released, 0);
    });
}

struct RetiringAccounts {
    #[expect(
        clippy::redundant_allocation,
        reason = "Preserve native panic object identity separately from its shared payload lifetime."
    )]
    panic_payload: Option<Box<Arc<Vec<u8>>>>,
}

impl Drop for RetiringAccounts {
    fn drop(&mut self) {
        if let Some(original) = self.panic_payload.take() {
            resume_unwind(original);
        }
    }
}

impl EntityDefinition for RetiringAccounts {
    type Id = u64;
    type Behavior = StopOnShutdown<ReceivingAccount>;
    type Hosts = AccountSpaces;
    type HydrationError = Never;
    type Terminal = Never;
    type ChildFailures = ();

    fn hydrate(
        &self,
        id: EntityId<u64>,
    ) -> impl Future<Output = Result<Self::Behavior, Never>> + Send {
        Accounts.hydrate(id)
    }

    fn activation_failed(
        &self,
        id: EntityId<u64>,
        activation: ActivationId,
        failure: EntityActivationError<Never, Self::Behavior, Never, ()>,
    ) {
        Accounts.activation_failed(id, activation, failure);
    }

    fn admission_refused(&self, id: EntityId<u64>, failure: AdmissionFailure<Never>) {
        Accounts.admission_refused(id, failure);
    }

    fn forced_retirement(
        &self,
        id: &EntityId<u64>,
        activation: ActivationId,
        failure: DrainFailure,
    ) {
        Accounts.forced_retirement(id, activation, failure);
    }

    fn retired(
        &self,
        id: &EntityId<u64>,
        activation: ActivationId,
        retirement: Result<ActorRetirement<Self::Behavior, Never, ()>, JoinError>,
        retirement_report: ActorRetirementReport,
    ) {
        Accounts.retired(id, activation, retirement, retirement_report);
    }
}

#[expect(
    clippy::too_many_lines,
    reason = "the original three-family disposal oracle keeps the complete acquired root, output, named family products and final discharge cuts in one trace"
)]
fn family_report_disposal_preserves_acquired_products(panic_payload: Option<Arc<Vec<u8>>>) {
    let panic_payload = panic_payload.map(Box::new);
    let original_fault = panic_payload
        .as_ref()
        .map(|payload| Arc::downgrade(payload.as_ref()));
    let original_carrier = panic_payload
        .as_ref()
        .map(|payload| ptr::from_ref(payload.as_ref()).cast::<()>());
    let output = Arc::new(vec![11, 19]);
    let original_output = Arc::downgrade(&output);
    let state = Arc::new(vec![31, 37]);
    let original_state = Arc::downgrade(&state);
    let head = Arc::new(vec![41, 43]);
    let original_head = Arc::downgrade(&head);
    let middle = Arc::new(vec![47, 53]);
    let original_middle = Arc::downgrade(&middle);
    let tail = Arc::new(vec![59, 61]);
    let original_tail = Arc::downgrade(&tail);
    let capacity = EntityCapacity::new(
        NonZeroUsize::new(1).expect("one resident"),
        NonZeroUsize::new(1).expect("one hydration"),
    );
    // entity_family prepends: declare tail, then faulting middle, then head.
    let application = App::new(
        ReceivingAccount {
            admission: AccountAdmission::Ready,
            original: state,
        }
        .stop_on_shutdown(),
        AccountSpaces {
            accounts: ActorSpace::new(),
        },
    )
    .entity_family(
        AccountsRole(tail),
        Accounts,
        DirectoryConfig::default(),
        capacity,
    )
    .unwrap_or_else(|_| panic!("the original tail directory is valid"))
    .entity_family(
        AccountsRole(middle),
        RetiringAccounts { panic_payload },
        DirectoryConfig::default(),
        capacity,
    )
    .unwrap_or_else(|_| panic!("the original middle directory is valid"))
    .entity_family(
        AccountsRole(head),
        Accounts,
        DirectoryConfig::default(),
        capacity,
    )
    .unwrap_or_else(|_| panic!("the original head directory is valid"));
    let mut returned = None;
    let caught = catch_unwind(AssertUnwindSafe(|| {
        let application_host = Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("the synchronous disposal controller owns one enabled application host");
        let products = application_host
            .block_on(application.run_with_entities::<AccountConclusion, _, _, _>(
                move |application| async move {
                    let requested = application.lifecycle().request_shutdown();
                    requested.expect("the actual live root accepts shutdown");
                    output
                },
            ))
            .unwrap_or_else(|(application, work, error)| {
                drop((application, work));
                panic!("the explicit application host must be entered: {error}");
            });
        drop(application_host);
        returned = Some(products);
    }));
    // The explicit owned host has joined and been disposed before every oracle.
    // Its returned products still own any caught installed-family disposal payload.
    let retained = (
        original_output.strong_count(),
        original_state.strong_count(),
        original_head.strong_count(),
        original_middle.strong_count(),
        original_tail.strong_count(),
    );
    let acquired_products = match returned.as_ref() {
        Some((
            ApplicationOutcome::Completed {
                output,
                cleanup: Ok(()),
            },
            Ok((
                origin,
                ActorRetirement::Completed {
                    behavior,
                    settlements,
                    control,
                    user,
                    descendants,
                    child_failures: (),
                    capability_failures,
                    unread_owner_cancellation,
                    completion,
                    interpretation,
                    source,
                    additional_failures,
                    received_interpretation,
                    received_source,
                    source_index,
                    acquired_ingress,
                    retirement_failures,
                    terminal_report,
                },
            )),
            Ok(ActorNotificationReceipts {
                termination: Ok(()),
                retirement: Ok(()),
            }),
            (
                Ok((
                    head,
                    (
                        EntityShutdown::Settled {
                            represented: head_represented,
                            entities: head_entities,
                        },
                        head_metrics,
                        head_family_disposal_failure,
                    ),
                )),
                (
                    Ok((
                        middle,
                        (
                            EntityShutdown::Settled {
                                represented: middle_represented,
                                entities: middle_entities,
                            },
                            middle_metrics,
                            middle_family_disposal_failure,
                        ),
                    )),
                    (
                        Ok((
                            tail,
                            (
                                EntityShutdown::Settled {
                                    represented: tail_represented,
                                    entities: tail_entities,
                                },
                                tail_metrics,
                                tail_family_disposal_failure,
                            ),
                        )),
                        (),
                    ),
                ),
            ),
        )) => {
            assert!(matches!(
                (
                    interpretation,
                    source,
                    received_interpretation,
                    received_source,
                    source_index,
                    acquired_ingress
                ),
                (None, None, None, None, None, None)
            ));
            assert!(additional_failures.is_empty());
            assert!(retirement_failures.is_empty());
            assert!(terminal_report.is_none());
            let output_exact = original_output
                .upgrade()
                .is_some_and(|original| Arc::ptr_eq(&original, output));
            let state_exact = original_state
                .upgrade()
                .is_some_and(|original| Arc::ptr_eq(&original, &behavior.base().original));
            let head_exact = original_head
                .upgrade()
                .is_some_and(|original| Arc::ptr_eq(&original, &head.0));
            let middle_exact = original_middle
                .upgrade()
                .is_some_and(|original| Arc::ptr_eq(&original, &middle.0));
            let tail_exact = original_tail
                .upgrade()
                .is_some_and(|original| Arc::ptr_eq(&original, &tail.0));
            Some((
                (
                    output_exact,
                    state_exact,
                    head_exact,
                    middle_exact,
                    tail_exact,
                ),
                (
                    output.as_slice().to_vec(),
                    behavior.base().original.as_slice().to_vec(),
                    head.0.as_slice().to_vec(),
                    middle.0.as_slice().to_vec(),
                    tail.0.as_slice().to_vec(),
                ),
                (origin.address(), behavior.base().admission),
                settlements
                    .iter()
                    .map(|settlement| {
                        (
                            settlement.creations.len(),
                            settlement.sends.owned,
                            settlement.sends.inner,
                            matches!(settlement.become_, Step::Stop(_)),
                        )
                    })
                    .collect::<Vec<_>>(),
                (
                    control.len(),
                    user.len(),
                    descendants.len(),
                    capability_failures.len(),
                    unread_owner_cancellation.is_none(),
                    matches!(completion, Completion::Stopped),
                ),
                (
                    *head_represented,
                    head_entities.len(),
                    *middle_represented,
                    middle_entities.len(),
                    *tail_represented,
                    tail_entities.len(),
                ),
                (*head_metrics, *middle_metrics, *tail_metrics),
                (
                    head_family_disposal_failure.is_some(),
                    middle_family_disposal_failure.is_some(),
                    tail_family_disposal_failure.is_some(),
                    middle_family_disposal_failure
                        .as_ref()
                        .map(|cause| ptr::from_ref(cause.as_ref()).cast::<()>()),
                ),
            ))
        }
        _ => None,
    };
    let public_call_returned = caught.is_ok();
    let fault_retained = original_fault.as_ref().map(Weak::strong_count);
    drop(returned);
    drop(caught);
    let fault_released = original_fault.as_ref().map(Weak::strong_count);
    let released = (
        original_output.strong_count(),
        original_state.strong_count(),
        original_head.strong_count(),
        original_middle.strong_count(),
        original_tail.strong_count(),
    );
    let empty_metrics = EntityMetrics {
        activations: 0,
        hydration_failures: 0,
        launch_failures: 0,
        capacity_refusals: 0,
        forced_retirements: 0,
        peak_hydrations: 0,
        residents: 0,
    };
    assert!(public_call_returned);
    assert_eq!(fault_retained, original_fault.as_ref().map(|_| 1));
    assert_eq!(fault_released, original_fault.as_ref().map(|_| 0));
    assert_eq!(
        acquired_products,
        Some((
            (true, true, true, true, true),
            (
                vec![11, 19],
                vec![31, 37],
                vec![41, 43],
                vec![47, 53],
                vec![59, 61]
            ),
            (MailAddr::APPLICATION_ROOT, AccountAdmission::Ready),
            vec![(0, NoSends, NoSends, true)],
            (0, 0, 0, 0, true, true),
            (0, 0, 0, 0, 0, 0),
            (empty_metrics, empty_metrics, empty_metrics),
            (false, original_fault.is_some(), false, original_carrier),
        )),
        "the original completed output, joined root and acquired family prefix must survive later family disposal",
    );
    assert_eq!(retained, (1, 1, 1, 1, 1));
    assert_eq!(released, (0, 0, 0, 0, 0));
}

#[test]
fn family_report_disposal_keeps_acquired_products_without_fault() {
    family_report_disposal_preserves_acquired_products(None);
}

#[test]
fn family_report_disposal_fault_keeps_acquired_products() {
    family_report_disposal_preserves_acquired_products(Some(Arc::new(vec![67, 71])));
}

struct DeclaredAccountRole {
    #[expect(
        clippy::redundant_allocation,
        reason = "Preserve native panic object identity separately from its shared payload lifetime."
    )]
    original_fault: Option<Box<Arc<Vec<u8>>>>,
}

impl Drop for DeclaredAccountRole {
    fn drop(&mut self) {
        if let Some(original) = self.original_fault.take() {
            resume_unwind(original);
        }
    }
}

#[expect(
    clippy::type_complexity,
    reason = "retain the genuine two-role declared actor and closed per-occurrence failure product"
)]
#[expect(
    dead_code,
    reason = "derive the exact declared-role projection contract; refused or cold-rejected roots never construct projected terminals"
)]
#[derive(TerminalProjection)]
enum StagedAccountConclusion {
    Root {
        origin: RootOrigin<StopOnShutdown<ReceivingAccount>>,
        terminal: ActorRetirement<
            ApplicationBehavior<
                StopOnShutdown<ReceivingAccount>,
                ChildCons<
                    MailAddr,
                    StopOnShutdown<ReceivingAccount>,
                    ChildCons<MailAddr, StopOnShutdown<ReceivingAccount>, NoChildren>,
                >,
            >,
            Self,
            (
                Vec<
                    ChildFailure<
                        ChildOrigin<StopOnShutdown<ReceivingAccount>, DeclaredAccountRole>,
                        StopOnShutdown<ReceivingAccount>,
                    >,
                >,
                (
                    Vec<
                        ChildFailure<
                            ChildOrigin<StopOnShutdown<ReceivingAccount>, AccountsRole>,
                            StopOnShutdown<ReceivingAccount>,
                        >,
                    >,
                    (),
                ),
            ),
        >,
    },
    #[application_actor]
    CurrentAccount {
        origin: ChildOrigin<StopOnShutdown<ReceivingAccount>, DeclaredAccountRole>,
        terminal: ActorRetirement<StopOnShutdown<ReceivingAccount>, Self, ()>,
    },
    #[application_actor]
    EarlierAccount {
        origin: ChildOrigin<StopOnShutdown<ReceivingAccount>, AccountsRole>,
        terminal: ActorRetirement<StopOnShutdown<ReceivingAccount>, Self, ()>,
    },
}

#[expect(
    clippy::too_many_lines,
    reason = "keep the two-role staging fault, actual whole refusal control and original allocation discharge in one independent trace"
)]
fn declaration_role_disposal_preserves_original_actors(original_fault: Option<Arc<Vec<u8>>>) {
    let original_fault = original_fault.map(Box::new);
    let fault = original_fault
        .as_ref()
        .map(|cause| Arc::downgrade(cause.as_ref()));
    let original_carrier = original_fault
        .as_ref()
        .map(|cause| ptr::from_ref(cause.as_ref()).cast::<()>());
    let root = Arc::new(vec![173, 179]);
    let original_root = Arc::downgrade(&root);
    let current = Arc::new(vec![181, 191]);
    let original_current = Arc::downgrade(&current);
    let earlier = Arc::new(vec![193, 197]);
    let original_earlier = Arc::downgrade(&earlier);
    let work = Arc::new(vec![199, 211]);
    let original_work = Arc::downgrade(&work);
    // Application::child prepends. The earlier declaration is staged first;
    // the current role is disposed while that complete child product is owned.
    let application = Application::new(
        ReceivingAccount {
            admission: AccountAdmission::Reject,
            original: root,
        }
        .stop_on_shutdown(),
    )
    .child(
        AccountsRole(Arc::new(vec![223, 227])),
        ReceivingAccount {
            admission: AccountAdmission::Ready,
            original: earlier,
        }
        .stop_on_shutdown(),
    )
    .child(
        DeclaredAccountRole { original_fault },
        ReceivingAccount {
            admission: AccountAdmission::Ready,
            original: current,
        }
        .stop_on_shutdown(),
    );
    let caller = Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("the actual caller runtime builds");
    let mut returned = None;
    let caught = catch_unwind(AssertUnwindSafe(|| {
        returned = Some(caller.block_on(
            application.run_with::<StagedAccountConclusion, _, _, _, _, _>(move |_| async move {
                drop(work);
                panic!("the original refused root must leave work uninvoked");
            }),
        ));
    }));
    // A faulting role returns actual cold partial inputs before actor handoff.
    // The control joins the refused root before the public result is acquired.
    let mut cold_inputs = None;
    let mut cold_metadata = None;
    let mut joined_refusal = None;
    let mut uninvoked_work_retained = None;
    match returned.take() {
        Some(Ok((
            ApplicationOutcome::StagingRejected {
                inputs:
                    (
                        root,
                        work,
                        ApplicationStagingError::RoleDisposalPanicked {
                            id,
                            actor,
                            children,
                            cause,
                        },
                    ),
            },
            Err(_),
            Err(_),
        ))) => {
            let mut creations = children.into_creates().into_iter();
            let creation = creations
                .next()
                .expect("the earlier original cold child survives");
            let remaining = creations.next();
            let (earlier_id, earlier, kind) = creation.into_parts();
            let earlier = match earlier {
                ChildChoice::Head(earlier) => earlier,
                ChildChoice::Tail(never) => match never {},
            };
            cold_metadata = Some((
                id,
                earlier_id,
                kind,
                remaining.is_none(),
                root.base().admission,
                root.base().original.as_slice().to_vec(),
                actor.base().admission,
                actor.base().original.as_slice().to_vec(),
                earlier.base().admission,
                earlier.base().original.as_slice().to_vec(),
            ));
            // The original root/F/current child/earlier child/native cause all
            // remain concrete owners outside the disposed user Role.
            cold_inputs = Some((root, work, actor, earlier, cause));
        }
        Some(Ok((
            ApplicationOutcome::NotInvoked {
                work,
                startup_error: Some(startup_error),
                cleanup: Ok(()),
            },
            Ok((origin, retirement)),
            Ok(ActorNotificationReceipts {
                termination: Ok(()),
                retirement: Ok(()),
            }),
        ))) => {
            uninvoked_work_retained = Some(original_work.strong_count());
            // This concrete consumer intentionally releases its returned uninvoked work.
            drop((work, startup_error));
            joined_refusal = Some((origin, retirement));
        }
        other => returned = other,
    }
    let actors_retained = (
        original_root.strong_count(),
        original_current.strong_count(),
        original_earlier.strong_count(),
    );
    let actor_contents = (
        original_root
            .upgrade()
            .map(|original| original.as_slice().to_vec()),
        original_current
            .upgrade()
            .map(|original| original.as_slice().to_vec()),
        original_earlier
            .upgrade()
            .map(|original| original.as_slice().to_vec()),
    );
    let work_released = original_work.strong_count();
    let complete_refusal = match joined_refusal.as_ref() {
        Some((
            origin,
            ActorRetirement::InitializationRejected {
                behavior,
                error,
                control,
                user,
                descendants,
                child_failures: (current_failures, (earlier_failures, ())),
                capability_failures,
                unread_owner_cancellation,
                additional_failures,
                received_interpretation,
                received_source,
                source_index,
                acquired_ingress,
                retirement_failures,
                terminal_report,
            },
        )) => {
            assert!(matches!(
                (
                    received_interpretation,
                    received_source,
                    source_index,
                    acquired_ingress
                ),
                (None, None, None, None)
            ));
            assert!(additional_failures.is_empty());
            assert!(retirement_failures.is_empty());
            assert!(terminal_report.is_none());
            let refusal = match error {
                ApplicationDefinitionError::Root(AccountRefusal::Initialization) => {
                    AccountRefusal::Initialization
                }
                ApplicationDefinitionError::InitializedTwice => {
                    panic!("the real application cannot initialize twice")
                }
            };
            Some((
                origin.address(),
                behavior.base().admission,
                behavior.base().original.as_slice().to_vec(),
                refusal,
                (
                    control.len(),
                    user.len(),
                    descendants.len(),
                    current_failures.len(),
                    earlier_failures.len(),
                    capability_failures.len(),
                    unread_owner_cancellation.is_none(),
                ),
            ))
        }
        _ => None,
    };
    let fault_owned = fault.as_ref().map(Weak::strong_count);
    let received_carrier = cold_inputs
        .as_ref()
        .map(|(_, _, _, _, cause)| ptr::from_ref(cause.as_ref()).cast::<()>());
    drop(cold_inputs);
    drop(joined_refusal);
    drop(returned);
    drop(caught);
    let actors_released = (
        original_root.strong_count(),
        original_current.strong_count(),
        original_earlier.strong_count(),
    );
    let fault_released = fault.as_ref().map(Weak::strong_count);
    let work_disposed = original_work.strong_count();
    // Full error/result cleanup and original native payload discharge precede
    // every omission oracle. Startup refusal now returns the original Work;
    // this consumer explicitly discharges it before the existing release oracle.
    assert_eq!(fault_owned, fault.as_ref().map(|_| 1));
    assert_eq!(received_carrier, original_carrier);
    assert_eq!(fault_released, fault.as_ref().map(|_| 0));
    if fault.is_none() {
        assert_eq!(uninvoked_work_retained, Some(1));
        assert_eq!(work_released, 0);
        assert!(cold_metadata.is_none());
        assert_eq!(
            complete_refusal,
            Some((
                MailAddr::APPLICATION_ROOT,
                AccountAdmission::Reject,
                vec![173, 179],
                AccountRefusal::Initialization,
                (0, 0, 0, 0, 0, 0, true)
            ))
        );
    } else {
        assert_eq!(uninvoked_work_retained, None);
        assert_eq!(work_released, 1);
        assert!(complete_refusal.is_none());
        let Some((
            current_id,
            earlier_id,
            kind,
            no_remaining,
            root_admission,
            root_contents,
            current_admission,
            current_contents,
            earlier_admission,
            earlier_contents,
        )) = cold_metadata
        else {
            panic!("the cold partial retains both actual actor declarations");
        };
        assert_ne!(current_id, earlier_id);
        assert_eq!(kind, CreationKind::Birth);
        assert!(no_remaining);
        assert_eq!(root_admission, AccountAdmission::Reject);
        assert_eq!(root_contents, vec![173, 179]);
        assert_eq!(current_admission, AccountAdmission::Ready);
        assert_eq!(current_contents, vec![181, 191]);
        assert_eq!(earlier_admission, AccountAdmission::Ready);
        assert_eq!(earlier_contents, vec![193, 197]);
    }
    assert_eq!(actors_retained, (1, 1, 1));
    assert_eq!(
        actor_contents,
        (
            Some(vec![173, 179]),
            Some(vec![181, 191]),
            Some(vec![193, 197])
        )
    );
    assert_eq!(work_disposed, 0);
    assert_eq!(actors_released, (0, 0, 0));
}

#[test]
fn declared_role_disposal_preserves_original_actors_without_fault() {
    declaration_role_disposal_preserves_original_actors(None);
}

#[test]
fn declared_role_disposal_fault_preserves_original_actors() {
    declaration_role_disposal_preserves_original_actors(Some(Arc::new(vec![229, 233])));
}

#[test]
fn unpolled_declared_execution_keeps_inputs_after_result_surrender() {
    let runtime = Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("the actual runtime builds");
    let root = Arc::new(vec![31, 37]);
    let original_root = Arc::downgrade(&root);
    let child = Arc::new(vec![53, 59]);
    let original_child = Arc::downgrade(&child);
    let role = Arc::new(vec![61, 67]);
    let original_role = Arc::downgrade(&role);
    let work = Rc::new(vec![71, 73]);
    let original_work = Rc::downgrade(&work);
    let borrowed = vec![79, 83];
    runtime.block_on(async {
        let borrowed = &borrowed;
        let application = Application::new(
            ReceivingAccount {
                admission: AccountAdmission::Reject,
                original: root,
            }
            .stop_on_shutdown(),
        )
        .child(
            AccountsRole(role),
            ReceivingAccount {
                admission: AccountAdmission::Ready,
                original: child,
            }
            .stop_on_shutdown(),
        );
        let (execution, result) = application
            .execute_with::<_, _, DeclaredAccountConclusion, _, _, _>(move |_| async move {
                (work, borrowed)
            })
            .unwrap_or_else(|_| panic!("the original executor is entered"));
        let execution = Box::pin(execution);
        drop(result);
        // No first poll: no staging/actor/task has consumed the actual declarations.
        let retained = (
            original_root.strong_count(),
            original_child.strong_count(),
            original_role.strong_count(),
            original_work.strong_count(),
        );
        drop(execution);
        let released = (
            original_root.strong_count(),
            original_child.strong_count(),
            original_role.strong_count(),
            original_work.strong_count(),
        );
        assert_eq!(retained, (1, 1, 1, 1));
        assert_eq!(released, (0, 0, 0, 0));
    });
}

#[expect(
    clippy::too_many_lines,
    reason = "retain the real declared refusal, every original terminal lane and the recovered borrowed callable through complete join and discharge"
)]
#[test]
fn declared_startup_refusal_returns_borrowed_non_send_callable() {
    let runtime = Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("the actual runtime builds");
    let root = Arc::new(vec![31, 37]);
    let original_root = Arc::downgrade(&root);
    let child = Arc::new(vec![53, 59]);
    let original_child = Arc::downgrade(&child);
    let role = Arc::new(vec![61, 67]);
    let original_role = Arc::downgrade(&role);
    let work = Rc::new(vec![71, 73]);
    let original_work = Rc::downgrade(&work);
    let borrowed = vec![79, 83];
    runtime.block_on(async {
        let borrowed = &borrowed;
        let application = Application::new(
            ReceivingAccount {
                admission: AccountAdmission::Reject,
                original: root,
            }
            .stop_on_shutdown(),
        )
        .child(
            AccountsRole(role),
            ReceivingAccount {
                admission: AccountAdmission::Ready,
                original: child,
            }
            .stop_on_shutdown(),
        );
        let (execution, result) = application
            .execute_with::<_, _, DeclaredAccountConclusion, _, _, _>(move |_| async move {
                (work, borrowed)
            })
            .unwrap_or_else(|_| panic!("the original executor is entered"));
        let ((), outcome) = tokio::join!(execution, result);
        let (
            ApplicationOutcome::NotInvoked {
                work,
                startup_error,
                cleanup: Ok(()),
            },
            root_receipt,
            Ok(ActorNotificationReceipts {
                termination: Ok(()),
                retirement: Ok(()),
            }),
        ) = outcome
        else {
            panic!("the actual refused root never invokes the original callable");
        };
        let Ok((
            origin,
            ActorRetirement::InitializationRejected {
                behavior,
                error,
                control,
                user,
                descendants,
                child_failures: (child_failures, ()),
                capability_failures,
                unread_owner_cancellation,
                additional_failures,
                received_interpretation,
                received_source,
                source_index,
                acquired_ingress,
                retirement_failures,
                terminal_report,
            },
        )) = &root_receipt
        else {
            panic!("both real joins return the complete declared initialization refusal");
        };
        assert!(matches!(
            (
                received_interpretation,
                received_source,
                source_index,
                acquired_ingress
            ),
            (None, None, None, None)
        ));
        assert!(additional_failures.is_empty());
        assert!(retirement_failures.is_empty());
        assert!(terminal_report.is_none());
        let refused = match error {
            ApplicationDefinitionError::Root(reason) => *reason,
            ApplicationDefinitionError::InitializedTwice => {
                panic!("the actual root initializes once")
            }
        };
        let trace = (
            origin.address(),
            behavior.base().admission,
            behavior.base().original.as_slice().to_vec(),
            refused,
            control.len(),
            user.len(),
            descendants.len(),
            child_failures.len(),
            capability_failures.len(),
            unread_owner_cancellation.is_none(),
        );
        let startup_refused = startup_error.is_some();
        let root_owned = original_root.strong_count();
        let child_owned = original_child.strong_count();
        let child_contents = original_child
            .upgrade()
            .map(|child| child.as_slice().to_vec());
        let role_released = original_role.strong_count();
        let work_owned = original_work.strong_count();
        drop((work, root_receipt, startup_error));
        let released = (
            original_root.strong_count(),
            original_child.strong_count(),
            original_work.strong_count(),
        );
        assert_eq!(
            trace,
            (
                MailAddr::APPLICATION_ROOT,
                AccountAdmission::Reject,
                vec![31, 37],
                AccountRefusal::Initialization,
                0,
                0,
                0,
                0,
                0,
                true
            )
        );
        assert!(startup_refused);
        assert_eq!(root_owned, 1);
        assert_eq!(child_owned, 1);
        assert_eq!(child_contents, Some(vec![53, 59]));
        assert_eq!(role_released, 0);
        assert_eq!(work_owned, 1);
        assert_eq!(released, (0, 0, 0));
    });
}

#[expect(
    clippy::too_many_lines,
    reason = "keep original root, two actual cold child declarations, non-Send borrowed callable and passive role cause through one complete discharge oracle"
)]
#[test]
fn declared_execution_returns_cold_partial_and_non_send_callable() {
    let runtime = Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("the actual runtime builds");
    let root = Arc::new(vec![173, 179]);
    let original_root = Arc::downgrade(&root);
    let current = Arc::new(vec![181, 191]);
    let original_current = Arc::downgrade(&current);
    let earlier = Arc::new(vec![193, 197]);
    let original_earlier = Arc::downgrade(&earlier);
    let work = Rc::new(vec![199, 211]);
    let original_work = Rc::downgrade(&work);
    let cause = Box::new(Arc::new(vec![229, 233]));
    let original_cause = Arc::downgrade(cause.as_ref());
    let original_carrier = ptr::from_ref(cause.as_ref()).cast::<()>();
    let borrowed = vec![239, 241];
    runtime.block_on(async {
        let borrowed = &borrowed;
        let application = Application::new(
            ReceivingAccount {
                admission: AccountAdmission::Reject,
                original: root,
            }
            .stop_on_shutdown(),
        )
        .child(
            AccountsRole(Arc::new(vec![223, 227])),
            ReceivingAccount {
                admission: AccountAdmission::Ready,
                original: earlier,
            }
            .stop_on_shutdown(),
        )
        .child(
            DeclaredAccountRole {
                original_fault: Some(cause),
            },
            ReceivingAccount {
                admission: AccountAdmission::Ready,
                original: current,
            }
            .stop_on_shutdown(),
        );
        let (execution, result) = application
            .execute_with::<_, _, StagedAccountConclusion, _, _, _>(move |_| async move {
                (work, borrowed)
            })
            .unwrap_or_else(|_| panic!("the original executor is entered"));
        let ((), outcome) = tokio::join!(execution, result);
        let (
            ApplicationOutcome::StagingRejected {
                inputs:
                    (
                        root,
                        work,
                        ApplicationStagingError::RoleDisposalPanicked {
                            id,
                            actor,
                            children,
                            cause,
                        },
                    ),
            },
            Err(_),
            Err(_),
        ) = outcome
        else {
            panic!(
                "the actual role fault returns cold partials without startup or cleanup fiction"
            );
        };
        let mut creations = children.into_creates().into_iter();
        let creation = creations
            .next()
            .expect("the actual earlier declaration survives");
        let remaining = creations.next();
        let (earlier_id, earlier, kind) = creation.into_parts();
        let earlier = match earlier {
            ChildChoice::Head(earlier) => earlier,
            ChildChoice::Tail(never) => match never {},
        };
        let trace = (
            id,
            earlier_id,
            kind,
            remaining.is_none(),
            root.base().admission,
            root.base().original.as_slice().to_vec(),
            actor.base().admission,
            actor.base().original.as_slice().to_vec(),
            earlier.base().admission,
            earlier.base().original.as_slice().to_vec(),
        );
        let retained = (
            original_root.strong_count(),
            original_current.strong_count(),
            original_earlier.strong_count(),
            original_work.strong_count(),
            original_cause.strong_count(),
        );
        let received_carrier = ptr::from_ref(cause.as_ref()).cast::<()>();
        drop((root, work, actor, earlier, cause));
        let released = (
            original_root.strong_count(),
            original_current.strong_count(),
            original_earlier.strong_count(),
            original_work.strong_count(),
            original_cause.strong_count(),
        );
        assert_ne!(trace.0, trace.1);
        assert_eq!(trace.2, CreationKind::Birth);
        assert!(trace.3);
        assert_eq!(
            (trace.4, trace.5),
            (AccountAdmission::Reject, vec![173, 179])
        );
        assert_eq!(
            (trace.6, trace.7),
            (AccountAdmission::Ready, vec![181, 191])
        );
        assert_eq!(
            (trace.8, trace.9),
            (AccountAdmission::Ready, vec![193, 197])
        );
        assert_eq!(received_carrier, original_carrier);
        assert_eq!(retained, (1, 1, 1, 1, 1));
        assert_eq!(released, (0, 0, 0, 0, 0));
    });
}

struct LiveAccountNotice;

impl Protocol for LiveAccountNotice {
    type Addr = MailAddr;
    type Msg = LiveAccountReply;
}

#[derive(Debug, Eq, PartialEq)]
enum LiveAccountReply {
    Accepted(u64),
}

struct LiveAccountCommand {
    observer: EstablishedRecipient<LiveAccountNotice>,
    value: u64,
}

struct LiveAccount {
    original: Arc<Vec<u8>>,
    accepted: Vec<u64>,
}

#[actor(sends = pub(crate) { notices: Vec<EstablishedDelivery<LiveAccountNotice>> })]
impl LiveAccount {
    fn receive(&mut self, command: LiveAccountCommand) -> BehaviorActed<Self> {
        self.accepted.push(command.value);
        Ok(Actions::cont().send_notices(EstablishedDelivery::new(
            command.observer,
            LiveAccountReply::Accepted(command.value),
        )))
    }
}

#[derive(ActorSpaces)]
struct LiveAccountSpaces {
    #[actor_space(ReceivingAccount)]
    roots: ActorSpace<ReceivingAccount>,
    #[actor_space(LiveAccount)]
    accounts: ActorSpace<LiveAccount>,
}

struct LiveAccounts {
    original: Arc<Vec<u8>>,
    hydrated: Arc<Mutex<Vec<EntityId<u64>>>>,
    #[expect(
        clippy::type_complexity,
        reason = "keep the actual activation identity and complete failure in the observational callback lane"
    )]
    activation_failures: Arc<
        Mutex<
            Vec<(
                EntityId<u64>,
                ActivationId,
                EntityActivationError<Never, StopOnShutdown<LiveAccount>, Never, ()>,
            )>,
        >,
    >,
    #[expect(
        clippy::type_complexity,
        reason = "retain the complete original admission refusal and its entity identity in the observational callback lane"
    )]
    refusals: Arc<Mutex<Vec<(EntityId<u64>, AdmissionFailure<LiveAccountCommand>)>>>,
    #[expect(
        clippy::type_complexity,
        reason = "retain the actual activation identity, entity identity, and exact native drain failure together in the observational callback lane"
    )]
    forced: Arc<Mutex<Vec<(EntityId<u64>, ActivationId, DrainFailure)>>>,
    #[expect(
        clippy::type_complexity,
        reason = "retain the exact issued activation and whole raw actor result in the observational callback lane"
    )]
    retired: Arc<
        Mutex<
            Vec<(
                EntityId<u64>,
                ActivationId,
                Result<ActorRetirement<StopOnShutdown<LiveAccount>, Never, ()>, JoinError>,
                ActorRetirementReport,
            )>,
        >,
    >,
    disposal_cause: Option<Arc<Vec<u8>>>,
}

impl Drop for LiveAccounts {
    fn drop(&mut self) {
        if let Some(original) = self.disposal_cause.take() {
            panic_any(original);
        }
    }
}

impl EntityDefinition for LiveAccounts {
    type Id = u64;
    type Behavior = StopOnShutdown<LiveAccount>;
    type Hosts = LiveAccountSpaces;
    type HydrationError = Never;
    type Terminal = Never;
    type ChildFailures = ();

    #[expect(
        clippy::unused_async_trait_impl,
        reason = "hydrate the original account when the existing host future is polled"
    )]
    async fn hydrate(&self, id: EntityId<u64>) -> Result<Self::Behavior, Never> {
        self.hydrated
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .push(id);
        Ok(LiveAccount {
            original: Arc::clone(&self.original),
            accepted: Vec::new(),
        }
        .stop_on_shutdown())
    }

    fn activation_failed(
        &self,
        id: EntityId<u64>,
        activation: ActivationId,
        failure: EntityActivationError<Never, Self::Behavior, Never, ()>,
    ) {
        self.activation_failures
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .push((id, activation, failure));
    }

    fn admission_refused(&self, id: EntityId<u64>, failure: AdmissionFailure<LiveAccountCommand>) {
        self.refusals
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .push((id, failure));
    }

    fn forced_retirement(
        &self,
        id: &EntityId<u64>,
        activation: ActivationId,
        failure: DrainFailure,
    ) {
        self.forced
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .push((*id, activation, failure));
    }

    fn retired(
        &self,
        id: &EntityId<u64>,
        activation: ActivationId,
        retirement: Result<ActorRetirement<Self::Behavior, Never, ()>, JoinError>,
        retirement_report: ActorRetirementReport,
    ) {
        self.retired
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .push((*id, activation, retirement, retirement_report));
    }
}

#[test]
fn admitted_family_shutdown_preserves_joined_live_account() {
    admitted_family_disposal_preserves_original_products(None);
}

#[test]
fn admitted_family_disposal_fault_preserves_joined_live_account() {
    admitted_family_disposal_preserves_original_products(Some(Arc::new(vec![177, 181])));
}

#[expect(
    clippy::too_many_lines,
    reason = "one end-to-end admitted-family trace keeps original work, actor join, all callbacks, shutdown, metrics and native disposal in one owning scope"
)]
fn admitted_family_disposal_preserves_original_products(disposal_cause: Option<Arc<Vec<u8>>>) {
    let original_cause = disposal_cause.as_ref().map(Arc::downgrade);
    let original = Arc::new(vec![241, 251]);
    let original_account = Arc::downgrade(&original);
    let root = Arc::new(vec![31, 37]);
    let original_root = Arc::downgrade(&root);
    let role = Arc::new(vec![157, 163]);
    let original_role = Arc::downgrade(&role);
    let selection = Arc::clone(&role);
    let output = Arc::new(vec![169u8, 171]);
    let original_output = Arc::downgrade(&output);
    let hydrated = Arc::new(Mutex::new(Vec::new()));
    let activation_failures = Arc::new(Mutex::new(Vec::new()));
    let refusals = Arc::new(Mutex::new(Vec::new()));
    let forced = Arc::new(Mutex::new(Vec::new()));
    let retired = Arc::new(Mutex::new(Vec::new()));
    let work_retirements = Arc::clone(&retired);
    let application = App::new(
        ReceivingAccount {
            admission: AccountAdmission::Ready,
            original: root,
        }
        .stop_on_shutdown(),
        LiveAccountSpaces {
            roots: ActorSpace::new(),
            accounts: ActorSpace::new(),
        },
    )
    .entity_family(
        AccountsRole(role),
        LiveAccounts {
            original,
            hydrated: Arc::clone(&hydrated),
            activation_failures: Arc::clone(&activation_failures),
            refusals: Arc::clone(&refusals),
            forced: Arc::clone(&forced),
            retired: Arc::clone(&retired),
            disposal_cause,
        },
        DirectoryConfig::default(),
        EntityCapacity::new(
            NonZeroUsize::new(1).expect("one resident"),
            NonZeroUsize::new(1).expect("one hydration"),
        ),
    )
    .unwrap_or_else(|_| panic!("the actual one-family directory is valid"));
    let mut returned = None;
    let caught = catch_unwind(AssertUnwindSafe(|| {
        let application_host = Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("the synchronous disposal controller owns one enabled application host");
        let products = application_host
            .block_on(application.run_with_entities::<AccountConclusion, _, _, _>(
                move |application| async move {
                    let accounts = application.entities(AccountsRole(selection));
                    let account = accounts.entity(73);
                    let interface = application.interface(account);
                    let mut observer = interface
                        .external::<LiveAccountNotice>()
                        .expect("the actual external recipient is established");
                    let admitted = observer
                        .send(
                            interface.api(),
                            LiveAccountCommand {
                                observer: observer.recipient(),
                                value: 283,
                            },
                        )
                        .await;
                    let ready = match &admitted {
                        Ok(()) => observer.receive().await,
                        Err(_) => None,
                    };
                    let shutdown = application.lifecycle().request_shutdown();
                    let termination = application.lifecycle().termination().await;
                    let retirements_before_work_completion = work_retirements
                        .lock()
                        .unwrap_or_else(PoisonError::into_inner)
                        .len();
                    (
                        output,
                        admitted,
                        ready,
                        shutdown,
                        termination,
                        retirements_before_work_completion,
                    )
                },
            ))
            .unwrap_or_else(|(application, work, error)| {
                drop((application, work));
                panic!("the explicit application host must be entered: {error}");
            });
        drop(application_host);
        returned = Some(products);
    }));
    // The explicit owned host completed its actual root/family joins and disposal before these observations.
    let hydrated = hydrated.lock().unwrap_or_else(PoisonError::into_inner);
    assert_eq!(hydrated.as_slice(), &[EntityId::new(73)]);
    drop(hydrated);
    let activation_failures = activation_failures
        .lock()
        .unwrap_or_else(PoisonError::into_inner);
    assert_eq!(activation_failures.len(), 0);
    drop(activation_failures);
    let refusals = refusals.lock().unwrap_or_else(PoisonError::into_inner);
    assert_eq!(refusals.len(), 0);
    drop(refusals);
    let forced = forced.lock().unwrap_or_else(PoisonError::into_inner);
    assert_eq!(forced.len(), 0);
    drop(forced);
    let mut retirement_rows = retired.lock().unwrap_or_else(PoisonError::into_inner);
    let retirement_rows = mem::take(&mut *retirement_rows);
    let mut retirement_rows = retirement_rows.into_iter();
    let row = retirement_rows
        .next()
        .expect("the genuine admitted incarnation joins once");
    let remaining = retirement_rows.next();
    assert!(remaining.is_none());
    let (id, activation, joined, report) = row;
    assert_eq!(report.retirement(), RetirementAssessment::Established);
    assert_eq!(report.failures(), ActorFailureAssessment::NoFailuresFound);
    println!("joined original Entity {id:?} activation {activation:?}");
    assert_eq!(id, EntityId::new(73));
    let retirement =
        joined.unwrap_or_else(|_| panic!("the original active account joins successfully"));
    match &retirement {
        ActorRetirement::Completed {
            behavior,
            settlements,
            control,
            user,
            descendants,
            child_failures: (),
            capability_failures,
            unread_owner_cancellation: None | Some(()),
            completion,
            interpretation,
            source,
            additional_failures,
            received_interpretation,
            received_source,
            source_index,
            acquired_ingress,
            retirement_failures,
            terminal_report,
        } => {
            assert!(matches!(
                (
                    interpretation,
                    source,
                    received_interpretation,
                    received_source,
                    source_index,
                    acquired_ingress
                ),
                (None, None, None, None, None, None)
            ));
            assert!(additional_failures.is_empty());
            assert!(retirement_failures.is_empty());
            assert!(terminal_report.is_none());
            assert_eq!(behavior.base().accepted, vec![283]);
            assert_eq!(behavior.base().original.as_slice(), &[241, 251]);
            assert_eq!(settlements.len(), 1);
            assert_eq!(settlements[0].creations.len(), 0);
            assert_eq!(settlements[0].sends.owned, NoSends);
            assert_eq!(settlements[0].sends.inner.notices.len(), 0);
            assert!(matches!(settlements[0].become_, Step::Stop(_)));
            assert_eq!(control.len(), 0);
            assert_eq!(user.len(), 0);
            assert_eq!(descendants.len(), 0);
            assert_eq!(capability_failures.len(), 0);
            assert_eq!(*completion, Completion::Stopped);
        }
        ActorRetirement::OwnerCancelled {
            behavior,
            settlements,
            control,
            user,
            descendants,
            child_failures: (),
            capability_failures,
            unread_owner_cancellation,
            interpretation,
            source,
            additional_failures,
            received_interpretation,
            received_source,
            source_index,
            acquired_ingress,
            retirement_failures,
            terminal_report,
        } => {
            assert!(matches!(
                (
                    interpretation,
                    source,
                    received_interpretation,
                    received_source,
                    source_index,
                    acquired_ingress
                ),
                (None, None, None, None, None, None)
            ));
            assert!(additional_failures.is_empty());
            assert!(retirement_failures.is_empty());
            assert!(terminal_report.is_none());
            assert_eq!(behavior.base().accepted, vec![283]);
            assert_eq!(behavior.base().original.as_slice(), &[241, 251]);
            assert_eq!(settlements.len(), 0);
            assert!(matches!(control.as_slice(), [EventLayer::Owned(_)]));
            assert_eq!(user.len(), 0);
            assert_eq!(descendants.len(), 0);
            assert_eq!(capability_failures.len(), 0);
            assert!(unread_owner_cancellation.is_none());
        }
        _ => panic!("the actual fence permits exactly stopped or same-lease cancellation"),
    }
    let account_exact = original_account
        .upgrade()
        .is_some_and(|original| match &retirement {
            ActorRetirement::Completed { behavior, .. }
            | ActorRetirement::OwnerCancelled { behavior, .. } => {
                Arc::ptr_eq(&original, &behavior.base().original)
            }
            _ => false,
        });
    let acquired_account = original_account.strong_count();
    drop(retirement);
    let discharged_account = original_account.strong_count();
    let acquired = match returned.as_ref() {
        Some((
            ApplicationOutcome::Completed {
                output:
                    (output, admitted, ready, shutdown, termination, retirements_before_work_completion),
                cleanup: Ok(()),
            },
            Ok((
                origin,
                ActorRetirement::Completed {
                    behavior,
                    settlements,
                    control,
                    user,
                    descendants,
                    child_failures: (),
                    capability_failures,
                    unread_owner_cancellation,
                    completion,
                    interpretation,
                    source,
                    additional_failures,
                    received_interpretation,
                    received_source,
                    source_index,
                    acquired_ingress,
                    retirement_failures,
                    terminal_report,
                },
            )),
            Ok(ActorNotificationReceipts {
                termination: Ok(()),
                retirement: Ok(()),
            }),
            (
                Ok((
                    role,
                    (
                        EntityShutdown::Settled {
                            represented,
                            entities,
                        },
                        metrics,
                        disposal_failure,
                    ),
                )),
                (),
            ),
        )) => {
            assert!(matches!(
                (
                    interpretation,
                    source,
                    received_interpretation,
                    received_source,
                    source_index,
                    acquired_ingress
                ),
                (None, None, None, None, None, None)
            ));
            assert!(additional_failures.is_empty());
            assert!(retirement_failures.is_empty());
            assert!(terminal_report.is_none());
            let Ok(()) = admitted else {
                panic!("the actual EntityAdmission accepted the original command");
            };
            let Some(User {
                from,
                message: LiveAccountReply::Accepted(value),
            }) = ready
            else {
                panic!("the real active account sent its complete readiness fact");
            };
            assert_ne!(*from, MailAddr::APPLICATION_ROOT);
            assert_eq!(*value, 283);
            assert_eq!(*shutdown, Ok(()));
            assert_eq!(*termination, Ok(Exit::Normal));
            assert_eq!(*retirements_before_work_completion, 0);
            assert_eq!(origin.address(), MailAddr::APPLICATION_ROOT);
            assert_eq!(behavior.base().admission, AccountAdmission::Ready);
            assert_eq!(behavior.base().original.as_slice(), &[31, 37]);
            assert_eq!(settlements.len(), 1);
            assert_eq!(settlements[0].creations.len(), 0);
            assert_eq!(settlements[0].sends.owned, NoSends);
            assert_eq!(settlements[0].sends.inner, NoSends);
            assert!(matches!(settlements[0].become_, Step::Stop(_)));
            assert_eq!(control.len(), 0);
            assert_eq!(user.len(), 0);
            assert_eq!(descendants.len(), 0);
            assert_eq!(capability_failures.len(), 0);
            assert!(unread_owner_cancellation.is_none());
            assert_eq!(*completion, Completion::Stopped);
            assert_eq!(role.0.as_slice(), &[157, 163]);
            assert_eq!(output.as_slice(), &[169, 171]);
            assert_eq!(*represented, 1);
            assert_eq!(entities.as_slice(), &[EntityId::new(73)]);
            assert_eq!(
                *metrics,
                EntityMetrics {
                    activations: 1,
                    hydration_failures: 0,
                    launch_failures: 0,
                    capacity_refusals: 0,
                    forced_retirements: 0,
                    peak_hydrations: 1,
                    residents: 0
                }
            );
            assert_eq!(disposal_failure.is_some(), original_cause.is_some());
            let output_exact = original_output
                .upgrade()
                .is_some_and(|original| Arc::ptr_eq(&original, output));
            let role_exact = original_role
                .upgrade()
                .is_some_and(|original| Arc::ptr_eq(&original, &role.0));
            let root_exact = original_root
                .upgrade()
                .is_some_and(|original| Arc::ptr_eq(&original, &behavior.base().original));
            assert!(output_exact);
            assert!(role_exact);
            assert!(root_exact);
            Some((*represented, entities.clone(), *metrics))
        }
        _ => None,
    };
    let retained = (
        original_root.strong_count(),
        original_role.strong_count(),
        original_output.strong_count(),
    );
    let cause_retained = original_cause.as_ref().map(Weak::strong_count);
    drop(returned);
    drop(caught);
    let released = (
        original_root.strong_count(),
        original_role.strong_count(),
        original_output.strong_count(),
    );
    let cause_released = original_cause.as_ref().map(Weak::strong_count);
    assert!(account_exact);
    assert_eq!(acquired_account, 1);
    assert_eq!(discharged_account, 0);
    assert_eq!(
        acquired,
        Some((
            1,
            vec![EntityId::new(73)],
            EntityMetrics {
                activations: 1,
                hydration_failures: 0,
                launch_failures: 0,
                capacity_refusals: 0,
                forced_retirements: 0,
                peak_hydrations: 1,
                residents: 0
            }
        )),
        "all original root work and genuine admitted-family products survive disposal"
    );
    assert_eq!(retained, (1, 1, 1));
    assert_eq!(released, (0, 0, 0));
    assert_eq!(cause_retained, original_cause.as_ref().map(|_| 1));
    assert_eq!(cause_released, original_cause.as_ref().map(|_| 0));
}

// Host observations belong to terminal projection and caller work, never a Behavior fold.
enum HostAccountConclusion {
    Root {
        origin: RootOrigin<StopOnShutdown<ReceivingAccount>>,
        #[expect(
            clippy::type_complexity,
            reason = "retain the exact composed root, declared child origin and full typed child failure product without a forwarding alias"
        )]
        terminal: ActorRetirement<
            ApplicationBehavior<
                StopOnShutdown<ReceivingAccount>,
                ChildCons<MailAddr, ReceivingAccount, NoChildren>,
            >,
            Self,
            (
                Vec<
                    ChildFailure<
                        ChildOrigin<StopOnShutdown<ReceivingAccount>, AccountsRole>,
                        ReceivingAccount,
                    >,
                >,
                (),
            ),
        >,
        executor: Id,
    },
    Account {
        origin: ChildOrigin<StopOnShutdown<ReceivingAccount>, AccountsRole>,
        terminal: ActorRetirement<ReceivingAccount, Self, ()>,
        executor: Id,
    },
}

impl
    ProjectTerminal<
        RootOrigin<StopOnShutdown<ReceivingAccount>>,
        ActorRetirement<
            ApplicationBehavior<
                StopOnShutdown<ReceivingAccount>,
                ChildCons<MailAddr, ReceivingAccount, NoChildren>,
            >,
            Self,
            (
                Vec<
                    ChildFailure<
                        ChildOrigin<StopOnShutdown<ReceivingAccount>, AccountsRole>,
                        ReceivingAccount,
                    >,
                >,
                (),
            ),
        >,
    > for HostAccountConclusion
{
    fn project(
        origin: RootOrigin<StopOnShutdown<ReceivingAccount>>,
        terminal: ActorRetirement<
            ApplicationBehavior<
                StopOnShutdown<ReceivingAccount>,
                ChildCons<MailAddr, ReceivingAccount, NoChildren>,
            >,
            Self,
            (
                Vec<
                    ChildFailure<
                        ChildOrigin<StopOnShutdown<ReceivingAccount>, AccountsRole>,
                        ReceivingAccount,
                    >,
                >,
                (),
            ),
        >,
    ) -> Self {
        Self::Root {
            origin,
            terminal,
            executor: Handle::current().id(),
        }
    }
}

impl
    ProjectTerminal<
        ChildOrigin<StopOnShutdown<ReceivingAccount>, AccountsRole>,
        ActorRetirement<ReceivingAccount, Self, ()>,
    > for HostAccountConclusion
{
    fn project(
        origin: ChildOrigin<StopOnShutdown<ReceivingAccount>, AccountsRole>,
        terminal: ActorRetirement<ReceivingAccount, Self, ()>,
    ) -> Self {
        Self::Account {
            origin,
            terminal,
            executor: Handle::current().id(),
        }
    }
}

#[test]
#[expect(
    clippy::too_many_lines,
    reason = "keep the two real hosts, joined root and child, all typed settlement lanes and original value disposal in one outside-fold affinity controller"
)]
fn selected_actor_host_survives_distinct_caller_polling_host() {
    let actor_host = Builder::new_multi_thread()
        .worker_threads(1)
        .enable_all()
        .build()
        .expect("the selected actor host builds");
    let caller_host = Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("the distinct caller host builds");
    let selected_id = actor_host.handle().id();
    let caller_id = caller_host.handle().id();
    let root = Arc::new(vec![31, 37]);
    let original_root = Arc::downgrade(&root);
    let child = Arc::new(vec![53, 59]);
    let original_child = Arc::downgrade(&child);
    let role = Arc::new(vec![61, 67]);
    let original_role = Arc::downgrade(&role);
    let output = Rc::new(vec![277, 281]);
    let original_output = Rc::downgrade(&output);
    let borrowed = vec![283, 293];
    let borrowed_input = &borrowed;
    let entered = actor_host.enter();
    let constructed_id = Handle::current().id();
    let pair = Application::new(
        ReceivingAccount {
            admission: AccountAdmission::Ready,
            original: root,
        }
        .stop_on_shutdown(),
    )
    .child(
        AccountsRole(role),
        ReceivingAccount {
            admission: AccountAdmission::Ready,
            original: child,
        },
    )
    .execute_with::<_, _, HostAccountConclusion, _, _, _>(move |application| {
        let work_id = Handle::current().id();
        async move {
            let requested = application.lifecycle().request_shutdown();
            requested.expect("the genuine live root receives its explicit shutdown request");
            (output, borrowed_input, work_id)
        }
    });
    drop(entered);
    let (execution, result) =
        pair.unwrap_or_else(|_| panic!("construction selects the entered actor host"));
    let (work_id, projected_id, child_id, root_address, child_address) =
        caller_host.block_on(async {
            let ((), outcome) = tokio::join!(execution, result);
            let (
                ApplicationOutcome::Completed {
                    output: (output, borrowed, work_id),
                    cleanup: Ok(()),
                },
                root_receipt,
                Ok(ActorNotificationReceipts {
                    termination: Ok(()),
                    retirement: Ok(()),
                }),
            ) = outcome
            else {
                panic!("the distinct caller receives its original complete work output");
            };
            let Ok((origin, retirement)) = root_receipt else {
                panic!("both actual actor and root_receipt joins complete");
            };
            // The pair never projects root retirement; this is explicit caller policy on K.
            let projected = HostAccountConclusion::project(origin, retirement);
            let HostAccountConclusion::Root {
                origin,
                terminal,
                executor: projected_id,
            } = projected
            else {
                panic!("the raw root is projected as the original root");
            };
            let ActorRetirement::Completed {
                behavior,
                interpretation,
                source,
                settlements,
                control,
                user,
                mut descendants,
                child_failures: (child_failures, ()),
                capability_failures,
                additional_failures,
                received_interpretation,
                received_source,
                source_index,
                acquired_ingress,
                retirement_failures,
                terminal_report,
                unread_owner_cancellation,
                completion,
            } = terminal
            else {
                panic!("the explicit root shutdown retains its complete ordinary retirement");
            };
            assert_eq!(
                Arc::as_ptr(&behavior.base().original),
                original_root.as_ptr()
            );
            assert_eq!(behavior.base().original.as_slice(), [31, 37]);
            assert_eq!(behavior.base().admission, AccountAdmission::Ready);
            assert!(matches!(
                (
                    &interpretation,
                    &source,
                    &received_interpretation,
                    &received_source,
                    &source_index,
                    &acquired_ingress
                ),
                (None, None, None, None, None, None)
            ));
            assert_eq!(settlements.len(), 2);
            let mut rows = settlements.into_iter();
            let ActionSettlement {
                creations,
                sends,
                become_,
            } = rows.next().expect("one complete stop settlement");
            let CreationSettlement::Settled(creations) = creations.into_settlement() else {
                panic!("the exact empty stop creation batch is settled");
            };
            assert_eq!(creations.len(), 0);
            assert_eq!(sends.owned, NoSends);
            assert_eq!(sends.inner, NoSends);
            assert!(matches!(become_, Step::Stop(_)));
            drop(creations);
            let ActionSettlement {
                creations,
                sends,
                become_,
            } = rows.next().expect("the original initialization settlement");
            let extra = rows.next();
            assert!(extra.is_none());
            let CreationSettlement::Settled(creations) = creations.into_settlement() else {
                panic!("the original declared creation is settled");
            };
            assert_eq!(creations.len(), 1);
            let creation = creations
                .into_iter()
                .next()
                .expect("one declared child settlement");
            let SettledItem::Attempted(ItemSettlement::Accepted(ChildChoice::Head(
                ChildCreationOutcome::Established(committed),
            ))) = creation
            else {
                panic!("the original declared child was actually established");
            };
            assert_eq!(committed.kind(), CreationKind::Birth);
            assert_eq!(sends.owned, NoSends);
            assert_eq!(sends.inner, NoSends);
            assert!(matches!(become_, Step::Continue));
            assert_eq!(control, []);
            assert_eq!(user, []);
            assert!(child_failures.is_empty());
            assert!(capability_failures.is_empty());
            assert!(additional_failures.is_empty());
            assert!(retirement_failures.is_empty());
            assert!(terminal_report.is_none());
            assert!(unread_owner_cancellation.is_none());
            assert_eq!(completion, Completion::Stopped);
            assert_eq!(descendants.len(), 1);
            let descendant = descendants.pop().expect("one actual declared child joined");
            let HostAccountConclusion::Account {
                origin: child_origin,
                terminal: child_terminal,
                executor: child_id,
            } = descendant
            else {
                panic!("the original declared child uses its total child projection");
            };
            drop(committed);
            let ActorRetirement::OwnerCancelled {
                behavior: child_behavior,
                interpretation,
                source,
                settlements,
                control,
                user,
                descendants,
                child_failures: (),
                capability_failures,
                additional_failures,
                received_interpretation,
                received_source,
                source_index,
                acquired_ingress,
                retirement_failures,
                terminal_report,
                unread_owner_cancellation,
            } = child_terminal
            else {
                panic!("root retirement joins its actually cancelled declared child");
            };
            assert_eq!(
                Arc::as_ptr(&child_behavior.base().original),
                original_child.as_ptr()
            );
            assert_eq!(child_behavior.base().original.as_slice(), [53, 59]);
            assert_eq!(child_behavior.base().admission, AccountAdmission::Ready);
            assert!(matches!(
                (
                    &interpretation,
                    &source,
                    &received_interpretation,
                    &received_source,
                    &source_index,
                    &acquired_ingress
                ),
                (None, None, None, None, None, None)
            ));
            assert_eq!(settlements, []);
            assert_eq!(control, []);
            assert_eq!(user, []);
            assert!(descendants.is_empty());
            assert!(capability_failures.is_empty());
            assert!(additional_failures.is_empty());
            assert!(retirement_failures.is_empty());
            assert!(terminal_report.is_none());
            assert!(unread_owner_cancellation.is_none());
            assert_eq!(Rc::as_ptr(&output), original_output.as_ptr());
            assert_eq!(output.as_slice(), [277, 281]);
            assert_eq!(borrowed.as_slice(), [283, 293]);
            let root_address = origin.address();
            let child_address = child_origin.address();
            drop((
                behavior,
                child_behavior,
                settlements,
                control,
                user,
                descendants,
                capability_failures,
                additional_failures,
                retirement_failures,
                terminal_report,
                output,
            ));
            (work_id, projected_id, child_id, root_address, child_address)
        });
    drop((actor_host, caller_host));
    assert_ne!(selected_id, caller_id);
    assert_eq!(constructed_id, selected_id);
    assert_eq!(
        child_id, selected_id,
        "standard child projection remains on the constructor-selected actor host"
    );
    assert_eq!(work_id, caller_id);
    assert_eq!(projected_id, caller_id);
    assert_eq!(root_address, MailAddr::APPLICATION_ROOT);
    assert_ne!(root_address, child_address);
    assert_eq!(original_root.strong_count(), 0);
    assert_eq!(original_child.strong_count(), 0);
    assert_eq!(original_role.strong_count(), 0);
    assert_eq!(original_output.strong_count(), 0);
}

#[test]
#[expect(
    clippy::too_many_lines,
    reason = "keep selected-host destruction, original uninvoked callable and exact task failure/control custody together before final discharge"
)]
fn destroyed_selected_host_preserves_uninvoked_work_and_untouched_control() {
    let selected_host = Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("the actual selected host builds");
    let caller_host = Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("the surviving caller host builds");
    let selected_id = selected_host.handle().id();
    let caller_id = caller_host.handle().id();
    let entered = selected_host.enter();
    let constructed_id = Handle::current().id();
    let root = Arc::new(vec![31, 37]);
    let original_root = Arc::downgrade(&root);
    let work = Rc::new(vec![307, 311]);
    let original_work = Rc::downgrade(&work);
    let invocations = Rc::new(Cell::new(0_usize));
    let observed_invocations = Rc::clone(&invocations);
    let borrowed = vec![313, 317];
    let borrowed_input = &borrowed;
    let pair = Application::new(
        ReceivingAccount {
            admission: AccountAdmission::Ready,
            original: root,
        }
        .stop_on_shutdown(),
    )
    .execute_with::<_, _, AccountConclusion, (), _, _>(move |application| {
        observed_invocations.set(observed_invocations.get() + 1);
        let work_id = Handle::current().id();
        async move {
            let requested = application.lifecycle().request_shutdown();
            requested.expect("the retried original callable requests actual new-root shutdown");
            (work, borrowed_input, work_id)
        }
    });
    let control_root = Arc::new(vec![31, 37]);
    let original_control_root = Arc::downgrade(&control_root);
    let control_work = Rc::new(vec![331, 337]);
    let original_control_work = Rc::downgrade(&control_work);
    let control_invocations = Rc::new(Cell::new(0_usize));
    let observed_control_invocations = Rc::clone(&control_invocations);
    let control = Application::new(
        ReceivingAccount {
            admission: AccountAdmission::Ready,
            original: control_root,
        }
        .stop_on_shutdown(),
    )
    .execute_with::<_, _, AccountConclusion, (), _, _>(move |application| {
        observed_control_invocations.set(observed_control_invocations.get() + 1);
        let work_id = Handle::current().id();
        async move {
            let requested = application.lifecycle().request_shutdown();
            requested.expect("the untouched original application receives its actual shutdown");
            (control_work, borrowed_input, work_id)
        }
    });
    drop(entered);
    let (execution, result) =
        pair.unwrap_or_else(|_| panic!("a real entered host constructed the cold pair"));
    let (control_execution, control_result) =
        control.unwrap_or_else(|_| panic!("the same live host constructed the untouched control"));
    // Actual Runtime destruction, outside an async context; a retained Handle is not a live Runtime.
    drop(selected_host);
    let before_poll = (
        original_root.strong_count(),
        original_work.strong_count(),
        original_control_root.strong_count(),
        original_control_work.strong_count(),
    );
    let (work_id, control_work_id) = caller_host.block_on(async {
        let ((), outcome) = tokio::join!(execution, result);
        let (ApplicationOutcome::NotInvoked {
            work,
            startup_error,
            cleanup: Err(ApplicationCleanupError::TaskFailed(cleanup_failure)),
        }, Err(root_closed), Err(notification_closed)) = outcome
        else {
            panic!(
                "destroyed selected host returns the uninvoked callable and actual root_receipt failure"
            );
        };
        assert!(cleanup_failure.is_cancelled());
        assert!(!cleanup_failure.is_panic());
        assert!(startup_error.is_some());
        assert_eq!(invocations.get(), 0);
        assert_eq!(original_work.strong_count(), 1);
        assert_eq!(original_root.strong_count(), 0);
        drop((cleanup_failure, startup_error, root_closed, notification_closed));
        // Explicit caller retry uses a genuine new application; the destroyed actor has no fabricated residual.
        let (execution, result) = paired_account(Arc::new(vec![31, 37]))
            .execute_with::<AccountConclusion, (), _, _>(work)
            .unwrap_or_else(|_| panic!("the surviving caller host receives original work"));
        let ((), outcome) = tokio::join!(execution, result);
        let (ApplicationOutcome::Completed {
            output: (output, borrowed, work_id),
            cleanup: Ok(()),
        }, root_receipt, Ok(ActorNotificationReceipts { termination: Ok(()), retirement: Ok(()) })) = outcome
        else {
            panic!("the original callable completes on the explicit new application");
        };
        assert_completed_account(&root_receipt);
        assert_eq!(Rc::as_ptr(&output), original_work.as_ptr());
        assert_eq!(output.as_slice(), [307, 311]);
        assert_eq!(borrowed.as_slice(), [313, 317]);
        assert_eq!(invocations.get(), 1);
        drop((output, root_receipt));
        // The second execution has never been polled or staged, even though its selected host is gone.
        drop(control_execution);
        let (ApplicationOutcome::Unstarted { application, work }, Err(_), Err(_)) = control_result.await else {
            panic!("destroyed host cannot turn untouched input into a begun application");
        };
        assert_eq!(control_invocations.get(), 0);
        assert_eq!(
            Arc::as_ptr(&application.root().base().original),
            original_control_root.as_ptr()
        );
        assert_eq!(application.root().base().original.as_slice(), [31, 37]);
        assert_eq!(application.root().base().admission, AccountAdmission::Ready);
        assert_eq!(original_control_work.strong_count(), 1);
        let (execution, result) = application
            .execute_with::<_, _, AccountConclusion, (), _, _>(work)
            .unwrap_or_else(|_| {
                panic!("the exact original application and work select the actual surviving host")
            });
        let ((), outcome) = tokio::join!(execution, result);
        let (ApplicationOutcome::Completed {
            output: (output, borrowed, control_work_id),
            cleanup: Ok(()),
        }, root_receipt, Ok(ActorNotificationReceipts { termination: Ok(()), retirement: Ok(()) })) = outcome
        else {
            panic!("the original untouched control runs after explicit retry");
        };
        assert_completed_account(&root_receipt);
        assert_eq!(Rc::as_ptr(&output), original_control_work.as_ptr());
        assert_eq!(output.as_slice(), [331, 337]);
        assert_eq!(borrowed.as_slice(), [313, 317]);
        assert_eq!(control_invocations.get(), 1);
        drop((output, root_receipt));
        (work_id, control_work_id)
    });
    drop(caller_host);
    assert_ne!(selected_id, caller_id);
    assert_eq!(constructed_id, selected_id);
    assert_eq!(before_poll, (1, 1, 1, 1));
    assert_eq!(work_id, caller_id);
    assert_eq!(control_work_id, caller_id);
    // No root-task identity, root residual or native cause was acquired or reconstructed.
    assert_eq!(original_root.strong_count(), 0);
    assert_eq!(original_work.strong_count(), 0);
    assert_eq!(original_control_root.strong_count(), 0);
    assert_eq!(original_control_work.strong_count(), 0);
}

#[test]
fn owned_blocking_retains_ready_output_native_fault_and_joined_root() {
    let runtime = Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("the owning runtime builds");
    let output = Rc::new(vec![173, 179]);
    let original_output = Rc::downgrade(&output);
    let root = Arc::new(vec![31, 37]);
    let original_root = Arc::downgrade(&root);
    let payload = Arc::new(vec![181, 191]);
    let original_payload = Arc::downgrade(&payload);
    let (execution, result) = {
        let entered = runtime.enter();
        let pair = paired_account(root)
            .execute_with::<AccountConclusion, (), _, _>(move |_| ReadyAccountWork {
                output: Some(output),
                panic_payload: Some(payload),
            })
            .unwrap_or_else(|_| panic!("the owning runtime is entered"));
        drop(entered);
        pair
    };
    // Only this original receiver is optional: the counterfactual consumes it.
    let mut result = Some(result);
    let execution_fault = catch_unwind(AssertUnwindSafe(|| runtime.block_on(execution)));
    let Err(payload) = execution_fault else {
        panic!("the original Ready work destructor panics natively");
    };
    let output_owned_before_join = original_output.strong_count();
    assert_eq!(output_owned_before_join, 1);
    let outcome = runtime.block_on(result.take().expect("the original receiver is retained"));
    let (
        ApplicationOutcome::Completed {
            output,
            cleanup: Ok(()),
        },
        root_receipt,
        Ok(ActorNotificationReceipts {
            termination: Ok(()),
            retirement: Ok(()),
        }),
    ) = outcome
    else {
        panic!("Ready was acquired before disposal of the work future");
    };
    assert_cancelled_account(&root_receipt);
    let Ok((_, ActorRetirement::OwnerCancelled { behavior, .. })) = &root_receipt else {
        panic!("the complete retirement oracle already established owner cancellation");
    };
    let exact_root = original_root
        .upgrade()
        .is_some_and(|original| Arc::ptr_eq(&original, &behavior.base().original));
    let exact_output = original_output
        .upgrade()
        .is_some_and(|original| Rc::ptr_eq(&original, &output));
    let contents = output.as_slice().to_vec();
    let cause_owned = original_payload.strong_count();
    let root_owned = original_root.strong_count();
    assert!(exact_output);
    assert!(exact_root);
    assert_eq!(contents, vec![173, 179]);
    assert_eq!(cause_owned, 1);
    assert_eq!(root_owned, 1);
    drop((output, root_receipt));
    let output_released = original_output.strong_count();
    let root_released = original_root.strong_count();
    let cause_still_owned = original_payload.strong_count();
    drop(payload);
    let cause_released = original_payload.strong_count();
    assert_eq!(output_released, 0);
    assert_eq!(root_released, 0);
    assert_eq!(cause_still_owned, 1);
    assert_eq!(cause_released, 0);
    // The acquired actor fact and native cause have been explicitly discharged.
    drop(runtime);
}

#[test]
fn owned_blocking_retains_unfinished_work_native_fault_and_joined_root() {
    let runtime = Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("the owning runtime builds");
    let work_value = Rc::new(vec![107, 109]);
    let original_work = Rc::downgrade(&work_value);
    let root = Arc::new(vec![31, 37]);
    let original_root = Arc::downgrade(&root);
    let panic_allocation = Arc::new(vec![197, 199]);
    let original_allocation = Arc::downgrade(&panic_allocation);
    // This opaque carrier belongs solely to the native Rust unwind boundary.
    let payload: Box<dyn Any + Send> = Box::new(panic_allocation);
    let original_panic = ptr::from_ref(payload.as_ref()).cast::<()>();
    let (execution, result) = {
        let entered = runtime.enter();
        let pair = paired_account(root)
            .execute_with::<AccountConclusion, (), _, _>(move |_| {
                let mut payload = Some(payload);
                poll_fn(move |_| -> Poll<Rc<Vec<u8>>> {
                    assert_eq!(work_value.as_slice(), &[107, 109]);
                    let payload = payload.take().expect("the original native panic is owned");
                    resume_unwind(payload)
                })
            })
            .unwrap_or_else(|_| panic!("the owning runtime is entered"));
        drop(entered);
        pair
    };
    let execution_fault = catch_unwind(AssertUnwindSafe(|| runtime.block_on(execution)));
    let Err(payload) = execution_fault else {
        panic!("the invoked unfinished work panics natively");
    };
    let received_panic = ptr::from_ref(payload.as_ref()).cast::<()>();
    let same_panic = original_panic == received_panic;
    let work_released_before_join = original_work.strong_count();
    let outcome = runtime.block_on(result);
    let (
        ApplicationOutcome::Interrupted { cleanup: Ok(()) },
        root_receipt,
        Ok(ActorNotificationReceipts {
            termination: Ok(()),
            retirement: Ok(()),
        }),
    ) = outcome
    else {
        panic!("no output or recoverable uninvoked callable was acquired");
    };
    assert_cancelled_account(&root_receipt);
    let Ok((_, ActorRetirement::OwnerCancelled { behavior, .. })) = &root_receipt else {
        panic!("the complete retirement oracle already established owner cancellation");
    };
    let exact_root = original_root
        .upgrade()
        .is_some_and(|original| Arc::ptr_eq(&original, &behavior.base().original));
    let cause_owned = original_allocation.strong_count();
    assert!(same_panic);
    assert!(exact_root);
    assert_eq!(work_released_before_join, 0);
    assert_eq!(cause_owned, 1);
    drop(root_receipt);
    let root_released = original_root.strong_count();
    let cause_still_owned = original_allocation.strong_count();
    drop(payload);
    let cause_released = original_allocation.strong_count();
    assert_eq!(root_released, 0);
    assert_eq!(cause_still_owned, 1);
    assert_eq!(cause_released, 0);
    drop(runtime);
}

// The advanced host accessor runs during setup, outside every Behavior fold.
struct UnwindingAccountSpaces {
    accounts: ActorSpace<ReceivingAccount>,
    hosting_failure: Mutex<Option<Box<dyn Any + Send>>>,
}

impl Hosts<ReceivingAccount> for UnwindingAccountSpaces {
    fn space(&self) -> &ActorSpace<ReceivingAccount> {
        let failure = self
            .hosting_failure
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .take();
        match failure {
            Some(failure) => resume_unwind(failure),
            None => &self.accounts,
        }
    }
}

#[test]
fn host_setup_unwind_keeps_uninvoked_work_until_receiver_discharge() {
    let runtime = Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("the real live host builds");
    let root = Arc::new(vec![31, 37]);
    let original_root = Arc::downgrade(&root);
    let cause = Arc::new(vec![347, 349]);
    let original_cause = Arc::downgrade(&cause);
    let cause: Box<dyn Any + Send> = Box::new(cause);
    let original_cause_object = ptr::from_ref(&*cause).cast::<()>();
    let work = Rc::new(vec![353, 359]);
    let original_work = Rc::downgrade(&work);
    let invocations = Rc::new(Cell::new(0_usize));
    let work_invocations = Rc::clone(&invocations);
    runtime.block_on(async {
        let application = App::new(
            ReceivingAccount {
                admission: AccountAdmission::Ready,
                original: root,
            }
            .stop_on_shutdown(),
            UnwindingAccountSpaces {
                accounts: ActorSpace::new(),
                hosting_failure: Mutex::new(Some(cause)),
            },
        );
        let (execution, receiver) = application
            .execute_with::<AccountConclusion, (), _, _>(move |application| {
                work_invocations.set(work_invocations.get() + 1);
                async move {
                    let requested = application.lifecycle().request_shutdown();
                    requested.expect("only an actually invoked Work requests shutdown");
                    work
                }
            })
            .unwrap_or_else(|_| panic!("the real host is entered"));
        let mut execution = Box::pin(execution);
        let execution_poll = poll_fn(|context| {
            Poll::Ready(catch_unwind(AssertUnwindSafe(|| {
                execution.as_mut().poll(context)
            })))
        })
        .await;
        drop(execution);
        let Err(cause) = execution_poll else {
            panic!("the actual supplied host accessor must raise its original native cause");
        };
        let received_cause_object = ptr::from_ref(&*cause).cast::<()>();
        let work_after_setup = original_work.strong_count();
        let root_after_setup = original_root.strong_count();
        let cause_after_setup = original_cause.strong_count();
        let invocations_after_setup = invocations.get();
        let mut receiver = Box::pin(receiver);
        let receiver_poll = poll_fn(|context| {
            Poll::Ready(catch_unwind(AssertUnwindSafe(|| {
                receiver.as_mut().poll(context)
            })))
        })
        .await;
        let work_after_receiving = original_work.strong_count();
        let root_after_receiving = original_root.strong_count();
        let receiver_unwound = receiver_poll.is_err();
        // Dispose actual acquired values and both original futures before assertions.
        drop(receiver_poll);
        drop(receiver);
        drop(cause);
        let work_after_discharge = original_work.strong_count();
        let root_after_discharge = original_root.strong_count();
        let cause_after_discharge = original_cause.strong_count();
        let host_reused = tokio::spawn(async { 367_u64 })
            .await
            .unwrap_or_else(|_| panic!("the same actual host remains live after the setup fault"));
        println!(
            "setup-native/receiver-unwound={receiver_unwound}/root={root_after_setup}/work={work_after_setup}->{work_after_receiving}->{work_after_discharge}"
        );
        assert_eq!(received_cause_object, original_cause_object);
        assert_eq!(invocations_after_setup, 0);
        assert_eq!(cause_after_setup, 1);
        assert_eq!(cause_after_discharge, 0);
        assert_eq!(work_after_setup, 1);
        assert_eq!(work_after_discharge, 0);
        assert_eq!(host_reused, 367);
        assert_eq!(root_after_setup, 1);
        assert_eq!(root_after_receiving, 1);
        assert_eq!(root_after_discharge, 0);
        assert_eq!(
            work_after_receiving, 1,
            "the surviving result receiver must retain original uninvoked Work until explicit receiver/result discharge"
        );
    });
}

#[test]
#[expect(
    clippy::too_many_lines,
    reason = "keep native host-setup cause identity, original uninvoked callable and real retry/disposal observations in one controller"
)]
fn host_setup_unwind_returns_original_uninvoked_work_for_real_retry() {
    let runtime = Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("the original live host builds");
    let root = Arc::new(vec![31, 37]);
    let original_root = Arc::downgrade(&root);
    let cause = Arc::new(vec![401, 409]);
    let original_cause = Arc::downgrade(&cause);
    let cause: Box<dyn Any + Send> = Box::new(cause);
    let original_cause_object = ptr::from_ref(&*cause).cast::<()>();
    let work = Rc::new(vec![373, 379]);
    let original_work = Rc::downgrade(&work);
    let borrowed = vec![389, 397];
    let invocations = Rc::new(Cell::new(0_usize));
    let work_invocations = Rc::clone(&invocations);
    runtime.block_on(async {
        let borrowed = borrowed.as_slice();
        let application = App::new(
            ReceivingAccount {
                admission: AccountAdmission::Ready,
                original: root,
            }
            .stop_on_shutdown(),
            UnwindingAccountSpaces {
                accounts: ActorSpace::new(),
                hosting_failure: Mutex::new(Some(cause)),
            },
        );
        let (execution, receiver) = application
            .execute_with::<AccountConclusion, (), _, _>(move |application| {
                work_invocations.set(work_invocations.get() + 1);
                async move {
                    let requested = application.lifecycle().request_shutdown();
                    requested.expect("the retried original Work requests actual shutdown");
                    (work, borrowed)
                }
            })
            .unwrap_or_else(|_| panic!("the original host is entered"));
        let mut execution = Box::pin(execution);
        let execution_poll = poll_fn(|context| {
            Poll::Ready(catch_unwind(AssertUnwindSafe(|| {
                execution.as_mut().poll(context)
            })))
        })
        .await;
        drop(execution);
        let Err(cause) = execution_poll else {
            panic!("the supplied host accessor raises its original native cause");
        };
        let received_cause_object = ptr::from_ref(&*cause).cast::<()>();
        let (
            ApplicationOutcome::Prepared {
                inputs: (prepared_root, prepared_spaces),
                work,
                cleanup: setup_cleanup,
            },
            Err(_),
            Err(_),
        ) = receiver.await
        else {
            panic!("setup failure returns the original prepared inputs and uninvoked callable");
        };
        // This Work-only controller deliberately surrenders the acquired prepared inputs.
        // The separate prepared-root witness checks their retained lifetime.
        drop((prepared_root, prepared_spaces));
        let Err(ApplicationCleanupError::PublicationClosed(publication_failure)) = setup_cleanup
        else {
            panic!("this exact pre-spawn cut returns the actual publication error");
        };
        let publication_failure: RecvError = publication_failure;
        let work_before_retry = original_work.strong_count();
        let cause_before_retry = original_cause.strong_count();
        let invocations_before_retry = invocations.get();
        let root_after_setup = original_root.strong_count();
        let retry_root = Arc::new(vec![31, 37]);
        let original_retry_root = Arc::downgrade(&retry_root);
        let (execution, receiver) = paired_account(retry_root)
            .execute_with::<AccountConclusion, (), _, _>(work)
            .unwrap_or_else(|_| panic!("the same original host permits a genuine new App"));
        let ((), outcome) = tokio::join!(execution, receiver);
        let (
            ApplicationOutcome::Completed {
                output,
                cleanup: Ok(()),
            },
            root_receipt,
            Ok(ActorNotificationReceipts {
                termination: Ok(()),
                retirement: Ok(()),
            }),
        ) = outcome
        else {
            panic!("the exact recovered callable completes once on the new App");
        };
        assert_completed_account(&root_receipt);
        let Ok((_, ActorRetirement::Completed { behavior, .. })) = &root_receipt else {
            panic!("the complete retry retirement retains its original root");
        };
        let exact_work = original_work
            .upgrade()
            .is_some_and(|original| Rc::ptr_eq(&original, &output.0));
        let exact_retry_root = original_retry_root
            .upgrade()
            .is_some_and(|original| Arc::ptr_eq(&original, &behavior.base().original));
        let exact_borrowed = ptr::eq(output.1, borrowed);
        let work_contents = output.0.as_slice().to_vec();
        let borrowed_contents = output.1.to_vec();
        let invocations_after_retry = invocations.get();
        let cause_after_retry = original_cause.strong_count();
        drop((output, root_receipt, publication_failure));
        let work_after_discharge = original_work.strong_count();
        let retry_root_after_discharge = original_retry_root.strong_count();
        let cause_before_discharge = original_cause.strong_count();
        drop(cause);
        let cause_after_discharge = original_cause.strong_count();
        println!("setup-root-disposal={root_after_setup}");
        assert_eq!(received_cause_object, original_cause_object);
        assert_eq!(invocations_before_retry, 0);
        assert_eq!(work_before_retry, 1);
        assert_eq!(cause_before_retry, 1);
        assert!(exact_work);
        assert!(exact_retry_root);
        assert!(exact_borrowed);
        assert_eq!(work_contents, vec![373, 379]);
        assert_eq!(borrowed_contents, vec![389, 397]);
        assert_eq!(invocations_after_retry, 1);
        assert_eq!(cause_after_retry, 1);
        assert_eq!(work_after_discharge, 0);
        assert_eq!(retry_root_after_discharge, 0);
        assert_eq!(cause_before_discharge, 1);
        assert_eq!(cause_after_discharge, 0);
    });
    drop(runtime);
}

#[test]
#[expect(
    clippy::too_many_lines,
    reason = "keep original prepared actor/spaces, native cause and uninvoked callable custody through the real same-input retry and final discharge"
)]
fn host_setup_unwind_preserves_prepared_inputs_for_same_actor_retry() {
    let runtime = Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("the original live host builds");
    let root = Arc::new(vec![31, 37]);
    let original_root = Arc::downgrade(&root);
    let original_root_allocation = Arc::as_ptr(&root);
    let accounts = ActorSpace::new();
    let original_scope = accounts.registration_scope_id();
    let cause = Arc::new(vec![419, 421]);
    let original_cause = Arc::downgrade(&cause);
    let cause: Box<dyn Any + Send> = Box::new(cause);
    let original_cause_object = ptr::from_ref(&*cause).cast::<()>();
    let work = Rc::new(vec![431, 433]);
    let original_work = Rc::downgrade(&work);
    let borrowed = vec![439, 443];
    let invocations = Rc::new(Cell::new(0_usize));
    let work_invocations = Rc::clone(&invocations);
    runtime.block_on(async {
        let borrowed = borrowed.as_slice();
        let application = App::new(
            ReceivingAccount {
                admission: AccountAdmission::Ready,
                original: root,
            }
            .stop_on_shutdown(),
            UnwindingAccountSpaces {
                accounts,
                hosting_failure: Mutex::new(Some(cause)),
            },
        );
        let (execution, receiver) = application
            .execute_with::<AccountConclusion, (), _, _>(move |application| {
                work_invocations.set(work_invocations.get() + 1);
                async move {
                    let requested = application.lifecycle().request_shutdown();
                    requested.expect("the retried original Work requests actual shutdown");
                    (work, borrowed)
                }
            })
            .unwrap_or_else(|_| panic!("the original host is entered"));
        let mut execution = Box::pin(execution);
        let execution_poll = poll_fn(|context| {
            Poll::Ready(catch_unwind(AssertUnwindSafe(|| {
                execution.as_mut().poll(context)
            })))
        })
        .await;
        drop(execution);
        let Err(cause) = execution_poll else {
            panic!("the supplied host accessor raises its original native cause");
        };
        let received_cause_object = ptr::from_ref(&*cause).cast::<()>();
        let (
            ApplicationOutcome::Prepared {
                inputs: (prepared_root, prepared_spaces),
                work,
                cleanup: setup_cleanup,
            },
            Err(_),
            Err(_),
        ) = receiver.await
        else {
            panic!("the receiver retains actual prepared inputs and original uninvoked Work");
        };
        let Err(ApplicationCleanupError::PublicationClosed(publication_failure)) = setup_cleanup
        else {
            panic!("the pre-handoff publication returns its actual receiving error");
        };
        let publication_failure: RecvError = publication_failure;
        let prepared_admission = prepared_root.base().admission;
        let prepared_values = prepared_root.base().original.as_slice().to_vec();
        let prepared_allocation = Arc::as_ptr(&prepared_root.base().original);
        let prepared_scope = prepared_spaces.accounts.registration_scope_id();
        let failure_consumed = prepared_spaces
            .hosting_failure
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .is_none();
        let root_before_retry = original_root.strong_count();
        let work_before_retry = original_work.strong_count();
        let cause_before_retry = original_cause.strong_count();
        let invocations_before_retry = invocations.get();
        // Materialize a new App value from the same acquired actor and changed Spaces.
        // This does not reconstruct an untouched declaration or consumed role.
        let (execution, receiver) = App::new(prepared_root, prepared_spaces)
            .execute_with::<AccountConclusion, (), _, _>(work)
            .unwrap_or_else(|_| panic!("the same original host permits acquired-input retry"));
        let ((), outcome) = tokio::join!(execution, receiver);
        let (
            ApplicationOutcome::Completed {
                output,
                cleanup: Ok(()),
            },
            root_receipt,
            Ok(ActorNotificationReceipts {
                termination: Ok(()),
                retirement: Ok(()),
            }),
        ) = outcome
        else {
            panic!("the exact acquired actor and original Work complete on retry");
        };
        assert_completed_account(&root_receipt);
        let Ok((_, ActorRetirement::Completed { behavior, .. })) = &root_receipt else {
            panic!("the full joined retry result retains the original actor");
        };
        let joined_allocation = Arc::as_ptr(&behavior.base().original);
        let exact_work = original_work
            .upgrade()
            .is_some_and(|original| Rc::ptr_eq(&original, &output.0));
        let exact_borrowed = ptr::eq(output.1, borrowed);
        let work_values = output.0.as_slice().to_vec();
        let borrowed_values = output.1.to_vec();
        let invocations_after_retry = invocations.get();
        let root_after_retry = original_root.strong_count();
        let cause_after_retry = original_cause.strong_count();
        drop((output, root_receipt, publication_failure));
        let root_after_discharge = original_root.strong_count();
        let work_after_discharge = original_work.strong_count();
        let cause_before_discharge = original_cause.strong_count();
        drop(cause);
        let cause_after_discharge = original_cause.strong_count();
        assert_eq!(received_cause_object, original_cause_object);
        assert_eq!(prepared_admission, AccountAdmission::Ready);
        assert_eq!(prepared_values, vec![31, 37]);
        assert_eq!(prepared_allocation, original_root_allocation);
        assert_eq!(prepared_scope, original_scope);
        assert!(failure_consumed);
        assert_eq!(root_before_retry, 1);
        assert_eq!(work_before_retry, 1);
        assert_eq!(cause_before_retry, 1);
        assert_eq!(invocations_before_retry, 0);
        assert_eq!(joined_allocation, original_root_allocation);
        assert!(exact_work);
        assert!(exact_borrowed);
        assert_eq!(work_values, vec![431, 433]);
        assert_eq!(borrowed_values, vec![439, 443]);
        assert_eq!(invocations_after_retry, 1);
        assert_eq!(root_after_retry, 1);
        assert_eq!(cause_after_retry, 1);
        assert_eq!(root_after_discharge, 0);
        assert_eq!(work_after_discharge, 0);
        assert_eq!(cause_before_discharge, 1);
        assert_eq!(cause_after_discharge, 0);
    });
    drop(runtime);
}

mod family_cleanup {
    use core::future::{Future, poll_fn};
    use core::num::NonZeroUsize;
    use core::pin::pin;
    use core::task::Poll;
    use std::any::Any;
    use std::panic::{AssertUnwindSafe, catch_unwind, resume_unwind};
    use std::ptr::from_ref;
    use std::sync::{Arc, Mutex, PoisonError, Weak};
    use std::thread;

    use behavior_actors::{Crash, Exit, ShutdownRequested, StopOnShutdown};
    use bombay::actors::ActorExt;
    use bombay::behavior::{
        ActionSettlement, Actions, BehaviorActed, BehaviorBase, Creations, EventLayer, Never,
        NoSends, Protocol, SendLayer, Step, Stopped,
    };
    use bombay::entity::{
        ActivationId, AdmissionFailure, DirectoryConfig, DrainFailure, EntityActivationError,
        EntityCapacity, EntityDefinition, EntityId, EntityMetrics, EntityShutdown,
    };
    use bombay::{
        ActorNotificationReceipts, ActorRetirement, ActorRetirementReport, ActorSpace, ActorSpaces,
        App, ApplicationCleanupError, ApplicationOutcome, Completion, MailAddr,
    };
    use tokio::runtime::{Builder, Handle};
    use tokio::sync::{OwnedSemaphorePermit, Semaphore, oneshot};
    use tokio::task::JoinError;

    struct ReceivingAccount {
        original: Arc<Vec<u8>>,
    }

    #[bombay::actor]
    #[allow(
        clippy::unnecessary_wraps,
        reason = "the fold computes only its typed Actions"
    )]
    impl ReceivingAccount {
        fn receive(&mut self, _: u64) -> BehaviorActed<Self> {
            Ok(Actions::cont())
        }
    }

    #[derive(ActorSpaces)]
    struct AccountSpaces {
        #[actor_space(ReceivingAccount)]
        accounts: ActorSpace<ReceivingAccount>,
    }

    struct AccountReplies;

    impl Protocol for AccountReplies {
        type Addr = MailAddr;
        type Msg = Never;
    }

    #[derive(Clone)]
    struct HeadAccounts(Arc<Vec<u8>>);

    #[expect(
        clippy::type_complexity,
        reason = "retain the native panic payload with its shared acquisition slot outside actor state and protocol routing"
    )]
    struct TailAccounts {
        original: Arc<Vec<u8>>,
        disposal_cause: Option<Arc<Mutex<Option<Box<dyn Any + Send>>>>>,
    }

    impl Clone for TailAccounts {
        fn clone(&self) -> Self {
            Self {
                original: Arc::clone(&self.original),
                // The installed original owns disposal; receptionist copies never gain it.
                disposal_cause: None,
            }
        }
    }

    impl Drop for TailAccounts {
        fn drop(&mut self) {
            if let Some(cause) = self.disposal_cause.take() {
                let original = cause.lock().unwrap_or_else(PoisonError::into_inner).take();
                if let Some(original) = original {
                    resume_unwind(original);
                }
            }
        }
    }

    struct PendingAccounts {
        hydration_started: Arc<Semaphore>,
        hydration_release: Arc<Semaphore>,
        completed_head: Option<oneshot::Sender<()>>,
    }

    impl Drop for PendingAccounts {
        fn drop(&mut self) {
            if let Some(completed_head) = self.completed_head.take() {
                let _publication = completed_head.send(());
            }
        }
    }

    impl EntityDefinition for PendingAccounts {
        type Id = u64;
        type Behavior = StopOnShutdown<ReceivingAccount>;
        type Hosts = AccountSpaces;
        type HydrationError = ();
        type Terminal = Never;
        type ChildFailures = ();

        async fn hydrate(&self, _: EntityId<u64>) -> Result<Self::Behavior, ()> {
            self.hydration_started.add_permits(1);
            let released = Arc::clone(&self.hydration_release).acquire_owned().await;
            match released {
                Ok(released) => released.forget(),
                Err(_) => return Err(()),
            }
            Ok(ReceivingAccount {
                original: Arc::new(vec![173]),
            }
            .stop_on_shutdown())
        }

        fn activation_failed(
            &self,
            _: EntityId<u64>,
            _: ActivationId,
            failure: EntityActivationError<(), Self::Behavior, Never, ()>,
        ) {
            // This concrete definition explicitly consumes its callback result.
            // The witness claims no callback completeness or incarnation retirement.
            drop(failure);
        }

        #[expect(
            clippy::drop_non_drop,
            reason = "the concrete Entity definition explicitly consumes this callback result; the witness claims no callback completeness"
        )]
        fn admission_refused(&self, _: EntityId<u64>, failure: AdmissionFailure<u64>) {
            drop(failure);
        }

        fn forced_retirement(&self, _: &EntityId<u64>, _: ActivationId, _: DrainFailure) {}

        fn retired(
            &self,
            _: &EntityId<u64>,
            _: ActivationId,
            retirement: Result<ActorRetirement<Self::Behavior, Never, ()>, JoinError>,
            retirement_report: ActorRetirementReport,
        ) {
            // This cancellation witness explicitly discharges both acquired callback facts;
            // it makes no claim about callback completeness or actor retirement.
            drop((retirement, retirement_report));
        }
    }

    #[expect(
        clippy::too_many_lines,
        clippy::manual_let_else,
        reason = "qualify the original retirement variant first, then inspect every shared typed field without splitting or weakening the complete root oracle"
    )]
    fn observe_original_root_retirement(
        retirement: &ActorRetirement<StopOnShutdown<ReceivingAccount>, Never, ()>,
        original_root: &Weak<Vec<u8>>,
    ) {
        // These are the selected owner equations for the actual pure root. An
        // accepted shutdown either folds to Stop, or remains the sole control input
        // when the distinct owner-cancellation source was acquired first.
        match retirement {
            ActorRetirement::Completed {
                completion,
                settlements,
                control,
                unread_owner_cancellation,
                ..
            } => {
                let expected_settlements = vec![ActionSettlement {
                    creations: Creations::empty(),
                    sends: SendLayer::new(NoSends, NoSends),
                    become_: Step::Stop(Stopped),
                }];
                assert_eq!(*completion, Completion::Stopped);
                assert_eq!(settlements, &expected_settlements);
                assert_eq!(control.as_slice(), []);
                // A shutdown win may leave a queued owner request unread. Keep
                // the actual closed fact in the whole report until final discharge.
                match unread_owner_cancellation {
                    None | Some(()) => {}
                }
            }
            ActorRetirement::OwnerCancelled {
                settlements,
                control,
                unread_owner_cancellation,
                ..
            } => {
                assert_eq!(settlements.as_slice(), []);
                assert_eq!(*unread_owner_cancellation, None);
                let [EventLayer::Owned(ShutdownRequested)] = control.as_slice() else {
                    panic!("owner cancellation retains the original unconsumed shutdown input");
                };
            }
            _ => panic!(
                "the pure original root must expose its whole stopped or owner-cancelled retirement"
            ),
        }
        let (
            behavior,
            interpretation,
            source,
            user,
            descendants,
            child_failures,
            capability_failures,
            additional_failures,
            received_interpretation,
            received_source,
            source_index,
            acquired_ingress,
            retirement_failures,
            terminal_report,
            unread_owner_cancellation,
        ) = match retirement {
            ActorRetirement::Completed {
                behavior,
                interpretation,
                source,
                settlements: _,
                control: _,
                user,
                descendants,
                child_failures,
                capability_failures,
                additional_failures,
                received_interpretation,
                received_source,
                source_index,
                acquired_ingress,
                retirement_failures,
                terminal_report,
                unread_owner_cancellation,
                completion: _,
            }
            | ActorRetirement::OwnerCancelled {
                behavior,
                interpretation,
                source,
                settlements: _,
                control: _,
                user,
                descendants,
                child_failures,
                capability_failures,
                additional_failures,
                received_interpretation,
                received_source,
                source_index,
                acquired_ingress,
                retirement_failures,
                terminal_report,
                unread_owner_cancellation,
            } => (
                behavior,
                interpretation,
                source,
                user,
                descendants,
                child_failures,
                capability_failures,
                additional_failures,
                received_interpretation,
                received_source,
                source_index,
                acquired_ingress,
                retirement_failures,
                terminal_report,
                unread_owner_cancellation,
            ),
            _ => unreachable!("the original typed retirement variant was already qualified"),
        };
        let root_identity = original_root
            .upgrade()
            .expect("the actual returned root remains alive");
        assert!(Arc::ptr_eq(&root_identity, &behavior.base().original));
        assert_eq!(behavior.base().original.as_slice(), &[31, 37]);
        drop(root_identity);
        assert!(interpretation.is_none());
        assert!(source.is_none());
        assert_eq!(user.as_slice(), []);
        assert_eq!(descendants.as_slice(), []);
        assert_eq!(*child_failures, ());
        assert!(capability_failures.is_empty());
        assert!(additional_failures.is_empty());
        assert!(received_interpretation.is_none());
        assert!(received_source.is_none());
        assert!(source_index.is_none());
        assert!(acquired_ingress.is_none());
        assert!(retirement_failures.is_empty());
        assert!(terminal_report.is_none());
        // Both actual closed cases are preserved in the original typed report;
        // the OwnerCancelled branch above proves its consumed one-shot separately.
        match unread_owner_cancellation {
            None | Some(()) => {}
        }
    }

    #[expect(
        clippy::too_many_lines,
        reason = "keep the complete ordered ownership trace in one observable regression; test length does not justify a new abstraction"
    )]
    async fn acquired_family_prefix_survives_host_cancellation(
        original_cause: Option<Box<dyn Any + Send>>,
        native_original: Option<Weak<Vec<u8>>>,
    ) {
        let cause_pointer = original_cause
            .as_ref()
            .map(|cause| from_ref::<dyn Any>(&**cause).cast::<()>());
        let disposal_cause = Arc::new(Mutex::new(None));
        let root = Arc::new(vec![31, 37]);
        let original_root = Arc::downgrade(&root);
        let head = Arc::new(vec![41, 43]);
        let original_head = Arc::downgrade(&head);
        let tail = Arc::new(vec![59, 61]);
        let original_tail = Arc::downgrade(&tail);
        let output = Arc::new(vec![11, 19]);
        let original_output = Arc::downgrade(&output);
        let hydration_started = Arc::new(Semaphore::new(0));
        let hydration_release = Arc::new(Semaphore::new(0));
        let (head_publication, acquired_head) = oneshot::channel();
        let capacity = EntityCapacity::new(NonZeroUsize::MIN, NonZeroUsize::MIN);
        let application = App::new(
            ReceivingAccount { original: root }.stop_on_shutdown(),
            AccountSpaces {
                accounts: ActorSpace::new(),
            },
        )
        .entity_family(
            TailAccounts {
                original: tail,
                disposal_cause: Some(Arc::clone(&disposal_cause)),
            },
            PendingAccounts {
                hydration_started: Arc::clone(&hydration_started),
                hydration_release: Arc::clone(&hydration_release),
                completed_head: None,
            },
            DirectoryConfig::default(),
            capacity,
        )
        .unwrap_or_else(|_| panic!("the concrete tail directory forms before host creation"))
        .entity_family(
            HeadAccounts(head),
            PendingAccounts {
                hydration_started: Arc::new(Semaphore::new(0)),
                hydration_release: Arc::new(Semaphore::new(1)),
                completed_head: Some(head_publication),
            },
            DirectoryConfig::default(),
            capacity,
        )
        .unwrap_or_else(|_| panic!("the concrete head directory forms before host creation"));
        let (host_publication, selected_host) = oneshot::channel();
        let (host_shutdown, shutdown_requested) = oneshot::channel();
        let host_thread = thread::spawn(move || {
            let runtime = Builder::new_current_thread()
                .enable_all()
                .build()
                .expect("the selected live host builds");
            runtime.block_on(async move {
                let published = host_publication.send(Handle::current());
                if published.is_ok() {
                    let shutdown = shutdown_requested.await;
                    drop(shutdown);
                }
            });
            // The real host destructor cancels its actual retained tasks.
            drop(runtime);
        });
        let selected_host = match selected_host.await {
            Ok(host) => host,
            Err(failure) => {
                let requested = host_shutdown.send(());
                let joined = tokio::task::spawn_blocking(move || host_thread.join()).await;
                drop((requested, joined));
                panic!("the selected host did not publish its handle: {failure}");
            }
        };
        let hydration_host = selected_host.clone();
        let selection_identity = original_tail
            .upgrade()
            .expect("the actual family owns its original role");
        let paired = {
            let entered = selected_host.enter();
            let paired = application.execute_with_entities::<Never, (), _, _>(
                move |application| async move {
                    let entities = application.entities(TailAccounts {
                        original: selection_identity,
                        disposal_cause: None,
                    });
                    let interface = application.interface(entities.entity(71));
                    let caller = interface.external::<AccountReplies>();
                    let admission = match caller {
                        Ok(caller) => {
                            // A Pending command is explicitly cancelled while the genuine
                            // shared hydration stays directory-owned. No command was accepted.
                            let admission = {
                                let mut admission = pin!(caller.send(interface.api(), 73));
                                poll_fn(|context| {
                                    let entered = hydration_host.enter();
                                    let progress = admission.as_mut().poll(context);
                                    drop(entered);
                                    Poll::Ready(progress)
                                })
                                .await
                            };
                            Ok(admission)
                        }
                        Err(failure) => Err(failure),
                    };
                    let hydrated = match &admission {
                        Ok(Poll::Pending) => Some(
                            Arc::clone(&hydration_started)
                                .acquire_owned()
                                .await
                                .map(OwnedSemaphorePermit::forget),
                        ),
                        Ok(Poll::Ready(_)) | Err(_) => None,
                    };
                    let stopped = application.lifecycle().request_shutdown();
                    (output, admission, hydrated, stopped)
                },
            );
            drop(entered);
            paired
        };
        let (execution, receiving) = match paired {
            Ok(paired) => paired,
            Err((application, work, failure)) => {
                let requested = host_shutdown.send(());
                let joined = tokio::task::spawn_blocking(move || host_thread.join()).await;
                drop((application, work, requested, joined));
                panic!("the live entered host rejected construction: {failure}");
            }
        };
        let close_host = async move {
            let acquired = acquired_head.await;
            // Head Definition Drop occurs only after its real shutdown+metrics;
            // this task cannot poll host stop until the cleanup poll yields at tail.
            if acquired.is_ok() {
                *disposal_cause
                    .lock()
                    .unwrap_or_else(PoisonError::into_inner) = original_cause;
            } else {
                drop(original_cause);
            }
            let requested = host_shutdown.send(());
            let joined = tokio::task::spawn_blocking(move || host_thread.join()).await;
            (acquired, requested, joined)
        };
        let execute = async move {
            let mut execution = pin!(execution);
            poll_fn(|context| {
                match catch_unwind(AssertUnwindSafe(|| execution.as_mut().poll(context))) {
                    Ok(Poll::Pending) => Poll::Pending,
                    Ok(Poll::Ready(())) => Poll::Ready(Ok(())),
                    Err(cause) => Poll::Ready(Err(cause)),
                }
            })
            .await
        };
        let (executed, (acquired, requested, joined)) = tokio::join!(execute, close_host);
        // Host and cleanup cancellation finish before any oracle can unwind.
        let (
            returned,
            root_receiving,
            Ok(ActorNotificationReceipts {
                termination: Ok(()),
                retirement: Ok(()),
            }),
            family_receiving,
        ) = receiving.await
        else {
            panic!("both original root notifications survive later cleanup cancellation");
        };
        assert!(executed.is_ok());
        assert!(acquired.is_ok());
        assert!(requested.is_ok());
        assert!(matches!(joined, Ok(Ok(()))));
        let ApplicationOutcome::Completed {
            output: (output, admission, hydrated, stopped),
            cleanup,
        } = returned
        else {
            panic!("actual supplied work must publish before selected-host cancellation");
        };
        assert_eq!(output.as_slice(), &[11, 19]);
        assert!(matches!(admission, Ok(Poll::Pending)));
        assert!(matches!(hydrated, Some(Ok(()))));
        assert!(stopped.is_ok());
        let failure = match cleanup {
            Err(ApplicationCleanupError::TaskFailed(failure)) => failure,
            other => {
                drop(other);
                panic!(
                    "the real pending cleanup task must retain its cancellation/disposal result"
                );
            }
        };
        let original_disposal_cause = if let Some(pointer) = cause_pointer {
            assert!(failure.is_panic());
            let cause = failure.into_panic();
            assert_eq!(from_ref::<dyn Any>(&*cause).cast::<()>(), pointer);
            Some(cause)
        } else {
            assert!(failure.is_cancelled());
            drop(failure);
            None
        };
        assert_eq!(
            (original_root.strong_count(), original_head.strong_count()),
            (1, 1),
            "already acquired original root and complete head shutdown custody must survive unfinished tail cancellation",
        );
        let (root_origin, root_report) = root_receiving.unwrap_or_else(|failure| {
            panic!("the original acquired root reply must survive: {failure}")
        });
        let root_report = match root_report {
            ActorRetirement::ActorTaskFailed(failure) => {
                panic!("the real pure root task must have joined: {failure}")
            }
            retirement => retirement,
        };
        assert_eq!(root_origin.address(), MailAddr::APPLICATION_ROOT);
        observe_original_root_retirement(&root_report, &original_root);
        let (head_receiving, (tail_receiving, ())) = family_receiving;
        let (HeadAccounts(head), (head_shutdown, head_metrics, head_disposal_failure)) =
            head_receiving.unwrap_or_else(|failure| {
                panic!("the original complete head report must survive: {failure}")
            });
        let head_identity = original_head
            .upgrade()
            .expect("the acquired original head role is alive");
        assert!(Arc::ptr_eq(&head_identity, &head));
        assert_eq!(head.as_slice(), &[41, 43]);
        drop(head_identity);
        let EntityShutdown::Settled {
            represented,
            entities,
        } = head_shutdown
        else {
            panic!("the actual zero-incarnation head owns its whole settled report");
        };
        assert_eq!(represented, 0);
        assert_eq!(entities, Vec::new());
        assert_eq!(
            head_metrics,
            EntityMetrics {
                activations: 0,
                hydration_failures: 0,
                launch_failures: 0,
                capacity_refusals: 0,
                forced_retirements: 0,
                peak_hydrations: 0,
                residents: 0,
            }
        );
        assert!(head_disposal_failure.is_none());
        let tail_failure = match tail_receiving {
            Err(failure) => failure,
            Ok(report) => {
                drop(report);
                panic!("the genuinely unfinished tail has no completed receiving report");
            }
        };
        match (original_disposal_cause.as_ref(), native_original.as_ref()) {
            (Some(_), Some(original)) => {
                assert_eq!(original.strong_count(), 1);
            }
            (None, None) => {}
            _ => panic!(
                "native cancellation cannot fabricate or erase its original cause allocation"
            ),
        }
        assert_eq!(original_output.strong_count(), 1);
        assert_eq!(original_tail.strong_count(), 0);
        // Concrete final receiving policy discharges every whole acquired owner,
        // each genuine receiving error and the original native cause once.
        drop((
            output,
            root_origin,
            root_report,
            head,
            entities,
            head_metrics,
            head_disposal_failure,
            tail_failure,
            original_disposal_cause,
            admission,
            hydrated,
            stopped,
            hydration_release,
        ));
        assert_eq!(original_root.strong_count(), 0);
        assert_eq!(original_head.strong_count(), 0);
        assert_eq!(original_output.strong_count(), 0);
        assert_eq!(original_tail.strong_count(), 0);
        if let Some(original) = native_original {
            assert_eq!(original.strong_count(), 0);
        }
    }

    #[tokio::test(flavor = "current_thread")]
    async fn acquired_root_and_head_survive_pending_family_host_cancellation() {
        acquired_family_prefix_survives_host_cancellation(None, None).await;
    }

    #[tokio::test(flavor = "current_thread")]
    async fn acquired_root_and_head_survive_native_pending_family_disposal() {
        let original = Arc::new(vec![67_u8, 71]);
        let native_original = Arc::downgrade(&original);
        let original: Box<dyn Any + Send> = Box::new(original);
        acquired_family_prefix_survives_host_cancellation(Some(original), Some(native_original))
            .await;
    }

    #[tokio::test(flavor = "current_thread")]
    #[expect(
        clippy::too_many_lines,
        reason = "keep the complete ordered ownership trace in one observable regression; test length does not justify a new abstraction"
    )]
    async fn surrendering_family_receiving_preserves_independent_root_observers() {
        let native = Arc::new(vec![67_u8, 71]);
        let original_native = Arc::downgrade(&native);
        let original_cause: Option<Box<dyn Any + Send>> = Some(Box::new(native));
        let disposal_cause = Arc::new(Mutex::new(None));
        let root = Arc::new(vec![31, 37]);
        let original_root = Arc::downgrade(&root);
        let head = Arc::new(vec![41, 43]);
        let original_head = Arc::downgrade(&head);
        let tail = Arc::new(vec![59, 61]);
        let original_tail = Arc::downgrade(&tail);
        let output = Arc::new(vec![11, 19]);
        let original_output = Arc::downgrade(&output);
        let hydration_started = Arc::new(Semaphore::new(0));
        let hydration_release = Arc::new(Semaphore::new(0));
        let (head_publication, acquired_head) = oneshot::channel();
        let (observation_publication, received_observations) = oneshot::channel();
        let capacity = EntityCapacity::new(NonZeroUsize::MIN, NonZeroUsize::MIN);
        let application = App::new(
            ReceivingAccount { original: root }.stop_on_shutdown(),
            AccountSpaces {
                accounts: ActorSpace::new(),
            },
        )
        .entity_family(
            TailAccounts {
                original: tail,
                disposal_cause: Some(Arc::clone(&disposal_cause)),
            },
            PendingAccounts {
                hydration_started: Arc::clone(&hydration_started),
                hydration_release: Arc::clone(&hydration_release),
                completed_head: None,
            },
            DirectoryConfig::default(),
            capacity,
        )
        .unwrap_or_else(|_| panic!("the concrete tail directory forms before host creation"))
        .entity_family(
            HeadAccounts(head),
            PendingAccounts {
                hydration_started: Arc::new(Semaphore::new(0)),
                hydration_release: Arc::new(Semaphore::new(1)),
                completed_head: Some(head_publication),
            },
            DirectoryConfig::default(),
            capacity,
        )
        .unwrap_or_else(|_| panic!("the concrete head directory forms before host creation"));
        let (host_publication, selected_host) = oneshot::channel();
        let (host_shutdown, shutdown_requested) = oneshot::channel();
        let host_thread = thread::spawn(move || {
            let runtime = Builder::new_current_thread()
                .enable_all()
                .build()
                .expect("the selected live host builds");
            runtime.block_on(async move {
                let published = host_publication.send(Handle::current());
                if published.is_ok() {
                    let shutdown = shutdown_requested.await;
                    drop(shutdown);
                }
            });
            // The real host destructor cancels its actual retained tasks.
            drop(runtime);
        });
        let selected_host = match selected_host.await {
            Ok(host) => host,
            Err(failure) => {
                let requested = host_shutdown.send(());
                let joined = tokio::task::spawn_blocking(move || host_thread.join()).await;
                drop((requested, joined));
                panic!("the selected host did not publish its handle: {failure}");
            }
        };
        let hydration_host = selected_host.clone();
        let selection_identity = original_tail
            .upgrade()
            .expect("the actual family owns its original role");
        let paired = {
            let entered = selected_host.enter();
            let paired = application.execute_with_entities::<Never, (), _, _>(
                move |application| async move {
                    let entities = application.entities(TailAccounts {
                        original: selection_identity,
                        disposal_cause: None,
                    });
                    let interface = application.interface(entities.entity(71));
                    let caller = interface.external::<AccountReplies>();
                    let admission = match caller {
                        Ok(caller) => {
                            // A Pending command is explicitly cancelled while the genuine
                            // shared hydration stays directory-owned. No command was accepted.
                            let admission = {
                                let mut admission = pin!(caller.send(interface.api(), 73));
                                poll_fn(|context| {
                                    let entered = hydration_host.enter();
                                    let progress = admission.as_mut().poll(context);
                                    drop(entered);
                                    Poll::Ready(progress)
                                })
                                .await
                            };
                            Ok(admission)
                        }
                        Err(failure) => Err(failure),
                    };
                    let hydrated = match &admission {
                        Ok(Poll::Pending) => Some(
                            Arc::clone(&hydration_started)
                                .acquire_owned()
                                .await
                                .map(OwnedSemaphorePermit::forget),
                        ),
                        Ok(Poll::Ready(_)) | Err(_) => None,
                    };
                    let lifecycle = application.lifecycle();
                    let published = observation_publication
                        .send((lifecycle.termination(), lifecycle.termination()));
                    drop(published);
                    let stopped = lifecycle.request_shutdown();
                    (output, admission, hydrated, stopped)
                },
            );
            drop(entered);
            paired
        };
        let (execution, receiving) = match paired {
            Ok(paired) => paired,
            Err((application, work, failure)) => {
                let requested = host_shutdown.send(());
                let joined = tokio::task::spawn_blocking(move || host_thread.join()).await;
                drop((application, work, requested, joined));
                panic!("the live entered host rejected construction: {failure}");
            }
        };
        let close_host = async move {
            let acquired = acquired_head.await;
            // This surrenders ONLY the final result future; the independent
            // observation futures remain separately owned outside the producer.
            drop(receiving);
            if acquired.is_ok() {
                *disposal_cause
                    .lock()
                    .unwrap_or_else(PoisonError::into_inner) = original_cause;
            } else {
                drop(original_cause);
            }
            let requested = host_shutdown.send(());
            let joined = tokio::task::spawn_blocking(move || host_thread.join()).await;
            let observations = received_observations.await;
            (acquired, requested, joined, observations)
        };
        let execute = async move {
            let mut execution = pin!(execution);
            poll_fn(|context| {
                match catch_unwind(AssertUnwindSafe(|| execution.as_mut().poll(context))) {
                    Ok(Poll::Pending) => Poll::Pending,
                    Ok(Poll::Ready(())) => Poll::Ready(Ok(())),
                    Err(cause) => Poll::Ready(Err(cause)),
                }
            })
            .await
        };
        let (executed, (acquired, requested, joined, observations)) =
            tokio::join!(execute, close_host);
        // Real H tasks/thread have finished before any observational assertion.
        let (first_observation, second_observation) = observations.unwrap_or_else(|failure| {
            panic!("the original independent observers were not published: {failure}")
        });
        let (first_observation, second_observation) =
            tokio::join!(first_observation, second_observation);
        assert!(executed.is_ok());
        assert!(acquired.is_ok());
        assert!(requested.is_ok());
        assert!(matches!(joined, Ok(Ok(()))));
        assert_eq!(first_observation, second_observation);
        match &first_observation {
            Ok(Exit::Normal) | Err(Crash::Cancelled) => {}
            _ => panic!(
                "surrendering a result receiver cannot replace the original stopped/cancelled root observation"
            ),
        }
        assert_eq!(original_root.strong_count(), 0);
        assert_eq!(original_head.strong_count(), 0);
        assert_eq!(original_tail.strong_count(), 0);
        assert_eq!(original_output.strong_count(), 0);
        assert_eq!(original_native.strong_count(), 0);
        // No cleanup JoinError is claimed after deliberately surrendering its
        // actual result owner. The two retained controls above inspect that fact.
        drop((first_observation, second_observation, hydration_release));
    }
}

#[test]
fn supplied_ready_unit_has_its_bare_original_output() {
    let runtime = Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("the actual host builds");
    runtime.block_on(async {
        let application = paired_account(Arc::new(vec![31, 37]));
        let (execution, receiving) = application
            .execute_with::<AccountConclusion, (), _, _>(move |application| async move {
                let requested = application.lifecycle().request_shutdown();
                requested.expect("the live root accepts shutdown");
            })
            .unwrap_or_else(|original| {
                drop(original);
                panic!("the actual host is entered");
            });
        let ((), outcome) = tokio::join!(execution, receiving);
        let (
            ApplicationOutcome::Completed {
                output,
                cleanup: Ok(()),
            },
            root_receipt,
            Ok(ActorNotificationReceipts {
                termination: Ok(()),
                retirement: Ok(()),
            }),
        ) = outcome
        else {
            panic!("actual supplied work retains its original acquired output");
        };
        let output: () = output;
        assert_completed_account(&root_receipt);
        let () = output;
        drop(root_receipt);
    });
}

#[test]
fn supplied_ready_work_error_has_its_bare_original_output() {
    let runtime = Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("the actual host builds");
    runtime.block_on(async {
        let application = paired_account(Arc::new(vec![31, 37]));
        let (execution, receiving) = application
            .execute_with::<AccountConclusion, (), _, _>(move |application| async move {
                let requested = application.lifecycle().request_shutdown();
                requested.expect("the live root accepts shutdown");
                Err("original work error")
            })
            .unwrap_or_else(|original| {
                drop(original);
                panic!("the actual host is entered");
            });
        let ((), outcome) = tokio::join!(execution, receiving);
        let (
            ApplicationOutcome::Completed {
                output,
                cleanup: Ok(()),
            },
            root_receipt,
            Ok(ActorNotificationReceipts {
                termination: Ok(()),
                retirement: Ok(()),
            }),
        ) = outcome
        else {
            panic!("actual supplied work retains its original acquired output");
        };
        let output: Result<(), &'static str> = output;
        assert_completed_account(&root_receipt);
        assert_eq!(output, Err("original work error"));
        drop(root_receipt);
    });
}

#[test]
fn supplied_ready_local_rc_has_its_bare_original_output() {
    let runtime = Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("the actual host builds");
    let original = Rc::new(vec![71, 73]);
    let output_owner = &original;
    runtime.block_on(async {
        let application = paired_account(Arc::new(vec![31, 37]));
        let (execution, receiving) = application
            .execute_with::<AccountConclusion, (), _, _>(move |application| async move {
                let requested = application.lifecycle().request_shutdown();
                requested.expect("the live root accepts shutdown");
                Rc::clone(output_owner)
            })
            .unwrap_or_else(|original| {
                drop(original);
                panic!("the actual host is entered");
            });
        let ((), outcome) = tokio::join!(execution, receiving);
        let (
            ApplicationOutcome::Completed {
                output,
                cleanup: Ok(()),
            },
            root_receipt,
            Ok(ActorNotificationReceipts {
                termination: Ok(()),
                retirement: Ok(()),
            }),
        ) = outcome
        else {
            panic!("actual supplied work retains its original acquired output");
        };
        let output: Rc<Vec<u64>> = output;
        assert_completed_account(&root_receipt);
        let same = Rc::ptr_eq(&original, &output);
        assert!(same);
        assert_eq!(output.as_slice(), &[71, 73]);
        drop(root_receipt);
    });
}

#[test]
fn supplied_ready_borrowed_has_its_bare_original_output() {
    let runtime = Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("the actual host builds");
    let values = [79, 83];
    let borrowed = values.as_slice();
    runtime.block_on(async {
        let application = paired_account(Arc::new(vec![31, 37]));
        let (execution, receiving) = application
            .execute_with::<AccountConclusion, (), _, _>(move |application| async move {
                let requested = application.lifecycle().request_shutdown();
                requested.expect("the live root accepts shutdown");
                borrowed
            })
            .unwrap_or_else(|original| {
                drop(original);
                panic!("the actual host is entered");
            });
        let ((), outcome) = tokio::join!(execution, receiving);
        let (
            ApplicationOutcome::Completed {
                output,
                cleanup: Ok(()),
            },
            root_receipt,
            Ok(ActorNotificationReceipts {
                termination: Ok(()),
                retirement: Ok(()),
            }),
        ) = outcome
        else {
            panic!("actual supplied work retains its original acquired output");
        };
        let output: &[u64] = output;
        assert_completed_account(&root_receipt);
        assert_eq!(output, &[79, 83]);
        drop(root_receipt);
    });
}

#[test]
fn supplied_ready_user_none_has_its_bare_original_output() {
    let runtime = Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("the actual host builds");
    runtime.block_on(async {
        let application = paired_account(Arc::new(vec![31, 37]));
        let (execution, receiving) = application
            .execute_with::<AccountConclusion, (), _, _>(move |application| async move {
                let requested = application.lifecycle().request_shutdown();
                requested.expect("the live root accepts shutdown");
                None
            })
            .unwrap_or_else(|original| {
                drop(original);
                panic!("the actual host is entered");
            });
        let ((), outcome) = tokio::join!(execution, receiving);
        let (
            ApplicationOutcome::Completed {
                output,
                cleanup: Ok(()),
            },
            root_receipt,
            Ok(ActorNotificationReceipts {
                termination: Ok(()),
                retirement: Ok(()),
            }),
        ) = outcome
        else {
            panic!("actual supplied work retains its original acquired output");
        };
        let output: Option<()> = output;
        assert_completed_account(&root_receipt);
        assert_eq!(output, None);
        drop(root_receipt);
    });
}
