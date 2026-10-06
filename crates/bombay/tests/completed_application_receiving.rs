use core::future::{Future, poll_fn};
use core::mem;
use core::num::NonZeroUsize;
use core::pin::{Pin, pin};
use core::task::{Context, Poll};
use std::panic::{AssertUnwindSafe, catch_unwind, panic_any};
use std::rc::Rc;
use std::sync::{Arc, Mutex, PoisonError, Weak};

use behavior_actors::{Exit, StopOnShutdown};
use bombay::behavior::{
    Actions, BehaviorActed, BehaviorBase, ChildChoice, ChildCons, CreationKind,
    EstablishedDelivery, EstablishedRecipient, EventLayer, Never, NoChildren, NoSends, Protocol,
    Step, User,
};
use bombay_engine::Completion;
use tokio::runtime::Builder;
use tokio::sync::oneshot;
use tokio::task::JoinError;

use bombay::actors::ActorExt;
use bombay::entity::{
    ActivationId, AdmissionFailure, DirectoryConfig, DrainFailure, EntityActivationError,
    EntityCapacity, EntityDefinition, EntityId, EntityMetrics, EntityShutdown,
};
use bombay::{
    ActorRetirement, ApplicationBehavior, ApplicationDefinitionError, ApplicationOutcome,
    ApplicationStagingError, ChildFailure, ChildOrigin, ProjectTerminal, RootOrigin,
    TerminalProjection,
};
use bombay::{ActorSpace, ActorSpaces, App, Application, MailAddr, RunError, actor};

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
    ) {
        drop(retirement);
        panic!("no incarnation is requested by this startup controller");
    }
}

fn assert_rejected_root(
    outcome: &Result<((), AccountConclusion), RunError<AccountRefusal, AccountConclusion>>,
) {
    let Err(RunError::Unpublished(AccountConclusion::Root { origin, terminal })) = outcome else {
        panic!("the actual pure initialization refusal remains unpublished");
    };
    let ActorRetirement::InitializationRejected {
        behavior,
        error,
        control,
        user,
        descendants,
        child_failures: (),
        capability_failures,
        unread_owner_cancellation,
    } = terminal
    else {
        panic!("the complete actual initialization refusal remains owned");
    };
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

#[test]
fn completed_output_survives_actual_root_terminal_conversion_failure() {
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
    let (output, origin, joined) = application
        .run_with::<AccountConclusion, _, _, _, _>(move |application| async move {
            let requested = application.lifecycle().request_shutdown();
            requested.expect("the actual live root accepts shutdown");
            output
        })
        .expect("the actual runtime and root startup succeed");
    let retirement = joined.unwrap_or_else(|_| panic!("the actual root joined normally"));
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
    } = &retirement
    else {
        panic!("same standard root completed normally");
    };
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
    let (outcome, shutdowns) = application
        .run_with_entities::<AccountConclusion, _, _, _, ()>(|_| async {
            panic!("startup refusal leaves original work uninvoked");
        })
        .expect("the actual configured runtime builds");
    let outcome = match outcome {
        Err(RunError::Unpublished((origin, retirement))) => Err(RunError::Unpublished(
            AccountConclusion::project(origin, retirement),
        )),
        outcome => {
            drop(outcome);
            panic!("actual root startup refuses before publication");
        }
    };
    assert_rejected_root(&outcome);
    let (returned_role, (shutdown, metrics, family_disposal_failure), ()) = shutdowns;
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
    drop((outcome, returned_role, entities));
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

#[test]
fn application_startup_refusal_returns_whole_unprojected_root() {
    let original = Arc::new(vec![31, 37]);
    let retained = Arc::downgrade(&original);
    let outcome = Application::new(
        ReceivingAccount {
            admission: AccountAdmission::Reject,
            original,
        }
        .stop_on_shutdown(),
    )
    .run_with::<AccountConclusion, _, _, _, _, _, ()>(|_| async {
        panic!("actual initialization refusal leaves this work uninvoked");
    });
    let outcome = match outcome {
        Err(RunError::Unpublished((origin, retirement))) => Err(RunError::Unpublished(
            AccountConclusion::project(origin, retirement),
        )),
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

#[test]
fn declared_application_refusal_retains_unstarted_original_child() {
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
    let outcome = application.run_with::<DeclaredAccountConclusion, _, _, _, _, _, ()>(|_| async {
        panic!("the refused root never invokes application work");
    });
    let (origin, retirement) = match outcome {
        Err(RunError::Unpublished(original)) => original,
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
        },
    ) = &returned
    else {
        panic!("the actual composed actor retains complete initialization rejection");
    };
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
    cleanup: &Result<
        (
            RootOrigin<StopOnShutdown<ReceivingAccount>>,
            Result<
                ActorRetirement<StopOnShutdown<ReceivingAccount>, AccountConclusion, ()>,
                JoinError,
            >,
        ),
        JoinError,
    >,
) {
    let Ok((
        origin,
        Ok(ActorRetirement::Completed {
            behavior,
            settlements,
            control,
            user,
            descendants,
            child_failures: (),
            capability_failures,
            unread_owner_cancellation,
            completion,
        }),
    )) = cleanup
    else {
        panic!("the standard account and cleanup join normally");
    };
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
    cleanup: &Result<
        (
            RootOrigin<StopOnShutdown<ReceivingAccount>>,
            Result<
                ActorRetirement<StopOnShutdown<ReceivingAccount>, AccountConclusion, ()>,
                JoinError,
            >,
        ),
        JoinError,
    >,
) {
    let Ok((
        origin,
        Ok(ActorRetirement::OwnerCancelled {
            behavior,
            settlements,
            control,
            user,
            descendants,
            child_failures: (),
            capability_failures,
            unread_owner_cancellation,
        }),
    )) = cleanup
    else {
        panic!("the original authority cancels the still-live account");
    };
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
        let pair = application.execute_with::<AccountConclusion, (), _, _, _>(
            move |application| async move {
                let requested = application.lifecycle().request_shutdown();
                requested.expect("the live root accepts shutdown");
                (output, borrowed)
            },
        );
        let (execution, result) =
            pair.unwrap_or_else(|_| panic!("the original entered executor is available"));
        let ((), result) = tokio::join!(execution, result);
        let ApplicationOutcome::Completed { output, cleanup } = result else {
            panic!("the original caller-local work completes");
        };
        assert_completed_account(&cleanup);
        let exact = original
            .upgrade()
            .is_some_and(|original| Rc::ptr_eq(&original, &output.0));
        let contents = output.0.as_slice().to_vec();
        let borrowed_contents = output.1.as_slice().to_vec();
        let retained = original.strong_count();
        drop((output, cleanup));
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
            .execute_with::<AccountConclusion, (), _, _, _>(move |application| async move {
                let requested = application.lifecycle().request_shutdown();
                requested.expect("the retried live root accepts shutdown");
                original
            })
            .unwrap_or_else(|_| panic!("the original entered executor is available"));
        drop(execution);
        let ApplicationOutcome::Unstarted { application, work } = result.await else {
            panic!("before-first-poll drop returns the whole original inputs");
        };
        let owned_before_retry = retained.strong_count();
        let (execution, result) = application
            .execute_with::<AccountConclusion, (), _, _, _>(work)
            .unwrap_or_else(|_| panic!("the exact original inputs retry in the live executor"));
        let ((), result) = tokio::join!(execution, result);
        let ApplicationOutcome::Completed { output, cleanup } = result else {
            panic!("the recovered original callable runs on the recovered original application");
        };
        assert_completed_account(&cleanup);
        let Ok((_, Ok(ActorRetirement::Completed { behavior, .. }))) = &cleanup else {
            panic!("the recovered application owns its original root");
        };
        let exact_root = state_retained
            .upgrade()
            .is_some_and(|original| Arc::ptr_eq(&original, &behavior.base().original));
        let exact = retained
            .upgrade()
            .is_some_and(|original| Rc::ptr_eq(&original, &output));
        let contents = output.as_slice().to_vec();
        drop((output, cleanup));
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
            .execute_with::<AccountConclusion, (), _, _, _>(move |application| async move {
                let requested = application.lifecycle().request_shutdown();
                requested.expect("the new live root accepts shutdown");
                callable
            })
            .unwrap_or_else(|_| panic!("the original entered executor is available"));
        let mut execution = Box::pin(execution);
        let first_poll = poll_fn(|context| Poll::Ready(execution.as_mut().poll(context))).await;
        drop(execution);
        let ApplicationOutcome::NotInvoked {
            work,
            startup_error,
            cleanup,
        } = result.await
        else {
            panic!("the current-thread first poll starts the task but cannot yet acquire startup");
        };
        assert_cancelled_account(&cleanup);
        let callable_retained = retained.strong_count();
        drop(cleanup);
        let (execution, result) = paired_account(Arc::new(vec![31, 37]))
            .execute_with::<AccountConclusion, (), _, _, _>(work)
            .unwrap_or_else(|_| panic!("the original callable runs against a genuine new handle"));
        let ((), result) = tokio::join!(execution, result);
        let ApplicationOutcome::Completed { output, cleanup } = result else {
            panic!("the recovered original callable completes on the new actor");
        };
        assert_completed_account(&cleanup);
        let exact = retained
            .upgrade()
            .is_some_and(|original| Rc::ptr_eq(&original, &output));
        let contents = output.as_slice().to_vec();
        drop((output, cleanup));
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
            .execute_with::<AccountConclusion, (), _, _, _>(move |_| async move {
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
        let ApplicationOutcome::Interrupted { cleanup } = outcome else {
            panic!("invoked unfinished work is surrendered, not reconstructed");
        };
        assert_cancelled_account(&cleanup);
        drop(cleanup);
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
            .execute_with::<AccountConclusion, (), _, _, _>(move |application| async move {
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
            .execute_with::<AccountConclusion, (), _, _, _>(move |application| async move {
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
        let ApplicationOutcome::Completed { output, cleanup } = outcome else {
            panic!("the original work survives root completion and then completes");
        };
        assert_completed_account(&cleanup);
        let exact = retained
            .upgrade()
            .is_some_and(|original| Rc::ptr_eq(&original, &output));
        let contents = output.as_slice().to_vec();
        drop((output, cleanup));
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
        application.execute_with::<AccountConclusion, (), _, _, _>(move |application| async move {
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
            .execute_with::<AccountConclusion, (), _, _, _>(work)
            .unwrap_or_else(|_| {
                panic!("the exact original input product retries in the entered executor")
            });
        let ((), outcome) = tokio::join!(execution, result);
        let ApplicationOutcome::Completed { output, cleanup } = outcome else {
            panic!("the recovered original callable completes");
        };
        assert_completed_account(&cleanup);
        let exact = retained
            .upgrade()
            .is_some_and(|original| Rc::ptr_eq(&original, &output));
        let contents = output.as_slice().to_vec();
        drop((output, cleanup));
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
            .execute_with::<AccountConclusion, (), _, _, _>(move |application| async move {
                let requested = application.lifecycle().request_shutdown();
                requested.expect("the real retry root accepts shutdown");
                callable
            })
            .unwrap_or_else(|_| panic!("the entered executor is available"));
        let ((), outcome) = tokio::join!(execution, result);
        let ApplicationOutcome::NotInvoked {
            work,
            startup_error,
            cleanup,
        } = outcome
        else {
            panic!("actual initialization refusal preserves the uninvoked callable");
        };
        let Ok((
            origin,
            Ok(ActorRetirement::InitializationRejected {
                behavior,
                error,
                control,
                user,
                descendants,
                child_failures: (),
                capability_failures,
                unread_owner_cancellation,
            }),
        )) = &cleanup
        else {
            panic!("the complete original initialization refusal remains available");
        };
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
        drop(cleanup);
        let root_released = root_retained.strong_count();
        let (execution, result) = paired_account(Arc::new(vec![31, 37]))
            .execute_with::<AccountConclusion, (), _, _, _>(work)
            .unwrap_or_else(|_| {
                panic!("the exact recovered callable retries on an actual new root")
            });
        let ((), outcome) = tokio::join!(execution, result);
        let ApplicationOutcome::Completed { output, cleanup } = outcome else {
            panic!("the original callable completes on its real retry");
        };
        assert_completed_account(&cleanup);
        let exact_callable = retained
            .upgrade()
            .is_some_and(|original| Rc::ptr_eq(&original, &output));
        let contents = output.as_slice().to_vec();
        drop((output, cleanup));
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
            .execute_with::<AccountConclusion, (), _, _, _>(move |application| async move {
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
        let ApplicationOutcome::Completed { output, cleanup } = outcome else {
            panic!("the same original receiver retains completed caller-local output");
        };
        assert_completed_account(&cleanup);
        let exact = retained
            .upgrade()
            .is_some_and(|original| Rc::ptr_eq(&original, &output));
        let contents = output.as_slice().to_vec();
        drop((output, cleanup));
        let released = retained.strong_count();
        requested.expect("the live root accepts the actual cleanup request");
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
            .execute_with::<AccountConclusion, (), _, _, _>(move |_| ReadyAccountWork {
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
        let ApplicationOutcome::Completed { output, cleanup } = result else {
            panic!("the already acquired Ready output survives W disposal and real actor cleanup");
        };
        assert_cancelled_account(&cleanup);
        let Err(payload) = execution_fault else {
            panic!("the caller receives the original work destructor fault independently");
        };
        let exact_output = original_output
            .upgrade()
            .is_some_and(|original| Rc::ptr_eq(&original, &output));
        let contents = output.as_slice().to_vec();
        let output_owned = original_output.strong_count();
        let cause_owned = original_payload.strong_count();
        drop((output, cleanup));
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
    panic_payload: Option<Arc<Vec<u8>>>,
}

impl Drop for RetiringAccounts {
    fn drop(&mut self) {
        if let Some(original) = self.panic_payload.take() {
            panic_any(original);
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
    ) {
        Accounts.retired(id, activation, retirement);
    }
}

#[expect(
    clippy::too_many_lines,
    reason = "the original three-family disposal oracle keeps the complete acquired root, output, named family products and final discharge cuts in one trace"
)]
fn family_report_disposal_preserves_acquired_products(panic_payload: Option<Arc<Vec<u8>>>) {
    let original_fault = panic_payload.as_ref().map(Arc::downgrade);
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
        let products = application
            .run_with_entities::<AccountConclusion, _, _, _, _>(move |application| async move {
                let requested = application.lifecycle().request_shutdown();
                requested.expect("the actual live root accepts shutdown");
                output
            })
            .expect("the actual configured runtime builds");
        returned = Some(products);
    }));
    // The blocking public call has joined its runtime before every oracle.
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
            Ok((
                output,
                origin,
                Ok(ActorRetirement::Completed {
                    behavior,
                    settlements,
                    control,
                    user,
                    descendants,
                    child_failures: (),
                    capability_failures,
                    unread_owner_cancellation,
                    completion,
                }),
            )),
            (
                head,
                (
                    EntityShutdown::Settled {
                        represented: head_represented,
                        entities: head_entities,
                    },
                    head_metrics,
                    head_family_disposal_failure,
                ),
                (
                    middle,
                    (
                        EntityShutdown::Settled {
                            represented: middle_represented,
                            entities: middle_entities,
                        },
                        middle_metrics,
                        middle_family_disposal_failure,
                    ),
                    (
                        tail,
                        (
                            EntityShutdown::Settled {
                                represented: tail_represented,
                                entities: tail_entities,
                            },
                            tail_metrics,
                            tail_family_disposal_failure,
                        ),
                        (),
                    ),
                ),
            ),
        )) => {
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
                ),
            ))
        }
        _ => None,
    };
    let public_call_returned = caught.is_ok();
    let fault_retained = original_fault.as_ref().map(Weak::strong_count);
    let fault_contents = original_fault.as_ref().and_then(|original| {
        original.upgrade().map(|original| {
            let contents = original.as_slice().to_vec();
            drop(original);
            contents
        })
    });
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
    assert_eq!(
        fault_contents,
        original_fault.as_ref().map(|_| vec![67, 71])
    );
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
            (false, original_fault.is_some(), false),
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
    original_fault: Option<Arc<Vec<u8>>>,
}

impl Drop for DeclaredAccountRole {
    fn drop(&mut self) {
        if let Some(original) = self.original_fault.take() {
            panic_any(original);
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
    let fault = original_fault.as_ref().map(Arc::downgrade);
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
    let mut returned = None;
    let caught = catch_unwind(AssertUnwindSafe(|| {
        returned = Some(
            application.run_with::<StagedAccountConclusion, _, _, _, _, _, ()>(
                move |_| async move {
                    drop(work);
                    panic!("the original refused root must leave work uninvoked");
                },
            ),
        );
    }));
    // A faulting role is disposed before Runtime/ActorSpace construction. In
    // the control, the real root refusal has joined before the public return.
    let mut cold_inputs = None;
    let mut cold_metadata = None;
    match returned.take() {
        Some(Err(RunError::StagingRejected {
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
        })) => {
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
    let complete_refusal = match returned.as_ref() {
        Some(Err(RunError::Unpublished((
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
            },
        )))) => {
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
    let fault_contents = fault
        .as_ref()
        .and_then(|fault| fault.upgrade().map(|original| original.as_slice().to_vec()));
    drop(cold_inputs);
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
    // every omission oracle. The normal synchronous refusal still consumes F;
    // cold staging returns the original F and only explicit discharge releases it.
    assert_eq!(fault_owned, fault.as_ref().map(|_| 1));
    assert_eq!(fault_contents, fault.as_ref().map(|_| vec![229, 233]));
    assert_eq!(fault_released, fault.as_ref().map(|_| 0));
    if fault.is_none() {
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
            .execute_with::<_, _, DeclaredAccountConclusion, _, _, _, _>(move |_| async move {
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
            .execute_with::<_, _, DeclaredAccountConclusion, _, _, _, _>(move |_| async move {
                (work, borrowed)
            })
            .unwrap_or_else(|_| panic!("the original executor is entered"));
        let ((), outcome) = tokio::join!(execution, result);
        let ApplicationOutcome::NotInvoked {
            work,
            startup_error,
            cleanup,
        } = outcome
        else {
            panic!("the actual refused root never invokes the original callable");
        };
        let Ok((
            origin,
            Ok(ActorRetirement::InitializationRejected {
                behavior,
                error,
                control,
                user,
                descendants,
                child_failures: (child_failures, ()),
                capability_failures,
                unread_owner_cancellation,
            }),
        )) = &cleanup
        else {
            panic!("both real joins return the complete declared initialization refusal");
        };
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
        drop((work, cleanup, startup_error));
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
    let cause = Arc::new(vec![229, 233]);
    let original_cause = Arc::downgrade(&cause);
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
            .execute_with::<_, _, StagedAccountConclusion, _, _, _, _>(move |_| async move {
                (work, borrowed)
            })
            .unwrap_or_else(|_| panic!("the original executor is entered"));
        let ((), outcome) = tokio::join!(execution, result);
        let ApplicationOutcome::StagingRejected {
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
        } = outcome
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
        let cause_contents = original_cause
            .upgrade()
            .map(|cause| cause.as_slice().to_vec());
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
        assert_eq!(cause_contents, Some(vec![229, 233]));
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
    ) {
        self.retired
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .push((*id, activation, retirement));
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
        let products = application
            .run_with_entities::<AccountConclusion, _, _, _, _>(move |application| async move {
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
            })
            .expect("the actual configured host runtime builds");
        returned = Some(products);
    }));
    // The blocking host has completed its actual root and family joins before these observations.
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
    let (id, activation, joined) = row;
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
        } => {
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
        } => {
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
            Ok((
                (
                    output,
                    admitted,
                    ready,
                    shutdown,
                    termination,
                    retirements_before_work_completion,
                ),
                origin,
                Ok(ActorRetirement::Completed {
                    behavior,
                    settlements,
                    control,
                    user,
                    descendants,
                    child_failures: (),
                    capability_failures,
                    unread_owner_cancellation,
                    completion,
                }),
            )),
            (
                role,
                (
                    EntityShutdown::Settled {
                        represented,
                        entities,
                    },
                    metrics,
                    disposal_failure,
                ),
                (),
            ),
        )) => {
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
