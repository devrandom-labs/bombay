use bombay::WorkOutcome;
use bombay::{ActorCustody, ActorNotificationReceipts};
use core::convert::Infallible;
use core::time::Duration;
use tokio::runtime::Builder;

use behavior_actors::diagnostic::DiagnosticDisposition;
use behavior_actors::lifecycle::ActorDrainPolicy;
use behavior_actors::supervisor::roster::{
    self, FailureReaction, FixedCommand, FixedSupervisor, Recovery, RecoveryPolicy, Strategy,
};
use behavior_actors::worker::{
    OrderedRoles, RestartLimit, RestartRelease, StartupLimit, StartupPlan, WorkerSource,
    WorkerSubmission,
};
use behavior_actors::{Exit, StopOnShutdown};
use bombay::actors::ActorExt as _;
use bombay::behavior::Never;
use bombay::composition::replies::ExactReplies;
use bombay::prelude::{ActorRetirement, Completion, MailAddr, RootOrigin};
use bombay::{Application, WorkerPreparationSource, WorkerPreparationStart};
use tokio::sync::oneshot;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum WorkerRole {
    Primary,
}

struct Worker;

#[bombay::actor(message = Never)]
impl Worker {}

type ManagedWorker = StopOnShutdown<Worker>;

struct ActivationNotice(Option<oneshot::Sender<()>>);

impl StartupPlan for ActivationNotice {
    type Ready = ();
    type Rejection = Never;

    #[expect(
        clippy::unused_async_trait_impl,
        reason = "Defer trait-port work and owned inputs until the future is polled."
    )]
    async fn start(self) -> Result<Self::Ready, Self::Rejection> {
        if let Some(sender) = self.0 {
            sender
                .send(())
                .unwrap_or_else(|()| panic!("the application boundary awaits activation"));
        }
        Ok(())
    }
}

struct Workshop;

impl WorkerSource<WorkerRole, ManagedWorker, ActivationNotice> for Workshop {
    type WorkerRejection = Never;
    type SourceRejection = Never;
}

impl WorkerPreparationSource<WorkerRole, ManagedWorker, ActivationNotice> for Workshop {
    #[expect(
        clippy::unused_async_trait_impl,
        reason = "Defer trait-port work and owned inputs until the future is polled."
    )]
    async fn prepare_first(
        &mut self,
        _: &WorkerRole,
    ) -> WorkerPreparationStart<ManagedWorker, ActivationNotice, Never, Never> {
        WorkerPreparationStart::Submitted(WorkerSubmission::with_startup(
            Worker.stop_on_shutdown(),
            ActivationNotice(None),
        ))
    }

    #[expect(
        clippy::unused_async_trait_impl,
        reason = "Defer trait-port work and owned inputs until the future is polled."
    )]
    async fn prepare_next(
        &mut self,
        _: &WorkerRole,
    ) -> Result<WorkerSubmission<ManagedWorker, ActivationNotice>, Never> {
        Ok(WorkerSubmission::with_startup(
            Worker.stop_on_shutdown(),
            ActivationNotice(None),
        ))
    }
}

type Supervisor = FixedSupervisor<
    WorkerRole,
    ManagedWorker,
    ActivationNotice,
    Workshop,
    Infallible,
    Infallible,
    ExactReplies,
    ExactReplies,
>;
type RootSupervisor = StopOnShutdown<Supervisor>;

bombay::topology! {
    supervisor_application {
        root { Proxy => proxy; }
        proxy { Worker => worker; }
        worker {}
    }
}

fn prepared_supervisor(activation_sender: oneshot::Sender<()>) -> Supervisor {
    let mut initial_activation = Some(activation_sender);
    let roles =
        OrderedRoles::new(WorkerRole::Primary, []).expect("the single declared role is unique");
    let release = RestartRelease::constant(Duration::from_millis(1))
        .expect("the positive release interval is valid");
    roster::new(
        move |_: &WorkerRole| {
            let sender = initial_activation
                .take()
                .expect("the sole initial role owns one activation notice");
            WorkerSubmission::with_startup(
                Worker.stop_on_shutdown(),
                ActivationNotice(Some(sender)),
            )
        },
        roles,
        roster::Config {
            startup: StartupLimit::new(1).expect("one activation is positive capacity"),
            recovery: Recovery::permanent(
                Workshop,
                RecoveryPolicy {
                    strategy: Strategy::OneForOne,
                    limit: RestartLimit::new(1, Duration::from_secs(30)),
                    delay: release,
                },
            ),
            failure: FailureReaction::StopSupervisor,
            shutdown: ActorDrainPolicy::WaitForActorGraph,
            diagnostics: DiagnosticDisposition::terminate(),
        },
    )
    .build()
    .unwrap_or_else(|_| panic!("the sole worker is prepared"))
}

#[test]
fn fixed_supervisor_executes_activation_and_retires_its_proxy_tree() {
    let (activation_sender, activation_received) = oneshot::channel();
    let supervisor = prepared_supervisor(activation_sender);
    let spaces = supervisor_application::Topology::new();
    let application_host = Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("the synchronous caller owns its explicit current-thread host");
    let application_outcome = application_host
        .block_on(
            Application::with_topology(supervisor.stop_on_shutdown(), spaces).run_with(
                move |application| async move {
                    activation_received
                        .await
                        .expect("the proxy activates its exact worker");
                    let caller = application
                        .sender()
                        .expect("the supervisor caller is established");
                    caller
                        .send(FixedCommand::shutdown())
                        .await
                        .expect("the supervisor accepts shutdown");
                    tokio::time::timeout(Duration::from_secs(5), application.termination())
                        .await
                        .expect("the supervisor terminates after shutdown")
                },
            ),
        )
        .unwrap_or_else(|failed| {
            drop(failed);
            panic!("the supervisor runs its worker and shuts down");
        })
        .into_parts();
    drop(application_host);
    let (
        WorkOutcome::Completed {
            output: termination,
            cleanup: Ok(()),
        },
        _root_report,
        Ok((root_origin, joined_actor)),
        Ok(ActorNotificationReceipts {
            termination: Ok(()),
            retirement: Ok(()),
        }),
        (),
    ) = application_outcome
    else {
        panic!("the original completed Work and joined root remain independently owned");
    };
    let origin = root_origin;
    let terminal = match joined_actor {
        ActorRetirement::ActorTaskFailed(failure) => {
            panic!("the actual application actor task failed: {failure}")
        }
        retirement => retirement,
    };

    assert_eq!(termination, Ok(Exit::Normal));
    assert_orderly_supervisor_terminal(origin, terminal);
}

fn assert_orderly_supervisor_terminal(
    origin: RootOrigin<RootSupervisor>,
    terminal: ActorRetirement<RootSupervisor, supervisor_application::RootRetirements<RootSupervisor>, Vec<<supervisor_application::RootRetirements<RootSupervisor> as bombay::RetirementSchema>::Failure>>,
) {
    assert_eq!(origin.address(), MailAddr::APPLICATION_ROOT);
    let ActorRetirement::Completed {
        custody:
            ActorCustody {
                child_failures,
                capability_failures,
                unread_owner_cancellation,
                descendants,
                ..
            },
        completion,
        ..
    } = terminal
    else {
        panic!("the supervisor completes after its proxy tree");
    };
    assert!(capability_failures.is_empty());
    assert!(unread_owner_cancellation.is_none());
    assert!(child_failures.is_empty());
    assert_eq!(completion, Completion::Stopped);
    assert_eq!(descendants.len(), 1);
    let supervisor_application::RootRetirements {
        origin,
        retirement: terminal,
    } = descendants
        .into_iter()
        .next()
        .expect("the declared role retains its proxy");
    assert_ne!(origin.address(), MailAddr::APPLICATION_ROOT);
    let ActorRetirement::Completed {
        custody:
            ActorCustody {
                child_failures,
                capability_failures,
                unread_owner_cancellation,
                descendants,
                ..
            },
        completion,
        ..
    } = *terminal
    else {
        panic!("the supervisor completes its proxy during shutdown");
    };
    assert!(capability_failures.is_empty());
    assert!(unread_owner_cancellation.is_none());
    assert!(child_failures.is_empty());
    assert_eq!(completion, Completion::Stopped);
    assert_eq!(descendants.len(), 1);
    let supervisor_application::ProxyRetirements {
        origin,
        retirement: terminal,
    } = descendants
        .into_iter()
        .next()
        .expect("the activated worker remains in retirement custody");
    assert_ne!(origin.address(), MailAddr::APPLICATION_ROOT);
    let ActorRetirement::Completed {
        custody:
            ActorCustody {
                child_failures,
                capability_failures,
                unread_owner_cancellation,
                descendants,
                ..
            },
        completion,
        ..
    } = *terminal
    else {
        panic!("the proxy completes its worker during shutdown");
    };
    assert_eq!(child_failures, []);
    assert!(capability_failures.is_empty());
    assert!(unread_owner_cancellation.is_none());
    assert_eq!(completion, Completion::Stopped);
    assert_eq!(descendants, []);
}
