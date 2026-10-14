//! Run one fixed supervisor through worker failure, replacement, and shutdown.
//! The supervisor owns recovery policy; Bombay interprets its typed actions.

use bombay::ActorCustody;
use bombay::ApplicationOutcome;
use bombay::atomic::{FixedSupervisor, StableProxy};
use bombay::behavior::{Behavior, BirthMode, ChildHead, CreationKind};
use bombay::{
    ApplicationTopology, ChildFailure, ChildOrigin, Hosts, NativeChild, ProjectTerminal,
    RetirementSchema, RootRegistration,
};
use core::convert::Infallible;
use core::time::Duration;
use tokio::runtime::Builder;

use bombay::actors::ActorExt as _;
use bombay::atomic::{
    ActivationPlan, ActivationPolicy, ActorDrainPolicy, CapabilityResult, DiagnosticDisposition,
    FailureReaction, FixedCommand, FixedSnapshot, OrderedRoles, Recovery, RestartLimit,
    RestartRelease, Strategy, WorkerSource, WorkerSubmission, fixed,
};
use bombay::behavior::{Actions, BehaviorActed, MessageProtocol, Never};
use bombay::prelude::{ActorRetirement, Completion, Exit, MailAddr, StopOnShutdown};
use bombay::{Application, WorkerPreparationSource, WorkerPreparationStart};
use tokio::sync::mpsc;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum WorkerRole {
    Primary,
}

enum WorkerCommand {
    Stop,
}

struct Worker;

#[bombay::actor]
impl Worker {
    fn receive(&mut self, message: WorkerCommand) -> BehaviorActed<Self> {
        match message {
            WorkerCommand::Stop => Ok(Actions::stop()),
        }
    }
}

type ManagedWorker = StopOnShutdown<Worker>;

struct ActivationNotice {
    role: WorkerRole,
    activated: mpsc::UnboundedSender<WorkerRole>,
}

impl ActivationPlan for ActivationNotice {
    type Ready = ();
    type Rejection = Never;

    #[expect(
        clippy::unused_async_trait_impl,
        reason = "Defer trait-port work and owned inputs until the future is polled."
    )]
    async fn activate(self) -> Result<Self::Ready, Self::Rejection> {
        self.activated
            .send(self.role)
            .expect("the application observes each worker activation");
        Ok(())
    }
}

struct Workshop {
    activated: mpsc::UnboundedSender<WorkerRole>,
}

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
        role: &WorkerRole,
    ) -> WorkerPreparationStart<ManagedWorker, ActivationNotice, Never, Never> {
        WorkerPreparationStart::Submitted(worker_submission(*role, &self.activated))
    }

    #[expect(
        clippy::unused_async_trait_impl,
        reason = "Defer trait-port work and owned inputs until the future is polled."
    )]
    async fn prepare_next(
        &mut self,
        role: &WorkerRole,
    ) -> Result<WorkerSubmission<ManagedWorker, ActivationNotice>, Never> {
        Ok(worker_submission(*role, &self.activated))
    }
}

fn worker_submission(
    role: WorkerRole,
    activated: &mpsc::UnboundedSender<WorkerRole>,
) -> WorkerSubmission<ManagedWorker, ActivationNotice> {
    WorkerSubmission::activated(
        Worker.stop_on_shutdown(),
        ActivationNotice {
            role,
            activated: activated.clone(),
        },
    )
}

type Status = MessageProtocol<MailAddr, FixedSnapshot<Worker>>;
type Capability = MessageProtocol<MailAddr, CapabilityResult<WorkerRole, Worker>>;

bombay::topology! {
    supervisor_application {
        root { Proxy => proxy; }
        proxy { Worker => worker; }
        worker {}
        hosts { status: Status; capability: Capability; }
    }
}

#[expect(
    clippy::too_many_lines,
    reason = "one complete replacement and drain trace preserves original incarnation and terminal custody"
)]
fn run_supervision<Spaces, RootDescendants, ProxyDescendants>(
    spaces: Spaces,
    primary_proxy: impl FnOnce(RootDescendants) -> (ProxyOrigin, Box<ProxyTerminal<ProxyDescendants>>),
    mut worker: impl FnMut(ProxyDescendants) -> (WorkerOrigin, Box<WorkerTerminal>),
) where
    RootDescendants: RetirementSchema<Failure = ChildFailure<ProxyOrigin, NativeProxy>>
        + ProjectTerminal<ProxyOrigin, NativeProxy, Descendants = ProxyDescendants>
        + Send
        + 'static,
    ProxyDescendants: RetirementSchema<Failure = ChildFailure<WorkerOrigin, NativeWorker>>
        + ProjectTerminal<WorkerOrigin, NativeWorker, Descendants = Never>
        + Send
        + 'static,
    Spaces: ApplicationTopology<ManagedSupervisor, (), Descendants = RootDescendants>
        + RootRegistration<<ManagedSupervisor as Behavior>::Protocol>
        + Hosts<Status>
        + Hosts<Capability>
        + NativeChild<ManagedSupervisor, ChildHead, NativeProxy, RootDescendants, ProxyDescendants>
        + NativeChild<NativeProxy, ChildHead, NativeWorker, ProxyDescendants, Never>,
{
    let (activated, mut activations) = mpsc::unbounded_channel();
    let supervisor = {
        let roles =
            OrderedRoles::new(WorkerRole::Primary, []).expect("the one-role roster is distinct");
        let initial_activated = activated.clone();
        fixed(
            move |role: &WorkerRole| worker_submission(*role, &initial_activated),
            roles,
            ActivationPolicy::new(1).expect("one worker fits the activation policy"),
            Recovery::permanent(
                Workshop { activated },
                Strategy::OneForOne,
                RestartLimit::new(1, Duration::from_secs(30)),
                RestartRelease::immediate(),
            ),
            FailureReaction::StopSupervisor,
            ActorDrainPolicy::WaitForActorGraph,
            DiagnosticDisposition::terminate(),
        )
        .build()
        .unwrap_or_else(|_| panic!("the declared worker is prepared"))
    };
    let application_host = Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("the synchronous caller owns its explicit current-thread host");
    let (application_outcome, native, notifications) = application_host
        .block_on(
            Application::with_topology(supervisor.stop_on_shutdown(), spaces).run_with(
                move |application| async move {
                    let first = tokio::time::timeout(Duration::from_secs(5), activations.recv())
                        .await
                        .expect("the first worker activates")
                        .expect("the first activation has a role");
                    assert_eq!(first, WorkerRole::Primary);

                    let supervisor = application.root().established_recipient();
                    let mut caller = application
                        .external::<Capability>()
                        .expect("the capability caller is established");
                    caller
                        .send(
                            &supervisor,
                            FixedCommand::capability(WorkerRole::Primary, caller.recipient()),
                        )
                        .await
                        .expect("the supervisor accepts the capability query");
                    let reply = caller
                        .receive()
                        .await
                        .expect("the primary capability is returned");
                    let CapabilityResult::Ready { role, proxy } = reply.message else {
                        panic!("the primary proxy is ready")
                    };
                    assert_eq!(role, WorkerRole::Primary);
                    caller
                        .send(&proxy, WorkerCommand::Stop)
                        .await
                        .expect("the first worker accepts its stop command");

                    let replacement =
                        tokio::time::timeout(Duration::from_secs(5), activations.recv())
                            .await
                            .expect("the replacement activates")
                            .expect("the replacement activation has a role");
                    assert_eq!(replacement, WorkerRole::Primary);
                    caller
                        .send(&supervisor, FixedCommand::shutdown())
                        .await
                        .expect("the supervisor accepts shutdown");
                    tokio::time::timeout(Duration::from_secs(5), application.termination())
                        .await
                        .expect("the supervisor retires after replacement and shutdown")
                },
            ),
        )
        .unwrap_or_else(|failed| {
            drop(failed);
            panic!("the supervisor runs its recovery policy");
        });
    drop(application_host);
    let ApplicationOutcome::Completed {
        output: termination,
        cleanup: Ok(()),
    } = application_outcome
    else {
        panic!("the original completed Work and joined root remain independently owned");
    };
    let notifications = notifications.expect("both original notification receipts survive");
    notifications
        .termination
        .expect("actual ordinary termination notification");
    notifications
        .retirement
        .expect("actual joined retirement notification");
    let (root_origin, joined_actor) =
        native.expect("the exact root retirement remains independent");
    assert_eq!(termination, Ok(Exit::Normal));
    let origin = root_origin;
    let terminal = joined_actor;
    assert_eq!(origin.address(), MailAddr::APPLICATION_ROOT);
    let ActorRetirement::Completed {
        custody:
            ActorCustody {
                capability_failures,
                unread_owner_cancellation,
                descendants,
                ..
            },
        completion,
        ..
    } = terminal
    else {
        panic!("the supervisor returns completed terminal custody")
    };
    assert_eq!(capability_failures.len(), 0);
    assert!(unread_owner_cancellation.is_none());
    assert_eq!(completion, Completion::Stopped);
    assert_eq!(descendants.len(), 1);
    let original_proxy = descendants
        .into_iter()
        .next()
        .expect("the primary role retains its proxy");
    let (origin, terminal) = primary_proxy(original_proxy);
    let terminal = *terminal;
    assert_ne!(origin.address(), MailAddr::APPLICATION_ROOT);
    let ActorRetirement::Completed {
        custody:
            ActorCustody {
                capability_failures,
                unread_owner_cancellation,
                descendants,
                ..
            },
        completion,
        ..
    } = terminal
    else {
        panic!("the proxy returns both worker incarnations")
    };
    assert_eq!(capability_failures.len(), 0);
    assert!(unread_owner_cancellation.is_none());
    assert_eq!(completion, Completion::Stopped);
    assert_eq!(descendants.len(), 2);
    let mut previous_creation = None;
    for descendant in descendants {
        let (origin, terminal) = worker(descendant);
        let kind = origin.creation_kind();
        match kind {
            CreationKind::Birth => assert_eq!(previous_creation, None),
            CreationKind::Replacement { previous } => {
                assert_eq!(previous_creation, Some(previous));
            }
        }
        let current_creation = origin.creation_id();
        assert_ne!(Some(current_creation), previous_creation);
        previous_creation = Some(current_creation);
        let terminal = *terminal;
        assert_ne!(origin.address(), MailAddr::APPLICATION_ROOT);
        let ActorRetirement::Completed {
            custody:
                ActorCustody {
                    capability_failures,
                    unread_owner_cancellation,
                    descendants,
                    ..
                },
            completion,
            ..
        } = terminal
        else {
            panic!("the worker returns completed terminal custody")
        };
        assert_eq!(capability_failures.len(), 0);
        assert!(unread_owner_cancellation.is_none());
        assert_eq!(completion, Completion::Stopped);
        assert_eq!(descendants.len(), 0);
    }
}

#[test]
fn supervisor_replaces_and_retires_native_worker() {
    run_supervision(
        supervisor_application::Topology::new(),
        |supervisor_application::RootRetirements { origin, retirement }| (origin, retirement),
        |supervisor_application::ProxyRetirements { origin, retirement }| (origin, retirement),
    );
}

#[cfg(test)]
mod tests {
    use super::{WorkerRole, Workshop, worker_submission};
    use bombay::atomic::ProxyPhase;
    use bombay::atomic::{
        ActivationPolicy, ActorDrainPolicy, DiagnosticDisposition, FailureReaction, OrderedRoles,
        Recovery, RestartLimit, RestartRelease, Strategy, fixed,
    };
    use bombay::behavior::{CreationKind, Step};
    use bombay::prelude::Activate as _;
    use core::time::Duration;
    use tokio::sync::mpsc;

    #[test]
    fn fixed_supervisor_initialization_preserves_role_and_correlation() {
        let (activated, _) = mpsc::unbounded_channel();
        let supervisor = {
            let roles = OrderedRoles::new(WorkerRole::Primary, [])
                .expect("the one-role roster is distinct");
            let initial_activated = activated.clone();
            fixed(
                move |role: &WorkerRole| worker_submission(*role, &initial_activated),
                roles,
                ActivationPolicy::new(1).expect("one worker fits the activation policy"),
                Recovery::permanent(
                    Workshop { activated },
                    Strategy::OneForOne,
                    RestartLimit::new(1, Duration::from_secs(30)),
                    RestartRelease::immediate(),
                ),
                FailureReaction::StopSupervisor,
                ActorDrainPolicy::WaitForActorGraph,
                DiagnosticDisposition::terminate(),
            )
            .build()
            .unwrap_or_else(|_| panic!("the declared worker is prepared"))
        };
        let initialized = supervisor
            .initialize()
            .unwrap_or_else(|_| panic!("the fixed supervisor initializes its stable proxy"));
        assert_eq!(initialized.actions.creates.len(), 1);
        let proxy = initialized
            .actions
            .creates
            .iter()
            .next()
            .expect("the primary role owns one stable proxy");
        let creation = proxy.id();
        assert_eq!(proxy.kind(), CreationKind::Birth);
        assert_eq!(proxy.child().phase(), ProxyPhase::Dormant);
        assert_eq!(initialized.actions.sends.proxy_observations.len(), 1);
        assert_eq!(
            initialized.actions.sends.proxy_observations[0].child,
            creation
        );
        assert_eq!(initialized.actions.become_, Step::Continue);
    }
}

type Supervisor =
    FixedSupervisor<WorkerRole, ManagedWorker, ActivationNotice, Workshop, Infallible, Infallible>;
type ManagedSupervisor = StopOnShutdown<Supervisor>;
type NativeProxy = StableProxy<ManagedWorker, ActivationNotice>;
type NativeWorker = <<NativeProxy as Behavior>::Birth as BirthMode>::Child;
type ProxyOrigin = ChildOrigin<Supervisor, ChildHead>;
type WorkerOrigin = ChildOrigin<NativeProxy, ChildHead>;
type ProxyTerminal<Descendants> =
    ActorRetirement<NativeProxy, Descendants, Vec<<Descendants as RetirementSchema>::Failure>>;
type WorkerTerminal = ActorRetirement<NativeWorker, Never, Vec<Never>>;

#[path = "supervision/topology_comparison.rs"]
mod topology_comparison;
