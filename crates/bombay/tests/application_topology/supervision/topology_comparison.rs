//! Handwritten native edge composition for the same real replacement journey.
use super::{
    Capability, ManagedSupervisor, NativeProxy, NativeWorker, ProxyOrigin, ProxyTerminal, Status,
    WorkerOrigin, WorkerTerminal, run_supervision, supervisor_application,
};
use bombay::behavior::{
    Behavior, ChildCreationOutcome, ChildHead, CreationId, CreationKind, CreationRejection,
    ItemSettlement, Never, RoutedCreation,
};
use bombay::{
    ActorSpace, ApplicationTopology, ChildFailure, EstablishNativeChild, Hosts, MailAddr,
    NativeChild, NativeHost, PendingChild, ProjectTerminal, RetirementSchema, RootRegistration,
    Runtime,
};
use core::future::Future;

type SupervisorProtocol = <ManagedSupervisor as Behavior>::Protocol;
struct SupervisorHosts {
    root: ActorSpace<SupervisorProtocol>,
    status: ActorSpace<Status>,
    capability: ActorSpace<Capability>,
}

impl NativeHost for SupervisorHosts {
    type Runtime = Runtime;
    fn runtime(&self) -> &Self::Runtime {
        const RUNTIME: Runtime = Runtime::new();
        &RUNTIME
    }
}
impl SupervisorHosts {
    fn new() -> Self {
        Self {
            root: ActorSpace::new(),
            status: ActorSpace::new(),
            capability: ActorSpace::new(),
        }
    }
}
impl Hosts<Status> for SupervisorHosts {
    fn space(&self) -> &ActorSpace<Status> {
        &self.status
    }
}
impl Hosts<Capability> for SupervisorHosts {
    fn space(&self) -> &ActorSpace<Capability> {
        &self.capability
    }
}
impl RootRegistration<SupervisorProtocol> for SupervisorHosts {
    fn root_space(&self) -> &ActorSpace<SupervisorProtocol> {
        &self.root
    }
}
struct WorkerDescendant {
    origin: WorkerOrigin,
    retirement: Box<WorkerTerminal>,
}
impl RetirementSchema for WorkerDescendant {
    type Failure = ChildFailure<WorkerOrigin, NativeWorker>;
}
impl ProjectTerminal<WorkerOrigin, NativeWorker> for WorkerDescendant {
    type Descendants = Never;
    fn project(origin: WorkerOrigin, retirement: WorkerTerminal) -> Self {
        Self {
            origin,
            retirement: Box::new(retirement),
        }
    }
}
struct ProxyDescendant {
    origin: ProxyOrigin,
    retirement: Box<ProxyTerminal<WorkerDescendant>>,
}
impl RetirementSchema for ProxyDescendant {
    type Failure = ChildFailure<ProxyOrigin, NativeProxy>;
}
impl ProjectTerminal<ProxyOrigin, NativeProxy> for ProxyDescendant {
    type Descendants = WorkerDescendant;
    fn project(origin: ProxyOrigin, retirement: ProxyTerminal<WorkerDescendant>) -> Self {
        Self {
            origin,
            retirement: Box::new(retirement),
        }
    }
}
impl ApplicationTopology<ManagedSupervisor, ()> for SupervisorHosts {
    type Descendants = ProxyDescendant;
}
impl NativeChild<ManagedSupervisor, ChildHead, NativeProxy, ProxyDescendant, WorkerDescendant>
    for SupervisorHosts
{
    fn registration(&self) -> ActorSpace<<NativeProxy as Behavior>::Protocol> {
        ActorSpace::new()
    }
    fn establish<Origin, OriginFn>(
        pending: PendingChild<
            '_,
            ManagedSupervisor,
            Self,
            ChildHead,
            NativeProxy,
            ProxyDescendant,
            WorkerDescendant,
            OriginFn,
        >,
    ) -> impl Future<
        Output = ItemSettlement<
            RoutedCreation<MailAddr, NativeProxy>,
            ChildCreationOutcome<NativeProxy, ChildHead>,
            CreationRejection,
            Never,
        >,
    > + Send
    where
        ProxyDescendant:
            ProjectTerminal<Origin, NativeProxy, Descendants = WorkerDescendant> + Send + 'static,
        Origin: Send + 'static,
        OriginFn: Fn(MailAddr, u64, CreationId, CreationKind) -> Origin + Send,
    {
        <Self as EstablishNativeChild<ManagedSupervisor, NativeProxy, WorkerDescendant>>::establish_native_child::<ChildHead, ProxyDescendant, Origin, OriginFn>(pending)
    }
}
impl NativeChild<NativeProxy, ChildHead, NativeWorker, WorkerDescendant, Never>
    for SupervisorHosts
{
    fn registration(&self) -> ActorSpace<<NativeWorker as Behavior>::Protocol> {
        ActorSpace::new()
    }
    fn establish<Origin, OriginFn>(
        pending: PendingChild<
            '_,
            NativeProxy,
            Self,
            ChildHead,
            NativeWorker,
            WorkerDescendant,
            Never,
            OriginFn,
        >,
    ) -> impl Future<
        Output = ItemSettlement<
            RoutedCreation<MailAddr, NativeWorker>,
            ChildCreationOutcome<NativeWorker, ChildHead>,
            CreationRejection,
            Never,
        >,
    > + Send
    where
        WorkerDescendant:
            ProjectTerminal<Origin, NativeWorker, Descendants = Never> + Send + 'static,
        Origin: Send + 'static,
        OriginFn: Fn(MailAddr, u64, CreationId, CreationKind) -> Origin + Send,
    {
        <Self as EstablishNativeChild<NativeProxy, NativeWorker, Never>>::establish_native_child::<
            ChildHead,
            WorkerDescendant,
            Origin,
            OriginFn,
        >(pending)
    }
}
#[test]
fn generated_and_handwritten_supervisors_preserve_replacement_and_native_retirement() {
    run_supervision(
        supervisor_application::Topology::new(),
        |supervisor_application::RootRetirements { origin, retirement }| (origin, retirement),
        |supervisor_application::ProxyRetirements { origin, retirement }| (origin, retirement),
    );
    run_supervision(
        SupervisorHosts::new(),
        |ProxyDescendant { origin, retirement }| (origin, retirement),
        |WorkerDescendant { origin, retirement }| (origin, retirement),
    );
}
