use bombay::actors::ActorExt;
use bombay::behavior::{Actions, BehaviorActed, Delivery, Never, Protocol};
use bombay::entity::{
    ActivationId, AdmissionFailure, DirectoryConfig, DrainFailure, EntityActivationError,
    EntityCapacity, EntityDefinition, EntityId,
};
use bombay::prelude::StopOnShutdown;
use bombay::{ActorRetirement, Application, MailAddr};
use core::num::NonZeroUsize;

struct Root;
#[bombay::actor(message = Never)]
impl Root {}

struct Ledger;
impl Protocol for Ledger {
    type Addr = MailAddr;
    type Msg = Box<[u8]>;
}

struct Auditor;
#[bombay::actor(sends = { ledger: Vec<Delivery<Ledger>> })]
impl Auditor {
    fn receive(&mut self, entry: Delivery<Ledger>) -> BehaviorActed<Self> {
        Ok(Actions::cont().send_ledger(entry))
    }
}

struct Auditors;
impl EntityDefinition for Auditors {
    type Id = u64;
    type Behavior = StopOnShutdown<Auditor>;
    type Hosts = audit_application::Topology;
    type HydrationError = Never;
    type Terminal = Never;
    type ChildFailures = Vec<Never>;

    async fn hydrate(&self, _: EntityId<u64>) -> Result<Self::Behavior, Never> {
        Ok(Auditor.stop_on_shutdown())
    }
    fn activation_failed(&self, _: EntityId<u64>, _: ActivationId,
        failure: EntityActivationError<Never, Self::Behavior, Never, Vec<Never>>) {
        drop(failure);
    }
    fn admission_refused(&self, _: EntityId<u64>, failure: AdmissionFailure<Delivery<Ledger>>) {
        drop(failure);
    }
    fn forced_retirement(&self, _: &EntityId<u64>, _: ActivationId, failure: DrainFailure) {
        drop(failure);
    }
    fn retired(&self, _: &EntityId<u64>, _: ActivationId,
        retirement: Result<ActorRetirement<Self::Behavior, Never, Vec<Never>>, tokio::task::JoinError>) {
        drop(retirement);
    }
}

#[derive(Clone, Copy)]
struct AuditorsRole;
bombay::topology! {
    audit_application {
        root: Root {}
        hosts { auditors: Auditor; }
        families { auditor: Auditors; }
        auditor {}
    }
}

fn main() {
    let application = Application::with_topology(Root.stop_on_shutdown(), audit_application::Topology::new())
        .entity_family(AuditorsRole, Auditors, DirectoryConfig::default(), EntityCapacity::new(
            NonZeroUsize::new(1).unwrap(), NonZeroUsize::new(1).unwrap()));
    let host = tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap();
    let _outcome = host.block_on(application.run_with_entities(|application| async move {
        application.request_shutdown().unwrap();
    }));
}
