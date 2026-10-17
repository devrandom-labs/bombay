#![cfg(test)]

#[cfg(feature = "axum")]
use axum::{Router, routing::get};
use bombay::WorkOutcome;
use bombay::entity::{
    ActivationId, AdmissionFailure, DirectoryConfig, DrainFailure, EntityActivationError,
    EntityCapacity, EntityDefinition, EntityId,
};
use bombay::{ActorRetirement, ActorRetirementReport};
use core::future::{Future, ready};
use core::num::NonZeroUsize;
use core::task::{Context, Poll, Waker};
#[cfg(feature = "axum")]
use std::cell::Cell;
#[cfg(feature = "axum")]
use std::net::TcpListener;
use std::rc::Rc;
use std::sync::{Arc, Mutex, PoisonError};
#[cfg(feature = "axum")]
use std::time::Duration;
use tokio::runtime::Builder;
use tokio::task::JoinError;

use bombay::behavior::{BehaviorBase, MessageProtocol};
use bombay::prelude::*;

type Count = MessageProtocol<MailAddr, u64>;

enum CounterCommand {
    Increment,
    Read(EstablishedRecipient<Count>),
}

enum CounterError {
    Overflow,
}

struct Counter {
    value: u64,
    audit: Box<[u8]>,
}

#[bombay::actor(sends = { replies: Vec<EstablishedDelivery<Count>> }, error = CounterError)]
impl Counter {
    fn receive(&mut self, command: CounterCommand) -> BehaviorActed<Self> {
        match command {
            CounterCommand::Increment => {
                self.value = self.value.checked_add(1).ok_or(CounterError::Overflow)?;
                Ok(Actions::cont())
            }
            CounterCommand::Read(recipient) => {
                Ok(Actions::cont().with_send(EstablishedDelivery::new(recipient, self.value)))
            }
        }
    }
}

struct UnavailableCounter {
    audit: Box<[u8]>,
}

enum CounterRefusal {
    Initialization,
}

#[bombay::actor(message = Never, error = CounterRefusal)]
impl UnavailableCounter {
    #[expect(
        clippy::unused_self,
        reason = "the actor initialization contract retains the original actor while refusing startup"
    )]
    fn init(&mut self) -> BehaviorActed<Self> {
        Err(CounterRefusal::Initialization)
    }
}

#[tokio::test(flavor = "current_thread")]
async fn completed_counter_inspects_work_retirement_and_notifications_without_positions() {
    let local = Rc::new([5_u8, 7]);
    let original = Rc::clone(&local);
    let audit = Box::new([11_u8, 13]);
    let allocation = audit.as_ptr();
    let outcome = Application::new(Counter { value: 0, audit }.stop_on_shutdown())
        .run_with(|application| async move {
            let mut caller = application
                .client()
                .expect("the actual count recipient exists");
            caller
                .send(CounterCommand::Increment)
                .await
                .expect("increment admitted");
            caller
                .send(CounterCommand::Read(caller.recipient()))
                .await
                .expect("read admitted");
            let reply = caller
                .receive()
                .await
                .expect("the original count reply arrives");
            application.shutdown().expect("shutdown admitted");
            application
                .termination()
                .await
                .expect("the counter stopped");
            (reply.message, local)
        })
        .await
        .unwrap_or_else(|_| panic!("the caller owns the actual native host"));
    let work = outcome
        .work_output()
        .expect("completed local work is inspectable");
    let (origin, retirement) = outcome
        .retirement()
        .as_ref()
        .expect("native retirement is inspectable");
    let notifications = outcome
        .notifications()
        .as_ref()
        .expect("notification receiving is inspectable");
    assert_eq!(work.0, 1);
    assert!(Rc::ptr_eq(&work.1, &original));
    assert_eq!(origin.address(), MailAddr::APPLICATION_ROOT);
    assert_eq!(retirement.completion(), Some(&Completion::Stopped));
    assert_eq!(
        retirement
            .behavior()
            .expect("the complete actor survives")
            .base()
            .audit
            .as_ptr(),
        allocation
    );
    assert!(notifications.termination.is_ok());
    assert!(notifications.retirement.is_ok());
    let ((value, returned), retirement) = outcome
        .try_into_completed()
        .unwrap_or_else(|_| panic!("completed selection retains all independent facts"));
    assert_eq!(value, 1);
    assert!(Rc::ptr_eq(&returned, &original));
    assert!(retirement.cleanup().is_ok());
    assert!(
        retirement
            .notifications()
            .as_ref()
            .expect("original notification receiving")
            .retirement
            .is_ok()
    );
    let (cleanup, native, notifications, ()) = retirement.into_parts();
    cleanup.expect("the original cleanup result remains available");
    let (_, native) = native.expect("the entire native result remains available");
    assert_eq!(
        native
            .behavior()
            .expect("the original actor survives owned work selection")
            .base()
            .audit
            .as_ptr(),
        allocation
    );
    let notifications = notifications.expect("the independent notifications remain owned");
    assert!(notifications.termination.is_ok());
    assert!(notifications.retirement.is_ok());
}

#[tokio::test(flavor = "current_thread")]
async fn rejected_work_selection_returns_the_whole_application_result() {
    let audit = Box::new([17_u8, 19]);
    let allocation = audit.as_ptr();
    let local = Rc::new([23_u8, 29]);
    let original = Rc::clone(&local);
    let outcome = Application::new(UnavailableCounter { audit }.stop_on_shutdown())
        .run_with(move |_| async move { local })
        .await
        .unwrap_or_else(|_| panic!("the caller owns the actual native host"));
    let Err(outcome) = outcome.try_into_completed() else {
        panic!("startup refusal cannot fabricate completed work")
    };
    assert!(outcome.work_output().is_none());
    let (_, retirement) = outcome
        .retirement()
        .as_ref()
        .expect("the original actor is retained");
    assert_eq!(
        retirement
            .behavior()
            .expect("the refused actor survives")
            .base()
            .audit
            .as_ptr(),
        allocation
    );
    let notifications = outcome
        .notifications()
        .as_ref()
        .expect("the actual notification results survive");
    assert!(notifications.termination.is_ok());
    assert!(notifications.retirement.is_ok());
    assert_eq!(Rc::strong_count(&original), 2);
    let (work, native, notifications, ()) = outcome.into_parts();
    let WorkOutcome::NotInvoked {
        work,
        startup_error: Some(_),
        cleanup,
    } = work
    else {
        panic!("the complete original uninvoked work phase survives selection rejection");
    };
    cleanup.expect("the original cleanup result remains available");
    let (_, native) = native.expect("the original actor remains owned");
    assert_eq!(
        native
            .behavior()
            .expect("the initialization refusal owns the actor")
            .base()
            .audit
            .as_ptr(),
        allocation
    );
    let notifications = notifications.expect("the original notification product remains owned");
    assert!(notifications.termination.is_ok());
    assert!(notifications.retirement.is_ok());
    drop(work);
    assert_eq!(Rc::strong_count(&original), 1);
}

#[tokio::test(flavor = "current_thread")]
async fn cancelled_cold_execution_returns_all_originals_for_real_retry() {
    let local = Rc::new([31_u8, 37]);
    let original = Rc::clone(&local);
    let audit = Box::new([41_u8, 43]);
    let allocation = audit.as_ptr();
    let (execution, receiving) = Application::new(Counter { value: 0, audit }.stop_on_shutdown())
        .execute_with(|application| async move {
            let mut caller = application
                .client()
                .expect("the real retry establishes its customer");
            caller
                .send(CounterCommand::Read(caller.recipient()))
                .await
                .expect("read admitted");
            let count = caller
                .receive()
                .await
                .expect("the original counter replies");
            application.shutdown().expect("shutdown admitted");
            application
                .termination()
                .await
                .expect("the counter stopped");
            (count.message, local)
        })
        .unwrap_or_else(|_| panic!("the actual entered host exists"));
    drop(execution);
    let outcome = receiving.await;
    let Err(outcome) = outcome.try_into_completed() else {
        panic!("unpolled execution cannot fabricate completed work")
    };
    assert!(outcome.work_output().is_none());
    assert!(outcome.retirement().is_err());
    assert!(outcome.notifications().is_err());
    let (work, native, notifications, ()) = outcome.into_parts();
    let WorkOutcome::Unstarted { application, work } = work else {
        panic!("the cold phase owns the actual untouched declaration and callable");
    };
    assert!(native.is_err());
    assert!(notifications.is_err());
    let outcome = application
        .run_with(work)
        .await
        .unwrap_or_else(|_| panic!("the real retry enters its native host"));
    let ((count, returned), retirement) = outcome
        .try_into_completed()
        .unwrap_or_else(|_| panic!("the same work completes on real retry"));
    assert_eq!(count, 0);
    assert!(Rc::ptr_eq(&returned, &original));
    assert!(retirement.cleanup().is_ok());
    assert_eq!(
        retirement
            .retirement()
            .as_ref()
            .expect("native retirement survives")
            .1
            .behavior()
            .expect("the original counter survives retry")
            .base()
            .audit
            .as_ptr(),
        allocation
    );
    let notifications = retirement
        .notifications()
        .as_ref()
        .expect("original notifications survive");
    assert!(notifications.termination.is_ok());
    assert!(notifications.retirement.is_ok());
}

struct Root;

#[bombay::actor(message = Never)]
impl Root {}

#[derive(Clone, Copy)]
struct CountersRole;

bombay::topology! {
    counter_application {
        root: Root {}
        hosts { counters: Counter; }
    }
}

type CounterRetirement =
    Result<ActorRetirement<StopOnShutdown<Counter>, Never, Vec<Never>>, JoinError>;

#[expect(
    dead_code,
    reason = "failure callbacks retain complete typed originals; the healthy Entity journey expects no failures"
)]
#[expect(
    clippy::large_enum_variant,
    reason = "the test custody sink preserves original native startup failures inline without adding an allocation"
)]
enum CounterFailure {
    Activation(
        EntityId<u64>,
        ActivationId,
        EntityActivationError<Never, StopOnShutdown<Counter>, Never, Vec<Never>>,
    ),
    Admission(EntityId<u64>, AdmissionFailure<CounterCommand>),
    Forced(EntityId<u64>, ActivationId, DrainFailure),
}

struct Counters {
    failures: Arc<Mutex<Vec<CounterFailure>>>,
    retirements: Arc<Mutex<Vec<(CounterRetirement, ActorRetirementReport)>>>,
}

impl EntityDefinition for Counters {
    type Id = u64;
    type Behavior = StopOnShutdown<Counter>;
    type Hosts = counter_application::Topology;
    type HydrationError = Never;
    type Terminal = Never;
    type ChildFailures = Vec<Never>;

    fn hydrate(&self, id: EntityId<u64>) -> impl Future<Output = Result<Self::Behavior, Never>> {
        ready(Ok(Counter {
            value: *id.get(),
            audit: Box::new([47_u8, 53]),
        }
        .stop_on_shutdown()))
    }

    fn activation_failed(
        &self,
        id: EntityId<u64>,
        activation: ActivationId,
        failure: EntityActivationError<Never, Self::Behavior, Never, Vec<Never>>,
    ) {
        self.failures
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .push(CounterFailure::Activation(id, activation, failure));
    }

    fn admission_refused(&self, id: EntityId<u64>, failure: AdmissionFailure<CounterCommand>) {
        self.failures
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .push(CounterFailure::Admission(id, failure));
    }

    fn forced_retirement(
        &self,
        id: &EntityId<u64>,
        activation: ActivationId,
        failure: DrainFailure,
    ) {
        self.failures
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .push(CounterFailure::Forced(*id, activation, failure));
    }

    fn retired(
        &self,
        _: &EntityId<u64>,
        _: ActivationId,
        retirement: CounterRetirement,
        report: ActorRetirementReport,
    ) {
        self.retirements
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .push((retirement, report));
    }
}

#[tokio::test(flavor = "current_thread")]
async fn entity_work_selection_keeps_original_family_shutdowns_and_actor_retirement() {
    let failures = Arc::new(Mutex::new(Vec::new()));
    let retirements = Arc::new(Mutex::new(Vec::new()));
    let counters = Counters {
        failures: Arc::clone(&failures),
        retirements: Arc::clone(&retirements),
    };
    let capacity = EntityCapacity::new(
        NonZeroUsize::new(2).expect("two is nonzero"),
        NonZeroUsize::new(4).expect("four is nonzero"),
    );
    let outcome = Application::with_topology(
        Root.stop_on_shutdown(),
        counter_application::Topology::new(),
    )
    .entity_family(CountersRole, counters, DirectoryConfig::default(), capacity)
    .run_with_entities(|application| async move {
        let counter = application.entities(CountersRole).entity(59);
        let mut caller = application
            .client_to(counter)
            .unwrap_or_else(|_| panic!("the actual Entity customer is established"));
        let increment = caller.send(CounterCommand::Increment).await;
        assert!(increment.is_ok());
        let read = caller.send(CounterCommand::Read(caller.recipient())).await;
        assert!(read.is_ok());
        let count = caller.receive().await.expect("the actual Entity replies");
        application.shutdown().expect("root shutdown admitted");
        application.termination().await.expect("root stopped");
        count.message
    })
    .await
    .unwrap_or_else(|_| panic!("the caller owns the native Entity host"));

    assert_eq!(outcome.work_output(), Some(&60));
    assert!(outcome.cleanup().expect("actual work cleanup").is_ok());
    let family = outcome
        .family(CountersRole)
        .expect("the complete family report is received");
    let shutdown = family.shutdown();
    let metrics = family.metrics();
    let disposal = family.disposal_failure();
    assert!(matches!(
        shutdown,
        bombay::entity::EntityShutdown::Settled { represented: 1, .. }
    ));
    assert_eq!(metrics.activations, 1);
    assert_eq!(metrics.residents, 0);
    assert!(disposal.is_none());
    let (count, retirement) = outcome
        .try_into_completed()
        .unwrap_or_else(|_| panic!("the complete Entity work is selected"));
    assert_eq!(count, 60);
    assert!(retirement.cleanup().is_ok());
    assert!(retirement.retirement().is_ok());
    let family = retirement
        .family(CountersRole)
        .expect("the original family report survives owned work selection");
    let shutdown = family.shutdown();
    let metrics = family.metrics();
    let disposal = family.disposal_failure();
    assert!(matches!(
        shutdown,
        bombay::entity::EntityShutdown::Settled { represented: 1, .. }
    ));
    assert_eq!(metrics.activations, 1);
    assert_eq!(metrics.residents, 0);
    assert!(disposal.is_none());
    let notifications = retirement
        .notifications()
        .as_ref()
        .expect("the original notification transfer survives");
    assert!(notifications.termination.is_ok());
    assert!(notifications.retirement.is_ok());
    let failures = failures.lock().unwrap_or_else(PoisonError::into_inner);
    assert!(failures.is_empty());
    let retirements = retirements.lock().unwrap_or_else(PoisonError::into_inner);
    assert_eq!(retirements.len(), 1);
    let (native, report) = &retirements[0];
    assert_eq!(
        report.retirement(),
        bombay::RetirementAssessment::Established
    );
    assert_eq!(
        report.failures(),
        bombay::ActorFailureAssessment::NoFailuresFound
    );
    let actor = native
        .as_ref()
        .expect("the definition owns the original Entity actor");
    assert_eq!(
        actor
            .behavior()
            .expect("the complete counter state survives")
            .base()
            .value,
        60
    );
}

#[tokio::test(flavor = "current_thread")]
async fn failed_counter_keeps_business_observation_and_original_native_error() {
    let audit = Box::new([61_u8, 67]);
    let allocation = audit.as_ptr();
    let outcome = Application::new(
        Counter {
            value: u64::MAX,
            audit,
        }
        .stop_on_shutdown(),
    )
    .run_with(|application| async move {
        let caller = application
            .sender()
            .expect("the real counter sender exists");
        caller
            .send(CounterCommand::Increment)
            .await
            .expect("increment admitted");
        application.termination().await
    })
    .await
    .unwrap_or_else(|_| panic!("the native caller host exists"));
    assert!(
        outcome
            .work_output()
            .expect("the caller observed the original failure")
            .is_err()
    );
    let (observed, retirement) = outcome
        .try_into_completed()
        .unwrap_or_else(|_| panic!("failed actor execution does not erase completed work"));
    assert!(observed.is_err());
    assert!(retirement.cleanup().is_ok());
    let (_, native) = retirement
        .retirement()
        .as_ref()
        .expect("the actual failed actor remains owned");
    assert!(matches!(
        native,
        ActorRetirement::BehaviorFailed {
            error: CounterError::Overflow,
            ..
        }
    ));
    assert_eq!(
        native
            .behavior()
            .expect("the failed actor remains available")
            .base()
            .audit
            .as_ptr(),
        allocation
    );
    assert_eq!(
        native
            .behavior()
            .expect("the count remains bounded")
            .base()
            .value,
        u64::MAX
    );
    let notifications = retirement
        .notifications()
        .as_ref()
        .expect("the original notification results remain independent");
    assert!(notifications.termination.is_ok());
    assert!(notifications.retirement.is_ok());
}

#[test]
fn destroyed_host_keeps_ready_work_and_every_receiving_failure() {
    let host = Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("the selected actor host exists");
    let local = Rc::new([71_u8, 73]);
    let original = Rc::clone(&local);
    let (execution, receiving) = host.block_on(async {
        Application::new(
            Counter {
                value: 0,
                audit: Box::new([79_u8, 83]),
            }
            .stop_on_shutdown(),
        )
        .execute_with(move |_| async move { local })
        .unwrap_or_else(|_| panic!("the exact selected host is entered"))
    });
    let mut execution = Box::pin(execution);
    let mut context = Context::from_waker(Waker::noop());
    let startup = execution.as_mut().poll(&mut context);
    assert!(matches!(startup, Poll::Pending));
    host.block_on(async { tokio::task::yield_now().await });
    let work = execution.as_mut().poll(&mut context);
    assert!(matches!(work, Poll::Pending));
    drop(execution);
    drop(host);
    let receiving_host = Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("the result consumer has its own host");
    let outcome = receiving_host.block_on(receiving);
    let returned = outcome
        .work_output()
        .expect("the already acquired ready output survives actor-host destruction");
    assert!(Rc::ptr_eq(returned, &original));
    assert!(
        outcome
            .cleanup()
            .expect("the actual cleanup receiving result survives")
            .is_err()
    );
    assert!(outcome.retirement().is_err());
    assert!(outcome.notifications().is_err());
    let (returned, retirement) = outcome
        .try_into_completed()
        .unwrap_or_else(|_| panic!("cleanup and publication failures do not erase ready work"));
    assert!(Rc::ptr_eq(&returned, &original));
    assert!(retirement.cleanup().is_err());
    assert!(retirement.retirement().is_err());
    assert!(retirement.notifications().is_err());
    let (cleanup, native, notifications, ()) = retirement.into_parts();
    assert!(cleanup.is_err());
    assert!(native.is_err());
    assert!(notifications.is_err());
    drop((returned, cleanup, native, notifications));
    assert_eq!(Rc::strong_count(&original), 1);
}

#[derive(Clone, Copy)]
struct VisitsRole;

#[tokio::test(flavor = "current_thread")]
#[expect(
    clippy::too_many_lines,
    reason = "the complete two-family journey checks separate replies, actors, borrowed projections and owned retirements in one lifecycle"
)]
async fn same_protocol_families_select_both_real_positions_and_preserve_separate_retirements() {
    let failures = Arc::new(Mutex::new(Vec::new()));
    let retirements = Arc::new(Mutex::new(Vec::new()));
    let visit_failures = Arc::new(Mutex::new(Vec::new()));
    let visit_retirements = Arc::new(Mutex::new(Vec::new()));
    let counters = Counters {
        failures: Arc::clone(&failures),
        retirements: Arc::clone(&retirements),
    };
    let visits = Counters {
        failures: Arc::clone(&visit_failures),
        retirements: Arc::clone(&visit_retirements),
    };
    let capacity = EntityCapacity::new(
        NonZeroUsize::new(2).expect("two is nonzero"),
        NonZeroUsize::new(4).expect("four is nonzero"),
    );
    let outcome = Application::with_topology(
        Root.stop_on_shutdown(),
        counter_application::Topology::new(),
    )
    .entity_family(CountersRole, counters, DirectoryConfig::default(), capacity)
    .entity_family(VisitsRole, visits, DirectoryConfig::default(), capacity)
    .run_with_entities(|application| async move {
        let target = application.entities(CountersRole).entity(59);
        let visit = application.entities(VisitsRole).entity(59);
        let mut caller = application
            .client_to(target)
            .unwrap_or_else(|_| panic!("the actual counter customer exists"));
        let mut visitor = application
            .client_to(visit)
            .unwrap_or_else(|_| panic!("the independent visitor exists"));
        let admitted = caller.send(CounterCommand::Increment).await;
        assert!(admitted.is_ok());
        let admitted = visitor.send(CounterCommand::Increment).await;
        assert!(admitted.is_ok());
        let admitted = visitor.send(CounterCommand::Increment).await;
        assert!(admitted.is_ok());
        let read = caller.send(CounterCommand::Read(caller.recipient())).await;
        assert!(read.is_ok());
        let read = visitor
            .send(CounterCommand::Read(visitor.recipient()))
            .await;
        assert!(read.is_ok());
        let counter = caller.receive().await.expect("the counter replies");
        let visit = visitor
            .receive()
            .await
            .expect("the independent visitor replies");
        application.shutdown().expect("root shutdown admitted");
        application.termination().await.expect("root stopped");
        (counter, visit)
    })
    .await
    .unwrap_or_else(|_| panic!("the caller owns the native host"));

    let (count, visit) = outcome
        .work_output()
        .expect("both exact domain replies survive");
    assert_eq!(count.message, 60);
    assert_eq!(visit.message, 61);
    assert_ne!(count.from, visit.from);
    let counters = outcome
        .family(CountersRole)
        .expect("the actual tail family is selected");
    let visits = outcome
        .family(VisitsRole)
        .expect("the actual head family is selected");
    assert_eq!(counters.metrics().activations, 1);
    assert_eq!(visits.metrics().activations, 1);
    assert_eq!(counters.metrics().residents, 0);
    assert_eq!(visits.metrics().residents, 0);
    assert!(counters.disposal_failure().is_none());
    assert!(visits.disposal_failure().is_none());
    let ((count, visit), retirement) = outcome
        .try_into_completed()
        .unwrap_or_else(|_| panic!("owned work selection preserves both families"));
    assert_eq!(count.message, 60);
    assert_eq!(visit.message, 61);
    let counters = retirement
        .family(CountersRole)
        .expect("the same tail family survives owned selection");
    let visits = retirement
        .family(VisitsRole)
        .expect("the same head family survives owned selection");
    assert!(matches!(
        counters.shutdown(),
        bombay::entity::EntityShutdown::Settled { represented: 1, .. }
    ));
    assert!(matches!(
        visits.shutdown(),
        bombay::entity::EntityShutdown::Settled { represented: 1, .. }
    ));
    let (cleanup, native, notifications, (visits, (counters, ()))) = retirement.into_parts();
    cleanup.expect("the original cleanup result survives");
    let (_, native) = native.expect("the original native root survives");
    assert_eq!(native.completion(), Some(&Completion::Stopped));
    let notifications = notifications.expect("the original notification results survive");
    assert!(notifications.termination.is_ok());
    assert!(notifications.retirement.is_ok());
    let (VisitsRole, visits) =
        visits.unwrap_or_else(|_| panic!("the original visitor report survives"));
    let (CountersRole, counters) =
        counters.unwrap_or_else(|_| panic!("the original counter report survives"));
    let (visit_shutdown, visit_metrics, visit_disposal) = visits.into_parts();
    let (counter_shutdown, counter_metrics, counter_disposal) = counters.into_parts();
    assert!(matches!(
        visit_shutdown,
        bombay::entity::EntityShutdown::Settled { represented: 1, .. }
    ));
    assert!(matches!(
        counter_shutdown,
        bombay::entity::EntityShutdown::Settled { represented: 1, .. }
    ));
    assert_eq!(visit_metrics.activations, 1);
    assert_eq!(counter_metrics.activations, 1);
    assert!(visit_disposal.is_none());
    assert!(counter_disposal.is_none());
    assert!(
        failures
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .is_empty()
    );
    assert!(
        visit_failures
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .is_empty()
    );
    let counters = retirements.lock().unwrap_or_else(PoisonError::into_inner);
    let visits = visit_retirements
        .lock()
        .unwrap_or_else(PoisonError::into_inner);
    assert_eq!(counters.len(), 1);
    assert_eq!(visits.len(), 1);
    assert_eq!(
        counters[0]
            .0
            .as_ref()
            .expect("the actual counter is retired")
            .behavior()
            .expect("the counter remains owned")
            .base()
            .value,
        60
    );
    assert_eq!(
        visits[0]
            .0
            .as_ref()
            .expect("the actual visitor is retired")
            .behavior()
            .expect("the visitor remains owned")
            .base()
            .value,
        61
    );
}

#[cfg(feature = "axum")]
#[tokio::test(flavor = "current_thread")]
async fn http_binding_refusal_returns_whole_inputs_for_real_retry_and_joined_retirement() {
    let occupied = TcpListener::bind("127.0.0.1:0").expect("the actual occupied port exists");
    let address = occupied.local_addr().expect("the actual address exists");
    let original = Rc::new(Box::new([101_u8, 103]));
    let allocation = original.as_ptr();
    let router_data = Rc::clone(&original);
    let calls = Rc::new(Cell::new(0));
    let router_calls = Rc::clone(&calls);
    let (execution, receiving) = Application::new(Root.stop_on_shutdown())
        .execute_axum(address, move |application| {
            router_calls.set(router_calls.get() + 1);
            assert_eq!(router_data.as_ptr(), allocation);
            application
                .shutdown()
                .expect("the configured HTTP root accepts shutdown");
            Router::new().route("/ready", get(|| async { "ready" }))
        })
        .unwrap_or_else(|_| panic!("the caller owns the actual HTTP host"));
    execution.await;
    let outcome = receiving.await;
    let Err(outcome) = outcome.try_into_completed() else {
        panic!("a bind refusal cannot claim completed serving")
    };
    assert!(outcome.work_output().is_none());
    assert!(outcome.retirement().is_err());
    assert!(outcome.notifications().is_err());
    assert_eq!(calls.get(), 0);
    let (work, native, notifications, ()) = outcome.into_parts();
    let WorkOutcome::StagingRejected {
        inputs: Ok((application, router, returned_address, error)),
    } = work
    else {
        panic!("the actual cold declaration, original router and address survive refusal");
    };
    assert_eq!(error.kind(), std::io::ErrorKind::AddrInUse);
    assert_eq!(returned_address, address);
    assert!(native.is_err());
    assert!(notifications.is_err());
    drop(occupied);
    let outcome = application
        .run_axum(returned_address, router)
        .await
        .unwrap_or_else(|_| panic!("the same original inputs are retryable"));
    assert!(
        outcome
            .work_output()
            .expect("actual serving completed")
            .is_ok()
    );
    let (serving, retirement) = outcome
        .try_into_completed()
        .unwrap_or_else(|_| panic!("real HTTP serving and retirement remain independent"));
    serving.expect("the actual root gracefully stopped HTTP serving");
    assert_eq!(calls.get(), 1);
    assert_eq!(original.as_ptr(), allocation);
    assert!(retirement.cleanup().is_ok());
    assert_eq!(
        retirement
            .retirement()
            .as_ref()
            .expect("the original HTTP root remains owned")
            .1
            .completion(),
        Some(&Completion::Stopped)
    );
    let notifications = retirement
        .notifications()
        .as_ref()
        .expect("original notifications survive retry");
    assert!(notifications.termination.is_ok());
    assert!(notifications.retirement.is_ok());
}

#[cfg(feature = "axum")]
#[tokio::test(flavor = "current_thread")]
async fn natural_root_expiry_returns_http_serving_beside_complete_timer_retirement() {
    let available =
        TcpListener::bind("127.0.0.1:0").expect("a real local address can be allocated");
    let address = available.local_addr().expect("the real address exists");
    drop(available);
    let outcome =
        Application::new(Root.with_one_shot(Duration::from_millis(1), |_| Actions::stop()))
            .run_axum(address, |_| {
                Router::new().route("/ready", get(|| async { "ready" }))
            })
            .await
            .unwrap_or_else(|_| panic!("the caller hosts actual local HTTP serving"));
    let (serving, retirement) = outcome.try_into_completed().unwrap_or_else(|_| {
        panic!("natural root completion retains serving and independent cleanup")
    });
    serving.expect("natural root termination gracefully finishes actual HTTP serving");
    assert!(retirement.cleanup().is_ok());
    assert_eq!(
        retirement
            .retirement()
            .as_ref()
            .expect("the exact timed root is retained")
            .1
            .completion(),
        Some(&Completion::Stopped)
    );
    let notifications = retirement
        .notifications()
        .as_ref()
        .expect("the original notifications remain independent");
    assert!(notifications.termination.is_ok());
    assert!(notifications.retirement.is_ok());
}
