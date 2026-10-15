//! Cold connection handoff through the existing Application execution owner.

mod startup;

use super::execution::{
    ApplicationWorkCustody, ApplicationWorkPresence, ApplicationWorkPublication,
    PreparedApplication, execute_application_with,
};
use super::{Application, ApplicationHandle, ApplicationOutcome, ApplicationScope};
use crate::actors::ActorExt;
use crate::behavior::{Behavior, BehaviorBase, ClassifySettlement, Never, SettlementStatus};
use crate::local::children::StructuralOrigins;
use crate::remote::counter::{Counter, CounterMessage, CounterValue};
use crate::remote::{Connection, listening_counter_network};
use crate::{ActorFailureAssessment, ActorRetirement, Completion, MailAddr, RetirementAssessment};
use behavior_actors::StopOnShutdown;
use core::future::{Future, poll_fn};
use core::pin::pin;
use std::cell::Cell;
use std::ptr;
use std::rc::Rc;
use std::task::{Context, Poll, Waker};
use tokio::runtime::{Builder, Handle};
use tokio::sync::oneshot;
use zenoh::Session;

#[test]
#[expect(
    clippy::result_large_err,
    clippy::too_many_lines,
    reason = "retain cold acquisition, parked work, actual native replies and every independent cleanup receipt"
)]
fn acquired_connection_reaches_work_and_waits_for_its_retirement_barrier() {
    Builder::new_multi_thread()
        .worker_threads(1)
        .enable_all()
        .build()
        .unwrap()
        .block_on(async {
            let application = Application::new(Counter::new().stop_on_shutdown());
            let network = listening_counter_network();
            let starts = Rc::new(Cell::new(0));
            let startup_calls = Rc::clone(&starts);
            let calls = Rc::new(Cell::new(0));
            let increments = Box::new(2_u64);
            let allocation = ptr::from_ref(increments.as_ref());
            let (acquired, mut acquisition) = oneshot::channel();
            let (resume, resumed) = oneshot::channel();
            let (entered, mut parked) = oneshot::channel();
            let (release, released) = oneshot::channel();
            let (acquire, closing) = Connection::startup();
            let (execution, receiving) = execute_application_with::<
                _,
                StopOnShutdown<Counter>,
                StopOnShutdown<Counter>,
                _,
                StructuralOrigins<<StopOnShutdown<Counter> as BehaviorBase>::Base>,
                Never,
                (),
                Never,
                _,
                _,
                _,
                _,
                _,
                _,
                _,
                _,
                Never,
            >(
                Handle::current(),
                (application, network),
                (increments, Rc::clone(&calls), entered, released),
                async move |(application, network)| {
                    startup_calls.set(startup_calls.get() + 1);
                    let (connection, startup_publication) = acquire(network).await;
                    let connection = connection.expect("actual configured TLS startup");
                    // The independent receiver owns these facts before this producer can drop.
                    let published = acquired.send((closing, connection.session().clone()));
                    let resumed = resumed.await;
                    (
                        (connection, startup_publication, published, resumed),
                        application.allocations(),
                    )
                },
                |original: ApplicationWorkPublication<_, _, _, _, Never, _>,
                 (connection, startup_publication, published, resumed),
                 allocations| {
                    let ((application, network), work, publication) = original.into_unstarted();
                    let (root, spaces, (), (), scope) = application.into_parts();
                    let scope = scope.unwrap_or(ApplicationScope { allocations });
                    let invoke = move |(increments, calls, entered, released): (
                        Box<u64>,
                        Rc<Cell<usize>>,
                        oneshot::Sender<_>,
                        oneshot::Receiver<()>,
                    ),
                                       application: ApplicationHandle<
                        <Counter as Behavior>::Protocol,
                        <StopOnShutdown<Counter> as Behavior>::Event,
                    >| async move {
                        calls.set(calls.get() + 1);
                        let runtime = application.session.as_ref().map(Session::zid);
                        let mut client = application
                            .export_client::<_, CounterValue>(
                                application.root().established_recipient(),
                            )
                            .unwrap_or_else(|_| panic!("actual Counter client"));
                        let mut admissions = Vec::new();
                        for _ in 0..*increments {
                            admissions.push(client.try_send(CounterMessage::Increment));
                        }
                        let read = client.try_send(CounterMessage::Read(client.recipient()));
                        let value = client.receive().await;
                        let stopped = application.request_shutdown();
                        let report = application.lifecycle().retirement().await;
                        let parked = entered
                            .send((runtime, admissions, read, value, stopped, report, client));
                        let released = released.await;
                        (
                            increments,
                            calls,
                            parked,
                            released,
                            network,
                            startup_publication,
                            published,
                            resumed,
                        )
                    };
                    Ok(PreparedApplication {
                        work: ApplicationWorkPublication {
                            publication: Some((
                                ApplicationWorkCustody::Prepared(
                                    (root, spaces, scope),
                                    ApplicationWorkPresence::Supplied((work, invoke)),
                                ),
                                publication,
                            )),
                        },
                        families: (),
                        connection: Some(connection),
                    })
                },
                ((), |_executor, (), ()| None),
            );
            let mut execution = Box::pin(execution);
            let mut completed = None;
            let (closing, session) = poll_fn(|context| {
                if completed.is_none()
                    && let Poll::Ready(output) = execution.as_mut().poll(context)
                {
                    completed = Some(output);
                }
                pin!(&mut acquisition).poll(context)
            })
            .await
            .unwrap();
            let mut closing = pin!(closing);
            let startup_pending = {
                let mut borrowed_execution = execution.as_mut();
                borrowed_execution
                    .as_mut()
                    .poll(&mut Context::from_waker(Waker::noop()))
            };
            let calls_before_resume = calls.get();
            let resume = resume.send(());
            let mut parked = poll_fn(|context| {
                if completed.is_none()
                    && let Poll::Ready(output) = execution.as_mut().poll(context)
                {
                    completed = Some(output);
                }
                pin!(&mut parked).poll(context)
            })
            .await;
            let before_release = closing
                .as_mut()
                .poll(&mut Context::from_waker(Waker::noop()));
            let closed_while_parked = session.is_closed();
            let release = release.send(());
            let ((), (outcome, native, notifications)) = tokio::join!(
                async {
                    if completed.is_none() {
                        execution.await;
                    }
                },
                receiving
            );
            let (close_before_release, close_report) = match before_release {
                Poll::Pending => (Poll::Pending, closing.await),
                Poll::Ready(original) => (Poll::Ready(()), original),
            };
            let mut tail = Vec::new();
            if let Ok((_, _, _, _, _, _, client)) = &mut parked {
                client.close_admission();
                while let Some(original) = client.receive().await {
                    tail.push(original);
                }
            }
            let closed_after_join = session.is_closed();
            let (output, cleanup) = outcome
                .try_into_completed()
                .unwrap_or_else(|_| panic!("actual Counter work completion"));
            let (
                increments,
                original_calls,
                parked_publication,
                released,
                _network,
                startup_publication,
                published,
                resumed,
            ) = output.into_supplied();
            let (runtime, admissions, read, value, stopped, report, _client) = parked.unwrap();
            let (joined, original_close) = close_report
                .expect("the actual startup report was published")
                .unwrap_or_else(|_| panic!("the configured native connection opened"));
            assert!(startup_publication.is_ok());
            assert!(cleanup.is_ok() && joined.is_ok());
            assert!(matches!(original_close, Ok(Ok(()))));
            assert_eq!(
                runtime,
                Some(session.zid()),
                "startup-acquired runtime must reach actual Work"
            );
            assert!(startup_pending.is_pending());
            assert_eq!(close_before_release, Poll::Pending);
            assert!(!closed_while_parked && closed_after_join);
            assert!(resume.is_ok() && resumed.is_ok());
            assert!(release.is_ok() && released.is_ok());
            assert!(published.is_ok() && parked_publication.is_ok());
            assert!(completed.is_none());
            assert_eq!(starts.get(), 1);
            assert_eq!(calls_before_resume, 0);
            assert_eq!(calls.get(), 1);
            assert!(Rc::ptr_eq(&calls, &original_calls));
            assert_eq!(ptr::from_ref(increments.as_ref()), allocation);
            assert_eq!(*increments, 2);
            assert_eq!(admissions.len(), 2);
            assert!(admissions.iter().all(Result::is_ok) && read.is_ok() && stopped.is_ok());
            let value = value.unwrap();
            assert_eq!((value.from, value.message), (MailAddr::APPLICATION_ROOT, 2));
            assert_eq!(tail.len(), 0);
            assert_eq!(report.retirement(), RetirementAssessment::Established);
            assert_eq!(report.failures(), ActorFailureAssessment::NoFailuresFound);
            let notifications = notifications.unwrap();
            assert!(notifications.termination.is_ok() && notifications.retirement.is_ok());
            let (origin, retirement) = native.unwrap();
            assert_eq!(origin.address(), MailAddr::APPLICATION_ROOT);
            let ActorRetirement::Completed {
                completion,
                custody,
                interpretation,
                source,
                settlements,
            } = retirement
            else {
                panic!("actual Counter native result");
            };
            assert_eq!(completion, Completion::Stopped);
            assert!(interpretation.is_none() && source.is_none());
            assert!(custody.control.is_empty() && custody.user.is_empty());
            assert_eq!(custody.descendants.as_slice(), [] as [Never; 0]);
            assert_eq!(custody.child_failures.as_slice(), [] as [Never; 0]);
            assert!(custody.retirement_failures.is_empty());
            assert!(
                custody.capability_failures.is_empty() && custody.additional_failures.is_empty()
            );
            assert!(custody.received_interpretation.is_none() && custody.received_source.is_none());
            assert!(custody.source_index.is_none() && custody.acquired_ingress.is_none());
            assert!(
                custody.terminal_report.is_none() && custody.unread_owner_cancellation.is_none()
            );
            let settlement = settlements.settlement_status();
            assert_eq!(settlement, SettlementStatus::Accepted);
        });
}

#[test]
#[expect(
    clippy::result_large_err,
    clippy::too_many_lines,
    reason = "unpolled cold acquisition returns each original owner without opening a transport or invoking work"
)]
fn unpolled_connection_acquisition_returns_original_application_network_and_work() {
    Builder::new_multi_thread()
        .worker_threads(1)
        .enable_all()
        .build()
        .unwrap()
        .block_on(async {
            let application = Box::new(Application::new(Counter::new().stop_on_shutdown()));
            let network = Box::new(listening_counter_network());
            let increments = Box::new(5_u64);
            let original_application = ptr::from_ref(application.as_ref());
            let original_network = ptr::from_ref(network.as_ref());
            let original_work = ptr::from_ref(increments.as_ref());
            let starts = Rc::new(Cell::new(0));
            let startup_calls = Rc::clone(&starts);
            let calls = Rc::new(Cell::new(0));
            let (acquire, closing) = Connection::startup();
            let (execution, receiving) = execute_application_with::<
                _,
                StopOnShutdown<Counter>,
                StopOnShutdown<Counter>,
                _,
                StructuralOrigins<<StopOnShutdown<Counter> as BehaviorBase>::Base>,
                Never,
                (),
                Never,
                _,
                _,
                _,
                _,
                _,
                _,
                _,
                _,
                Never,
            >(
                Handle::current(),
                (application, network),
                (increments, Rc::clone(&calls)),
                async move |(application, network)| {
                    startup_calls.set(startup_calls.get() + 1);
                    let acquired = acquire(network).await;
                    (acquired, application.allocations())
                },
                |original: ApplicationWorkPublication<_, _, _, _, Never, _>,
                 acquired,
                 allocations| {
                    let ((application, network), work, publication) = original.into_unstarted();
                    let (connection, startup_publication) = acquired;
                    let connection = connection.expect("actual cold TLS acquisition");
                    let (root, spaces, (), (), scope) = (*application).into_parts();
                    let scope = scope.unwrap_or(ApplicationScope { allocations });
                    let invoke = move |(increments, calls): (Box<u64>, Rc<Cell<usize>>),
                                       application: ApplicationHandle<
                        <Counter as Behavior>::Protocol,
                        <StopOnShutdown<Counter> as Behavior>::Event,
                    >| async move {
                        calls.set(calls.get() + 1);
                        let mut client = application
                            .export_client::<_, CounterValue>(
                                application.root().established_recipient(),
                            )
                            .unwrap_or_else(|_| panic!("actual cold Counter client"));
                        let read = client.try_send(CounterMessage::Read(client.recipient()));
                        let value = client.receive().await;
                        let stopped = application.request_shutdown();
                        let report = application.lifecycle().retirement().await;
                        client.close_admission();
                        let mut tail = Vec::new();
                        while let Some(original) = client.receive().await {
                            tail.push(original);
                        }
                        (
                            increments,
                            calls,
                            network,
                            startup_publication,
                            read,
                            value,
                            stopped,
                            report,
                            client,
                            tail,
                        )
                    };
                    Ok(PreparedApplication {
                        work: ApplicationWorkPublication {
                            publication: Some((
                                ApplicationWorkCustody::Prepared(
                                    (root, spaces, scope),
                                    ApplicationWorkPresence::Supplied((work, invoke)),
                                ),
                                publication,
                            )),
                        },
                        families: (),
                        connection: Some(connection),
                    })
                },
                ((), |_executor, (), ()| None),
            );
            drop(execution);
            let (outcome, native, notifications) = receiving.await;
            let startup_report = closing.await;
            let ApplicationOutcome::Unstarted {
                application: (application, network),
                work: (increments, original_calls),
            } = outcome
            else {
                panic!("unpolled startup returns original cold owners");
            };
            assert!(startup_report.is_err());
            assert_eq!(starts.get(), 0);
            assert_eq!(calls.get(), 0);
            assert!(Rc::ptr_eq(&calls, &original_calls));
            assert_eq!(ptr::from_ref(application.as_ref()), original_application);
            assert_eq!(ptr::from_ref(network.as_ref()), original_network);
            assert_eq!(ptr::from_ref(increments.as_ref()), original_work);
            assert_eq!(*increments, 5);
            assert!(native.is_err() && notifications.is_err());
        });
}
