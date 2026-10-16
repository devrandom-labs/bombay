//! Startup reports outlive disposal of the producer that acquired their owners.

use super::{
    Application, ApplicationHandle, ApplicationOutcome, ApplicationScope, ApplicationWorkCustody,
    ApplicationWorkPresence, ApplicationWorkPublication, Counter, CounterMessage, CounterValue,
    PreparedApplication, StructuralOrigins, execute_application_with,
};
use crate::actors::ActorExt;
use crate::behavior::{Behavior, BehaviorBase, Never};
use crate::remote::{
    Connection, Network, NetworkLayout, NodeStartupRefusal, listening_counter_network,
};
use behavior_actors::StopOnShutdown;
use core::future::{Future, poll_fn};
use std::cell::Cell;
use std::error::Error;
use std::panic::{AssertUnwindSafe, catch_unwind, panic_any};
use std::ptr;
use std::rc::Rc;
use std::task::Poll;
use tokio::runtime::{Builder, Handle};
use tokio::sync::oneshot;
use tokio::sync::oneshot::error::TryRecvError;

#[derive(Clone, Copy)]
enum Startup {
    Listening,
    InvalidAddress,
}

struct Disposal {
    cause: Option<Box<u64>>,
    calls: Rc<Cell<usize>>,
    allocation: Rc<Cell<Option<*const u64>>>,
}

impl Drop for Disposal {
    fn drop(&mut self) {
        let cause = self.cause.take().expect("one original disposal cause");
        self.calls.set(self.calls.get() + 1);
        self.allocation.set(Some(ptr::from_ref(cause.as_ref())));
        panic_any(cause);
    }
}

#[test]
fn listening_startup_report_survives_ready_producer_disposal() {
    startup_disposal(Startup::Listening);
}

#[test]
fn refused_startup_report_survives_ready_producer_disposal() {
    startup_disposal(Startup::InvalidAddress);
}

#[test]
fn surrendered_receiving_returns_the_original_startup_report() {
    let observations = {
        let host = Builder::new_multi_thread()
            .worker_threads(1)
            .enable_all()
            .build()
            .unwrap();
        host.block_on(async {
            let mut outcomes = Vec::new();
            for startup in [Startup::Listening, Startup::InvalidAddress] {
                let mut network = Box::new(listening_counter_network());
                if let Startup::InvalidAddress = startup {
                    let NetworkLayout::Peer {
                        listen: Some(listen),
                        ..
                    } = &mut network.layout
                    else {
                        panic!("the owning fixture supplies its actual listening direction");
                    };
                    listen.addresses[0] = "a".repeat(256);
                }
                let original = ptr::from_ref(network.as_ref());
                let (acquire, receiving) = Connection::startup();
                drop(receiving);
                let (connection, published) = acquire(&network).await;
                if let Some(connection) = connection {
                    connection.retire();
                }
                let returned = match published {
                    Ok(()) => None,
                    Err(Ok((closing, received))) => {
                        let joined = closing.await;
                        let closed = received.await;
                        Some(Ok((joined, closed)))
                    }
                    Err(Err(cause)) => Some(Err(cause)),
                };
                outcomes.push((startup, network, original, returned));
            }
            move || {
                assert_eq!(outcomes.len(), 2);
                for (startup, network, original, returned) in outcomes {
                    assert_eq!(ptr::from_ref(network.as_ref()), original);
                    match (startup, returned) {
                        (Startup::Listening, Some(Ok((joined, closed)))) => {
                            assert!(joined.is_ok());
                            assert!(matches!(closed, Ok(Ok(()))));
                        }
                        (Startup::InvalidAddress, Some(Err(refusal))) => {
                            let source = refusal.source().expect("the actual refused native cause");
                            let NodeStartupRefusal::Address { field, cause } = &refusal else {
                                panic!("the actual endpoint parser supplies the refused original");
                            };
                            assert_eq!(*field, "listen/endpoints");
                            assert!(ptr::addr_eq(source, cause.as_ref()));
                        }
                        (_, original) => panic!(
                            "the actual send did not return its original report: {original:?}"
                        ),
                    }
                }
            }
        })
    };
    observations();
}

#[expect(
    clippy::result_large_err,
    clippy::too_many_lines,
    reason = "retain real startup, exact cold owners and every original disposal/native outcome across one unwind cut"
)]
fn startup_disposal(startup: Startup) {
    let observations = {
        let host = Builder::new_multi_thread()
            .worker_threads(1)
            .enable_all()
            .build()
            .unwrap();
        host.block_on(async {
            let application = Box::new(Application::new(Counter::new().stop_on_shutdown()));
            let mut network = listening_counter_network();
            if let Startup::InvalidAddress = startup {
                let NetworkLayout::Peer {
                    listen: Some(listen),
                    ..
                } = &mut network.layout
                else {
                    panic!("the owning fixture supplies its actual listening direction");
                };
                listen.addresses[0] = "a".repeat(256);
            }
            let network = Rc::new(Box::new(network));
            let NetworkLayout::Peer {
                listen: Some(listen),
                ..
            } = &network.layout
            else {
                panic!("the supplied listening address remains explicit");
            };
            let original_address = (listen.addresses[0].as_ptr(), listen.addresses[0].len());
            let original_application = ptr::from_ref(application.as_ref());
            let original_network = ptr::from_ref(network.as_ref().as_ref());
            let calls = Rc::new(Cell::new(0_usize));
            let work = Box::new((5_u64, Rc::clone(&calls)));
            let original_work = ptr::from_ref(work.as_ref());
            let starts = Rc::new(Cell::new(0_usize));
            let startup_calls = Rc::clone(&starts);
            let ready = Rc::new(Cell::new(0_usize));
            let ready_calls = Rc::clone(&ready);
            let preparations = Rc::new(Cell::new(0_usize));
            let preparation_calls = Rc::clone(&preparations);
            let disposals = Rc::new(Cell::new(0_usize));
            let disposed = Rc::new(Cell::new(None));
            let cause = Box::new(71_u64);
            let original_cause = ptr::from_ref(cause.as_ref());
            let disposal = Disposal {
                cause: Some(cause),
                calls: Rc::clone(&disposals),
                allocation: Rc::clone(&disposed),
            };
            let (acquire, report) = Connection::startup();
            let (custody, mut acquired_custody) = oneshot::channel::<()>();
            let (observation, observed_session) = oneshot::channel();
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
                work,
                move |(application, network): &(
                    Box<Application<StopOnShutdown<Counter>, _>>,
                    Rc<Box<Network>>,
                )| {
                    startup_calls.set(startup_calls.get() + 1);
                    let network = Rc::clone(network);
                    let allocations = application.allocations();
                    let mut opening = Box::pin(async move { acquire(&network).await });
                    let mut custody = Some(custody);
                    let mut observation = Some(observation);
                    // This exact outer producer returns the actual Ready output;
                    // its capture panics only when the kernel then disposes it.
                    poll_fn(move |context| {
                        let _original_disposal = &disposal;
                        match opening.as_mut().poll(context) {
                            Poll::Pending => Poll::Pending,
                            Poll::Ready(original) => {
                                ready_calls.set(ready_calls.get() + 1);
                                let (connection, published) = original;
                                let observation = observation
                                    .take()
                                    .expect("one actual Ready publishes its native observation")
                                    .send(
                                        connection
                                            .as_ref()
                                            .map(|connection| connection.session().clone()),
                                    );
                                let custody = custody.take().expect("one original acquired owner");
                                Poll::Ready((
                                    (connection, published, observation, custody),
                                    allocations.clone(),
                                ))
                            }
                        }
                    })
                },
                move |original: ApplicationWorkPublication<_, _, _, _, Never, _>,
                      (connection, published, observation, custody),
                      allocations| {
                    preparation_calls.set(preparation_calls.get() + 1);
                    let Some(connection) = connection else {
                        return Err(original);
                    };
                    let ((application, network), work, publication) = original.into_unstarted();
                    let (root, spaces, (), (), scope) = (*application).into_parts();
                    let scope = scope.unwrap_or(ApplicationScope { allocations });
                    let invoke = move |work: Box<(u64, Rc<Cell<usize>>)>,
                                       application: ApplicationHandle<
                        <Counter as Behavior>::Protocol,
                        <StopOnShutdown<Counter> as Behavior>::Event,
                    >| async move {
                        work.1.set(work.1.get() + 1);
                        let mut client = application
                            .export_client::<_, CounterValue>(
                                application.root().established_recipient(),
                            )
                            .unwrap_or_else(|_| panic!("actual Counter client after preparation"));
                        let mut admissions = Vec::new();
                        for _ in 0..work.0 {
                            admissions.push(client.try_send(CounterMessage::Increment));
                        }
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
                            work,
                            network,
                            published,
                            observation,
                            custody,
                            admissions,
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
            let mut execution = Box::pin(execution);
            let caught = poll_fn(|context| {
                match catch_unwind(AssertUnwindSafe(|| execution.as_mut().poll(context))) {
                    Ok(Poll::Pending) => Poll::Pending,
                    actual => Poll::Ready(actual),
                }
            })
            .await;
            drop(execution);
            let output_custody = acquired_custody.try_recv();
            let observed_session = observed_session.await;
            let (outcome, native, notifications) = receiving.await;
            let (startup_report, forced_close) = if let Err(TryRecvError::Closed) = &output_custody
            {
                let original = report.await;
                let forced_close = match (&original, &observed_session) {
                    (Err(_), Ok(Some(session))) => Some(session.close().await),
                    (Ok(_) | Err(_), _) => None,
                };
                (Some(original), forced_close)
            } else {
                // Failure cleanup is separate from the missing original join/result.
                let forced_close = if let Ok(Some(session)) = &observed_session {
                    Some(session.close().await)
                } else {
                    None
                };
                drop(report);
                (None, forced_close)
            };
            // The original close is acquired only when custody permits waiting.
            // Any failed-custody producer is cancelled by runtime retirement.
            move || {
                let ApplicationOutcome::Unstarted {
                    application: (application, network),
                    work,
                } = outcome
                else {
                    panic!(
                        "producer disposal preserves original cold Application, Network and Work"
                    );
                };
                let Err(original_panic) = caught else {
                    panic!("the actual Ready producer must return its original disposal panic");
                };
                let caught_original = original_panic
                    .downcast_ref::<Box<u64>>()
                    .expect("the original boxed disposal cause remains borrowed");
                assert_eq!(ptr::from_ref(caught_original.as_ref()), original_cause);
                assert_eq!(**caught_original, 71);
                assert_eq!(starts.get(), 1);
                assert_eq!(ready.get(), 1);
                assert_eq!(disposals.get(), 1);
                assert_eq!(disposed.get(), Some(original_cause));
                assert_eq!(preparations.get(), 0);
                assert_eq!(calls.get(), 0);
                assert!(native.is_err() && notifications.is_err());
                assert_eq!(ptr::from_ref(application.as_ref()), original_application);
                assert_eq!(ptr::from_ref(network.as_ref().as_ref()), original_network);
                assert_eq!(ptr::from_ref(work.as_ref()), original_work);
                assert_eq!(work.0, 5);
                assert!(Rc::ptr_eq(&work.1, &calls));
                assert_eq!(Rc::strong_count(&network), 1);
                let NetworkLayout::Peer {
                    listen: Some(listen),
                    ..
                } = &network.layout
                else {
                    panic!("the original listening layout remains owned");
                };
                assert_eq!(
                    (listen.addresses[0].as_ptr(), listen.addresses[0].len()),
                    original_address
                );
                assert_eq!(
                    output_custody,
                    Err(TryRecvError::Closed),
                    "dropping execution must release its original acquired affine owner"
                );
                assert!(
                    matches!(&startup_report, Some(Ok(_))),
                    "independent startup receiving must retain the original report"
                );
                assert!(forced_close.is_none());
                match (startup, observed_session, startup_report) {
                    (
                        Startup::Listening,
                        Ok(Some(session)),
                        Some(Ok(Ok((joined, original_close)))),
                    ) => {
                        let closed = session.is_closed();
                        assert!(joined.is_ok());
                        assert!(matches!(original_close, Ok(Ok(()))));
                        assert!(closed);
                    }
                    (Startup::InvalidAddress, Ok(None), Some(Ok(Err(refusal)))) => {
                        let source = refusal.source().expect("the actual native address cause");
                        let NodeStartupRefusal::Address { field, cause } = &refusal else {
                            panic!("the actual endpoint parser must refuse this address");
                        };
                        assert_eq!(*field, "listen/endpoints");
                        assert!(ptr::addr_eq(source, cause.as_ref()));
                    }
                    (_, _, original) => {
                        panic!("independent startup report was not retained: {original:?}")
                    }
                }
            }
        })
    };
    observations();
}
