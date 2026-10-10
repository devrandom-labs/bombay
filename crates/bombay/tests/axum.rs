#![cfg(feature = "axum")]
use axum::routing::get;
use behavior_actors::ShutdownRequested;
use bombay::ActorNotificationReceipts;
use bombay::ProjectTerminal;
use bombay::behavior::{
    ActionSettlement, Behavior, BehaviorBase, ChildChoice, ChildCons, ChildCreationOutcome,
    CreationKind, CreationSettlement, Creations, EventLayer, ItemSettlement, NoChildren, NoSends,
    SendLayer, SettledItem, Step, Stopped, User,
};
use core::pin::pin;
use std::io::{self, Read, Write};
use std::net::{TcpListener, TcpStream};
use std::panic::resume_unwind;
use std::ptr;
use std::time::Duration;
use tokio::net::TcpListener as TokioTcpListener;
use tokio::runtime::Builder as TokioBuilder;
use tokio::sync::oneshot;
use tokio::task::{JoinHandle, spawn};

use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};

use axum::Router;
use bombay::prelude::*;

use bombay::ApplicationOutcome;

struct Root;

#[bombay::behavior::behavior(addr = MailAddr, message = Never)]
impl Root {
    #[allow(
        clippy::unused_self,
        clippy::unnecessary_wraps,
        reason = "the Behavior initialization contract returns its exact error type"
    )]
    fn init(&mut self) -> BehaviorActed<Self> {
        Ok(Actions::cont())
    }

    #[allow(
        clippy::unused_self,
        reason = "the Behavior receive signature owns self"
    )]
    fn receive(&mut self, _: MailAddr, message: Never) -> BehaviorActed<Self> {
        match message {}
    }
}

#[test]
fn axum_router_receives_the_live_root_reference_exactly_once() {
    let calls = Arc::new(AtomicUsize::new(0));
    let observed = calls.clone();

    let caller = TokioBuilder::new_current_thread()
        .enable_all()
        .build()
        .expect("the caller owns its explicit HTTP host");
    let returned = caller
        .block_on(
            Application::new(Root.stop_on_shutdown()).run_axum::<Never, _, _, (), _>(
                "127.0.0.1:0".parse().expect("socket address parses"),
                move |application| {
                    assert_eq!(application.root().address(), MailAddr(0));
                    observed.fetch_add(1, Ordering::SeqCst);
                    let shutdown = application.lifecycle().request_shutdown();
                    assert_eq!(shutdown, Ok(()));
                    let repeated = application.lifecycle().request_shutdown();
                    assert_eq!(repeated, Err(ShutdownRejection::AlreadyStopping));
                    Router::new()
                },
            ),
        )
        .unwrap_or_else(|failed| {
            drop(failed);
            panic!("the actual caller HTTP host is entered");
        });
    let (
        ApplicationOutcome::Completed {
            output: serving,
            cleanup: Ok(()),
        },
        Ok((origin, retirement)),
        Ok(ActorNotificationReceipts {
            termination: Ok(()),
            retirement: Ok(()),
        }),
    ) = returned
    else {
        panic!("HTTP serving and the complete joined root must both return");
    };
    serving.expect("a normally stopping root gracefully stops Axum");
    drop(caller);

    assert_eq!(calls.load(Ordering::SeqCst), 1);
    let ActorRetirement::Completed {
        child_failures: (),
        capability_failures,
        unread_owner_cancellation,
        settlements,
        control,
        user,
        descendants,
        completion,
        ..
    } = retirement
    else {
        panic!("the shutdown request must complete the published root")
    };
    assert!(capability_failures.is_empty());
    assert!(unread_owner_cancellation.is_none());
    assert_eq!(origin.address(), MailAddr::APPLICATION_ROOT);
    assert_eq!(settlements.len(), 1);
    assert_eq!(control.len(), 0);
    assert_eq!(user.len(), 0);
    assert_eq!(descendants, [] as [Never; 0]);
    assert_eq!(completion, Completion::Stopped);
}

#[test]
fn bind_failure_does_not_activate_or_build_the_router() {
    let occupied = TcpListener::bind("127.0.0.1:0").expect("test port binds");
    let address = occupied.local_addr().expect("bound address is available");
    let calls = Arc::new(AtomicUsize::new(0));
    let observed = calls.clone();

    let caller = TokioBuilder::new_current_thread()
        .enable_all()
        .build()
        .expect("the caller owns its explicit HTTP host");
    let refused = caller
        .block_on(
            Application::new(Root.stop_on_shutdown()).run_axum::<Never, _, _, (), _>(
                address,
                move |_| {
                    observed.fetch_add(1, Ordering::SeqCst);
                    Router::new()
                },
            ),
        )
        .unwrap_or_else(|failed| {
            drop(failed);
            panic!("the actual caller HTTP host is entered");
        });
    let (
        ApplicationOutcome::StagingRejected {
            inputs: Ok((application, router, rejected, bind_error)),
        },
        Err(_),
        Err(_),
    ) = refused
    else {
        panic!("the occupied address must reject a second listener");
    };
    drop((application, router, bind_error));
    drop(caller);

    assert_eq!(rejected, address);
    assert_eq!(calls.load(Ordering::SeqCst), 0);
}

#[test]
fn axum_protocol_inversion_is_compile_checked() {
    let cases = trybuild::TestCases::new();
    cases.compile_fail("tests/compile/fail/axum_wrong_root_protocol.rs");
}

struct HttpWorkerRole;

struct HttpWorker {
    entries: Vec<u64>,
}

#[bombay::actor(message = Never)]
impl HttpWorker {}

#[expect(
    clippy::type_complexity,
    reason = "the concrete declared actor retirement preserves its original composition and failures"
)]
#[derive(TerminalProjection)]
enum HttpTerminal {
    Root {
        origin: RootOrigin<StopOnShutdown<Root>>,
        terminal: ActorRetirement<
            ApplicationBehavior<
                StopOnShutdown<Root>,
                ChildCons<MailAddr, StopOnShutdown<HttpWorker>, NoChildren>,
            >,
            Self,
            (
                Vec<
                    ChildFailure<
                        ChildOrigin<StopOnShutdown<Root>, HttpWorkerRole>,
                        StopOnShutdown<HttpWorker>,
                    >,
                >,
                (),
            ),
        >,
    },
    #[application_actor]
    HttpWorker {
        origin: ChildOrigin<StopOnShutdown<Root>, HttpWorkerRole>,
        terminal: ActorRetirement<StopOnShutdown<HttpWorker>, Self, ()>,
    },
}

#[test]
#[expect(
    clippy::too_many_lines,
    reason = "one actual declared HTTP journey retains all cold inputs, native joins and complete root/child retirements"
)]
fn declared_http_preserves_cold_bind_retry_and_distinct_owner_actor_custody() {
    let occupied = TcpListener::bind("127.0.0.1:0").expect("the actual test listener binds");
    let address = occupied
        .local_addr()
        .expect("the actual bound address exists");
    let calls = Arc::new(AtomicUsize::new(0));
    let invoked = calls.clone();
    let router_calls = calls.clone();
    let child_entries = vec![263_u64, 269, 271];
    let child_allocation = child_entries.as_ptr();
    let router_entries = vec![277_u64, 281, 283];
    let router_allocation = router_entries.as_ptr();
    let (router_started, router_output) = oneshot::channel();
    let application = Application::new(Root.stop_on_shutdown()).child(
        HttpWorkerRole,
        HttpWorker {
            entries: child_entries,
        }
        .stop_on_shutdown(),
    );
    let router = move |application: ApplicationHandle<
        <StopOnShutdown<Root> as Behavior>::Protocol,
        <StopOnShutdown<Root> as Behavior>::Event,
    >| {
        invoked.fetch_add(1, Ordering::SeqCst);
        let lifecycle = application.lifecycle();
        let sent = router_started.send((router_entries, lifecycle.clone()));
        assert!(
            sent.is_ok(),
            "the one original router output transfers to its actual caller"
        );
        Router::new().route(
            "/shutdown",
            get(move || {
                let lifecycle = lifecycle.clone();
                async move {
                    let requested = lifecycle.request_shutdown();
                    assert_eq!(requested, Ok(()));
                    "stopped"
                }
            }),
        )
    };
    let runtime = TokioBuilder::new_current_thread()
        .enable_all()
        .build()
        .expect("the actual caller owns its configured host");
    let journey = runtime.block_on(async move {
        tokio::time::timeout(Duration::from_secs(15), async move {
            let (execution, receiving) = application
                .execute_axum::<HttpTerminal, _, _, _, _>(address, router)
                .unwrap_or_else(|_| panic!("the actual caller host is entered"));
            drop(execution);
            let (ApplicationOutcome::Unstarted {
                application: (application, router, returned_address),
                work: (),
            }, Err(_), Err(_)) = receiving.await
            else {
                panic!("unpolled HTTP execution must return all untouched cold inputs");
            };
            let cold_calls = calls.load(Ordering::SeqCst);
            assert_eq!(returned_address, address);
            let refused = application
                .run_axum::<HttpTerminal, _, _, _, _>(returned_address, router)
                .await
                .unwrap_or_else(|_| panic!("binding still uses the actual entered host"));
            let (ApplicationOutcome::StagingRejected {
                inputs: Ok((application, router, returned_address, bind_error)),
            }, Err(_), Err(_)) = refused
            else {
                panic!(
                    "the real occupied address refuses before actor staging or router invocation"
                );
            };
            let bind_calls = calls.load(Ordering::SeqCst);
            assert_eq!(returned_address, address);
            let mut occupied = Some(occupied);
            let mut application = application;
            let mut router = router;
            let mut router_output = router_output;
            let (outcome, router_entries, response) = loop {
                let (execution, receiving) = application
                    .execute_axum::<HttpTerminal, _, _, _, _>(returned_address, router)
                    .unwrap_or_else(|_| panic!("the exact originals retry on the entered caller host"));
                let mut completion = pin!(async move {
                    execution.await;
                    receiving.await
                });
                tokio::select! {
                    returned = &mut completion => {
                        let (ApplicationOutcome::StagingRejected {
                            inputs: Ok((returned_application, returned_router, retry_address, error)),
                        }, Err(_), Err(_)) = returned else {
                            panic!("an early HTTP result must retain the actual bind refusal and original inputs");
                        };
                        assert_eq!(error.kind(), io::ErrorKind::AddrInUse);
                        assert_eq!(retry_address, address);
                        assert_eq!(calls.load(Ordering::SeqCst), 0);
                        application = returned_application;
                        router = returned_router;
                        drop(error);
                        let released_listener = occupied.take();
                        drop(released_listener);
                        tokio::task::yield_now().await;
                    }
                    started = &mut router_output => {
                        let (router_entries, lifecycle) = started.expect(
                            "the actual router publishes its original non-Clone callback output",
                        );
                        let client = async move {
                            let response = tokio::task::spawn_blocking(move || -> io::Result<Vec<u8>> {
                                let mut connection =
                                    TcpStream::connect_timeout(&address, Duration::from_secs(5))?;
                                connection.set_read_timeout(Some(Duration::from_secs(5)))?;
                                connection.set_write_timeout(Some(Duration::from_secs(5)))?;
                                connection.write_all(
                                    b"GET /shutdown HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n",
                                )?;
                                let mut response = Vec::new();
                                connection.read_to_end(&mut response)?;
                                Ok(response)
                            })
                            .await;
                            match &response {
                                Ok(Ok(_)) => {}
                                Ok(Err(_)) | Err(_) => {
                                    // A real client failure must still release the running root and server.
                                    match lifecycle.request_shutdown() {
                                        Ok(())
                                        | Err(
                                            ShutdownRejection::AlreadyStopping
                                            | ShutdownRejection::AlreadyStopped,
                                        ) => {}
                                    }
                                }
                            }
                            response
                        };
                        let (outcome, response) = tokio::join!(completion, client);
                        break (outcome, router_entries, response);
                    }
                }
            };
            assert!(
                occupied.is_none(),
                "the actual retry refusal was acquired before releasing the original listener",
            );
            (
                cold_calls,
                bind_calls,
                bind_error,
                outcome,
                router_entries,
                response,
            )
        })
        .await
    });
    drop(runtime);
    let (cold_calls, bind_calls, bind_error, outcome, router_entries, response) =
        journey.expect("the actual complete HTTP journey must finish before its watchdog");
    // All real serving/client tasks and the complete root/child tree are joined first.
    assert_eq!(cold_calls, 0);
    assert_eq!(bind_calls, 0);
    assert_eq!(bind_error.kind(), io::ErrorKind::AddrInUse);
    drop(bind_error);
    assert_eq!(router_calls.load(Ordering::SeqCst), 1);
    assert_eq!(router_entries.as_ptr(), router_allocation);
    assert_eq!(router_entries, [277, 281, 283]);
    let response = response
        .expect("the actual native blocking HTTP client joins")
        .expect("the real HTTP request completes before graceful close");
    let (headers, body) = response.split_at(
        response
            .windows(4)
            .position(|separator| separator == b"\r\n\r\n")
            .expect("the actual HTTP response owns a complete header delimiter")
            + 4,
    );
    assert!(headers.starts_with(b"HTTP/1.1 200 OK\r\n"));
    assert_eq!(body, b"stopped");
    let (
        ApplicationOutcome::Completed {
            output: Ok(()),
            cleanup: Ok(()),
        },
        Ok((origin, retirement)),
        Ok(ActorNotificationReceipts {
            termination: Ok(()),
            retirement: Ok(()),
        }),
    ) = outcome
    else {
        panic!("actual graceful serving and all native cleanup facts must coexist");
    };
    let HttpTerminal::Root {
        origin,
        terminal: retirement,
    } = HttpTerminal::project(origin, retirement)
    else {
        panic!(
            "the genuine root projection preserves the original Root owner and composed Actor retirement"
        );
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
    } = retirement
    else {
        panic!("the composed actor retains the actual declared root's stopped retirement");
    };
    let _: &Root = behavior.base();
    drop(behavior);
    assert_eq!(origin.address(), MailAddr::APPLICATION_ROOT);
    assert!(interpretation.is_none());
    assert!(source.is_none());
    assert_eq!(
        control,
        [] as [EventLayer<ShutdownRequested, User<MailAddr, Never>>; 0]
    );
    assert_eq!(user, [] as [User<MailAddr, Never>; 0]);
    assert!(child_failures.is_empty());
    assert!(capability_failures.is_empty());
    assert!(additional_failures.is_empty());
    assert!(received_interpretation.is_none());
    assert!(received_source.is_none());
    assert!(source_index.is_none());
    assert!(acquired_ingress.is_none());
    assert!(retirement_failures.is_empty());
    assert!(terminal_report.is_none());
    assert!(unread_owner_cancellation.is_none());
    assert_eq!(completion, Completion::Stopped);
    let [stopped, initialized] = settlements.try_into().unwrap_or_else(|_| {
        panic!(
            "the active declared actor retains full ordered Stop and committed birth settlements"
        );
    });
    assert_eq!(stopped.sends.owned, NoSends);
    assert_eq!(stopped.sends.inner, NoSends);
    assert_eq!(stopped.become_, Step::Stop(Stopped));
    let CreationSettlement::Settled(empty) = stopped.creations.into_settlement() else {
        panic!("the actual shutdown retains its complete empty creation lane");
    };
    assert!(empty.is_empty());
    assert_eq!(initialized.sends.owned, NoSends);
    assert_eq!(initialized.sends.inner, NoSends);
    assert_eq!(initialized.become_, Step::Continue);
    let CreationSettlement::Settled(creations) = initialized.creations.into_settlement() else {
        panic!("the complete declaration retains its original committed creation");
    };
    let [created] = creations
        .into_iter()
        .collect::<Vec<_>>()
        .try_into()
        .unwrap_or_else(|_| panic!("the one original declaration commits exactly once"));
    let SettledItem::Attempted(ItemSettlement::Accepted(ChildChoice::Head(
        ChildCreationOutcome::Established(committed),
    ))) = created
    else {
        panic!("the actual child receipt retains its committed capability");
    };
    let (created_id, created_kind, committed_actor) = committed.into_parts();
    assert_eq!(created_kind, CreationKind::Birth);
    // Selected EstablishedActor keeps its endpoint opaque; discharge its genuine whole capability.
    drop((created_id, committed_actor));
    assert_eq!(descendants.len(), 1);
    let descendant = descendants
        .pop()
        .expect("the actual single child was joined");
    let HttpTerminal::HttpWorker {
        origin: child_origin,
        terminal,
    } = descendant
    else {
        panic!("the declared role preserves its genuine child origin and retirement");
    };
    assert_ne!(child_origin.address(), origin.address());
    let ActorRetirement::OwnerCancelled {
        behavior,
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
    } = terminal
    else {
        panic!("the live declared child retains its actual parent-owned cancellation");
    };
    let child = behavior.into_inner();
    assert_eq!(child.entries.as_ptr(), child_allocation);
    assert_eq!(child.entries, [263, 269, 271]);
    assert!(interpretation.is_none());
    assert!(source.is_none());
    assert_eq!(
        settlements,
        [] as [ActionSettlement<Creations<Never>, SendLayer<NoSends, NoSends>, Never>; 0]
    );
    assert_eq!(
        control,
        [] as [EventLayer<ShutdownRequested, User<MailAddr, Never>>; 0]
    );
    assert_eq!(user, [] as [User<MailAddr, Never>; 0]);
    assert!(descendants.is_empty());
    assert!(capability_failures.is_empty());
    assert!(additional_failures.is_empty());
    assert!(received_interpretation.is_none());
    assert!(received_source.is_none());
    assert!(source_index.is_none());
    assert!(acquired_ingress.is_none());
    assert!(retirement_failures.is_empty());
    assert!(terminal_report.is_none());
    // Carry the actual unread value through complete acquisition; do not invent a race winner.
    match unread_owner_cancellation {
        None | Some(()) => {}
    }
}

// These are the two former acquire_axum_retirement ownership laws.
// The native task below is a supplied Work-owned retirement input. It is not
// the application root task. Likewise Err(source) is a supplied completed
// serving result from a real bind refusal, not a native Axum serving error.
#[test]
fn failed_work_owned_retirement_preserves_completed_serving_result() {
    let caller = TokioBuilder::new_current_thread()
        .enable_all()
        .build()
        .expect("the test owns its explicit application host");
    let returned = caller.block_on(async {
        let listener = TokioTcpListener::bind("127.0.0.1:0")
            .await
            .expect("the test owns one genuine occupied listener");
        let address = listener
            .local_addr()
            .expect("the listener has its actual address");
        let rejected = TokioTcpListener::bind(address).await;
        let Err(source) = rejected else {
            panic!("the second actual bind is rejected while the first listener lives");
        };
        drop(listener);
        let mut returned = Vec::new();
        for serve in [Ok(()), Err(source)] {
            let original_serve = serve
                .as_ref()
                .err()
                .map(|error| (error.kind(), error.raw_os_error()));
            let payload = Box::new(Arc::new(vec![103_u64, 107]));
            let original_payload = Arc::downgrade(payload.as_ref());
            let original_carrier = ptr::from_ref(payload.as_ref()).cast::<()>();
            let task: JoinHandle<()> = spawn(async move { resume_unwind(payload) });
            let original_task_id = task.id();
            let (execution, receiving) = Application::new(Root.stop_on_shutdown())
                .execute_with::<_, _, Never, (), _, _>(move |application| async move {
                    let supplied_retirement = task.await;
                    let requested = application.lifecycle().request_shutdown();
                    match requested {
                        Ok(())
                        | Err(
                            ShutdownRejection::AlreadyStopping | ShutdownRejection::AlreadyStopped,
                        ) => {}
                    }
                    (serve, supplied_retirement)
                })
                .unwrap_or_else(|failed| {
                    drop(failed);
                    panic!("the actual caller host is entered");
                });
            execution.await;
            let outcome = receiving.await;
            returned.push((
                original_serve,
                original_payload,
                original_carrier,
                original_task_id,
                outcome,
            ));
        }
        returned
    });
    drop(caller);
    // Every supplied task and every actual root/cleanup is joined before oracles.
    for (original_serve, original_payload, original_carrier, original_task_id, outcome) in returned
    {
        let (
            ApplicationOutcome::Completed {
                output: (retained_serve, supplied_retirement),
                cleanup: Ok(()),
            },
            Ok((origin, root_retirement)),
            Ok(ActorNotificationReceipts {
                termination: Ok(()),
                retirement: Ok(()),
            }),
        ) = outcome
        else {
            panic!("completed Work input and the separately joined root must both survive");
        };
        let Err(retirement_failure) = supplied_retirement else {
            panic!("the supplied native task cannot invent a successful retirement result");
        };
        let task_id = retirement_failure.id();
        let task_panicked = retirement_failure.is_panic();
        assert!(task_panicked);
        let payload_count = original_payload.strong_count();
        let payload = retirement_failure.into_panic();
        let received_carrier = ptr::from_ref(payload.as_ref()).cast::<()>();
        drop(payload);
        let discharged_payload = original_payload.strong_count();
        let retained_serve = match retained_serve {
            Ok(()) => None,
            Err(source) => {
                let facts = (source.kind(), source.raw_os_error());
                drop(source);
                Some(facts)
            }
        };
        assert_eq!(task_id, original_task_id);
        assert_eq!(payload_count, 1);
        assert_eq!(received_carrier, original_carrier);
        assert_eq!(discharged_payload, 0);
        assert_eq!(retained_serve, original_serve);
        assert_joined_http_root(origin, root_retirement);
    }
}

#[test]
fn joined_work_owned_retirement_preserves_original_outcome() {
    let values = Arc::new(vec![17_u64, 43]);
    let allocation = Arc::as_ptr(&values);
    let original = Arc::downgrade(&values);
    let caller = TokioBuilder::new_current_thread()
        .enable_all()
        .build()
        .expect("the test owns its explicit application host");
    let outcome = caller.block_on(async {
        let task = spawn(async move { values });
        let (execution, receiving) = Application::new(Root.stop_on_shutdown())
            .execute_with::<_, _, Never, (), _, _>(move |application| async move {
                let supplied_retirement = task.await;
                let requested = application.lifecycle().request_shutdown();
                match requested {
                    Ok(())
                    | Err(ShutdownRejection::AlreadyStopping | ShutdownRejection::AlreadyStopped) =>
                        {}
                }
                (Ok::<(), io::Error>(()), supplied_retirement)
            })
            .unwrap_or_else(|failed| {
                drop(failed);
                panic!("the actual caller host is entered");
            });
        execution.await;
        receiving.await
    });
    drop(caller);
    let (
        ApplicationOutcome::Completed {
            output: (serve, Ok(values)),
            cleanup: Ok(()),
        },
        Ok((origin, root_retirement)),
        Ok(ActorNotificationReceipts {
            termination: Ok(()),
            retirement: Ok(()),
        }),
    ) = outcome
    else {
        panic!("the joined task returns its whole original outcome beside the real root");
    };
    let retained_allocation = Arc::as_ptr(&values);
    let retained_values = values.as_slice().to_vec();
    let count = original.strong_count();
    drop(values);
    assert!(serve.is_ok());
    assert_eq!(retained_allocation, allocation);
    assert_eq!(retained_values, [17, 43]);
    assert_eq!(count, 1);
    assert_eq!(original.strong_count(), 0);
    assert_joined_http_root(origin, root_retirement);
}

// Complete joined no-child root law shared by the actual custody consumers.
#[expect(
    clippy::drop_non_drop,
    reason = "explicitly discharge the original retired pure root before the remaining receipt oracles"
)]
fn assert_joined_http_root(
    origin: RootOrigin<StopOnShutdown<Root>>,
    root_retirement: ActorRetirement<StopOnShutdown<Root>, Never, ()>,
) {
    let ActorRetirement::Completed {
        behavior,
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
        completion,
    } = root_retirement
    else {
        panic!("the separate actual application root completes after its own shutdown");
    };
    let _: &Root = behavior.base();
    drop(behavior);
    assert_eq!(origin.address(), MailAddr::APPLICATION_ROOT);
    assert!(interpretation.is_none());
    assert!(source.is_none());
    assert_eq!(
        control,
        [] as [EventLayer<ShutdownRequested, User<MailAddr, Never>>; 0]
    );
    assert_eq!(user, [] as [User<MailAddr, Never>; 0]);
    assert_eq!(descendants, [] as [Never; 0]);
    assert!(capability_failures.is_empty());
    assert!(additional_failures.is_empty());
    assert!(received_interpretation.is_none());
    assert!(received_source.is_none());
    assert!(source_index.is_none());
    assert!(acquired_ingress.is_none());
    assert!(retirement_failures.is_empty());
    assert!(terminal_report.is_none());
    assert!(unread_owner_cancellation.is_none());
    assert_eq!(completion, Completion::Stopped);
    let [stopped] = settlements.try_into().unwrap_or_else(|_| {
        panic!("the real no-child root retains its one complete shutdown settlement");
    });
    assert_eq!(stopped.sends.owned, NoSends);
    assert_eq!(stopped.sends.inner, NoSends);
    assert_eq!(stopped.become_, Step::Stop(Stopped));
    assert!(stopped.creations.is_empty());
}
