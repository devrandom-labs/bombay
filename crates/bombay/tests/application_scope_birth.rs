//! Early external endpoints and one root share one affine application scope.

use bombay::actors::ActorExt as _;
use bombay::atomic::{
    ActivationPolicy, ActorDrainPolicy, CancelAuthority, CancellationReceipt,
    DiagnosticDisposition, DynamicCommand, DynamicLifecycle, DynamicStatus, EntryCapacity,
    EntryRetirement, ImmediateActivation, QueryReply, ReplaceRejection, StartRejection,
    StopRejection, UnexpectedExit, WorkerChangeReceipt, WorkerChangeRejection, WorkerSubmission,
    dynamic,
};
use bombay::behavior::{
    Actions, BehaviorActed, BehaviorBase, EstablishedDelivery, EstablishedRecipient,
    MessageProtocol,
};
use bombay::prelude::{ActorRetirement, Completion, Exit, MailAddr, StopOnShutdown};
use bombay::{ActorNotificationReceipts, ApplicationScope};
use std::collections::BTreeMap;
use std::ptr;
use tokio::runtime::Builder;

type Values = MessageProtocol<MailAddr, u64>;

struct Worker {
    value: Box<u64>,
}

enum WorkerCommand {
    Read(EstablishedRecipient<Values>),
}

#[bombay::actor(sends = { values: Vec<EstablishedDelivery<Values>> })]
impl Worker {
    fn receive(&mut self, message: WorkerCommand) -> BehaviorActed<Self> {
        match message {
            WorkerCommand::Read(reply) => {
                Ok(Actions::cont().send_values(EstablishedDelivery::new(reply, *self.value)))
            }
        }
    }
}

type ManagedWorker = StopOnShutdown<Worker>;
type Lifecycle =
    MessageProtocol<MailAddr, DynamicLifecycle<String, ManagedWorker, ImmediateActivation>>;
type Starts = MessageProtocol<
    MailAddr,
    Result<
        WorkerChangeReceipt<String>,
        WorkerChangeRejection<String, ManagedWorker, ImmediateActivation, StartRejection>,
    >,
>;
type Replacements = MessageProtocol<
    MailAddr,
    Result<
        WorkerChangeReceipt<String>,
        WorkerChangeRejection<String, ManagedWorker, ImmediateActivation, ReplaceRejection>,
    >,
>;
type Stops = MessageProtocol<MailAddr, Result<String, StopRejection<String>>>;
type Queries = MessageProtocol<MailAddr, QueryReply<String, Worker>>;
type Cancellations =
    MessageProtocol<MailAddr, CancellationReceipt<String, ManagedWorker, ImmediateActivation>>;

bombay::topology! {
    service_application {
        root { Proxy => proxy; }
        proxy { Worker => worker; }
        worker {}
        hosts {
            starts: Starts;
            replacements: Replacements;
            stops: Stops;
            queries: Queries;
            cancellations: Cancellations;
        }
    }
}

#[test]
#[expect(
    clippy::too_many_lines,
    reason = "Keep the full start, exact rejection reuse, cancellation, stop and joined custody oracle together."
)]
fn dynamic_service_retains_early_lifecycle_and_exact_rejection_and_retirement() {
    let scope = ApplicationScope::new();
    let mut lifecycle = scope
        .external::<Lifecycle>()
        .expect("durable lifecycle endpoint");
    let lifecycle_address = lifecycle.address();
    let root = dynamic(
        EntryCapacity::new(2).expect("bounded service table"),
        ActivationPolicy::new(1).expect("one active preparation"),
        UnexpectedExit::KeepEmpty,
        ActorDrainPolicy::WaitForActorGraph,
        lifecycle.recipient(),
        DiagnosticDisposition::terminate(),
    );
    let worker_value = Box::new(41);
    let worker_pointer = ptr::from_ref(worker_value.as_ref());
    let rejected_value = Box::new(99);
    let rejected_pointer = ptr::from_ref(rejected_value.as_ref());
    let host = Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("caller host");
    let (outcome, native, notifications, ()) = host
        .block_on(
            scope
                .with_topology(root, service_application::Topology::new())
                .run_with(|application| async move {
                    let mut starts = application.client::<Starts>().expect("management replies");
                    assert_ne!(starts.address(), lifecycle_address);
                    starts
                        .send(DynamicCommand::Start {
                            key: String::from("search"),
                            submission: WorkerSubmission::immediate(
                                Worker {
                                    value: worker_value,
                                }
                                .stop_on_shutdown(),
                            ),
                            reply_to: starts.recipient().into(),
                        })
                        .await
                        .expect("start admitted by mailbox");
                    let receipt = match starts.receive().await.expect("start result").message {
                        Ok(receipt) => receipt,
                        Err(original) => {
                            drop(original);
                            panic!("first start accepted");
                        }
                    };
                    assert_eq!(receipt.key, "search");
                    let proxy = match lifecycle
                        .receive()
                        .await
                        .expect("durable start report")
                        .message
                    {
                        DynamicLifecycle::Started { key, proxy, .. } => {
                            assert_eq!(key, "search");
                            proxy.into_recipient()
                        }
                        original => {
                            drop(original);
                            panic!("successful start reported");
                        }
                    };
                    let mut queries = application.client::<Queries>().expect("query replies");
                    queries
                        .send(DynamicCommand::Query {
                            key: String::from("search"),
                            reply_to: queries.recipient().into(),
                        })
                        .await
                        .expect("query admitted");
                    let reported_status = queries.receive().await.expect("current service").message;
                    let QueryReply::Known {
                        key,
                        status:
                            DynamicStatus::Ready {
                                proxy: queried_proxy,
                            },
                    } = reported_status
                    else {
                        drop(reported_status);
                        panic!("current service is ready");
                    };
                    assert_eq!(key, "search");
                    assert_eq!(proxy, queried_proxy);
                    let mut values = application.external::<Values>().expect("business replies");
                    assert_ne!(values.address(), lifecycle_address);
                    assert_ne!(values.address(), starts.address());
                    values
                        .send(&proxy, WorkerCommand::Read(values.recipient()))
                        .await
                        .expect("business read admitted");
                    let value = values.receive().await.expect("business result");
                    assert_eq!(value.message, 41);

                    starts
                        .send(DynamicCommand::Start {
                            key: String::from("search"),
                            submission: WorkerSubmission::immediate(
                                Worker {
                                    value: rejected_value,
                                }
                                .stop_on_shutdown(),
                            ),
                            reply_to: starts.recipient().into(),
                        })
                        .await
                        .expect("duplicate command admitted");
                    let rejected = match starts.receive().await.expect("complete rejection").message
                    {
                        Err(rejected) => rejected,
                        Ok(original) => {
                            drop(original);
                            panic!("duplicate key rejected");
                        }
                    };
                    assert_eq!(rejected.key, "search");
                    assert_eq!(rejected.reason, StartRejection::AlreadyExists);
                    let WorkerChangeRejection {
                        key: rejected_key,
                        submission,
                        reason,
                    } = rejected;
                    starts
                        .send(DynamicCommand::Start {
                            key: String::from("recovered"),
                            submission,
                            reply_to: starts.recipient().into(),
                        })
                        .await
                        .expect("original rejected submission readmitted");
                    let recovered_receipt = match starts
                        .receive()
                        .await
                        .expect("recovered start result")
                        .message
                    {
                        Ok(receipt) => receipt,
                        Err(original) => {
                            drop(original);
                            panic!("recovered key accepted");
                        }
                    };
                    assert_eq!(recovered_receipt.key, "recovered");
                    let recovered_proxy = match lifecycle
                        .receive()
                        .await
                        .expect("recovered lifecycle")
                        .message
                    {
                        DynamicLifecycle::Started { key, proxy, .. } => {
                            assert_eq!(key, "recovered");
                            proxy.into_recipient()
                        }
                        original => {
                            drop(original);
                            panic!("original submission became ready");
                        }
                    };
                    assert_ne!(proxy, recovered_proxy);
                    values
                        .send(&recovered_proxy, WorkerCommand::Read(values.recipient()))
                        .await
                        .expect("recovered business request");
                    let recovered_value =
                        values.receive().await.expect("recovered business result");
                    assert_eq!(recovered_value.message, 99);

                    let mut cancellations = application
                        .client::<Cancellations>()
                        .expect("cancellation replies");
                    cancellations
                        .send(DynamicCommand::Cancel {
                            authority: receipt.cancel,
                            reply_to: cancellations.recipient().into(),
                        })
                        .await
                        .expect("cancellation query admitted");
                    let authority: CancelAuthority<String> = match cancellations
                        .receive()
                        .await
                        .expect("exact authority returned")
                        .message
                    {
                        CancellationReceipt::Committed { authority, .. } => authority,
                        original => {
                            drop(original);
                            panic!("ready change is already committed");
                        }
                    };
                    assert_eq!(authority.key(), "search");

                    let mut stops = application.client::<Stops>().expect("stop replies");
                    for service in ["search", "recovered"] {
                        stops
                            .send(DynamicCommand::Stop {
                                key: String::from(service),
                                reply_to: stops.recipient().into(),
                            })
                            .await
                            .expect("stop admitted");
                        let accepted_stop = stops.receive().await.expect("stop acceptance").message;
                        assert_eq!(accepted_stop, Ok(String::from(service)));
                        let finished = lifecycle.receive().await.expect("stop completion").message;
                        let DynamicLifecycle::StopFinished {
                            key, result: Ok(_), ..
                        } = finished
                        else {
                            drop(finished);
                            panic!("exact stop completed");
                        };
                        assert_eq!(key, service);
                        let retired = lifecycle.receive().await.expect("entry retirement").message;
                        let DynamicLifecycle::EntryRetired { key, cause, .. } = retired else {
                            drop(retired);
                            panic!("entry retired");
                        };
                        assert_eq!(key, service);
                        assert_eq!(cause, EntryRetirement::Stop);
                    }
                    let shutdown = application.request_shutdown();
                    assert_eq!(shutdown, Ok(()));
                    let termination = application.termination().await;
                    assert_eq!(termination, Ok(Exit::Normal));
                    (rejected_key, reason, authority, recovered_receipt.cancel)
                }),
        )
        .unwrap_or_else(|original| {
            drop(original);
            panic!("application starts");
        })
        .into_parts();
    let (_work, cleanup) = outcome.try_into_completed().unwrap_or_else(|original| {
        drop(original);
        panic!("work and independent cleanup retained");
    });
    let () = cleanup.expect("application cleanup completes");
    let ActorNotificationReceipts {
        termination,
        retirement: retirement_notification,
    } = notifications.expect("independent original notification receipts");
    assert!(termination.is_ok());
    assert!(retirement_notification.is_ok());
    let (origin, retirement) = native.expect("joined root cleanup");
    assert_eq!(origin.address(), MailAddr::APPLICATION_ROOT);
    assert_eq!(retirement.completion(), Some(&Completion::Stopped));
    let ActorRetirement::Completed { custody, .. } = retirement else {
        drop(retirement);
        panic!("root completed");
    };
    assert!(custody.capability_failures.is_empty());
    assert!(custody.child_failures.is_empty());
    assert_eq!(custody.descendants.len(), 2);
    let mut workers = BTreeMap::new();
    for proxy in custody.descendants {
        assert_eq!(proxy.retirement.completion(), Some(&Completion::Stopped));
        let ActorRetirement::Completed { custody, .. } = *proxy.retirement else {
            panic!("proxy completed");
        };
        assert!(custody.capability_failures.is_empty());
        assert!(custody.child_failures.is_empty());
        assert_eq!(custody.descendants.len(), 1);
        let worker = custody.descendants.into_iter().next().expect("one worker");
        assert_eq!(worker.retirement.completion(), Some(&Completion::Stopped));
        let ActorRetirement::Completed { custody, .. } = *worker.retirement else {
            panic!("worker completed");
        };
        let value = custody.behavior.base().value.as_ref();
        let previous = workers.insert(*value, ptr::from_ref(value));
        assert!(previous.is_none());
        assert!(custody.capability_failures.is_empty());
        assert_eq!(custody.child_failures, []);
        assert_eq!(custody.descendants, []);
    }
    assert_eq!(workers.get(&41), Some(&worker_pointer));
    assert_eq!(workers.get(&99), Some(&rejected_pointer));
}
