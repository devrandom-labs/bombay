use super::{Counter, CounterMessage, CounterProcessed, CounterValue};
use bombay::actors::ActorExt as _;
use bombay::atomic::{
    ActivationPlan, ActivationPolicy, ActorDrainPolicy, CapabilityResult, DiagnosticDisposition,
    FailureReaction, FixedSnapshot, FixedSupervisor, OrderedRoles, Recovery, StableProxy,
    WorkerActivation, WorkerSubmission, fixed,
};
use bombay::behavior::{Behavior, BehaviorSettlements, Here, MessageProtocol, Never, RecoverEvent};
use bombay::prelude::*;
use bombay::{
    ActorCustody, ActorFailureAssessment, ActorNotificationReceipts, Application,
    RetirementAssessment, RetirementSchema, WorkOutcome,
};
use futures_util::FutureExt;
use std::convert::Infallible;
use std::mem::take;
use std::pin::pin;
use std::sync::Arc;
use tokio::{
    runtime::Builder,
    sync::{OwnedSemaphorePermit, Semaphore, oneshot},
};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum CounterRole {
    Current,
}
struct HeldCounterActivation {
    reached: oneshot::Sender<()>,
    release: oneshot::Receiver<()>,
    preparation_resource: OwnedSemaphorePermit,
}
impl ActivationPlan for HeldCounterActivation {
    type Ready = ();
    type Rejection = oneshot::error::RecvError;
    async fn activate(self) -> Result<Self::Ready, Self::Rejection> {
        self.reached
            .send(())
            .expect("the controller owns the reached receiver");
        let released = self.release.await;
        drop(self.preparation_resource);
        released
    }
}
type CounterProxy = StableProxy<Counter, HeldCounterActivation>;
type CounterSupervisor =
    FixedSupervisor<CounterRole, Counter, HeldCounterActivation, Never, Infallible, Infallible>;
type CounterSupervisorRoot = StopOnShutdown<CounterSupervisor>;
type CounterCapability = MessageProtocol<MailAddr, CapabilityResult<CounterRole, Counter>>;
type CounterStatus = MessageProtocol<MailAddr, FixedSnapshot<Counter>>;

bombay::topology! {
    counter_subtree_application {
        root: CounterSupervisor { Proxy => proxy; }
        proxy { Worker => worker; }
        worker {}
        hosts { capabilities: CounterCapability; status: CounterStatus; }
    }
}
use counter_subtree_application::RootRetirements;
fn held_counter_supervisor(activation: HeldCounterActivation) -> CounterSupervisor {
    let mut activation = Some(activation);
    fixed(
        move |_: &CounterRole| {
            Ok::<_, Infallible>(WorkerSubmission::activated(
                Counter::new(),
                activation
                    .take()
                    .expect("one declared role owns one activation plan"),
            ))
        },
        OrderedRoles::new(CounterRole::Current, []).expect("the declared role is unique"),
        ActivationPolicy::new(1).expect("explicit fixture activation capacity"),
        Recovery::temporary(),
        FailureReaction::StopSupervisor,
        ActorDrainPolicy::WaitForActorGraph,
        DiagnosticDisposition::terminate(),
    )
    .build()
    .unwrap_or_else(|_| panic!("the actual Counter is prepared"))
}
fn joined_counter_descendants<B, Descendants>(
    retirement: &ActorRetirement<B, Descendants, Vec<Descendants::Failure>>,
) -> (&[Descendants], &[Descendants::Failure], &[B::Event])
where
    B: BehaviorSettlements<Protocol: Protocol<Addr = MailAddr>, Ph = Never>,
    Descendants: RetirementSchema,
{
    let (ActorRetirement::Completed {
        custody:
            ActorCustody {
                control,
                received_interpretation,
                received_source,
                source_index,
                acquired_ingress,
                user,
                descendants,
                child_failures,
                capability_failures,
                unread_owner_cancellation,
                additional_failures,
                terminal_report,
                retirement_failures,
                ..
            },
        interpretation,
        source,
        completion: Completion::Stopped,
        ..
    }
    | ActorRetirement::OwnerCancelled {
        custody:
            ActorCustody {
                control,
                received_interpretation,
                received_source,
                source_index,
                acquired_ingress,
                user,
                descendants,
                child_failures,
                capability_failures,
                unread_owner_cancellation,
                additional_failures,
                terminal_report,
                retirement_failures,
                ..
            },
        interpretation,
        source,
        ..
    }) = retirement
    else {
        panic!("the original joined owner is normal or owner-cancelled");
    };
    assert!(interpretation.is_none());
    assert!(source.is_none());
    assert!(received_interpretation.is_none());
    assert!(received_source.is_none());
    assert!(source_index.is_none());
    assert!(acquired_ingress.is_none());
    assert!(user.is_empty());
    assert!(capability_failures.is_empty());
    assert!(unread_owner_cancellation.is_none());
    assert!(additional_failures.is_empty());
    assert!(terminal_report.is_none());
    assert!(retirement_failures.is_empty());
    (descendants, child_failures, control)
}
fn inspect_joined_counter_subtree(
    origin: RootOrigin<CounterSupervisorRoot>,
    terminal: &ActorRetirement<
        CounterSupervisorRoot,
        RootRetirements,
        Vec<<RootRetirements as RetirementSchema>::Failure>,
    >,
    target_address: MailAddr,
) {
    assert_eq!(origin.address(), target_address);
    let (proxies, proxy_failures, root_control) = joined_counter_descendants(terminal);
    eprintln!("NATIVE_ROOT_UNCLAIMED_CONTROLS {}", root_control.len());
    assert!(proxy_failures.is_empty());
    assert_eq!(proxies.len(), 1);
    let RootRetirements { origin, retirement } = &proxies[0];
    let terminal = retirement.as_ref();
    assert_ne!(origin.address(), target_address);
    let (workers, worker_failures, proxy_control) = joined_counter_descendants(terminal);
    eprintln!("NATIVE_PROXY_UNCLAIMED_CONTROLS {}", proxy_control.len());
    assert!(worker_failures.is_empty());
    assert_eq!(workers.len(), 1);
    let origin = &workers[0].origin;
    let terminal = workers[0].retirement.as_ref();
    assert_ne!(origin.address(), target_address);
    let (descendants, worker_failures, worker_control) = joined_counter_descendants(terminal);
    eprintln!("NATIVE_COUNTER_UNCLAIMED_CONTROLS {}", worker_control.len());
    assert_eq!(descendants, []);
    assert_eq!(worker_failures, []);
}
#[test]
#[expect(
    clippy::too_many_lines,
    reason = "One causal trace preserves held activation, lawful release and every native and notification outcome before its observations."
)]
#[expect(
    clippy::result_large_err,
    reason = "Recovery retains each original unboxed proxy event beside the joined native tree without adding an allocation owner."
)]
fn supervisor_retirement_keeps_held_counter_activation_and_independent_progress() {
    let host = Builder::new_current_thread().enable_all().build().unwrap();
    let resource = Arc::new(Semaphore::new(1));
    let preparation_resource = resource.clone().try_acquire_owned().unwrap();
    let (reached, reached_receiver) = oneshot::channel();
    let (release, release_receiver) = oneshot::channel();
    let supervisor = held_counter_supervisor(HeldCounterActivation {
        reached,
        release: release_receiver,
        preparation_resource,
    });
    let spaces = counter_subtree_application::Topology::new();
    let target_outcome = host
        .block_on(
            Application::with_topology(supervisor.stop_on_shutdown(), spaces).run_with(
                move |target| async move {
                    let target_address = target.root().address();
                    let reached_result = reached_receiver.await;
                    let lifecycle = target.lifecycle();
                    let shutdown = lifecycle.request_shutdown();
                    let termination_before = lifecycle.termination().now_or_never();
                    let report_before = lifecycle.retirement().now_or_never();
                    let retirement = lifecycle.retirement();
                    let permits_before = resource.available_permits();
                    let sibling_outcome = Application::new(Counter::new().stop_on_shutdown())
                        .run_with(move |sibling| async move {
                            let mut retirement = pin!(retirement);
                            let borrowed_before = retirement.as_mut().now_or_never();
                            let captured = sibling.root().established_recipient();
                            let sibling_address = sibling.root().address();
                            let interface = sibling.interface(());
                            let mut processed = interface.external::<CounterProcessed>().unwrap();
                            let mut values = interface.external::<CounterValue>().unwrap();
                            let increment = processed
                                .send(
                                    &captured,
                                    CounterMessage::IncrementAndReply {
                                        request_sequence: 0,
                                        reply_to: processed.recipient(),
                                    },
                                )
                                .await;
                            let processing = processed.receive().await;
                            let read = values
                                .send(&captured, CounterMessage::Read(values.recipient()))
                                .await;
                            let value = values.receive().await;
                            let termination_after_progress = lifecycle.termination().now_or_never();
                            let report_after_progress = lifecycle.retirement().now_or_never();
                            let permits_after_progress = resource.available_permits();
                            let release_result = release.send(());
                            let target_report = match borrowed_before {
                                Some(report) => report,
                                None => retirement.await,
                            };
                            let target_termination = lifecycle.termination().await;
                            let replayed_report = lifecycle.retirement().await;
                            let permits_settled = resource.available_permits();
                            let sibling_shutdown = sibling.lifecycle().request_shutdown();
                            let sibling_termination = sibling.lifecycle().termination().await;
                            let sibling_report = sibling.lifecycle().retirement().await;
                            (
                                sibling_address,
                                borrowed_before,
                                increment,
                                processing,
                                read,
                                value,
                                termination_after_progress,
                                report_after_progress,
                                permits_after_progress,
                                release_result,
                                target_report,
                                target_termination,
                                replayed_report,
                                permits_settled,
                                sibling_shutdown,
                                sibling_termination,
                                sibling_report,
                            )
                        })
                        .await;
                    (
                        target_address,
                        reached_result,
                        shutdown,
                        termination_before,
                        report_before,
                        permits_before,
                        sibling_outcome,
                    )
                },
            ),
        )
        .unwrap_or_else(|_| panic!("the target enters the actual caller-owned host"))
        .into_parts();
    drop(host);
    let (
        WorkOutcome::Completed {
            output: observations,
            cleanup: Ok(()),
        },
        Ok((origin, native)),
        Ok(ActorNotificationReceipts {
            termination: Ok(()),
            retirement: Ok(()),
        }),
        (),
    ) = target_outcome
    else {
        panic!("target Work, native result and both notifications remain owned");
    };
    let (
        target_address,
        reached_result,
        shutdown,
        termination_before,
        report_before,
        permits_before,
        sibling_outcome,
    ) = observations;
    let (
        WorkOutcome::Completed {
            output: progress,
            cleanup: Ok(()),
        },
        Ok((sibling_origin, sibling_native)),
        Ok(ActorNotificationReceipts {
            termination: Ok(()),
            retirement: Ok(()),
        }),
        (),
    ) = sibling_outcome
        .unwrap_or_else(|_| panic!("the sibling enters the same caller-owned host"))
        .into_parts()
    else {
        panic!("sibling Work, native result and both notifications remain owned");
    };
    let (
        sibling_address,
        borrowed_before,
        increment,
        processing,
        read,
        value,
        termination_after_progress,
        report_after_progress,
        permits_after_progress,
        release_result,
        target_report,
        target_termination,
        replayed_report,
        permits_settled,
        sibling_shutdown,
        sibling_termination,
        sibling_report,
    ) = progress;
    let mut native = (origin, native);
    inspect_joined_counter_subtree(native.0, &native.1, target_address);
    let terminal = &mut native.1;
    let (ActorRetirement::Completed {
        custody: ActorCustody { descendants, .. },
        ..
    }
    | ActorRetirement::OwnerCancelled {
        custody: ActorCustody { descendants, .. },
        ..
    }) = terminal
    else {
        unreachable!("the native root was checked");
    };
    let terminal = descendants[0].retirement.as_mut();
    let (ActorRetirement::Completed {
        custody:
            ActorCustody {
                control,
                descendants,
                ..
            },
        ..
    }
    | ActorRetirement::OwnerCancelled {
        custody:
            ActorCustody {
                control,
                descendants,
                ..
            },
        ..
    }) = terminal
    else {
        unreachable!("the native proxy was checked");
    };
    let worker_origin = &descendants[0].origin;
    eprintln!("NATIVE_COUNTER_ORIGIN {worker_origin:?}");
    let proxy_controls = take(control);
    let activation_results: Vec<_> = proxy_controls
        .into_iter()
        .map(|event| {
            <<CounterProxy as Behavior>::Event as RecoverEvent<
                WorkerActivation<Counter, HeldCounterActivation>,
                Here,
            >>::recover(event)
            .map(|activation| {
                let worker = activation.worker();
                let ready = activation.into_ready();
                (worker, ready)
            })
        })
        .collect();
    let mut returned_activations = 0;
    for (worker, _) in activation_results.iter().flatten() {
        let creation = worker.creation();
        eprintln!("NATIVE_ACTIVATION_CREATION {creation:?}");
        returned_activations += 1;
    }
    assert!(
        matches!(activation_results.as_slice(), [Ok((_, Ok(())))]),
        "the proxy retains exactly one original Ready activation and no recovery alternative"
    );
    eprintln!("NATIVE_RECOVERED_ACTIVATIONS {returned_activations}");
    let ActorRetirement::Completed {
        custody:
            ActorCustody {
                child_failures,
                capability_failures,
                additional_failures,
                retirement_failures,
                descendants,
                ..
            },
        completion: Completion::Stopped,
        ..
    } = &sibling_native
    else {
        panic!("the independent Counter retains its completed original");
    };
    assert_eq!(child_failures.as_slice(), []);
    assert!(capability_failures.is_empty());
    assert!(additional_failures.is_empty());
    assert!(retirement_failures.is_empty());
    assert_eq!(descendants.as_slice(), []);
    assert_eq!(sibling_origin.address(), sibling_address);
    assert!(reached_result.is_ok());
    assert!(shutdown.is_ok());
    assert!(increment.is_ok());
    let processing = processing.expect("the actual sibling processing reply is retained");
    assert_eq!(processing.from, sibling_address);
    assert_eq!(processing.message.request_sequence, 0);
    assert!(read.is_ok());
    let value = value.expect("the actual sibling Read reply is retained");
    assert_eq!(value.from, sibling_address);
    assert_eq!(value.message, 1);
    assert!(release_result.is_ok());
    assert!(sibling_shutdown.is_ok());
    assert_eq!(target_termination, Ok(Exit::Normal));
    assert_eq!(sibling_termination, Ok(Exit::Normal));
    assert_eq!(
        target_report.retirement(),
        RetirementAssessment::Established
    );
    assert_eq!(
        target_report.failures(),
        ActorFailureAssessment::NoFailuresFound
    );
    assert_eq!(replayed_report, target_report);
    assert_eq!(
        sibling_report.retirement(),
        RetirementAssessment::Established
    );
    assert_eq!(
        sibling_report.failures(),
        ActorFailureAssessment::NoFailuresFound
    );
    assert_eq!(permits_settled, 1);
    assert_eq!(
        permits_before, 0,
        "the reached Counter activation owns its real resource"
    );
    assert_eq!(
        permits_after_progress, 0,
        "independent progress cannot retire the held resource"
    );
    assert!(
        termination_before.is_none(),
        "accepted Stop is not settled descendant termination"
    );
    assert!(
        termination_after_progress.is_none(),
        "parent termination waits for the held child join"
    );
    assert!(
        report_before.is_none(),
        "accepted Stop is not joined subtree retirement"
    );
    assert!(
        borrowed_before.is_none(),
        "cancelling a borrowed wait keeps the report owner pending"
    );
    assert!(
        report_after_progress.is_none(),
        "joined retirement cannot precede the held child"
    );
}
