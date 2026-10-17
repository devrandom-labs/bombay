//! Key affinity, complete admission rejection, and joined worker custody.

use bombay::Application;
use bombay::behavior::{ClassifySettlement, MessageProtocol, SettlementStatus};
use bombay::diagnostic::DiagnosticDisposition;
use bombay::lifecycle::ActorDrainPolicy;
use bombay::pool::router::{
    self, BindingCapacity, BindingExpectation, BindingReply, BindingRequestId,
    KeyedAdmissionRejection, KeyedCommand, KeyedOutcome,
};
use bombay::pool::{
    BacklogCapacity, Interruption, PoolFailureReaction, PoolRecovery, SubmissionId,
};
use bombay::prelude::{ActorRetirement, Completion, Exit, MailAddr};
use bombay::worker::{OrderedRoles, StartupLimit, WorkerSubmission};
use tokio::runtime::Builder;

use super::search_worker::{SearchJob, SearchResult, SearchWorker};

#[derive(Debug, Eq, PartialEq)]
enum WorkerRole {
    Primary,
    Replica,
}

type CustomerProtocol =
    MessageProtocol<MailAddr, KeyedOutcome<String, WorkerRole, SearchJob, SearchResult>>;
type BindingProtocol = MessageProtocol<MailAddr, BindingReply<MailAddr, String, WorkerRole>>;

bombay::topology! {
    search_application {
        root { Worker => worker; }
        worker {}
        hosts { customers: CustomerProtocol; bindings: BindingProtocol; }
    }
}

#[test]
#[expect(
    clippy::too_many_lines,
    reason = "one complete affinity, rejection and retirement trace keeps each independent custody oracle"
)]
fn keyed_search_preserves_affinity_rejected_payload_and_joined_worker() {
    let roles = OrderedRoles::new(WorkerRole::Primary, []).expect("the roster is distinct");
    let pool = router::new(
        |_: &WorkerRole| WorkerSubmission::immediate(SearchWorker),
        roles,
        |key: &String| match key.as_str() {
            "account" => WorkerRole::Primary,
            _ => WorkerRole::Replica,
        },
        router::Config {
            startup: StartupLimit::new(1).expect("one worker fits activation capacity"),
            recovery: PoolRecovery::temporary(PoolFailureReaction::RetireRole),
            backlog: BacklogCapacity::new(8),
            bindings: BindingCapacity::new(2).expect("two retained bindings are positive capacity"),
            interruption: Interruption::Fail,
            shutdown: ActorDrainPolicy::WaitForActorGraph,
            diagnostics: DiagnosticDisposition::terminate(),
        },
    )
    .unwrap_or_else(|_| panic!("the declared worker is prepared"));
    let application_host = Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("the caller owns its current-thread host");
    let (outcome, _root_report, native, notifications, ()) = application_host
        .block_on(
            Application::with_topology(pool, search_application::Topology::new()).run_with(
                |application| async move {
                    let mut customer = application
                        .client::<CustomerProtocol>()
                        .expect("the typed customer is established");
                    let mut generations = Vec::new();
                    let mut jobs = Vec::new();
                    for submission in [SubmissionId::new(7), SubmissionId::new(11)] {
                        customer
                            .send(KeyedCommand::submit(
                                submission,
                                String::from("account"),
                                SearchJob {
                                    document: String::from("bombay behavior"),
                                    needle: 'b',
                                },
                                customer.recipient(),
                            ))
                            .await
                            .expect("the pool admits the search command");
                        let accepted = customer.receive().await.expect("admission reply").message;
                        let KeyedOutcome::Accepted {
                            submission: accepted_submission,
                            job,
                            binding,
                        } = accepted
                        else {
                            panic!("search enters the exact affinity binding");
                        };
                        assert_eq!(accepted_submission, submission);
                        assert_eq!(binding.role(), &WorkerRole::Primary);
                        let completed = customer.receive().await.expect("worker reply").message;
                        let KeyedOutcome::Completed {
                            job: completed_job,
                            binding: completed_binding,
                            worker_result,
                        } = completed
                        else {
                            panic!("the accepted assignment completes");
                        };
                        assert_eq!(completed_job, job);
                        assert_eq!(completed_binding.role(), binding.role());
                        assert_eq!(completed_binding.generation(), binding.generation());
                        assert_eq!(worker_result.matches, 3);
                        jobs.push(job);
                        generations.push(binding.generation().clone());

                        let key = String::from("unavailable-account");
                        let key_allocation = key.as_ptr() as usize;
                        let document = String::from("the rejected owned document");
                        let document_allocation = document.as_ptr() as usize;
                        customer
                            .send(KeyedCommand::submit(
                                submission,
                                key,
                                SearchJob {
                                    document,
                                    needle: 'd',
                                },
                                customer.recipient(),
                            ))
                            .await
                            .expect("command admission precedes domain rejection");
                        let rejected = customer
                            .receive()
                            .await
                            .expect("complete rejection")
                            .message;
                        let KeyedOutcome::Rejected {
                            submission: rejected_submission,
                            key,
                            payload,
                            reason,
                        } = rejected
                        else {
                            panic!("an undeclared selected role cannot take ownership");
                        };
                        assert_eq!(rejected_submission, submission);
                        assert_eq!(reason, KeyedAdmissionRejection::UnknownSelectedRole);
                        assert_eq!(key, "unavailable-account");
                        assert_eq!(key.as_ptr() as usize, key_allocation);
                        assert_eq!(payload.document, "the rejected owned document");
                        assert_eq!(payload.document.as_ptr() as usize, document_allocation);
                        assert_eq!(payload.needle, 'd');
                    }
                    assert_ne!(jobs[0], jobs[1]);
                    assert_eq!(generations[0], generations[1]);
                    let mut bindings = application
                        .client::<BindingProtocol>()
                        .expect("binding management owns its distinct reply lane");
                    let request = BindingRequestId::new(19);
                    bindings
                        .send(KeyedCommand::unbind(
                            request,
                            String::from("account"),
                            BindingExpectation::Exact(generations[0].clone()),
                            bindings.recipient(),
                        ))
                        .await
                        .expect("the exact-generation unbind is admitted");
                    let reply = bindings.receive().await.expect("binding reply").message;
                    let BindingReply::Unbound {
                        request: received_request,
                        key,
                        removed,
                    } = reply
                    else {
                        panic!("the exact retained binding is removed");
                    };
                    assert_eq!(received_request, request);
                    assert_eq!(key, "account");
                    assert_eq!(removed.role(), &WorkerRole::Primary);
                    assert_eq!(removed.generation(), &generations[0]);
                    customer
                        .send(KeyedCommand::shutdown())
                        .await
                        .expect("the pool admits orderly drain");
                    let termination = application.termination().await;
                    assert_eq!(termination, Ok(Exit::Normal));
                },
            ),
        )
        .unwrap_or_else(|failed| {
            drop(failed);
            panic!("the complete application returns its joined custody");
        })
        .into_parts();
    drop(application_host);
    let ((), cleanup) = outcome.try_into_completed().unwrap_or_else(|original| {
        drop(original);
        panic!("business completion retains its independent cleanup");
    });
    cleanup.expect("the actual cleanup producer completed independently");
    let notifications = notifications.expect("both original notification receipts survive");
    notifications
        .termination
        .expect("actual ordinary termination notification");
    notifications
        .retirement
        .expect("actual joined retirement notification");
    let (origin, retirement) = native.expect("the exact root retirement is received");
    assert_eq!(origin.address(), MailAddr::APPLICATION_ROOT);
    assert_eq!(retirement.completion(), Some(&Completion::Stopped));
    let ActorRetirement::Completed {
        custody,
        settlements,
        interpretation,
        source,
        ..
    } = retirement
    else {
        panic!("the pool completes orderly retirement");
    };
    assert_eq!(settlements.settlement_status(), SettlementStatus::Accepted);
    assert!(interpretation.is_none());
    assert!(source.is_none());
    assert!(custody.control.is_empty());
    assert!(custody.user.is_empty());
    assert!(custody.additional_failures.is_empty());
    assert!(custody.received_interpretation.is_none());
    assert!(custody.received_source.is_none());
    assert!(custody.source_index.is_none());
    assert!(custody.acquired_ingress.is_none());
    assert!(custody.retirement_failures.is_empty());
    assert!(custody.terminal_report.is_none());
    assert!(custody.capability_failures.is_empty());
    assert!(custody.child_failures.is_empty());
    assert!(custody.unread_owner_cancellation.is_none());
    assert_eq!(custody.descendants.len(), 1);
    let search_application::RootRetirements { origin, retirement } = custody
        .descendants
        .into_iter()
        .next()
        .expect("the exact worker remains owned");
    assert_ne!(origin.address(), MailAddr::APPLICATION_ROOT);
    assert_eq!(retirement.completion(), Some(&Completion::Stopped));
    let ActorRetirement::Completed {
        custody,
        settlements,
        interpretation,
        source,
        ..
    } = *retirement
    else {
        panic!("the worker receives orderly shutdown");
    };
    assert_eq!(settlements.settlement_status(), SettlementStatus::Accepted);
    assert!(interpretation.is_none());
    assert!(source.is_none());
    assert!(custody.control.is_empty());
    assert!(custody.user.is_empty());
    assert!(custody.additional_failures.is_empty());
    assert!(custody.received_interpretation.is_none());
    assert!(custody.received_source.is_none());
    assert!(custody.source_index.is_none());
    assert!(custody.acquired_ingress.is_none());
    assert!(custody.retirement_failures.is_empty());
    assert!(custody.terminal_report.is_none());
    assert_eq!(custody.descendants, []);
    assert_eq!(custody.child_failures, []);
    assert!(custody.capability_failures.is_empty());
    assert!(custody.unread_owner_cancellation.is_none());
}
