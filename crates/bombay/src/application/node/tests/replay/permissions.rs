//! Public Export permission and request-key composition.
//! Real Node Sessions/Queryable owners, but no network Query traffic or shipping authority claim.
use super::{Binding, EndpointRole, Metadata, ProofScope, ProtectedMessage, ProviderProfile};
use super::{Counter, CounterMessage, CounterOperation, CounterValue};
use super::{LedgerCommand, LedgerOperation, MailLedger, Receipts, recorded, scheduled};
use super::{Purpose, ReadRequestRef, VerificationError, assert_completed, assert_notifications};
use super::{configuration, current_scope, metadata, reserved_address};
use bombay::actors::ActorExt;
use bombay::behavior::{BehaviorBase, EstablishedRecipient, Protocol, User};
use bombay::prelude::{Application, MailAddr};
use bombay::remote::{
    Admission, Export, ExportError, ExportLimits, IngressError, RequestKey, RequestRefusal, verify,
};
use bombay::{ActorFailureAssessment, ApplicationClient, RetirementAssessment, TrySendError};
use core::ops::AsyncFn;
use serde::Serialize;
use std::cell::{BorrowError, Ref, RefCell};
use std::num::NonZeroUsize;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, TryLockError};
use std::time::{Duration, Instant};
use tokio::runtime::Builder;
use tokio::sync::{OwnedSemaphorePermit, mpsc};
use zenoh::query::Query;
use zenoh::{Error as ZenohError, Session};

#[derive(Debug)]
enum LocalAuthorityRefusal {
    Borrow {
        #[expect(
            dead_code,
            reason = "retain the original opaque native refusal through the public attempt without inventing a payload inspection"
        )]
        cause: BorrowError,
    },
    Scope {
        cause: VerificationError,
    },
    GrantRemoved,
    BindingMismatch,
}
#[derive(Debug)]
enum SynchronizedAuthorityRefusal<'loan> {
    Lock {
        cause: TryLockError<MutexGuard<'loan, Option<Binding>>>,
    },
    Scope {
        cause: VerificationError,
    },
    GrantRemoved,
    BindingMismatch,
}
fn local_current<'loan, Receipt, Operation: Eq>(
    authority: &'loan RefCell<Option<Binding>>,
    receipt: &Receipt,
    projection: fn(&Receipt) -> &ProofScope<Operation>,
    identity: fn(&Receipt) -> (&Binding, u64),
    current: &Metadata,
    operation: &Operation,
    deadline: Instant,
) -> Result<Ref<'loan, Option<Binding>>, LocalAuthorityRefusal> {
    let guard = authority
        .try_borrow()
        .map_err(|cause| LocalAuthorityRefusal::Borrow { cause })?;
    current_scope(
        projection(receipt),
        current,
        operation,
        Purpose::Request,
        deadline,
    )
    .map_err(|cause| LocalAuthorityRefusal::Scope { cause })?;
    match guard.as_ref() {
        None => Err(LocalAuthorityRefusal::GrantRemoved),
        Some(binding) if binding != identity(receipt).0 => {
            Err(LocalAuthorityRefusal::BindingMismatch)
        }
        Some(_) => Ok(guard),
    }
}
fn synchronized_current<'loan, Receipt, Operation: Eq>(
    authority: &'loan Mutex<Option<Binding>>,
    receipt: &Receipt,
    projection: fn(&Receipt) -> &ProofScope<Operation>,
    identity: fn(&Receipt) -> (&Binding, u64),
    current: &Metadata,
    operation: &Operation,
    deadline: Instant,
) -> Result<MutexGuard<'loan, Option<Binding>>, SynchronizedAuthorityRefusal<'loan>> {
    let guard = authority
        .try_lock()
        .map_err(|cause| SynchronizedAuthorityRefusal::Lock { cause })?;
    current_scope(
        projection(receipt),
        current,
        operation,
        Purpose::Request,
        deadline,
    )
    .map_err(|cause| SynchronizedAuthorityRefusal::Scope { cause })?;
    match guard.as_ref() {
        None => Err(SynchronizedAuthorityRefusal::GrantRemoved),
        Some(binding) if binding != identity(receipt).0 => {
            Err(SynchronizedAuthorityRefusal::BindingMismatch)
        }
        Some(_) => Ok(guard),
    }
}

// Fixture scope is independently configured; logical_session is not an SDK/native generation.
fn binding(protocol: &'static str, logical_session: u64) -> Binding {
    Binding {
        deployment: "two-node-read-study",
        caller: "caller",
        controller: "owner",
        actor: protocol,
        host: "receiver",
        protocol,
        target_assignment: "request-key-comparison",
        logical_session,
    }
}
async fn receipt<Operation, Receipt>(
    profile: ProviderProfile,
    session: &Session,
    protocol: &str,
    operation: Operation,
    binding: Binding,
    provider: impl AsyncFn(
        &ProtectedMessage,
        &[ProtectedMessage],
        Binding,
    ) -> Result<Receipt, VerificationError>,
) -> (
    Option<ProtectedMessage>,
    Option<Result<Receipt, VerificationError>>,
)
where
    Operation: Copy + Serialize,
{
    let current = metadata(
        profile,
        protocol,
        session.zid().to_string(),
        session.zid().to_string(),
    );
    let bytes = serde_json::to_vec(&ReadRequestRef {
        bombay: &current,
        kind: Purpose::Request,
        operation,
    })
    .unwrap();
    let records = [ProtectedMessage {
        protected: bytes.clone().into_boxed_slice(),
        proof: Box::from([7, 11]),
    }];
    let original = ProtectedMessage {
        protected: bytes.into_boxed_slice(),
        proof: Box::from([7, 11]),
    };
    let mut verified = None;
    verify(&original, &mut verified, |r| provider(r, &records, binding)).await;
    (Some(original), verified)
}
#[expect(
    clippy::type_complexity,
    reason = "preserve the exact duplicate or refusal product from the public export without another owner"
)]
fn retain_verified<Target, Replies, Receipt>(
    export: &mut Export<Target, Replies, Receipt>,
    original: &mut Option<ProtectedMessage>,
    verified: &mut Option<Result<Receipt, VerificationError>>,
) -> Option<
    Result<
        (RequestKey<Target>, Option<(Box<[u8]>, Box<[u8]>, Receipt)>),
        ((Box<[u8]>, Box<[u8]>, Receipt), ExportError),
    >,
>
where
    Target: Protocol<Addr = MailAddr>,
    Target::Msg: Send + 'static,
    Replies: Protocol<Addr = MailAddr>,
    Replies::Msg: Send,
{
    match verified.take() {
        Some(Ok(receipt)) => {
            let original = original
                .take()
                .expect("the provider borrowed this existing original");
            Some(export.retain(original.protected, original.proof, receipt))
        }
        verification => {
            *verified = verification;
            None
        }
    }
}
async fn undeclare<Target, Replies, Receipt>(
    export: Export<Target, Replies, Receipt>,
) -> (
    Session,
    Vec<(
        RequestKey<Target>,
        Box<[u8]>,
        Box<[u8]>,
        Receipt,
        Admission<Target::Msg>,
        Option<Option<User<MailAddr, Replies::Msg>>>,
        ApplicationClient<EstablishedRecipient<Target>, Replies>,
        Vec<User<MailAddr, Replies::Msg>>,
    )>,
    Result<(), ZenohError>,
    Vec<(Query, OwnedSemaphorePermit)>,
    mpsc::error::TryRecvError,
)
where
    Target: Protocol<Addr = MailAddr>,
    Target::Msg: Send + 'static,
    Replies: Protocol<Addr = MailAddr>,
    Replies::Msg: Send,
{
    let (undeclared, session, mut incoming, retained) = export.undeclare().await;
    let mut entries = Vec::new();
    for (key, protected, proof, receipt, admission, native, mut client) in retained {
        client.close_admission();
        let mut tail = Vec::new();
        while let Some(original) = client.receive().await {
            tail.push(original);
        }
        entries.push((
            key, protected, proof, receipt, admission, native, client, tail,
        ));
    }
    let mut queries = Vec::new();
    let incoming_end = loop {
        match incoming.try_recv() {
            Ok(original) => queries.push(original),
            Err(cause) => break cause,
        }
    };
    (session, entries, undeclared, queries, incoming_end)
}

#[expect(
    clippy::too_many_lines,
    reason = "one Counter trace retains all permission outcomes, original replies and actual cleanup before assertions"
)]
async fn counter_case<Receipt>(
    profile: ProviderProfile,
    provider: impl AsyncFn(
        &ProtectedMessage,
        &[ProtectedMessage],
        Binding,
    ) -> Result<Receipt, VerificationError>,
    projection: fn(&Receipt) -> &ProofScope<CounterOperation>,
    identity: fn(&Receipt) -> (&Binding, u64),
    same_request: fn(&Receipt, &Receipt) -> bool,
) {
    let first_binding = binding("counter", 41);
    let second_binding = binding("counter", 43);
    let authority = RefCell::new(Some(first_binding));
    let cross_calls = AtomicUsize::new(0);
    let same_calls = AtomicUsize::new(0);
    let guard_observation = RefCell::new(None);
    let refusals = Arc::new(Mutex::new(Vec::<IngressError<Query>>::new()));
    let authority = &authority;
    let cross_calls = &cross_calls;
    let same_calls = &same_calls;
    let guard_observation = &guard_observation;
    let refusals = &refusals;
    let first_address = reserved_address();
    let mut node = Application::new(Counter::new().stop_on_shutdown())
        .open_node(configuration(&first_address, &EndpointRole::Receiver))
        .await
        .unwrap_or_else(|_| panic!("actual first Counter Node setup"));
    let connection = node.session.clone();
    let session = &connection;
    let (execution, receiving) = node
        .execute_with(|application| async move {
            let target = application.root().established_recipient();
            let address = application.root().address();
            let callbacks = Arc::clone(refusals);
            let mut first = application
                .export::<_, CounterValue, Receipt>(
                    target.clone(),
                    "bombay/keys/counter/first",
                    ExportLimits {
                        queued_queries: NonZeroUsize::new(2).unwrap(),
                        retained_queries: NonZeroUsize::new(2).unwrap(),
                        retained_requests: NonZeroUsize::new(2).unwrap(),
                    },
                    same_request,
                    move |original| callbacks.lock().unwrap().push(original),
                )
                .await
                .unwrap_or_else(|_| panic!("actual certified Counter export"));
            let callbacks = Arc::clone(refusals);
            let mut other = application
                .export::<_, CounterValue, Receipt>(
                    target,
                    "bombay/keys/counter/other",
                    ExportLimits {
                        queued_queries: NonZeroUsize::new(2).unwrap(),
                        retained_queries: NonZeroUsize::new(2).unwrap(),
                        retained_requests: NonZeroUsize::new(1).unwrap(),
                    },
                    same_request,
                    move |original| callbacks.lock().unwrap().push(original),
                )
                .await
                .unwrap_or_else(|_| panic!("actual second Counter export"));
            let (mut original_a, mut verified_a) = receipt(
                profile,
                session,
                "counter",
                CounterOperation::Read,
                first_binding,
                &provider,
            )
            .await;
            let (mut original_b, mut verified_b) = receipt(
                profile,
                session,
                "counter",
                CounterOperation::Read,
                second_binding,
                &provider,
            )
            .await;
            let (mut original_other, mut verified_other) = receipt(
                profile,
                session,
                "counter",
                CounterOperation::Read,
                first_binding,
                &provider,
            )
            .await;
            let (mut original_duplicate, mut verified_duplicate) = receipt(
                profile,
                session,
                "counter",
                CounterOperation::Read,
                first_binding,
                &provider,
            )
            .await;
            let duplicate_allocations = original_duplicate
                .as_ref()
                .map(|r| (r.protected.as_ptr(), r.proof.as_ptr()));
            let allocations = (
                original_a
                    .as_ref()
                    .map(|r| (r.protected.as_ptr(), r.proof.as_ptr())),
                original_b
                    .as_ref()
                    .map(|r| (r.protected.as_ptr(), r.proof.as_ptr())),
            );
            let retained_a = retain_verified(&mut first, &mut original_a, &mut verified_a);
            let retained_b = retain_verified(&mut first, &mut original_b, &mut verified_b);
            let retained_other =
                retain_verified(&mut other, &mut original_other, &mut verified_other);
            let retained_duplicate =
                retain_verified(&mut first, &mut original_duplicate, &mut verified_duplicate);
            let sender = application.sender().unwrap();
            let primed = sender.send(CounterMessage::Increment).await;
            let second_address = reserved_address();
            let mut foreign = Application::new(Counter::new().stop_on_shutdown())
                .open_node(configuration(&second_address, &EndpointRole::Receiver))
                .await
                .unwrap_or_else(|_| panic!("actual foreign Counter Node setup"));
            let foreign_connection = foreign.session.clone();
            let foreign_session = &foreign_connection;
            let (foreign_execution, foreign_receiving) = foreign
                .execute_with(|foreign_application| async move {
                    let callbacks = Arc::clone(refusals);
                    let mut export = foreign_application
                        .export::<_, CounterValue, Receipt>(
                            foreign_application.root().established_recipient(),
                            "bombay/keys/counter/foreign",
                            ExportLimits {
                                queued_queries: NonZeroUsize::new(2).unwrap(),
                                retained_queries: NonZeroUsize::new(2).unwrap(),
                                retained_requests: NonZeroUsize::new(1).unwrap(),
                            },
                            same_request,
                            move |original| callbacks.lock().unwrap().push(original),
                        )
                        .await
                        .unwrap_or_else(|_| panic!("actual foreign certified export"));
                    let (mut original, mut verified) = receipt(
                        profile,
                        foreign_session,
                        "counter",
                        CounterOperation::Read,
                        first_binding,
                        &provider,
                    )
                    .await;
                    let retained = retain_verified(&mut export, &mut original, &mut verified);
                    let stopped = foreign_application.shutdown();
                    let report = foreign_application.lifecycle().retirement().await;
                    (export, retained, original, verified, stopped, report)
                })
                .unwrap_or_else(|_| panic!("foreign Counter execution"));
            let ((), foreign_outcome) = tokio::join!(foreign_execution, foreign_receiving);
            let (foreign_outcome, _root_report, foreign_native, foreign_notifications, ()) =
                foreign_outcome.into_parts();
            let (
                (
                    foreign_export,
                    foreign_retained,
                    foreign_original,
                    foreign_verified,
                    foreign_stopped,
                    foreign_report,
                ),
                foreign_cleanup,
            ) = foreign_outcome
                .try_into_completed()
                .unwrap_or_else(|_| panic!("foreign original Work"));
            let foreign_export = undeclare(foreign_export).await;
            let foreign_closed = foreign.close().await;
            assert!(foreign_connection.is_closed());
            let current = metadata(
                profile,
                "counter",
                session.zid().to_string(),
                session.zid().to_string(),
            );
            let deadline = Instant::now() + Duration::from_secs(30);
            let mut busy_attempt = None;
            let mut removed_attempt = None;
            let mut scope_attempt = None;
            if let Some(Ok((key, _))) = &retained_a {
                let held_authority = authority.borrow_mut();
                busy_attempt = Some(
                    first
                        .attempt(
                            key,
                            |receipt| {
                                local_current(
                                    authority,
                                    receipt,
                                    projection,
                                    identity,
                                    &current,
                                    &CounterOperation::Read,
                                    deadline,
                                )
                            },
                            CounterMessage::Read,
                        )
                        .map(|_| ()),
                );
                drop(held_authority);
                *authority.borrow_mut() = None;
                removed_attempt = Some(
                    first
                        .attempt(
                            key,
                            |receipt| {
                                local_current(
                                    authority,
                                    receipt,
                                    projection,
                                    identity,
                                    &current,
                                    &CounterOperation::Read,
                                    deadline,
                                )
                            },
                            CounterMessage::Read,
                        )
                        .map(|_| ()),
                );
                *authority.borrow_mut() = Some(first_binding);
                let mut replaced = metadata(
                    profile,
                    "counter",
                    session.zid().to_string(),
                    session.zid().to_string(),
                );
                replaced.authority_generation =
                    replaced.authority_generation.checked_add(1).unwrap();
                scope_attempt = Some(
                    first
                        .attempt(
                            key,
                            |receipt| {
                                local_current(
                                    authority,
                                    receipt,
                                    projection,
                                    identity,
                                    &replaced,
                                    &CounterOperation::Read,
                                    deadline,
                                )
                            },
                            CounterMessage::Read,
                        )
                        .map(|_| ()),
                );
            }
            let mut cross_attempt = None;
            let mut same_attempt = None;
            let mut first_attempt = None;
            let mut second_attempt = None;
            let mut revoked_attempt = None;
            let mut first_receive = None;
            let mut second_receive = None;
            if let Some(Ok((foreign_key, _))) = &foreign_retained {
                cross_attempt = Some(
                    first
                        .attempt(
                            foreign_key,
                            |receipt| {
                                cross_calls.fetch_add(1, Ordering::SeqCst);
                                local_current(
                                    authority,
                                    receipt,
                                    projection,
                                    identity,
                                    &current,
                                    &CounterOperation::Read,
                                    deadline,
                                )
                            },
                            CounterMessage::Read,
                        )
                        .map(|_| ()),
                );
            }
            if let Some(Ok((other_key, _))) = &retained_other {
                same_attempt = Some(
                    first
                        .attempt(
                            other_key,
                            |receipt| {
                                same_calls.fetch_add(1, Ordering::SeqCst);
                                local_current(
                                    authority,
                                    receipt,
                                    projection,
                                    identity,
                                    &current,
                                    &CounterOperation::Read,
                                    deadline,
                                )
                            },
                            CounterMessage::Read,
                        )
                        .map(|_| ()),
                );
            }
            if let Some(Ok((key, _))) = &retained_a {
                first_attempt = Some(
                    first
                        .attempt(
                            key,
                            |receipt| {
                                local_current(
                                    authority,
                                    receipt,
                                    projection,
                                    identity,
                                    &current,
                                    &CounterOperation::Read,
                                    deadline,
                                )
                            },
                            |reply_to| {
                                *guard_observation.borrow_mut() =
                                    Some(authority.try_borrow_mut().map(drop));
                                CounterMessage::Read(reply_to)
                            },
                        )
                        .map(|_| ()),
                );
                if matches!(
                    first.request(key),
                    Some((_, _, _, Admission::Returned(Ok(())), _))
                ) {
                    first_receive = Some(first.receive_native(key).await);
                }
                *authority.borrow_mut() = None;
                revoked_attempt = Some(
                    first
                        .attempt(
                            key,
                            |receipt| {
                                local_current(
                                    authority,
                                    receipt,
                                    projection,
                                    identity,
                                    &current,
                                    &CounterOperation::Read,
                                    deadline,
                                )
                            },
                            CounterMessage::Read,
                        )
                        .map(|_| ()),
                );
            }
            *authority.borrow_mut() = Some(second_binding);
            if let Some(Ok((key, _))) = &retained_b {
                second_attempt = Some(
                    first
                        .attempt(
                            key,
                            |receipt| {
                                local_current(
                                    authority,
                                    receipt,
                                    projection,
                                    identity,
                                    &current,
                                    &CounterOperation::Read,
                                    deadline,
                                )
                            },
                            CounterMessage::Read,
                        )
                        .map(|_| ()),
                );
                if matches!(
                    first.request(key),
                    Some((_, _, _, Admission::Returned(Ok(())), _))
                ) {
                    second_receive = Some(first.receive_native(key).await);
                }
            }
            let stopped = application.shutdown();
            let report = application.lifecycle().retirement().await;
            (
                first,
                other,
                address,
                allocations,
                retained_a,
                retained_b,
                retained_other,
                retained_duplicate,
                original_duplicate,
                verified_duplicate,
                duplicate_allocations,
                original_a,
                verified_a,
                original_b,
                verified_b,
                original_other,
                verified_other,
                busy_attempt,
                removed_attempt,
                scope_attempt,
                cross_attempt,
                same_attempt,
                first_attempt,
                second_attempt,
                revoked_attempt,
                first_receive,
                second_receive,
                primed,
                stopped,
                report,
                foreign_export,
                foreign_retained,
                foreign_original,
                foreign_verified,
                foreign_stopped,
                foreign_report,
                foreign_cleanup,
                foreign_native,
                foreign_notifications,
                foreign_closed,
            )
        })
        .unwrap_or_else(|_| panic!("first Counter execution"));
    let ((), outcome) = tokio::join!(execution, receiving);
    let (outcome, _root_report, native, notifications, ()) = outcome.into_parts();
    let (
        (
            first,
            other,
            address,
            allocations,
            retained_a,
            retained_b,
            retained_other,
            retained_duplicate,
            original_duplicate,
            verified_duplicate,
            duplicate_allocations,
            original_a,
            verified_a,
            original_b,
            verified_b,
            original_other,
            verified_other,
            busy_attempt,
            removed_attempt,
            scope_attempt,
            cross_attempt,
            same_attempt,
            first_attempt,
            second_attempt,
            revoked_attempt,
            first_receive,
            second_receive,
            primed,
            stopped,
            report,
            foreign_export,
            foreign_retained,
            foreign_original,
            foreign_verified,
            foreign_stopped,
            foreign_report,
            foreign_cleanup,
            foreign_native,
            foreign_notifications,
            foreign_closed,
        ),
        cleanup,
    ) = outcome
        .try_into_completed()
        .unwrap_or_else(|_| panic!("first original Counter Work"));
    let first = undeclare(first).await;
    let other = undeclare(other).await;
    let closed = node.close().await;
    assert!(connection.is_closed());
    let native = native.expect("first original Counter native tree");
    let foreign_native = foreign_native.expect("foreign original Counter native tree");
    assert_completed(native, None);
    assert_completed(foreign_native, None);
    assert_notifications(&notifications);
    assert_notifications(&foreign_notifications);
    assert!(closed.is_ok() && foreign_closed.is_ok() && cleanup.is_ok() && foreign_cleanup.is_ok());
    assert!(
        matches!(
            &busy_attempt,
            Some(Err(RequestRefusal::Authority {
                cause: LocalAuthorityRefusal::Borrow { .. }
            }))
        ),
        "native loan error crossed actual attempt"
    );
    assert!(
        matches!(
            &removed_attempt,
            Some(Err(RequestRefusal::Authority {
                cause: LocalAuthorityRefusal::GrantRemoved
            }))
        ),
        "removed grant reached actual admission"
    );
    assert!(
        matches!(
            &scope_attempt,
            Some(Err(RequestRefusal::Authority {
                cause: LocalAuthorityRefusal::Scope {
                    cause: VerificationError::Invalid
                }
            }))
        ),
        "changed scope reached actual admission"
    );
    assert!(
        matches!(&cross_attempt, Some(Err(RequestRefusal::UnknownRequest))),
        "another Application key reached admission"
    );
    assert_eq!(cross_calls.load(Ordering::SeqCst), 0);
    assert!(
        matches!(&same_attempt, Some(Err(RequestRefusal::UnknownRequest))),
        "another export key reached admission"
    );
    assert_eq!(same_calls.load(Ordering::SeqCst), 0);
    assert!(matches!(guard_observation.borrow().as_ref(), Some(Err(_))));
    assert!(matches!(
        &revoked_attempt,
        Some(Err(RequestRefusal::Authority {
            cause: LocalAuthorityRefusal::GrantRemoved
        }))
    ));
    assert!(matches!(&first_attempt, Some(Ok(()))) && matches!(&second_attempt, Some(Ok(()))));
    assert_eq!(first_receive, Some(Some(())));
    assert_eq!(second_receive, Some(Some(())));
    assert!(primed.is_ok() && stopped.is_ok() && foreign_stopped.is_ok());
    assert_eq!(report.retirement(), RetirementAssessment::Established);
    assert_eq!(report.failures(), ActorFailureAssessment::NoFailuresFound);
    assert_eq!(
        foreign_report.retirement(),
        RetirementAssessment::Established
    );
    assert_eq!(
        foreign_report.failures(),
        ActorFailureAssessment::NoFailuresFound
    );
    let (Some(Ok((a, None))), Some(Ok((b, None)))) = (&retained_a, &retained_b) else {
        panic!("distinct retained requests");
    };
    let a_row = first
        .1
        .iter()
        .find(|row| &row.0 == a)
        .expect("actual first issued key");
    let b_row = first
        .1
        .iter()
        .find(|row| &row.0 == b)
        .expect("actual second issued key");
    let (Some(Ok((foreign_key, None))), Some(Ok((other_key, None)))) =
        (&foreign_retained, &retained_other)
    else {
        panic!("actual independent issued keys");
    };
    let foreign_row = foreign_export
        .1
        .iter()
        .find(|row| &row.0 == foreign_key)
        .unwrap();
    let other_row = other.1.iter().find(|row| &row.0 == other_key).unwrap();
    let actual_a = a_row.6.recipient();
    let actual_b = b_row.6.recipient();
    let actual_foreign = foreign_row.6.recipient();
    let actual_other = other_row.6.recipient();
    assert_eq!(a_row.6.address(), foreign_row.6.address());
    assert_ne!(actual_a, actual_foreign);
    assert_ne!(actual_a, actual_other);
    assert_ne!(actual_a, actual_b);
    assert_eq!(first.1.len(), 2);
    assert_eq!(a_row.1, b_row.1);
    assert_ne!(identity(&a_row.3), identity(&b_row.3));
    assert_eq!(allocations.0, Some((a_row.1.as_ptr(), a_row.2.as_ptr())));
    assert_eq!(allocations.1, Some((b_row.1.as_ptr(), b_row.2.as_ptr())));
    assert_ne!(a, b);
    let Some(Ok((duplicate_key, Some(duplicate)))) = &retained_duplicate else {
        panic!("same-request duplicate retains new originals");
    };
    assert_eq!(duplicate_key, a);
    assert_eq!(
        duplicate_allocations,
        Some((duplicate.0.as_ptr(), duplicate.1.as_ptr()))
    );
    assert_eq!(duplicate.0.as_ref(), a_row.1.as_ref());
    assert_eq!(duplicate.1.as_ref(), a_row.2.as_ref());
    assert_eq!(identity(&duplicate.2), identity(&a_row.3));
    assert!(original_duplicate.is_none() && verified_duplicate.is_none());
    for entry in &first.1 {
        let Some(Some(original)) = &entry.5 else {
            panic!("actual Counter User acquired");
        };
        assert_eq!(original.from, address);
        assert_eq!(original.message, 1);
        assert!(matches!(&entry.4, Admission::Returned(Ok(()))));
    }
    for disposed in [&first, &other, &foreign_export] {
        assert!(disposed.2.is_ok());
        assert!(disposed.3.is_empty());
        for row in &disposed.1 {
            assert_eq!(row.7.len(), 0);
        }
        assert!(matches!(
            &disposed.4,
            mpsc::error::TryRecvError::Disconnected
        ));
    }
    assert!(
        original_a.is_none()
            && verified_a.is_none()
            && original_b.is_none()
            && verified_b.is_none()
    );
    assert!(
        original_other.is_none()
            && verified_other.is_none()
            && foreign_original.is_none()
            && foreign_verified.is_none()
    );
    assert!(
        matches!(&retained_other, Some(Ok((_, None))))
            && matches!(&foreign_retained, Some(Ok((_, None))))
    );
    assert!(refusals.lock().unwrap().is_empty());
}

#[expect(
    clippy::too_many_lines,
    reason = "one Ledger trace preserves both Applications and every affine refusal through actual cleanup"
)]
async fn ledger_case<Receipt>(
    profile: ProviderProfile,
    provider: impl AsyncFn(
        &ProtectedMessage,
        &[ProtectedMessage],
        Binding,
    ) -> Result<Receipt, VerificationError>,
    projection: fn(&Receipt) -> &ProofScope<LedgerOperation>,
    identity: fn(&Receipt) -> (&Binding, u64),
    same_request: fn(&Receipt, &Receipt) -> bool,
) {
    let first_binding = binding("ledger", 41);
    let second_binding = binding("ledger", 43);
    let authority = Mutex::new(Some(first_binding));
    let cross_calls = AtomicUsize::new(0);
    let same_calls = AtomicUsize::new(0);
    let guard_observation = RefCell::new(None);
    let refusals = Arc::new(Mutex::new(Vec::<IngressError<Query>>::new()));
    let authority = &authority;
    let cross_calls = &cross_calls;
    let same_calls = &same_calls;
    let guard_observation = &guard_observation;
    let refusals = &refusals;
    let first_address = reserved_address();
    let mut node = Application::new(
        MailLedger {
            entries: Vec::new(),
        }
        .stop_on_shutdown(),
    )
    .open_node(configuration(&first_address, &EndpointRole::Receiver))
    .await
    .unwrap_or_else(|_| panic!("actual first Ledger Node setup"));
    let connection = node.session.clone();
    let session = &connection;
    let (execution, receiving) = node
        .execute_with(|application| async move {
            let target = application.root().established_recipient();
            let address = application.root().address();
            let callbacks = Arc::clone(refusals);
            let mut first = application
                .export::<_, Receipts, Receipt>(
                    target.clone(),
                    "bombay/keys/ledger/first",
                    ExportLimits {
                        queued_queries: NonZeroUsize::new(2).unwrap(),
                        retained_queries: NonZeroUsize::new(2).unwrap(),
                        retained_requests: NonZeroUsize::new(2).unwrap(),
                    },
                    same_request,
                    move |original| callbacks.lock().unwrap().push(original),
                )
                .await
                .unwrap_or_else(|_| panic!("actual certified Ledger export"));
            let callbacks = Arc::clone(refusals);
            let mut other = application
                .export::<_, Receipts, Receipt>(
                    target,
                    "bombay/keys/ledger/other",
                    ExportLimits {
                        queued_queries: NonZeroUsize::new(2).unwrap(),
                        retained_queries: NonZeroUsize::new(2).unwrap(),
                        retained_requests: NonZeroUsize::new(1).unwrap(),
                    },
                    same_request,
                    move |original| callbacks.lock().unwrap().push(original),
                )
                .await
                .unwrap_or_else(|_| panic!("actual second Ledger export"));
            let (mut original_a, mut verified_a) = receipt(
                profile,
                session,
                "ledger",
                LedgerOperation::Read,
                first_binding,
                &provider,
            )
            .await;
            let (mut original_b, mut verified_b) = receipt(
                profile,
                session,
                "ledger",
                LedgerOperation::Read,
                second_binding,
                &provider,
            )
            .await;
            let (mut original_other, mut verified_other) = receipt(
                profile,
                session,
                "ledger",
                LedgerOperation::Read,
                first_binding,
                &provider,
            )
            .await;
            let (mut original_duplicate, mut verified_duplicate) = receipt(
                profile,
                session,
                "ledger",
                LedgerOperation::Read,
                first_binding,
                &provider,
            )
            .await;
            let duplicate_allocations = original_duplicate
                .as_ref()
                .map(|r| (r.protected.as_ptr(), r.proof.as_ptr()));
            let allocations = (
                original_a
                    .as_ref()
                    .map(|r| (r.protected.as_ptr(), r.proof.as_ptr())),
                original_b
                    .as_ref()
                    .map(|r| (r.protected.as_ptr(), r.proof.as_ptr())),
            );
            let retained_a = retain_verified(&mut first, &mut original_a, &mut verified_a);
            let retained_b = retain_verified(&mut first, &mut original_b, &mut verified_b);
            let retained_other =
                retain_verified(&mut other, &mut original_other, &mut verified_other);
            let retained_duplicate =
                retain_verified(&mut first, &mut original_duplicate, &mut verified_duplicate);
            let sender = application.sender().unwrap();
            let caller = sender.address();
            let append = Box::<[u8]>::from([59, 61]);
            let append_allocation = append.as_ptr();
            let primed = sender.send(LedgerCommand::Append(append)).await;
            let second_address = reserved_address();
            let mut foreign = Application::new(
                MailLedger {
                    entries: Vec::new(),
                }
                .stop_on_shutdown(),
            )
            .open_node(configuration(&second_address, &EndpointRole::Receiver))
            .await
            .unwrap_or_else(|_| panic!("actual foreign Ledger Node setup"));
            let foreign_connection = foreign.session.clone();
            let foreign_session = &foreign_connection;
            let (foreign_execution, foreign_receiving) = foreign
                .execute_with(|foreign_application| async move {
                    let callbacks = Arc::clone(refusals);
                    let mut export = foreign_application
                        .export::<_, Receipts, Receipt>(
                            foreign_application.root().established_recipient(),
                            "bombay/keys/ledger/foreign",
                            ExportLimits {
                                queued_queries: NonZeroUsize::new(2).unwrap(),
                                retained_queries: NonZeroUsize::new(2).unwrap(),
                                retained_requests: NonZeroUsize::new(1).unwrap(),
                            },
                            same_request,
                            move |original| callbacks.lock().unwrap().push(original),
                        )
                        .await
                        .unwrap_or_else(|_| panic!("actual foreign certified export"));
                    let (mut original, mut verified) = receipt(
                        profile,
                        foreign_session,
                        "ledger",
                        LedgerOperation::Read,
                        first_binding,
                        &provider,
                    )
                    .await;
                    let retained = retain_verified(&mut export, &mut original, &mut verified);
                    let stopped = foreign_application.shutdown();
                    let report = foreign_application.lifecycle().retirement().await;
                    (export, retained, original, verified, stopped, report)
                })
                .unwrap_or_else(|_| panic!("foreign Ledger execution"));
            let ((), foreign_outcome) = tokio::join!(foreign_execution, foreign_receiving);
            let (foreign_outcome, _root_report, foreign_native, foreign_notifications, ()) =
                foreign_outcome.into_parts();
            let (
                (
                    foreign_export,
                    foreign_retained,
                    foreign_original,
                    foreign_verified,
                    foreign_stopped,
                    foreign_report,
                ),
                foreign_cleanup,
            ) = foreign_outcome
                .try_into_completed()
                .unwrap_or_else(|_| panic!("foreign original Work"));
            let foreign_export = undeclare(foreign_export).await;
            let foreign_closed = foreign.close().await;
            assert!(foreign_connection.is_closed());
            let current = metadata(
                profile,
                "ledger",
                session.zid().to_string(),
                session.zid().to_string(),
            );
            let deadline = Instant::now() + Duration::from_secs(30);
            let mut busy_attempt = None;
            let mut removed_attempt = None;
            let mut scope_attempt = None;
            if let Some(Ok((key, _))) = &retained_a {
                let held_authority = authority.lock().unwrap();
                busy_attempt = Some(
                    first
                        .attempt(
                            key,
                            |receipt| {
                                synchronized_current(
                                    authority,
                                    receipt,
                                    projection,
                                    identity,
                                    &current,
                                    &LedgerOperation::Read,
                                    deadline,
                                )
                            },
                            LedgerCommand::Read,
                        )
                        .map(|_| ()),
                );
                drop(held_authority);
                *authority.lock().unwrap() = None;
                removed_attempt = Some(
                    first
                        .attempt(
                            key,
                            |receipt| {
                                synchronized_current(
                                    authority,
                                    receipt,
                                    projection,
                                    identity,
                                    &current,
                                    &LedgerOperation::Read,
                                    deadline,
                                )
                            },
                            LedgerCommand::Read,
                        )
                        .map(|_| ()),
                );
                *authority.lock().unwrap() = Some(first_binding);
                let mut replaced = metadata(
                    profile,
                    "ledger",
                    session.zid().to_string(),
                    session.zid().to_string(),
                );
                replaced.authority_generation =
                    replaced.authority_generation.checked_add(1).unwrap();
                scope_attempt = Some(
                    first
                        .attempt(
                            key,
                            |receipt| {
                                synchronized_current(
                                    authority,
                                    receipt,
                                    projection,
                                    identity,
                                    &replaced,
                                    &LedgerOperation::Read,
                                    deadline,
                                )
                            },
                            LedgerCommand::Read,
                        )
                        .map(|_| ()),
                );
            }
            let mut cross_attempt = None;
            let mut same_attempt = None;
            let mut first_attempt = None;
            let mut second_attempt = None;
            let mut revoked_attempt = None;
            let mut first_receive = None;
            let mut second_receive = None;
            let mut first_report = None;
            if let Some(Ok((foreign_key, _))) = &foreign_retained {
                cross_attempt = Some(
                    first
                        .attempt(
                            foreign_key,
                            |receipt| {
                                cross_calls.fetch_add(1, Ordering::SeqCst);
                                synchronized_current(
                                    authority,
                                    receipt,
                                    projection,
                                    identity,
                                    &current,
                                    &LedgerOperation::Read,
                                    deadline,
                                )
                            },
                            LedgerCommand::Read,
                        )
                        .map(|_| ()),
                );
            }
            if let Some(Ok((other_key, _))) = &retained_other {
                same_attempt = Some(
                    first
                        .attempt(
                            other_key,
                            |receipt| {
                                same_calls.fetch_add(1, Ordering::SeqCst);
                                synchronized_current(
                                    authority,
                                    receipt,
                                    projection,
                                    identity,
                                    &current,
                                    &LedgerOperation::Read,
                                    deadline,
                                )
                            },
                            LedgerCommand::Read,
                        )
                        .map(|_| ()),
                );
            }
            if let Some(Ok((key, _))) = &retained_a {
                first_attempt = Some(
                    first
                        .attempt(
                            key,
                            |receipt| {
                                synchronized_current(
                                    authority,
                                    receipt,
                                    projection,
                                    identity,
                                    &current,
                                    &LedgerOperation::Read,
                                    deadline,
                                )
                            },
                            |reply_to| {
                                *guard_observation.borrow_mut() =
                                    Some(authority.try_lock().map(drop));
                                LedgerCommand::Read(reply_to)
                            },
                        )
                        .map(|_| ()),
                );
                if matches!(
                    first.request(key),
                    Some((_, _, _, Admission::Returned(Ok(())), _))
                ) {
                    first_receive = Some(first.receive_native(key).await);
                    if matches!(first.request(key), Some((_, _, _, _, Some(Some(_))))) {
                        first_report = Some(application.lifecycle().retirement().await);
                    }
                }
                *authority.lock().unwrap() = None;
                revoked_attempt = Some(
                    first
                        .attempt(
                            key,
                            |receipt| {
                                synchronized_current(
                                    authority,
                                    receipt,
                                    projection,
                                    identity,
                                    &current,
                                    &LedgerOperation::Read,
                                    deadline,
                                )
                            },
                            LedgerCommand::Read,
                        )
                        .map(|_| ()),
                );
            }
            *authority.lock().unwrap() = Some(second_binding);
            if let Some(Ok((key, _))) = &retained_b {
                second_attempt = Some(
                    first
                        .attempt(
                            key,
                            |receipt| {
                                synchronized_current(
                                    authority,
                                    receipt,
                                    projection,
                                    identity,
                                    &current,
                                    &LedgerOperation::Read,
                                    deadline,
                                )
                            },
                            LedgerCommand::Read,
                        )
                        .map(|_| ()),
                );
                if matches!(
                    first.request(key),
                    Some((_, _, _, Admission::Returned(Ok(())), _))
                ) {
                    second_receive = Some(first.receive_native(key).await);
                }
            }
            let stopped = if first_report.is_none() {
                Some(application.shutdown())
            } else {
                None
            };
            let report = application.lifecycle().retirement().await;
            (
                first,
                other,
                address,
                allocations,
                retained_a,
                retained_b,
                retained_other,
                retained_duplicate,
                original_duplicate,
                verified_duplicate,
                duplicate_allocations,
                original_a,
                verified_a,
                original_b,
                verified_b,
                original_other,
                verified_other,
                busy_attempt,
                removed_attempt,
                scope_attempt,
                cross_attempt,
                same_attempt,
                first_attempt,
                second_attempt,
                revoked_attempt,
                first_receive,
                second_receive,
                primed,
                caller,
                append_allocation,
                first_report,
                stopped,
                report,
                foreign_export,
                foreign_retained,
                foreign_original,
                foreign_verified,
                foreign_stopped,
                foreign_report,
                foreign_cleanup,
                foreign_native,
                foreign_notifications,
                foreign_closed,
            )
        })
        .unwrap_or_else(|_| panic!("first Ledger execution"));
    let ((), outcome) = tokio::join!(execution, receiving);
    let (outcome, _root_report, native, notifications, ()) = outcome.into_parts();
    let (
        (
            first,
            other,
            address,
            allocations,
            retained_a,
            retained_b,
            retained_other,
            retained_duplicate,
            original_duplicate,
            verified_duplicate,
            duplicate_allocations,
            original_a,
            verified_a,
            original_b,
            verified_b,
            original_other,
            verified_other,
            busy_attempt,
            removed_attempt,
            scope_attempt,
            cross_attempt,
            same_attempt,
            first_attempt,
            second_attempt,
            revoked_attempt,
            first_receive,
            second_receive,
            primed,
            caller,
            append_allocation,
            first_report,
            stopped,
            report,
            foreign_export,
            foreign_retained,
            foreign_original,
            foreign_verified,
            foreign_stopped,
            foreign_report,
            foreign_cleanup,
            foreign_native,
            foreign_notifications,
            foreign_closed,
        ),
        cleanup,
    ) = outcome
        .try_into_completed()
        .unwrap_or_else(|_| panic!("first original Ledger Work"));
    let first = undeclare(first).await;
    let other = undeclare(other).await;
    let closed = node.close().await;
    assert!(connection.is_closed());
    let native = native.expect("first original Ledger native tree");
    let foreign_native = foreign_native.expect("foreign original Ledger native tree");
    let ledger = native
        .1
        .custody()
        .expect("actual Ledger state")
        .behavior
        .base();
    let [(append_from, append)] = ledger.entries.as_slice() else {
        panic!("one actual append");
    };
    assert_eq!(*append_from, caller);
    assert_eq!(append.as_ptr(), append_allocation);
    assert_eq!(append.as_ref(), [59, 61]);
    assert_eq!(
        foreign_native
            .1
            .custody()
            .expect("foreign actual state")
            .behavior
            .base()
            .entries
            .len(),
        0
    );
    assert_completed(native, None);
    assert_completed(foreign_native, None);
    assert_notifications(&notifications);
    assert_notifications(&foreign_notifications);
    assert!(closed.is_ok() && foreign_closed.is_ok() && cleanup.is_ok() && foreign_cleanup.is_ok());
    assert!(
        matches!(
            &busy_attempt,
            Some(Err(RequestRefusal::Authority {
                cause: SynchronizedAuthorityRefusal::Lock {
                    cause: TryLockError::WouldBlock
                }
            }))
        ),
        "native loan error crossed actual attempt"
    );
    assert!(
        matches!(
            &removed_attempt,
            Some(Err(RequestRefusal::Authority {
                cause: SynchronizedAuthorityRefusal::GrantRemoved
            }))
        ),
        "removed grant reached actual admission"
    );
    assert!(
        matches!(
            &scope_attempt,
            Some(Err(RequestRefusal::Authority {
                cause: SynchronizedAuthorityRefusal::Scope {
                    cause: VerificationError::Invalid
                }
            }))
        ),
        "changed scope reached actual admission"
    );
    assert!(
        matches!(&cross_attempt, Some(Err(RequestRefusal::UnknownRequest))),
        "another Application key reached admission"
    );
    assert_eq!(cross_calls.load(Ordering::SeqCst), 0);
    assert!(
        matches!(&same_attempt, Some(Err(RequestRefusal::UnknownRequest))),
        "another export key reached admission"
    );
    assert_eq!(same_calls.load(Ordering::SeqCst), 0);
    assert!(matches!(
        guard_observation.borrow().as_ref(),
        Some(Err(TryLockError::WouldBlock))
    ));
    assert!(matches!(
        &revoked_attempt,
        Some(Err(RequestRefusal::Authority {
            cause: SynchronizedAuthorityRefusal::GrantRemoved
        }))
    ));
    assert!(matches!(&first_attempt, Some(Ok(()))) && matches!(&second_attempt, Some(Ok(()))));
    assert_eq!(first_receive, Some(Some(())));
    assert_eq!(second_receive, None);
    assert!(primed.is_ok() && stopped.is_none() && foreign_stopped.is_ok());
    let first_report = first_report.expect("actual joined Ledger before second attempt");
    assert_eq!(first_report.retirement(), RetirementAssessment::Established);
    assert_eq!(
        first_report.failures(),
        ActorFailureAssessment::NoFailuresFound
    );
    assert_eq!(report.retirement(), RetirementAssessment::Established);
    assert_eq!(report.failures(), ActorFailureAssessment::NoFailuresFound);
    assert_eq!(
        foreign_report.retirement(),
        RetirementAssessment::Established
    );
    assert_eq!(
        foreign_report.failures(),
        ActorFailureAssessment::NoFailuresFound
    );
    let (Some(Ok((a, None))), Some(Ok((b, None)))) = (&retained_a, &retained_b) else {
        panic!("distinct retained requests");
    };
    let a_row = first
        .1
        .iter()
        .find(|row| &row.0 == a)
        .expect("actual first issued key");
    let b_row = first
        .1
        .iter()
        .find(|row| &row.0 == b)
        .expect("actual second issued key");
    let (Some(Ok((foreign_key, None))), Some(Ok((other_key, None)))) =
        (&foreign_retained, &retained_other)
    else {
        panic!("actual independent issued keys");
    };
    let foreign_row = foreign_export
        .1
        .iter()
        .find(|row| &row.0 == foreign_key)
        .unwrap();
    let other_row = other.1.iter().find(|row| &row.0 == other_key).unwrap();
    let actual_a = a_row.6.recipient();
    let actual_b = b_row.6.recipient();
    let actual_foreign = foreign_row.6.recipient();
    let actual_other = other_row.6.recipient();
    assert_eq!(a_row.6.address(), foreign_row.6.address());
    assert_ne!(actual_a, actual_foreign);
    assert_ne!(actual_a, actual_other);
    assert_ne!(actual_a, actual_b);
    assert_eq!(first.1.len(), 2);
    assert_eq!(a_row.1, b_row.1);
    assert_ne!(identity(&a_row.3), identity(&b_row.3));
    assert_eq!(allocations.0, Some((a_row.1.as_ptr(), a_row.2.as_ptr())));
    assert_eq!(allocations.1, Some((b_row.1.as_ptr(), b_row.2.as_ptr())));
    assert_ne!(a, b);
    let Some(Ok((duplicate_key, Some(duplicate)))) = &retained_duplicate else {
        panic!("same-request duplicate retains new originals");
    };
    assert_eq!(duplicate_key, a);
    assert_eq!(
        duplicate_allocations,
        Some((duplicate.0.as_ptr(), duplicate.1.as_ptr()))
    );
    assert_eq!(duplicate.0.as_ref(), a_row.1.as_ref());
    assert_eq!(duplicate.1.as_ref(), a_row.2.as_ref());
    assert_eq!(identity(&duplicate.2), identity(&a_row.3));
    assert!(original_duplicate.is_none() && verified_duplicate.is_none());
    let Some(Some(original)) = &a_row.5 else {
        panic!("actual first Ledger User");
    };
    assert_eq!(original.from, address);
    assert_eq!(original.message.as_slice(), [caller]);
    assert!(matches!(&a_row.4, Admission::Returned(Ok(()))));
    let Admission::Returned(Err(TrySendError::Closed(LedgerCommand::Read(returned)))) = &b_row.4
    else {
        panic!("actual second Closed(Read) after joined retirement");
    };
    assert_eq!(returned, &actual_b);
    assert!(b_row.5.is_none());
    for disposed in [&first, &other, &foreign_export] {
        assert!(disposed.2.is_ok());
        assert!(disposed.3.is_empty());
        for row in &disposed.1 {
            assert_eq!(row.7.len(), 0);
        }
        assert!(matches!(
            &disposed.4,
            mpsc::error::TryRecvError::Disconnected
        ));
    }
    assert!(
        original_a.is_none()
            && verified_a.is_none()
            && original_b.is_none()
            && verified_b.is_none()
    );
    assert!(
        original_other.is_none()
            && verified_other.is_none()
            && foreign_original.is_none()
            && foreign_verified.is_none()
    );
    assert!(
        matches!(&retained_other, Some(Ok((_, None))))
            && matches!(&foreign_retained, Some(Ok((_, None))))
    );
    assert!(refusals.lock().unwrap().is_empty());
}

#[test]
fn recorded_counter_request_keys_reject_other_exports() {
    Builder::new_multi_thread()
        .worker_threads(1)
        .enable_all()
        .build()
        .unwrap()
        .block_on(counter_case(
            ProviderProfile::Recorded,
            recorded::verify::<CounterOperation>,
            recorded::projection,
            recorded::identity,
            super::same_recorded_counter,
        ));
}
#[test]
fn scheduled_counter_request_keys_reject_other_exports() {
    Builder::new_multi_thread()
        .worker_threads(1)
        .enable_all()
        .build()
        .unwrap()
        .block_on(counter_case(
            ProviderProfile::Scheduled,
            scheduled::verify::<CounterOperation>,
            scheduled::projection,
            scheduled::identity,
            super::same_scheduled_counter,
        ));
}
#[test]
fn recorded_ledger_request_keys_reject_other_exports() {
    Builder::new_multi_thread()
        .worker_threads(1)
        .enable_all()
        .build()
        .unwrap()
        .block_on(ledger_case(
            ProviderProfile::Recorded,
            recorded::verify::<LedgerOperation>,
            recorded::projection,
            recorded::identity,
            super::same_recorded_ledger,
        ));
}
#[test]
fn scheduled_ledger_request_keys_reject_other_exports() {
    Builder::new_multi_thread()
        .worker_threads(1)
        .enable_all()
        .build()
        .unwrap()
        .block_on(ledger_case(
            ProviderProfile::Scheduled,
            scheduled::verify::<LedgerOperation>,
            scheduled::projection,
            scheduled::identity,
            super::same_scheduled_ledger,
        ));
}
