//! A decoder unwind must not preempt the existing Node's joined finalization.
use super::protocol::{
    Binding, EndpointRole, ProtectedMessage, ProviderProfile, Purpose, ReadRequest, ReadRequestRef,
    VerificationError, configuration, current_scope, limits, metadata, recorded,
};
use super::{Counter, CounterMessage, CounterValue, LedgerCommand, MailLedger};
use crate::actors::ActorExt;
use crate::application::{Application, ApplicationClient, MailboxCapacity};
use crate::remote::{Attempt, Export, ExportLimits, Metadata, reply, submission};
use crate::{ActorRetirement, MailAddr, TrySendError, WorkOutcome};
use behavior::{EstablishedRecipient, Never, User};
use core::future::{pending, poll_fn};
use core::task::Poll;
use std::cell::{Cell, Ref, RefCell};
use std::mem;
use std::num::NonZeroUsize;
use std::panic::{AssertUnwindSafe, catch_unwind, panic_any};
use std::ptr;
use std::sync::mpsc::{self, RecvTimeoutError};
use std::time::{Duration, Instant};
use tokio::runtime::Builder;
use tokio::sync::oneshot;
use tokio::time::timeout;
use zenoh::query::ConsolidationMode;
use zenoh::sample::Locality;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[expect(
    clippy::too_many_lines,
    clippy::await_holding_refcell_ref,
    reason = "one unwind trace preserves every independent cleanup and original panic fact; its provider table is immutable throughout verification"
)]
async fn decoder_unwind_preserves_joined_node_finalization() {
    timeout(Duration::from_secs(40), async {
        let requests = RefCell::new(Vec::<ProtectedMessage>::new());
        let claims = RefCell::new(None::<Metadata>);
        let joined = RefCell::new(None);
        let retirement = RefCell::new(None);
        let decoded_allocation = Cell::new(None);
        let cause = Box::new(307_u64);
        let allocation = ptr::from_ref(cause.as_ref());
        let cause = RefCell::new(Some(cause));
        let verifications = Cell::new(0);
        let constructors = Cell::new(0);
        let encodings = Cell::new(0);
        let protections = Cell::new(0);
        let binding = Binding {
            deployment: "single-decoder-unwind",
            caller: "caller",
            controller: "owner",
            actor: "ledger",
            host: "receiver",
            protocol: "ledger",
            target_assignment: "ledger",
            logical_session: 1,
        };
        let maximum = NonZeroUsize::new(2).unwrap();
        let deadline = Instant::now() + Duration::from_secs(30);
        let current = |receipt: &recorded::Receipt<()>, runtime: &str, target: MailAddr| {
            let scope = recorded::projection(receipt);
            let actual = claims.borrow();
            let actual = actual.as_ref().expect("the actual binding supplies claims");
            current_scope(scope, actual, &(), Purpose::Request, deadline)?;
            if scope.bombay.runtime_generation.as_ref() != runtime
                || scope.bombay.actor_generation != target.0
            {
                return Err(VerificationError::Invalid);
            }
            Ok(())
        };
        let export = Export::new(
            "bombay/decoder-unwind",
            ExportLimits {
                queued_queries: maximum,
                retained_queries: maximum,
                retained_requests: maximum,
                diagnostics: maximum,
            },
            limits(),
            |bytes: &[u8]| -> Result<(), Never> {
                decoded_allocation.set(Some(bytes.as_ptr()));
                let original = cause.take().expect("one original decoder panic allocation");
                panic_any(original);
            },
            submission(
                |()| {
                    Ok::<_, ()>(|| {
                        constructors.set(constructors.get() + 1);
                        LedgerCommand::Append(Box::from([29, 31]))
                    })
                },
                |_: &recorded::Receipt<()>,
                 _: &Attempt<Result<(), TrySendError<LedgerCommand>>>| {
                    encodings.set(encodings.get() + 1);
                    Ok::<_, Never>(Box::<[u8]>::default())
                },
            ),
            async |bytes: &[u8], proof: &[u8]| {
                verifications.set(verifications.get() + 1);
                let original = ProtectedMessage {
                    protected: bytes.into(),
                    proof: proof.into(),
                };
                recorded::verify::<()>(&original, &requests.borrow(), binding).await
            },
            async |_: &[u8], _: &recorded::Receipt<()>| {
                protections.set(protections.get() + 1);
                Err::<Box<[u8]>, ()>(())
            },
            |left, right| recorded::identity(left) == recorded::identity(right),
            &current,
            &current,
        );
        let node = Application::new(MailLedger {
            entries: Vec::new(),
        })
        .open_node(configuration("tls/localhost:0", &EndpointRole::Receiver))
        .await
        .unwrap_or_else(|_| panic!("the actual Ledger Node opens"));
        let runtime = node.runtime_id();
        // Deliberate owning-test access, never an ordinary application port.
        let session = node.session.clone();
        let (configured, configured_request) = oneshot::channel();
        let mut node = node.exports(export, async |application| {
            let target = application.root().established_recipient();
            let mut actual = metadata(
                ProviderProfile::Recorded,
                "ledger",
                runtime.clone(),
                runtime,
            );
            actual.deployment = binding.deployment.into();
            actual.actor_generation = application.actor_address(&target).unwrap().0;
            let protected = serde_json::to_vec(&ReadRequestRef {
                bombay: &actual,
                kind: Purpose::Request,
                operation: &(),
            })
            .unwrap()
            .into_boxed_slice();
            let proof: Box<[u8]> = [19, 23].into();
            requests.borrow_mut().push(ProtectedMessage {
                protected: protected.clone(),
                proof: proof.clone(),
            });
            *claims.borrow_mut() = Some(actual);
            *joined.borrow_mut() = application.cleanup();
            *retirement.borrow_mut() = Some(application.lifecycle().retirement());
            let transferred = configured.send(ProtectedMessage { protected, proof });
            transferred.unwrap_or_else(|_| panic!("the actual Work awaits its configured request"));
            Ok::<_, Never>(target)
        });
        let mut operation = Box::pin(node.run_with(|_| async {
            let original = configured_request.await.unwrap();
            let incoming = session
                .get("bombay/decoder-unwind")
                .allowed_destination(Locality::SessionLocal)
                .payload(original.protected.to_vec())
                .attachment(original.proof.to_vec())
                .consolidation(ConsolidationMode::None)
                .timeout(Duration::from_secs(20))
                .await
                .expect("the actual query starts before Work remains pending");
            pending::<()>().await;
            incoming
        }));
        let caught = poll_fn(|context| {
            match catch_unwind(AssertUnwindSafe(|| operation.as_mut().poll(context))) {
                Ok(Poll::Pending) => Poll::Pending,
                original => Poll::Ready(original),
            }
        })
        .await;
        let disposed = catch_unwind(AssertUnwindSafe(|| drop(operation)));
        let joined = joined
            .take()
            .expect("actual startup supplies its real cleanup-completion observation");
        let joined = joined.await;
        // Original-source escape gets finite external cleanup, not fake Node evidence.
        let external_close = if caught.is_err() {
            Some(node.close().await)
        } else {
            None
        };
        let finalized = node.into_parts();
        let retirement = retirement
            .take()
            .expect("startup retains a separate consumer of the existing root observation")
            .await;
        let remaining_cause = cause.borrow();
        assert!(joined.is_ok() && disposed.is_ok() && session.is_closed());
        assert!(external_close.as_ref().is_none_or(Result::is_ok));
        assert_eq!(verifications.get(), 1);
        assert_eq!(constructors.get(), 0);
        assert_eq!(encodings.get(), 0);
        assert_eq!(protections.get(), 0);
        assert!(remaining_cause.is_none());
        if let Err(original) = &caught {
            let original = original
                .downcast_ref::<Box<u64>>()
                .expect("the external catcher retains the actual original decoder cause");
            assert_eq!(ptr::from_ref(original.as_ref()), allocation);
            assert_eq!(**original, 307);
        }
        assert!(
            finalized.is_ok(),
            "the Node must preserve its finalization facts after a decoder unwind"
        );
        let (export, bound, closed, polling, disposal) =
            finalized.unwrap_or_else(|_| panic!("the Node acquired its actual post-join close"));
        let Ok(Poll::Ready(Ok(outcome))) = caught else {
            panic!("the actual Node operation must return its acquired outcome");
        };
        let (work, root_report, native, notifications, ()) = outcome.into_parts();
        let (origin, native) =
            native.expect("actual root cancellation remains independently owned");
        let notifications = notifications.expect("both actual root notifications remain owned");
        let (_, _, startup, withdrawal, diagnostics, requests_retained) = export.into_parts();
        let rows = requests_retained
            .into_iter()
            .map(|request| {
                request
                    .try_into_parts()
                    .unwrap_or_else(|_| panic!("kernel disposal releases the actual request lease"))
            })
            .collect::<Vec<_>>();
        let [row] = rows.as_slice() else {
            panic!("one original verified request survives finalization");
        };
        let receipt = row
            .2
            .as_ref()
            .expect("verification returned")
            .as_ref()
            .unwrap();
        let expected = requests.borrow();
        let [expected] = expected.as_slice() else {
            panic!("one independently configured protected request");
        };
        let polling = polling.expect("retain the actual kernel poll panic outside its producer");
        let polling = polling.downcast_ref::<Box<u64>>().unwrap();
        let ActorRetirement::OwnerCancelled { custody, .. } = native else {
            panic!("kernel disposal cancels the original still-running Ledger");
        };
        let claims = claims.borrow();
        let claims = claims
            .as_ref()
            .expect("actual startup metadata remains available");
        assert!(matches!(
            &work,
            WorkOutcome::Interrupted { cleanup: Ok(()) }
        ));
        assert_eq!(root_report, Some(retirement));
        assert!(notifications.termination.is_ok() && notifications.retirement.is_ok());
        assert!(matches!(&bound, Some(Ok(_))) && closed.is_ok());
        assert!(external_close.is_none() && disposal.is_none());
        assert_eq!(ptr::from_ref(polling.as_ref()), allocation);
        assert_eq!(**polling, 307);
        assert_eq!(origin.address().0, claims.actor_generation);
        assert!(custody.behavior.entries.is_empty() && custody.user.is_empty());
        assert!(custody.capability_failures.is_empty() && custody.additional_failures.is_empty());
        assert!(
            custody.unread_owner_cancellation.is_none() && custody.retirement_failures.is_empty()
        );
        assert!(matches!(startup, Some(Ok(()))) && matches!(withdrawal, Some(Ok(()))));
        assert!(diagnostics.is_empty() && row.0.is_none());
        assert_eq!(row.1.0.as_ref(), expected.protected.as_ref());
        assert_eq!(row.1.1.as_ref(), expected.proof.as_ref());
        assert_eq!(Some(row.1.0.as_ptr()), decoded_allocation.get());
        assert_eq!(&recorded::projection(receipt).bombay, claims);
        assert_eq!(recorded::projection(receipt).kind, Purpose::Request);
        assert!(row.3.is_none() && row.4.is_none());
        assert!(row.5.is_none() && row.6.is_none() && row.7.is_empty());
        assert!(row.8.0.is_none() && row.8.1.is_none() && row.8.2.is_none());
        assert!(
            row.9.0.is_none()
                && row.9.1.is_none()
                && row.9.2.is_none()
                && row.9.3.is_none()
                && row.9.4.is_none()
                && row.9.5.is_none()
        );
    })
    .await
    .expect("the complete decoder-unwind and actual cleanup trace terminates");
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum PermissionStage {
    Scope,
    Admission,
    Retry,
}

enum PermissionRelease<'permission> {
    Return,
    FirstNative {
        announced: oneshot::Sender<()>,
        returned: &'permission Cell<Option<Result<(), ()>>>,
    },
    Panic(Box<u64>),
}

struct PermissionLoan<'permission> {
    claims: Ref<'permission, Option<Metadata>>,
    release: PermissionRelease<'permission>,
}

impl Drop for PermissionLoan<'_> {
    fn drop(&mut self) {
        match mem::replace(&mut self.release, PermissionRelease::Return) {
            PermissionRelease::Return => {}
            PermissionRelease::FirstNative {
                announced,
                returned,
            } => {
                returned.set(Some(announced.send(())));
            }
            PermissionRelease::Panic(original) => {
                let target = self.claims.as_ref().map(|claims| claims.actor_generation);
                panic_any((target, original));
            }
        }
    }
}

#[test]
#[expect(
    clippy::too_many_lines,
    clippy::await_holding_refcell_ref,
    reason = "one real second-Full retry retains the original permission panic, native reply endpoint and complete Node finalization; nominal provider tables are immutable during verification"
)]
fn retry_permission_unwind_preserves_the_second_native_full() {
    Builder::new_multi_thread()
        .worker_threads(1)
        .enable_all()
        .build()
        .unwrap()
        .block_on(async {
            timeout(Duration::from_secs(40), async {
                let requests = RefCell::new(Vec::<ProtectedMessage>::new());
                let claims = RefCell::new(None::<Metadata>);
                let joined = RefCell::new(None);
                let retirement = RefCell::new(None);
                let stage = Cell::new(PermissionStage::Scope);
                let grants = RefCell::new(Vec::new());
                let verified_allocations = Cell::new(None);
                let decoded_allocation = Cell::new(None);
                let constructions = Cell::new(0);
                let encodings = Cell::new(0);
                let protections = Cell::new(0);
                let cause = Box::new(401_u64);
                let cause_allocation = ptr::from_ref(cause.as_ref());
                let cause = RefCell::new(Some(cause));
                let ordinary = RefCell::new(
                    None::<ApplicationClient<EstablishedRecipient<Counter>, CounterValue>>,
                );
                let prefix = RefCell::new(Vec::new());
                let first_full = RefCell::new(None);
                let refill = RefCell::new(Vec::new());
                let second_full = RefCell::new(None);
                let first_entered = RefCell::new(None);
                let first_worker = RefCell::new(None);
                let second_worker = RefCell::new(None);
                let second_entered = RefCell::new(None);
                let (release_second, retry_wait) = mpsc::channel::<()>();
                let retry_wait = RefCell::new(Some(retry_wait));
                let (first_returned, returned_first) = oneshot::channel();
                let first_returned = RefCell::new(Some(first_returned));
                let first_return_receipt = Cell::new(None);
                let incoming = RefCell::new(None);
                let binding = Binding {
                    deployment: "second-full-unwind",
                    caller: "caller",
                    controller: "owner",
                    actor: "counter",
                    host: "receiver",
                    protocol: "counter",
                    target_assignment: "counter",
                    logical_session: 1,
                };
                let maximum = NonZeroUsize::new(2).unwrap();
                let deadline = Instant::now() + Duration::from_secs(30);
                let current = |receipt: &recorded::Receipt<()>, runtime: &str, target: MailAddr| {
                    let scope = recorded::projection(receipt);
                    let actual = claims.borrow();
                    {
                        let actual = actual.as_ref().ok_or(VerificationError::Invalid)?;
                        current_scope(scope, actual, &(), Purpose::Request, deadline)?;
                        if scope.bombay.runtime_generation.as_ref() != runtime
                            || scope.bombay.actor_generation != target.0
                        {
                            return Err(VerificationError::Invalid);
                        }
                    }
                    Ok::<_, VerificationError>(actual)
                };
                let current_request =
                    |receipt: &recorded::Receipt<()>, runtime: &str, target: MailAddr| {
                        let claims = current(receipt, runtime, target)?;
                        let actual = stage.get();
                        grants.borrow_mut().push(actual);
                        let release = match actual {
                            PermissionStage::Scope => {
                                stage.set(PermissionStage::Admission);
                                PermissionRelease::Return
                            }
                            PermissionStage::Admission => {
                                stage.set(PermissionStage::Retry);
                                PermissionRelease::FirstNative {
                                    announced: first_returned
                                        .borrow_mut()
                                        .take()
                                        .expect("the first native return has one observer"),
                                    returned: &first_return_receipt,
                                }
                            }
                            PermissionStage::Retry => {
                                // The actual capacity wait has completed. Freeze the
                                // sole actor worker before a real competing sender
                                // consumes that available capacity ahead of retry.
                                let released = retry_wait
                                    .borrow_mut()
                                    .take()
                                    .expect("exactly one actual retry occupies the worker");
                                let (announced, entered) = mpsc::channel::<()>();
                                let worker = tokio::spawn(async move {
                                    let announced = announced.send(());
                                    let released = released.recv_timeout(Duration::from_secs(5));
                                    (announced, released)
                                });
                                *second_worker.borrow_mut() = Some(worker);
                                let entered = entered.recv_timeout(Duration::from_secs(5));
                                *second_entered.borrow_mut() = Some(entered);
                                second_entered.borrow().as_ref().unwrap().as_ref().expect(
                                    "finite setup: the sole actor worker is blocked before refill",
                                );
                                let ordinary = ordinary.borrow();
                                let ordinary = ordinary
                                    .as_ref()
                                    .expect("Work owns an independently allocated actual sender");
                                for _ in 0..2 {
                                    refill
                                        .borrow_mut()
                                        .push(ordinary.try_send(CounterMessage::Increment));
                                }
                                *second_full.borrow_mut() =
                                    Some(ordinary.try_send(CounterMessage::Increment));
                                PermissionRelease::Panic(
                                    cause
                                        .borrow_mut()
                                        .take()
                                        .expect("the actual retry guard owns one original panic"),
                                )
                            }
                        };
                        Ok::<_, VerificationError>(PermissionLoan { claims, release })
                    };
                let export = Export::new(
                    "bombay/second-full-unwind",
                    ExportLimits {
                        queued_queries: maximum,
                        retained_queries: maximum,
                        retained_requests: maximum,
                        diagnostics: maximum,
                    },
                    limits(),
                    |bytes: &[u8]| {
                        decoded_allocation.set(Some(bytes.as_ptr()));
                        let ReadRequest { operation: (), .. } =
                            serde_json::from_slice::<ReadRequest<()>>(bytes)?;
                        Ok::<_, serde_json::Error>(())
                    },
                    reply(
                        |()| {
                            Ok::<_, ()>(|reply_to| {
                                constructions.set(constructions.get() + 1);
                                CounterMessage::Read(reply_to)
                            })
                        },
                        |_: &recorded::Receipt<()>, _: &User<MailAddr, u64>| {
                            encodings.set(encodings.get() + 1);
                            Ok::<_, Never>(Box::<[u8]>::default())
                        },
                    ),
                    async |bytes: &[u8], proof: &[u8]| {
                        verified_allocations.set(Some((bytes.as_ptr(), proof.as_ptr())));
                        let original = ProtectedMessage {
                            protected: bytes.into(),
                            proof: proof.into(),
                        };
                        recorded::verify::<()>(&original, &requests.borrow(), binding).await
                    },
                    async |_: &[u8], _: &recorded::Receipt<()>| {
                        protections.set(protections.get() + 1);
                        Err::<Box<[u8]>, ()>(())
                    },
                    |left, right| recorded::identity(left) == recorded::identity(right),
                    &current_request,
                    &current,
                );
                let node = Application::new(Counter::new().stop_on_shutdown())
                    .mailbox_capacity(MailboxCapacity::new(2).unwrap())
                    .open_node(configuration("tls/localhost:0", &EndpointRole::Receiver))
                    .await
                    .unwrap_or_else(|_| panic!("the real bounded Counter Node opens"));
                let runtime = node.runtime_id();
                let session = node.session.clone();
                let (configured, configured_request) = oneshot::channel();
                let mut node = node.exports(export, async |application| {
                    let target = application.root().established_recipient();
                    let mut actual = metadata(
                        ProviderProfile::Recorded,
                        "counter",
                        runtime.clone(),
                        runtime,
                    );
                    actual.deployment = binding.deployment.into();
                    actual.actor_generation = application.actor_address(&target).unwrap().0;
                    let protected = serde_json::to_vec(&ReadRequestRef {
                        bombay: &actual,
                        kind: Purpose::Request,
                        operation: &(),
                    })
                    .unwrap()
                    .into_boxed_slice();
                    let proof: Box<[u8]> = [43, 47].into();
                    requests.borrow_mut().push(ProtectedMessage {
                        protected: protected.clone(),
                        proof: proof.clone(),
                    });
                    *claims.borrow_mut() = Some(actual);
                    *joined.borrow_mut() = application.cleanup();
                    *retirement.borrow_mut() = Some(application.lifecycle().retirement());
                    let published = configured.send(ProtectedMessage { protected, proof });
                    published.unwrap_or_else(|_| panic!("Work awaits the actual configured query"));
                    Ok::<_, Never>(target)
                });
                let session_ref = &session;
                let ordinary_ref = &ordinary;
                let first_worker_ref = &first_worker;
                let first_entered_ref = &first_entered;
                let prefix_ref = &prefix;
                let first_full_ref = &first_full;
                let incoming_ref = &incoming;
                let mut operation = Box::pin(node.run_with(move |application| async move {
                    // This sender is owned by Work, not by the permission
                    // callback. Kernel disposal releases the second worker
                    // before waiting for native actor cleanup.
                    let request = configured_request.await.unwrap();
                    let ordinary = application.client::<CounterValue>().unwrap_or_else(|_| {
                        panic!("the independent actual Counter client allocates")
                    });
                    *ordinary_ref.borrow_mut() = Some(ordinary);
                    let (release_first, released) = mpsc::channel::<()>();
                    let (announced, entered) = oneshot::channel();
                    *first_worker_ref.borrow_mut() = Some(tokio::spawn(async move {
                        let announced = announced.send(());
                        let released = released.recv_timeout(Duration::from_secs(5));
                        (announced, released)
                    }));
                    *first_entered_ref.borrow_mut() = Some(entered.await);
                    {
                        let ordinary = ordinary_ref.borrow();
                        let ordinary = ordinary.as_ref().unwrap();
                        for _ in 0..2 {
                            prefix_ref
                                .borrow_mut()
                                .push(ordinary.try_send(CounterMessage::Increment));
                        }
                        *first_full_ref.borrow_mut() =
                            Some(ordinary.try_send(CounterMessage::Increment));
                    }
                    let receiving = session_ref
                        .get("bombay/second-full-unwind")
                        .allowed_destination(Locality::SessionLocal)
                        .payload(request.protected.to_vec())
                        .attachment(request.proof.to_vec())
                        .consolidation(ConsolidationMode::None)
                        .timeout(Duration::from_secs(20))
                        .await
                        .expect("the actual native query starts");
                    *incoming_ref.borrow_mut() = Some(receiving);
                    returned_first
                        .await
                        .expect("the first native call returns before capacity is released");
                    drop(release_first);
                    pending::<()>().await;
                    drop(release_second);
                }));
                let caught = poll_fn(|context| {
                    match catch_unwind(AssertUnwindSafe(|| operation.as_mut().poll(context))) {
                        Ok(Poll::Pending) => Poll::Pending,
                        original => Poll::Ready(original),
                    }
                })
                .await;
                let disposed = catch_unwind(AssertUnwindSafe(|| drop(operation)));
                let joined = joined
                    .take()
                    .expect("actual startup retains its cleanup observer")
                    .await;
                let external_close = if caught.is_err() {
                    Some(node.close().await)
                } else {
                    None
                };
                let finalized = node.into_parts();
                let retirement = retirement
                    .take()
                    .expect("actual startup retains its independent root observer")
                    .await;
                let first_worker = first_worker
                    .take()
                    .expect("the first real worker was installed")
                    .await;
                let second_worker = second_worker
                    .take()
                    .expect("the retry installed its actual blocker")
                    .await;
                let incoming = incoming
                    .take()
                    .expect("the actual native reply receiver survives Work");
                let mut wire = Vec::new();
                while let Ok(original) = incoming.recv_async().await {
                    wire.push(original);
                }
                let mut ordinary = ordinary
                    .take()
                    .expect("the actual independent client survives Work");
                let caller = ordinary.address();
                ordinary.close_admission();
                let mut ordinary_tail = Vec::new();
                while let Some(original) = ordinary.receive().await {
                    ordinary_tail.push(original);
                }
                let (export, bound, closed, polling, disposal) = finalized
                    .unwrap_or_else(|_| panic!("the Node acquires cleanup and actual close"));
                let Ok(Poll::Ready(Ok(outcome))) = caught else {
                    panic!("the actual Node retains the guard panic and returns its outcome");
                };
                let (work, root_report, native, notifications, ()) = outcome.into_parts();
                let (origin, native) = native.expect("actual original actor retirement survives");
                let notifications = notifications.expect("both actual notifications survive");
                let (_, _, startup, withdrawal, diagnostics, rows) = export.into_parts();
                let [request] = <[_; 1]>::try_from(rows)
                    .unwrap_or_else(|_| panic!("one actual protected query is retained"));
                let (
                    canonical,
                    original,
                    verified,
                    decoded,
                    native_reply,
                    encoded,
                    protected,
                    published,
                    refusals,
                    panics,
                ) = request
                    .try_into_parts()
                    .unwrap_or_else(|_| panic!("joined disposal releases every request producer"));
                let receipt = verified.expect("actual verification completed").unwrap();
                let polling =
                    polling.expect("the kernel retains the actual guard destructor panic");
                let (panic_target, panic_original) = polling
                    .downcast_ref::<(Option<u64>, Box<u64>)>()
                    .expect("retain the original coherent scope and panic payload");
                let claims = claims
                    .try_borrow_mut()
                    .expect("joined disposal releases every actual coherent permission loan");
                let claims = claims
                    .as_ref()
                    .expect("actual configured metadata survives");
                let originals = requests.borrow();
                let [expected] = originals.as_slice() else {
                    panic!("one original provider request survives");
                };
                let (first_announced, first_released) =
                    first_worker.expect("the first blocker joins");
                let (second_announced, second_released) =
                    second_worker.expect("the retry blocker joins");
                let custody = native
                    .custody()
                    .expect("the complete original actor remains owned");
                let first_full = first_full.take();
                let second_full = second_full.take();
                assert!(joined.is_ok() && disposed.is_ok());
                assert!(matches!(work, WorkOutcome::Interrupted { cleanup: Ok(()) }));
                assert!(external_close.is_none() && disposal.is_none());
                assert!(matches!(bound, Some(Ok(_))) && closed.is_ok() && session.is_closed());
                assert_eq!(root_report, Some(retirement));
                assert!(notifications.termination.is_ok() && notifications.retirement.is_ok());
                assert!(matches!(&native, ActorRetirement::OwnerCancelled { .. }));
                assert_eq!(origin.address().0, claims.actor_generation);
                assert!(
                    custody.capability_failures.is_empty()
                        && custody.additional_failures.is_empty()
                );
                assert!(
                    custody.retirement_failures.is_empty() && custody.child_failures.is_empty()
                );
                for original in &custody.user {
                    assert_eq!(original.from, caller);
                    assert!(matches!(original.message, CounterMessage::Increment));
                }
                let first_entered = first_entered.borrow();
                let second_entered = second_entered.borrow();
                let grants = grants.borrow();
                let cause = cause.borrow();
                assert!(first_entered.as_ref().is_some_and(Result::is_ok));
                assert!(second_entered.as_ref().is_some_and(Result::is_ok));
                assert!(first_announced.is_ok());
                assert!(matches!(
                    first_released,
                    Err(RecvTimeoutError::Disconnected)
                ));
                assert!(second_announced.is_ok());
                assert!(matches!(
                    second_released,
                    Err(RecvTimeoutError::Disconnected)
                ));
                assert_eq!(first_return_receipt.get(), Some(Ok(())));
                assert_eq!(
                    grants.as_slice(),
                    [
                        PermissionStage::Scope,
                        PermissionStage::Admission,
                        PermissionStage::Retry,
                    ]
                );
                let prefix = prefix.borrow();
                assert_eq!(prefix.len(), 2);
                assert!(prefix.iter().all(Result::is_ok));
                assert!(matches!(
                    first_full,
                    Some(Err(TrySendError::Full(CounterMessage::Increment)))
                ));
                let refill = refill.borrow();
                assert_eq!(refill.len(), 2);
                for original in refill.iter() {
                    assert!(matches!(
                        original,
                        Ok(()) | Err(TrySendError::Full(CounterMessage::Increment))
                    ));
                }
                assert!(matches!(
                    second_full,
                    Some(Err(TrySendError::Full(CounterMessage::Increment)))
                ));
                assert_eq!(*panic_target, Some(claims.actor_generation));
                assert_eq!(ptr::from_ref(panic_original.as_ref()), cause_allocation);
                assert_eq!(**panic_original, 401);
                assert!(cause.is_none());
                assert!(matches!(startup, Some(Ok(()))) && matches!(withdrawal, Some(Ok(()))));
                assert!(diagnostics.is_empty() && canonical.is_some());
                assert_eq!(original.0.as_ref(), expected.protected.as_ref());
                assert_eq!(original.1.as_ref(), expected.proof.as_ref());
                assert_eq!(
                    verified_allocations.get(),
                    Some((original.0.as_ptr(), original.1.as_ptr()))
                );
                assert_eq!(decoded_allocation.get(), Some(original.0.as_ptr()));
                assert_eq!(&recorded::projection(&receipt).bombay, claims);
                assert_eq!(recorded::projection(&receipt).kind, Purpose::Request);
                assert!(decoded.is_none());
                assert_eq!(constructions.get(), 1);
                assert!(encoded.is_none() && protected.is_none());
                assert!(published.is_empty() && wire.is_empty() && ordinary_tail.is_empty());
                assert_eq!(encodings.get(), 0);
                assert_eq!(protections.get(), 0);
                assert!(matches!(refusals, (None, None, None)));
                assert!(matches!(panics, (None, None, None, None, None, None)));
                let (constructor, client, admission, received, tail) =
                    native_reply.expect("the actual second Full remains outside its producer");
                let Attempt::Returned(Ok(client)) = client else {
                    panic!("the original actual reply client survives the guard unwind");
                };
                let Attempt::Returned(Err(TrySendError::Full(CounterMessage::Read(reply_to)))) =
                    admission
                else {
                    panic!("the second actual native Full survives permission release");
                };
                let recipient = client.recipient();
                assert!(constructor.is_none());
                assert_eq!(recipient, reply_to);
                assert!(received.is_none());
                assert_eq!(tail.len(), 1);
                assert!(tail[0].is_none());
            })
            .await
            .expect("the actual retry unwind, worker release and joined cleanup terminate");
        });
}
