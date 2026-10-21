//! A decoder unwind must not preempt the existing Node's joined finalization.
use super::protocol::{
    Binding, EndpointRole, ProtectedMessage, ProviderProfile, Purpose, ReadRequestRef,
    VerificationError, configuration, current_scope, limits, metadata, recorded,
};
use super::{LedgerCommand, MailLedger};
use crate::application::Application;
use crate::remote::{Attempt, Export, ExportLimits, Metadata, submission};
use crate::{ActorRetirement, MailAddr, TrySendError, WorkOutcome};
use behavior::Never;
use core::future::{pending, poll_fn};
use core::task::Poll;
use std::cell::{Cell, RefCell};
use std::num::NonZeroUsize;
use std::panic::{AssertUnwindSafe, catch_unwind, panic_any};
use std::ptr;
use std::time::{Duration, Instant};
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
