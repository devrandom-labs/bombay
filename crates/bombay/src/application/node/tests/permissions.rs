//! Native permission refusal survives redacted diagnostics and complete Node cleanup.
use super::custody::completed_actor;
use super::protocol::{
    Binding, CounterOperation, EndpointRole, ProtectedMessage, ProviderProfile, Purpose,
    ReadRequest, ReadRequestRef, configuration, limits, metadata, recorded,
};
use super::{Counter, CounterMessage};
use bombay::actors::ActorExt;
use bombay::behavior::{Never, User};
use bombay::remote::{Attempt, Export, ExportLimits, reply};
use bombay::{ActorFailureAssessment, Application, MailAddr, RetirementAssessment, TrySendError};
use std::cell::{BorrowError, Cell, RefCell};
use std::num::NonZeroUsize;
use std::time::Duration;
use tokio::sync::oneshot;
use zenoh::query::ConsolidationMode;
use zenoh::sample::Locality;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn admission_diagnostics_preserve_opaque_commands_and_native_permission_causes() {
    // CounterMessage has no Debug implementation, including inside a native refusal.
    let attempts = [
        Attempt::Unattempted,
        Attempt::Uncertain,
        Attempt::Returned(Ok(())),
        Attempt::Returned(Err(TrySendError::Full(CounterMessage::Increment))),
        Attempt::Returned(Err(TrySendError::Closed(CounterMessage::Increment))),
    ];
    let diagnostics = attempts
        .iter()
        .map(|attempt| format!("{attempt:?}"))
        .collect::<Vec<_>>();
    assert_eq!(
        diagnostics,
        [
            "Unattempted",
            "Uncertain",
            "Returned(..)",
            "Returned(..)",
            "Returned(..)"
        ]
    );
    tokio::time::timeout(Duration::from_secs(40), borrowed_permission())
        .await
        .expect("the actual permission refusal and joined cleanup complete");
}

async fn borrowed_permission() {
    let permission = RefCell::new(());
    let checks = Cell::new(0);
    let decoded = Cell::new(0);
    let constructed = Cell::new(0);
    let encoded = Cell::new(0);
    let protected = Cell::new(0);
    let records = RefCell::new(Vec::<ProtectedMessage>::new());
    let binding = Binding {
        deployment: "two-node-read-study",
        caller: "caller",
        controller: "owner",
        actor: "counter",
        host: "receiver",
        protocol: "counter",
        target_assignment: "permission-counter",
        logical_session: 359,
    };
    let maximum = NonZeroUsize::new(1).unwrap();
    let current = |_: &recorded::Receipt<CounterOperation>, _: &str, _: MailAddr| {
        checks.set(checks.get() + 1);
        permission.try_borrow()
    };
    let export = Export::new(
        "bombay/permission/read",
        ExportLimits {
            queued_queries: maximum,
            retained_queries: maximum,
            retained_requests: maximum,
            diagnostics: maximum,
        },
        limits(),
        |bytes| {
            decoded.set(decoded.get() + 1);
            serde_json::from_slice::<ReadRequest<CounterOperation>>(bytes)
                .map(|request| request.operation)
        },
        reply(
            |CounterOperation::Read| {
                let constructed = &constructed;
                Ok::<_, CounterOperation>(move |reply_to| {
                    constructed.set(constructed.get() + 1);
                    CounterMessage::Read(reply_to)
                })
            },
            |_: &recorded::Receipt<CounterOperation>, original: &User<MailAddr, u64>| {
                encoded.set(encoded.get() + 1);
                serde_json::to_vec(&original.message).map(Vec::into_boxed_slice)
            },
        ),
        async |bytes: &[u8], proof: &[u8]| {
            let original = ProtectedMessage {
                protected: bytes.into(),
                proof: proof.into(),
            };
            recorded::verify::<CounterOperation>(&original, &records.borrow(), binding).await
        },
        async |_: &[u8], _: &recorded::Receipt<CounterOperation>| {
            protected.set(protected.get() + 1);
            Ok::<_, Never>(Box::<[u8]>::from([13_u8, 17]))
        },
        |left, right| recorded::identity(left) == recorded::identity(right),
        &current,
        &current,
    );
    let node = Application::new(Counter::new().stop_on_shutdown())
        .open_node(configuration("tls/localhost:0", &EndpointRole::Receiver))
        .await
        .unwrap_or_else(|_| panic!("the actual Counter Node opens"));
    let runtime = node.runtime_id();
    let session = node.session.clone();
    let (configured, request) = oneshot::channel();
    let mut node = node.exports(export, async |application| {
        let target = application.root().established_recipient();
        let mut claims = metadata(
            ProviderProfile::Recorded,
            "counter",
            runtime.clone(),
            runtime,
        );
        claims.actor_generation = application.actor_address(&target).unwrap().0;
        let original = ProtectedMessage {
            protected: serde_json::to_vec(&ReadRequestRef {
                bombay: &claims,
                kind: Purpose::Request,
                operation: CounterOperation::Read,
            })
            .unwrap()
            .into_boxed_slice(),
            proof: Box::from([7, 11]),
        };
        records.borrow_mut().push(ProtectedMessage {
            protected: original.protected.clone(),
            proof: original.proof.clone(),
        });
        configured
            .send(original)
            .unwrap_or_else(|_| panic!("Work retains the actual configured request"));
        Ok::<_, Never>(target)
    });
    let held = permission.borrow_mut();
    let outcome = node
        .run_with(|application| {
            let session = &session;
            async move {
                let request = request.await.expect("actual binding supplies the request");
                let receiving = session
                    .get("bombay/permission/read")
                    .allowed_destination(Locality::SessionLocal)
                    .consolidation(ConsolidationMode::None)
                    .timeout(Duration::from_secs(5))
                    .payload(request.protected.to_vec())
                    .attachment(request.proof.to_vec())
                    .await
                    .expect("the actual native query starts");
                let ending = receiving.recv_async().await;
                let shutdown = application.shutdown();
                (request, ending, shutdown)
            }
        })
        .await
        .unwrap_or_else(|_| panic!("the actual Counter Node enters its host"));
    drop(held);
    let (export, bound, closed, polling, disposal) = node
        .into_parts()
        .unwrap_or_else(|_| panic!("the actual Node finalization remains acquired"));
    let (work, report, native, notifications, ()) = outcome.into_parts();
    let ((original, ending, shutdown), cleanup) = work
        .try_into_completed()
        .unwrap_or_else(|_| panic!("actual Work and its Query ending remain acquired"));
    cleanup.expect("actual Application cleanup joins");
    closed.expect("actual Node Session close succeeds");
    let (origin, native) = native.expect("the complete original Counter root remains");
    let _counter = completed_actor(origin, native);
    let report = report.expect("the actual root report remains independent");
    let notifications = notifications.expect("both actual notification receipts remain");
    let (_, _, startup, withdrawal, diagnostics, requests) = export.into_parts();
    assert_eq!(report.retirement(), RetirementAssessment::Established);
    assert_eq!(report.failures(), ActorFailureAssessment::NoFailuresFound);
    assert!(notifications.termination.is_ok() && notifications.retirement.is_ok());
    assert!(polling.is_none() && disposal.is_none());
    assert!(matches!(bound, Some(Ok(_))));
    assert!(matches!(startup, Some(Ok(()))) && matches!(withdrawal, Some(Ok(()))));
    assert!(diagnostics.is_empty() && ending.is_err() && shutdown.is_ok() && session.is_closed());
    let [request] =
        <[_; 1]>::try_from(requests).unwrap_or_else(|_| panic!("one actual request is retained"));
    let (key, raw, verified, input, native, encoding, protection, publications, refusals, panics) =
        request
            .try_into_parts()
            .unwrap_or_else(|_| panic!("the actual request releases its producer loans"));
    let _cause: &BorrowError = refusals
        .0
        .as_ref()
        .expect("the original typed native permission cause remains inspectable");
    assert!(key.is_none());
    assert_eq!(raw.0, original.protected);
    assert_eq!(raw.1, original.proof);
    assert!(matches!(verified, Some(Ok(_))));
    assert!(input.is_none() && native.is_none() && encoding.is_none() && protection.is_none());
    assert!(publications.is_empty() && refusals.1.is_none() && refusals.2.is_none());
    assert!(matches!(panics, (None, None, None, None, None, None)));
    assert_eq!(
        (
            checks.get(),
            decoded.get(),
            constructed.get(),
            encoded.get(),
            protected.get()
        ),
        (1, 0, 0, 0, 0)
    );
}
