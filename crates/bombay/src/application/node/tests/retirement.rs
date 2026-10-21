//! A retained result remains available after target retirement while Work is live.
use super::protocol::{
    Binding, EndpointRole, ProtectedMessage, ProviderProfile, Purpose, ReadRequest, ReadRequestRef,
    configuration, limits, metadata, recorded,
};
use super::{Counter, CounterMessage};
use crate::actors::ActorExt;
use crate::application::Application;
use crate::remote::{Attempt, Export, ExportLimits, submission};
use crate::{ActorFailureAssessment, Completion, MailAddr, RetirementAssessment, TrySendError};
use behavior::Never;
use serde::{Deserialize, Serialize};
use std::cell::{Cell, RefCell};
use std::num::NonZeroUsize;
use std::time::Duration;
use zenoh::query::ConsolidationMode;
use zenoh::sample::Locality;

#[derive(Debug, Deserialize, Serialize, PartialEq, Eq)]
enum Command {
    Increment,
    Read,
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn retired_target_replays_while_work_remains_live() {
    tokio::time::timeout(Duration::from_secs(40), replay_after_retirement())
        .await
        .expect("actual replay and joined cleanup finish within the watchdog");
}

async fn replay_after_retirement() {
    let records = RefCell::new(Vec::<ProtectedMessage>::new());
    let decoded = Cell::new(0);
    let constructed = Cell::new(0);
    let encoded = Cell::new(0);
    let protected = Cell::new(0);
    let maximum = NonZeroUsize::new(2).unwrap();
    let export = Export::new(
        "bombay/retired-replay",
        ExportLimits {
            queued_queries: maximum,
            retained_queries: maximum,
            retained_requests: maximum,
            diagnostics: maximum,
        },
        limits(),
        |bytes| {
            decoded.set(decoded.get() + 1);
            serde_json::from_slice::<ReadRequest<Command>>(bytes).map(|request| request.operation)
        },
        submission(
            |command| match command {
                Command::Increment => Ok(|| {
                    constructed.set(constructed.get() + 1);
                    CounterMessage::Increment
                }),
                command => Err(command),
            },
            |_: &recorded::Receipt<Command>,
             admission: &Attempt<Result<(), TrySendError<CounterMessage>>>| {
                encoded.set(encoded.get() + 1);
                match admission {
                    Attempt::Returned(Ok(())) => Ok::<_, ()>(Box::<[u8]>::from([59_u8])),
                    _ => Err(()),
                }
            },
        ),
        async |bytes: &[u8], proof: &[u8]| {
            let original = ProtectedMessage {
                protected: bytes.into(),
                proof: proof.into(),
            };
            recorded::verify::<Command>(
                &original,
                &records.borrow(),
                Binding {
                    deployment: "two-node-read-study",
                    caller: "caller",
                    controller: "owner",
                    actor: "counter",
                    host: "receiver",
                    protocol: "counter",
                    target_assignment: "root",
                    logical_session: 331,
                },
            )
            .await
        },
        async |_: &[u8], _: &recorded::Receipt<Command>| {
            protected.set(protected.get() + 1);
            Ok::<_, Never>(Box::<[u8]>::from([61_u8]))
        },
        |left, right| recorded::identity(left) == recorded::identity(right),
        |_: &recorded::Receipt<Command>, _: &str, _: MailAddr| Ok::<_, Never>(()),
        |_: &recorded::Receipt<Command>, _: &str, _: MailAddr| Ok::<_, Never>(()),
    );
    let node = Application::new(Counter::new().stop_on_shutdown())
        .open_node(configuration("tls/localhost:0", &EndpointRole::Receiver))
        .await
        .unwrap_or_else(|_| panic!("the actual cold Node opens"));
    let runtime = node.runtime_id();
    let session = node.session.clone();
    let mut node = node.exports(export, async |application| {
        let target = application.root().established_recipient();
        let mut claims = metadata(
            ProviderProfile::Recorded,
            "counter",
            runtime.clone(),
            runtime,
        );
        claims.actor_generation = application.actor_address(&target).unwrap().0;
        records.borrow_mut().push(ProtectedMessage {
            protected: serde_json::to_vec(&ReadRequestRef {
                bombay: &claims,
                kind: Purpose::Request,
                operation: Command::Increment,
            })
            .unwrap()
            .into_boxed_slice(),
            proof: Box::from([53_u8]),
        });
        Ok::<_, Never>(target)
    });
    let outcome = node
        .run_with(|application| {
            let records = &records;
            let session = &session;
            async move {
                let original = {
                    let records = records.borrow();
                    let original = &records[0];
                    (original.protected.to_vec(), original.proof.to_vec())
                };
                let mut endings = Vec::new();
                let mut replies = Vec::new();
                let mut retirement = None;
                let mut shutdown = None;
                for _ in 0..2 {
                    let incoming = session
                        .get("bombay/retired-replay")
                        .allowed_destination(Locality::SessionLocal)
                        .payload(original.0.clone())
                        .attachment(original.1.clone())
                        .consolidation(ConsolidationMode::None)
                        .timeout(Duration::from_secs(5))
                        .await
                        .expect("the actual native query is issued");
                    let mut received = Vec::new();
                    let ending = loop {
                        match incoming.recv_async().await {
                            Ok(reply) => received.push(reply),
                            Err(ending) => break ending,
                        }
                    };
                    endings.push(ending);
                    replies.push(received);
                    if retirement.is_none() {
                        shutdown = Some(application.shutdown());
                        retirement = Some(application.lifecycle().retirement().await);
                    }
                }
                (replies, endings, shutdown, retirement)
            }
        })
        .await
        .unwrap_or_else(|_| panic!("the actual cold Node execution starts"));
    let (export, binding, close, polling, disposal) = node
        .into_parts()
        .unwrap_or_else(|_| panic!("the actual Node finalization remains acquired"));
    let (work, report, native, notifications, ()) = outcome.into_parts();
    let ((replies, endings, shutdown, retirement), cleanup) = work
        .try_into_completed()
        .unwrap_or_else(|_| panic!("Work completes after both real query streams"));
    cleanup.expect("actual Application cleanup joins");
    close.expect("actual Node Session close succeeds");
    let (origin, native) = native.expect("the complete original Counter remains acquired");
    let notifications = notifications.expect("both actual notifications are acquired");
    let (_, _, startup, withdrawal, diagnostics, requests) = export.into_parts();
    assert!(polling.is_none() && disposal.is_none());
    assert!(matches!(binding, Some(Ok(_))));
    assert!(matches!(startup, Some(Ok(()))) && matches!(withdrawal, Some(Ok(()))));
    assert!(diagnostics.is_empty());
    assert_eq!(endings.len(), 2);
    assert_eq!(shutdown, Some(Ok(())));
    let retirement =
        retirement.expect("the actual root retirement was observed before the duplicate");
    assert_eq!(retirement.retirement(), RetirementAssessment::Established);
    assert_eq!(
        retirement.failures(),
        ActorFailureAssessment::NoFailuresFound
    );
    assert_eq!(report, Some(retirement));
    assert_eq!(native.completion(), Some(&Completion::Stopped));
    assert_eq!(origin.address(), MailAddr::APPLICATION_ROOT);
    assert!(native.custody().is_some());
    assert!(notifications.termination.is_ok() && notifications.retirement.is_ok());
    assert_eq!(replies.iter().map(Vec::len).collect::<Vec<_>>(), [1, 1]);
    for reply in replies.iter().flatten() {
        let received = reply.result();
        let sample = received
            .as_ref()
            .expect("the actual cached reply is successful");
        let payload = sample.payload().to_bytes();
        let attachment = sample
            .attachment()
            .expect("the actual provider attachment remains")
            .to_bytes();
        assert_eq!(payload.as_ref(), [59]);
        assert_eq!(attachment.as_ref(), [61]);
    }
    assert_eq!(
        (
            decoded.get(),
            constructed.get(),
            encoded.get(),
            protected.get()
        ),
        (1, 1, 1, 1)
    );
    assert_eq!(requests.len(), 2);
    let [first, second] = <[_; 2]>::try_from(requests)
        .unwrap_or_else(|_| panic!("two actual request originals remain retained"));
    let first = first
        .try_into_parts()
        .unwrap_or_else(|_| panic!("the canonical request releases its producer loans"));
    let second = second
        .try_into_parts()
        .unwrap_or_else(|_| panic!("the duplicate request releases its producer loans"));
    assert!(first.0.is_some());
    assert_eq!(first.0, second.0);
    assert_eq!(first.1, second.1);
    assert!(matches!(first.2, Some(Ok(_))) && matches!(second.2, Some(Ok(_))));
    let (_, _, admitted) = first.4.expect("the first native attempt is retained");
    assert!(matches!(admitted, Attempt::Returned(Ok(()))));
    assert!(second.4.is_none() && second.5.is_none() && second.6.is_none());
    assert!(first.7.iter().all(Result::is_ok) && second.7.iter().all(Result::is_ok));
    assert_eq!((first.7.len(), second.7.len()), (1, 1));
    assert!(matches!(first.8, (None, None, None)) && matches!(second.8, (None, None, None)));
    assert!(matches!(first.9, (None, None, None, None, None, None)));
    assert!(matches!(second.9, (None, None, None, None, None, None)));
}
