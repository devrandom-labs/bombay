//! Target-specific keys from real Counter reply protocols and native declarations.
//! Recorded fixture authority only; no Query traffic or callback-lifetime claim.
use super::super::super::counter::{CounterIncremented, CounterProcessed};
use super::{
    ActorFailureAssessment, Admission, Application, Binding, Counter, CounterMessage, CounterValue,
    EndpointRole, Export, ExportLimits, IngressError, MailAddr, ProviderProfile, ReadRequestRef,
    RequestKey, RequestRefusal, RetirementAssessment, Session, assert_completed,
    assert_notifications, binding, configuration, local_current, metadata, receipt, recorded,
    retain_verified, undeclare,
};
use bombay::actors::ActorExt;
use bombay::behavior::{Protocol, User};
use serde::{Deserialize, Serialize};
use std::cell::{Cell, RefCell};
use std::num::NonZeroUsize;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use tokio::runtime::Builder;
use tokio::sync::mpsc;
use zenoh::query::Query;

#[derive(Clone, Copy, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
enum CounterOperation {
    Increment,
    Read,
}

fn same_request(
    first: &recorded::Receipt<CounterOperation>,
    second: &recorded::Receipt<CounterOperation>,
) -> bool {
    recorded::identity(first) == recorded::identity(second)
}

fn originals(
    protected: &[u8],
    proof: &[u8],
    receipt: &recorded::Receipt<CounterOperation>,
) -> (Vec<u8>, Vec<u8>, Vec<u8>, Binding, [*const u8; 12]) {
    let scope = recorded::projection(receipt);
    let metadata = &scope.bombay;
    (
        protected.to_vec(),
        proof.to_vec(),
        serde_json::to_vec(&ReadRequestRef {
            bombay: metadata,
            kind: scope.kind,
            operation: scope.operation,
        })
        .unwrap(),
        *recorded::identity(receipt).0,
        [
            protected.as_ptr(),
            proof.as_ptr(),
            metadata.provider_profile.as_ptr(),
            metadata.deployment.as_ptr(),
            metadata.caller.as_ptr(),
            metadata.caller_session.as_ptr(),
            metadata.controller.as_ptr(),
            metadata.actor.as_ptr(),
            metadata.host.as_ptr(),
            metadata.protocol.as_ptr(),
            metadata.grant_issuer.as_ptr(),
            metadata.runtime_generation.as_ptr(),
        ],
    )
}

async fn retain<Replies>(
    export: &mut Export<Counter, Replies, recorded::Receipt<CounterOperation>>,
    session: &Session,
    operation: CounterOperation,
) -> (
    RequestKey<Counter>,
    Option<(Box<[u8]>, Box<[u8]>, recorded::Receipt<CounterOperation>)>,
    (Vec<u8>, Vec<u8>, Vec<u8>, Binding, [*const u8; 12]),
)
where
    Replies: Protocol<Addr = MailAddr>,
    Replies::Msg: Send,
{
    let (mut original, mut verified) = receipt(
        ProviderProfile::Recorded,
        session,
        "counter",
        operation,
        binding("counter", 1),
        recorded::verify::<CounterOperation>,
    )
    .await;
    let before = {
        let original = original
            .as_ref()
            .expect("actual protected fixture original");
        let verified = verified.as_ref().unwrap().as_ref().unwrap();
        originals(&original.protected, &original.proof, verified)
    };
    let retained = retain_verified(export, &mut original, &mut verified);
    let Some(Ok((key, duplicate))) = retained else {
        panic!("actual recorded request retention is setup, not a key refusal");
    };
    (key, duplicate, before)
}

#[test]
#[expect(
    clippy::too_many_lines,
    clippy::large_futures,
    reason = "one original native cleanup trace precedes every request-key oracle"
)]
fn request_keys_preserve_declarations_across_reply_protocols() {
    let host = Builder::new_multi_thread()
        .worker_threads(1)
        .enable_all()
        .build()
        .unwrap();
    host.block_on(async {
        tokio::time::timeout(Duration::from_secs(30), async {
            let mut node = Application::new(Counter::new().stop_on_shutdown())
                .open_node(configuration("tls/localhost:0", &EndpointRole::Receiver))
                .await
                .unwrap_or_else(|_| panic!("actual Counter Node startup"));
            let connection = node.session.clone();
            let session = &connection;
            let (execution, receiving) = node
                .execute_with(|application| async move {
                    let target = application.root().established_recipient();
                    let origin = application.root().address();
                    let publications = Arc::new(Mutex::new(Vec::<IngressError<Query>>::new()));
                    let refused = || {
                        let observed = Arc::clone(&publications);
                        move |original| observed.lock().unwrap().push(original)
                    };
                    let limits = ExportLimits {
                        queued_queries: NonZeroUsize::new(1).unwrap(),
                        retained_queries: NonZeroUsize::new(1).unwrap(),
                        retained_requests: NonZeroUsize::new(1).unwrap(),
                    };
                    let mut processing = application
                        .export::<_, CounterProcessed, _>(
                            target.clone(),
                            "bombay/keys/processed",
                            limits,
                            same_request,
                            refused(),
                        )
                        .await
                        .unwrap_or_else(|_| panic!("actual processing declaration"));
                    let mut reading = application
                        .export::<_, CounterValue, _>(
                            target.clone(),
                            "bombay/keys/value",
                            limits,
                            same_request,
                            refused(),
                        )
                        .await
                        .unwrap_or_else(|_| panic!("actual Read declaration"));
                    let mut neighbor = application
                        .export::<_, CounterValue, _>(
                            target.clone(),
                            "bombay/keys/neighbor",
                            limits,
                            same_request,
                            refused(),
                        )
                        .await
                        .unwrap_or_else(|_| panic!("actual neighboring declaration"));
                    let (processed_key, processed_duplicate, processed_before) =
                        retain(&mut processing, session, CounterOperation::Increment).await;
                    let (read_key, read_duplicate, read_before) =
                        retain(&mut reading, session, CounterOperation::Read).await;
                    let (duplicate_key, duplicate, duplicate_before) =
                        retain(&mut reading, session, CounterOperation::Read).await;
                    let (neighbor_key, neighbor_duplicate, neighbor_before) =
                        retain(&mut neighbor, session, CounterOperation::Read).await;
                    let keys: Vec<RequestKey<Counter>> =
                        vec![processed_key, read_key, neighbor_key];
                    let [processed_key, read_key, neighbor_key] = keys.as_slice() else {
                        panic!("three real keys");
                    };
                    let authority = &RefCell::new(Some(binding("counter", 1)));
                    let current = &metadata(
                        ProviderProfile::Recorded,
                        "counter",
                        session.zid().to_string(),
                        session.zid().to_string(),
                    );
                    let deadline = Instant::now() + Duration::from_secs(30);
                    let own_authority = Cell::new(0);
                    let own_commands = Cell::new(0);
                    let foreign_authority = Cell::new(0);
                    let foreign_commands = Cell::new(0);
                    let read_permission = |receipt: &recorded::Receipt<CounterOperation>| {
                        own_authority.set(own_authority.get() + 1);
                        local_current(
                            authority,
                            receipt,
                            recorded::projection,
                            recorded::identity,
                            current,
                            &CounterOperation::Read,
                            deadline,
                        )
                    };
                    let foreign_permission = |receipt: &recorded::Receipt<CounterOperation>| {
                        foreign_authority.set(foreign_authority.get() + 1);
                        local_current(
                            authority,
                            receipt,
                            recorded::projection,
                            recorded::identity,
                            current,
                            &CounterOperation::Read,
                            deadline,
                        )
                    };
                    let read_command = |reply_to| {
                        own_commands.set(own_commands.get() + 1);
                        CounterMessage::Read(reply_to)
                    };
                    let foreign_command = |reply_to| {
                        foreign_commands.set(foreign_commands.get() + 1);
                        CounterMessage::Read(reply_to)
                    };
                    let request_sequence = processing
                        .request(processed_key)
                        .map(|(_, _, receipt, _, _)| recorded::identity(receipt).1)
                        .unwrap();
                    let incremented = processing
                        .attempt(
                            processed_key,
                            |receipt| {
                                own_authority.set(own_authority.get() + 1);
                                local_current(
                                    authority,
                                    receipt,
                                    recorded::projection,
                                    recorded::identity,
                                    current,
                                    &CounterOperation::Increment,
                                    deadline,
                                )
                            },
                            |reply_to| {
                                own_commands.set(own_commands.get() + 1);
                                CounterMessage::IncrementAndReply {
                                    request_sequence,
                                    reply_to,
                                }
                            },
                        )
                        .map(|_| ());
                    let processed = processing.receive_native(processed_key).await;
                    let mut foreign = Vec::new();
                    for key in [neighbor_key, processed_key] {
                        foreign.push(
                            reading
                                .attempt(key, foreign_permission, foreign_command)
                                .map(|_| ()),
                        );
                    }
                    let read = reading
                        .attempt(read_key, read_permission, read_command)
                        .map(|_| ());
                    let received = reading.receive_native(read_key).await;
                    let adjacent = neighbor
                        .attempt(neighbor_key, read_permission, read_command)
                        .map(|_| ());
                    let adjacent_received = neighbor.receive_native(neighbor_key).await;
                    let reading = undeclare(reading).await;
                    let mut renewed = application
                        .export::<_, CounterValue, _>(
                            target,
                            "bombay/keys/value",
                            limits,
                            same_request,
                            refused(),
                        )
                        .await
                        .unwrap_or_else(|_| panic!("actual same-path redeclaration"));
                    let (renewed_key, renewed_duplicate, renewed_before) =
                        retain(&mut renewed, session, CounterOperation::Read).await;
                    foreign.push(
                        renewed
                            .attempt(read_key, foreign_permission, foreign_command)
                            .map(|_| ()),
                    );
                    let fresh = renewed
                        .attempt(&renewed_key, read_permission, read_command)
                        .map(|_| ());
                    let fresh_received = renewed.receive_native(&renewed_key).await;
                    let processing = undeclare(processing).await;
                    let neighbor = undeclare(neighbor).await;
                    let renewed = undeclare(renewed).await;
                    let stopped = application.request_shutdown();
                    let report = application.lifecycle().retirement().await;
                    (
                        keys,
                        duplicate_key,
                        duplicate,
                        renewed_key,
                        processing,
                        reading,
                        neighbor,
                        renewed,
                        [
                            processed_before,
                            read_before,
                            neighbor_before,
                            renewed_before,
                        ],
                        duplicate_before,
                        [
                            processed_duplicate,
                            read_duplicate,
                            neighbor_duplicate,
                            renewed_duplicate,
                        ],
                        foreign,
                        [
                            own_authority.get(),
                            own_commands.get(),
                            foreign_authority.get(),
                            foreign_commands.get(),
                        ],
                        [incremented, read, adjacent, fresh],
                        [processed, received, adjacent_received, fresh_received],
                        request_sequence,
                        origin,
                        stopped,
                        report,
                        publications,
                    )
                })
                .unwrap_or_else(|_| panic!("actual Counter execution"));
            let ((), outcome) = tokio::join!(execution, receiving);
            let (work, native, notifications, ()) = outcome.into_parts();
            let (facts, cleanup) = work
                .try_into_completed()
                .unwrap_or_else(|_| panic!("actual original Work"));
            let closed = node.close().await;
            let (
                keys,
                duplicate_key,
                duplicate,
                renewed_key,
                processing,
                reading,
                neighbor,
                renewed,
                before,
                duplicate_before,
                firsts,
                foreign,
                calls,
                attempts,
                received,
                request_sequence,
                origin,
                stopped,
                report,
                publications,
            ) = facts;
            let publications = publications.lock().unwrap();
            assert_completed(native.expect("complete actual Counter native tree"), None);
            assert_notifications(&notifications);
            assert!(cleanup.is_ok());
            assert!(closed.is_ok());
            assert!(connection.is_closed());
            assert!(stopped.is_ok());
            assert_eq!(report.retirement(), RetirementAssessment::Established);
            assert_eq!(report.failures(), ActorFailureAssessment::NoFailuresFound);
            let [processed_key, read_key, neighbor_key] = keys.as_slice() else {
                panic!("original key order");
            };
            let [processed_row] = processing.1.as_slice() else {
                panic!("one processing row");
            };
            let [read_row] = reading.1.as_slice() else {
                panic!("one Read row");
            };
            let [neighbor_row] = neighbor.1.as_slice() else {
                panic!("one neighboring row");
            };
            let [renewed_row] = renewed.1.as_slice() else {
                panic!("one recreated row");
            };
            let after = [
                originals(&processed_row.1, &processed_row.2, &processed_row.3),
                originals(&read_row.1, &read_row.2, &read_row.3),
                originals(&neighbor_row.1, &neighbor_row.2, &neighbor_row.3),
                originals(&renewed_row.1, &renewed_row.2, &renewed_row.3),
            ];
            let duplicate = duplicate.expect("actual duplicate originals");
            let duplicate_after = originals(&duplicate.0, &duplicate.1, &duplicate.2);
            assert_eq!(before, after);
            assert_eq!(duplicate_before, duplicate_after);
            assert!(firsts.iter().all(Option::is_none));
            assert_eq!(&duplicate_key, read_key);
            assert_eq!(&processed_row.0, processed_key);
            assert_eq!(&read_row.0, read_key);
            assert_eq!(&neighbor_row.0, neighbor_key);
            assert_eq!(renewed_row.0, renewed_key);
            assert_ne!(read_key, &renewed_key);
            assert_ne!(processed_key, read_key);
            assert_ne!(read_key, neighbor_key);
            assert!(attempts.iter().all(Result::is_ok));
            assert_eq!(received, [Some(()); 4]);
            assert_eq!(
                processed_row.5,
                Some(Some(User::new(
                    origin,
                    CounterIncremented { request_sequence }
                )))
            );
            for row in [read_row, neighbor_row, renewed_row] {
                assert!(matches!(row.4, Admission::Returned(Ok(()))));
                assert_eq!(row.5, Some(Some(User::new(origin, 1))));
                assert_eq!(row.7, []);
            }
            assert!(matches!(processed_row.4, Admission::Returned(Ok(()))));
            assert_eq!(processed_row.7, []);
            assert!(processing.2.is_ok());
            assert!(processing.3.is_empty());
            assert!(matches!(
                processing.4,
                mpsc::error::TryRecvError::Disconnected
            ));
            for disposed in [&reading, &neighbor, &renewed] {
                assert!(disposed.2.is_ok());
                assert!(disposed.3.is_empty());
                assert!(matches!(
                    disposed.4,
                    mpsc::error::TryRecvError::Disconnected
                ));
            }
            assert!(publications.is_empty());
            assert_eq!(calls, [4, 4, 0, 0]);
            for original in &foreign {
                assert!(matches!(original, Err(RequestRefusal::UnknownRequest)));
            }
        })
        .await
        .expect("complete native key trace is bounded");
    });
}
