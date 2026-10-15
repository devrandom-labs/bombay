//! Actual Node consumers of JSON checks; SessionLocal and recorded proof fixtures only.
use super::{Counter, CounterMessage, LedgerCommand, MailLedger, Receipts};
use bombay::actors::ActorExt;
use bombay::behavior::BehaviorBase;
use bombay::prelude::Exit;
use bombay::remote::{
    Metadata,
    json::{self, Limits},
    try_retain, verify,
};
use bombay::{
    ActorFailureAssessment, ActorNotificationReceipts, Application, Completion,
    RetirementAssessment,
};
use serde::Deserialize;
use std::future::ready;
use std::num::NonZeroUsize;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use tokio::runtime::Builder;
use tokio::sync::{Semaphore, mpsc};
use zenoh::query::ConsolidationMode;
use zenoh::sample::Locality;
use zenoh_config::Config;

#[derive(Debug, Deserialize)]
#[serde(rename_all = "snake_case")]
enum RequestKind {
    Request,
}
#[derive(Debug, Deserialize)]
#[serde(rename_all = "snake_case")]
enum CounterOperation {
    Increment,
}
#[derive(Debug, Deserialize)]
#[serde(rename_all = "snake_case")]
enum LedgerOperation {
    Read,
}
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct CounterRequest {
    bombay: Metadata,
    kind: RequestKind,
    operation: CounterOperation,
}
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct LedgerRequest {
    bombay: Metadata,
    kind: RequestKind,
    operation: LedgerOperation,
}

fn listening_configuration() -> Config {
    let certificates = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../../docs/prds/static-remote-actors/transport-custody/certificates");
    let declaration = serde_json::json!({
        "mode":"peer", "queries_max_retained":2,
        "connect":{"endpoints":[]}, "listen":{"endpoints":["tls/localhost:0"]},
        "scouting":{"multicast":{"enabled":false,"listen":false},"gossip":{"enabled":false}},
        "transport":{"link":{"protocols":["tls"],"tls":{
            "root_ca_certificate":certificates.join("root-ca.pem"),
            "listen_certificate":certificates.join("recipient-certificate.pem"),
            "listen_private_key":certificates.join("recipient-private-key.pem"),
            "enable_mtls":true,"verify_name_on_connect":true,"use_public_pki":false
        }}}
    })
    .to_string();
    Config::from_deserializer(&mut serde_json::Deserializer::from_str(&declaration)).unwrap()
}

const COUNTER_OBJECT: &[u8] = br#"{"bombay":{"wire_version":1,"schema_version":1,"provider_profile":"counter-record-test-v1","deployment":"comparison","caller":"caller","caller_session":"fedcba9876543210fedcba9876543210","controller":"owner","actor":"counter","host":"host","protocol":"counter","grant_issuer":"issuer","runtime_generation":"123456789abcdef0123456789abcdef0","actor_generation":17,"authority_generation":19,"request_sequence":23},"kind":"request","operation":"increment"}"#;
const OUTER_ARRAY: &[u8] = br#"[{"wire_version":1,"schema_version":1,"provider_profile":"counter-record-test-v1","deployment":"comparison","caller":"caller","caller_session":"fedcba9876543210fedcba9876543210","controller":"owner","actor":"counter","host":"host","protocol":"counter","grant_issuer":"issuer","runtime_generation":"123456789abcdef0123456789abcdef0","actor_generation":17,"authority_generation":19,"request_sequence":23},"request","increment"]"#;
const LEDGER_OBJECT: &[u8] = br#"{"bombay":{"wire_version":1,"schema_version":1,"provider_profile":"counter-record-test-v1","deployment":"comparison","caller":"caller","caller_session":"fedcba9876543210fedcba9876543210","controller":"owner","actor":"ledger","host":"host","protocol":"ledger","grant_issuer":"issuer","runtime_generation":"123456789abcdef0123456789abcdef0","actor_generation":31,"authority_generation":19,"request_sequence":29},"kind":"request","operation":"read"}"#;
const METADATA_ARRAY: &[u8] = br#"{"bombay":[1,1,"counter-record-test-v1","comparison","caller","fedcba9876543210fedcba9876543210","owner","ledger","host","ledger","issuer","123456789abcdef0123456789abcdef0",31,19,29],"kind":"request","operation":"read"}"#;
const PROOF: &[u8] = &[9, 255];

#[test]
fn node_counter_checks_original_json_before_its_exact_root_effect() {
    let host = Builder::new_multi_thread()
        .worker_threads(1)
        .enable_all()
        .build()
        .unwrap();
    host.block_on(async {
        let mut node = Application::new(Counter::new().stop_on_shutdown())
            .open_node(listening_configuration())
            .await
            .unwrap_or_else(|_| panic!("Node opens"));
        let (
            observations,
            publications,
            read,
            value,
            root,
            stopped,
            report,
            cleanup,
            native,
            notifications,
        ) = {
            let (execution, receiving) = node
                .execute_with(|application, session| async move {
                    let limits = Limits {
                        input_bytes: NonZeroUsize::new(2048).unwrap(),
                        proof_bytes: NonZeroUsize::new(2).unwrap(),
                        token_bytes: NonZeroUsize::new(64).unwrap(),
                        nesting: NonZeroUsize::new(4).unwrap(),
                        parser_states: NonZeroUsize::new(16).unwrap(),
                    };
                    let records = [(COUNTER_OBJECT, PROOF), (OUTER_ARRAY, PROOF)];
                    let capacity = Arc::new(Semaphore::new(2));
                    let (incoming, mut received) = mpsc::channel(2);
                    let publications = Arc::new(Mutex::new(Vec::new()));
                    let callback_publications = Arc::clone(&publications);
                    let callback_capacity = Arc::clone(&capacity);
                    let queryable = session
                        .declare_queryable("bombay/json/counter")
                        .allowed_origin(Locality::SessionLocal)
                        .callback(move |query| {
                            let original = try_retain(&incoming, &callback_capacity, query);
                            callback_publications.lock().unwrap().push(original);
                        })
                        .await
                        .unwrap();
                    let mut client = application.client().unwrap();
                    let mut observations = Vec::new();
                    for (payload, proof) in records {
                        let replies = session
                            .get("bombay/json/counter")
                            .allowed_destination(Locality::SessionLocal)
                            .consolidation(ConsolidationMode::None)
                            .payload(payload.to_vec())
                            .attachment(proof.to_vec())
                            .await
                            .unwrap();
                        let original = received.recv().await.unwrap();
                        let observation = {
                            let protected = original.0.payload().unwrap().to_bytes();
                            let proof = original.0.attachment().unwrap().to_bytes();
                            let shape = json::check(&protected, &proof, &limits);
                            let declaration = shape
                                .as_ref()
                                .ok()
                                .map(|()| serde_json::from_slice::<CounterRequest>(&protected));
                            let mut verification = None;
                            let mut admitted = None;
                            if let Some(Ok(CounterRequest {
                                kind: RequestKind::Request,
                                operation: CounterOperation::Increment,
                                ..
                            })) = &declaration
                            {
                                verify(&original.0, &mut verification, |query| {
                                    let protected = query.payload().unwrap().to_bytes();
                                    let proof = query.attachment().unwrap().to_bytes();
                                    ready(
                                        records
                                            .iter()
                                            .position(|record| {
                                                record.0 == protected.as_ref()
                                                    && record.1 == proof.as_ref()
                                            })
                                            .ok_or("unregistered record"),
                                    )
                                })
                                .await;
                                if matches!(&verification, Some(Ok(_))) {
                                    admitted = Some(client.send(CounterMessage::Increment).await);
                                }
                            }
                            let replied = if admitted.as_ref().is_some_and(Result::is_ok) {
                                original.0.reply("bombay/json/counter", "accepted").await
                            } else {
                                original.0.reply_err("refused").await
                            };
                            let reply = if replied.is_ok() {
                                Some(replies.recv_async().await)
                            } else {
                                None
                            };
                            (
                                shape,
                                declaration,
                                verification,
                                admitted,
                                replied,
                                reply,
                                protected.to_vec(),
                                proof.to_vec(),
                                capacity.available_permits(),
                            )
                        };
                        drop(original);
                        let released = capacity.available_permits();
                        observations.push((observation, released));
                    }
                    let read = client.send(CounterMessage::Read(client.recipient())).await;
                    let value = if read.is_ok() {
                        client.receive().await
                    } else {
                        None
                    };
                    let root = application.root().address();
                    let stopped = application.request_shutdown();
                    let report = application.lifecycle().retirement().await;
                    let undeclared = queryable.undeclare().await;
                    (
                        observations,
                        publications,
                        read,
                        value,
                        root,
                        stopped,
                        report,
                        undeclared,
                    )
                })
                .unwrap_or_else(|_| panic!("exact Node execution"));
            execution.await;
            let (outcome, native, notifications) = receiving.await;
            let (
                (observations, publications, read, value, root, stopped, report, undeclared),
                cleanup,
            ) = outcome
                .try_into_completed()
                .unwrap_or_else(|_| panic!("original Work outcome"));
            (
                observations,
                (publications, undeclared),
                read,
                value,
                root,
                stopped,
                report,
                cleanup,
                native,
                notifications,
            )
        };
        let closed = node.close().await;
        let (_, retirement) = native.expect("actual root joined");
        let custody = retirement.custody().unwrap();
        let transfer_results = publications.0.lock().unwrap();
        assert!(closed.is_ok() && cleanup.is_ok() && publications.1.is_ok());
        assert!(matches!(
            &notifications,
            Ok(ActorNotificationReceipts {
                termination: Ok(()),
                retirement: Ok(())
            })
        ));
        assert_eq!(retirement.completion(), Some(&Completion::Stopped));
        assert_eq!(custody.descendants, []);
        assert_eq!(custody.child_failures, []);
        assert_eq!(report.retirement(), RetirementAssessment::Established);
        assert_eq!(report.failures(), ActorFailureAssessment::NoFailuresFound);
        assert!(read.is_ok() && stopped.is_ok());
        let value = value.expect("actual processing fence reply");
        assert_eq!((value.from, value.message), (root, 1));
        assert_eq!(transfer_results.len(), 2);
        assert!(transfer_results.iter().all(Result::is_ok));
        let [(accepted, after_accepted), (refused, after_refused)] = observations.as_slice() else {
            panic!("both native Queries observed");
        };
        let expected_metadata = Metadata {
            wire_version: 1,
            schema_version: 1,
            provider_profile: "counter-record-test-v1".into(),
            deployment: "comparison".into(),
            caller: "caller".into(),
            caller_session: "fedcba9876543210fedcba9876543210".into(),
            controller: "owner".into(),
            actor: "counter".into(),
            host: "host".into(),
            protocol: "counter".into(),
            grant_issuer: "issuer".into(),
            runtime_generation: "123456789abcdef0123456789abcdef0".into(),
            actor_generation: 17,
            authority_generation: 19,
            request_sequence: 23,
        };
        let declaration = accepted.1.as_ref().unwrap().as_ref().unwrap();
        let accepted_payload = accepted
            .5
            .as_ref()
            .and_then(|received| received.as_ref().ok())
            .and_then(|reply| reply.result().ok())
            .map(|sample| sample.payload().to_bytes());
        let refused_payload = refused
            .5
            .as_ref()
            .and_then(|received| received.as_ref().ok())
            .and_then(|reply| reply.result().err())
            .map(|cause| cause.payload().to_bytes());
        assert_eq!(declaration.bombay, expected_metadata);
        assert_eq!(accepted_payload.as_deref(), Some("accepted".as_bytes()));
        assert_eq!(refused_payload.as_deref(), Some(b"refused".as_slice()));
        assert!(accepted.0.is_ok());
        assert!(matches!(&accepted.2, Some(Ok(0))));
        assert!(accepted.3.as_ref().is_some_and(Result::is_ok));
        assert!(accepted.4.is_ok());
        assert!(matches!(&accepted.5, Some(Ok(reply)) if reply.result().is_ok()));
        assert_eq!((&*accepted.6, &*accepted.7), (COUNTER_OBJECT, PROOF));
        assert_eq!((accepted.8, *after_accepted), (1, 2));
        assert!(matches!(&refused.0, Err(json::Error::OuterObject)));
        assert!(refused.1.is_none() && refused.2.is_none() && refused.3.is_none());
        assert!(refused.4.is_ok());
        assert!(matches!(&refused.5, Some(Ok(reply)) if reply.result().is_err()));
        assert_eq!((&*refused.6, &*refused.7), (OUTER_ARRAY, PROOF));
        assert_eq!((refused.8, *after_refused), (1, 2));
    });
}

#[test]
fn node_ledger_checks_metadata_object_before_its_terminating_read() {
    let host = Builder::new_multi_thread()
        .worker_threads(1)
        .enable_all()
        .build()
        .unwrap();
    host.block_on(async {
        let first = Box::<[u8]>::from([13, 17]);
        let second = Box::<[u8]>::from([19, 23]);
        let allocations = (first.as_ptr(), second.as_ptr());
        let mut node = Application::new(
            MailLedger {
                entries: Vec::new(),
            }
            .stop_on_shutdown(),
        )
        .open_node(listening_configuration())
        .await
        .unwrap_or_else(|_| panic!("Node opens"));
        let (
            observations,
            publications,
            origins,
            appends,
            receipt,
            root,
            termination,
            report,
            rejection,
            original_rejection_allocation,
            cleanup,
            native,
            notifications,
        ) = {
            let (execution, receiving) = node
                .execute_with(|application, session| async move {
                    let limits = Limits {
                        input_bytes: NonZeroUsize::new(2048).unwrap(),
                        proof_bytes: NonZeroUsize::new(2).unwrap(),
                        token_bytes: NonZeroUsize::new(64).unwrap(),
                        nesting: NonZeroUsize::new(4).unwrap(),
                        parser_states: NonZeroUsize::new(16).unwrap(),
                    };
                    let records = [(METADATA_ARRAY, PROOF), (LEDGER_OBJECT, PROOF)];
                    let capacity = Arc::new(Semaphore::new(2));
                    let (incoming, mut received) = mpsc::channel(2);
                    let publications = Arc::new(Mutex::new(Vec::new()));
                    let callback_publications = Arc::clone(&publications);
                    let callback_capacity = Arc::clone(&capacity);
                    let queryable = session
                        .declare_queryable("bombay/json/ledger")
                        .allowed_origin(Locality::SessionLocal)
                        .callback(move |query| {
                            let original = try_retain(&incoming, &callback_capacity, query);
                            callback_publications.lock().unwrap().push(original);
                        })
                        .await
                        .unwrap();
                    let sender = application.sender().unwrap();
                    let mut client = application.client::<Receipts>().unwrap();
                    let origins = (sender.address(), client.address());
                    let first_append = sender.send(LedgerCommand::Append(first)).await;
                    let second_append = client.send(LedgerCommand::Append(second)).await;
                    let mut observations = Vec::new();
                    let mut receipt = None;
                    for (payload, proof) in records {
                        let replies = session
                            .get("bombay/json/ledger")
                            .allowed_destination(Locality::SessionLocal)
                            .consolidation(ConsolidationMode::None)
                            .payload(payload.to_vec())
                            .attachment(proof.to_vec())
                            .await
                            .unwrap();
                        let original = received.recv().await.unwrap();
                        let observation = {
                            let protected = original.0.payload().unwrap().to_bytes();
                            let proof = original.0.attachment().unwrap().to_bytes();
                            let shape = json::check(&protected, &proof, &limits);
                            let declaration = shape
                                .as_ref()
                                .ok()
                                .map(|()| serde_json::from_slice::<LedgerRequest>(&protected));
                            let mut verification = None;
                            let mut admitted = None;
                            if let Some(Ok(LedgerRequest {
                                kind: RequestKind::Request,
                                operation: LedgerOperation::Read,
                                ..
                            })) = &declaration
                            {
                                verify(&original.0, &mut verification, |query| {
                                    let protected = query.payload().unwrap().to_bytes();
                                    let proof = query.attachment().unwrap().to_bytes();
                                    ready(
                                        records
                                            .iter()
                                            .position(|record| {
                                                record.0 == protected.as_ref()
                                                    && record.1 == proof.as_ref()
                                            })
                                            .ok_or("unregistered record"),
                                    )
                                })
                                .await;
                                if matches!(&verification, Some(Ok(_))) {
                                    admitted = Some(
                                        client.send(LedgerCommand::Read(client.recipient())).await,
                                    );
                                    if admitted.as_ref().is_some_and(Result::is_ok) {
                                        receipt = client.receive().await;
                                    }
                                }
                            }
                            let replied = if admitted.as_ref().is_some_and(Result::is_ok)
                                && receipt.is_some()
                            {
                                original
                                    .0
                                    .reply("bombay/json/ledger", "read completed")
                                    .await
                            } else {
                                original.0.reply_err("refused").await
                            };
                            let reply = if replied.is_ok() {
                                Some(replies.recv_async().await)
                            } else {
                                None
                            };
                            (
                                shape,
                                declaration,
                                verification,
                                admitted,
                                replied,
                                reply,
                                protected.to_vec(),
                                proof.to_vec(),
                                capacity.available_permits(),
                            )
                        };
                        drop(original);
                        let released = capacity.available_permits();
                        observations.push((observation, released));
                    }
                    let root = application.root().address();
                    let termination = application.termination().await;
                    let report = application.lifecycle().retirement().await;
                    let late = Box::<[u8]>::from([29, 31]);
                    let original_rejection_allocation = late.as_ptr();
                    let rejection = client.send(LedgerCommand::Append(late)).await;
                    let undeclared = queryable.undeclare().await;
                    (
                        observations,
                        publications,
                        origins,
                        (first_append, second_append),
                        receipt,
                        root,
                        termination,
                        report,
                        rejection,
                        original_rejection_allocation,
                        undeclared,
                    )
                })
                .unwrap_or_else(|_| panic!("exact Node execution"));
            execution.await;
            let (outcome, native, notifications) = receiving.await;
            let (
                (
                    observations,
                    publications,
                    origins,
                    appends,
                    receipt,
                    root,
                    termination,
                    report,
                    rejection,
                    original_rejection_allocation,
                    undeclared,
                ),
                cleanup,
            ) = outcome
                .try_into_completed()
                .unwrap_or_else(|_| panic!("original Work outcome"));
            (
                observations,
                (publications, undeclared),
                origins,
                appends,
                receipt,
                root,
                termination,
                report,
                rejection,
                original_rejection_allocation,
                cleanup,
                native,
                notifications,
            )
        };
        let closed = node.close().await;
        let (_, retirement) = native.expect("actual ledger joined");
        let custody = retirement.custody().unwrap();
        let ledger = custody.behavior.base();
        let rejection = rejection.expect_err("closed exact root returns the original command");
        let LedgerCommand::Append(rejected) = rejection.into_message() else {
            panic!("original closed Append remains typed");
        };
        let transfer_results = publications.0.lock().unwrap();
        assert!(closed.is_ok() && cleanup.is_ok() && publications.1.is_ok());
        assert!(matches!(
            &notifications,
            Ok(ActorNotificationReceipts {
                termination: Ok(()),
                retirement: Ok(())
            })
        ));
        assert_eq!(retirement.completion(), Some(&Completion::Stopped));
        assert_eq!(custody.descendants, []);
        assert_eq!(custody.child_failures, []);
        assert_eq!(report.retirement(), RetirementAssessment::Established);
        assert_eq!(report.failures(), ActorFailureAssessment::NoFailuresFound);
        let [(refused, after_refused), (accepted, after_accepted)] = observations.as_slice() else {
            panic!("both native Queries observed");
        };
        assert!(
            matches!(&accepted.3, Some(Ok(()))),
            "the second permitted Ledger Read remains admitted after the positional refusal: {:?}",
            &accepted.3
        );
        assert!(appends.0.is_ok() && appends.1.is_ok());
        assert!(matches!(&termination, Ok(Exit::Normal)));
        let receipt = receipt.expect("original terminating read receipt");
        let expected_origins = vec![origins.0, origins.1];
        assert_eq!(receipt.from, root);
        assert_eq!(receipt.message.as_slice(), expected_origins.as_slice());
        assert_eq!(rejected.as_ptr(), original_rejection_allocation);
        assert_eq!(&*rejected, &[29, 31]);
        assert_eq!(ledger.entries.len(), 2);
        assert_eq!((ledger.entries[0].0, ledger.entries[1].0), origins);
        assert_eq!(
            (ledger.entries[0].1.as_ptr(), ledger.entries[1].1.as_ptr()),
            allocations
        );
        assert_eq!(
            (&*ledger.entries[0].1, &*ledger.entries[1].1),
            (&[13, 17][..], &[19, 23][..])
        );
        assert_eq!(transfer_results.len(), 2);
        assert!(transfer_results.iter().all(Result::is_ok));
        assert!(matches!(&refused.0, Err(json::Error::MetadataObject)));
        assert!(refused.1.is_none() && refused.2.is_none() && refused.3.is_none());
        assert!(refused.4.is_ok());
        assert!(matches!(&refused.5, Some(Ok(reply)) if reply.result().is_err()));
        assert_eq!((&*refused.6, &*refused.7), (METADATA_ARRAY, PROOF));
        assert_eq!((refused.8, *after_refused), (1, 2));
        let expected_metadata = Metadata {
            wire_version: 1,
            schema_version: 1,
            provider_profile: "counter-record-test-v1".into(),
            deployment: "comparison".into(),
            caller: "caller".into(),
            caller_session: "fedcba9876543210fedcba9876543210".into(),
            controller: "owner".into(),
            actor: "ledger".into(),
            host: "host".into(),
            protocol: "ledger".into(),
            grant_issuer: "issuer".into(),
            runtime_generation: "123456789abcdef0123456789abcdef0".into(),
            actor_generation: 31,
            authority_generation: 19,
            request_sequence: 29,
        };
        let declaration = accepted.1.as_ref().unwrap().as_ref().unwrap();
        let accepted_payload = accepted
            .5
            .as_ref()
            .and_then(|received| received.as_ref().ok())
            .and_then(|reply| reply.result().ok())
            .map(|sample| sample.payload().to_bytes());
        let refused_payload = refused
            .5
            .as_ref()
            .and_then(|received| received.as_ref().ok())
            .and_then(|reply| reply.result().err())
            .map(|cause| cause.payload().to_bytes());
        assert_eq!(declaration.bombay, expected_metadata);
        assert_eq!(
            accepted_payload.as_deref(),
            Some("read completed".as_bytes())
        );
        assert_eq!(refused_payload.as_deref(), Some(b"refused".as_slice()));
        assert!(accepted.0.is_ok());
        assert!(matches!(&accepted.2, Some(Ok(1))));
        assert!(accepted.4.is_ok());
        assert!(matches!(&accepted.5, Some(Ok(reply)) if reply.result().is_ok()));
        assert_eq!((&*accepted.6, &*accepted.7), (LEDGER_OBJECT, PROOF));
        assert_eq!((accepted.8, *after_accepted), (1, 2));
    });
}
