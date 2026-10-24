//! Verification receives the original protected text and independent opaque proof.
use super::super::super::ledger::{LedgerCommand, MailLedger, Receipts};
use super::super::assert_retirement;
use super::{peer, protocol, same_ledger, wire};
use bombay::prelude::Application;
use bombay::remote::{Attempt, Export, ExportLimits, RefusalRetention, submission};
use bombay::{ActorFailureAssessment, ActorRetirement, MailAddr, RetirementAssessment};
use protocol::{
    Binding, EndpointRole, ProtectedMessage, ProviderProfile, Purpose, configuration, limits,
    reserved_address, scheduled,
};
use std::cell::{Cell, RefCell};
use std::convert::Infallible;
use std::num::NonZeroUsize;
use std::pin::pin;
use std::time::{Duration, Instant};
use tokio::sync::oneshot;
use wire::LedgerInput;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn noncanonical_requests_keep_their_original_registered_pair() {
    let trace = pin!(registered_originals());
    tokio::time::timeout(Duration::from_secs(40), trace)
        .await
        .expect("watchdog supplies no byte preservation, processing or cleanup credit");
}

#[expect(
    clippy::too_many_lines,
    reason = "one native request retains the complete protected pair, moved Ledger payload and independently joined owners before the byte oracle"
)]
async fn registered_originals() {
    let address = reserved_address();
    let node = Application::new(MailLedger {
        entries: Vec::new(),
    })
    .open_node(configuration(&address, &EndpointRole::Receiver))
    .await
    .unwrap_or_else(|_| panic!("the actual Ledger Node opens"));
    let receiver_id = node.runtime_id();
    let peer = peer::Peer::open(&address).await;
    let caller_id = peer.runtime_id();
    let binding = Binding {
        caller: "café 🦀",
        ..wire::binding("ledger")
    };
    let sequence = 9_007_199_254_740_993_u64;
    let payload = "café 🦀".as_bytes();
    let requests = RefCell::new(Vec::new());
    let replies = RefCell::new(Vec::new());
    let observations = RefCell::new(Vec::new());
    let allocation = RefCell::new(None);
    let constructors = Cell::new(0);
    let maximum = NonZeroUsize::new(1).unwrap();
    let deadline = Instant::now() + Duration::from_secs(30);
    let current = |receipt: &scheduled::Receipt<LedgerInput>, runtime: &str, target: MailAddr| {
        wire::current(
            scheduled::projection(receipt),
            runtime,
            target,
            binding,
            &caller_id,
            deadline,
        )
    };
    #[expect(
        clippy::await_holding_refcell_ref,
        reason = "the actual Scheduled transfer borrows an immutable registered-pair table; no producer mutates it after binding"
    )]
    let verify = async |bytes: &[u8], proof: &[u8]| {
        observations.borrow_mut().push((
            bytes.to_vec(),
            proof.to_vec(),
            bytes.as_ptr(),
            proof.as_ptr(),
        ));
        let original = ProtectedMessage {
            protected: bytes.into(),
            proof: proof.into(),
        };
        let records = requests.borrow();
        scheduled::verify::<LedgerInput>(&original, &records, binding).await
    };
    let export = Export::new(
        peer::LEDGER,
        ExportLimits {
            refusals: RefusalRetention::Retain,
            queued_queries: maximum,
            retained_queries: maximum,
            retained_requests: maximum,
            diagnostics: maximum,
        },
        limits(),
        |bytes| wire::decode_ledger(bytes, &allocation),
        submission(
            |input| {
                let constructors = &constructors;
                wire::append(input).map(|command| {
                    move || {
                        constructors.set(constructors.get() + 1);
                        command()
                    }
                })
            },
            wire::appended,
        ),
        verify,
        async |bytes: &[u8], receipt: &scheduled::Receipt<LedgerInput>| {
            wire::protect(bytes, scheduled::projection(receipt), &replies, maximum)
        },
        same_ledger,
        &current,
        &current,
    );
    let (configured, inputs) = oneshot::channel();
    let mut node = node.exports(export, async |application| {
        let target = application.root().established_recipient();
        let actor = application
            .actor_address(&target)
            .expect("actual certified Ledger");
        let mut claims = wire::claims(
            ProviderProfile::Scheduled,
            "ledger",
            &caller_id,
            &receiver_id,
            actor,
            sequence,
        );
        claims.caller = binding.caller.into();
        let request = wire::Request {
            bombay: claims,
            kind: Purpose::Request,
            operation: LedgerInput::Append(payload.into()),
        };
        let pretty = serde_json::to_vec_pretty(&request).expect("the existing Request serializes");
        let mut protected = Vec::from(b"\t ");
        protected.extend_from_slice(&pretty);
        protected.extend_from_slice(b"\n ");
        let original = ProtectedMessage {
            protected: protected.into_boxed_slice(),
            proof: Box::from(*b"\0current-node-proof\xff\x80\r\n"),
        };
        requests.borrow_mut().push(ProtectedMessage {
            protected: original.protected.clone(),
            proof: original.proof.clone(),
        });
        let publication = configured.send((actor, original));
        publication.expect("the peer retains the exact configured original pair");
        Ok::<_, Infallible>(target)
    });
    let (finished, finish) = oneshot::channel();
    let (closed, closing) = oneshot::channel();
    let execution = async {
        let outcome = node
            .run_with(|application| async move {
                let finished = finish.await;
                let mut local = application.client::<Receipts>().unwrap();
                let admitted = local.send(LedgerCommand::Read(local.recipient())).await;
                let received = local.receive().await;
                local.close_admission();
                let mut tail = Vec::new();
                let eof = loop {
                    match local.receive().await {
                        Some(original) => tail.push(original),
                        original @ None => break original,
                    }
                };
                (finished, admitted, received, tail, eof, local)
            })
            .await;
        let publication = closed.send(());
        (outcome, publication)
    };
    let exchange = async {
        let (actor, original) = inputs
            .await
            .expect("binding supplies the registered original");
        peer.wait_for(peer::LEDGER).await;
        let (mut stream, refused) = peer.query(peer::LEDGER, &original).await;
        let mut received = Vec::new();
        let eof = loop {
            match stream.recv().await {
                Some(original) => received.push(original),
                original @ None => break original,
            }
        };
        // A refusal also produces real native EOF, so both counterfactuals
        // reach the local terminal Read and cleanup without a reply unwrap.
        let finished = finished.send(());
        let closed = closing.await;
        let peer = peer.close().await;
        (
            actor, original, received, eof, refused, finished, closed, peer,
        )
    };
    let ((outcome, publication), exchange) = tokio::join!(execution, exchange);
    let outcome = outcome.unwrap_or_else(|_| panic!("actual Ledger Work is invoked"));
    let (export, bound, closed, polling, disposal) = node
        .into_parts()
        .unwrap_or_else(|_| panic!("the actual Node finalizes"));
    let (work, report, native, notes, ()) = outcome.into_parts();
    let ((finished, admitted, received, tail, local_eof, _local), cleanup) = work
        .try_into_completed()
        .unwrap_or_else(|_| panic!("the real local terminal Read finishes"));
    let (_inputs, _declarations, startup, withdrawal, ingress, rows) = export.into_parts();
    let originals = rows
        .into_iter()
        .map(|row| {
            row.try_into_parts()
                .unwrap_or_else(|_| panic!("joined cleanup ended every actual producer loan"))
        })
        .collect::<Vec<_>>();
    let (actor, original, samples, eof, refused, completed, close_observed, peer) = exchange;
    // All cleanup operations have completed before either pair-axis oracle.
    cleanup.expect("actual application cleanup joins");
    closed.expect("the actual Node Session closes");
    peer.closed
        .as_ref()
        .expect("the independent native peer Session closes");
    publication.expect("the peer awaited the actual Node close");
    close_observed.expect("the actual Node close was observed");
    finished.expect("Work observed actual query EOF");
    completed.expect("the peer published its actual query completion");
    let observed_pointers = {
        let observations = observations.borrow();
        let [(observed_bytes, observed_proof, bytes_pointer, proof_pointer)] =
            observations.as_slice()
        else {
            panic!("one actual verifier invocation must retain its supplied pair");
        };
        assert_eq!(
            observed_bytes.as_slice(),
            original.protected.as_ref(),
            "verification must receive the original noncanonical protected text"
        );
        assert_eq!(
            observed_proof.as_slice(),
            original.proof.as_ref(),
            "verification must receive the entire independent opaque proof"
        );
        (*bytes_pointer, *proof_pointer)
    };

    let [request] = originals.as_slice() else {
        panic!("the actual accepted query retains one original Request");
    };
    let declared: wire::Request<LedgerInput> =
        serde_json::from_slice(&original.protected).expect("the original current Request is valid");
    let compact = serde_json::to_vec(&declared).expect("the typed current Request serializes");
    let unicode = binding.caller.as_bytes();
    let contains_unicode = original
        .protected
        .windows(unicode.len())
        .any(|window| window == unicode);
    let input_pointers = (request.1.0.as_ptr(), request.1.1.as_ptr());
    assert_eq!(request.1.0, original.protected);
    assert_eq!(request.1.1, original.proof);
    assert_eq!(input_pointers, observed_pointers);
    assert_ne!(original.protected.as_ref(), compact.as_slice());
    assert!(original.protected.starts_with(b"\t "));
    assert!(original.protected.ends_with(b"\n "));
    assert!(contains_unicode);
    assert_eq!(declared.bombay.request_sequence, sequence);
    assert_eq!(declared.bombay.caller.as_ref(), binding.caller);
    let expected_operation = LedgerInput::Append(payload.into());
    assert_eq!(declared.operation, expected_operation);
    assert_eq!(declared.kind, Purpose::Request);
    {
        let registry = requests.borrow();
        let [registered] = registry.as_slice() else {
            panic!("the actual provider owns exactly the transmitted pair");
        };
        assert_eq!(registered.protected, original.protected);
        assert_eq!(registered.proof, original.proof);
    }
    let receipt = request
        .2
        .as_ref()
        .expect("actual verification returned")
        .as_ref()
        .expect("the exact complete registered pair verified");
    let scope = scheduled::projection(receipt);
    let proof = scheduled::proof(receipt);
    let (receipt_binding, receipt_sequence) = scheduled::identity(receipt);
    assert_eq!(scope.bombay, declared.bombay);
    assert_eq!(scope.kind, declared.kind);
    assert_eq!(scope.operation, declared.operation);
    assert_eq!(receipt_binding, &binding);
    assert_eq!(receipt_sequence, sequence);
    assert_eq!(proof, original.proof.as_ref());
    assert!(request.0.is_some() && request.3.is_none());
    assert!(matches!(&request.8, (None, None, None)));
    assert!(matches!(&request.9, (None, None, None, None, None, None)));
    let (unsent, sender, admission) = request
        .4
        .as_ref()
        .expect("the actual Submission retains its native facts");
    let Attempt::Returned(Ok(sender)) = sender else {
        panic!("the actual native sender was acquired");
    };
    let sender = sender.address();
    assert!(unsent.is_none());
    assert!(matches!(admission, Attempt::Returned(Ok(()))));
    let constructed = constructors.get();
    assert_eq!(constructed, 1);
    let encoded = request
        .5
        .as_ref()
        .expect("actual admission encoding was attempted")
        .as_ref()
        .expect("actual admission encoding succeeded");
    let protected = request
        .6
        .as_ref()
        .expect("actual reply protection was attempted")
        .as_ref()
        .expect("actual reply protection succeeded");
    let [published] = request.7.as_slice() else {
        panic!("one actual publication remains");
    };
    assert!(published.is_ok());
    let reply_registry = replies.take();
    let [registered_reply] = reply_registry.as_slice() else {
        panic!("one independently registered reply remains");
    };
    assert_eq!(encoded, &registered_reply.protected);
    assert_eq!(protected, &registered_reply.proof);
    let [sample] = samples.as_slice() else {
        panic!("one actual native admitted reply precedes EOF");
    };
    let result = sample.result();
    let sample = result
        .as_ref()
        .expect("the actual successful native Sample");
    let native_payload = sample.payload().to_bytes();
    let attachment = sample.attachment().expect("the actual reply carries proof");
    let native_proof = attachment.to_bytes();
    let sample_key = sample.key_expr().as_str();
    assert_eq!(sample_key, peer::LEDGER);
    assert_eq!(native_payload.as_ref(), encoded.as_ref());
    assert_eq!(native_proof.as_ref(), protected.as_ref());
    let response = ProtectedMessage {
        protected: native_payload.into_owned().into_boxed_slice(),
        proof: native_proof.into_owned().into_boxed_slice(),
    };
    let reply_receipt = scheduled::verify::<LedgerInput>(&response, &reply_registry, binding)
        .await
        .expect("the whole actual wire reply pair is independently registered");
    let reply_scope = scheduled::projection(&reply_receipt);
    assert_eq!(reply_scope.bombay, scope.bombay);
    assert_eq!(reply_scope.operation, scope.operation);
    assert_eq!(reply_scope.kind, Purpose::Admitted);
    let wire_reply: serde_json::Value =
        serde_json::from_slice(&response.protected).expect("the actual encoded reply parses");
    assert_eq!(wire_reply["value"], "admitted");
    let expected_origins = vec![sender];
    let received = received.expect("the actual local terminal Read returns its origins");
    assert_eq!(received.from, actor);
    assert_eq!(received.message, expected_origins);
    assert!(admitted.is_ok() && tail.is_empty() && local_eof.is_none());
    assert!(eof.is_none());
    let refused = refused.lock().unwrap();
    assert!(refused.is_none());
    assert!(ingress.is_empty());
    let (origin, native) = native.expect("the actual native Ledger root remains");
    assert_retirement(&native);
    let ActorRetirement::Completed {
        custody,
        interpretation,
        source,
        ..
    } = &native
    else {
        panic!("the actual Ledger Read completed its root");
    };
    assert_eq!(origin.address(), actor);
    assert!(interpretation.is_none() && source.is_none());
    assert!(
        custody.control.is_empty() && custody.user.is_empty() && custody.descendants.is_empty()
    );
    assert!(custody.unread_owner_cancellation.is_none());
    assert!(custody.received_interpretation.is_none() && custody.received_source.is_none());
    assert!(custody.source_index.is_none() && custody.acquired_ingress.is_none());
    assert!(custody.terminal_report.is_none());
    let [(entry_origin, entry)] = custody.behavior.entries.as_slice() else {
        panic!("the actual native Ledger stores one original Append");
    };
    let allocation = *allocation.borrow();
    let allocation = allocation.expect("the real decoder recorded its selected native Box");
    assert_eq!(*entry_origin, sender);
    assert_eq!(entry.as_ref(), payload);
    assert_eq!(entry.as_ptr(), allocation);
    let report = report.expect("the independent actual root report remains");
    assert_eq!(report.retirement(), RetirementAssessment::Established);
    assert_eq!(report.failures(), ActorFailureAssessment::NoFailuresFound);
    let notes = notes.expect("both original notification results remain");
    notes.termination.expect("actual termination publication");
    notes.retirement.expect("actual retirement publication");
    bound
        .expect("actual binding returned")
        .expect("actual binding succeeded");
    startup
        .expect("actual declaration attempted")
        .expect("actual declaration succeeded");
    withdrawal
        .expect("actual withdrawal attempted")
        .expect("actual withdrawal succeeded");
    assert!(polling.is_none() && disposal.is_none());
}
