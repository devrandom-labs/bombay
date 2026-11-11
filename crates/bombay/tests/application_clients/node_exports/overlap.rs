//! One command reaches both real declarations; only its assigned actor executes.
use super::super::super::counter::{Counter, CounterMessage, CounterValue};
use super::super::assert_retirement;
use super::{peer, protocol, same_counter, wire};
use bombay::actors::ActorExt;
use bombay::behavior::User;
use bombay::prelude::Application;
use bombay::remote::{Attempt, Export, ExportLimits, RefusalRetention, reply};
use bombay::{ActorFailureAssessment, ActorRetirement, MailAddr, RetirementAssessment};
use futures_util::future::Either;
use protocol::{
    EndpointRole, ProtectedMessage, ProviderProfile, configuration, limits, recorded,
    reserved_address,
};
use std::cell::{Cell, RefCell};
use std::convert::Infallible;
use std::num::NonZeroUsize;
use std::time::{Duration, Instant};
use tokio::sync::oneshot;
use wire::CounterInput;

const ASSIGNED: &str = "bombay/overlap/selected";
const OTHER: &str = "bombay/overlap/unselected";
const COMMAND: &str = "bombay/overlap/*";

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn overlapping_queries_admit_only_the_assigned_counter() {
    tokio::time::timeout(Duration::from_secs(40), Box::pin(overlapping_counters()))
        .await
        .expect("watchdog supplies no reception, ownership or cleanup credit");
}

#[expect(
    clippy::too_many_lines,
    reason = "one two-responder trace preserves both original receptions, native actor states and every joined owner before its ownership oracle"
)]
async fn overlapping_counters() {
    let address = reserved_address();
    let assigned = Application::new(Counter::new().stop_on_shutdown())
        .open_node(configuration(&address, &EndpointRole::Receiver))
        .await
        .unwrap_or_else(|_| panic!("the assigned Counter Node opens"));
    let other_address = reserved_address();
    let other = Application::new(Counter::new().stop_on_shutdown())
        .open_node(configuration(&other_address, &EndpointRole::Receiver))
        .await
        .unwrap_or_else(|_| panic!("the other Counter Node opens"));
    let assigned_runtime = assigned.runtime_id();
    let other_runtime = other.runtime_id();
    // Each receiver has a direct connection; no implicit Peer relay is assumed.
    let peer = peer::Peer::open_to(&[&address, &other_address]).await;
    let caller_runtime = peer.runtime_id();
    let requests = RefCell::new(Vec::new());
    let protected = RefCell::new(Vec::new());
    let other_protected = RefCell::new(Vec::new());
    let originals = RefCell::new(Vec::new());
    let other_originals = RefCell::new(Vec::new());
    let transfers = RefCell::new(Vec::new());
    let other_transfers = RefCell::new(Vec::new());
    let decoded = Cell::new(0);
    let other_decoded = Cell::new(0);
    let constructed = Cell::new(0);
    let other_constructed = Cell::new(0);
    let (entered, entering) = oneshot::channel();
    let (other_entered, other_entering) = oneshot::channel();
    let (release, released) = oneshot::channel();
    let (other_release, other_released) = oneshot::channel();
    let first = RefCell::new(Some((entered, released)));
    let other_first = RefCell::new(Some((other_entered, other_released)));
    let maximum = NonZeroUsize::new(2).unwrap();
    let export_limits = ExportLimits {
        refusals: RefusalRetention::Retain,
        queued_queries: maximum,
        retained_queries: maximum,
        retained_requests: maximum,
        diagnostics: maximum,
    };
    let deadline = Instant::now() + Duration::from_secs(30);
    let current = |receipt: &recorded::Receipt<CounterInput>, runtime: &str, target: MailAddr| {
        wire::current(
            recorded::projection(receipt),
            runtime,
            target,
            wire::binding("counter"),
            &caller_runtime,
            deadline,
        )
    };
    let other_current =
        |receipt: &recorded::Receipt<CounterInput>, runtime: &str, target: MailAddr| {
            wire::current(
                recorded::projection(receipt),
                runtime,
                target,
                wire::binding("counter"),
                &caller_runtime,
                deadline,
            )
        };
    let export = Export::new(
        ASSIGNED,
        export_limits,
        limits(),
        |bytes| {
            decoded.set(decoded.get() + 1);
            wire::decode_counter(bytes)
        },
        (
            reply(
                |input| {
                    let constructed = &constructed;
                    wire::increment(input).map(|command| {
                        move |reply_to| {
                            constructed.set(constructed.get() + 1);
                            command(reply_to)
                        }
                    })
                },
                wire::processed,
            ),
            reply(
                |input| {
                    let constructed = &constructed;
                    wire::counter_read(input).map(|command| {
                        move |reply_to| {
                            constructed.set(constructed.get() + 1);
                            command(reply_to)
                        }
                    })
                },
                wire::value,
            ),
        ),
        async |bytes: &[u8], proof: &[u8]| {
            let original = ProtectedMessage {
                protected: bytes.into(),
                proof: proof.into(),
            };
            let result = recorded::verify::<CounterInput>(
                &original,
                &requests.borrow(),
                wire::binding("counter"),
            )
            .into_inner();
            let scope = result.as_ref().ok().map(|receipt| {
                serde_json::to_vec(&recorded::projection(receipt).bombay)
                    .expect("actual receipt metadata serializes for observation")
            });
            originals
                .borrow_mut()
                .push((bytes.as_ptr(), proof.as_ptr(), scope));
            let first = first.borrow_mut().take();
            if let Some((entered, released)) = first {
                let announced = entered.send(());
                let released = released.await;
                transfers.borrow_mut().push((announced, released));
            }
            result
        },
        async |bytes: &[u8], receipt: &recorded::Receipt<CounterInput>| {
            wire::protect(bytes, recorded::projection(receipt), &protected, maximum)
        },
        same_counter,
        &current,
        &current,
    );
    let other_export = Export::new(
        OTHER,
        export_limits,
        limits(),
        |bytes| {
            other_decoded.set(other_decoded.get() + 1);
            wire::decode_counter(bytes)
        },
        (
            reply(
                |input| {
                    let other_constructed = &other_constructed;
                    wire::increment(input).map(|command| {
                        move |reply_to| {
                            other_constructed.set(other_constructed.get() + 1);
                            command(reply_to)
                        }
                    })
                },
                wire::processed,
            ),
            reply(
                |input| {
                    let other_constructed = &other_constructed;
                    wire::counter_read(input).map(|command| {
                        move |reply_to| {
                            other_constructed.set(other_constructed.get() + 1);
                            command(reply_to)
                        }
                    })
                },
                wire::value,
            ),
        ),
        async |bytes: &[u8], proof: &[u8]| {
            let original = ProtectedMessage {
                protected: bytes.into(),
                proof: proof.into(),
            };
            let result = recorded::verify::<CounterInput>(
                &original,
                &requests.borrow(),
                wire::binding("counter"),
            )
            .into_inner();
            let scope = result.as_ref().ok().map(|receipt| {
                serde_json::to_vec(&recorded::projection(receipt).bombay)
                    .expect("actual receipt metadata serializes for observation")
            });
            other_originals
                .borrow_mut()
                .push((bytes.as_ptr(), proof.as_ptr(), scope));
            let first = other_first.borrow_mut().take();
            if let Some((entered, released)) = first {
                let announced = entered.send(());
                let released = released.await;
                other_transfers.borrow_mut().push((announced, released));
            }
            result
        },
        async |bytes: &[u8], receipt: &recorded::Receipt<CounterInput>| {
            wire::protect(
                bytes,
                recorded::projection(receipt),
                &other_protected,
                maximum,
            )
        },
        same_counter,
        &other_current,
        &current,
    );
    let (configured, inputs) = oneshot::channel();
    let (other_bound, other_binding) = oneshot::channel();
    let mut assigned = assigned.exports(export, async |application| {
        let target = application.root().established_recipient();
        let actor = application
            .actor_address(&target)
            .expect("actual assigned Counter");
        let increment = wire::register(
            wire::claims(
                ProviderProfile::Recorded,
                "counter",
                &caller_runtime,
                &assigned_runtime,
                actor,
                431,
            ),
            CounterInput::Increment(431),
            &requests,
        );
        let read = wire::register(
            wire::claims(
                ProviderProfile::Recorded,
                "counter",
                &caller_runtime,
                &assigned_runtime,
                actor,
                432,
            ),
            CounterInput::Read,
            &requests,
        );
        let publication = configured.send((actor, [increment, read]));
        publication.unwrap_or_else(|_| panic!("peer retains the actual configured originals"));
        Ok::<_, Infallible>(target)
    });
    let mut other = other.exports(other_export, async |application| {
        let target = application.root().established_recipient();
        let actor = application
            .actor_address(&target)
            .expect("actual other Counter");
        let publication = other_bound.send(actor);
        publication.expect("peer retains the independently certified other actor");
        Ok::<_, Infallible>(target)
    });
    let (finished, finish) = oneshot::channel();
    let (other_finished, other_finish) = oneshot::channel();
    let (closed, closing) = oneshot::channel();
    let (other_closed, other_closing) = oneshot::channel();
    let execution = async {
        let outcome = assigned
            .run_with(|application| async move {
                let finished = finish.await;
                let mut local = application.client::<CounterValue>().unwrap();
                let admitted = local.send(CounterMessage::Read(local.recipient())).await;
                let value = local.receive().await;
                local.close_admission();
                let mut tail = Vec::new();
                while let Some(original) = local.receive().await {
                    tail.push(original);
                }
                let shutdown = application.shutdown();
                (finished, admitted, value, tail, shutdown, local)
            })
            .await;
        let publication = closed.send(());
        (outcome, publication)
    };
    let other_execution = async {
        let outcome = other
            .run_with(|application| async move {
                let finished = other_finish.await;
                let mut local = application.client::<CounterValue>().unwrap();
                let admitted = local.send(CounterMessage::Read(local.recipient())).await;
                let value = local.receive().await;
                local.close_admission();
                let mut tail = Vec::new();
                while let Some(original) = local.receive().await {
                    tail.push(original);
                }
                let shutdown = application.shutdown();
                (finished, admitted, value, tail, shutdown, local)
            })
            .await;
        let publication = other_closed.send(());
        (outcome, publication)
    };
    let exchange = async {
        let (configured, other_actor) = tokio::join!(inputs, other_binding);
        let (actor, pair) = configured.expect("actual assigned binding supplies both requests");
        let other_actor = other_actor.expect("actual other binding precedes its declaration");
        peer.wait_for(ASSIGNED).await;
        peer.wait_for(OTHER).await;
        let (mut increment, increment_refusal) = peer.query(COMMAND, &pair[0]).await;
        let entered = tokio::join!(entering, other_entering);
        // Both actual verifier entries are acquired; neither assertion nor reply
        // expectation can prevent either producer from being released.
        let released = (release.send(()), other_release.send(()));
        let mut processed = Vec::new();
        let increment_eof = loop {
            match increment.recv().await {
                Some(original) => processed.push(original),
                original @ None => break original,
            }
        };
        let (mut read, read_refusal) = peer.query(COMMAND, &pair[1]).await;
        let mut values = Vec::new();
        let read_eof = loop {
            match read.recv().await {
                Some(original) => values.push(original),
                original @ None => break original,
            }
        };
        let completed = (finished.send(()), other_finished.send(()));
        let close_observations = tokio::join!(closing, other_closing);
        let peer = peer.close().await;
        (
            actor,
            other_actor,
            pair,
            entered,
            released,
            completed,
            close_observations,
            processed,
            values,
            increment_eof,
            read_eof,
            increment_refusal,
            read_refusal,
            peer,
        )
    };
    let ((outcome, publication), (other_outcome, other_publication), exchange) =
        tokio::join!(execution, other_execution, exchange);
    let outcome = outcome.unwrap_or_else(|_| panic!("assigned Work enters the actual Node"));
    let other_outcome =
        other_outcome.unwrap_or_else(|_| panic!("other Work enters the actual Node"));
    let (export, bound, closed, polling, disposal) = assigned
        .into_parts()
        .unwrap_or_else(|_| panic!("the assigned Node finalizes"));
    let (other_export, other_bound, other_closed, other_polling, other_disposal) = other
        .into_parts()
        .unwrap_or_else(|_| panic!("the other Node finalizes independently"));
    let (work, report, native, notes, ()) = outcome.into_parts();
    let (other_work, other_report, other_native, other_notes, ()) = other_outcome.into_parts();
    let ((finished, admitted, value, tail, shutdown, _local), cleanup) = work
        .try_into_completed()
        .unwrap_or_else(|_| panic!("assigned Work retained its independent FIFO Read"));
    let (
        (other_finished, other_admitted, other_value, other_tail, other_shutdown, _other_local),
        other_cleanup,
    ) = other_work
        .try_into_completed()
        .unwrap_or_else(|_| panic!("other Work retained its independent FIFO Read"));
    let (origin, native) = native.expect("the actual assigned native root remains");
    let (other_origin, other_native) = other_native.expect("the actual other native root remains");
    let report = report.expect("the independent assigned report remains");
    let other_report = other_report.expect("the independent other report remains");
    let notes = notes.expect("both assigned notifications remain");
    let other_notes = other_notes.expect("both other notifications remain");
    let (_inputs, _declarations, startup, withdrawal, ingress, rows) = export.into_parts();
    let (
        _other_inputs,
        _other_declarations,
        other_startup,
        other_withdrawal,
        other_ingress,
        other_rows,
    ) = other_export.into_parts();
    let (
        actor,
        other_actor,
        pair,
        entered,
        released,
        completed,
        close_observations,
        processed,
        values,
        increment_eof,
        read_eof,
        increment_refusal,
        read_refusal,
        peer,
    ) = exchange;
    // Complete both independent native owner chains and the peer close BEFORE
    // the first ownership oracle. Preserve the original results, never labels.
    cleanup.expect("actual assigned cleanup joins");
    other_cleanup.expect("actual other cleanup joins");
    closed.expect("assigned Session close succeeds");
    other_closed.expect("other Session close succeeds");
    peer.closed
        .as_ref()
        .expect("independent peer Session close succeeds");
    publication.expect("peer awaited assigned Node close");
    other_publication.expect("peer awaited other Node close");
    close_observations
        .0
        .expect("actual assigned close publication");
    close_observations
        .1
        .expect("actual other close publication");
    for notes in [notes, other_notes] {
        notes.termination.expect("actual termination publication");
        notes
            .retirement
            .expect("actual joined retirement publication");
    }
    let expected_other = Some(User::new(other_actor, 0));
    assert_eq!(
        other_value, expected_other,
        "a matching advertisement must not authorize the unassigned Counter"
    );
    let expected = Some(User::new(actor, 1));
    assert_eq!(value, expected);
    assert_retirement(&native);
    assert_retirement(&other_native);
    for original in [&native, &other_native] {
        let ActorRetirement::Completed {
            custody,
            interpretation,
            source,
            ..
        } = original
        else {
            panic!("both original native roots completed their typed shutdown");
        };
        assert!(interpretation.is_none() && source.is_none());
        assert!(
            custody.control.is_empty() && custody.user.is_empty() && custody.descendants.is_empty()
        );
        assert!(custody.unread_owner_cancellation.is_none());
        assert!(custody.received_interpretation.is_none() && custody.received_source.is_none());
        assert!(
            custody.source_index.is_none()
                && custody.acquired_ingress.is_none()
                && custody.terminal_report.is_none()
        );
    }
    for report in [report, other_report] {
        assert_eq!(report.retirement(), RetirementAssessment::Established);
        assert_eq!(report.failures(), ActorFailureAssessment::NoFailuresFound);
    }
    assert_eq!(origin.address(), actor);
    assert_eq!(other_origin.address(), other_actor);
    assert_ne!(assigned_runtime, other_runtime);
    assert_ne!(caller_runtime, assigned_runtime);
    assert_ne!(caller_runtime, other_runtime);
    assert!(
        polling.is_none()
            && disposal.is_none()
            && other_polling.is_none()
            && other_disposal.is_none()
    );
    assert!(matches!(bound, Some(Ok(_))) && matches!(other_bound, Some(Ok(_))));
    assert!(matches!(startup, Some(Ok(()))) && matches!(withdrawal, Some(Ok(()))));
    assert!(matches!(other_startup, Some(Ok(()))) && matches!(other_withdrawal, Some(Ok(()))));
    assert!(ingress.is_empty() && other_ingress.is_empty());
    assert!(
        finished.is_ok() && other_finished.is_ok() && admitted.is_ok() && other_admitted.is_ok()
    );
    assert!(shutdown.is_ok() && other_shutdown.is_ok() && tail.is_empty() && other_tail.is_empty());
    assert!(entered.0.is_ok() && entered.1.is_ok() && released.0.is_ok() && released.1.is_ok());
    assert!(completed.0.is_ok() && completed.1.is_ok());
    assert!(increment_eof.is_none() && read_eof.is_none());
    let increment_refusal = increment_refusal.lock().unwrap();
    let read_refusal = read_refusal.lock().unwrap();
    assert!(increment_refusal.is_none() && read_refusal.is_none());
    assert_eq!((rows.len(), other_rows.len()), (2, 2));
    assert_eq!((decoded.get(), constructed.get()), (2, 2));
    assert_eq!((other_decoded.get(), other_constructed.get()), (0, 0));
    let originals = originals.borrow();
    let other_originals = other_originals.borrow();
    let registered = requests.borrow();
    let protected = protected.borrow();
    let other_protected = other_protected.borrow();
    let transfers = transfers.borrow();
    let other_transfers = other_transfers.borrow();
    let binding = wire::binding("counter");
    assert_eq!((transfers.len(), other_transfers.len()), (1, 1));
    for (announced, released) in transfers.iter().chain(other_transfers.iter()) {
        assert!(announced.is_ok() && released.is_ok());
    }
    assert_eq!(
        (originals.len(), other_originals.len(), registered.len()),
        (2, 2, 2)
    );
    for (registered, original) in registered.iter().zip(&pair) {
        assert_eq!(registered.protected, original.protected);
        assert_eq!(registered.proof, original.proof);
    }
    assert_eq!(protected.len(), 2);
    assert!(other_protected.is_empty());
    let mut keys = Vec::new();
    let mut clients = Vec::new();
    for (index, row) in rows.into_iter().enumerate() {
        let (
            key,
            raw,
            verified,
            input,
            native,
            encoding,
            protection,
            publications,
            refusals,
            panics,
        ) = row
            .try_into_parts()
            .unwrap_or_else(|_| panic!("assigned joined producers release original custody"));
        let receipt = verified
            .expect("assigned verification was attempted")
            .unwrap_or_else(|_| panic!("the complete actual registered pair verifies"));
        let scope = recorded::projection(&receipt);
        let expected: wire::Request<CounterInput> =
            serde_json::from_slice(&pair[index].protected).unwrap();
        let metadata = serde_json::to_vec(&scope.bombay).unwrap();
        assert_eq!(raw.0, pair[index].protected);
        assert_eq!(raw.1, pair[index].proof);
        assert_eq!(
            (raw.0.as_ptr(), raw.1.as_ptr()),
            (originals[index].0, originals[index].1)
        );
        assert_eq!(originals[index].2.as_ref(), Some(&metadata));
        assert_eq!(scope.bombay, expected.bombay);
        assert_eq!(scope.kind, expected.kind);
        assert_eq!(scope.operation, expected.operation);
        let identity = recorded::identity(&receipt);
        assert_eq!(identity, (&binding, scope.bombay.request_sequence));
        assert!(input.is_none() && matches!(refusals, (None, None, None)));
        assert!(matches!(panics, (None, None, None, None, None, None)));
        keys.push(key.expect("a real admitted operation issued its own key"));
        match native.expect("assigned native operation remains") {
            Either::Left((constructor, client, admission, received, tail)) => {
                let Attempt::Returned(Ok(client)) = client else {
                    panic!("actual processing client");
                };
                let received = received
                    .expect("actual native receive")
                    .expect("actual processing reply");
                assert_eq!(index, 0);
                assert_eq!(
                    (received.from, received.message.request_sequence),
                    (actor, 431)
                );
                assert!(constructor.is_none() && matches!(admission, Attempt::Returned(Ok(()))));
                assert!(matches!(tail.as_slice(), [None]));
                clients.push(client.address());
            }
            Either::Right((constructor, client, admission, received, tail)) => {
                let Attempt::Returned(Ok(client)) = client else {
                    panic!("actual value client");
                };
                let received = received
                    .expect("actual native receive")
                    .expect("actual value reply");
                assert_eq!(index, 1);
                assert_eq!((received.from, received.message), (actor, 1));
                assert!(constructor.is_none() && matches!(admission, Attempt::Returned(Ok(()))));
                assert!(matches!(tail.as_slice(), [None]));
                clients.push(client.address());
            }
        }
        let encoding = encoding
            .expect("actual encoding result")
            .unwrap_or_else(|_| panic!("native original encodes"));
        let protection = protection
            .expect("actual protection result")
            .unwrap_or_else(|_| panic!("actual pair protection succeeds"));
        assert_eq!(encoding, protected[index].protected);
        assert_eq!(protection, protected[index].proof);
        assert!(matches!(publications.as_slice(), [Ok(())]));
    }
    assert_ne!(keys[0], keys[1]);
    assert_ne!(clients[0], clients[1]);
    assert!(clients.iter().all(|client| *client != actor));
    for (index, row) in other_rows.into_iter().enumerate() {
        let (
            key,
            raw,
            verified,
            input,
            native,
            encoding,
            protection,
            publications,
            refusals,
            panics,
        ) = row
            .try_into_parts()
            .unwrap_or_else(|_| panic!("other joined producers release original custody"));
        let receipt = verified
            .expect("other verification was actually attempted")
            .unwrap_or_else(|_| panic!("the SAME full pair verifies at the other responder"));
        let scope = recorded::projection(&receipt);
        let expected: wire::Request<CounterInput> =
            serde_json::from_slice(&pair[index].protected).unwrap();
        let metadata = serde_json::to_vec(&scope.bombay).unwrap();
        assert_eq!(raw.0, pair[index].protected);
        assert_eq!(raw.1, pair[index].proof);
        assert_eq!(
            (raw.0.as_ptr(), raw.1.as_ptr()),
            (other_originals[index].0, other_originals[index].1)
        );
        assert_eq!(other_originals[index].2.as_ref(), Some(&metadata));
        assert_eq!(scope.bombay, expected.bombay);
        assert_eq!(scope.kind, expected.kind);
        assert_eq!(scope.operation, expected.operation);
        let identity = recorded::identity(&receipt);
        assert_eq!(identity, (&binding, scope.bombay.request_sequence));
        assert_ne!(scope.bombay.runtime_generation.as_ref(), other_runtime);
        assert!(matches!(refusals, (Some(wire::Refusal::Scope), None, None)));
        assert!(
            key.is_none()
                && input.is_none()
                && native.is_none()
                && encoding.is_none()
                && protection.is_none()
                && publications.is_empty()
        );
        assert!(matches!(panics, (None, None, None, None, None, None)));
    }
    assert_eq!((processed.len(), values.len()), (1, 1));
    for (index, replies) in [&processed, &values].into_iter().enumerate() {
        let result = replies[0].result();
        let sample = result
            .as_ref()
            .expect("the actual selected native Sample remains");
        let bytes = sample.payload().to_bytes();
        let proof = sample
            .attachment()
            .expect("actual complete proof")
            .to_bytes();
        let original = ProtectedMessage {
            protected: bytes.as_ref().into(),
            proof: proof.as_ref().into(),
        };
        let receipt =
            recorded::verify::<CounterInput>(&original, &protected, wire::binding("counter"))
                .into_inner()
                .unwrap_or_else(|_| panic!("the exact received full pair verifies"));
        let scope = recorded::projection(&receipt);
        let request: wire::Request<CounterInput> =
            serde_json::from_slice(&pair[index].protected).unwrap();
        let value: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
        let expected_kind = if index == 0 {
            protocol::Purpose::Processed
        } else {
            protocol::Purpose::ReadReply
        };
        let expected_value = if index == 0 { 431 } else { 1 };
        assert_eq!(sample.key_expr().as_str(), ASSIGNED);
        assert_eq!(bytes.as_ref(), protected[index].protected.as_ref());
        assert_eq!(proof.as_ref(), protected[index].proof.as_ref());
        assert_eq!(scope.bombay, request.bombay);
        assert_eq!(scope.operation, request.operation);
        assert_eq!(scope.kind, expected_kind);
        assert_eq!(value["value"], expected_value);
    }
}
