//! One command reaches both real declarations; only its assigned actor executes.
use super::super::super::counter::{Counter, CounterMessage, CounterValue};
use super::super::assert_retirement;
use super::{peer, protocol, same_counter, wire};
use bombay::RetirementOutcome;
use bombay::actors::ActorExt;
use bombay::behavior::User;
use bombay::prelude::Application;
use bombay::remote::{Attempt, Export, ExportLimits, RefusalRetention, reply};
use bombay::{ActorFailureAssessment, ActorRetirement, MailAddr, RetirementAssessment};
use futures_util::future::Either;
use protocol::{
    Binding, EndpointRole, ProtectedMessage, ProviderProfile, configuration, limits,
    listener_address, recorded,
};
use std::cell::{Cell, RefCell};
use std::convert::Infallible;
use std::error::Error;
use std::num::NonZeroUsize;
use std::time::{Duration, Instant};
use tokio::runtime::Builder as RuntimeBuilder;
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

#[test]
fn same_name_deployments_authenticate_without_sharing_placement() {
    RuntimeBuilder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .expect("the ordinary native host opens")
        .block_on(async {
            tokio::time::timeout(Duration::from_secs(40), isolated_deployments())
                .await
                .expect("a watchdog supplies no deployment, admission or cleanup evidence");
        });
}

#[expect(
    clippy::too_many_lines,
    reason = "two reciprocal live deployments retain every original and join both owners before the first independent effect oracle"
)]
async fn isolated_deployments() {
    const COUNTER: &str = "bombay/deployments/counter";
    let mut first_deployment = wire::binding("counter");
    first_deployment.deployment = "first-shop";
    let mut second_deployment = wire::binding("counter");
    second_deployment.deployment = "second-shop";
    let (first_ready, first_matched) = oneshot::channel();
    let (second_ready, second_matched) = oneshot::channel();
    let (first_release, first_begin) = oneshot::channel();
    let (second_release, second_begin) = oneshot::channel();
    let journey = async |local: Binding,
                         foreign: Binding,
                         ready: oneshot::Sender<()>,
                         begin: oneshot::Receiver<()>| {
        let mut network = configuration("tls/localhost:0", &EndpointRole::Receiver);
        network.max_retained_queries = NonZeroUsize::new(2).unwrap();
        let node = Application::new(Counter::new().stop_on_shutdown())
            .open_node(network)
            .await
            .unwrap_or_else(|_| panic!("the actual configured Counter Node opens"));
        let address = listener_address(node.listen_addresses().await);
        let runtime = node.runtime_id();
        let peer = peer::Peer::open(&address).await;
        let caller = peer.runtime_id();
        let catalog = RefCell::new(Vec::<(Binding, ProtectedMessage)>::new());
        let protected = RefCell::new(Vec::new());
        let observed = RefCell::new(Vec::new());
        let decoded = Cell::new(0usize);
        let authenticated = Cell::new(0usize);
        let maximum = NonZeroUsize::new(2).unwrap();
        let deadline = Instant::now() + Duration::from_secs(30);
        let current =
            |receipt: &recorded::Receipt<CounterInput>, runtime: &str, target: MailAddr| {
                let authenticated = recorded::identity(receipt).0;
                wire::current(
                    recorded::projection(receipt),
                    runtime,
                    target,
                    *authenticated,
                    &caller,
                    deadline,
                )?;
                // A trusted identity scope and this receiver's placement are independent.
                if authenticated.deployment != local.deployment {
                    return Err(wire::Refusal::Scope);
                }
                Ok(())
            };
        let export = Export::new(
            COUNTER,
            ExportLimits {
                refusals: RefusalRetention::Retain,
                queued_queries: maximum,
                retained_queries: maximum,
                retained_requests: maximum,
                diagnostics: maximum,
            },
            limits(),
            |bytes| {
                decoded.set(decoded.get() + 1);
                wire::decode_counter(bytes)
            },
            reply(wire::increment, wire::processed),
            async |bytes: &[u8], proof: &[u8]| {
                observed.borrow_mut().push((bytes.as_ptr(), proof.as_ptr()));
                let original = ProtectedMessage {
                    protected: bytes.into(),
                    proof: proof.into(),
                };
                let catalog = catalog.borrow();
                let result = match catalog.iter().find(|(_, registered)| {
                    registered.protected.as_ref() == bytes && registered.proof.as_ref() == proof
                }) {
                    Some((trusted, registered)) => recorded::verify::<CounterInput>(
                        &original,
                        std::slice::from_ref(registered),
                        *trusted,
                    )
                    .into_inner(),
                    None => Err(protocol::VerificationError::Invalid),
                };
                if result.is_ok() {
                    authenticated.set(authenticated.get() + 1);
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
        let (published, configured) = oneshot::channel();
        let mut node = node.exports(export, async |application| {
            let target = application.root().established_recipient();
            let actor = application
                .actor_address(&target)
                .expect("the actual Counter is certified");
            let registered = RefCell::new(Vec::new());
            let mut requests = Vec::new();
            for (trusted, sequence) in [(local, 701), (foreign, 702)] {
                let mut claims = wire::claims(
                    ProviderProfile::Recorded,
                    "counter",
                    &caller,
                    &runtime,
                    actor,
                    sequence,
                );
                claims.deployment = trusted.deployment.into();
                requests.push(wire::register(
                    claims,
                    CounterInput::Increment(sequence),
                    &registered,
                ));
            }
            for (trusted, original) in [local, foreign].into_iter().zip(registered.into_inner()) {
                catalog.borrow_mut().push((trusted, original));
            }
            let published = published.send((actor, requests));
            published.unwrap_or_else(|_| panic!("the ordinary Work awaits both registered inputs"));
            Ok::<_, Infallible>(target)
        });
        let outcome = node
            .run_with(|application| {
                let peer = &peer;
                async move {
                    let (actor, requests) = configured
                        .await
                        .expect("actual binding supplies the requests");
                    peer.wait_for(COUNTER).await;
                    let ready = ready.send(());
                    let begin = begin.await;
                    let mut replies = Vec::new();
                    let mut endings = Vec::new();
                    let mut refusals = Vec::new();
                    for original in &requests {
                        let (mut receiving, refusal) = peer.query(COUNTER, original).await;
                        let mut acquired = Vec::new();
                        let ending = loop {
                            match receiving.recv().await {
                                Some(original) => acquired.push(original),
                                ending @ None => break ending,
                            }
                        };
                        replies.push(acquired);
                        endings.push(ending);
                        refusals.push(refusal);
                    }
                    let mut local = application
                        .client::<CounterValue>()
                        .unwrap_or_else(|_| panic!("the independent FIFO client allocates"));
                    let admitted = local.send(CounterMessage::Read(local.recipient())).await;
                    let value = local.receive().await;
                    local.close_admission();
                    let mut tail = Vec::new();
                    while let Some(original) = local.receive().await {
                        tail.push(original);
                    }
                    let shutdown = application.shutdown();
                    (
                        actor, requests, replies, endings, refusals, ready, begin, admitted, value,
                        tail, shutdown, local,
                    )
                }
            })
            .await
            .unwrap_or_else(|_| panic!("the actual deployment execution is issued"));
        let (export, bound, closed, polling, disposal, withdrawal_polling, withdrawal_disposal) =
            node.into_parts()
                .unwrap_or_else(|_| panic!("every native Node result remains retained"));
        let (work, report, native, notifications, ()) = outcome.into_parts();
        let (exchange, cleanup) = work
            .try_into_completed()
            .unwrap_or_else(|_| panic!("the completed ordinary Work remains owned"));
        // Drop captured verifier/policy inputs here; no local catalog loan escapes.
        let (policies, declarations, startup, withdrawal, diagnostics, rows) = export.into_parts();
        drop((policies, declarations));
        let rows = rows
            .into_iter()
            .map(|row| {
                row.try_into_parts()
                    .unwrap_or_else(|_| panic!("joined producers release original request custody"))
            })
            .collect::<Vec<_>>();
        let peer = peer.close().await;
        (
            local,
            foreign,
            runtime,
            caller,
            catalog.into_inner(),
            protected.into_inner(),
            observed.into_inner(),
            (decoded.get(), authenticated.get()),
            exchange,
            cleanup,
            report,
            native,
            notifications,
            bound,
            closed,
            (polling, disposal, withdrawal_polling, withdrawal_disposal),
            startup,
            withdrawal,
            diagnostics,
            rows,
            peer,
        )
    };
    let release = async {
        let matched = futures_util::future::join(first_matched, second_matched).await;
        let released = (first_release.send(()), second_release.send(()));
        (matched, released)
    };
    let ((first, second), barrier) = Box::pin(futures_util::future::join(
        futures_util::future::join(
            journey(
                first_deployment,
                second_deployment,
                first_ready,
                first_begin,
            ),
            journey(
                second_deployment,
                first_deployment,
                second_ready,
                second_begin,
            ),
        ),
        release,
    ))
    .await;
    // BOTH owners and peers have completed cleanup before the first effect oracle.
    let first_value = first.8.8.as_ref();
    let second_value = second.8.8.as_ref();
    assert_eq!(
        (first_value, second_value),
        (
            Some(&User::new(first.8.0, 1)),
            Some(&User::new(second.8.0, 1))
        )
    );
    assert_ne!(first.2, second.2);
    assert_ne!(first.3, second.3);
    assert_eq!(first.0, second.1);
    assert_eq!(first.1, second.0);
    let ((first_matched, second_matched), (first_released, second_released)) = barrier;
    assert!(
        first_matched.is_ok()
            && second_matched.is_ok()
            && first_released.is_ok()
            && second_released.is_ok()
    );
    for (
        local,
        foreign,
        runtime,
        caller,
        catalog,
        protected,
        observed,
        counts,
        exchange,
        cleanup,
        report,
        native,
        notifications,
        bound,
        closed,
        panics,
        startup,
        withdrawal,
        diagnostics,
        rows,
        peer,
    ) in [first, second]
    {
        let (
            actor,
            requests,
            replies,
            endings,
            refusals,
            ready,
            begin,
            admitted,
            value,
            tail,
            shutdown,
            _local,
        ) = exchange;
        cleanup.expect("the actual Application cleanup joins");
        closed.expect("the actual receiving Node Session closes");
        peer.closed
            .as_ref()
            .expect("the directed peer closes independently");
        let report = report.expect("the joined original report remains");
        let (origin, native) = native.expect("the complete original Counter remains");
        let notifications = notifications.expect("both original notifications remain");
        notifications.termination.expect("termination is published");
        notifications
            .retirement
            .expect("joined retirement is published");
        assert_retirement(&native);
        assert_eq!(report.retirement(), RetirementAssessment::Established);
        assert_eq!(report.failures(), ActorFailureAssessment::NoFailuresFound);
        assert_eq!(origin.address(), actor);
        assert_ne!(runtime, caller);
        assert!(
            matches!(bound, Some(Ok(_)))
                && matches!(startup, Some(Ok(())))
                && matches!(withdrawal, Some(Ok(())))
        );
        assert!(
            panics.0.is_none() && panics.1.is_none() && panics.2.is_none() && panics.3.is_none()
        );
        assert!(diagnostics.is_empty() && ready.is_ok() && begin.is_ok());
        assert!(admitted.is_ok() && shutdown.is_ok() && tail.is_empty());
        assert_eq!(value, Some(User::new(actor, 1)));
        assert_eq!(counts, (1, 2));
        assert_eq!(catalog.len(), 2);
        assert_eq!(requests.len(), 2);
        assert_eq!(observed.len(), 2);
        assert_eq!(rows.len(), 2);
        assert_eq!(protected.len(), 1);
        assert_eq!(replies.iter().map(Vec::len).collect::<Vec<_>>(), [1, 0]);
        assert!(endings.iter().all(Option::is_none));
        for refusal in &refusals {
            let refusal = refusal.lock().unwrap();
            assert!(refusal.is_none());
        }
        let [own, rejected]: [_; 2] = rows
            .try_into()
            .unwrap_or_else(|_| panic!("both expected actual originals remain"));
        for (index, row) in [own, rejected].into_iter().enumerate() {
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
            ) = row;
            assert_eq!(raw.0, requests[index].protected);
            assert_eq!(raw.1, requests[index].proof);
            assert_eq!((raw.0.as_ptr(), raw.1.as_ptr()), observed[index]);
            assert_eq!(raw.0, catalog[index].1.protected);
            assert_eq!(raw.1, catalog[index].1.proof);
            let receipt = verified
                .expect("both requests reached the actual verifier")
                .unwrap_or_else(|_| panic!("both trusted exact pairs authenticate"));
            let scope = recorded::projection(&receipt);
            let declared: wire::Request<CounterInput> = serde_json::from_slice(&raw.0).unwrap();
            assert_eq!(scope.bombay, declared.bombay);
            assert_eq!(scope.kind, protocol::Purpose::Request);
            assert_eq!(scope.operation, declared.operation);
            assert_eq!(
                recorded::identity(&receipt),
                (&catalog[index].0, scope.bombay.request_sequence)
            );
            assert_eq!(scope.bombay.runtime_generation.as_ref(), runtime);
            assert_eq!(scope.bombay.actor_generation, actor.0);
            assert_eq!(scope.bombay.caller_session.as_ref(), caller);
            assert!(matches!(panics, (None, None, None, None, None, None)));
            if index == 0 {
                assert_eq!(catalog[index].0, local);
                assert_eq!(scope.bombay.deployment.as_ref(), local.deployment);
                assert!(key.is_some() && input.is_none() && matches!(refusals, (None, None, None)));
                let (constructor, client, admission, received, tail) =
                    native.expect("the own native increment remains");
                assert!(constructor.is_none() && matches!(admission, Attempt::Returned(Ok(()))));
                let Attempt::Returned(Ok(client)) = client else {
                    panic!("the real native reply client remains");
                };
                assert_ne!(client.address(), actor);
                let received = received
                    .expect("the native receive completed")
                    .expect("the processed User remains");
                assert_eq!(
                    (received.from, received.message.request_sequence),
                    (actor, 701)
                );
                assert!(matches!(tail.as_slice(), [None]));
                let encoding = encoding
                    .expect("the native result encodes")
                    .unwrap_or_else(|_| panic!("actual encoding succeeds"));
                let protection = protection
                    .expect("the reply is protected")
                    .unwrap_or_else(|_| panic!("actual protection succeeds"));
                assert_eq!(encoding, protected[0].protected);
                assert_eq!(protection, protected[0].proof);
                assert!(matches!(publications.as_slice(), [Ok(())]));
            } else {
                assert_eq!(catalog[index].0, foreign);
                assert_ne!(local.deployment, foreign.deployment);
                assert_eq!(scope.bombay.deployment.as_ref(), foreign.deployment);
                assert!(matches!(refusals, (Some(wire::Refusal::Scope), None, None)));
                assert!(
                    key.is_none()
                        && input.is_none()
                        && native.is_none()
                        && encoding.is_none()
                        && protection.is_none()
                        && publications.is_empty()
                );
            }
        }
        let native_reply = replies[0][0].result();
        let sample = native_reply
            .as_ref()
            .expect("the own processed Sample remains");
        let original = ProtectedMessage {
            protected: sample.payload().to_bytes().as_ref().into(),
            proof: sample
                .attachment()
                .expect("the own proof remains")
                .to_bytes()
                .as_ref()
                .into(),
        };
        let receipt = recorded::verify::<CounterInput>(&original, &protected, local)
            .into_inner()
            .unwrap_or_else(|_| panic!("the actual published own pair verifies"));
        let parsed: serde_json::Value = serde_json::from_slice(&original.protected).unwrap();
        assert_eq!(sample.key_expr().as_str(), COUNTER);
        assert_eq!(original.protected, protected[0].protected);
        assert_eq!(original.proof, protected[0].proof);
        assert_eq!(
            parsed,
            serde_json::json!({
                "bombay": recorded::projection(&receipt).bombay,
                "operation": CounterInput::Increment(701),
                "kind": protocol::Purpose::Processed,
                "value": 701,
            })
        );
    }
}

#[expect(
    clippy::too_many_lines,
    reason = "one two-responder trace preserves both original receptions, native actor states and every joined owner before its ownership oracle"
)]
async fn overlapping_counters() {
    let address = "tls/localhost:0".to_owned();
    let assigned = Application::new(Counter::new().stop_on_shutdown())
        .open_node(configuration(&address, &EndpointRole::Receiver))
        .await
        .unwrap_or_else(|(_, _, cause)| {
            panic!(
                "the assigned Counter Node opens: {cause}; source: {:?}",
                cause.source()
            )
        });
    let other_address = "tls/localhost:0".to_owned();
    let other = Application::new(Counter::new().stop_on_shutdown())
        .open_node(configuration(&other_address, &EndpointRole::Receiver))
        .await
        .unwrap_or_else(|(_, _, cause)| {
            panic!(
                "the other Counter Node opens: {cause}; source: {:?}",
                cause.source()
            )
        });
    let address = listener_address(assigned.listen_addresses().await);
    let other_address = listener_address(other.listen_addresses().await);
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
    let (export, bound, closed, polling, disposal, withdrawal_polling, withdrawal_disposal) =
        assigned
            .into_parts()
            .unwrap_or_else(|_| panic!("the assigned Node finalizes"));
    let (
        other_export,
        other_bound,
        other_closed,
        other_polling,
        other_disposal,
        other_withdrawal_polling,
        other_withdrawal_disposal,
    ) = other
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
        let ActorRetirement::Retained {
            custody,
            outcome:
                RetirementOutcome::Completed {
                    interpretation,
                    source,
                    ..
                },
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
    assert!(withdrawal_polling.is_none() && withdrawal_disposal.is_none());
    assert!(other_withdrawal_polling.is_none() && other_withdrawal_disposal.is_none());
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
