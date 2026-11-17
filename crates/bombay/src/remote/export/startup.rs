//! Native declaration rollback before later Node cleanup can repair a prefix.
use super::super::query::Exports;
use super::counter::{Counter, CounterMessage};
use super::protocol::{
    Binding, EndpointRole, ProtectedMessage, ProviderProfile, Purpose, ReadRequest, ReadRequestRef,
    VerificationError, configuration, current_scope, limits, metadata, recorded,
};
use super::{
    Command, Event, Export, ExportError, ExportLimits, RefusalRetention, completed_actor,
    query_replies, reply,
};
use crate::actors::ActorExt;
use crate::{ActorFailureAssessment, Application, MailAddr, RetirementAssessment};
use behavior::{EstablishedRecipient, Never, User};
use futures_util::future::Either;
use std::error::Error;
use std::num::NonZeroUsize;
use std::ptr;
use std::sync::{Arc, Weak};
use std::time::{Duration, Instant};
use tokio::time::timeout;
use zenoh::sample::Locality;

enum Declarations {
    PrefixProduct,
    SuffixProduct,
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn refused_suffix_withdraws_a_declared_product_before_returning() {
    failed_startups_restore_the_declared_prefix(Declarations::PrefixProduct).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn refused_nested_suffix_withdraws_each_declared_prefix_before_returning() {
    failed_startups_restore_the_declared_prefix(Declarations::SuffixProduct).await;
}

#[expect(
    clippy::too_many_lines,
    reason = "one nested prefix trace snapshots native facts before rescue, then checks repeated recovery after real cleanup"
)]
async fn failed_startups_restore_the_declared_prefix(declarations: Declarations) {
    timeout(Duration::from_secs(30), async {
        let session = zenoh::open(
            configuration("tls/localhost:0", &EndpointRole::Receiver)
                .to_configuration()
                .unwrap(),
        )
        .await
        .unwrap();
        let paths = [
            "bombay/startup/first",
            "bombay/startup/second",
            "bombay/startup/last",
        ];
        let mut queriers = Vec::new();
        for path in paths {
            queriers.push(
                session
                    .declare_querier(path)
                    .allowed_destination(Locality::SessionLocal)
                    .await
                    .unwrap(),
            );
        }
        let outcome = Application::new(Counter::new().stop_on_shutdown())
            .run_with(|application| {
                let session = &session;
                let queriers = &queriers;
                async move {
                    let mut caller = application.client().unwrap();
                    let incremented = caller.send(CounterMessage::Increment).await;
                    let read = caller.send(CounterMessage::Read(caller.recipient())).await;
                    let before = caller.receive().await;
                    let origin = application
                        .actor_address(&application.root().established_recipient())
                        .unwrap();
                    let runtime = session.zid().to_string();
                    let mut claims = metadata(
                        ProviderProfile::Recorded,
                        "counter",
                        runtime.clone(),
                        runtime,
                    );
                    claims.actor_generation = origin.0;
                    let binding = Binding {
                        deployment: "two-node-read-study",
                        caller: "caller",
                        controller: "owner",
                        actor: "counter",
                        host: "receiver",
                        protocol: "counter",
                        target_assignment: "one-live-counter",
                        logical_session: 503,
                    };
                    let record = ProtectedMessage {
                        protected: serde_json::to_vec(&ReadRequestRef {
                            bombay: &claims,
                            kind: Purpose::Request,
                            operation: Command::Read,
                        })
                        .unwrap()
                        .into_boxed_slice(),
                        proof: Box::from([11_u8, 17]),
                    };
                    let records = [record];
                    let verify = async |bytes: &[u8], proof: &[u8]| {
                        recorded::verify::<Command>(
                            &ProtectedMessage {
                                protected: bytes.into(),
                                proof: proof.into(),
                            },
                            &records,
                            binding,
                        )
                        .await
                    };
                    let protect = async |_: &[u8], _: &recorded::Receipt<Command>| {
                        Ok::<_, Never>(Box::<[u8]>::from([19_u8, 23]))
                    };
                    let deadline = Instant::now() + Duration::from_secs(20);
                    let current = |receipt: &recorded::Receipt<Command>,
                                   runtime: &str,
                                   target: MailAddr| {
                        let scope = recorded::projection(receipt);
                        if scope.bombay.runtime_generation.as_ref() != runtime
                            || scope.bombay.actor_generation != target.0
                            || recorded::identity(receipt).0 != &binding
                        {
                            return Err(VerificationError::Invalid);
                        }
                        current_scope(scope, &claims, &scope.operation, Purpose::Request, deadline)
                    };
                    let maximum = NonZeroUsize::new(2).unwrap();
                    let reads = |path: &str| {
                        Export::new(
                            path,
                            ExportLimits {
                                refusals: RefusalRetention::Retain,
                                queued_queries: maximum,
                                retained_queries: maximum,
                                retained_requests: maximum,
                                diagnostics: maximum,
                            },
                            limits(),
                            |bytes: &[u8]| {
                                serde_json::from_slice::<ReadRequest<Command>>(bytes)
                                    .map(|request| request.operation)
                            },
                            reply(
                                |command| match command {
                                    Command::Read => Ok(CounterMessage::Read),
                                    command @ Command::Increment => Err(command),
                                },
                                |_: &recorded::Receipt<Command>, original: &User<MailAddr, u64>| {
                                    Ok::<_, Never>(
                                        original.message.to_ne_bytes().to_vec().into_boxed_slice(),
                                    )
                                },
                            ),
                            &verify,
                            &protect,
                            |left, right| recorded::identity(left) == recorded::identity(right),
                            &current,
                            &current,
                        )
                    };
                    // This private declaration law does not claim public target preflight.
                    let mut complete = match declarations {
                        Declarations::PrefixProduct => {
                            Either::Left(((reads(paths[0]), reads(paths[1])), reads(paths[2])))
                        }
                        Declarations::SuffixProduct => {
                            Either::Right((reads(paths[0]), (reads(paths[1]), reads(paths[2]))))
                        }
                    };
                    let (first, second, last) = match &complete {
                        Either::Left(((first, second), last))
                        | Either::Right((first, (second, last))) => (first, second, last),
                    };
                    let control_allocations = [
                        Arc::downgrade(&first.ingress),
                        Arc::downgrade(&second.ingress),
                        Arc::downgrade(&last.ingress),
                    ];
                    let control_baseline = control_allocations.each_ref().map(Weak::strong_count);
                    let declared = match &mut complete {
                        Either::Left(exports) => {
                            Exports::<
                                Counter,
                                Event,
                                (),
                                (
                                    (EstablishedRecipient<Counter>, EstablishedRecipient<Counter>),
                                    EstablishedRecipient<Counter>,
                                ),
                            >::declare(exports, session)
                            .await
                        }
                        Either::Right(exports) => {
                            Exports::<
                                Counter,
                                Event,
                                (),
                                (
                                    EstablishedRecipient<Counter>,
                                    (EstablishedRecipient<Counter>, EstablishedRecipient<Counter>),
                                ),
                            >::declare(exports, session)
                            .await
                        }
                    };
                    let control_declared = control_allocations.each_ref().map(Weak::strong_count);
                    let mut advertised = Vec::new();
                    for querier in queriers {
                        advertised.push(querier.matching_status().await);
                    }
                    match &mut complete {
                        Either::Left(exports) => {
                            Exports::<
                                Counter,
                                Event,
                                (),
                                (
                                    (EstablishedRecipient<Counter>, EstablishedRecipient<Counter>),
                                    EstablishedRecipient<Counter>,
                                ),
                            >::withdraw(exports)
                            .await;
                        }
                        Either::Right(exports) => {
                            Exports::<
                                Counter,
                                Event,
                                (),
                                (
                                    EstablishedRecipient<Counter>,
                                    (EstablishedRecipient<Counter>, EstablishedRecipient<Counter>),
                                ),
                            >::withdraw(exports)
                            .await;
                        }
                    }
                    let control_withdrawn = control_allocations.each_ref().map(Weak::strong_count);
                    let complete = match complete {
                        Either::Left(((first, second), last))
                        | Either::Right((first, (second, last))) => [first, second, last],
                    }
                    .map(|export| {
                        let (inputs, _declarations, startup, withdrawal, diagnostics, requests) =
                            export.into_parts();
                        (inputs.0, startup, withdrawal, diagnostics, requests)
                    });
                    let control_released = control_allocations.each_ref().map(Weak::strong_count);
                    let control_owners = (
                        control_baseline,
                        control_declared,
                        control_withdrawn,
                        control_released,
                    );
                    let mut observations = Vec::new();
                    for _ in 0..3 {
                        let mut refused = match declarations {
                            Declarations::PrefixProduct => {
                                Either::Left(((reads(paths[0]), reads(paths[1])), reads("")))
                            }
                            Declarations::SuffixProduct => {
                                Either::Right((reads(paths[0]), (reads(paths[1]), reads(""))))
                            }
                        };
                        let (first, second, last) = match &refused {
                            Either::Left(((first, second), last))
                            | Either::Right((first, (second, last))) => (first, second, last),
                        };
                        let allocations = [
                            Arc::downgrade(&first.ingress),
                            Arc::downgrade(&second.ingress),
                            Arc::downgrade(&last.ingress),
                        ];
                        let baseline = allocations.each_ref().map(Weak::strong_count);
                        let refused_result = match &mut refused {
                            Either::Left(exports) => {
                                Exports::<
                                    Counter,
                                    Event,
                                    (),
                                    (
                                        (
                                            EstablishedRecipient<Counter>,
                                            EstablishedRecipient<Counter>,
                                        ),
                                        EstablishedRecipient<Counter>,
                                    ),
                                >::declare(exports, session)
                                .await
                            }
                            Either::Right(exports) => {
                                Exports::<
                                    Counter,
                                    Event,
                                    (),
                                    (
                                        EstablishedRecipient<Counter>,
                                        (
                                            EstablishedRecipient<Counter>,
                                            EstablishedRecipient<Counter>,
                                        ),
                                    ),
                                >::declare(exports, session)
                                .await
                            }
                        };
                        // Capture every cutoff fact before any rescue or extraction.
                        let mut matching = Vec::new();
                        for querier in queriers {
                            matching.push(querier.matching_status().await);
                        }
                        let (first, second, last) = match &refused {
                            Either::Left(((first, second), last))
                            | Either::Right((first, (second, last))) => (first, second, last),
                        };
                        let prefix = [first.withdrawal.try_get(), second.withdrawal.try_get()];
                        let suffix = last.withdrawal.try_get();
                        let owners = allocations.each_ref().map(Weak::strong_count);
                        let native_source = match last.startup.as_ref() {
                            Some(Err(ExportError::Declaration { cause })) => {
                                Some(ptr::from_ref(cause.as_ref()).cast::<()>())
                            }
                            _ => None,
                        };
                        let session_closed = session.is_closed();
                        // This later cleanup must not manufacture the captured prefix result.
                        match &mut refused {
                            Either::Left(exports) => {
                                Exports::<
                                    Counter,
                                    Event,
                                    (),
                                    (
                                        (
                                            EstablishedRecipient<Counter>,
                                            EstablishedRecipient<Counter>,
                                        ),
                                        EstablishedRecipient<Counter>,
                                    ),
                                >::withdraw(exports)
                                .await;
                            }
                            Either::Right(exports) => {
                                Exports::<
                                    Counter,
                                    Event,
                                    (),
                                    (
                                        EstablishedRecipient<Counter>,
                                        (
                                            EstablishedRecipient<Counter>,
                                            EstablishedRecipient<Counter>,
                                        ),
                                    ),
                                >::withdraw(exports)
                                .await;
                            }
                        }
                        let entries =
                            match refused {
                                Either::Left(((first, second), last))
                                | Either::Right((first, (second, last))) => [first, second, last],
                            }
                            .map(|export| {
                                let (
                                    inputs,
                                    _declarations,
                                    startup,
                                    withdrawal,
                                    diagnostics,
                                    requests,
                                ) = export.into_parts();
                                (inputs.0, startup, withdrawal, diagnostics, requests)
                            });
                        let released = allocations.each_ref().map(Weak::strong_count);
                        observations.push((
                            refused_result,
                            matching,
                            prefix,
                            suffix,
                            baseline,
                            owners,
                            native_source,
                            session_closed,
                            entries,
                            released,
                        ));
                    }
                    let incremented_again = caller.send(CounterMessage::Increment).await;
                    let read_again = caller.send(CounterMessage::Read(caller.recipient())).await;
                    let after = caller.receive().await;
                    let mut recovered = reads(paths[0]);
                    let replies = query_replies(
                        &mut recovered,
                        &application,
                        session,
                        paths[0],
                        &[&records[0]],
                    )
                    .await;
                    let (_inputs, _declarations, startup, withdrawal, diagnostics, requests) =
                        recovered.into_parts();
                    let recovered = (startup, withdrawal, diagnostics, requests);
                    let shutdown = application.shutdown();
                    (
                        declared,
                        advertised,
                        complete,
                        control_owners,
                        observations,
                        recovered,
                        replies,
                        incremented,
                        read,
                        before,
                        incremented_again,
                        read_again,
                        after,
                        origin,
                        shutdown,
                    )
                }
            })
            .await
            .unwrap_or_else(|_| panic!("the real Counter Application enters its host"));
        let (work, report, native, notifications, ()) = outcome.into_parts();
        let (
            (
                declared,
                advertised,
                complete,
                control_owners,
                observations,
                recovered,
                replies,
                incremented,
                read,
                before,
                incremented_again,
                read_again,
                after,
                origin,
                shutdown,
            ),
            cleanup,
        ) = work
            .try_into_completed()
            .unwrap_or_else(|_| panic!("real Work and cleanup complete"));
        let mut inspections = Vec::new();
        for querier in queriers {
            inspections.push(querier.undeclare().await);
        }
        let closed = session.close().await;
        let (root_origin, root) = native.expect("the actual Counter retirement remains");
        let root_address = root_origin.address();
        let _counter = completed_actor(root_origin, root);
        let report = report.expect("the actual independent retirement report remains");
        let notifications = notifications.expect("both actual notification receipts remain");
        assert!(cleanup.is_ok() && closed.is_ok() && shutdown.is_ok());
        assert_eq!(inspections.len(), 3);
        assert!(inspections.iter().all(Result::is_ok));
        assert!(declared.is_ok());
        assert_eq!(advertised.len(), 3);
        assert!(advertised.iter().all(|status| {
            status
                .as_ref()
                .expect("actual positive native matching")
                .matching()
        }));
        let (baseline, declared_owners, withdrawn_owners, released_owners) = control_owners;
        assert_eq!(baseline, [1, 1, 1]);
        assert_eq!(declared_owners, [2, 2, 2]);
        assert_eq!(
            withdrawn_owners, baseline,
            "native withdrawal restores diagnostic ownership"
        );
        assert_eq!(released_owners, [0, 0, 0]);
        assert_eq!(root_address, origin);
        assert_eq!(report.retirement(), RetirementAssessment::Established);
        assert_eq!(report.failures(), ActorFailureAssessment::NoFailuresFound);
        assert!(notifications.termination.is_ok() && notifications.retirement.is_ok());
        assert!(
            incremented.is_ok() && read.is_ok() && incremented_again.is_ok() && read_again.is_ok()
        );
        assert_eq!(before, Some(User::new(origin, 1)));
        assert_eq!(after, Some(User::new(origin, 2)));
        for (_, startup, withdrawal, diagnostics, requests) in complete {
            assert!(matches!(startup, Some(Ok(()))) && matches!(withdrawal, Some(Ok(()))));
            assert!(diagnostics.is_empty() && requests.is_empty());
        }
        assert_eq!(observations.len(), 3);
        for (
            refused,
            matching,
            prefix,
            suffix,
            baseline,
            owners,
            native_source,
            session_closed,
            entries,
            released,
        ) in observations
        {
            assert!(refused.is_err() && !session_closed);
            assert_eq!(matching.len(), 3);
            assert!(
                matching.iter().all(|status| !status
                    .as_ref()
                    .expect("actual prefix native matching")
                    .matching()),
                "an actual refused tuple must withdraw every advertised prefix before returning"
            );
            assert!(
                prefix
                    .iter()
                    .all(|result| matches!(result, Some(Some(Ok(())))))
            );
            assert!(
                suffix.is_none(),
                "a never-declared suffix is still pending before rescue"
            );
            assert_eq!(baseline, [1, 1, 1]);
            assert_eq!(
                owners, baseline,
                "rollback restores the measured diagnostic ownership"
            );
            assert_eq!(released, [0, 0, 0]);
            let [first, second, last] = entries;
            for (_, startup, withdrawal, diagnostics, requests) in [first, second] {
                assert!(matches!(startup, Some(Ok(()))) && matches!(withdrawal, Some(Ok(()))));
                assert!(diagnostics.is_empty() && requests.is_empty());
            }
            let (key, startup, withdrawal, diagnostics, requests) = last;
            assert_eq!(key, "");
            assert!(withdrawal.is_none() && diagnostics.is_empty() && requests.is_empty());
            let Some(Err(cause @ ExportError::Declaration { .. })) = startup else {
                panic!("the original native suffix constructor refusal remains typed");
            };
            assert_eq!(
                Some(ptr::from_ref(cause.source().unwrap()).cast::<()>()),
                native_source
            );
        }
        let (startup, withdrawal, diagnostics, requests) = recovered;
        assert!(matches!(startup, Some(Ok(()))) && matches!(withdrawal, Some(Ok(()))));
        assert!(diagnostics.is_empty());
        assert_eq!(requests.len(), 1);
        let [replies] = <[_; 1]>::try_from(replies).unwrap();
        let [reply] = <[_; 1]>::try_from(replies).unwrap();
        let received = reply.result();
        let sample = received.as_ref().unwrap();
        assert_eq!(sample.payload().to_bytes().as_ref(), 2_u64.to_ne_bytes());
        assert_eq!(sample.attachment().unwrap().to_bytes().as_ref(), [19, 23]);
    })
    .await
    .expect("failure-only watchdog; rollback is observed before unconditional rescue");
}
