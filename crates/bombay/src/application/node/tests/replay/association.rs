//! Exact request reply-endpoint isolation and post-retirement retention.
mod counter {
    //! A late A reply stays with A while B acquires its own actual value.
    //! One finite Recorded provider and local native Counter; no Query or shipping identity claim.
    use super::super::{
        Binding, Counter, CounterMessage, CounterOperation, CounterValue, EndpointRole,
    };
    use super::super::{
        ProtectedMessage, ProviderProfile, Purpose, ReadRequestRef, VerificationError,
    };
    use super::super::{
        assert_completed, assert_notifications, configuration, current_scope, metadata,
    };
    use super::super::{recorded, reserved_address};
    use bombay::actors::ActorExt;
    use bombay::prelude::Application;
    use bombay::remote::{Admission, ExportLimits, IngressError, verify};
    use bombay::{ActorFailureAssessment, RetirementAssessment};
    use std::num::NonZeroUsize;
    use std::sync::{Arc, Mutex};
    use std::time::{Duration, Instant};
    use tokio::runtime::Builder;
    use tokio::sync::mpsc;
    use zenoh::query::Query;

    #[test]
    #[expect(
        clippy::too_many_lines,
        reason = "one actual A/B reply trace retains every client and native cleanup before asserting isolation"
    )]
    fn public_counter_request_b_does_not_acquire_a_late_reply() {
        Builder::new_multi_thread()
            .worker_threads(1)
            .enable_all()
            .build()
            .unwrap()
            .block_on(async {
                let address = reserved_address();
                let refusals = Arc::new(Mutex::new(Vec::<IngressError<Query>>::new()));
                let observed = Arc::clone(&refusals);
                let mut node = Application::new(Counter::new().stop_on_shutdown())
                    .open_node(configuration(&address, &EndpointRole::Receiver))
                    .await
                    .unwrap_or_else(|_| panic!("actual configured receiver Node"));
                let connection = node.session.clone();
                let session = &connection;
                let (execution, receiving) = node
                    .execute_with(|application| async move {
                        let target = application.root().established_recipient();
                        let target_origin = application.root().address();
                        let mut export = application
                            .export::<_, CounterValue, recorded::Receipt<CounterOperation>>(
                                target,
                                "bombay/association/read",
                                ExportLimits {
                                    queued_queries: NonZeroUsize::new(2).unwrap(),
                                    retained_queries: NonZeroUsize::new(2).unwrap(),
                                    retained_requests: NonZeroUsize::new(2).unwrap(),
                                },
                                |a, b| recorded::identity(a) == recorded::identity(b),
                                move |original| observed.lock().unwrap().push(original),
                            )
                            .await
                            .unwrap_or_else(|_| panic!("actual public export"));
                        let sender = application.sender().unwrap();
                        let mut fence = application
                            .client_to::<_, CounterValue>(
                                application.root().established_recipient(),
                            )
                            .unwrap_or_else(|_| panic!("actual independent FIFO fence client"));
                        let primed = sender.send(CounterMessage::Increment).await;
                        let binding = Binding {
                            deployment: "two-node-read-study",
                            caller: "caller",
                            controller: "owner",
                            actor: "counter",
                            host: "receiver",
                            protocol: "counter",
                            target_assignment: "association-counter",
                            logical_session: 41,
                        };
                        let current_a = metadata(
                            ProviderProfile::Recorded,
                            "counter",
                            session.zid().to_string(),
                            session.zid().to_string(),
                        );
                        let mut current_b = metadata(
                            ProviderProfile::Recorded,
                            "counter",
                            session.zid().to_string(),
                            session.zid().to_string(),
                        );
                        current_b.request_sequence =
                            current_a.request_sequence.checked_add(1).unwrap();
                        let bytes_a = serde_json::to_vec(&ReadRequestRef {
                            bombay: &current_a,
                            kind: Purpose::Request,
                            operation: CounterOperation::Read,
                        })
                        .unwrap();
                        let bytes_b = serde_json::to_vec(&ReadRequestRef {
                            bombay: &current_b,
                            kind: Purpose::Request,
                            operation: CounterOperation::Read,
                        })
                        .unwrap();
                        let records = [
                            ProtectedMessage {
                                protected: bytes_a.clone().into_boxed_slice(),
                                proof: Box::from([7, 11]),
                            },
                            ProtectedMessage {
                                protected: bytes_b.clone().into_boxed_slice(),
                                proof: Box::from([7, 11]),
                            },
                        ];
                        let original_a = ProtectedMessage {
                            protected: bytes_a.into_boxed_slice(),
                            proof: Box::from([7, 11]),
                        };
                        let original_b = ProtectedMessage {
                            protected: bytes_b.into_boxed_slice(),
                            proof: Box::from([7, 11]),
                        };
                        let allocations = [
                            (original_a.protected.as_ptr(), original_a.proof.as_ptr()),
                            (original_b.protected.as_ptr(), original_b.proof.as_ptr()),
                        ];
                        let mut verified_a = None;
                        let mut verified_b = None;
                        verify(&original_a, &mut verified_a, |r| {
                            recorded::verify::<CounterOperation>(r, &records, binding)
                        })
                        .await;
                        verify(&original_b, &mut verified_b, |r| {
                            recorded::verify::<CounterOperation>(r, &records, binding)
                        })
                        .await;
                        let receipt_a = verified_a
                            .take()
                            .expect("actual acquired A provider result")
                            .expect("configured positive A proof");
                        let receipt_b = verified_b
                            .take()
                            .expect("actual acquired B provider result")
                            .expect("configured positive B proof");
                        let retained_a =
                            export.retain(original_a.protected, original_a.proof, receipt_a);
                        let retained_b =
                            export.retain(original_b.protected, original_b.proof, receipt_b);
                        let deadline = Instant::now() + Duration::from_secs(30);
                        let acquired = if let (Ok((a, None)), Ok((b, None))) =
                            (&retained_a, &retained_b)
                        {
                            let mut saved_reply = None;
                            let admitted_a = export
                                .attempt(
                                    a,
                                    |receipt| {
                                        current_scope(
                                            recorded::projection(receipt),
                                            &current_a,
                                            &CounterOperation::Read,
                                            Purpose::Request,
                                            deadline,
                                        )?;
                                        Ok::<_, VerificationError>(&current_a)
                                    },
                                    |reply_to| {
                                        saved_reply = Some(reply_to.clone());
                                        CounterMessage::Read(reply_to)
                                    },
                                )
                                .map(|_| ());
                            let received_a = export.receive_native(a).await;
                            let extra_a = if let Some(reply_to) = &saved_reply {
                                Some(sender.send(CounterMessage::Read(reply_to.clone())).await)
                            } else {
                                None
                            };
                            let incremented = sender.send(CounterMessage::Increment).await;
                            let admitted_b = export
                                .attempt(
                                    b,
                                    |receipt| {
                                        current_scope(
                                            recorded::projection(receipt),
                                            &current_b,
                                            &CounterOperation::Read,
                                            Purpose::Request,
                                            deadline,
                                        )?;
                                        Ok::<_, VerificationError>(&current_b)
                                    },
                                    CounterMessage::Read,
                                )
                                .map(|_| ());
                            let received_b = export.receive_native(b).await;
                            // A control-lane shutdown must not overtake the accepted B prefix.
                            let fence_reply = fence.recipient();
                            let fence_sent = fence.send(CounterMessage::Read(fence_reply)).await;
                            let fence_user = if fence_sent.is_ok() {
                                fence.receive().await
                            } else {
                                None
                            };
                            Some((
                                admitted_a,
                                received_a,
                                extra_a,
                                incremented,
                                admitted_b,
                                received_b,
                                saved_reply,
                                fence_sent,
                                fence_user,
                            ))
                        } else {
                            None
                        };
                        let stopped = application.request_shutdown();
                        let report = application.lifecycle().retirement().await;
                        (
                            export,
                            retained_a,
                            retained_b,
                            acquired,
                            primed,
                            stopped,
                            report,
                            target_origin,
                            allocations,
                            fence,
                        )
                    })
                    .unwrap_or_else(|_| panic!("actual receiver Work enters the host"));
                let ((), outcome) = tokio::join!(execution, receiving);
                let (outcome, native, notifications, ()) = outcome.into_parts();
                let (
                    (
                        export,
                        retained_a,
                        retained_b,
                        acquired,
                        primed,
                        stopped,
                        report,
                        target_origin,
                        allocations,
                        mut fence,
                    ),
                    cleanup,
                ) = outcome
                    .try_into_completed()
                    .unwrap_or_else(|_| panic!("actual original Work acquired"));
                let (undeclared, session, mut incoming, requests) = export.undeclare().await;
                let mut retained = Vec::new();
                for (key, protected, proof, receipt, admission, native, mut client) in requests {
                    client.close_admission();
                    let mut tail = Vec::new();
                    while let Some(original) = client.receive().await {
                        tail.push(original);
                    }
                    retained.push((
                        key, protected, proof, receipt, admission, native, client, tail,
                    ));
                }
                let requests = retained;
                let mut query_drain = Vec::new();
                let query_end = loop {
                    match incoming.try_recv() {
                        Ok(original) => query_drain.push(original),
                        Err(cause) => break cause,
                    }
                };
                fence.close_admission();
                let mut fence_drain = Vec::new();
                while let Some(original) = fence.receive().await {
                    fence_drain.push(original);
                }
                drop(session);
                let closed = node.close().await;
                assert!(connection.is_closed());
                let native = native.expect("actual full native root");
                assert_completed(native, None);
                assert_notifications(&notifications);
                assert!(cleanup.is_ok() && undeclared.is_ok() && closed.is_ok());
                assert!(primed.is_ok() && stopped.is_ok());
                assert_eq!(report.retirement(), RetirementAssessment::Established);
                assert_eq!(report.failures(), ActorFailureAssessment::NoFailuresFound);
                assert!(query_drain.is_empty());
                assert!(matches!(query_end, mpsc::error::TryRecvError::Disconnected));
                assert!(refusals.lock().unwrap().is_empty());
                let (Ok((a, None)), Ok((b, None))) = (&retained_a, &retained_b) else {
                    panic!("distinct actual retained requests");
                };
                let first = requests.iter().find(|row| &row.0 == a).unwrap();
                let second = requests.iter().find(|row| &row.0 == b).unwrap();
                let actual_reply_recipient = first.6.recipient();
                let other_reply_recipient = second.6.recipient();
                let Some((
                    admitted_a,
                    received_a,
                    extra_a,
                    incremented,
                    admitted_b,
                    received_b,
                    saved_reply,
                    fence_sent,
                    fence_user,
                )) = acquired
                else {
                    panic!("both actual attempts were acquired");
                };
                assert!(admitted_a.is_ok() && admitted_b.is_ok());
                assert_eq!(received_a, Some(()));
                assert_eq!(received_b, Some(()));
                assert!(matches!(&extra_a, Some(Ok(()))));
                assert!(incremented.is_ok());
                assert_eq!(saved_reply.as_ref(), Some(&actual_reply_recipient));
                assert_ne!(actual_reply_recipient, other_reply_recipient);
                assert!(matches!(&first.4, Admission::Returned(Ok(()))));
                assert!(matches!(&second.4, Admission::Returned(Ok(()))));
                let Some(Some(first_user)) = &first.5 else {
                    panic!("A original User acquired");
                };
                let Some(Some(second_user)) = &second.5 else {
                    panic!("B native User acquired");
                };
                assert_eq!(first_user.from, target_origin);
                assert_eq!(first_user.message, 1);
                assert_eq!(second_user.from, target_origin);
                for (row, original) in [(first, allocations[0]), (second, allocations[1])] {
                    assert_eq!((row.1.as_ptr(), row.2.as_ptr()), original);
                }
                assert_ne!(recorded::identity(&first.3), recorded::identity(&second.3));
                for row in &requests {
                    for original in &row.7 {
                        assert_eq!(original.from, target_origin);
                    }
                }
                let drain_values = requests
                    .iter()
                    .flat_map(|row| row.7.iter())
                    .map(|original| original.message)
                    .collect::<Vec<_>>();
                let fence_user =
                    fence_user.expect("the independent FIFO fence acquired its actual User");
                assert!(fence_sent.is_ok());
                assert_eq!(fence_user.from, target_origin);
                assert_eq!(fence_user.message, 2);
                assert_eq!(fence_drain.len(), 0);
                let [late_a] = first.7.as_slice() else {
                    panic!("A retains exactly its extra original late reply");
                };
                let late_b = second.7.as_slice();
                assert_eq!(late_a.from, target_origin);
                assert_eq!(late_a.message, 1);
                assert_eq!(late_b, []);
                let actual_b = second_user.message;
                assert_eq!(
                    actual_b, 2,
                    "B must acquire B's value; complete original native drain={drain_values:?}"
                );
            });
    }
}
mod ledger {
    //! Each new Ledger request owns its client even after the actor has joined retirement.
    use super::super::{
        Binding, EndpointRole, LedgerCommand, LedgerOperation, MailLedger, Receipts,
    };
    use super::super::{
        ProtectedMessage, ProviderProfile, Purpose, ReadRequestRef, VerificationError,
    };
    use super::super::{
        assert_completed, assert_notifications, configuration, current_scope, metadata, recorded,
        reserved_address,
    };
    use bombay::actors::ActorExt;
    use bombay::behavior::BehaviorBase;
    use bombay::prelude::Application;
    use bombay::remote::{Admission, ExportLimits, IngressError, verify};
    use bombay::{ActorFailureAssessment, RetirementAssessment, TrySendError};
    use core::future::{Future, poll_fn};
    use core::pin::pin;
    use core::task::Poll;
    use std::num::NonZeroUsize;
    use std::sync::{Arc, Mutex};
    use std::time::{Duration, Instant};
    use tokio::runtime::Builder;
    use tokio::sync::mpsc;
    use zenoh::query::Query;

    #[test]
    #[expect(
        clippy::too_many_lines,
        reason = "one terminating Ledger trace preserves exact command allocations and independent clients through cleanup"
    )]
    fn public_ledger_retains_a_closed_request_with_its_own_native_client() {
        Builder::new_multi_thread()
            .worker_threads(1)
            .enable_all()
            .build()
            .unwrap()
            .block_on(async {
                let address = reserved_address();
                let refusals = Arc::new(Mutex::new(Vec::<IngressError<Query>>::new()));
                let observed = Arc::clone(&refusals);
                let mut node = Application::new(
                    MailLedger {
                        entries: Vec::new(),
                    }
                    .stop_on_shutdown(),
                )
                .open_node(configuration(&address, &EndpointRole::Receiver))
                .await
                .unwrap_or_else(|_| panic!("actual configured Ledger Node"));
                let connection = node.session.clone();
                let session = &connection;
                let (execution, receiving) = node
                    .execute_with(|application| async move {
                        let target_origin = application.root().address();
                        let mut export = application
                            .export::<_, Receipts, recorded::Receipt<LedgerOperation>>(
                                application.root().established_recipient(),
                                "bombay/isolation/ledger",
                                ExportLimits {
                                    queued_queries: NonZeroUsize::new(2).unwrap(),
                                    retained_queries: NonZeroUsize::new(2).unwrap(),
                                    retained_requests: NonZeroUsize::new(2).unwrap(),
                                },
                                |a, b| recorded::identity(a) == recorded::identity(b),
                                move |original| observed.lock().unwrap().push(original),
                            )
                            .await
                            .unwrap_or_else(|_| panic!("actual public Ledger export"));
                        let sender = application.sender().unwrap();
                        let caller = sender.address();
                        let append = Box::<[u8]>::from([59, 61]);
                        let append_allocation = append.as_ptr();
                        let appended = sender.send(LedgerCommand::Append(append)).await;
                        let binding = Binding {
                            deployment: "two-node-read-study",
                            caller: "caller",
                            controller: "owner",
                            actor: "ledger",
                            host: "receiver",
                            protocol: "ledger",
                            target_assignment: "closed-ledger-eligibility",
                            logical_session: 41,
                        };
                        let current_a = metadata(
                            ProviderProfile::Recorded,
                            "ledger",
                            session.zid().to_string(),
                            session.zid().to_string(),
                        );
                        let mut current_b = metadata(
                            ProviderProfile::Recorded,
                            "ledger",
                            session.zid().to_string(),
                            session.zid().to_string(),
                        );
                        current_b.request_sequence =
                            current_a.request_sequence.checked_add(1).unwrap();
                        let bytes_a = serde_json::to_vec(&ReadRequestRef {
                            bombay: &current_a,
                            kind: Purpose::Request,
                            operation: LedgerOperation::Read,
                        })
                        .unwrap();
                        let bytes_b = serde_json::to_vec(&ReadRequestRef {
                            bombay: &current_b,
                            kind: Purpose::Request,
                            operation: LedgerOperation::Read,
                        })
                        .unwrap();
                        let records = [
                            ProtectedMessage {
                                protected: bytes_a.clone().into_boxed_slice(),
                                proof: Box::from([7, 11]),
                            },
                            ProtectedMessage {
                                protected: bytes_b.clone().into_boxed_slice(),
                                proof: Box::from([7, 11]),
                            },
                        ];
                        let original_a = ProtectedMessage {
                            protected: bytes_a.into_boxed_slice(),
                            proof: Box::from([7, 11]),
                        };
                        let original_b = ProtectedMessage {
                            protected: bytes_b.into_boxed_slice(),
                            proof: Box::from([7, 11]),
                        };
                        let allocations = [
                            (original_a.protected.as_ptr(), original_a.proof.as_ptr()),
                            (original_b.protected.as_ptr(), original_b.proof.as_ptr()),
                        ];
                        let mut verified_a = None;
                        let mut verified_b = None;
                        verify(&original_a, &mut verified_a, |r| {
                            recorded::verify::<LedgerOperation>(r, &records, binding)
                        })
                        .await;
                        verify(&original_b, &mut verified_b, |r| {
                            recorded::verify::<LedgerOperation>(r, &records, binding)
                        })
                        .await;
                        let receipt_a = verified_a.take().unwrap().unwrap();
                        let receipt_b = verified_b.take().unwrap().unwrap();
                        let retained_a =
                            export.retain(original_a.protected, original_a.proof, receipt_a);
                        let deadline = Instant::now() + Duration::from_secs(30);
                        let first = if let Ok((a, None)) = &retained_a {
                            let admission = export
                                .attempt(
                                    a,
                                    |receipt| {
                                        current_scope(
                                            recorded::projection(receipt),
                                            &current_a,
                                            &LedgerOperation::Read,
                                            Purpose::Request,
                                            deadline,
                                        )?;
                                        Ok::<_, VerificationError>(&current_a)
                                    },
                                    LedgerCommand::Read,
                                )
                                .map(|_| ());
                            let received = if matches!(
                                export.request(a),
                                Some((_, _, _, Admission::Returned(Ok(())), _))
                            ) {
                                Some(export.receive_native(a).await)
                            } else {
                                None
                            };
                            Some((admission, received))
                        } else {
                            None
                        };
                        let fallback_shutdown = match &retained_a {
                            Ok((key, _))
                                if matches!(
                                    export.request(key),
                                    Some((_, _, _, _, Some(Some(_))))
                                ) =>
                            {
                                None
                            }
                            Ok(_) | Err(_) => Some(application.request_shutdown()),
                        };
                        let report = application.lifecycle().retirement().await;
                        // Actual joined retirement precedes B's lazy reply-endpoint allocation.
                        let retained_b =
                            export.retain(original_b.protected, original_b.proof, receipt_b);
                        let second = if let Ok((b, None)) = &retained_b {
                            let admission = export
                                .attempt(
                                    b,
                                    |receipt| {
                                        current_scope(
                                            recorded::projection(receipt),
                                            &current_b,
                                            &LedgerOperation::Read,
                                            Purpose::Request,
                                            deadline,
                                        )?;
                                        Ok::<_, VerificationError>(&current_b)
                                    },
                                    LedgerCommand::Read,
                                )
                                .map(|_| ());
                            let received = {
                                let mut receiving = pin!(export.receive_native(b));
                                poll_fn(|cx| Poll::Ready(receiving.as_mut().poll(cx))).await
                            };
                            Some((admission, received))
                        } else {
                            None
                        };
                        (
                            export,
                            retained_a,
                            retained_b,
                            first,
                            second,
                            appended,
                            report,
                            caller,
                            append_allocation,
                            target_origin,
                            allocations,
                            fallback_shutdown,
                        )
                    })
                    .unwrap_or_else(|_| panic!("actual Ledger host enters"));
                let ((), outcome) = tokio::join!(execution, receiving);
                let (outcome, native, notifications, ()) = outcome.into_parts();
                let (
                    (
                        export,
                        retained_a,
                        retained_b,
                        first,
                        second,
                        appended,
                        report,
                        caller,
                        append_allocation,
                        target_origin,
                        allocations,
                        fallback_shutdown,
                    ),
                    cleanup,
                ) = outcome
                    .try_into_completed()
                    .unwrap_or_else(|_| panic!("original Ledger Work acquired"));
                let (undeclared, session, mut incoming, requests) = export.undeclare().await;
                let mut rows = Vec::new();
                for (key, protected, proof, receipt, admission, native, mut client) in requests {
                    client.close_admission();
                    let mut tail = Vec::new();
                    while let Some(original) = client.receive().await {
                        tail.push(original);
                    }
                    rows.push((
                        key, protected, proof, receipt, admission, native, client, tail,
                    ));
                }
                let mut query_drain = Vec::new();
                let query_end = loop {
                    match incoming.try_recv() {
                        Ok(original) => query_drain.push(original),
                        Err(cause) => break cause,
                    }
                };
                drop(session);
                let closed = node.close().await;
                assert!(connection.is_closed());
                let native = native.expect("complete Ledger native tree");
                assert_notifications(&notifications);
                assert!(
                    cleanup.is_ok() && undeclared.is_ok() && closed.is_ok() && appended.is_ok()
                );
                assert!(fallback_shutdown.is_none());
                assert_eq!(report.retirement(), RetirementAssessment::Established);
                assert_eq!(report.failures(), ActorFailureAssessment::NoFailuresFound);
                assert!(query_drain.is_empty() && refusals.lock().unwrap().is_empty());
                assert!(matches!(query_end, mpsc::error::TryRecvError::Disconnected));
                let ledger = native.1.custody().unwrap().behavior.base();
                let [(from, append)] = ledger.entries.as_slice() else {
                    panic!("actual retained Append");
                };
                assert_eq!(*from, caller);
                assert_eq!(append.as_ptr(), append_allocation);
                assert_eq!(append.as_ref(), [59, 61]);
                let (Ok((a, None)), Ok((b, None))) = (&retained_a, &retained_b) else {
                    panic!("two retained original requests");
                };
                let a = rows.iter().find(|row| &row.0 == a).unwrap();
                let b = rows.iter().find(|row| &row.0 == b).unwrap();
                let reply_a = a.6.recipient();
                let reply_b = b.6.recipient();
                assert_ne!(reply_a, reply_b);
                assert!(matches!(&first, Some((Ok(()), Some(Some(()))))));
                assert!(matches!(&second, Some((Ok(()), Poll::Ready(Some(()))))));
                assert!(matches!(&a.4, Admission::Returned(Ok(()))));
                let Some(Some(user)) = &a.5 else {
                    panic!("original first Ledger User");
                };
                assert_eq!(user.from, target_origin);
                assert_eq!(user.message.as_slice(), [caller]);
                let Admission::Returned(Err(TrySendError::Closed(LedgerCommand::Read(original)))) =
                    &b.4
                else {
                    panic!("actual Closed Read retains B's own reply capability");
                };
                assert_eq!(original, &reply_b);
                assert!(b.5.is_none());
                for (row, allocation) in [(a, allocations[0]), (b, allocations[1])] {
                    assert_eq!((row.1.as_ptr(), row.2.as_ptr()), allocation);
                    assert_eq!(row.7.len(), 0);
                }
                assert_completed(native, None);
            });
    }
}
