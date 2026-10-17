//! Exact child capability isolation across two actual Applications.
//! Client association is selected; authentication and network publication remain open.
use super::counter_children_application;
use super::{
    Counter, CounterChildren, CounterExports, CounterMessage, CounterParent, CounterStopExport,
    CounterValue, listening_configuration,
};
use bombay::actors::ActorExt;
use bombay::behavior::{ClassifySettlement, EstablishedRecipient, SettlementStatus};
use bombay::prelude::Completion;
use bombay::remote::ExportError;
use bombay::{ActorRetirement, ApplicationScope, Hosts, WorkOutcome};
use tokio::runtime::Builder;
use tokio::sync::oneshot;

bombay::topology! {
    hosted_children {
        root: CounterParent { Counter => counter; }
        counter in counters {}
        hosts { counters: Counter; }
    }
}

#[test]
fn same_address_foreign_child_keeps_its_owning_application() {
    let host = Builder::new_multi_thread()
        .worker_threads(1)
        .enable_all()
        .build()
        .unwrap();
    host.block_on(async {
        // Pre-establish equal real reply owners; no Work allocates another port.
        let scope_a = ApplicationScope::new();
        let mut exports_a = scope_a.external::<CounterExports>().unwrap();
        let mut values_a = scope_a.external::<CounterValue>().unwrap();
        let topology_a = hosted_children::Topology::new();
        let hosted_counter = Hosts::<Counter>::space(&topology_a).clone();
        let mut node = scope_a
            .with_topology(CounterParent::default().stop_on_shutdown(), topology_a)
            .open_node(listening_configuration())
            .await
            .unwrap_or_else(|_| panic!("actual single Node opens"));
        let scope_b = ApplicationScope::new();
        let mut exports_b = scope_b.external::<CounterExports>().unwrap();
        let mut values_b = scope_b.external::<CounterValue>().unwrap();
        let application_b = scope_b.with_topology(
            CounterParent::default().stop_on_shutdown(),
            hosted_children::Topology::new(),
        );
        let (offered, foreign) = oneshot::channel();
        let (advance, permission) = oneshot::channel::<()>();
        let (execution_b, receiving_b) = application_b
            .execute_with(move |application| async move {
                let export = exports_b
                    .send(
                        &application.root().established_recipient(),
                        exports_b.recipient(),
                    )
                    .await;
                let published = exports_b.receive().await;
                let CounterChildren { target, sibling } =
                    &published.as_ref().expect("actual B export").message;
                let root_address = application.root().address();
                let sibling_address = sibling
                    .recipient()
                    .interpret(&mut CounterStopExport::default());
                let recipient = target.recipient();
                let address = recipient
                    .clone()
                    .interpret(&mut CounterStopExport::default());
                let first = values_b.send(&recipient, CounterMessage::Increment).await;
                let second = values_b.send(&recipient, CounterMessage::Increment).await;
                let fence = values_b
                    .send(&recipient, CounterMessage::Read(values_b.recipient()))
                    .await;
                let prepared = if fence.is_ok() {
                    values_b.receive().await
                } else {
                    None
                };
                let offer = offered.send((recipient.clone(), address));
                let permitted = permission.await;
                let read = values_b
                    .send(&recipient, CounterMessage::Read(values_b.recipient()))
                    .await;
                let audited = if read.is_ok() {
                    values_b.receive().await
                } else {
                    None
                };
                let stopped = application.shutdown();
                (
                    export,
                    published,
                    root_address,
                    address,
                    sibling_address,
                    first,
                    second,
                    fence,
                    prepared,
                    offer,
                    permitted,
                    read,
                    audited,
                    stopped,
                    exports_b,
                    values_b,
                )
            })
            .unwrap_or_else(|_| panic!("actual ordinary B execution"));
        let (output_a, cleanup_a, native_a, notifications_a, received_b) = {
            let (execution_a, receiving_a) = node
                .execute_with(move |application| async move {
                    let export = exports_a
                        .send(
                            &application.root().established_recipient(),
                            exports_a.recipient(),
                        )
                        .await;
                    let published = exports_a.receive().await;
                    let CounterChildren { target, sibling } =
                        &published.as_ref().expect("actual A export").message;
                    let root_address = application.root().address();
                    let sibling_address = sibling
                        .recipient()
                        .interpret(&mut CounterStopExport::default());
                    let recipient = target.recipient();
                    let address = recipient
                        .clone()
                        .interpret(&mut CounterStopExport::default());
                    // This is the exact table moved into A's actual Application.
                    // Check hosting when association is formed, not after retirement.
                    let associate = |candidate: EstablishedRecipient<Counter>| match hosted_counter
                        .resolve(&address)
                    {
                        Some(registered) if registered.established_recipient() == candidate => {
                            Ok(candidate)
                        }
                        Some(_) | None => Err(candidate),
                    };
                    let offered = foreign.await;
                    let (foreign_recipient, foreign_address) =
                        offered.as_ref().expect("actual B capability");
                    let challenged = associate(foreign_recipient.clone());
                    let attempted = match &challenged {
                        Ok(target) => Some(values_a.try_send(target, CounterMessage::Increment)),
                        Err(_) => None,
                    };
                    let associated = associate(recipient.clone());
                    let control = match &associated {
                        Ok(target) => Some(values_a.try_send(target, CounterMessage::Increment)),
                        Err(_) => None,
                    };
                    let read = values_a
                        .send(&recipient, CounterMessage::Read(values_a.recipient()))
                        .await;
                    let audited = if read.is_ok() {
                        values_a.receive().await
                    } else {
                        None
                    };
                    let foreign_address = *foreign_address;
                    let advanced = advance.send(());
                    let stopped = application.shutdown();
                    (
                        export,
                        published,
                        root_address,
                        address,
                        sibling_address,
                        offered,
                        foreign_address,
                        challenged,
                        attempted,
                        associated,
                        control,
                        read,
                        audited,
                        advanced,
                        stopped,
                        exports_a,
                        values_a,
                    )
                })
                .unwrap_or_else(|_| panic!("actual Node A execution"));
            tokio::join!(execution_a, execution_b);
            let (outcome_a, _root_report, native_a, notifications_a, ()) = receiving_a.await.into_parts();
            let WorkOutcome::Completed { output, cleanup } = outcome_a else {
                panic!("actual A Work output");
            };
            (
                output,
                cleanup,
                native_a,
                notifications_a,
                receiving_b.await.into_parts(),
            )
        };
        let closed = node.close().await;
        let (
            export_a,
            published_a,
            root_address_a,
            address_a,
            sibling_address_a,
            offered_b,
            foreign_address,
            challenged,
            attempted,
            associated,
            control,
            read_a,
            audited_a,
            advanced,
            stopped_a,
            exports_a,
            values_a,
        ) = output_a;
        let (outcome_b, _root_report, native_b, notifications_b, ()) = received_b;
        let WorkOutcome::Completed {
            output,
            cleanup: cleanup_b,
        } = outcome_b
        else {
            panic!("actual B Work output");
        };
        let (
            export_b,
            published_b,
            root_address_b,
            address_b,
            sibling_address_b,
            first,
            second,
            fence,
            prepared,
            offer,
            permitted,
            read_b,
            audited_b,
            stopped_b,
            exports_b,
            values_b,
        ) = output;
        let native_a = native_a.expect("original A root and complete descendants");
        let native_b = native_b.expect("original B root and complete descendants");
        let notifications_a = notifications_a.expect("actual A notification pair");
        let notifications_b = notifications_b.expect("actual B notification pair");
        assert!(closed.is_ok() && cleanup_a.is_ok() && cleanup_b.is_ok());
        assert!(export_a.is_ok() && export_b.is_ok());
        assert!(first.is_ok() && second.is_ok() && fence.is_ok());
        assert_eq!(
            prepared.as_ref().map(|reply| (reply.from, reply.message)),
            Some((address_b, 2))
        );
        assert!(offer.is_ok() && offered_b.is_ok() && permitted.is_ok() && advanced.is_ok());
        assert!(read_a.is_ok() && read_b.is_ok() && stopped_a.is_ok() && stopped_b.is_ok());
        assert!(
            matches!(&notifications_a.termination, Ok(()))
                && matches!(&notifications_a.retirement, Ok(()))
        );
        assert!(
            matches!(&notifications_b.termination, Ok(()))
                && matches!(&notifications_b.retirement, Ok(()))
        );
        assert_eq!(address_a, address_b);
        assert_eq!(foreign_address, address_b);
        assert_eq!(
            published_a.as_ref().map(|reply| reply.from),
            Some(root_address_a)
        );
        assert_eq!(
            published_b.as_ref().map(|reply| reply.from),
            Some(root_address_b)
        );
        // Independent native effects FIRST: the omission cut must expose B=3.
        assert_eq!(
            audited_a.as_ref().map(|reply| (reply.from, reply.message)),
            Some((address_a, 1))
        );
        assert_eq!(
            audited_b.as_ref().map(|reply| (reply.from, reply.message)),
            Some((address_b, 2))
        );
        let offered_recipient = offered_b.as_ref().ok().map(|(recipient, _)| recipient);
        assert_eq!(challenged.as_ref().err(), offered_recipient);
        assert!(challenged.is_err() && attempted.is_none());
        assert!(associated.is_ok() && matches!(&control, Some(Ok(()))));
        assert_ne!(challenged.as_ref().err(), associated.as_ref().ok());
        for ((origin, retirement), root_address, expected_children) in [
            (native_a, root_address_a, [address_a, sibling_address_a]),
            (native_b, root_address_b, [address_b, sibling_address_b]),
        ] {
            assert_eq!(origin.address(), root_address);
            let ActorRetirement::Completed {
                interpretation,
                source,
                settlements,
                ..
            } = &retirement
            else {
                panic!("actual parent must retire completely");
            };
            assert!(interpretation.is_none() && source.is_none());
            for settlement in settlements {
                assert_eq!(settlement.settlement_status(), SettlementStatus::Accepted);
            }
            assert_eq!(retirement.completion(), Some(&Completion::Stopped));
            let custody = retirement.custody().expect("original parent custody");
            assert!(custody.child_failures.is_empty() && custody.capability_failures.is_empty());
            assert!(custody.control.is_empty() && custody.user.is_empty());
            assert!(
                custody.unread_owner_cancellation.is_none()
                    && custody.additional_failures.is_empty()
            );
            assert!(custody.received_interpretation.is_none() && custody.received_source.is_none());
            assert!(custody.source_index.is_none() && custody.acquired_ingress.is_none());
            assert!(custody.retirement_failures.is_empty() && custody.terminal_report.is_none());
            let mut observed = custody
                .descendants
                .iter()
                .map(|child| child.origin.address())
                .collect::<Vec<_>>();
            let mut expected = expected_children.to_vec();
            observed.sort();
            expected.sort();
            assert_ne!(expected_children[0], expected_children[1]);
            assert_eq!(observed, expected);
            for child in &custody.descendants {
                let ActorRetirement::OwnerCancelled {
                    interpretation,
                    source,
                    settlements,
                    ..
                } = child.retirement.as_ref()
                else {
                    panic!("actual child must retain its joined owner retirement");
                };
                assert!(interpretation.is_none() && source.is_none());
                for settlement in settlements {
                    assert_eq!(settlement.settlement_status(), SettlementStatus::Accepted);
                }
                assert!(child.retirement.completion().is_none());
                let custody = child.retirement.custody().expect("original child custody");
                assert!(custody.descendants.is_empty() && custody.child_failures.is_empty());
                assert!(
                    custody.capability_failures.is_empty()
                        && custody.additional_failures.is_empty()
                );
                assert!(custody.control.is_empty() && custody.user.is_empty());
                assert!(custody.unread_owner_cancellation.is_none());
                assert!(
                    custody.received_interpretation.is_none() && custody.received_source.is_none()
                );
                assert!(custody.source_index.is_none() && custody.acquired_ingress.is_none());
                assert!(
                    custody.retirement_failures.is_empty() && custody.terminal_report.is_none()
                );
            }
            drop((origin, retirement));
        }
        drop((
            notifications_a,
            notifications_b,
            published_a,
            published_b,
            offered_b,
            challenged,
            attempted,
            associated,
            control,
            exports_a,
            values_a,
            exports_b,
            values_b,
        ));
    });
}

#[test]
fn unhosted_export_clients_preserve_exact_application_ownership() {
    let host = Builder::new_multi_thread()
        .worker_threads(1)
        .enable_all()
        .build()
        .unwrap();
    host.block_on(async {
        // Establish matching reply owners before either Application starts.
        let scope_a = ApplicationScope::new();
        let mut exports_a = scope_a.external::<CounterExports>().unwrap();
        let mut values_a = scope_a.external::<CounterValue>().unwrap();
        let topology_a = counter_children_application::Topology::new();
        let mut node = scope_a
            .with_topology(CounterParent::default().stop_on_shutdown(), topology_a)
            .open_node(listening_configuration())
            .await
            .unwrap_or_else(|_| panic!("actual single Node opens"));
        let scope_b = ApplicationScope::new();
        let mut exports_b = scope_b.external::<CounterExports>().unwrap();
        let mut values_b = scope_b.external::<CounterValue>().unwrap();
        let application_b = scope_b.with_topology(
            CounterParent::default().stop_on_shutdown(),
            counter_children_application::Topology::new(),
        );
        let (offered, foreign) = oneshot::channel();
        let (advance, permission) = oneshot::channel::<()>();
        let (execution_b, receiving_b) = application_b
            .execute_with(move |application| async move {
                let export = exports_b
                    .send(
                        &application.root().established_recipient(),
                        exports_b.recipient(),
                    )
                    .await;
                let published = exports_b.receive().await;
                let CounterChildren { target, sibling } =
                    &published.as_ref().expect("actual B export").message;
                let root_address = application.root().address();
                let sibling_address = sibling
                    .recipient()
                    .interpret(&mut CounterStopExport::default());
                let recipient = target.recipient();
                let address = recipient
                    .clone()
                    .interpret(&mut CounterStopExport::default());
                let first = values_b.send(&recipient, CounterMessage::Increment).await;
                let second = values_b.send(&recipient, CounterMessage::Increment).await;
                let fence = values_b
                    .send(&recipient, CounterMessage::Read(values_b.recipient()))
                    .await;
                let prepared = if fence.is_ok() {
                    values_b.receive().await
                } else {
                    None
                };
                let offer = offered.send((recipient.clone(), address));
                let permitted = permission.await;
                let read = values_b
                    .send(&recipient, CounterMessage::Read(values_b.recipient()))
                    .await;
                let audited = if read.is_ok() {
                    values_b.receive().await
                } else {
                    None
                };
                let stopped = application.shutdown();
                (
                    export,
                    published,
                    root_address,
                    address,
                    sibling_address,
                    first,
                    second,
                    fence,
                    prepared,
                    offer,
                    permitted,
                    read,
                    audited,
                    stopped,
                    exports_b,
                    values_b,
                )
            })
            .unwrap_or_else(|_| panic!("actual ordinary B execution"));
        let (output_a, cleanup_a, native_a, notifications_a, received_b) = {
            let (execution_a, receiving_a) = node
                .execute_with(move |application| async move {
                    let export = exports_a
                        .send(
                            &application.root().established_recipient(),
                            exports_a.recipient(),
                        )
                        .await;
                    let published = exports_a.receive().await;
                    let CounterChildren { target, sibling } =
                        &published.as_ref().expect("actual A export").message;
                    let root_address = application.root().address();
                    let sibling_address = sibling
                        .recipient()
                        .interpret(&mut CounterStopExport::default());
                    let recipient = target.recipient();
                    let address = recipient
                        .clone()
                        .interpret(&mut CounterStopExport::default());
                    let offered = foreign.await;
                    let (foreign_recipient, foreign_address) =
                        offered.as_ref().expect("actual B capability");
                    let before_refusals = application.external::<CounterValue>().unwrap();
                    let challenged = application.export_client::<_, CounterValue>(foreign_recipient.clone());
                    let after_foreign = application.external::<CounterValue>().unwrap();
                    let external = application.external::<Counter>().unwrap();
                    let external_recipient = external.recipient();
                    let challenged_external = application.export_client::<_, CounterValue>(external_recipient.clone());
                    let after_external = application.external::<CounterValue>().unwrap();
                    let allocation_observation = (before_refusals.address(), after_foreign.address(), external.address(), after_external.address());
                    let attempted = match &challenged {
                        Ok(client) => Some(client.send(CounterMessage::Increment).await),
                        Err(_) => None,
                    };
                    let associated = application.export_client::<_, CounterValue>(recipient.clone());
                    let control = match &associated {
                        Ok(client) => Some(client.send(CounterMessage::Increment).await),
                        Err(_) => None,
                    };
                    let read = values_a
                        .send(&recipient, CounterMessage::Read(values_a.recipient()))
                        .await;
                    let audited = if read.is_ok() {
                        values_a.receive().await
                    } else {
                        None
                    };
                    let foreign_address = *foreign_address;
                    let advanced = advance.send(());
                    let stopped = application.shutdown();
                    (
                        export,
                        published,
                        root_address,
                        address,
                        sibling_address,
                        offered,
                        foreign_address,
                        challenged,
                        attempted,
                        associated,
                        control,
                        read,
                        audited,
                        (challenged_external, external_recipient, allocation_observation, external, before_refusals, after_foreign, after_external),
                        advanced,
                        stopped,
                        exports_a,
                        values_a,
                    )
                })
                .unwrap_or_else(|_| panic!("actual Node A execution"));
            tokio::join!(execution_a, execution_b);
            let (outcome_a, _root_report, native_a, notifications_a, ()) = receiving_a.await.into_parts();
            let WorkOutcome::Completed { output, cleanup } = outcome_a else {
                panic!("actual A Work output");
            };
            (
                output,
                cleanup,
                native_a,
                notifications_a,
                receiving_b.await.into_parts(),
            )
        };
        let closed = node.close().await;
        let (
            export_a,
            published_a,
            root_address_a,
            address_a,
            sibling_address_a,
            offered_b,
            foreign_address,
            challenged,
            attempted,
            associated,
            control,
            read_a,
            audited_a,
            (challenged_external, external_recipient, allocation_observation, external, before_refusals, after_foreign, after_external),
            advanced,
            stopped_a,
            exports_a,
            values_a,
        ) = output_a;
        let (outcome_b, _root_report, native_b, notifications_b, ()) = received_b;
        let WorkOutcome::Completed {
            output,
            cleanup: cleanup_b,
        } = outcome_b
        else {
            panic!("actual B Work output");
        };
        let (
            export_b,
            published_b,
            root_address_b,
            address_b,
            sibling_address_b,
            first,
            second,
            fence,
            prepared,
            offer,
            permitted,
            read_b,
            audited_b,
            stopped_b,
            exports_b,
            values_b,
        ) = output;
        let native_a = native_a.expect("original A root and complete descendants");
        let native_b = native_b.expect("original B root and complete descendants");
        let notifications_a = notifications_a.expect("actual A notification pair");
        let notifications_b = notifications_b.expect("actual B notification pair");
        assert!(closed.is_ok() && cleanup_a.is_ok() && cleanup_b.is_ok());
        assert!(export_a.is_ok() && export_b.is_ok());
        assert!(first.is_ok() && second.is_ok() && fence.is_ok());
        assert_eq!(
            prepared.as_ref().map(|reply| (reply.from, reply.message)),
            Some((address_b, 2))
        );
        assert!(offer.is_ok() && offered_b.is_ok() && permitted.is_ok() && advanced.is_ok());
        assert!(read_a.is_ok() && read_b.is_ok() && stopped_a.is_ok() && stopped_b.is_ok());
        assert!(
            matches!(&notifications_a.termination, Ok(()))
                && matches!(&notifications_a.retirement, Ok(()))
        );
        assert!(
            matches!(&notifications_b.termination, Ok(()))
                && matches!(&notifications_b.retirement, Ok(()))
        );
        assert_eq!(address_a, address_b);
        assert_eq!(foreign_address, address_b);
        assert_eq!(
            published_a.as_ref().map(|reply| reply.from),
            Some(root_address_a)
        );
        assert_eq!(
            published_b.as_ref().map(|reply| reply.from),
            Some(root_address_b)
        );
        for ((origin, retirement), root_address, expected_children) in [
            (native_a, root_address_a, [address_a, sibling_address_a]),
            (native_b, root_address_b, [address_b, sibling_address_b]),
        ] {
            assert_eq!(origin.address(), root_address);
            let ActorRetirement::Completed {
                interpretation,
                source,
                settlements,
                ..
            } = &retirement
            else {
                panic!("actual parent must retire completely");
            };
            assert!(interpretation.is_none() && source.is_none());
            for settlement in settlements {
                assert_eq!(settlement.settlement_status(), SettlementStatus::Accepted);
            }
            assert_eq!(retirement.completion(), Some(&Completion::Stopped));
            let custody = retirement.custody().expect("original parent custody");
            assert!(custody.child_failures.is_empty() && custody.capability_failures.is_empty());
            assert!(custody.control.is_empty() && custody.user.is_empty());
            assert!(
                custody.unread_owner_cancellation.is_none()
                    && custody.additional_failures.is_empty()
            );
            assert!(custody.received_interpretation.is_none() && custody.received_source.is_none());
            assert!(custody.source_index.is_none() && custody.acquired_ingress.is_none());
            assert!(custody.retirement_failures.is_empty() && custody.terminal_report.is_none());
            let mut observed = custody
                .descendants
                .iter()
                .map(|child| child.origin.address())
                .collect::<Vec<_>>();
            let mut expected = expected_children.to_vec();
            observed.sort();
            expected.sort();
            assert_ne!(expected_children[0], expected_children[1]);
            assert_eq!(observed, expected);
            for child in &custody.descendants {
                let ActorRetirement::OwnerCancelled {
                    interpretation,
                    source,
                    settlements,
                    ..
                } = child.retirement.as_ref()
                else {
                    panic!("actual child must retain its joined owner retirement");
                };
                assert!(interpretation.is_none() && source.is_none());
                for settlement in settlements {
                    assert_eq!(settlement.settlement_status(), SettlementStatus::Accepted);
                }
                assert!(child.retirement.completion().is_none());
                let custody = child.retirement.custody().expect("original child custody");
                assert!(custody.descendants.is_empty() && custody.child_failures.is_empty());
                assert!(
                    custody.capability_failures.is_empty()
                        && custody.additional_failures.is_empty()
                );
                assert!(custody.control.is_empty() && custody.user.is_empty());
                assert!(custody.unread_owner_cancellation.is_none());
                assert!(
                    custody.received_interpretation.is_none() && custody.received_source.is_none()
                );
                assert!(custody.source_index.is_none() && custody.acquired_ingress.is_none());
                assert!(
                    custody.retirement_failures.is_empty() && custody.terminal_report.is_none()
                );
            }
            drop((origin, retirement));
        }
        // Independent native effects FIRST: the omission cut must expose B=3.
        assert_eq!(
            audited_a.as_ref().map(|reply| (reply.from, reply.message)),
            Some((address_a, 1))
        );
        assert_eq!(
            audited_b.as_ref().map(|reply| (reply.from, reply.message)),
            Some((address_b, 2))
        );
        let offered_recipient = offered_b.as_ref().ok().map(|(recipient, _)| recipient);
        let refused_recipient = challenged.as_ref().err().map(|(target, _)| target);
        let associated_recipient = published_a.as_ref().map(|reply| reply.message.target.recipient());
        assert_eq!(refused_recipient, offered_recipient);
        assert!(matches!(&challenged, Err((_, ExportError::UnownedTarget))));
        assert!(matches!(&challenged_external, Err((target, ExportError::UnownedTarget)) if target == &external_recipient));
        let (before_refusals_address, after_foreign_address, external_address, after_external_address) = allocation_observation;
        assert_eq!(after_foreign_address.0, before_refusals_address.0 + 1);
        assert_eq!(after_external_address.0, external_address.0 + 1);
        assert!(challenged.is_err() && attempted.is_none());
        assert!(associated.is_ok() && matches!(&control, Some(Ok(()))));
        assert_ne!(refused_recipient, associated_recipient.as_ref());
        drop((
            notifications_a,
            notifications_b,
            published_a,
            published_b,
            offered_b,
            challenged,
            attempted,
            associated,
            control,
            exports_a,
            values_a,
            challenged_external,
            external_recipient,
            external,
            before_refusals,
            after_foreign,
            after_external,
            exports_b,
            values_b,
        ));
    });
}
