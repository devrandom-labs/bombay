//! Native configuration and private Application connection custody; no service-readiness proof.
use super::{Connection, ConnectionEndpoints, CredentialSource, Network, NetworkLayout};
use crate::actors::ActorExt;
use crate::application::{Application, ApplicationOutcome};
use crate::behavior::{ClassifySettlement, Never, SettlementStatus};
use crate::remote::startup::NodeStartupRefusal;
use crate::{
    ActorCustody, ActorFailureAssessment, ActorRetirement, Completion, MailAddr,
    RetirementAssessment,
};
use behavior_actors::StopOnShutdown;
use core::future::{Future, poll_fn};
use core::pin::pin;
use serde_json::Value;
use std::cell::Cell;
use std::error::Error;
#[cfg(unix)]
use std::ffi::OsString;
use std::num::NonZeroUsize;
#[cfg(unix)]
use std::os::unix::ffi::OsStringExt;
use std::path::PathBuf;
use std::ptr;
use std::rc::Rc;
use std::task::{Context, Poll, Waker};
use tokio::runtime::Builder;
use tokio::sync::{Semaphore, oneshot};
use zenoh_config::{EndPoints, ModeDependent, WhatAmI};
use zenoh_protocol::core::LocatorsStrategy;

#[path = "../../../../../examples/counter/src/counter.rs"]
pub(crate) mod counter;
use counter::{Counter, CounterMessage, CounterValue};

fn connecting() -> ConnectionEndpoints {
    ConnectionEndpoints {
        addresses: vec!["router.example.test:7447".into(), "[::1]:7448".into()],
        certificate: CredentialSource::File("unopened-connect-certificate.pem".into()),
        private_key: CredentialSource::File("unopened-connect-key.pem".into()),
    }
}

fn listening() -> ConnectionEndpoints {
    ConnectionEndpoints {
        addresses: vec!["[::]:7449".into()],
        certificate: CredentialSource::File("unopened-listen-certificate.pem".into()),
        private_key: CredentialSource::File("unopened-listen-key.pem".into()),
    }
}

fn peer() -> Network {
    Network {
        layout: NetworkLayout::Peer {
            connect: Some(connecting()),
            listen: Some(listening()),
        },
        deployment_roots: CredentialSource::File("unopened-deployment-roots.pem".into()),
        max_retained_queries: NonZeroUsize::new(7).unwrap(),
    }
}

// Observe every original address/path/secret allocation without copying contents.
fn originals(network: &Network) -> Vec<(*const u8, usize)> {
    let directions = match &network.layout {
        NetworkLayout::Peer { connect, listen } => [connect.as_ref(), listen.as_ref()],
        NetworkLayout::RouterClient { connect } => [Some(connect), None],
    };
    let mut sources = vec![&network.deployment_roots];
    let mut originals = Vec::new();
    for direction in directions.into_iter().flatten() {
        originals.extend(
            direction
                .addresses
                .iter()
                .map(|address| (address.as_ptr(), address.len())),
        );
        sources.extend([&direction.certificate, &direction.private_key]);
    }
    originals.extend(sources.into_iter().map(|source| {
        let bytes = match source {
            CredentialSource::File(path) => path.as_os_str().as_encoded_bytes(),
            CredentialSource::Base64(secret) => secret.as_bytes(),
        };
        (bytes.as_ptr(), bytes.len())
    }));
    originals
}

#[test]
#[expect(
    clippy::too_many_lines,
    reason = "compare every independent native getter with the explicitly supplied connection inputs"
)]
fn layouts_preserve_addresses_credentials_and_the_fixed_profile() {
    for (layout, expected_mode, expected_connect, expected_listen) in [
        (
            NetworkLayout::Peer {
                connect: Some(connecting()),
                listen: None,
            },
            WhatAmI::Peer,
            vec!["router.example.test:7447", "[::1]:7448"],
            vec![],
        ),
        (
            NetworkLayout::Peer {
                connect: None,
                listen: Some(listening()),
            },
            WhatAmI::Peer,
            vec![],
            vec!["[::]:7449"],
        ),
        (
            NetworkLayout::Peer {
                connect: Some(connecting()),
                listen: Some(listening()),
            },
            WhatAmI::Peer,
            vec!["router.example.test:7447", "[::1]:7448"],
            vec!["[::]:7449"],
        ),
        (
            NetworkLayout::RouterClient {
                connect: connecting(),
            },
            WhatAmI::Client,
            vec!["router.example.test:7447", "[::1]:7448"],
            vec![],
        ),
    ] {
        let network = Network { layout, ..peer() };
        let before = originals(&network);
        let config = network.configuration().unwrap();
        let after = originals(&network);
        assert_eq!(before, after);
        assert_eq!(config.id(), &None);
        assert_eq!(config.mode(), &Some(expected_mode));
        assert_eq!(
            config.queries_max_retained(),
            &Some(network.max_retained_queries)
        );
        let connect = config.connect().endpoints().get(expected_mode).unwrap();
        let endpoints = match connect.as_slice() {
            [] => &[][..],
            [EndPoints::Locators(group)] => {
                assert_eq!(group.strategy, LocatorsStrategy::AllOf);
                group.locators.as_slice()
            }
            _ => panic!("outgoing addresses must form one native all-of group"),
        };
        let listen = config.listen().endpoints().get(expected_mode).unwrap();
        let actual_connect: Vec<_> = endpoints
            .iter()
            .map(|endpoint| endpoint.address().to_string())
            .collect();
        let actual_listen: Vec<_> = listen
            .iter()
            .map(|endpoint| endpoint.address().to_string())
            .collect();
        assert_eq!(actual_connect, expected_connect);
        assert_eq!(actual_listen, expected_listen);
        for endpoint in endpoints.iter().chain(listen) {
            assert_eq!(endpoint.protocol().as_ref(), "tls");
            assert!(endpoint.metadata().is_empty());
            assert!(endpoint.config().is_empty());
        }
        let tls = config.transport().link().tls();
        assert_eq!(
            config.transport().link().protocols().as_deref(),
            Some(["tls".to_owned()].as_slice())
        );
        assert_eq!(tls.enable_mtls(), &Some(true));
        assert_eq!(tls.verify_name_on_connect(), &Some(true));
        assert_eq!(tls.use_public_pki(), &Some(false));
        assert_eq!(config.scouting().multicast().enabled(), &Some(false));
        assert_eq!(config.scouting().gossip().enabled(), &Some(false));
        let multicast_listen = config
            .scouting()
            .multicast()
            .listen()
            .as_ref()
            .and_then(|value| value.get(expected_mode));
        assert_eq!(multicast_listen, Some(&false));
        assert_eq!(
            tls.root_ca_certificate().as_deref(),
            Some("unopened-deployment-roots.pem")
        );
        let expected_connect_certificate =
            (!expected_connect.is_empty()).then_some("unopened-connect-certificate.pem");
        let expected_connect_key =
            (!expected_connect.is_empty()).then_some("unopened-connect-key.pem");
        let expected_listen_certificate =
            (!expected_listen.is_empty()).then_some("unopened-listen-certificate.pem");
        let expected_listen_key =
            (!expected_listen.is_empty()).then_some("unopened-listen-key.pem");
        assert_eq!(
            tls.connect_certificate().as_deref(),
            expected_connect_certificate
        );
        assert_eq!(tls.connect_private_key().as_deref(), expected_connect_key);
        assert_eq!(
            tls.listen_certificate().as_deref(),
            expected_listen_certificate
        );
        assert_eq!(tls.listen_private_key().as_deref(), expected_listen_key);
        for source in [
            tls.root_ca_certificate_base64(),
            tls.connect_certificate_base64(),
            tls.connect_private_key_base64(),
            tls.listen_certificate_base64(),
            tls.listen_private_key_base64(),
        ] {
            assert!(source.is_none());
        }
    }
}

#[test]
fn all_five_base64_sources_remain_distinct_in_the_native_getters() {
    let network = Network {
        deployment_roots: CredentialSource::Base64("cm9vdHM=".into()),
        layout: NetworkLayout::Peer {
            connect: Some(ConnectionEndpoints {
                certificate: CredentialSource::Base64("Y29ubmVjdC1jZXJ0".into()),
                private_key: CredentialSource::Base64("Y29ubmVjdC1rZXk=".into()),
                ..connecting()
            }),
            listen: Some(ConnectionEndpoints {
                certificate: CredentialSource::Base64("bGlzdGVuLWNlcnQ=".into()),
                private_key: CredentialSource::Base64("bGlzdGVuLWtleQ==".into()),
                ..listening()
            }),
        },
        ..peer()
    };
    let before = originals(&network);
    let config = network.configuration().unwrap();
    let after = originals(&network);
    let tls = config.transport().link().tls();
    assert_eq!(before, after);
    for (source, expected) in [
        (tls.root_ca_certificate_base64(), "cm9vdHM="),
        (tls.connect_certificate_base64(), "Y29ubmVjdC1jZXJ0"),
        (tls.connect_private_key_base64(), "Y29ubmVjdC1rZXk="),
        (tls.listen_certificate_base64(), "bGlzdGVuLWNlcnQ="),
        (tls.listen_private_key_base64(), "bGlzdGVuLWtleQ=="),
    ] {
        // Serialize only the actual typed secret getter; Config skips these fields.
        let actual = serde_json::to_value(source).unwrap();
        assert_eq!(actual, Value::String(expected.into()));
    }
    for source in [
        tls.root_ca_certificate(),
        tls.connect_certificate(),
        tls.connect_private_key(),
        tls.listen_certificate(),
        tls.listen_private_key(),
    ] {
        assert!(source.is_none());
    }
}

#[test]
fn mixed_credential_sources_preserve_each_selection_and_refused_original() {
    let mut network = Network {
        layout: NetworkLayout::Peer {
            connect: Some(ConnectionEndpoints {
                certificate: CredentialSource::Base64("Y29ubmVjdC1jZXJ0".into()),
                ..connecting()
            }),
            listen: Some(ConnectionEndpoints {
                private_key: CredentialSource::Base64("bGlzdGVuLWtleQ==".into()),
                ..listening()
            }),
        },
        ..peer()
    };
    let before = originals(&network);
    let config = network.configuration().unwrap();
    let after = originals(&network);
    let NetworkLayout::Peer {
        connect: Some(connect),
        ..
    } = &mut network.layout
    else {
        panic!("the mixed profile supplies the connecting direction");
    };
    connect.addresses[0] = String::new();
    let before_refusal = originals(&network);
    let refusal = network.configuration().unwrap_err();
    let after_refusal = originals(&network);
    let tls = config.transport().link().tls();
    assert_eq!(before, after);
    assert_eq!(before_refusal, after_refusal);
    assert!(matches!(
        refusal,
        NodeStartupRefusal::Address {
            field: "connect/endpoints",
            ..
        }
    ));
    for (file, encoded, expected_file, expected_encoded) in [
        (
            tls.root_ca_certificate(),
            tls.root_ca_certificate_base64(),
            Some("unopened-deployment-roots.pem"),
            None,
        ),
        (
            tls.connect_certificate(),
            tls.connect_certificate_base64(),
            None,
            Some("Y29ubmVjdC1jZXJ0"),
        ),
        (
            tls.connect_private_key(),
            tls.connect_private_key_base64(),
            Some("unopened-connect-key.pem"),
            None,
        ),
        (
            tls.listen_certificate(),
            tls.listen_certificate_base64(),
            Some("unopened-listen-certificate.pem"),
            None,
        ),
        (
            tls.listen_private_key(),
            tls.listen_private_key_base64(),
            None,
            Some("bGlzdGVuLWtleQ=="),
        ),
    ] {
        let actual_encoded = serde_json::to_value(encoded).unwrap();
        let expected_encoded =
            expected_encoded.map_or(Value::Null, |encoded| Value::String(encoded.into()));
        assert_eq!(file.as_deref(), expected_file);
        assert_eq!(actual_encoded, expected_encoded);
    }
}

#[test]
fn empty_directions_and_absent_peer_connections_refuse_without_consuming_inputs() {
    let mut empty_connect = connecting();
    empty_connect.addresses.clear();
    let mut empty_listen = listening();
    empty_listen.addresses.clear();
    let mut empty_client = connecting();
    empty_client.addresses.clear();
    let mut empty_connect_beside_listen = connecting();
    empty_connect_beside_listen.addresses.clear();
    let mut empty_listen_beside_connect = listening();
    empty_listen_beside_connect.addresses.clear();
    for (layout, expected_field) in [
        (
            NetworkLayout::Peer {
                connect: None,
                listen: None,
            },
            "connect/listen/endpoints",
        ),
        (
            NetworkLayout::Peer {
                connect: Some(empty_connect),
                listen: None,
            },
            "connect/endpoints",
        ),
        (
            NetworkLayout::Peer {
                connect: None,
                listen: Some(empty_listen),
            },
            "listen/endpoints",
        ),
        (
            NetworkLayout::RouterClient {
                connect: empty_client,
            },
            "connect/endpoints",
        ),
        (
            NetworkLayout::Peer {
                connect: Some(empty_connect_beside_listen),
                listen: Some(listening()),
            },
            "connect/endpoints",
        ),
        (
            NetworkLayout::Peer {
                connect: Some(connecting()),
                listen: Some(empty_listen_beside_connect),
            },
            "listen/endpoints",
        ),
    ] {
        let network = Network { layout, ..peer() };
        let before = originals(&network);
        let refusal = network.configuration().unwrap_err();
        let after = originals(&network);
        assert_eq!(before, after);
        assert!(
            matches!(refusal, NodeStartupRefusal::Unsupported { field } if field == expected_field)
        );
    }
}

#[test]
fn injected_endpoint_metadata_and_configuration_are_not_plain_addresses() {
    for address in [
        "localhost:7447?priority=1",
        "localhost:7447#server_name=other",
    ] {
        let mut connect = connecting();
        connect.addresses = vec![address.into()];
        let mut listen = listening();
        listen.addresses = vec![address.into()];
        for (layout, expected_field) in [
            (NetworkLayout::RouterClient { connect }, "connect/endpoints"),
            (
                NetworkLayout::Peer {
                    connect: None,
                    listen: Some(listen),
                },
                "listen/endpoints",
            ),
        ] {
            let network = Network { layout, ..peer() };
            let before = originals(&network);
            let refusal = network.configuration().unwrap_err();
            let after = originals(&network);
            assert_eq!(before, after);
            assert!(
                matches!(refusal, NodeStartupRefusal::Unsupported { field } if field == expected_field)
            );
        }
    }
}

#[test]
fn native_address_failures_preserve_the_actual_error_source_and_input() {
    for address in [String::new(), "a".repeat(256)] {
        let mut connect = connecting();
        connect.addresses = vec![address];
        let network = Network {
            layout: NetworkLayout::RouterClient { connect },
            ..peer()
        };
        let before = originals(&network);
        let refusal = network.configuration().unwrap_err();
        let after = originals(&network);
        let source = refusal
            .source()
            .expect("the native endpoint failure remains available");
        let NodeStartupRefusal::Address { field, cause } = &refusal else {
            panic!("native endpoint grammar owns this refusal");
        };
        let display = refusal.to_string();
        let debug = format!("{refusal:?}");
        let native_display = cause.to_string();
        assert_eq!(before, after);
        assert_eq!(*field, "connect/endpoints");
        assert!(ptr::addr_eq(source, cause.as_ref()));
        assert_eq!(display, "invalid connection address: connect/endpoints");
        assert_eq!(
            debug,
            "NodeStartupRefusal(invalid connection address: connect/endpoints)"
        );
        assert_ne!(display, native_display);
    }
}

#[test]
fn native_query_capacity_accepts_the_maximum_and_refuses_one_beyond() {
    let mut network = peer();
    network.max_retained_queries = NonZeroUsize::new(Semaphore::MAX_PERMITS).unwrap();
    let config = network.configuration().unwrap();
    network.max_retained_queries = NonZeroUsize::new(Semaphore::MAX_PERMITS + 1).unwrap();
    let before = originals(&network);
    let refusal = network.configuration().unwrap_err();
    let after = originals(&network);
    assert_eq!(
        config.queries_max_retained().unwrap().get(),
        Semaphore::MAX_PERMITS
    );
    assert_eq!(before, after);
    assert!(matches!(refusal, NodeStartupRefusal::QueryLimitExcessive));
}

#[cfg(unix)]
#[test]
fn non_utf8_credential_paths_preserve_all_five_original_allocations() {
    for expected_field in [
        "root_ca_certificate",
        "connect_certificate",
        "connect_private_key",
        "listen_certificate",
        "listen_private_key",
    ] {
        let mut network = peer();
        let NetworkLayout::Peer {
            connect: Some(connect),
            listen: Some(listen),
        } = &mut network.layout
        else {
            panic!("the explicit fixture supplies both directions");
        };
        let source = match expected_field {
            "root_ca_certificate" => &mut network.deployment_roots,
            "connect_certificate" => &mut connect.certificate,
            "connect_private_key" => &mut connect.private_key,
            "listen_certificate" => &mut listen.certificate,
            "listen_private_key" => &mut listen.private_key,
            _ => unreachable!("the finite fields are exhaustive"),
        };
        *source = CredentialSource::File(PathBuf::from(OsString::from_vec(vec![b'p', 255])));
        let before = originals(&network);
        let refusal = network.configuration().unwrap_err();
        let after = originals(&network);
        assert_eq!(before, after);
        assert!(
            matches!(refusal, NodeStartupRefusal::CredentialPath { field } if field == expected_field)
        );
    }
}

pub(crate) fn listening_counter_network() -> Network {
    let certificates = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../../docs/prds/static-remote-actors/transport-custody/certificates");
    Network {
        layout: NetworkLayout::Peer {
            connect: None,
            listen: Some(ConnectionEndpoints {
                addresses: vec!["localhost:0".into()],
                certificate: CredentialSource::File(certificates.join("recipient-certificate.pem")),
                private_key: CredentialSource::File(certificates.join("recipient-private-key.pem")),
            }),
        },
        deployment_roots: CredentialSource::File(certificates.join("root-ca.pem")),
        max_retained_queries: NonZeroUsize::new(2).unwrap(),
    }
}

fn assert_stopped_counter(retirement: ActorRetirement<StopOnShutdown<Counter>, Never, Vec<Never>>) {
    let ActorRetirement::Completed {
        custody:
            ActorCustody {
                behavior: _actor,
                control,
                user,
                descendants,
                child_failures,
                capability_failures,
                additional_failures,
                received_interpretation,
                received_source,
                source_index,
                acquired_ingress,
                retirement_failures,
                terminal_report,
                unread_owner_cancellation,
            },
        interpretation,
        source,
        settlements,
        completion,
    } = retirement
    else {
        panic!("actual Counter must complete under its selected shutdown template");
    };
    assert_eq!(completion, Completion::Stopped);
    assert!(interpretation.is_none() && source.is_none());
    assert!(received_interpretation.is_none() && received_source.is_none());
    assert!(source_index.is_none() && acquired_ingress.is_none());
    assert!(unread_owner_cancellation.is_none() && terminal_report.is_none());
    assert!(control.is_empty() && user.is_empty());
    assert_eq!(descendants.as_slice(), [] as [Never; 0]);
    assert_eq!(child_failures.as_slice(), [] as [Never; 0]);
    assert!(capability_failures.is_empty());
    assert!(additional_failures.is_empty() && retirement_failures.is_empty());
    let settlement = settlements.settlement_status();
    assert_eq!(settlement, SettlementStatus::Accepted);
}

#[test]
#[expect(
    clippy::too_many_lines,
    reason = "preserve the complete native and connection retirement trace in one ownership test"
)]
fn connection_custody_waits_for_work_after_root_retirement() {
    Builder::new_multi_thread()
        .worker_threads(1)
        .enable_all()
        .build()
        .unwrap()
        .block_on(async {
            let network = listening_counter_network();
            let (connection, closing) = Connection::open(&network)
                .await
                .unwrap_or_else(|_| panic!("explicit private TLS connection"));
            drop(network);
            let session = connection.session().clone();
            let mut closing = pin!(closing);
            let (entered, mut parked) = oneshot::channel();
            let (release, released) = oneshot::channel();
            let calls = Rc::new(Cell::new(0));
            let increments = Box::new(2_u64);
            let allocation = ptr::from_ref(increments.as_ref());
            let work = (increments, Rc::clone(&calls), entered, released);
            let (execution, receiving) = Application::new(Counter::new().stop_on_shutdown())
                .execute_with_invocation(
                    work,
                    |(increments, calls, entered, released), application| {
                        calls.set(calls.get() + 1);
                        async move {
                            let mut client = application
                                .export_client::<_, CounterValue>(
                                    application.root().established_recipient(),
                                )
                                .unwrap_or_else(|_| panic!("actual root client"));
                            let mut admissions = Vec::new();
                            for _ in 0..*increments {
                                admissions.push(client.try_send(CounterMessage::Increment));
                            }
                            let read = client.try_send(CounterMessage::Read(client.recipient()));
                            let value = client.receive().await;
                            let stopped = application.request_shutdown();
                            let report = application.lifecycle().retirement().await;
                            let published =
                                entered.send((admissions, read, value, stopped, report, client));
                            let released = released.await;
                            (increments, calls, published, released)
                        }
                    },
                    Some(connection),
                )
                .unwrap_or_else(|_| panic!("entered Application host"));
            let mut execution = Box::pin(execution);
            let mut completed = None;
            let mut parked = poll_fn(|context| {
                if completed.is_none()
                    && let Poll::Ready(output) = execution.as_mut().poll(context)
                {
                    completed = Some(output);
                }
                pin!(&mut parked).poll(context)
            })
            .await;
            let before_release = {
                let mut borrowed_close = closing.as_mut();
                borrowed_close
                    .as_mut()
                    .poll(&mut Context::from_waker(Waker::noop()))
            };
            let close_poll = match &before_release {
                Poll::Pending => Poll::Pending,
                Poll::Ready(_) => Poll::Ready(()),
            };
            let native_closed_while_parked = session.is_closed();
            let release = release.send(());
            let ((), (outcome, native, notifications)) = tokio::join!(
                async {
                    if completed.is_none() {
                        execution.await;
                    }
                },
                receiving
            );
            let closed = match before_release {
                Poll::Pending => closing.await,
                Poll::Ready(original) => original,
            };
            let mut tail = Vec::new();
            if let Ok((_, _, _, _, _, client)) = &mut parked {
                client.close_admission();
                while let Some(original) = client.receive().await {
                    tail.push(original);
                }
            }
            let native_closed_after_join = session.is_closed();
            let (output, cleanup) = outcome
                .try_into_completed()
                .unwrap_or_else(|_| panic!("original Work completion"));
            let (increments, original_calls, published, released) = output;
            let (admissions, read, value, stopped, report, _client) = parked.unwrap();
            let (joined, original_close) = closed;
            assert!(cleanup.is_ok() && joined.is_ok());
            assert!(matches!(original_close, Ok(Ok(()))));
            assert_eq!(close_poll, Poll::Pending);
            assert!(!native_closed_while_parked && native_closed_after_join);
            assert!(release.is_ok() && published.is_ok() && released.is_ok());
            assert!(completed.is_none());
            assert!(admissions.iter().all(Result::is_ok) && read.is_ok() && stopped.is_ok());
            assert_eq!(admissions.len(), 2);
            let value = value.unwrap();
            assert_eq!((value.from, value.message), (MailAddr::APPLICATION_ROOT, 2));
            assert_eq!(tail.len(), 0);
            assert_eq!(ptr::from_ref(increments.as_ref()), allocation);
            assert_eq!(*increments, 2);
            assert_eq!(calls.get(), 1);
            assert!(Rc::ptr_eq(&calls, &original_calls));
            assert_eq!(report.retirement(), RetirementAssessment::Established);
            assert_eq!(report.failures(), ActorFailureAssessment::NoFailuresFound);
            let notifications = notifications.unwrap();
            assert!(notifications.termination.is_ok() && notifications.retirement.is_ok());
            let (origin, retirement) = native.unwrap();
            assert_eq!(origin.address(), MailAddr::APPLICATION_ROOT);
            assert_stopped_counter(retirement);
        });
}

#[test]
fn connection_custody_survives_whole_parked_execution_disposal() {
    Builder::new_multi_thread()
        .worker_threads(1)
        .enable_all()
        .build()
        .unwrap()
        .block_on(async {
            let network = listening_counter_network();
            let (connection, closing) = Connection::open(&network)
                .await
                .unwrap_or_else(|_| panic!("explicit private TLS connection"));
            drop(network);
            let session = connection.session().clone();
            let closing = pin!(closing);
            let (entered, mut parked) = oneshot::channel();
            let (release, released) = oneshot::channel();
            let calls = Rc::new(Cell::new(0));
            let work = (Box::new(3_u64), Rc::clone(&calls), entered, released);
            let (execution, receiving) = Application::new(Counter::new().stop_on_shutdown())
                .execute_with_invocation(
                    work,
                    |(increments, calls, entered, released), application| {
                        calls.set(calls.get() + 1);
                        async move {
                            let mut client = application
                                .export_client::<_, CounterValue>(
                                    application.root().established_recipient(),
                                )
                                .unwrap_or_else(|_| panic!("actual root client"));
                            let mut admissions = Vec::new();
                            for _ in 0..*increments {
                                admissions.push(client.try_send(CounterMessage::Increment));
                            }
                            let read = client.try_send(CounterMessage::Read(client.recipient()));
                            let value = client.receive().await;
                            let stopped = application.request_shutdown();
                            let report = application.lifecycle().retirement().await;
                            let published =
                                entered.send((admissions, read, value, stopped, report, client));
                            let released = released.await;
                            (increments, calls, published, released)
                        }
                    },
                    Some(connection),
                )
                .unwrap_or_else(|_| panic!("entered Application host"));
            let mut execution = Box::pin(execution);
            let mut completed = None;
            let mut parked = poll_fn(|context| {
                if completed.is_none()
                    && let Poll::Ready(output) = execution.as_mut().poll(context)
                {
                    completed = Some(output);
                }
                pin!(&mut parked).poll(context)
            })
            .await;
            drop(execution);
            let abandoned_release = release.send(());
            let ((outcome, native, notifications), (joined, original_close)) =
                tokio::join!(receiving, closing);
            let mut tail = Vec::new();
            if let Ok((_, _, _, _, _, client)) = &mut parked {
                client.close_admission();
                while let Some(original) = client.receive().await {
                    tail.push(original);
                }
            }
            let native_closed = session.is_closed();
            let (admissions, read, value, stopped, report, _client) = parked.unwrap();
            let ApplicationOutcome::Interrupted { cleanup } = outcome else {
                panic!("dropping the actual parked execution must retain Interrupted");
            };
            assert!(cleanup.is_ok() && joined.is_ok());
            assert!(matches!(original_close, Ok(Ok(()))));
            assert!(native_closed && abandoned_release.is_err());
            assert!(completed.is_none());
            assert_eq!(calls.get(), 1);
            assert!(admissions.iter().all(Result::is_ok) && read.is_ok() && stopped.is_ok());
            assert_eq!(admissions.len(), 3);
            let value = value.unwrap();
            assert_eq!((value.from, value.message), (MailAddr::APPLICATION_ROOT, 3));
            assert_eq!(tail.len(), 0);
            assert_eq!(report.retirement(), RetirementAssessment::Established);
            assert_eq!(report.failures(), ActorFailureAssessment::NoFailuresFound);
            let notifications = notifications.unwrap();
            assert!(notifications.termination.is_ok() && notifications.retirement.is_ok());
            let (origin, retirement) = native.unwrap();
            assert_eq!(origin.address(), MailAddr::APPLICATION_ROOT);
            assert_stopped_counter(retirement);
        });
}

#[test]
#[expect(
    clippy::too_many_lines,
    reason = "preserve the complete native and connection retirement trace in one ownership test"
)]
fn connection_custody_closes_after_unpolled_execution_without_consuming_cold_work() {
    Builder::new_multi_thread()
        .worker_threads(1)
        .enable_all()
        .build()
        .unwrap()
        .block_on(async {
            let network = listening_counter_network();
            let (connection, closing) = Connection::open(&network)
                .await
                .unwrap_or_else(|_| panic!("explicit private TLS connection"));
            drop(network);
            let session = connection.session().clone();
            let closing = pin!(closing);
            let calls = Rc::new(Cell::new(0));
            let increments = Box::new(5_u64);
            let allocation = ptr::from_ref(increments.as_ref());
            let work = (increments, Rc::clone(&calls));
            let (execution, receiving) = Application::new(Counter::new().stop_on_shutdown())
                .execute_with_invocation(
                    work,
                    |(increments, calls), application| {
                        calls.set(calls.get() + 1);
                        async move {
                            let mut client = application
                                .export_client::<_, CounterValue>(
                                    application.root().established_recipient(),
                                )
                                .unwrap_or_else(|_| {
                                    panic!("actual original cold Application root")
                                });
                            let mut admissions = Vec::new();
                            for _ in 0..*increments {
                                admissions.push(client.try_send(CounterMessage::Increment));
                            }
                            let read = client.try_send(CounterMessage::Read(client.recipient()));
                            let value = client.receive().await;
                            let stopped = application.request_shutdown();
                            let report = application.lifecycle().retirement().await;
                            client.close_admission();
                            let mut tail = Vec::new();
                            while let Some(original) = client.receive().await {
                                tail.push(original);
                            }
                            (
                                increments, calls, admissions, read, value, stopped, report,
                                client, tail,
                            )
                        }
                    },
                    Some(connection),
                )
                .unwrap_or_else(|_| panic!("entered Application host"));
            drop(execution);
            let ((outcome, unstarted_native, unstarted_notifications), (joined, original_close)) =
                tokio::join!(receiving, closing);
            let native_closed = session.is_closed();
            let ApplicationOutcome::Unstarted {
                application,
                work: (increments, original_calls),
            } = outcome
            else {
                panic!("unpolled execution returns original Application and Work");
            };
            let recovered_allocation = ptr::from_ref(increments.as_ref());
            let calls_before_retry = original_calls.get();
            let (execution, receiving) = application
                .execute_with(move |application| {
                    original_calls.set(original_calls.get() + 1);
                    async move {
                        let mut client = application
                            .export_client::<_, CounterValue>(
                                application.root().established_recipient(),
                            )
                            .unwrap_or_else(|_| panic!("returned original Application root"));
                        let mut admissions = Vec::new();
                        for _ in 0..*increments {
                            admissions.push(client.try_send(CounterMessage::Increment));
                        }
                        let read = client.try_send(CounterMessage::Read(client.recipient()));
                        let value = client.receive().await;
                        let stopped = application.request_shutdown();
                        let report = application.lifecycle().retirement().await;
                        client.close_admission();
                        let mut tail = Vec::new();
                        while let Some(original) = client.receive().await {
                            tail.push(original);
                        }
                        (
                            increments,
                            original_calls,
                            admissions,
                            read,
                            value,
                            stopped,
                            report,
                            client,
                            tail,
                        )
                    }
                })
                .unwrap_or_else(|_| panic!("original cold inputs remain lawful"));
            let ((), (outcome, native, notifications)) = tokio::join!(execution, receiving);
            let (output, cleanup) = outcome
                .try_into_completed()
                .unwrap_or_else(|_| panic!("returned original Work inputs complete"));
            let (
                increments,
                original_calls,
                admissions,
                read,
                value,
                stopped,
                report,
                _client,
                tail,
            ) = output;
            assert!(joined.is_ok() && cleanup.is_ok());
            assert!(matches!(original_close, Ok(Ok(()))));
            assert!(native_closed && unstarted_native.is_err() && unstarted_notifications.is_err());
            assert_eq!(allocation, recovered_allocation);
            assert_eq!(ptr::from_ref(increments.as_ref()), allocation);
            assert_eq!(*increments, 5);
            assert_eq!(calls_before_retry, 0);
            assert_eq!(calls.get(), 1);
            assert!(Rc::ptr_eq(&calls, &original_calls));
            assert!(admissions.iter().all(Result::is_ok) && read.is_ok() && stopped.is_ok());
            assert_eq!(admissions.len(), 5);
            let value = value.unwrap();
            assert_eq!((value.from, value.message), (MailAddr::APPLICATION_ROOT, 5));
            assert_eq!(tail.len(), 0);
            assert_eq!(report.retirement(), RetirementAssessment::Established);
            assert_eq!(report.failures(), ActorFailureAssessment::NoFailuresFound);
            let notifications = notifications.unwrap();
            assert!(notifications.termination.is_ok() && notifications.retirement.is_ok());
            let (origin, retirement) = native.unwrap();
            assert_eq!(origin.address(), MailAddr::APPLICATION_ROOT);
            assert_stopped_counter(retirement);
        });
}

#[test]
fn connection_custody_refuses_missing_host_and_keeps_original_application_work() {
    let host = Builder::new_multi_thread()
        .worker_threads(1)
        .enable_all()
        .build()
        .unwrap();
    let network = listening_counter_network();
    let (connection, closing) = host.block_on(Connection::open(&network)).unwrap();
    drop(network);
    let session = connection.session().clone();
    let calls = Rc::new(Cell::new(0));
    let increments = Box::new(8_u64);
    let allocation = ptr::from_ref(increments.as_ref());
    let refused = Application::new(Counter::new().stop_on_shutdown()).execute_with_invocation(
        (increments, Rc::clone(&calls)),
        |(increments, calls), application| {
            calls.set(calls.get() + 1);
            async move {
                let stopped = application.request_shutdown();
                (increments, calls, stopped)
            }
        },
        Some(connection),
    );
    let Err((application, (increments, original_calls), cause)) = refused else {
        panic!("an actual missing entered host must refuse before invoking Work");
    };
    let recovered_allocation = ptr::from_ref(increments.as_ref());
    let calls_before_retry = original_calls.get();
    let ((joined, closed), outcome, native, notifications) = host.block_on(async move {
        let closed = closing.await;
        let (execution, receiving) = application
            .execute_with(move |application| async move {
                original_calls.set(original_calls.get() + 1);
                let mut client = application
                    .export_client::<_, CounterValue>(application.root().established_recipient())
                    .unwrap_or_else(|_| panic!("returned actual root"));
                let mut admissions = Vec::new();
                for _ in 0..*increments {
                    admissions.push(client.try_send(CounterMessage::Increment));
                }
                let read = client.try_send(CounterMessage::Read(client.recipient()));
                let value = client.receive().await;
                let stopped = application.request_shutdown();
                let report = application.lifecycle().retirement().await;
                client.close_admission();
                let mut tail = Vec::new();
                while let Some(original) = client.receive().await {
                    tail.push(original);
                }
                (
                    increments,
                    original_calls,
                    admissions,
                    read,
                    value,
                    stopped,
                    report,
                    tail,
                )
            })
            .unwrap_or_else(|_| {
                panic!("the actual original inputs can execute on the retained host")
            });
        let ((), (outcome, native, notifications)) = tokio::join!(execution, receiving);
        (closed, outcome, native, notifications)
    });
    let (output, cleanup) = outcome
        .try_into_completed()
        .unwrap_or_else(|_| panic!("returned original Work finishes"));
    let (increments, original_calls, admissions, read, value, stopped, report, tail) = output;
    assert!(cause.is_missing_context());
    assert!(joined.is_ok() && matches!(closed, Ok(Ok(()))) && cleanup.is_ok());
    assert!(session.is_closed());
    assert_eq!(calls_before_retry, 0);
    assert_eq!(calls.get(), 1);
    assert!(Rc::ptr_eq(&calls, &original_calls));
    assert_eq!(allocation, recovered_allocation);
    assert_eq!(ptr::from_ref(increments.as_ref()), allocation);
    assert_eq!(*increments, 8);
    assert!(admissions.iter().all(Result::is_ok) && read.is_ok() && stopped.is_ok());
    assert_eq!(admissions.len(), 8);
    let value = value.unwrap();
    assert_eq!((value.from, value.message), (MailAddr::APPLICATION_ROOT, 8));
    assert_eq!(tail.len(), 0);
    assert_eq!(report.retirement(), RetirementAssessment::Established);
    assert_eq!(report.failures(), ActorFailureAssessment::NoFailuresFound);
    let notifications = notifications.unwrap();
    assert!(notifications.termination.is_ok() && notifications.retirement.is_ok());
    let (origin, retirement) = native.unwrap();
    assert_eq!(origin.address(), MailAddr::APPLICATION_ROOT);
    assert_stopped_counter(retirement);
}

#[test]
fn connection_custody_keeps_actual_producer_cancellation_and_missing_result() {
    let host = Builder::new_multi_thread()
        .worker_threads(1)
        .enable_all()
        .build()
        .unwrap();
    let network = listening_counter_network();
    let (connection, closing) = host.block_on(Connection::open(&network)).unwrap();
    drop(network);
    let session = connection.session().clone();
    host.shutdown_background();
    let receiver_host = Builder::new_multi_thread()
        .worker_threads(1)
        .enable_all()
        .build()
        .unwrap();
    let ((joined, original_close), cleanup) = receiver_host.block_on(async {
        let received = closing.await;
        let cleanup = session.close().await;
        connection.retire();
        (received, cleanup)
    });
    assert!(cleanup.is_ok() && session.is_closed());
    let failure = joined.expect_err("the actual close producer loses its executor");
    assert!(failure.is_cancelled());
    assert!(original_close.is_err());
}
