//! Finite native count/disposal controls; no actor, flood or stalled-egress claim.
use super::super::{ConnectionEndpoints, CredentialSource, Network, NetworkLayout};
use crate::remote::{IngressError, try_retain};
use std::net::TcpListener;
use std::num::NonZeroUsize;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use tokio::sync::mpsc::{self, error::TrySendError};
use tokio::sync::{Semaphore, TryAcquireError};
use zenoh::qos::CongestionControl;
use zenoh::query::{ConsolidationMode, Reply};
use zenoh::{Error as ZenohError, Session};
use zenoh_config::WhatAmI;

#[derive(Debug, PartialEq)]
enum Refusal {
    Capacity(TryAcquireError),
    QueueFull,
    QueueClosed,
}

#[derive(Debug, PartialEq)]
enum Transfer {
    Accepted,
    Refused {
        cause: Refusal,
        available_before: usize,
        available_after_query: usize,
        available_after_permit: Option<usize>,
    },
}

fn credentials(address: String, certificate: &str) -> ConnectionEndpoints {
    let directory = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../../docs/prds/static-remote-actors/transport-custody/certificates");
    ConnectionEndpoints {
        addresses: vec![address],
        certificate: CredentialSource::File(
            directory.join(format!("{certificate}-certificate.pem")),
        ),
        private_key: CredentialSource::File(
            directory.join(format!("{certificate}-private-key.pem")),
        ),
    }
}

fn network(layout: NetworkLayout) -> Network {
    Network {
        layout,
        deployment_roots: CredentialSource::File(PathBuf::from(env!("CARGO_MANIFEST_DIR")).join(
            "../../docs/prds/static-remote-actors/transport-custody/certificates/root-ca.pem",
        )),
        max_retained_queries: NonZeroUsize::new(16).unwrap(),
    }
}

async fn submit_query(
    session: &Session,
    key: &str,
    marker: u8,
) -> (
    Result<(), ZenohError>,
    mpsc::Receiver<Reply>,
    Arc<Mutex<Option<TrySendError<Reply>>>>,
    Instant,
) {
    let (publication, receiving) = mpsc::channel(1);
    let refused = Arc::new(Mutex::new(None));
    let original_refusal = Arc::clone(&refused);
    let issued = Instant::now();
    let submitted = session
        .get(key)
        .payload(vec![marker])
        .timeout(Duration::from_secs(60))
        .congestion_control(CongestionControl::Drop)
        .consolidation(ConsolidationMode::None)
        .callback(move |original| {
            if let Err(cause) = publication.try_send(original) {
                let mut retained = original_refusal.lock().unwrap();
                if retained.is_none() {
                    *retained = Some(cause);
                }
            }
        })
        .await;
    (submitted, receiving, refused, issued)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[expect(
    clippy::too_many_lines,
    reason = "one finite native transfer trace acquires all EOF and cleanup results before its oracles"
)]
async fn refused_queries_complete_while_accepted_queries_remain_retained() {
    let observations = tokio::time::timeout(Duration::from_secs(10), async {
        let mut observations = Vec::new();
        for role in [WhatAmI::Peer, WhatAmI::Router] {
            let certificate = match role {
                WhatAmI::Peer => "recipient",
                WhatAmI::Router => "router",
                WhatAmI::Client => unreachable!("the controlled listener is a Peer or Router"),
            };
            let reservation = TcpListener::bind(("127.0.0.1", 0)).unwrap();
            let endpoint = reservation.local_addr().unwrap();
            drop(reservation);
            let listener = network(NetworkLayout::Peer {
                connect: None,
                listen: Some(credentials(endpoint.to_string(), certificate)),
            });
            let mut configuration = listener.to_configuration().unwrap();
            // Controlled native Router, not a new public Network layout.
            configuration.set_mode(Some(role)).unwrap();
            let serving = zenoh::open(configuration).await.unwrap();
            let connection = credentials(format!("localhost:{}", endpoint.port()), "caller");
            let layout = match role {
                WhatAmI::Peer => NetworkLayout::Peer {
                    connect: Some(connection),
                    listen: None,
                },
                WhatAmI::Router => NetworkLayout::RouterClient {
                    connect: connection,
                },
                WhatAmI::Client => unreachable!("the controlled listener is a Peer or Router"),
            };
            let outgoing = zenoh::open(network(layout).to_configuration().unwrap())
                .await
                .unwrap();
            let mut transfers_observed = Vec::new();
            for (retention, expected) in [
                (2, Refusal::Capacity(TryAcquireError::NoPermits)),
                (3, Refusal::QueueFull),
            ] {
                let key = format!("bombay/ingress/{role}/{retention}");
                let capacity = Arc::new(Semaphore::new(retention));
                let retained_capacity = Arc::clone(&capacity);
                let (ingress, mut incoming) = mpsc::channel(1);
                let (publication, mut transfers) = mpsc::channel(1);
                let undelivered = Arc::new(Mutex::new(None));
                let original_transfer = Arc::clone(&undelivered);
                let queryable = serving
                    .declare_queryable(&key)
                    .callback(move |query| {
                        let transfer = match try_retain(&ingress, &retained_capacity, query) {
                            Ok(()) => Transfer::Accepted,
                            Err(original) => {
                                let available_before = retained_capacity.available_permits();
                                let (cause, available_after_query, available_after_permit) =
                                    match original {
                                        IngressError::Capacity { original, cause } => {
                                            drop(original);
                                            (
                                                Refusal::Capacity(cause),
                                                retained_capacity.available_permits(),
                                                None,
                                            )
                                        }
                                        IngressError::Queue { cause } => {
                                            let (cause, (original, permit)) = match cause {
                                                TrySendError::Full(original) => {
                                                    (Refusal::QueueFull, original)
                                                }
                                                TrySendError::Closed(original) => {
                                                    (Refusal::QueueClosed, original)
                                                }
                                            };
                                            drop(original);
                                            let after_query = retained_capacity.available_permits();
                                            drop(permit);
                                            (
                                                cause,
                                                after_query,
                                                Some(retained_capacity.available_permits()),
                                            )
                                        }
                                    };
                                Transfer::Refused {
                                    cause,
                                    available_before,
                                    available_after_query,
                                    available_after_permit,
                                }
                            }
                        };
                        if let Err(cause) = publication.try_send(transfer) {
                            let mut retained = original_transfer.lock().unwrap();
                            if retained.is_none() {
                                *retained = Some(cause);
                            }
                        }
                    })
                    .await
                    .unwrap();
                let querier = outgoing.declare_querier(&key).await.unwrap();
                let (route_updates, mut notices) = mpsc::channel(4);
                let matching = querier
                    .matching_listener()
                    .callback(move |status| {
                        let _status_transfer = route_updates.try_send(status);
                    })
                    .await
                    .unwrap();
                while let Some(status) = notices.recv().await {
                    if status.matching() {
                        break;
                    }
                }
                let (active_submission, mut active_replies, active_errors, _) =
                    submit_query(&outgoing, &key, 0).await;
                let active_transfer = if active_submission.is_ok() {
                    transfers.recv().await
                } else {
                    None
                };
                let active = if matches!(active_transfer, Some(Transfer::Accepted)) {
                    incoming.recv().await
                } else {
                    None
                };
                let active_marker = active
                    .as_ref()
                    .and_then(|(query, _)| query.payload())
                    .map(|original| original.to_bytes().to_vec());
                let (queued_submission, mut queued_replies, queued_errors, _) =
                    submit_query(&outgoing, &key, 1).await;
                let queued_transfer = if queued_submission.is_ok() {
                    transfers.recv().await
                } else {
                    None
                };
                let before_overflow = capacity.available_permits();
                let overflow = if matches!(queued_transfer, Some(Transfer::Accepted)) {
                    let (submitted, mut replies, errors, issued) =
                        submit_query(&outgoing, &key, 2).await;
                    let transferred = if submitted.is_ok() {
                        transfers.recv().await
                    } else {
                        None
                    };
                    let mut originals = Vec::new();
                    while let Some(original) = replies.recv().await {
                        originals.push(original);
                    }
                    Some((submitted, transferred, originals, errors, issued.elapsed()))
                } else {
                    None
                };
                // Overflow completion precedes releasing either accepted original.
                drop(active);
                let mut queued_markers = Vec::new();
                while let Ok((query, permit)) = incoming.try_recv() {
                    queued_markers
                        .push(query.payload().map(|original| original.to_bytes().to_vec()));
                    drop((query, permit));
                }
                let mut active_outputs = Vec::new();
                while let Some(original) = active_replies.recv().await {
                    active_outputs.push(original);
                }
                let mut queued_outputs = Vec::new();
                while let Some(original) = queued_replies.recv().await {
                    queued_outputs.push(original);
                }
                let matching_withdrawal = matching.undeclare().await;
                let querier_withdrawal = querier.undeclare().await;
                let queryable_withdrawal = queryable.undeclare().await;
                let incoming_end = incoming.try_recv();
                transfers_observed.push((
                    retention,
                    expected,
                    capacity.available_permits(),
                    before_overflow,
                    active_submission,
                    active_transfer,
                    active_marker,
                    active_outputs,
                    active_errors,
                    queued_submission,
                    queued_transfer,
                    queued_markers,
                    queued_outputs,
                    queued_errors,
                    overflow,
                    undelivered,
                    matching_withdrawal,
                    querier_withdrawal,
                    queryable_withdrawal,
                    incoming_end,
                ));
            }
            let outgoing_close = outgoing.close().await;
            let serving_close = serving.close().await;
            observations.push((transfers_observed, outgoing_close, serving_close));
        }
        observations
    })
    .await
    .expect("finite TLS setup/query/cleanup watchdog; never a semantic inverse oracle");
    assert_eq!(observations.len(), 2);
    for (transfers_observed, outgoing_close, serving_close) in observations {
        assert!(outgoing_close.is_ok() && serving_close.is_ok());
        assert_eq!(transfers_observed.len(), 2);
        for (
            retention,
            expected,
            released,
            before_overflow,
            active_submission,
            active_transfer,
            active_marker,
            active_outputs,
            active_errors,
            queued_submission,
            queued_transfer,
            queued_markers,
            queued_outputs,
            queued_errors,
            overflow,
            undelivered,
            matching_withdrawal,
            querier_withdrawal,
            queryable_withdrawal,
            incoming_end,
        ) in transfers_observed
        {
            assert!(
                matching_withdrawal.is_ok()
                    && querier_withdrawal.is_ok()
                    && queryable_withdrawal.is_ok()
            );
            assert!(active_submission.is_ok() && queued_submission.is_ok());
            assert_eq!(active_transfer, Some(Transfer::Accepted));
            assert_eq!(queued_transfer, Some(Transfer::Accepted));
            assert_eq!(active_marker, Some(vec![0]));
            assert_eq!(queued_markers, [Some(vec![1])]);
            assert_eq!((before_overflow, released), (retention - 2, retention));
            let (submitted, transferred, replies, errors, elapsed) =
                overflow.expect("actual second transfer permits overflow probe");
            assert!(submitted.is_ok());
            let expected_after_permit = match &expected {
                Refusal::Capacity(_) => None,
                Refusal::QueueFull | Refusal::QueueClosed => Some(retention - 2),
            };
            assert_eq!(
                transferred,
                Some(Transfer::Refused {
                    cause: expected,
                    available_before: 0,
                    available_after_query: 0,
                    available_after_permit: expected_after_permit
                })
            );
            assert!(
                elapsed < Duration::from_secs(60),
                "native EOF precedes configured query expiry"
            );
            assert!(replies.is_empty() && active_outputs.is_empty() && queued_outputs.is_empty());
            let overflow_delivery = errors.lock().unwrap();
            let active_delivery = active_errors.lock().unwrap();
            let queued_delivery = queued_errors.lock().unwrap();
            let transfer_delivery = undelivered.lock().unwrap();
            assert!(
                overflow_delivery.is_none()
                    && active_delivery.is_none()
                    && queued_delivery.is_none()
                    && transfer_delivery.is_none()
            );
            assert!(matches!(
                incoming_end,
                Err(mpsc::error::TryRecvError::Disconnected)
            ));
        }
    }
}
