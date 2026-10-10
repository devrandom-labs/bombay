use bombay::behavior::{
    BehaviorBase, ClassifySettlement, EstablishedDelivery, MessageProtocol, NoSends,
    SettlementStatus, Step,
};
use bombay::prelude::*;
use bombay::{ActorNotificationReceipts, ApplicationOutcome, TrySendError};
use core::ops::AsyncFn;
use tokio::{runtime::Builder, sync::oneshot};

mod application_support;
use application_support::RootTerminal;

#[derive(Clone, Copy)]
enum ProviderAvailability {
    Connected,
    Unavailable,
}
struct FixtureCredential {
    marker: u8,
    revision: u64,
    availability: ProviderAvailability,
}
struct VerifiedFixture {
    deadline: u64,
}
#[derive(Debug)]
enum VerificationError {
    Invalid,
    Stale,
    Unavailable(Option<oneshot::error::RecvError>),
}

async fn lookup_provider(
    credential: FixtureCredential,
) -> Result<VerifiedFixture, VerificationError> {
    let available = match credential.availability {
        ProviderAvailability::Connected => Some([(71, 3, 10)]),
        ProviderAvailability::Unavailable => None,
    };
    let Some(entries) = available else {
        return Err(VerificationError::Unavailable(None));
    };
    for (marker, revision, deadline) in entries {
        if credential.marker == marker {
            return if credential.revision == revision {
                Ok(VerifiedFixture { deadline })
            } else {
                Err(VerificationError::Stale)
            };
        }
    }
    Err(VerificationError::Invalid)
}

async fn scheduled_provider(
    credential: FixtureCredential,
) -> Result<VerifiedFixture, VerificationError> {
    let (transaction, receiving) = oneshot::channel();
    match credential.availability {
        ProviderAvailability::Connected => {
            let transferred = transaction.send(credential);
            assert!(transferred.is_ok());
        }
        ProviderAvailability::Unavailable => drop(transaction),
    }
    let credential = receiving
        .await
        .map_err(|cause| VerificationError::Unavailable(Some(cause)))?;
    match credential.marker {
        71 => match credential.revision {
            3 => Ok(VerifiedFixture { deadline: 10 }),
            _ => Err(VerificationError::Stale),
        },
        _ => Err(VerificationError::Invalid),
    }
}

#[derive(Debug, PartialEq)]
enum PermissionRejection {
    Invalid,
    Stale,
    Unavailable,
    Revoked,
    Expired,
    Full,
    Closed,
}
enum Permission {
    Granted,
    Revoked,
}
struct PermissionAdmission {
    permission: Permission,
    now: u64,
}

impl PermissionAdmission {
    fn attempt(
        &mut self,
        verification: &Result<VerifiedFixture, VerificationError>,
        service: &ExternalActor<ProcessingReplies>,
        target: &EstablishedRecipient<AdmissionLedger>,
        command: AdmissionCommand,
    ) -> Result<(), (PermissionRejection, AdmissionCommand)> {
        let refusal = match verification {
            Err(VerificationError::Invalid) => Some(PermissionRejection::Invalid),
            Err(VerificationError::Stale) => Some(PermissionRejection::Stale),
            Err(VerificationError::Unavailable(_)) => Some(PermissionRejection::Unavailable),
            Ok(evidence) => match self.permission {
                Permission::Revoked => Some(PermissionRejection::Revoked),
                Permission::Granted if self.now >= evidence.deadline => {
                    Some(PermissionRejection::Expired)
                }
                Permission::Granted => None,
            },
        };
        if let Some(refusal) = refusal {
            return Err((refusal, command));
        }
        match service.try_send(target, command) {
            Ok(()) => Ok(()),
            Err(TrySendError::Full(original)) => Err((PermissionRejection::Full, original)),
            Err(TrySendError::Closed(original)) => Err((PermissionRejection::Closed, original)),
        }
    }
}

#[derive(Debug, PartialEq)]
struct ProcessingReceipt {
    prefix_count: usize,
    protected_count: usize,
}
type ProcessingReplies = MessageProtocol<MailAddr, ProcessingReceipt>;
#[derive(Debug, PartialEq)]
enum VerificationObservation {
    Accepted,
    Invalid,
    Stale,
    Unavailable,
}

fn verification_command(
    verification: &Result<VerifiedFixture, VerificationError>,
    reply_to: EstablishedRecipient<ProcessingReplies>,
) -> AdmissionCommand {
    let observation = match verification {
        Ok(_) => VerificationObservation::Accepted,
        Err(VerificationError::Invalid) => VerificationObservation::Invalid,
        Err(VerificationError::Stale) => VerificationObservation::Stale,
        Err(VerificationError::Unavailable(_)) => VerificationObservation::Unavailable,
    };
    AdmissionCommand::Verification {
        observation,
        reply_to,
    }
}
enum AdmissionCommand {
    Verification {
        observation: VerificationObservation,
        reply_to: EstablishedRecipient<ProcessingReplies>,
    },
    Prefix(usize),
    Protected(Box<[u8]>),
    Snapshot(EstablishedRecipient<ProcessingReplies>),
}
#[derive(Default)]
struct AdmissionLedger {
    prefix: Vec<(MailAddr, usize)>,
    protected: Vec<(MailAddr, Box<[u8]>)>,
    snapshots: usize,
    verification: Option<VerificationObservation>,
}

#[bombay::actor(sends = pub(crate) { processing: Vec<EstablishedDelivery<ProcessingReplies>>, })]
#[allow(
    clippy::unnecessary_wraps,
    reason = "the pure owning Behavior contract returns typed Actions"
)]
impl AdmissionLedger {
    fn receive(&mut self, from: MailAddr, command: AdmissionCommand) -> BehaviorActed<Self> {
        let reply_to = match command {
            AdmissionCommand::Prefix(sequence) => {
                self.prefix.push((from, sequence));
                None
            }
            AdmissionCommand::Protected(payload) => {
                self.protected.push((from, payload));
                None
            }
            AdmissionCommand::Verification {
                observation,
                reply_to,
            } => {
                self.verification = Some(observation);
                Some(reply_to)
            }
            AdmissionCommand::Snapshot(reply_to) => {
                self.snapshots += 1;
                Some(reply_to)
            }
        };
        let actions = match reply_to {
            Some(reply_to) => Actions::cont().send_processing(EstablishedDelivery::new(
                reply_to,
                ProcessingReceipt {
                    prefix_count: self.prefix.len(),
                    protected_count: self.protected.len(),
                },
            )),
            None => Actions::cont(),
        };
        Ok(actions)
    }
}

#[derive(Clone, Copy, Debug)]
enum AdmissionCase {
    Granted,
    Revoked,
    Expired,
    Invalid,
    Stale,
    Unavailable,
}

#[expect(
    clippy::too_many_lines,
    reason = "one temporal trace retains provider, payload, Actions and complete native custody"
)]
fn verify_provider_admission<Provider>(
    provider: Provider,
    consume_verification: fn(
        &Result<VerifiedFixture, VerificationError>,
        EstablishedRecipient<ProcessingReplies>,
    ) -> AdmissionCommand,
) -> Option<oneshot::error::RecvError>
where
    Provider: AsyncFn(FixtureCredential) -> Result<VerifiedFixture, VerificationError>,
{
    let mut unavailable_receipt = None;
    for case in [
        AdmissionCase::Granted,
        AdmissionCase::Revoked,
        AdmissionCase::Expired,
        AdmissionCase::Invalid,
        AdmissionCase::Stale,
        AdmissionCase::Unavailable,
    ] {
        let host = Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("the explicit fixture host");
        let provider = &provider;
        let received = host
            .block_on(
                Application::new(AdmissionLedger::default().stop_on_shutdown())
                    .run_with::<RootTerminal<StopOnShutdown<AdmissionLedger>>, _, _, _, _, _>(
                        |application| async move {
                            let credential = FixtureCredential {
                                marker: match case {
                                    AdmissionCase::Invalid => 72,
                                    _ => 71,
                                },
                                revision: match case {
                                    AdmissionCase::Stale => 2,
                                    _ => 3,
                                },
                                availability: match case {
                                    AdmissionCase::Unavailable => ProviderAvailability::Unavailable,
                                    _ => ProviderAvailability::Connected,
                                },
                            };
                            let verification = provider(credential).await;
                            let mut admission = PermissionAdmission {
                                permission: Permission::Granted,
                                now: 9,
                            };
                            let interface = application.interface(());
                            let mut service = interface
                                .external::<ProcessingReplies>()
                                .expect("the service endpoint");
                            let target = application.root().established_recipient();
                            let actor_address = application.root().address();
                            let verification_input =
                                consume_verification(&verification, service.recipient());
                            let verification_admission =
                                service.send(&target, verification_input).await;
                            assert!(verification_admission.is_ok());
                            let verification_receipt = service
                                .receive()
                                .await
                                .expect("the provider result reached pure Behavior Actions");
                            assert_eq!(verification_receipt.from, actor_address);
                            assert_eq!(
                                verification_receipt.message,
                                ProcessingReceipt {
                                    prefix_count: 0,
                                    protected_count: 0
                                }
                            );
                            // Existing native capacity, not a new protected-admission default.
                            for sequence in 0..1_024 {
                                let accepted =
                                    service.try_send(&target, AdmissionCommand::Prefix(sequence));
                                match accepted {
                                    Ok(()) => {}
                                    Err(TrySendError::Full(_)) => panic!("the native prefix fits"),
                                    Err(TrySendError::Closed(_)) => {
                                        panic!("the native prefix remains open")
                                    }
                                }
                            }
                            let payload = vec![23, 29].into_boxed_slice();
                            let allocation = payload.as_ptr();
                            let first = admission.attempt(
                                &verification,
                                &service,
                                &target,
                                AdmissionCommand::Protected(payload),
                            );
                            let (initial_refusal, original) =
                                first.expect_err("no protected command fits this prefix");
                            let AdmissionCommand::Protected(payload) = &original else {
                                panic!("same typed command")
                            };
                            assert_eq!(payload.as_ptr(), allocation);
                            match case {
                                AdmissionCase::Revoked => {
                                    admission.permission = Permission::Revoked;
                                }
                                AdmissionCase::Expired => admission.now = 10,
                                _ => {}
                            }
                            // A useful Actions receipt proves all prefix work has run before retry.
                            let snapshot = service
                                .send(&target, AdmissionCommand::Snapshot(service.recipient()))
                                .await;
                            assert!(snapshot.is_ok());
                            let before = service
                                .receive()
                                .await
                                .expect("the Actions processing receipt");
                            assert_eq!(before.from, actor_address);
                            assert_eq!(
                                before.message,
                                ProcessingReceipt {
                                    prefix_count: 1_024,
                                    protected_count: 0
                                }
                            );
                            let retried =
                                admission.attempt(&verification, &service, &target, original);
                            let snapshot = service
                                .send(&target, AdmissionCommand::Snapshot(service.recipient()))
                                .await;
                            assert!(snapshot.is_ok());
                            let after = service
                                .receive()
                                .await
                                .expect("the final Actions processing receipt");
                            assert_eq!(after.from, actor_address);
                            service.close_admission();
                            let exhausted = service.receive().await;
                            assert!(
                                exhausted.is_none(),
                                "the complete processing reply trace has no duplicate"
                            );
                            let stopped = application.lifecycle().request_shutdown();
                            assert_eq!(stopped, Ok(()));
                            (
                                actor_address,
                                service.address(),
                                allocation,
                                initial_refusal,
                                retried,
                                verification,
                                after.message,
                            )
                        },
                    ),
            )
            .unwrap_or_else(|failure| {
                drop(failure);
                panic!("the native Application remains successful")
            });
        drop(host);
        let (
            ApplicationOutcome::Completed {
                output: (actor_address, sender, allocation, initial, retried, verification, after),
                cleanup: Ok(()),
            },
            Ok((
                origin,
                ActorRetirement::Completed {
                    behavior,
                    settlements,
                    control,
                    user,
                    descendants,
                    child_failures: (),
                    completion,
                    capability_failures,
                    unread_owner_cancellation,
                    interpretation,
                    source,
                    additional_failures,
                    received_interpretation,
                    received_source,
                    source_index,
                    acquired_ingress,
                    retirement_failures,
                    terminal_report,
                },
            )),
            Ok(ActorNotificationReceipts {
                termination: Ok(()),
                retirement: Ok(()),
            }),
        ) = received
        else {
            panic!("Work, full native retirement and both notification receipts remain acquired")
        };
        let expected_count = match case {
            AdmissionCase::Granted => 1,
            _ => 0,
        };
        // This independent actor-effect oracle runs only after native cleanup.
        assert_eq!(
            behavior.base().protected.len(),
            expected_count,
            "{case:?}: fresh permission refuses stale admission"
        );
        assert_eq!(
            after,
            ProcessingReceipt {
                prefix_count: 1_024,
                protected_count: expected_count
            }
        );
        assert_eq!(origin.address(), actor_address);
        assert_eq!(behavior.base().prefix.len(), 1_024);
        for (sequence, &(from, admitted_sequence)) in behavior.base().prefix.iter().enumerate() {
            assert_eq!((from, admitted_sequence), (sender, sequence));
        }
        assert_eq!(behavior.base().snapshots, 2);
        match (case, initial, retried) {
            (AdmissionCase::Granted, PermissionRejection::Full, Ok(())) => {
                let [(from, original)] = behavior.base().protected.as_slice() else {
                    panic!("one exact original")
                };
                assert_eq!(*from, sender);
                assert_eq!(original.as_ptr(), allocation);
                assert_eq!(original.as_ref(), [23, 29]);
            }
            (case, initial, Err((refusal, AdmissionCommand::Protected(original)))) => {
                let (expected_initial, expected_final) = match case {
                    AdmissionCase::Revoked => {
                        (PermissionRejection::Full, PermissionRejection::Revoked)
                    }
                    AdmissionCase::Expired => {
                        (PermissionRejection::Full, PermissionRejection::Expired)
                    }
                    AdmissionCase::Invalid => {
                        (PermissionRejection::Invalid, PermissionRejection::Invalid)
                    }
                    AdmissionCase::Stale => {
                        (PermissionRejection::Stale, PermissionRejection::Stale)
                    }
                    AdmissionCase::Unavailable => (
                        PermissionRejection::Unavailable,
                        PermissionRejection::Unavailable,
                    ),
                    AdmissionCase::Granted => panic!("granted original must be admitted"),
                };
                assert_eq!((initial, refusal), (expected_initial, expected_final));
                assert_eq!(original.as_ptr(), allocation);
                assert_eq!(original.as_ref(), [23, 29]);
            }
            _ => panic!("the exact expected ownership-bearing admission result"),
        }
        match (case, &verification, &behavior.base().verification) {
            (
                AdmissionCase::Granted | AdmissionCase::Revoked | AdmissionCase::Expired,
                Ok(_),
                Some(VerificationObservation::Accepted),
            )
            | (
                AdmissionCase::Invalid,
                Err(VerificationError::Invalid),
                Some(VerificationObservation::Invalid),
            )
            | (
                AdmissionCase::Stale,
                Err(VerificationError::Stale),
                Some(VerificationObservation::Stale),
            )
            | (
                AdmissionCase::Unavailable,
                Err(VerificationError::Unavailable(_)),
                Some(VerificationObservation::Unavailable),
            ) => {}
            _ => panic!(
                "provider cause and independently consumed observation must agree with the fixture"
            ),
        }
        if let Err(VerificationError::Unavailable(original_receipt)) = verification {
            unavailable_receipt = original_receipt;
        }
        assert_eq!(completion, Completion::Stopped);
        assert!(control.is_empty() && user.is_empty() && descendants.is_empty());
        assert!(capability_failures.is_empty() && unread_owner_cancellation.is_none());
        assert!(interpretation.is_none() && source.is_none() && additional_failures.is_empty());
        assert!(
            received_interpretation.is_none()
                && received_source.is_none()
                && source_index.is_none()
        );
        assert!(
            acquired_ingress.is_none()
                && retirement_failures.is_empty()
                && terminal_report.is_none()
        );
        let [settlement] = settlements.as_slice() else {
            panic!("the final retained action product")
        };
        assert_eq!(settlement.settlement_status(), SettlementStatus::Accepted);
        assert!(settlement.creations.is_empty());
        assert!(matches!(settlement.sends.owned, NoSends));
        assert_eq!(settlement.sends.inner.processing.len(), 0);
        assert!(matches!(settlement.become_, Step::Stop(_)));
    }
    unavailable_receipt
}

#[test]
fn lookup_permission_is_rechecked_after_actual_capacity_pressure() {
    let receiving = verify_provider_admission(lookup_provider, verification_command);
    assert!(
        receiving.is_none(),
        "lookup has no Tokio transaction cause to invent"
    );
}
#[test]
fn scheduled_permission_is_rechecked_after_actual_capacity_pressure() {
    let original_receipt = verify_provider_admission(scheduled_provider, verification_command);
    assert!(
        original_receipt.is_some(),
        "the dropped transaction's original RecvError remains owned"
    );
}
