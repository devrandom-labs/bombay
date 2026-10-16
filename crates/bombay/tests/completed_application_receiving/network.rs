//! Networked Application work retains live Entity families and complete retirement.
use super::{
    AccountAdmission, AccountsRole, LiveAccountCommand, LiveAccountNotice, LiveAccountReply,
    LiveAccounts, ReceivingAccount, assert_completed_account, live_application,
};
use bombay::actors::ActorExt;
use bombay::behavior::{BehaviorBase, EventLayer, Never, NoSends, Step, User};
use bombay::entity::{DirectoryConfig, EntityCapacity, EntityId, EntityMetrics, EntityShutdown};
use bombay::remote::{ConnectionEndpoints, CredentialSource, Network, NetworkLayout};
use bombay::{
    ActorCustody, ActorFailureAssessment, ActorRetirement, Application, MailAddr,
    RetirementAssessment, WorkOutcome,
};
use bombay_engine::Completion;
use std::cell::Cell;
use std::mem;
use std::num::NonZeroUsize;
use std::path::PathBuf;
use std::rc::Rc;
use std::sync::{Arc, Mutex, PoisonError};
use std::time::Duration;
use tokio::time::timeout;

fn listening_network() -> Network {
    let certificates = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../../docs/prds/static-remote-actors/transport-custody/certificates");
    Network {
        layout: NetworkLayout::Peer {
            connect: None,
            listen: Some(ConnectionEndpoints {
                addresses: vec!["localhost:0".to_owned()],
                certificate: CredentialSource::File(certificates.join("recipient-certificate.pem")),
                private_key: CredentialSource::File(certificates.join("recipient-private-key.pem")),
            }),
        },
        deployment_roots: CredentialSource::File(certificates.join("root-ca.pem")),
        max_retained_queries: NonZeroUsize::new(2).unwrap(),
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[expect(
    clippy::too_many_lines,
    reason = "one real Node journey retains original caller-local Work and independent live-family retirement"
)]
async fn node_keeps_live_family_receiving_and_caller_local_work() {
    let borrowed = Cell::new(0);
    let original = Rc::new(vec![17_u8, 19]);
    let acquired = Rc::clone(&original);
    let completed = timeout(Duration::from_secs(10), async {
        let account_original = Arc::new(vec![241_u8, 251]);
        let role_original = Arc::new(vec![157_u8, 163]);
        let selection = Arc::clone(&role_original);
        let hydrated = Arc::new(Mutex::new(Vec::new()));
        let activation_failures = Arc::new(Mutex::new(Vec::new()));
        let refusals = Arc::new(Mutex::new(Vec::new()));
        let forced = Arc::new(Mutex::new(Vec::new()));
        let retired = Arc::new(Mutex::new(Vec::new()));
        let work_retirements = Arc::clone(&retired);
        let application = Application::with_topology(
            ReceivingAccount {
                admission: AccountAdmission::Ready,
                original: Arc::new(vec![31, 37]),
            }
            .stop_on_shutdown(),
            live_application::Topology::new(),
        )
        .entity_family(
            AccountsRole(Arc::clone(&role_original)),
            LiveAccounts {
                original: Arc::clone(&account_original),
                hydrated: Arc::clone(&hydrated),
                activation_failures: Arc::clone(&activation_failures),
                refusals: Arc::clone(&refusals),
                forced: Arc::clone(&forced),
                retired: Arc::clone(&retired),
                disposal_cause: None,
            },
            DirectoryConfig::default(),
            EntityCapacity::new(NonZeroUsize::new(1).unwrap(), NonZeroUsize::new(1).unwrap()),
        );
        let mut node = application
            .open_node(listening_network())
            .await
            .unwrap_or_else(|_| panic!("the actual listening Node opens"));
        let borrowed_work = &borrowed;
        let (execution, receiving) = node
            .execute_with(move |application| async move {
                borrowed_work.set(borrowed_work.get() + 1);
                let accounts = application.entities(AccountsRole(selection));
                let account = accounts.entity(73);
                let interface = application.interface(account);
                let mut observer = interface.external::<LiveAccountNotice>().unwrap();
                let admitted = observer
                    .send(
                        interface.api(),
                        LiveAccountCommand {
                            observer: observer.recipient(),
                            value: 283,
                        },
                    )
                    .await;
                let reply = match &admitted {
                    Ok(()) => observer.receive().await,
                    Err(_) => None,
                };
                let shutdown = application.request_shutdown();
                let root_report = application.lifecycle().retirement().await;
                let admitted_again = observer
                    .send(
                        interface.api(),
                        LiveAccountCommand {
                            observer: observer.recipient(),
                            value: 293,
                        },
                    )
                    .await;
                let reply_again = match &admitted_again {
                    Ok(()) => observer.receive().await,
                    Err(_) => None,
                };
                let retired_before_work_completion = work_retirements
                    .lock()
                    .unwrap_or_else(PoisonError::into_inner)
                    .len();
                (
                    admitted,
                    reply,
                    shutdown,
                    root_report,
                    admitted_again,
                    reply_again,
                    retired_before_work_completion,
                    acquired,
                    observer,
                )
            })
            .unwrap_or_else(|_| panic!("the entered host issues original Work"));
        execution.await;
        let (work, root, notifications, families) = receiving.await.into_parts();
        let closed = node.close().await;
        let WorkOutcome::Completed {
            output:
                (
                    admitted,
                    reply,
                    shutdown,
                    root_report,
                    admitted_again,
                    reply_again,
                    retired_before_work_completion,
                    acquired,
                    _observer,
                ),
            cleanup,
        } = work
        else {
            panic!("caller-local Work completes beside joined retirement");
        };
        let notifications = notifications.expect("both original notifications");
        let (family, ()) = families;
        let (role, family) = family.expect("the original live family receiving succeeds");
        let EntityShutdown::Settled {
            represented,
            entities,
        } = family.shutdown()
        else {
            panic!("the actual family finishes its shutdown");
        };
        let mut retained = retired.lock().unwrap_or_else(PoisonError::into_inner);
        let native_accounts = mem::take(&mut *retained);
        drop(retained);
        let [(id, _, joined, report)] = native_accounts.as_slice() else {
            panic!("one complete actual account retirement remains owned");
        };
        let retirement = joined
            .as_ref()
            .unwrap_or_else(|_| panic!("the actual account task joins"));
        let (custody, interpretation, source) = match retirement {
            ActorRetirement::Completed {
                custody,
                settlements,
                completion,
                interpretation,
                source,
            } => {
                assert_eq!(settlements.len(), 1);
                assert_eq!(settlements[0].creations.len(), 0);
                assert_eq!(settlements[0].sends.owned, NoSends);
                assert_eq!(settlements[0].sends.inner.notices.len(), 0);
                assert!(matches!(settlements[0].become_, Step::Stop(_)));
                assert_eq!(*completion, Completion::Stopped);
                assert!(custody.control.is_empty());
                // Completed permits both existing unread cancellation alternatives.
                (custody, interpretation, source)
            }
            ActorRetirement::OwnerCancelled {
                custody,
                settlements,
                interpretation,
                source,
            } => {
                assert!(settlements.is_empty());
                assert!(matches!(custody.control.as_slice(), [EventLayer::Owned(_)]));
                assert!(custody.unread_owner_cancellation.is_none());
                (custody, interpretation, source)
            }
            _ => panic!("the real fence permits stopped or same-lease cancellation"),
        };
        let ActorCustody {
            behavior,
            control: _control,
            user,
            descendants,
            child_failures,
            capability_failures,
            unread_owner_cancellation: _unread_owner_cancellation,
            additional_failures,
            received_interpretation,
            received_source,
            source_index,
            acquired_ingress,
            retirement_failures,
            terminal_report,
        } = custody;
        let Some(User {
            from,
            message: LiveAccountReply::Accepted(value),
        }) = reply
        else {
            panic!("the actual account returns its typed processing reply");
        };
        let Some(User {
            from: from_again,
            message: LiveAccountReply::Accepted(value_again),
        }) = reply_again
        else {
            panic!("the same live account processes a command after root retirement");
        };
        assert_completed_account(&root);
        assert!(admitted.is_ok());
        assert!(admitted_again.is_ok());
        assert_eq!(shutdown, Ok(()));
        assert!(cleanup.is_ok());
        assert!(closed.is_ok());
        assert!(matches!(notifications.termination, Ok(())));
        assert!(matches!(notifications.retirement, Ok(())));
        assert_eq!(root_report.retirement(), RetirementAssessment::Established);
        assert_eq!(
            root_report.failures(),
            ActorFailureAssessment::NoFailuresFound
        );
        assert_eq!(retired_before_work_completion, 0);
        assert_eq!(borrowed.get(), 1);
        assert!(Rc::ptr_eq(&acquired, &original));
        assert_eq!(acquired.as_slice(), &[17, 19]);
        assert_ne!(from, MailAddr::APPLICATION_ROOT);
        assert_eq!(value, 283);
        assert_eq!(from_again, from);
        assert_eq!(value_again, 293);
        assert!(Arc::ptr_eq(&role.0, &role_original));
        assert_eq!(*represented, 1);
        assert_eq!(entities.as_slice(), &[EntityId::new(73)]);
        assert_eq!(*id, EntityId::new(73));
        assert!(Arc::ptr_eq(&behavior.base().original, &account_original));
        assert_eq!(behavior.base().original.as_slice(), &[241, 251]);
        assert_eq!(behavior.base().accepted, vec![283, 293]);
        assert_eq!(child_failures.as_slice(), [] as [Never; 0]);
        assert!(user.is_empty());
        assert_eq!(descendants.len(), 0);
        assert!(capability_failures.is_empty());
        assert!(additional_failures.is_empty());
        assert!(retirement_failures.is_empty());
        assert!(terminal_report.is_none());
        assert!(matches!(
            (
                interpretation,
                source,
                received_interpretation,
                received_source,
                source_index,
                acquired_ingress
            ),
            (None, None, None, None, None, None)
        ));
        assert_eq!(report.retirement(), RetirementAssessment::Established);
        assert_eq!(report.failures(), ActorFailureAssessment::NoFailuresFound);
        assert_eq!(
            *family.metrics(),
            EntityMetrics {
                activations: 1,
                hydration_failures: 0,
                launch_failures: 0,
                capacity_refusals: 0,
                forced_retirements: 0,
                peak_hydrations: 1,
                residents: 0,
            }
        );
        assert!(family.disposal_failure().is_none());
        assert_eq!(
            hydrated
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .as_slice(),
            &[EntityId::new(73)]
        );
        assert!(
            activation_failures
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .is_empty()
        );
        assert!(
            refusals
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .is_empty()
        );
        assert!(
            forced
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .is_empty()
        );
    })
    .await;
    completed.expect("Node work, actual family retirement and close finish within the watchdog");
}
