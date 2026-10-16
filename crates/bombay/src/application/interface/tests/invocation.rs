//! Genuine absent Work uses the existing family installer and native retirement.
use super::{AccountReport, Accounts, AccountsRole};
use crate::actors::ActorExt;
use crate::application::execution::ApplicationWorkPresence;
use crate::behavior::{
    BehaviorBase, ClassifySettlement, EventLayer, Never, SettlementStatus, User,
};
use crate::entity::{DirectoryConfig, EntityCapacity, EntityId, EntityMetrics, EntityShutdown};
use crate::{
    ActorCustody, ActorFailureAssessment, ActorNotificationReceipts, ActorRetirement, Application,
    Completion, MailAddr, RetirementAssessment, ShutdownLedger, WorkOutcome,
};
use behavior_actors::StopOnShutdown;
use core::future::Ready;
use std::cell::Cell;
use std::num::NonZeroUsize;
use std::ptr;
use std::rc::Rc;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::sync::oneshot;
use tokio::time::timeout;

fn ledger_custody(
    native: &ActorRetirement<StopOnShutdown<ShutdownLedger>, Never, Vec<Never>>,
) -> &ActorCustody<StopOnShutdown<ShutdownLedger>, Never, Vec<Never>> {
    let custody = match native {
        ActorRetirement::Completed {
            custody,
            settlements,
            completion,
            interpretation: None,
            source: None,
        } => {
            assert_eq!(*completion, Completion::Stopped);
            assert_eq!(settlements.settlement_status(), SettlementStatus::Accepted);
            assert_eq!(custody.control.len(), 0);
            // A completed actor may retain an unread owner cancellation; its
            // exact original remains in this native report alongside the state.
            custody
        }
        ActorRetirement::OwnerCancelled {
            custody,
            settlements,
            interpretation: None,
            source: None,
        } => {
            assert_eq!(settlements.len(), 0);
            assert!(matches!(custody.control.as_slice(), [EventLayer::Owned(_)]));
            assert!(custody.unread_owner_cancellation.is_none());
            custody
        }
        _ => panic!("the same family incarnation stops or receives its actual owner cancellation"),
    };
    assert_eq!(custody.descendants.len(), 0);
    assert_eq!(custody.child_failures.len(), 0);
    assert!(custody.capability_failures.is_empty());
    assert!(custody.additional_failures.is_empty());
    assert!(custody.received_interpretation.is_none());
    assert!(custody.received_source.is_none());
    assert!(custody.source_index.is_none());
    assert!(custody.acquired_ingress.is_none());
    assert!(custody.retirement_failures.is_empty());
    assert!(custody.terminal_report.is_none());
    custody
}

fn assert_account_retirement(reports: &Arc<Mutex<Vec<AccountReport>>>, sender: MailAddr) {
    let reports = reports
        .lock()
        .expect("original family reports remain owned");
    let [AccountReport::Retired(id, _activation, native, report)] = reports.as_slice() else {
        panic!("one exact family native result and no refusal or forced retirement");
    };
    assert_eq!(id, &EntityId::new(Box::<[u64]>::from([101, 103])));
    let native = native.as_ref().expect("the original family actor joins");
    let custody = ledger_custody(native);
    assert_eq!(custody.behavior.base().entries, [101, 103]);
    assert_eq!(
        custody.behavior.base().received,
        [
            User::new(sender, vec![107, 109]),
            User::new(sender, vec![108, 110])
        ]
    );
    assert_eq!(custody.user.len(), 0);
    assert_eq!(report.retirement(), RetirementAssessment::Established);
    assert_eq!(report.failures(), ActorFailureAssessment::NoFailuresFound);
}

#[tokio::test(flavor = "current_thread")]
#[expect(
    clippy::too_many_lines,
    reason = "one absent invocation and complete root and nonempty-family native trace"
)]
async fn absent_work_drives_root_and_nonempty_family_retirement() {
    timeout(Duration::from_secs(10), async {
        let reports = Arc::new(Mutex::new(Vec::new()));
        let calls = Cell::new(0);
        let commands = Rc::new(vec![107_u64, 109]);
        let original = vec![127_u64, 131];
        let allocation = original.as_ptr();
        let mut family_admissions = None;
        let mut family_origin = None;
        let mut root_admission = None;
        let mut root_sender = None;
        let mut handoff = None;
        let acquired_family_admissions = &mut family_admissions;
        let acquired_family_origin = &mut family_origin;
        let acquired_root_admission = &mut root_admission;
        let acquired_root_sender = &mut root_sender;
        let invocations = &calls;
        let account_commands = &commands;
        let (published, active) = oneshot::channel();
        let (finished, export_finished) = oneshot::channel();
        let application = Application::new(
            ShutdownLedger {
                entries: vec![113],
                received: Vec::new(),
            }
            .stop_on_shutdown(),
        )
        .entity_family(
            AccountsRole,
            Accounts {
                reports: Arc::clone(&reports),
            },
            DirectoryConfig::default(),
            EntityCapacity::new(NonZeroUsize::new(1).unwrap(), NonZeroUsize::new(2).unwrap()),
        );
        let (execution, (receiving, received_families)) = application
            .execute_with_entities_invocation(
                ApplicationWorkPresence::<Never, ()>::Absent(()),
                |presence: ApplicationWorkPresence<Never, ()>, application| {
                    match presence {
                        ApplicationWorkPresence::Absent(()) => {}
                        ApplicationWorkPresence::Supplied(never) => match never {},
                    }
                    invocations.set(invocations.get() + 1);
                    handoff = Some(published.send(application.clone()));
                    let exporting = async move {
                        let account = application
                            .entities(AccountsRole)
                            .entity(Box::<[u64]>::from([101, 103]));
                        let account = application
                            .sender_to(account)
                            .unwrap_or_else(|_| panic!("the actual Entity sender allocates"));
                        *acquired_family_origin = Some(account.address());
                        let first = account.send((**account_commands).clone()).await;
                        let second = account
                            .send(account_commands.iter().map(|value| value + 1).collect())
                            .await;
                        *acquired_family_admissions = Some((first, second));
                        let sender = application
                            .sender()
                            .expect("the genuine one-way root sender allocates");
                        *acquired_root_sender = Some(sender.address());
                        *acquired_root_admission = Some(sender.send(original).await);
                        let _published = finished.send(());
                    };
                    (
                        ApplicationWorkPresence::<Ready<Never>, ()>::Absent(()),
                        Some(exporting),
                    )
                },
            )
            .unwrap_or_else(|_| panic!("the actual host is entered"));
        let observer = async {
            let application = active
                .await
                .expect("actual startup transfers independent shutdown authority");
            let finished = export_finished.await;
            let stopped = application.request_shutdown();
            let report = application.lifecycle().retirement().await;
            (finished, stopped, report)
        };
        let ((), observer) = tokio::join!(execution, observer);
        let (work, native, notifications) = receiving.await;
        let families = received_families.await;
        match work {
            WorkOutcome::Completed {
                output: ApplicationWorkPresence::Absent(()),
                cleanup: Ok(()),
            } => (),
            original => {
                drop(original);
                panic!("actual absent invocation and cleanup must complete");
            }
        }
        let (finished, stopped, report) = observer;
        let (family, ()) = families;
        let (_role, family) = family.expect("the exact nonempty family retirement is received");
        let (shutdown, metrics, disposal) = family.into_parts();
        let EntityShutdown::Settled {
            represented,
            entities,
        } = shutdown
        else {
            panic!("the actual nonempty family shutdown settles");
        };
        let (origin, native) =
            native.expect("the complete original root native result remains owned");
        assert_eq!(origin.address(), MailAddr::APPLICATION_ROOT);
        assert!(matches!(
            native,
            ActorRetirement::Completed {
                completion: Completion::Stopped,
                ..
            }
        ));
        let root = ledger_custody(&native);
        let handoff = handoff.expect("startup attempted one actual authority transfer");
        let (first, second) =
            family_admissions.expect("both actual family admissions were attempted");
        let sender = family_origin.expect("the actual Entity sender identity remains owned");
        let root_sender = root_sender.expect("the actual root sender identity remains owned");
        assert!(handoff.is_ok());
        assert_eq!(calls.get(), 1);
        finished.expect("the independently driven export finishes normally");
        stopped.expect("the exact root accepts independent shutdown");
        assert_eq!(report.retirement(), RetirementAssessment::Established);
        assert_eq!(report.failures(), ActorFailureAssessment::NoFailuresFound);
        first.expect("the first original family command is admitted");
        second.expect("the second original family command is admitted");
        root_admission
            .expect("the actual root command was attempted")
            .expect("the root command is admitted");
        assert!(matches!(
            notifications,
            Ok(ActorNotificationReceipts {
                termination: Ok(()),
                retirement: Ok(())
            })
        ));
        assert_eq!(root.behavior.base().entries, [113]);
        // Admission is independent of processing. Exact shutdown can leave this
        // accepted original unread; conserve both possible native dispositions.
        let (([original], []) | ([], [original])) = (
            root.behavior.base().received.as_slice(),
            root.user.as_slice(),
        ) else {
            panic!("exactly one admitted original remains in native custody");
        };
        let User { from, message } = original;
        assert_eq!(*from, root_sender);
        assert!(ptr::eq(message.as_ptr(), allocation));
        assert_eq!(message, &[127, 131]);
        assert_eq!(represented, 1);
        assert_eq!(entities, [EntityId::new(Box::<[u64]>::from([101, 103]))]);
        assert_eq!(
            metrics,
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
        assert!(disposal.is_none());
        assert_account_retirement(&reports, sender);
    })
    .await
    .expect("the genuine invocation and complete native family cleanup finish within the watchdog");
}
