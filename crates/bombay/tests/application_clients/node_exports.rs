//! Public remote consumer cohort. This first control validates its concrete actors/Hosts.
use super::counter::{CounterMessage, CounterProcessed, CounterValue};
use super::ledger::{LedgerCommand, Receipts};
use actors::{ActorExports, CounterReport, Counters, CountersRole, Creator, Exports, topology};
use bombay::actors::ActorExt;
use bombay::behavior::{
    BehaviorBase, BehaviorSettlements, ChildChoice, ChildCreationOutcome, ClassifySettlement,
    CreationSettlement, ItemSettlement, Never, Protocol, SettledItem, SettlementStatus,
};
use bombay::entity::{DirectoryConfig, EntityCapacity, EntityId, EntityMetrics, EntityShutdown};
use bombay::prelude::{ActorRetirement, Completion, MailAddr};
use bombay::{ActorFailureAssessment, ApplicationScope, RetirementAssessment};
use std::num::NonZeroUsize;
use std::sync::{Arc, Mutex};

#[path = "node_exports/actors.rs"]
mod actors;

#[tokio::test(flavor = "current_thread")]
#[expect(
    clippy::too_many_lines,
    reason = "one concrete child-proof, move-only command and nominal family fixture control"
)]
async fn combined_actor_exports_use_the_exact_family_hosts() {
    let reports = Arc::new(Mutex::new(Vec::new()));
    let scope = ApplicationScope::new();
    let mut exports = scope.external::<Exports>().unwrap();
    let mut processed = scope.external::<CounterProcessed>().unwrap();
    let mut values = scope.external::<CounterValue>().unwrap();
    let mut receipts = scope.external::<Receipts>().unwrap();
    let original = Box::<[u8]>::from([227, 229]);
    let allocation = original.as_ptr();
    let application = scope
        .with_topology(
            Creator::default().stop_on_shutdown(),
            topology::Topology::new(),
        )
        .entity_family(
            CountersRole,
            Counters {
                reports: Arc::clone(&reports),
            },
            DirectoryConfig::default(),
            EntityCapacity::new(NonZeroUsize::new(1).unwrap(), NonZeroUsize::new(2).unwrap()),
        );
    let outcome = application
        .run_with_entities(|application| async move {
            let export_admission = exports
                .send(
                    &application.root().established_recipient(),
                    exports.recipient(),
                )
                .await;
            let exported = exports.receive().await;
            let ActorExports {
                counter,
                sibling,
                ledger,
            } = &exported
                .as_ref()
                .expect("actual Creator Actions reply")
                .message;
            let increment = processed
                .send(
                    &counter.recipient(),
                    CounterMessage::IncrementAndReply {
                        request_sequence: 233,
                        reply_to: processed.recipient(),
                    },
                )
                .await;
            let incremented = processed.receive().await;
            let read = values
                .send(
                    &counter.recipient(),
                    CounterMessage::Read(values.recipient()),
                )
                .await;
            let value = values.receive().await;
            let sibling_read = values
                .send(
                    &sibling.recipient(),
                    CounterMessage::Read(values.recipient()),
                )
                .await;
            let sibling_value = values.receive().await;
            let append = receipts
                .send(&ledger.recipient(), LedgerCommand::Append(original))
                .await;
            let ledger_read = receipts
                .send(
                    &ledger.recipient(),
                    LedgerCommand::Read(receipts.recipient()),
                )
                .await;
            let ledger_reply = receipts.receive().await;
            let ledger_origin = receipts.address();
            let account = application.entities(CountersRole).entity(239);
            let interface = application.interface(());
            let mut family = interface
                .client_to::<_, CounterProcessed>(account)
                .unwrap_or_else(|_| {
                    panic!("the concrete compatible family reply client allocates")
                });
            let family_admission = family
                .send(CounterMessage::IncrementAndReply {
                    request_sequence: 241,
                    reply_to: family.recipient(),
                })
                .await;
            let family_reply = family.receive().await;
            let shutdown = application.request_shutdown();
            (
                export_admission,
                exported,
                increment,
                incremented,
                read,
                value,
                sibling_read,
                sibling_value,
                append,
                ledger_read,
                ledger_reply,
                ledger_origin,
                family_admission,
                family_reply,
                shutdown,
            )
        })
        .await
        .unwrap_or_else(|_| panic!("the concrete fixture's host is entered"));
    // Acquire every independent outcome before inspecting this fixture's observations.
    let (work, native, notifications, families) = outcome.into_parts();
    let (output, cleanup) = work
        .try_into_completed()
        .unwrap_or_else(|_| panic!("the fixture retains the complete supplied Work result"));
    let (origin, native) = native.expect("the concrete root retirement remains independent");
    let notifications = notifications.expect("both original notifications are acquired");
    let (family, ()) = families;
    let (_, family) = family.expect("the exact family shutdown is acquired");
    let (family_shutdown, family_metrics, family_disposal) = family.into_parts();
    let reports = Arc::try_unwrap(reports)
        .unwrap_or_else(|_| panic!("the joined family releases its definition"))
        .into_inner()
        .unwrap();
    let (
        export_admission,
        exported,
        increment,
        incremented,
        read,
        value,
        sibling_read,
        sibling_value,
        append,
        ledger_read,
        ledger_reply,
        ledger_origin,
        family_admission,
        family_reply,
        shutdown,
    ) = output;
    cleanup.expect("the actual application cleanup joins");
    notifications
        .termination
        .expect("ordinary termination was published");
    notifications
        .retirement
        .expect("joined retirement was published");
    assert!(export_admission.is_ok());
    let exported = exported.expect("the original child proof reply remains owned");
    assert_eq!(exported.from, origin.address());
    assert_ne!(
        exported.message.counter.recipient(),
        exported.message.sibling.recipient()
    );
    assert!(increment.is_ok() && read.is_ok() && sibling_read.is_ok());
    let incremented = incremented.expect("the actual target processed one correlated command");
    let value = value.expect("the actual target returns its current state");
    let sibling_value = sibling_value.expect("the independently live sibling returns its state");
    assert_eq!(incremented.message.request_sequence, 233);
    assert_eq!((value.from, value.message), (incremented.from, 1));
    assert_ne!(sibling_value.from, value.from);
    assert_eq!(sibling_value.message, 0);
    assert!(append.is_ok() && ledger_read.is_ok());
    let ledger_reply = ledger_reply.expect("the ledger returns before its actual terminal Read");
    assert_eq!(ledger_reply.message, [ledger_origin]);
    assert!(family_admission.is_ok());
    let family_reply =
        family_reply.expect("the actual hydrated family actor processes its command");
    assert_eq!(family_reply.message.request_sequence, 241);
    assert_ne!(family_reply.from, value.from);
    assert_eq!(shutdown, Ok(()));
    assert_eq!(origin.address(), MailAddr::APPLICATION_ROOT);
    assert_eq!(native.completion(), Some(&Completion::Stopped));
    assert_retirement(&native);
    let custody = native
        .custody()
        .expect("the complete Creator remains native");
    assert_eq!(custody.behavior.base().counters.len(), 2);
    assert!(custody.behavior.base().ledger.is_some());
    let [CreationSettlement::Settled(creations)] = custody.behavior.base().creations.as_slice()
    else {
        panic!("the one complete creation batch remains owned");
    };
    let committed = creations.iter().collect::<Vec<_>>();
    let [
        SettledItem::Attempted(ItemSettlement::Accepted(ChildChoice::Head(
            ChildCreationOutcome::Established(counter),
        ))),
        SettledItem::Attempted(ItemSettlement::Accepted(ChildChoice::Head(
            ChildCreationOutcome::Established(sibling),
        ))),
        SettledItem::Attempted(ItemSettlement::Accepted(ChildChoice::Tail(ChildChoice::Head(
            ChildCreationOutcome::Established(ledger),
        )))),
    ] = committed.as_slice()
    else {
        panic!("the actual first target, second sibling and third Ledger are committed");
    };
    assert_eq!(
        exported.message.counter.recipient(),
        counter.actor().recipient()
    );
    assert_eq!(
        exported.message.sibling.recipient(),
        sibling.actor().recipient()
    );
    assert_eq!(
        exported.message.ledger.recipient(),
        ledger.actor().recipient()
    );
    assert!(custody.child_failures.is_empty());
    assert_eq!(custody.descendants.len(), 3);
    let mut counter_origins = Vec::new();
    let mut ledger_origins = Vec::new();
    for child in &custody.descendants {
        match child {
            topology::RootRetirements::Counter { origin, retirement } => {
                assert_retirement(retirement);
                assert!(matches!(
                    retirement.as_ref(),
                    ActorRetirement::OwnerCancelled { .. }
                ));
                counter_origins.push(origin.address());
            }
            topology::RootRetirements::MailLedger { origin, retirement } => {
                assert_retirement(retirement);
                assert_eq!(retirement.completion(), Some(&Completion::Stopped));
                let ledger = retirement
                    .custody()
                    .expect("the actual completed Ledger survives");
                assert_eq!(ledger.behavior.entries.len(), 1);
                let [(from, bytes)] = ledger.behavior.entries.as_slice() else {
                    panic!("one actual move-only Append is retained");
                };
                assert_eq!(*from, ledger_origin);
                assert_eq!(bytes.as_ptr(), allocation);
                assert_eq!(&**bytes, &[227, 229]);
                ledger_origins.push(origin.address());
            }
        }
    }
    counter_origins.sort_unstable();
    let mut expected_counters = vec![value.from, sibling_value.from];
    expected_counters.sort_unstable();
    assert_eq!(counter_origins, expected_counters);
    assert_eq!(ledger_origins, [ledger_reply.from]);
    let EntityShutdown::Settled {
        represented,
        entities,
    } = family_shutdown
    else {
        panic!("the real nonempty family reaches its shutdown barrier");
    };
    assert_eq!(represented, 1);
    assert_eq!(entities, [EntityId::new(239)]);
    assert_eq!(
        family_metrics,
        EntityMetrics {
            activations: 1,
            hydration_failures: 0,
            launch_failures: 0,
            capacity_refusals: 0,
            forced_retirements: 0,
            peak_hydrations: 1,
            residents: 0
        }
    );
    assert!(family_disposal.is_none());
    let [CounterReport::Retired(id, _activation, retirement, report)] = reports.as_slice() else {
        panic!("the exact one activated family actor publishes its native retirement");
    };
    assert_eq!(*id.get(), 239);
    let retirement = retirement
        .as_ref()
        .expect("the original Entity actor task joins");
    // Graceful family retirement fences commands, then requests shutdown and cancels/joins
    // the same lease. Either lawful native disposition remains an original, distinct fact.
    assert_retirement(retirement);
    assert_eq!(report.retirement(), RetirementAssessment::Established);
    assert_eq!(report.failures(), ActorFailureAssessment::NoFailuresFound);
}

fn assert_retirement<Actor, Descendants, Failure>(
    retirement: &ActorRetirement<Actor, Descendants, Vec<Failure>>,
) where
    Actor: BehaviorSettlements<Protocol: Protocol<Addr = MailAddr>, Ph = Never>,
    Actor::Settlements: ClassifySettlement,
{
    let (ActorRetirement::Completed {
        custody,
        completion: Completion::Stopped,
        settlements,
        ..
    }
    | ActorRetirement::OwnerCancelled {
        custody,
        settlements,
        ..
    }) = retirement
    else {
        panic!("only actual stopped or owner-cancelled native actors are selected");
    };
    assert!(custody.child_failures.is_empty());
    assert!(custody.capability_failures.is_empty());
    assert!(custody.additional_failures.is_empty());
    assert!(custody.retirement_failures.is_empty());
    assert_eq!(settlements.settlement_status(), SettlementStatus::Accepted);
}
