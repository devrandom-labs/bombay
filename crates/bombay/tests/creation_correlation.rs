#![cfg(test)]
use bombay::ActorNotificationReceipts;
use bombay::actors::ActorExt;
use bombay::behavior::{
    Actions, BehaviorActed, ClassifySettlement, CreateChild, CreationKind, CreationRejection,
    CreationSequence, Creations, Never, NoSends, SettlementStatus, Step, Stopped,
};
use bombay::prelude::StopOnShutdown;
use bombay::{
    ActorCustody, ActorRetirement, Application, MailAddr, SettlementFailure, WorkOutcome,
};

struct Worker {
    payload: Box<[u8]>,
}

#[bombay::actor(message = Never)]
impl Worker {}

type ManagedWorker = StopOnShutdown<Worker>;

struct Creator {
    children: Option<Creations<CreateChild<MailAddr, ManagedWorker>>>,
}

#[derive(Debug)]
enum CreatorError {
    ChildrenAlreadyTransferred,
}

#[bombay::actor(
    message = Never,
    error = CreatorError,
    births = { worker: ManagedWorker },
    creation_settlements = retain_for_retirement,
)]
impl Creator {
    fn init(&mut self) -> BehaviorActed<Self> {
        let children = self
            .children
            .take()
            .ok_or(CreatorError::ChildrenAlreadyTransferred)?;
        Ok(Actions::new(NoSends, children, Step::Stop(Stopped)))
    }
}

bombay::topology! {
    native_application {
        root { Worker => worker; }
        worker {}
    }
}
#[tokio::test(flavor = "current_thread")]
#[expect(
    clippy::too_many_lines,
    reason = "one native creation and joined retirement trace preserves both original workers and every independent custody lane"
)]
async fn recorded_creation_correlation_rejects_the_original_child_and_keeps_later_births() {
    let mut sequence = CreationSequence::new();
    let first_id = sequence.issue().expect("two fresh requests fit");
    let second_id = sequence.issue().expect("later fresh request fits");
    for rejected_kind in [CreationKind::Birth, CreationKind::replacement(first_id)] {
        let first: Box<[u8]> = Box::from([2, 5]);
        let first_allocation = first.as_ptr();
        let second: Box<[u8]> = Box::from([11, 19]);
        let second_allocation = second.as_ptr();
        let rejected: Box<[u8]> = Box::from([23, 29]);
        let rejected_allocation = rejected.as_ptr();
        let replay: Box<[u8]> = Box::from([31, 37]);
        let replay_allocation = replay.as_ptr();
        let children = Creations::one(CreateChild::birth(
            first_id,
            Worker { payload: first }.stop_on_shutdown(),
        ))
        .and(match rejected_kind {
            CreationKind::Birth => {
                CreateChild::birth(first_id, Worker { payload: rejected }.stop_on_shutdown())
            }
            CreationKind::Replacement { previous } => CreateChild::replacement(
                first_id,
                previous,
                Worker { payload: rejected }.stop_on_shutdown(),
            ),
        })
        .and(CreateChild::birth(
            first_id,
            Worker { payload: replay }.stop_on_shutdown(),
        ))
        .and(CreateChild::birth(
            second_id,
            Worker { payload: second }.stop_on_shutdown(),
        ));
        let outcome = Application::with_topology(
            Creator {
                children: Some(children),
            }
            .stop_on_shutdown(),
            native_application::Topology::new(),
        )
        .run()
        .await
        .unwrap_or_else(|original| {
            drop(original);
            panic!("the native host is entered");
        })
        .into_parts();
        let (
            WorkOutcome::NotInvoked {
                work: None,
                startup_error: Some(_),
                cleanup: Ok(()),
            },
            _root_report,
            Ok((root_origin, retirement)),
            Ok(ActorNotificationReceipts {
                termination: Ok(()),
                retirement: Ok(()),
            }),
            (),
        ) = outcome
        else {
            drop(outcome);
            panic!("the creator stops after its complete initialization effects");
        };
        assert_eq!(root_origin.address(), MailAddr::APPLICATION_ROOT);
        let ActorRetirement::EffectsFailed {
            custody,
            settlements,
            initialization,
            error,
            interpretation,
            source,
        } = retirement
        else {
            panic!("creator retirement owns both children");
        };
        assert_eq!(error, SettlementFailure::Rejected);
        assert!(initialization.is_none());
        assert!(interpretation.is_none());
        assert!(source.is_none());
        let ActorCustody {
            behavior,
            control,
            user,
            descendants,
            child_failures: failures,
            additional_failures,
            received_interpretation,
            received_source,
            source_index,
            acquired_ingress,
            retirement_failures,
            terminal_report,
            capability_failures,
            unread_owner_cancellation,
        } = custody;
        let creator = behavior.into_inner();
        assert!(creator.children.is_none());
        assert_eq!(control, []);
        assert_eq!(user, []);
        assert!(failures.is_empty());
        assert!(additional_failures.is_empty());
        assert!(received_interpretation.is_none());
        assert!(received_source.is_none());
        assert!(source_index.is_none());
        assert!(acquired_ingress.is_none());
        assert!(retirement_failures.is_empty());
        assert!(terminal_report.is_none());
        assert!(capability_failures.is_empty());
        assert!(unread_owner_cancellation.is_none());
        let status = settlements.settlement_status();
        assert_eq!(status, SettlementStatus::Rejected);
        let mut root_rows = settlements.into_iter();
        let root_row = root_rows
            .next()
            .expect("one original complete initialization row");
        let remainder = root_rows.next();
        assert!(remainder.is_none());
        let bombay::behavior::CreationSettlement::Settled(creations) =
            root_row.creations.into_settlement()
        else {
            panic!("the complete four-request creation batch is routed");
        };
        let creations = creations.into_iter().collect::<Vec<_>>();
        assert_eq!(creations.len(), 4);
        let mut creations = creations.into_iter();
        let first = creations.next().expect("the first child is accepted");
        assert!(matches!(
            first,
            bombay::behavior::SettledItem::Attempted(bombay::behavior::ItemSettlement::Accepted(_))
        ));
        let rejected = creations.next().expect("the exact duplicate is rejected");
        let bombay::behavior::SettledItem::Attempted(bombay::behavior::ItemSettlement::Rejected {
            item,
            reason,
        }) = rejected
        else {
            panic!("the original duplicate request must be rejected without unwinding");
        };
        let (request, _) = item.into_parts();
        let (id, worker, kind) = request.into_parts();
        assert_eq!(id, first_id);
        assert_eq!(kind, rejected_kind);
        let rejected = worker.into_inner().payload;
        assert_eq!(rejected.as_ptr(), rejected_allocation);
        assert_eq!(rejected.as_ref(), [23, 29]);
        assert_eq!(reason, CreationRejection::CorrelationAlreadyRecorded);
        let replay = creations.next().expect("replay is rejected independently");
        let bombay::behavior::SettledItem::Attempted(bombay::behavior::ItemSettlement::Rejected {
            item,
            reason,
        }) = replay
        else {
            panic!("replay cannot rebind the recorded correlation");
        };
        let (request, _) = item.into_parts();
        let (id, worker, kind) = request.into_parts();
        assert_eq!(id, first_id);
        assert_eq!(kind, CreationKind::Birth);
        let replay = worker.into_inner().payload;
        assert_eq!(replay.as_ptr(), replay_allocation);
        assert_eq!(replay.as_ref(), [31, 37]);
        assert_eq!(reason, CreationRejection::CorrelationAlreadyRecorded);
        let later = creations
            .next()
            .expect("the independent later child is attempted");
        assert!(matches!(
            later,
            bombay::behavior::SettledItem::Attempted(bombay::behavior::ItemSettlement::Accepted(_))
        ));
        assert_eq!(descendants.len(), 2);
        let mut actual = Vec::new();
        for native_application::RootRetirements { origin, retirement } in descendants {
            let ActorRetirement::OwnerCancelled {
                custody,
                settlements,
                interpretation,
                source,
            } = *retirement
            else {
                panic!("the parent joins each original cancelled child");
            };
            assert!(interpretation.is_none());
            assert!(source.is_none());
            let ActorCustody {
                behavior,
                control,
                user,
                descendants,
                child_failures,
                additional_failures,
                received_interpretation,
                received_source,
                source_index,
                acquired_ingress,
                retirement_failures,
                terminal_report,
                capability_failures,
                unread_owner_cancellation,
            } = custody;
            assert_eq!(control, []);
            assert_eq!(user, []);
            assert_eq!(descendants, []);
            assert_eq!(child_failures, []);
            assert!(additional_failures.is_empty());
            assert!(received_interpretation.is_none());
            assert!(received_source.is_none());
            assert!(source_index.is_none());
            assert!(acquired_ingress.is_none());
            assert!(retirement_failures.is_empty());
            assert!(terminal_report.is_none());
            assert!(capability_failures.is_empty());
            assert!(unread_owner_cancellation.is_none());
            assert_eq!(settlements.len(), 0);
            assert_eq!(origin.creation_kind(), CreationKind::Birth);
            assert_ne!(origin.address(), MailAddr::APPLICATION_ROOT);
            let payload = behavior.into_inner().payload;
            actual.push((
                origin.creation_id(),
                origin.address(),
                payload.as_ptr(),
                payload,
            ));
        }
        let mut actual = actual.into_iter();
        let (id, address, allocation, payload) = actual.next().expect("first original child");
        assert_eq!(id, first_id);
        assert_eq!(allocation, first_allocation);
        assert_eq!(&*payload, &[2, 5]);
        let (id, second_address, allocation, payload) =
            actual.next().expect("later original sibling");
        assert_eq!(id, second_id);
        assert_eq!(allocation, second_allocation);
        assert_eq!(&*payload, &[11, 19]);
        assert_ne!(address, second_address);
        let remaining = actual.next();
        assert!(remaining.is_none());
    }
}
