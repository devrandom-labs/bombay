use bombay::actors::ActorExt;
use bombay::behavior::{
    Actions, BehaviorActed, ClassifySettlement, CreateChild, CreationKind, CreationSequence,
    Creations, Never, NoSends, SettlementStatus, Step, Stopped,
};
use bombay::lifecycle::StopOnShutdown;
use bombay::{
    ActorCustody, ActorRetirement, Application, ApplicationOutcome, Completion, MailAddr,
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
async fn leaf_retirement_keeps_its_empty_descendants_inside_a_nonempty_report_family() {
    let first: Box<[u8]> = Box::from([2, 5]);
    let first_allocation = first.as_ptr();
    let second: Box<[u8]> = Box::from([11, 19]);
    let second_allocation = second.as_ptr();
    let mut sequence = CreationSequence::new();
    let first_id = sequence.issue().expect("two fresh requests fit");
    let second_id = sequence.issue().expect("two fresh requests fit");
    let children = Creations::one(CreateChild::birth(
        first_id,
        Worker { payload: first }.stop_on_shutdown(),
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
    });
    let ApplicationOutcome::NotInvoked {
        work: None,
        startup_error: Some(_),
        cleanup: Ok((root_origin, retirement)),
    } = outcome
    else {
        drop(outcome);
        panic!("the creator stops after its complete initialization effects");
    };
    assert_eq!(root_origin.address(), MailAddr::APPLICATION_ROOT);
    let ActorRetirement::Completed {
        custody,
        settlements,
        completion,
        interpretation,
        source,
    } = retirement
    else {
        panic!("creator retirement owns both children");
    };
    assert_eq!(completion, Completion::Stopped);
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
    assert_eq!(status, SettlementStatus::Accepted);
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
    let (id, second_address, allocation, payload) = actual.next().expect("later original sibling");
    assert_eq!(id, second_id);
    assert_eq!(allocation, second_allocation);
    assert_eq!(&*payload, &[11, 19]);
    assert_ne!(address, second_address);
    let remaining = actual.next();
    assert!(remaining.is_none());
}
