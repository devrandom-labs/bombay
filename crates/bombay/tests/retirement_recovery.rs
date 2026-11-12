use bombay::actors::ActorExt as _;
use bombay::behavior::{
    Actions, AllocationRejection, BehaviorActed, ClassifySettlement as _, Never, SettlementStatus,
};
use bombay::{
    ActorFailureAssessment, ActorRetirement, Application, Completion, MailboxCapacity,
    RetirementAssessment, RetirementOutcome,
};

struct Counter {
    value: u64,
    audit: Box<[u8]>,
}

enum CounterCommand {
    Increment(Box<[u8]>),
}

enum CounterError {
    Overflow(Box<[u8]>),
}

#[bombay::actor(error = CounterError)]
impl Counter {
    fn receive(&mut self, command: CounterCommand) -> BehaviorActed<Self> {
        let CounterCommand::Increment(audit) = command;
        match self.value.checked_add(1) {
            Some(value) => {
                self.value = value;
                Ok(Actions::cont())
            }
            None => Err(CounterError::Overflow(audit)),
        }
    }
}

#[tokio::test(flavor = "current_thread")]
async fn overflow_returns_the_original_counter_command_and_completed_work() {
    let actor_audit = vec![3_u8, 5].into_boxed_slice();
    let actor_pointer = actor_audit.as_ptr();
    let command_audit = vec![7_u8, 11].into_boxed_slice();
    let command_pointer = command_audit.as_ptr();
    let output = vec![13_u8, 17].into_boxed_slice();
    let output_pointer = output.as_ptr();
    let outcome = Application::new(Counter {
        value: u64::MAX,
        audit: actor_audit,
    })
    .run_with(move |application| async move {
        let sender = application
            .sender()
            .expect("the counter sender is established");
        sender
            .send(CounterCommand::Increment(command_audit))
            .await
            .unwrap_or_else(|_| panic!("the original increment is admitted"));
        let observed = application.termination().await;
        (output, observed)
    })
    .await
    .unwrap_or_else(|_| panic!("the caller's native host exists"));
    let ((output, observed), retirement) = outcome
        .try_into_completed()
        .unwrap_or_else(|_| panic!("actor failure preserves completed business work"));
    assert_eq!((output.as_ptr(), &*output), (output_pointer, &[13, 17][..]));
    assert!(observed.is_err());
    let report = retirement
        .root_report()
        .expect("the joined root publishes its report");
    assert_eq!(report.retirement(), RetirementAssessment::Established);
    assert_eq!(report.failures(), ActorFailureAssessment::FailuresFound);
    let (cleanup, retained_report, root, notifications, families) = retirement.into_parts();
    assert!(cleanup.is_ok());
    assert_eq!(retained_report, Some(report));
    assert_eq!(families, ());
    let notifications = notifications.expect("both original notification results are received");
    assert!(notifications.termination.is_ok());
    assert!(notifications.retirement.is_ok());
    let (_origin, root) = root.expect("the exact native retirement is received");
    let Some(RetirementOutcome::BehaviorFailed {
        error: CounterError::Overflow(borrowed_command),
        ..
    }) = root.outcome()
    else {
        panic!("the borrowed overflow is available without custody extraction");
    };
    assert_eq!(
        (borrowed_command.as_ptr(), borrowed_command.as_ref()),
        (command_pointer, &[7, 11][..])
    );
    let (custody, outcome) = root
        .try_into_custody()
        .unwrap_or_else(|_| panic!("failure still returns the complete original actor custody"));
    let RetirementOutcome::BehaviorFailed {
        interpretation,
        source,
        settlements,
        error: CounterError::Overflow(command_audit),
    } = outcome
    else {
        panic!("the original overflow is the primary actor failure");
    };
    assert!(interpretation.is_none());
    assert!(source.is_none());
    assert_eq!(settlements, []);
    assert_eq!(custody.behavior.value, u64::MAX);
    assert_eq!(
        (custody.behavior.audit.as_ptr(), &*custody.behavior.audit),
        (actor_pointer, &[3, 5][..])
    );
    assert_eq!(
        (command_audit.as_ptr(), &*command_audit),
        (command_pointer, &[7, 11][..])
    );
    assert!(custody.control.is_empty());
    assert!(custody.user.is_empty());
    assert_eq!(custody.descendants, []);
    assert_eq!(custody.child_failures, []);
    assert!(custody.capability_failures.is_empty());
    assert!(custody.additional_failures.is_empty());
    assert!(custody.received_interpretation.is_none());
    assert!(custody.received_source.is_none());
    assert!(custody.source_index.is_none());
    assert!(custody.acquired_ingress.is_none());
    assert!(custody.retirement_failures.is_empty());
    assert!(custody.terminal_report.is_none());
    assert!(custody.unread_owner_cancellation.is_none());
}

#[tokio::test(flavor = "current_thread")]
async fn overflow_returns_the_unread_command_with_its_original_sender() {
    let actor_audit = vec![53_u8, 59].into_boxed_slice();
    let actor_pointer = actor_audit.as_ptr();
    let failed_audit = vec![61_u8, 67].into_boxed_slice();
    let failed_pointer = failed_audit.as_ptr();
    let unread_audit = vec![71_u8, 73].into_boxed_slice();
    let unread_pointer = unread_audit.as_ptr();
    let output = vec![79_u8, 83].into_boxed_slice();
    let output_pointer = output.as_ptr();
    let outcome = Application::new(Counter {
        value: u64::MAX,
        audit: actor_audit,
    })
    .mailbox_capacity(MailboxCapacity::new(2).expect("both original commands fit"))
    .run_with(move |application| async move {
        let first = application
            .sender()
            .expect("the first sender is established");
        let second = application
            .sender()
            .expect("the second sender is established");
        let from = second.address();
        assert_ne!(first.address(), from);
        first
            .try_send(CounterCommand::Increment(failed_audit))
            .unwrap_or_else(|_| panic!("the overflowing command is admitted"));
        second
            .try_send(CounterCommand::Increment(unread_audit))
            .unwrap_or_else(|_| panic!("the later command is admitted before execution"));
        let observed = application.termination().await;
        (output, from, observed)
    })
    .await
    .unwrap_or_else(|_| panic!("the caller's native host exists"));
    let ((output, from, observed), retirement) = outcome
        .try_into_completed()
        .unwrap_or_else(|_| panic!("actor failure preserves completed business work"));
    assert_eq!((output.as_ptr(), &*output), (output_pointer, &[79, 83][..]));
    assert!(observed.is_err());
    let report = retirement.root_report().expect("the joined report remains");
    assert_eq!(report.retirement(), RetirementAssessment::Established);
    assert_eq!(report.failures(), ActorFailureAssessment::FailuresFound);
    let (cleanup, retained_report, root, notifications, families) = retirement.into_parts();
    assert!(cleanup.is_ok());
    assert_eq!(retained_report, Some(report));
    assert_eq!(families, ());
    let notifications = notifications.expect("both notification results remain");
    assert!(notifications.termination.is_ok());
    assert!(notifications.retirement.is_ok());
    let (_origin, root) = root.expect("the original native retirement remains");
    let (mut custody, outcome) = root
        .try_into_custody()
        .unwrap_or_else(|_| panic!("failure returns complete original custody"));
    let RetirementOutcome::BehaviorFailed {
        interpretation,
        source,
        settlements,
        error: CounterError::Overflow(failed_audit),
    } = outcome
    else {
        panic!("the first command's overflow remains the primary failure");
    };
    assert!(interpretation.is_none());
    assert!(source.is_none());
    assert_eq!(settlements, []);
    assert_eq!(custody.behavior.value, u64::MAX);
    assert_eq!(
        (custody.behavior.audit.as_ptr(), &*custody.behavior.audit),
        (actor_pointer, &[53, 59][..])
    );
    assert_eq!(
        (failed_audit.as_ptr(), &*failed_audit),
        (failed_pointer, &[61, 67][..])
    );
    let unread = custody
        .user
        .pop()
        .expect("the original queued User remains");
    assert_eq!(unread.from, from);
    let CounterCommand::Increment(unread_audit) = unread.message;
    assert_eq!(
        (unread_audit.as_ptr(), &*unread_audit),
        (unread_pointer, &[71, 73][..])
    );
    assert!(custody.control.is_empty());
    assert!(custody.user.is_empty());
    assert_eq!(custody.descendants, []);
    assert_eq!(custody.child_failures, []);
    assert!(custody.capability_failures.is_empty());
    assert!(custody.additional_failures.is_empty());
    assert!(custody.received_interpretation.is_none());
    assert!(custody.received_source.is_none());
    assert!(custody.source_index.is_none());
    assert!(custody.acquired_ingress.is_none());
    assert!(custody.retirement_failures.is_empty());
    assert!(custody.terminal_report.is_none());
    assert!(custody.unread_owner_cancellation.is_none());
}

#[tokio::test(flavor = "current_thread")]
async fn actual_shutdown_returns_the_whole_nondefault_wrapped_counter() {
    let actor_audit = vec![19_u8, 23].into_boxed_slice();
    let actor_pointer = actor_audit.as_ptr();
    let output = vec![29_u8, 31].into_boxed_slice();
    let output_pointer = output.as_ptr();
    let outcome = Application::new(
        Counter {
            value: 37,
            audit: actor_audit,
        }
        .stop_on_shutdown(),
    )
    .run_with(move |application| async move {
        application
            .shutdown()
            .expect("the real shutdown request is admitted");
        output
    })
    .await
    .unwrap_or_else(|_| panic!("the caller's native host exists"));
    let (output, retirement) = outcome
        .try_into_completed()
        .unwrap_or_else(|_| panic!("completed work remains owned after shutdown"));
    assert_eq!((output.as_ptr(), &*output), (output_pointer, &[29, 31][..]));
    let report = retirement
        .root_report()
        .expect("the joined root publishes its report");
    assert_eq!(report.retirement(), RetirementAssessment::Established);
    assert_eq!(report.failures(), ActorFailureAssessment::NoFailuresFound);
    let (cleanup, retained_report, root, notifications, families) = retirement.into_parts();
    assert!(cleanup.is_ok());
    assert_eq!(retained_report, Some(report));
    assert_eq!(families, ());
    let notifications = notifications.expect("both notification results remain received");
    assert!(notifications.termination.is_ok());
    assert!(notifications.retirement.is_ok());
    let (_origin, root) = root.expect("the complete native retirement is received");
    let Some(RetirementOutcome::Completed { completion, .. }) = root.outcome() else {
        panic!("the wrapped actor's normal outcome remains available to borrow");
    };
    assert_eq!(completion, &Completion::Stopped);
    let (custody, outcome) = root
        .try_into_custody()
        .unwrap_or_else(|_| panic!("actual shutdown returns the whole wrapped actor"));
    let RetirementOutcome::Completed {
        interpretation,
        source,
        settlements,
        completion,
    } = outcome
    else {
        panic!("the real shutdown stopped this counter");
    };
    assert_eq!(completion, Completion::Stopped);
    assert!(interpretation.is_none());
    assert!(source.is_none());
    assert_eq!(settlements.len(), 1);
    assert_eq!(settlements.settlement_status(), SettlementStatus::Accepted);
    let actor = custody.behavior.into_inner();
    assert_eq!(actor.value, 37);
    assert_eq!(
        (actor.audit.as_ptr(), &*actor.audit),
        (actor_pointer, &[19, 23][..])
    );
    assert!(custody.control.is_empty());
    assert!(custody.user.is_empty());
    assert_eq!(custody.descendants, []);
    assert_eq!(custody.child_failures, []);
    assert!(custody.capability_failures.is_empty());
    assert!(custody.additional_failures.is_empty());
    assert!(custody.received_interpretation.is_none());
    assert!(custody.received_source.is_none());
    assert!(custody.source_index.is_none());
    assert!(custody.acquired_ingress.is_none());
    assert!(custody.retirement_failures.is_empty());
    assert!(custody.terminal_report.is_none());
    assert!(custody.unread_owner_cancellation.is_none());
}

#[test]
fn allocation_refusal_returns_the_original_counter_without_native_custody() {
    for reason in [
        AllocationRejection::Exhausted,
        AllocationRejection::AddressAlreadyClaimed,
    ] {
        let audit = vec![41_u8, 43].into_boxed_slice();
        let pointer = audit.as_ptr();
        let retirement = ActorRetirement::<Counter, Never, Vec<Never>>::AllocationRejected {
            behavior: Counter { value: 47, audit },
            reason,
        };
        assert!(retirement.outcome().is_none());
        let Err(returned) = retirement.try_into_custody() else {
            panic!("allocation refusal established no native custody");
        };
        let ActorRetirement::AllocationRejected {
            behavior,
            reason: original_reason,
        } = returned
        else {
            panic!("the original allocation refusal remains exact");
        };
        assert_eq!(original_reason, reason);
        assert_eq!(behavior.value, 47);
        assert_eq!(
            (behavior.audit.as_ptr(), &*behavior.audit),
            (pointer, &[41, 43][..])
        );
    }
}

#[test]
fn unreturned_panic_stays_unreturned_without_fabricated_actor_state() {
    let retirement = ActorRetirement::<Counter, Never, Vec<Never>>::Panicked;
    assert!(retirement.outcome().is_none());
    let Err(returned) = retirement.try_into_custody() else {
        panic!("unreturned panic cannot fabricate an actor");
    };
    assert!(matches!(returned, ActorRetirement::Panicked));
}

#[test]
fn unreturned_cancellation_stays_unreturned_without_fabricated_actor_state() {
    let retirement = ActorRetirement::<Counter, Never, Vec<Never>>::Cancelled;
    assert!(retirement.outcome().is_none());
    let Err(returned) = retirement.try_into_custody() else {
        panic!("unreturned cancellation cannot fabricate an actor");
    };
    assert!(matches!(returned, ActorRetirement::Cancelled));
}

#[tokio::test(flavor = "current_thread")]
async fn executor_failure_returns_its_original_task_failure_without_actor_state() {
    let failure = tokio::spawn(async { panic!("the original executor failure") })
        .await
        .expect_err("this real task fails");
    let task = failure.id();
    let retirement = ActorRetirement::<Counter, Never, Vec<Never>>::ActorTaskFailed(failure);
    assert!(retirement.outcome().is_none());
    let Err(returned) = retirement.try_into_custody() else {
        panic!("executor failure cannot fabricate native custody");
    };
    let ActorRetirement::ActorTaskFailed(failure) = returned else {
        panic!("the exact original task failure remains owned");
    };
    assert_eq!(failure.id(), task);
    assert!(failure.is_panic());
}
