//! Complete pure application actions; no runtime or effect interpretation.
//! Proposed destination: examples/remote-counter/tests/protocol.rs.
use bombay::ApplicationScope;
use bombay::actors::ActorExt;
use bombay::behavior::{BehaviorBase, EventLayer, Step, User, delegate_transition, initialize};
use bombay::testing::InfallibleResultExt;
use std::time::Duration;

#[path = "../../counter/src/counter.rs"]
#[expect(
    dead_code,
    reason = "the pure caller tests compile the real counter protocol without executing its separately tested actor"
)]
mod counter;
#[path = "../src/deployment.rs"]
#[expect(
    dead_code,
    reason = "the pure caller tests reuse operation types without performing assignment IO or verification"
)]
mod deployment;
#[path = "../src/protocol.rs"]
#[expect(
    dead_code,
    reason = "the pure caller tests exercise actions; separate process tests execute the real wire functions"
)]
mod protocol;
use deployment::Operation;
use protocol::{Caller, Journey, Outcome, Reply, Requests, Results, Unexpected, Value};

#[test]
fn replies_emit_the_complete_ordered_request_and_terminal_result_actions() {
    let scope = ApplicationScope::new();
    let requests = scope
        .external::<Requests>()
        .expect("actual typed request port");
    let results = scope
        .external::<Results>()
        .expect("actual typed domain-result port");
    let request_destination = requests.recipient();
    let result_destination = results.recipient();
    let mut caller = Caller {
        service: request_destination.clone(),
        results: result_destination.clone(),
        journey: Journey::Increment,
    };
    let initialized = initialize(&mut caller).infallible();
    let [request]: [_; 1] = initialized
        .sends
        .requests
        .try_into()
        .unwrap_or_else(|_| panic!("initialization sends exactly one actual Increment"));
    assert!(initialized.sends.results.is_empty() && initialized.creates.is_empty());
    assert!(matches!(initialized.become_, Step::Continue));
    assert_eq!(request.to, request_destination);
    assert_eq!(request.message, Operation::Increment);
    assert_eq!(caller.journey, Journey::Increment);

    let progress = [
        (
            Reply {
                operation: Operation::Increment,
                sequence: 101,
                value: Value::Processed(101),
            },
            Operation::Read,
            Journey::Read,
        ),
        (
            Reply {
                operation: Operation::Read,
                sequence: 102,
                value: Value::Read(17),
            },
            Operation::Stop,
            Journey::Stop(17),
        ),
    ];
    for (reply, expected_request, expected_journey) in progress {
        let actions =
            delegate_transition(&mut caller, User::new(requests.address(), reply)).infallible();
        let [request]: [_; 1] = actions
            .sends
            .requests
            .try_into()
            .unwrap_or_else(|_| panic!("this checked reply emits exactly its next operation"));
        assert!(actions.sends.results.is_empty() && actions.creates.is_empty());
        assert!(matches!(actions.become_, Step::Continue));
        assert_eq!(request.to, request_destination);
        assert_eq!(request.message, expected_request);
        assert_eq!(caller.journey, expected_journey);
    }
    let stopped = Reply {
        operation: Operation::Stop,
        sequence: 103,
        value: Value::Stopped,
    };
    let actions =
        delegate_transition(&mut caller, User::new(requests.address(), stopped)).infallible();
    let [result]: [_; 1] = actions
        .sends
        .results
        .try_into()
        .unwrap_or_else(|_| panic!("checked Stop emits exactly one original domain result"));
    let Outcome::Complete(value) = result.message else {
        panic!("the checked three-operation journey is Complete, not uncertain or failed");
    };
    assert!(actions.sends.requests.is_empty() && actions.creates.is_empty());
    assert!(matches!(actions.become_, Step::Stop(_)));
    assert_eq!(result.to, result_destination);
    assert_eq!(value, 17);
    assert_eq!(caller.journey, Journey::Complete(17));

    // The application's callback also preserves terminality if invoked after
    // completion. No second result may be emitted from the same completed fact.
    let expired = caller.expire();
    assert!(expired.sends.requests.is_empty() && expired.sends.results.is_empty());
    assert!(expired.creates.is_empty() && matches!(expired.become_, Step::Stop(_)));
    assert_eq!(caller.journey, Journey::Complete(17));
}

#[test]
fn refused_replies_stop_with_the_exact_original_and_no_further_request() {
    let scope = ApplicationScope::new();
    let requests = scope
        .external::<Requests>()
        .expect("actual typed request port");
    let results = scope
        .external::<Results>()
        .expect("actual typed domain-result port");
    let request_destination = requests.recipient();
    let result_destination = results.recipient();
    let cases = [
        (
            Journey::Increment,
            Reply {
                operation: Operation::Increment,
                sequence: 997,
                value: Value::Processed(101),
            },
            (Operation::Increment, 997, Value::Processed(101)),
        ),
        (
            Journey::Read,
            Reply {
                operation: Operation::Stop,
                sequence: 103,
                value: Value::Stopped,
            },
            (Operation::Stop, 103, Value::Stopped),
        ),
    ];
    for (journey, reply, expected) in cases {
        let mut caller = Caller {
            service: request_destination.clone(),
            results: result_destination.clone(),
            journey,
        };
        let actions =
            delegate_transition(&mut caller, User::new(requests.address(), reply)).infallible();
        let [result]: [_; 1] = actions
            .sends
            .results
            .try_into()
            .unwrap_or_else(|_| panic!("one exact rejected reply is reported"));
        let Outcome::Failed(Unexpected { original }) = result.message else {
            panic!("sequence/phase refusal retains the original Failed reply");
        };
        let observed = (original.operation, original.sequence, original.value);
        assert!(actions.sends.requests.is_empty() && actions.creates.is_empty());
        assert!(matches!(actions.become_, Step::Stop(_)));
        assert_eq!(result.to, result_destination);
        assert_eq!(observed, expected);
        assert_eq!(caller.journey, journey);
    }
}

#[test]
#[expect(
    clippy::too_many_lines,
    reason = "each phase's complete actions and actual timer generation precede replay of that same fact"
)]
fn each_active_receive_timeout_reports_only_its_actual_operation_once() {
    for (expected_journey, expected_operation) in [
        (Journey::Increment, Operation::Increment),
        (Journey::Read, Operation::Read),
        (Journey::Stop(17), Operation::Stop),
    ] {
        let scope = ApplicationScope::new();
        let requests = scope
            .external::<Requests>()
            .expect("actual typed request port");
        let results = scope
            .external::<Results>()
            .expect("actual typed domain-result port");
        let request_destination = requests.recipient();
        let result_destination = results.recipient();
        let caller = Caller {
            service: request_destination.clone(),
            results: result_destination.clone(),
            journey: Journey::Increment,
        };
        let period = Duration::from_secs(20);
        let mut caller = caller.with_receive_timeout(period, Caller::expire);
        let initialized = initialize(&mut caller).infallible();
        let [request]: [_; 1] = initialized
            .sends
            .inner
            .requests
            .try_into()
            .unwrap_or_else(|_| panic!("the wrapped initializer preserves Increment"));
        let [schedule]: [_; 1] = initialized
            .sends
            .owned
            .into_requests()
            .try_into()
            .unwrap_or_else(|_| panic!("the selected real template emits one timer request"));
        assert!(initialized.sends.inner.results.is_empty() && initialized.creates.is_empty());
        assert!(matches!(initialized.become_, Step::Continue));
        assert_eq!(request.to, request_destination);
        assert_eq!(request.message, Operation::Increment);
        assert_eq!(schedule.after, period);
        let mut schedules = vec![schedule];
        let replies = match expected_journey {
            Journey::Increment => Vec::new(),
            Journey::Read => vec![(
                Reply {
                    operation: Operation::Increment,
                    sequence: 101,
                    value: Value::Processed(101),
                },
                Operation::Read,
                Journey::Read,
            )],
            Journey::Stop(value) => vec![
                (
                    Reply {
                        operation: Operation::Increment,
                        sequence: 101,
                        value: Value::Processed(101),
                    },
                    Operation::Read,
                    Journey::Read,
                ),
                (
                    Reply {
                        operation: Operation::Read,
                        sequence: 102,
                        value: Value::Read(value),
                    },
                    Operation::Stop,
                    Journey::Stop(value),
                ),
            ],
            Journey::Complete(_) => unreachable!("only the three active phases are selected"),
        };
        for (reply, expected_request, expected_journey) in replies {
            let actions = delegate_transition(
                &mut caller,
                EventLayer::Inner(User::new(requests.address(), reply)),
            )
            .infallible();
            let [request]: [_; 1] = actions.sends.inner.requests.try_into().unwrap_or_else(|_| {
                panic!("the actual wrapper preserves the next typed operation")
            });
            let [schedule]: [_; 1] = actions
                .sends
                .owned
                .into_requests()
                .try_into()
                .unwrap_or_else(|_| {
                    panic!("the successful continuing reply rearms one exact timer")
                });
            let previous = schedules
                .last()
                .expect("the previous actual timer intent remains");
            let journey = caller.base().journey;
            assert!(actions.sends.inner.results.is_empty() && actions.creates.is_empty());
            assert!(matches!(actions.become_, Step::Continue));
            assert_eq!(request.to, request_destination);
            assert_eq!(request.message, expected_request);
            assert_eq!(journey, expected_journey);
            assert_eq!(schedule.after, period);
            assert_ne!(schedule.generation, previous.generation);
            schedules.push(schedule);
        }
        let schedule = schedules
            .last()
            .expect("the latest actual generation remains owned");
        // Infer the existing owner timer event from its actual id/generation;
        // no generation is predicted and no additional timer API is invented.
        let elapsed = (schedule.id, schedule.generation).into();
        let actions = delegate_transition(&mut caller, EventLayer::Owned(elapsed)).infallible();
        let [result]: [_; 1] = actions
            .sends
            .inner
            .results
            .try_into()
            .unwrap_or_else(|_| panic!("one timeout emits one actual domain outcome"));
        let Outcome::Uncertain(operation) = result.message else {
            panic!("a timeout cannot fabricate Complete or misclassify an incoming reply");
        };
        let journey = caller.base().journey;
        assert!(actions.sends.owned.is_empty() && actions.sends.inner.requests.is_empty());
        assert!(actions.creates.is_empty() && matches!(actions.become_, Step::Stop(_)));
        assert_eq!(result.to, result_destination);
        assert_eq!(operation, expected_operation);
        assert_eq!(journey, expected_journey);

        let replay = delegate_transition(&mut caller, EventLayer::Owned(elapsed)).infallible();
        let replayed_journey = caller.base().journey;
        assert!(replay.sends.owned.is_empty());
        assert!(replay.sends.inner.requests.is_empty() && replay.sends.inner.results.is_empty());
        assert!(replay.creates.is_empty() && matches!(replay.become_, Step::Continue));
        assert_eq!(replayed_journey, expected_journey);
    }
}
