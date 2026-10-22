//! Common target certification precedes every declaration and Work invocation.
use super::super::super::counter::{Counter, CounterMessage, CounterValue};
use super::super::assert_retirement;
use super::{protocol, same_counter, wire};
use bombay::actors::ActorExt;
use bombay::behavior::EstablishedRecipient;
use bombay::prelude::{Application, Completion};
use bombay::remote::{Export, ExportError, ExportLimits, reply};
use bombay::{ActorFailureAssessment, MailAddr, RetirementAssessment, WorkOutcome};
use protocol::{EndpointRole, ProtectedMessage, configuration, limits, recorded, reserved_address};
use std::cell::{Cell, RefCell};
use std::convert::Infallible;
use std::num::NonZeroUsize;
use std::time::{Duration, Instant};
use tokio::sync::oneshot;
use wire::CounterInput;

#[derive(Clone, Copy)]
enum Selection {
    Duplicate,
    Foreign,
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn duplicate_actor_targets_refuse_before_any_declaration_or_work() {
    tokio::time::timeout(Duration::from_secs(10), refused(Selection::Duplicate))
        .await
        .expect("finite duplicate-target startup and both cleanups complete");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn foreign_actor_target_refuses_before_numeric_duplicate_comparison() {
    tokio::time::timeout(Duration::from_secs(10), refused(Selection::Foreign))
        .await
        .expect("finite foreign-target startup and both cleanups complete");
}

#[expect(
    clippy::too_many_lines,
    reason = "both actual Applications join before checking target refusal and original Work custody"
)]
async fn refused(selection: Selection) {
    let address = reserved_address();
    let configuration_scope = String::from("borrowed target refusal scope");
    let payload: Box<[u8]> = [107, 109].into();
    let allocation = payload.as_ptr();
    let invoked = &Cell::new(0);
    let verification_calls = Cell::new(0);
    let protection_calls = Cell::new(0);
    let records = RefCell::new(Vec::new());
    let verify = async |bytes: &[u8], proof: &[u8]| {
        verification_calls.set(verification_calls.get() + 1);
        let request = ProtectedMessage {
            protected: bytes.into(),
            proof: proof.into(),
        };
        let verification =
            recorded::verify::<CounterInput>(&request, &records.borrow(), wire::binding("counter"));
        verification.await
    };
    let maximum = NonZeroUsize::new(2).unwrap();
    let protect = async |bytes: &[u8], receipt: &recorded::Receipt<CounterInput>| {
        protection_calls.set(protection_calls.get() + 1);
        wire::protect(bytes, recorded::projection(receipt), &records, maximum)
    };
    let node = Application::new(Counter::new().stop_on_shutdown())
        .open_node(configuration(&address, &EndpointRole::Receiver))
        .await
        .unwrap_or_else(|_| panic!("actual owned Node opens"));
    let caller = node.runtime_id();
    let deadline = Instant::now() + Duration::from_secs(30);
    let current = |receipt: &recorded::Receipt<CounterInput>, runtime: &str, target: MailAddr| {
        wire::current(
            recorded::projection(receipt),
            runtime,
            target,
            wire::binding("counter"),
            &caller,
            deadline,
        )
    };
    let make_export = |key| {
        Export::new(
            key,
            ExportLimits {
                queued_queries: maximum,
                retained_queries: maximum,
                retained_requests: maximum,
                diagnostics: maximum,
            },
            limits(),
            wire::decode_counter,
            reply(wire::counter_read, wire::value),
            &verify,
            &protect,
            same_counter,
            &current,
            &current,
        )
    };
    let (published, foreign_target) = oneshot::channel::<EstablishedRecipient<Counter>>();
    let (recovered, unused_work) = oneshot::channel();
    let scope = configuration_scope.as_str();
    let execution = async {
        let foreign = foreign_target
            .await
            .expect("actual foreign actor exists before binding");
        let mut node = node.exports(
            (
                make_export("bombay/target/first"),
                make_export("bombay/target/second"),
            ),
            async |application| {
                let own = application.root().established_recipient();
                let second = match selection {
                    Selection::Duplicate => own.clone(),
                    Selection::Foreign => foreign.clone(),
                };
                Ok::<_, Infallible>((own, second))
            },
        );
        let outcome = node
            .run_with(move |application| {
                invoked.set(invoked.get() + 1);
                async move {
                    let mut client = application.client::<CounterValue>().unwrap();
                    let target = application.root().address();
                    let admitted = client.send(CounterMessage::Read(client.recipient())).await;
                    let original = client.receive().await;
                    let shutdown = application.shutdown();
                    let retirement = application.retirement().await;
                    (
                        scope, payload, target, admitted, original, shutdown, retirement,
                    )
                }
            })
            .await
            .unwrap_or_else(|_| panic!("actual host accepts the cold Node operation"));
        let ((first, second), bound, closed, polling, disposal) =
            node.into_parts().unwrap_or_else(|_| {
                panic!("actual startup refusal cleanup and native close are acquired")
            });
        let (work, root_report, native, notifications, ()) = outcome.into_parts();
        let (work, refusal) = match work {
            WorkOutcome::NotInvoked {
                work,
                startup_error,
                cleanup,
            } => (Some(work), Ok((startup_error, cleanup))),
            original => (None, Err(original)),
        };
        let invocations_at_refusal = invoked.get();
        let publication = recovered.send(work);
        publication.unwrap_or_else(|_| {
            panic!("the other live Application receives the exact original callable")
        });
        (
            first,
            second,
            bound,
            closed,
            root_report,
            polling,
            disposal,
            native,
            notifications,
            refusal,
            foreign,
            invocations_at_refusal,
        )
    };
    let other =
        Application::new(Counter::new().stop_on_shutdown()).run_with(|application| async move {
            let target = application.root().established_recipient();
            let address = application.root().address();
            let publication = published.send(target);
            publication.unwrap_or_else(|_| {
                panic!("the target consumer waits for a real live foreign actor")
            });
            let work = unused_work
                .await
                .expect("the exact original Work disposition returns after startup");
            let result = if let Some(work) = work {
                Ok(work(application).await)
            } else {
                let shutdown = application.shutdown();
                let retirement = application.retirement().await;
                Err((shutdown, retirement))
            };
            (address, result)
        });
    let (own, other) = tokio::join!(execution, other);
    let (
        first,
        second,
        bound,
        closed,
        root_report,
        polling,
        disposal,
        native,
        notifications,
        refusal,
        foreign,
        at_refusal,
    ) = own;
    let other =
        other.unwrap_or_else(|_| panic!("the other real Application host accepts execution"));
    let (work, other_report, other_native, other_notifications, ()) = other.into_parts();
    let ((foreign_address, recovered), other_cleanup) = work
        .try_into_completed()
        .unwrap_or_else(|_| panic!("the other Work completes after either original disposition"));
    let (origin, native) = native.expect("actual refused Node root retirement");
    let (_, other_native) = other_native.expect("actual other Application root retirement");
    let notifications = notifications.expect("actual Node notification originals");
    let other_notifications = other_notifications.expect("actual other notification originals");
    let (
        _first_inputs,
        _first_declaration,
        first_startup,
        first_withdrawal,
        first_ingress,
        first_requests,
    ) = first.into_parts();
    let (
        _second_inputs,
        _second_declaration,
        second_startup,
        second_withdrawal,
        second_ingress,
        second_requests,
    ) = second.into_parts();
    let (own_target, second_target) = bound
        .expect("actual binding result is retained")
        .expect("binding itself succeeds");
    let (startup_error, cleanup) = refusal.unwrap_or_else(|_| {
        panic!("common certification refusal must preserve the unused callable after both cleanups")
    });
    let (scope, payload, read_target, admission, received, shutdown, retirement) = recovered
        .unwrap_or_else(|_| {
            panic!("the exact original callable must be recovered and invoked once")
        });
    assert_eq!(other_report, Some(retirement));
    cleanup.expect("refused startup uses ordinary Application cleanup");
    assert!(startup_error.is_none());
    assert_eq!(retirement.retirement(), RetirementAssessment::Established);
    other_cleanup.expect("recovered callable uses ordinary Application cleanup");
    closed.expect("actual Node network close result");
    let root_report = root_report.expect("the joined root retains its runtime report");
    assert_eq!(root_report.retirement(), RetirementAssessment::Established);
    assert_eq!(
        root_report.failures(),
        ActorFailureAssessment::NoFailuresFound
    );
    assert!(polling.is_none() && disposal.is_none());
    notifications
        .termination
        .expect("Node termination publication");
    notifications
        .retirement
        .expect("Node joined retirement publication");
    other_notifications
        .termination
        .expect("other termination publication");
    other_notifications
        .retirement
        .expect("other joined retirement publication");
    assert_retirement(&native);
    assert_retirement(&other_native);
    assert_eq!(other_native.completion(), Some(&Completion::Stopped));
    assert_eq!(at_refusal, 0);
    assert_eq!(invoked.get(), 1);
    assert_eq!(scope, configuration_scope);
    assert_eq!(payload.as_ptr(), allocation);
    assert_eq!(payload.as_ref(), [107, 109]);
    assert!(admission.is_ok() && shutdown.is_ok());
    let original = received.expect("the recovered callable reads the other actual Counter");
    assert_eq!(original.from, read_target);
    assert_eq!(read_target, foreign_address);
    assert_eq!(original.message, 0);
    assert_eq!(verification_calls.get(), 0);
    assert_eq!(protection_calls.get(), 0);
    assert!(first_startup.is_none() && first_withdrawal.is_none() && second_withdrawal.is_none());
    assert!(first_ingress.is_empty() && second_ingress.is_empty());
    assert!(first_requests.is_empty() && second_requests.is_empty());
    let local_address = origin.address();
    assert_eq!(
        local_address, foreign_address,
        "actual distinct namespaces have colliding local addresses"
    );
    assert_ne!(own_target, foreign);
    match selection {
        Selection::Duplicate => {
            assert_eq!(second_target, own_target);
            assert!(matches!(
                second_startup,
                Some(Err(ExportError::DuplicateTarget))
            ));
        }
        Selection::Foreign => {
            assert_eq!(second_target, foreign);
            assert!(matches!(
                second_startup,
                Some(Err(ExportError::UnownedTarget))
            ));
        }
    }
}
