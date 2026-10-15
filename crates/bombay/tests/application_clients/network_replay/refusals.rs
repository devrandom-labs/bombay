//! Constructor refusal laws; no protected requests or Query traffic are issued.
use super::{Counter, CounterOperation, CounterValue, EndpointRole};
use super::{assert_completed, assert_notifications, configuration, recorded, reserved_address};
use bombay::actors::ActorExt;
use bombay::prelude::Application;
use bombay::remote::{ExportError, ExportLimits, IngressError};
use bombay::{ActorFailureAssessment, RetirementAssessment};
use std::error::Error;
use std::num::NonZeroUsize;
use std::ptr;
use std::sync::{Arc, Mutex};
use tokio::runtime::Builder;
use tokio::sync::Semaphore;
use zenoh::query::Query;

#[test]
#[expect(
    clippy::too_many_lines,
    reason = "retain every original constructor and cleanup outcome before checking distinct refusal laws"
)]
fn public_exports_preserve_constructor_refusals_and_original_targets() {
    Builder::new_multi_thread()
        .worker_threads(1)
        .enable_all()
        .build()
        .unwrap()
        .block_on(async {
            let limits = ExportLimits {
                queued_queries: NonZeroUsize::new(2).unwrap(),
                retained_queries: NonZeroUsize::new(2).unwrap(),
                retained_requests: NonZeroUsize::new(2).unwrap(),
            };
            let beyond_queries = NonZeroUsize::new(
                Semaphore::MAX_PERMITS
                    .checked_add(1)
                    .expect("native semaphore leaves a representable overflow test value"),
            )
            .unwrap();
            let same_request =
                |a: &recorded::Receipt<CounterOperation>,
                 b: &recorded::Receipt<CounterOperation>| {
                    recorded::identity(a) == recorded::identity(b)
                };
            let refusals = Arc::new(Mutex::new(Vec::<IngressError<Query>>::new()));
            let local_refusals = Arc::clone(&refusals);
            let local = Application::new(Counter::new().stop_on_shutdown())
                .run_with(|application| async move {
                    let expected = application.root().established_recipient();
                    let declaration = application
                        .export::<_, CounterValue, recorded::Receipt<CounterOperation>>(
                            expected.clone(),
                            "bombay/refusals/local",
                            limits,
                            same_request,
                            move |original| local_refusals.lock().unwrap().push(original),
                        )
                        .await;
                    let stopped = application.request_shutdown();
                    let report = application.lifecycle().retirement().await;
                    (declaration, expected, stopped, report)
                })
                .await
                .unwrap_or_else(|_| panic!("local Application enters the actual host"));
            let (local_outcome, local_native, local_notifications, ()) = local.into_parts();
            let ((local_declaration, local_target, local_stopped, local_report), local_cleanup) =
                local_outcome
                    .try_into_completed()
                    .unwrap_or_else(|_| panic!("original local Work is received"));

            let address = reserved_address();
            let mut node = Application::new(Counter::new().stop_on_shutdown())
                .open_node(configuration(&address, &EndpointRole::Receiver))
                .await
                .unwrap_or_else(|_| panic!("configured owning Node starts"));
            let node_refusals = Arc::clone(&refusals);
            let (execution, receiving) = node
                .execute_with(|application| async move {
                    let target = application.root().established_recipient();
                    let foreign = Application::new(Counter::new().stop_on_shutdown())
                        .run_with(|foreign_application| async move {
                            let target = foreign_application.root().established_recipient();
                            let stopped = foreign_application.request_shutdown();
                            let report = foreign_application.lifecycle().retirement().await;
                            (target, stopped, report)
                        })
                        .await
                        .unwrap_or_else(|_| panic!("independent Application enters the host"));
                    let (foreign_outcome, foreign_native, foreign_notifications, ()) =
                        foreign.into_parts();
                    let ((foreign_target, foreign_stopped, foreign_report), foreign_cleanup) =
                        foreign_outcome
                            .try_into_completed()
                            .unwrap_or_else(|_| panic!("foreign original Work is received"));
                    let observed = Arc::clone(&node_refusals);
                    let foreign_declaration = application
                        .export::<_, CounterValue, recorded::Receipt<CounterOperation>>(
                            foreign_target.clone(),
                            "bombay/refusals/foreign",
                            limits,
                            same_request,
                            move |original| observed.lock().unwrap().push(original),
                        )
                        .await;
                    let observed = Arc::clone(&node_refusals);
                    let queued_declaration = application
                        .export::<_, CounterValue, recorded::Receipt<CounterOperation>>(
                            target.clone(),
                            "bombay/refusals/queued",
                            ExportLimits {
                                queued_queries: beyond_queries,
                                ..limits
                            },
                            same_request,
                            move |original| observed.lock().unwrap().push(original),
                        )
                        .await;
                    let observed = Arc::clone(&node_refusals);
                    let retained_declaration = application
                        .export::<_, CounterValue, recorded::Receipt<CounterOperation>>(
                            target.clone(),
                            "bombay/refusals/retained",
                            ExportLimits {
                                retained_queries: beyond_queries,
                                ..limits
                            },
                            same_request,
                            move |original| observed.lock().unwrap().push(original),
                        )
                        .await;
                    let observed = Arc::clone(&node_refusals);
                    let storage_declaration = application
                        .export::<_, CounterValue, recorded::Receipt<CounterOperation>>(
                            target.clone(),
                            "bombay/refusals/storage",
                            ExportLimits {
                                retained_requests: NonZeroUsize::new(usize::MAX).unwrap(),
                                ..limits
                            },
                            same_request,
                            move |original| observed.lock().unwrap().push(original),
                        )
                        .await;
                    let observed = Arc::clone(&node_refusals);
                    let invalid_declaration = application
                        .export::<_, CounterValue, recorded::Receipt<CounterOperation>>(
                            target.clone(),
                            "",
                            limits,
                            same_request,
                            move |original| observed.lock().unwrap().push(original),
                        )
                        .await;
                    let stopped = application.request_shutdown();
                    let report = application.lifecycle().retirement().await;
                    (
                        application,
                        target,
                        foreign_target,
                        foreign_declaration,
                        queued_declaration,
                        retained_declaration,
                        storage_declaration,
                        invalid_declaration,
                        stopped,
                        report,
                        foreign_stopped,
                        foreign_report,
                        foreign_cleanup,
                        foreign_native,
                        foreign_notifications,
                    )
                })
                .unwrap_or_else(|_| panic!("actual owning Node execution enters"));
            let ((), outcome) = tokio::join!(execution, receiving);
            let (outcome, native, notifications, ()) = outcome.into_parts();
            let (
                (
                    application,
                    target,
                    foreign_target,
                    foreign_declaration,
                    queued_declaration,
                    retained_declaration,
                    storage_declaration,
                    invalid_declaration,
                    stopped,
                    report,
                    foreign_stopped,
                    foreign_report,
                    foreign_cleanup,
                    foreign_native,
                    foreign_notifications,
                ),
                cleanup,
            ) = outcome
                .try_into_completed()
                .unwrap_or_else(|_| panic!("all original Node Work outcomes are received"));
            let closed = node.close().await;
            let observed = Arc::clone(&refusals);
            let closed_declaration = application
                .export::<_, CounterValue, recorded::Receipt<CounterOperation>>(
                    target.clone(),
                    "bombay/refusals/closed",
                    limits,
                    same_request,
                    move |original| observed.lock().unwrap().push(original),
                )
                .await;

            // Even an unexpectedly successful declaration is undeclared and its
            // complete native products are retained before the refusal oracles.
            // Closing the Node first may itself determine its native undeclare
            // result; preserve that Result rather than asserting false success.
            let mut settled = Vec::new();
            for declaration in [
                local_declaration,
                closed_declaration,
                foreign_declaration,
                queued_declaration,
                retained_declaration,
                storage_declaration,
                invalid_declaration,
            ] {
                let retained = match declaration {
                    Err(original) => Err(original),
                    Ok(export) => {
                        let (undeclared, session, mut incoming, retained) =
                            export.undeclare().await;
                        let mut rows = Vec::new();
                        for (key, protected, proof, receipt, admission, native, mut client) in
                            retained
                        {
                            client.close_admission();
                            let mut tail = Vec::new();
                            while let Some(original) = client.receive().await {
                                tail.push(original);
                            }
                            rows.push((
                                key, protected, proof, receipt, admission, native, client, tail,
                            ));
                        }
                        let mut queries = Vec::new();
                        let end = loop {
                            match incoming.try_recv() {
                                Ok(original) => queries.push(original),
                                Err(cause) => break cause,
                            }
                        };
                        Ok((undeclared, session, incoming, rows, queries, end))
                    }
                };
                settled.push(retained);
            }
            let [
                local,
                closed_declaration,
                foreign,
                queued,
                retained,
                storage,
                invalid,
            ] = settled
                .try_into()
                .unwrap_or_else(|_| panic!("all seven original declarations remain"));

            assert_completed(local_native.expect("local native root retained"), None);
            assert_completed(native.expect("owning Node native root retained"), None);
            assert_completed(foreign_native.expect("foreign native root retained"), None);
            assert_notifications(&local_notifications);
            assert_notifications(&notifications);
            assert_notifications(&foreign_notifications);
            assert!(local_cleanup.is_ok() && cleanup.is_ok() && foreign_cleanup.is_ok());
            assert!(
                closed.is_ok()
                    && local_stopped.is_ok()
                    && stopped.is_ok()
                    && foreign_stopped.is_ok()
            );
            for report in [local_report, report, foreign_report] {
                assert_eq!(report.retirement(), RetirementAssessment::Established);
                assert_eq!(report.failures(), ActorFailureAssessment::NoFailuresFound);
            }
            let Err((returned, cause)) = &local else {
                panic!("local-only export must refuse");
            };
            assert_eq!(returned, &local_target);
            assert!(matches!(cause, ExportError::NodeAbsent));
            let Err((returned, cause)) = &closed_declaration else {
                panic!("closed Node must refuse");
            };
            assert_eq!(returned, &target);
            assert!(matches!(cause, ExportError::SessionClosed));
            let Err((returned, cause)) = &foreign else {
                panic!("foreign actor must refuse");
            };
            assert_eq!(returned, &foreign_target);
            assert_ne!(returned, &target);
            assert!(matches!(cause, ExportError::UnownedTarget));
            for refused in [&queued, &retained] {
                let Err((returned, cause)) = refused else {
                    panic!("overflow query count must refuse");
                };
                assert_eq!(returned, &target);
                assert!(matches!(cause, ExportError::QueryCapacity));
            }
            let Err((returned, cause @ ExportError::Storage { cause: native })) = &storage else {
                panic!("unrepresentable retained storage must return its actual cause");
            };
            assert_eq!(returned, &target);
            assert_eq!(
                ptr::from_ref(cause.source().unwrap()).addr(),
                ptr::from_ref(native).addr()
            );
            let Err((returned, cause @ ExportError::Declaration { cause: native })) = &invalid
            else {
                panic!("malformed SDK key must return its actual declaration cause");
            };
            assert_eq!(returned, &target);
            assert_eq!(
                ptr::from_ref(cause.source().unwrap()).addr(),
                ptr::from_ref(native.as_ref()).addr()
            );
            assert!(refusals.lock().unwrap().is_empty());
        });
}
