//! Public Node-associated export construction; no network Query exchange.
use super::{Counter, CounterOperation, EndpointRole, LedgerOperation, MailLedger};
use super::{CounterValue, Receipts, assert_completed, assert_notifications};
use super::{configuration, recorded, scheduled};
use bombay::actors::ActorExt;
use bombay::prelude::Application;
use bombay::remote::{ExportLimits, IngressError};
use bombay::{ActorFailureAssessment, RetirementAssessment};
use std::error::Error;
use std::num::NonZeroUsize;
use std::sync::{Arc, Mutex};
use tokio::runtime::Builder;
use tokio::sync::mpsc;
use zenoh::query::Query;

async fn counter_constructor<Receipt>(same_request: fn(&Receipt, &Receipt) -> bool) {
    let refusals = Arc::new(Mutex::new(Vec::<IngressError<Query>>::new()));
    let observed = Arc::clone(&refusals);
    let mut node = Application::new(Counter::new().stop_on_shutdown())
        .open_node(configuration("tls/localhost:0", &EndpointRole::Receiver))
        .await
        .unwrap_or_else(|(_, _, cause)| {
            panic!(
                "actual configured Counter Node: {cause:?}; source: {:?}",
                cause.source()
            )
        });
    let outcome = node
        .run_with(|application| async move {
            let target = application.root().established_recipient();
            let declaration = application
                .export::<_, CounterValue, Receipt>(
                    target,
                    "bombay/public/counter",
                    ExportLimits {
                        queued_queries: NonZeroUsize::new(2).unwrap(),
                        retained_queries: NonZeroUsize::new(2).unwrap(),
                        retained_requests: NonZeroUsize::new(2).unwrap(),
                    },
                    same_request,
                    move |original| observed.lock().unwrap().push(original),
                )
                .await;
            let stopped = application.request_shutdown();
            let report = application.lifecycle().retirement().await;
            (declaration, stopped, report)
        })
        .await
        .unwrap_or_else(|_| panic!("actual Counter execution host"));
    let (outcome, native, notifications, ()) = outcome.into_parts();
    let ((declaration, stopped, report), cleanup) = outcome
        .try_into_completed()
        .unwrap_or_else(|_| panic!("original Counter Work acquired"));
    let declaration = match declaration {
        Ok(export) => {
            let diagnostic = format!("{export:?}");
            let (undeclared, session, mut incoming, retained) = export.undeclare().await;
            let mut rows = Vec::new();
            for (key, protected, proof, receipt, admission, native, mut client) in retained {
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
            Ok((undeclared, session, rows, queries, end, diagnostic))
        }
        Err(original) => Err(original),
    };
    if let Ok((_, session, ..)) = &declaration {
        assert!(
            !session.is_closed(),
            "the Node connection remains open before close"
        );
    }
    let closed = node.close().await;
    let native = native.expect("complete Counter native original");
    assert_completed(native, None);
    assert_notifications(&notifications);
    assert!(cleanup.is_ok() && closed.is_ok() && stopped.is_ok());
    let (undeclared, session, rows, queries, end, diagnostic) = declaration
        .unwrap_or_else(|(_, cause)| panic!("actual Node must issue its public export: {cause:?}"));
    assert_eq!(diagnostic, "Export { .. }");
    assert!(undeclared.is_ok());
    assert!(session.is_closed());
    assert!(rows.is_empty() && queries.is_empty());
    assert!(matches!(end, mpsc::error::TryRecvError::Disconnected));
    assert_eq!(report.retirement(), RetirementAssessment::Established);
    assert_eq!(report.failures(), ActorFailureAssessment::NoFailuresFound);
    assert!(refusals.lock().unwrap().is_empty());
}

async fn ledger_constructor<Receipt>(same_request: fn(&Receipt, &Receipt) -> bool) {
    let refusals = Arc::new(Mutex::new(Vec::<IngressError<Query>>::new()));
    let observed = Arc::clone(&refusals);
    let mut node = Application::new(
        MailLedger {
            entries: Vec::new(),
        }
        .stop_on_shutdown(),
    )
    .open_node(configuration("tls/localhost:0", &EndpointRole::Receiver))
    .await
    .unwrap_or_else(|(_, _, cause)| {
        panic!(
            "actual configured Ledger Node: {cause:?}; source: {:?}",
            cause.source()
        )
    });
    let (execution, receiving) = node
        .execute_with(|application| async move {
            let target = application.root().established_recipient();
            let declaration = application
                .export::<_, Receipts, Receipt>(
                    target,
                    "bombay/public/ledger",
                    ExportLimits {
                        queued_queries: NonZeroUsize::new(2).unwrap(),
                        retained_queries: NonZeroUsize::new(2).unwrap(),
                        retained_requests: NonZeroUsize::new(2).unwrap(),
                    },
                    same_request,
                    move |original| observed.lock().unwrap().push(original),
                )
                .await;
            let stopped = application.request_shutdown();
            let report = application.lifecycle().retirement().await;
            (declaration, stopped, report)
        })
        .unwrap_or_else(|_| panic!("actual Ledger execution host"));
    let ((), outcome) = tokio::join!(execution, receiving);
    let (outcome, native, notifications, ()) = outcome.into_parts();
    let ((declaration, stopped, report), cleanup) = outcome
        .try_into_completed()
        .unwrap_or_else(|_| panic!("original Ledger Work acquired"));
    let declaration = match declaration {
        Ok(export) => {
            let diagnostic = format!("{export:?}");
            let (undeclared, session, mut incoming, retained) = export.undeclare().await;
            let mut rows = Vec::new();
            for (key, protected, proof, receipt, admission, native, mut client) in retained {
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
            Ok((undeclared, session, rows, queries, end, diagnostic))
        }
        Err(original) => Err(original),
    };
    if let Ok((_, session, ..)) = &declaration {
        assert!(
            !session.is_closed(),
            "the Node connection remains open before close"
        );
    }
    let closed = node.close().await;
    let native = native.expect("complete Ledger native original");
    assert_completed(native, None);
    assert_notifications(&notifications);
    assert!(cleanup.is_ok() && closed.is_ok() && stopped.is_ok());
    let (undeclared, session, rows, queries, end, diagnostic) =
        declaration.unwrap_or_else(|_| panic!("actual Node must issue its public export"));
    assert_eq!(diagnostic, "Export { .. }");
    assert!(undeclared.is_ok());
    assert!(session.is_closed());
    assert!(rows.is_empty() && queries.is_empty());
    assert!(matches!(end, mpsc::error::TryRecvError::Disconnected));
    assert_eq!(report.retirement(), RetirementAssessment::Established);
    assert_eq!(report.failures(), ActorFailureAssessment::NoFailuresFound);
    assert!(refusals.lock().unwrap().is_empty());
}

#[test]
fn recorded_counter_joined_node_issues_export() {
    Builder::new_multi_thread()
        .worker_threads(1)
        .enable_all()
        .build()
        .unwrap()
        .block_on(counter_constructor::<recorded::Receipt<CounterOperation>>(
            |a, b| recorded::identity(a) == recorded::identity(b),
        ));
}
#[test]
fn scheduled_counter_joined_node_issues_export() {
    Builder::new_multi_thread()
        .worker_threads(1)
        .enable_all()
        .build()
        .unwrap()
        .block_on(counter_constructor::<scheduled::Receipt<CounterOperation>>(
            |a, b| scheduled::identity(a) == scheduled::identity(b),
        ));
}
#[test]
fn recorded_ledger_public_node_issues_export() {
    Builder::new_multi_thread()
        .worker_threads(1)
        .enable_all()
        .build()
        .unwrap()
        .block_on(ledger_constructor::<recorded::Receipt<LedgerOperation>>(
            |a, b| recorded::identity(a) == recorded::identity(b),
        ));
}
#[test]
fn scheduled_ledger_public_node_issues_export() {
    Builder::new_multi_thread()
        .worker_threads(1)
        .enable_all()
        .build()
        .unwrap()
        .block_on(ledger_constructor::<scheduled::Receipt<LedgerOperation>>(
            |a, b| scheduled::identity(a) == scheduled::identity(b),
        ));
}
