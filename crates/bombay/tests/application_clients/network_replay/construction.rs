//! Public cold declaration construction; no protected requests or native Query traffic.
use super::{Binding, Counter, CounterMessage, CounterOperation, EndpointRole};
use super::{LedgerCommand, LedgerOperation, MailLedger, ProtectedMessage, Purpose};
use super::{ReadReply, ReadReplyRef, ReadRequest, VerificationError};
use super::{assert_completed, assert_notifications, configuration, limits, recorded, scheduled};
use bombay::actors::ActorExt;
use bombay::behavior::User;
use bombay::prelude::{Application, MailAddr};
use bombay::remote::{Export, ExportLimits, RefusalRetention, reply};
use bombay::{ActorFailureAssessment, RetirementAssessment};
use std::cell::RefCell;
use std::num::NonZeroUsize;

const COUNTER_BINDING: Binding = Binding {
    deployment: "two-node-read-study",
    caller: "caller",
    controller: "owner",
    actor: "counter",
    host: "receiver",
    protocol: "counter",
    target_assignment: "construction",
    logical_session: 41,
};
const LEDGER_BINDING: Binding = Binding {
    actor: "ledger",
    protocol: "ledger",
    ..COUNTER_BINDING
};

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
#[expect(
    clippy::too_many_lines,
    clippy::result_large_err,
    reason = "one complete construction trace preserves its exact original request rejection type"
)]
async fn recorded_counter_joined_node_issues_export() {
    let maximum = NonZeroUsize::new(2).unwrap();
    // An empty registered set denies every unsolicited request; these leaves issue none.
    let records: [ProtectedMessage; 0] = [];
    let protected_replies = RefCell::new(Vec::<ProtectedMessage>::new());
    let current =
        |receipt: &recorded::Receipt<CounterOperation>, runtime: &str, target: MailAddr| {
            let scope = recorded::projection(receipt);
            if scope.kind == Purpose::Request
                && scope.operation == CounterOperation::Read
                && scope.bombay.runtime_generation.as_ref() == runtime
                && scope.bombay.actor_generation == target.0
                && recorded::identity(receipt).0 == &COUNTER_BINDING
            {
                Ok(())
            } else {
                Err(VerificationError::Invalid)
            }
        };
    let export = Export::new(
        "bombay/public/counter",
        ExportLimits {
            refusals: RefusalRetention::Retain,
            queued_queries: maximum,
            retained_queries: maximum,
            retained_requests: maximum,
            diagnostics: maximum,
        },
        limits(),
        |bytes: &[u8]| serde_json::from_slice::<ReadRequest<CounterOperation>>(bytes),
        reply(
            |request: ReadRequest<CounterOperation>| match request.operation {
                CounterOperation::Read => {
                    Ok::<_, ReadRequest<CounterOperation>>(CounterMessage::Read)
                }
            },
            |receipt: &recorded::Receipt<CounterOperation>, original: &User<MailAddr, u64>| {
                serde_json::to_vec(&ReadReplyRef {
                    bombay: &recorded::projection(receipt).bombay,
                    kind: Purpose::ReadReply,
                    operation: CounterOperation::Read,
                    value: &original.message,
                })
                .map(Vec::into_boxed_slice)
            },
        ),
        async |bytes: &[u8], proof: &[u8]| {
            recorded::verify::<CounterOperation>(
                &ProtectedMessage {
                    protected: bytes.into(),
                    proof: proof.into(),
                },
                &records,
                COUNTER_BINDING,
            )
            .await
        },
        async |bytes: &[u8], receipt: &recorded::Receipt<CounterOperation>| {
            let reply: ReadReply<u64, CounterOperation> =
                serde_json::from_slice(bytes).map_err(VerificationError::Declaration)?;
            if reply.bombay == recorded::projection(receipt).bombay
                && reply.kind == Purpose::ReadReply
                && reply.operation == CounterOperation::Read
            {
                let mut replies = protected_replies.borrow_mut();
                if replies.len() >= maximum.get() {
                    return Err(VerificationError::Invalid);
                }
                let proof = Box::<[u8]>::from([13, 17]);
                replies.push(ProtectedMessage {
                    protected: bytes.into(),
                    proof: proof.clone(),
                });
                Ok(proof)
            } else {
                Err(VerificationError::Invalid)
            }
        },
        |left, right| recorded::identity(left) == recorded::identity(right),
        &current,
        &current,
    );
    let node = Application::new(Counter::new().stop_on_shutdown())
        .open_node(configuration("tls/localhost:0", &EndpointRole::Receiver))
        .await
        .unwrap_or_else(|_| panic!("the actual Counter Node opens"));
    let mut node = node.exports(export, async |application| {
        Ok::<_, VerificationError>(application.root().established_recipient())
    });
    let outcome = node
        .run_with(|application| async move {
            let target = application.root().established_recipient();
            let stopped = application.shutdown();
            let retired = application.lifecycle().retirement().await;
            (target, stopped, retired)
        })
        .await
        .unwrap_or_else(|_| panic!("the actual Counter enters its host"));
    let (export, bound, closed, polling, disposal) = node
        .into_parts()
        .unwrap_or_else(|_| panic!("joined Node finalization remains acquired"));
    let diagnostic = format!("{export:?}");
    let (_, _, declared, withdrawn, diagnostics, requests) = export.into_parts();
    let (work, report, native, notifications, ()) = outcome.into_parts();
    let ((expected, stopped, retired), cleanup) = work
        .try_into_completed()
        .unwrap_or_else(|_| panic!("the original Work and cleanup remain acquired"));
    let target = bound
        .expect("binding was invoked")
        .expect("the root binding succeeds");
    let report = report.expect("the independent root report remains acquired");
    let native = native.expect("the complete original Counter remains acquired");
    let protected_replies = protected_replies.borrow();
    assert_completed(native, None);
    assert_notifications(&notifications);
    assert!(cleanup.is_ok() && closed.is_ok() && stopped.is_ok());
    assert_eq!(target, expected);
    assert_eq!(report, retired);
    assert_eq!(report.retirement(), RetirementAssessment::Established);
    assert_eq!(report.failures(), ActorFailureAssessment::NoFailuresFound);
    assert!(polling.is_none() && disposal.is_none());
    assert_eq!(diagnostic, "Export { .. }");
    assert!(matches!(&declared, Some(Ok(()))) && matches!(&withdrawn, Some(Ok(()))));
    assert!(requests.is_empty() && diagnostics.is_empty());
    assert!(protected_replies.is_empty());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
#[expect(
    clippy::too_many_lines,
    clippy::result_large_err,
    reason = "one complete construction trace preserves its exact original request rejection type"
)]
async fn scheduled_counter_joined_node_issues_export() {
    let maximum = NonZeroUsize::new(2).unwrap();
    let records: [ProtectedMessage; 0] = [];
    let protected_replies = RefCell::new(Vec::<ProtectedMessage>::new());
    let current =
        |receipt: &scheduled::Receipt<CounterOperation>, runtime: &str, target: MailAddr| {
            let scope = scheduled::projection(receipt);
            if scope.kind == Purpose::Request
                && scope.operation == CounterOperation::Read
                && scope.bombay.runtime_generation.as_ref() == runtime
                && scope.bombay.actor_generation == target.0
                && scheduled::identity(receipt).0 == &COUNTER_BINDING
            {
                Ok(())
            } else {
                Err(VerificationError::Invalid)
            }
        };
    let export = Export::new(
        "bombay/public/counter",
        ExportLimits {
            refusals: RefusalRetention::Retain,
            queued_queries: maximum,
            retained_queries: maximum,
            retained_requests: maximum,
            diagnostics: maximum,
        },
        limits(),
        |bytes: &[u8]| serde_json::from_slice::<ReadRequest<CounterOperation>>(bytes),
        reply(
            |request: ReadRequest<CounterOperation>| match request.operation {
                CounterOperation::Read => {
                    Ok::<_, ReadRequest<CounterOperation>>(CounterMessage::Read)
                }
            },
            |receipt: &scheduled::Receipt<CounterOperation>, original: &User<MailAddr, u64>| {
                serde_json::to_vec(&ReadReplyRef {
                    bombay: &scheduled::projection(receipt).bombay,
                    kind: Purpose::ReadReply,
                    operation: CounterOperation::Read,
                    value: &original.message,
                })
                .map(Vec::into_boxed_slice)
            },
        ),
        async |bytes: &[u8], proof: &[u8]| {
            scheduled::verify::<CounterOperation>(
                &ProtectedMessage {
                    protected: bytes.into(),
                    proof: proof.into(),
                },
                &records,
                COUNTER_BINDING,
            )
            .await
        },
        async |bytes: &[u8], receipt: &scheduled::Receipt<CounterOperation>| {
            let reply: ReadReply<u64, CounterOperation> =
                serde_json::from_slice(bytes).map_err(VerificationError::Declaration)?;
            if reply.bombay == scheduled::projection(receipt).bombay
                && reply.kind == Purpose::ReadReply
                && reply.operation == CounterOperation::Read
            {
                let mut replies = protected_replies.borrow_mut();
                if replies.len() >= maximum.get() {
                    return Err(VerificationError::Invalid);
                }
                let proof = Box::<[u8]>::from([13, 17]);
                replies.push(ProtectedMessage {
                    protected: bytes.into(),
                    proof: proof.clone(),
                });
                Ok(proof)
            } else {
                Err(VerificationError::Invalid)
            }
        },
        |left, right| scheduled::identity(left) == scheduled::identity(right),
        &current,
        &current,
    );
    let node = Application::new(Counter::new().stop_on_shutdown())
        .open_node(configuration("tls/localhost:0", &EndpointRole::Receiver))
        .await
        .unwrap_or_else(|_| panic!("the actual Counter Node opens"));
    let mut node = node.exports(export, async |application| {
        Ok::<_, VerificationError>(application.root().established_recipient())
    });
    let outcome = node
        .run_with(|application| async move {
            let target = application.root().established_recipient();
            let stopped = application.shutdown();
            let retired = application.lifecycle().retirement().await;
            (target, stopped, retired)
        })
        .await
        .unwrap_or_else(|_| panic!("the actual Counter enters its host"));
    let (export, bound, closed, polling, disposal) = node
        .into_parts()
        .unwrap_or_else(|_| panic!("joined Node finalization remains acquired"));
    let diagnostic = format!("{export:?}");
    let (_, _, declared, withdrawn, diagnostics, requests) = export.into_parts();
    let (work, report, native, notifications, ()) = outcome.into_parts();
    let ((expected, stopped, retired), cleanup) = work
        .try_into_completed()
        .unwrap_or_else(|_| panic!("the original Work and cleanup remain acquired"));
    let target = bound
        .expect("binding was invoked")
        .expect("the root binding succeeds");
    let report = report.expect("the independent root report remains acquired");
    let native = native.expect("the complete original Counter remains acquired");
    let protected_replies = protected_replies.borrow();
    assert_completed(native, None);
    assert_notifications(&notifications);
    assert!(cleanup.is_ok() && closed.is_ok() && stopped.is_ok());
    assert_eq!(target, expected);
    assert_eq!(report, retired);
    assert_eq!(report.retirement(), RetirementAssessment::Established);
    assert_eq!(report.failures(), ActorFailureAssessment::NoFailuresFound);
    assert!(polling.is_none() && disposal.is_none());
    assert_eq!(diagnostic, "Export { .. }");
    assert!(matches!(&declared, Some(Ok(()))) && matches!(&withdrawn, Some(Ok(()))));
    assert!(requests.is_empty() && diagnostics.is_empty());
    assert!(protected_replies.is_empty());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
#[expect(
    clippy::too_many_lines,
    clippy::result_large_err,
    reason = "one complete construction trace preserves its exact original request rejection type"
)]
async fn recorded_ledger_public_node_issues_export() {
    let maximum = NonZeroUsize::new(2).unwrap();
    let records: [ProtectedMessage; 0] = [];
    let protected_replies = RefCell::new(Vec::<ProtectedMessage>::new());
    let current =
        |receipt: &recorded::Receipt<LedgerOperation>, runtime: &str, target: MailAddr| {
            let scope = recorded::projection(receipt);
            if scope.kind == Purpose::Request
                && scope.operation == LedgerOperation::Read
                && scope.bombay.runtime_generation.as_ref() == runtime
                && scope.bombay.actor_generation == target.0
                && recorded::identity(receipt).0 == &LEDGER_BINDING
            {
                Ok(())
            } else {
                Err(VerificationError::Invalid)
            }
        };
    let export = Export::new(
        "bombay/public/ledger",
        ExportLimits {
            refusals: RefusalRetention::Retain,
            queued_queries: maximum,
            retained_queries: maximum,
            retained_requests: maximum,
            diagnostics: maximum,
        },
        limits(),
        |bytes: &[u8]| serde_json::from_slice::<ReadRequest<LedgerOperation>>(bytes),
        reply(
            |request: ReadRequest<LedgerOperation>| match request.operation {
                LedgerOperation::Read => Ok::<_, ReadRequest<LedgerOperation>>(LedgerCommand::Read),
            },
            |receipt: &recorded::Receipt<LedgerOperation>,
             original: &User<MailAddr, Vec<MailAddr>>| {
                let origins = original
                    .message
                    .iter()
                    .copied()
                    .map(u64::from)
                    .collect::<Vec<_>>();
                serde_json::to_vec(&ReadReplyRef {
                    bombay: &recorded::projection(receipt).bombay,
                    kind: Purpose::ReadReply,
                    operation: LedgerOperation::Read,
                    value: &origins,
                })
                .map(Vec::into_boxed_slice)
            },
        ),
        async |bytes: &[u8], proof: &[u8]| {
            recorded::verify::<LedgerOperation>(
                &ProtectedMessage {
                    protected: bytes.into(),
                    proof: proof.into(),
                },
                &records,
                LEDGER_BINDING,
            )
            .await
        },
        async |bytes: &[u8], receipt: &recorded::Receipt<LedgerOperation>| {
            let reply: ReadReply<Vec<u64>, LedgerOperation> =
                serde_json::from_slice(bytes).map_err(VerificationError::Declaration)?;
            if reply.bombay == recorded::projection(receipt).bombay
                && reply.kind == Purpose::ReadReply
                && reply.operation == LedgerOperation::Read
            {
                let mut replies = protected_replies.borrow_mut();
                if replies.len() >= maximum.get() {
                    return Err(VerificationError::Invalid);
                }
                let proof = Box::<[u8]>::from([13, 17]);
                replies.push(ProtectedMessage {
                    protected: bytes.into(),
                    proof: proof.clone(),
                });
                Ok(proof)
            } else {
                Err(VerificationError::Invalid)
            }
        },
        |left, right| recorded::identity(left) == recorded::identity(right),
        &current,
        &current,
    );
    let node = Application::new(
        MailLedger {
            entries: Vec::new(),
        }
        .stop_on_shutdown(),
    )
    .open_node(configuration("tls/localhost:0", &EndpointRole::Receiver))
    .await
    .unwrap_or_else(|_| panic!("the actual Ledger Node opens"));
    let mut node = node.exports(export, async |application| {
        Ok::<_, VerificationError>(application.root().established_recipient())
    });
    let outcome = node
        .run_with(|application| async move {
            let target = application.root().established_recipient();
            let stopped = application.shutdown();
            let retired = application.lifecycle().retirement().await;
            (target, stopped, retired)
        })
        .await
        .unwrap_or_else(|_| panic!("the actual Ledger enters its host"));
    let (export, bound, closed, polling, disposal) = node
        .into_parts()
        .unwrap_or_else(|_| panic!("joined Node finalization remains acquired"));
    let diagnostic = format!("{export:?}");
    let (_, _, declared, withdrawn, diagnostics, requests) = export.into_parts();
    let (work, report, native, notifications, ()) = outcome.into_parts();
    let ((expected, stopped, retired), cleanup) = work
        .try_into_completed()
        .unwrap_or_else(|_| panic!("the original Work and cleanup remain acquired"));
    let target = bound
        .expect("binding was invoked")
        .expect("the root binding succeeds");
    let report = report.expect("the independent root report remains acquired");
    let native = native.expect("the complete original Ledger remains acquired");
    let protected_replies = protected_replies.borrow();
    assert_completed(native, None);
    assert_notifications(&notifications);
    assert!(cleanup.is_ok() && closed.is_ok() && stopped.is_ok());
    assert_eq!(target, expected);
    assert_eq!(report, retired);
    assert_eq!(report.retirement(), RetirementAssessment::Established);
    assert_eq!(report.failures(), ActorFailureAssessment::NoFailuresFound);
    assert!(polling.is_none() && disposal.is_none());
    assert_eq!(diagnostic, "Export { .. }");
    assert!(matches!(&declared, Some(Ok(()))) && matches!(&withdrawn, Some(Ok(()))));
    assert!(requests.is_empty() && diagnostics.is_empty());
    assert!(protected_replies.is_empty());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
#[expect(
    clippy::too_many_lines,
    clippy::result_large_err,
    reason = "one complete construction trace preserves its exact original request rejection type"
)]
async fn scheduled_ledger_public_node_issues_export() {
    let maximum = NonZeroUsize::new(2).unwrap();
    let records: [ProtectedMessage; 0] = [];
    let protected_replies = RefCell::new(Vec::<ProtectedMessage>::new());
    let current =
        |receipt: &scheduled::Receipt<LedgerOperation>, runtime: &str, target: MailAddr| {
            let scope = scheduled::projection(receipt);
            if scope.kind == Purpose::Request
                && scope.operation == LedgerOperation::Read
                && scope.bombay.runtime_generation.as_ref() == runtime
                && scope.bombay.actor_generation == target.0
                && scheduled::identity(receipt).0 == &LEDGER_BINDING
            {
                Ok(())
            } else {
                Err(VerificationError::Invalid)
            }
        };
    let export = Export::new(
        "bombay/public/ledger",
        ExportLimits {
            refusals: RefusalRetention::Retain,
            queued_queries: maximum,
            retained_queries: maximum,
            retained_requests: maximum,
            diagnostics: maximum,
        },
        limits(),
        |bytes: &[u8]| serde_json::from_slice::<ReadRequest<LedgerOperation>>(bytes),
        reply(
            |request: ReadRequest<LedgerOperation>| match request.operation {
                LedgerOperation::Read => Ok::<_, ReadRequest<LedgerOperation>>(LedgerCommand::Read),
            },
            |receipt: &scheduled::Receipt<LedgerOperation>,
             original: &User<MailAddr, Vec<MailAddr>>| {
                let origins = original
                    .message
                    .iter()
                    .copied()
                    .map(u64::from)
                    .collect::<Vec<_>>();
                serde_json::to_vec(&ReadReplyRef {
                    bombay: &scheduled::projection(receipt).bombay,
                    kind: Purpose::ReadReply,
                    operation: LedgerOperation::Read,
                    value: &origins,
                })
                .map(Vec::into_boxed_slice)
            },
        ),
        async |bytes: &[u8], proof: &[u8]| {
            scheduled::verify::<LedgerOperation>(
                &ProtectedMessage {
                    protected: bytes.into(),
                    proof: proof.into(),
                },
                &records,
                LEDGER_BINDING,
            )
            .await
        },
        async |bytes: &[u8], receipt: &scheduled::Receipt<LedgerOperation>| {
            let reply: ReadReply<Vec<u64>, LedgerOperation> =
                serde_json::from_slice(bytes).map_err(VerificationError::Declaration)?;
            if reply.bombay == scheduled::projection(receipt).bombay
                && reply.kind == Purpose::ReadReply
                && reply.operation == LedgerOperation::Read
            {
                let mut replies = protected_replies.borrow_mut();
                if replies.len() >= maximum.get() {
                    return Err(VerificationError::Invalid);
                }
                let proof = Box::<[u8]>::from([13, 17]);
                replies.push(ProtectedMessage {
                    protected: bytes.into(),
                    proof: proof.clone(),
                });
                Ok(proof)
            } else {
                Err(VerificationError::Invalid)
            }
        },
        |left, right| scheduled::identity(left) == scheduled::identity(right),
        &current,
        &current,
    );
    let node = Application::new(
        MailLedger {
            entries: Vec::new(),
        }
        .stop_on_shutdown(),
    )
    .open_node(configuration("tls/localhost:0", &EndpointRole::Receiver))
    .await
    .unwrap_or_else(|_| panic!("the actual Ledger Node opens"));
    let mut node = node.exports(export, async |application| {
        Ok::<_, VerificationError>(application.root().established_recipient())
    });
    let outcome = node
        .run_with(|application| async move {
            let target = application.root().established_recipient();
            let stopped = application.shutdown();
            let retired = application.lifecycle().retirement().await;
            (target, stopped, retired)
        })
        .await
        .unwrap_or_else(|_| panic!("the actual Ledger enters its host"));
    let (export, bound, closed, polling, disposal) = node
        .into_parts()
        .unwrap_or_else(|_| panic!("joined Node finalization remains acquired"));
    let diagnostic = format!("{export:?}");
    let (_, _, declared, withdrawn, diagnostics, requests) = export.into_parts();
    let (work, report, native, notifications, ()) = outcome.into_parts();
    let ((expected, stopped, retired), cleanup) = work
        .try_into_completed()
        .unwrap_or_else(|_| panic!("the original Work and cleanup remain acquired"));
    let target = bound
        .expect("binding was invoked")
        .expect("the root binding succeeds");
    let report = report.expect("the independent root report remains acquired");
    let native = native.expect("the complete original Ledger remains acquired");
    let protected_replies = protected_replies.borrow();
    assert_completed(native, None);
    assert_notifications(&notifications);
    assert!(cleanup.is_ok() && closed.is_ok() && stopped.is_ok());
    assert_eq!(target, expected);
    assert_eq!(report, retired);
    assert_eq!(report.retirement(), RetirementAssessment::Established);
    assert_eq!(report.failures(), ActorFailureAssessment::NoFailuresFound);
    assert!(polling.is_none() && disposal.is_none());
    assert_eq!(diagnostic, "Export { .. }");
    assert!(matches!(&declared, Some(Ok(()))) && matches!(&withdrawn, Some(Ok(()))));
    assert!(requests.is_empty() && diagnostics.is_empty());
    assert!(protected_replies.is_empty());
}
