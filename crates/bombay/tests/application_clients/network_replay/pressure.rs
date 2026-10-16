//! Real Node mailbox pressure, using opaque local receipts rather than authentication claims.
//! Every retry rechecks current permission and retains the exact native Full command.
use super::super::counter::{CounterProcessed, CounterValue};
use super::{Counter, CounterMessage, EndpointRole, LedgerCommand, MailLedger, Receipts};
use super::{
    assert_completed, assert_notifications, assert_report, configuration, reserved_address,
};
use bombay::TrySendError;
use bombay::actors::ActorExt;
use bombay::behavior::BehaviorBase;
use bombay::prelude::Application;
use bombay::remote::{Admission, ExportLimits, IngressError, RequestRefusal};
use std::cell::{Cell, Ref, RefCell};
use std::num::NonZeroUsize;
use std::panic::{AssertUnwindSafe, catch_unwind, panic_any};
use std::sync::{Arc, Mutex, mpsc as release};
use tokio::runtime::Builder;
use tokio::sync::{mpsc, oneshot};
use zenoh::query::Query;

// This loan retains the real coherent permission borrow through native admission.
// Its one deliberately panicking release tests custody after a native Full return.
struct PermissionLoan<'permission> {
    permission: Ref<'permission, Result<(), u64>>,
}

impl Drop for PermissionLoan<'_> {
    fn drop(&mut self) {
        panic_any(*self.permission);
    }
}

#[test]
#[expect(
    clippy::too_many_lines,
    reason = "one actual full-mailbox trace retains permission loans, original command and native cleanup before assertions"
)]
fn counter_retries_only_its_original_full_request_with_current_permission() {
    Builder::new_multi_thread().worker_threads(1).enable_all().build().unwrap().block_on(async {
        let address = reserved_address();
        let refusals = Arc::new(Mutex::new(Vec::<IngressError<Query>>::new()));
        let observed = Arc::clone(&refusals);
        let mut node = Application::new(Counter::new().stop_on_shutdown())
            .open_node(configuration(&address, &EndpointRole::Receiver)).await
            .unwrap_or_else(|_| panic!("configured actual Counter Node"));
        let (execution, receiving) = node.execute_with(|application| async move {
            let target = application.root().established_recipient();
            let target_address = application.root().address();
            let mut ordinary = application.export_client::<_, CounterValue>(target.clone())
                .unwrap_or_else(|_| panic!("independent actual Counter client"));
            let mut processing = application.external::<CounterProcessed>().unwrap();
            let mut export = application.export::<_, CounterValue, u64>(target, "bombay/pressure/counter",
                ExportLimits { queued_queries: NonZeroUsize::new(2).unwrap(),
                    retained_queries: NonZeroUsize::new(2).unwrap(),
                    retained_requests: NonZeroUsize::new(2).unwrap() },
                u64::eq, move |original| observed.lock().unwrap().push(original)).await
                .unwrap_or_else(|_| panic!("actual Node-associated Counter export"));
            let denied_bytes = Box::<[u8]>::from([31, 37]);
            let denied_allocation = denied_bytes.as_ptr();
            let allowed_bytes = Box::<[u8]>::from([41, 43]);
            let allowed_allocation = allowed_bytes.as_ptr();
            let (denied, denied_duplicate) = export.retain(denied_bytes, Box::from([5]), 17)
                .unwrap_or_else(|_| panic!("first retained opaque request"));
            let (allowed, allowed_duplicate) = export.retain(allowed_bytes, Box::from([7]), 19)
                .unwrap_or_else(|_| panic!("second retained opaque request"));
            let permission = RefCell::new(Ok::<(), u64>(()));
            let constructions = Cell::new(0usize);
            let mut denied_recipient = None;
            let mut allowed_recipient = None;
            // Work stays on block_on's calling thread. This task occupies the sole worker.
            // Dropping the only release sender, including during unwind, releases recv.
            let (release_worker, released) = release::channel::<()>();
            let (announced, entered) = oneshot::channel();
            let worker = tokio::spawn(async move {
                let announced = announced.send(());
                let released = released.recv();
                (announced, released)
            });
            let entered = entered.await;
            // No awaits from this point until the only release sender is dropped.
            let prefix = (0..1_024).map(|_| ordinary.try_send(CounterMessage::Increment))
                .collect::<Vec<_>>();
            let first_denied = export.attempt(&denied, |_| Ok::<_, u64>(permission.borrow()), |reply_to| {
                constructions.set(constructions.get() + 1);
                denied_recipient = Some(reply_to.clone());
                CounterMessage::Read(reply_to)
            }).map(|_| ());
            let first_allowed = export.attempt(&allowed, |_| Ok::<_, u64>(permission.borrow()), |reply_to| {
                constructions.set(constructions.get() + 1);
                allowed_recipient = Some(reply_to.clone());
                CounterMessage::Read(reply_to)
            }).map(|_| ());
            let full_again = catch_unwind(AssertUnwindSafe(|| {
                export.retry(&allowed, |_| Ok::<_, u64>(PermissionLoan {
                    permission: permission.borrow(),
                })).map(|_| ())
            }));
            let full_recipient = match export.request(&allowed).unwrap().3 {
                Admission::Returned(Err(TrySendError::Full(CounterMessage::Read(reply_to)))) => Some(reply_to.clone()),
                _ => None,
            };
            *permission.borrow_mut() = Err(47);
            let refused_full = export.retry(&denied, |_| {
                let loan = permission.borrow();
                match *loan { Ok(()) => Ok(loan), Err(cause) => Err(cause) }
            }).map(|_| ());
            drop(release_worker);
            let worker = worker.await;
            // This unmodified awaited native path independently establishes freed capacity.
            let fenced_before = ordinary.send(CounterMessage::Read(ordinary.recipient())).await;
            let before = if fenced_before.is_ok() { ordinary.receive().await } else { None };
            let refused_after = export.retry(&denied, |_| {
                let loan = permission.borrow();
                match *loan { Ok(()) => Ok(loan), Err(cause) => Err(cause) }
            }).map(|_| ());
            *permission.borrow_mut() = Ok(());
            let admitted = export.retry(&allowed, |_| Ok::<_, u64>(permission.borrow())).map(|_| ());
            let repeated = export.retry(&allowed, |_| Ok::<_, u64>(permission.borrow())).map(|_| ());
            let repeated_attempt = export.attempt(&allowed, |_| Ok::<_, u64>(permission.borrow()), |reply_to| {
                constructions.set(constructions.get() + 1);
                CounterMessage::Read(reply_to)
            }).map(|_| ());
            // A false retry Ok must become a finite missing-reply assertion, not an empty wait.
            let fenced_after = ordinary.send(CounterMessage::IncrementAndReply {
                request_sequence: 53, reply_to: processing.recipient(),
            }).await;
            let after = if fenced_after.is_ok() { processing.receive().await } else { None };
            let (undeclared, session, mut incoming, rows) = export.undeclare().await;
            let mut rows = rows.collect::<Vec<_>>();
            let mut queries = Vec::new();
            let query_end = loop { match incoming.try_recv() {
                Ok(original) => queries.push(original), Err(cause) => break cause,
            }};
            let mut replies = Vec::new();
            for row in &mut rows {
                row.6.close_admission();
                let mut originals = Vec::new();
                while let Some(original) = row.6.receive().await { originals.push(original); }
                replies.push(originals);
            }
            ordinary.close_admission();
            let mut ordinary_tail = Vec::new();
            while let Some(original) = ordinary.receive().await { ordinary_tail.push(original); }
            processing.close_admission();
            let mut processing_tail = Vec::new();
            while let Some(original) = processing.receive().await { processing_tail.push(original); }
            let stopped = application.request_shutdown();
            let report = application.lifecycle().retirement().await;
            (target_address, denied, allowed, denied_duplicate, allowed_duplicate,
                denied_allocation, allowed_allocation, denied_recipient, allowed_recipient,
                entered, worker, prefix, first_denied, first_allowed, full_again, full_recipient,
                refused_full, refused_after, admitted, repeated, repeated_attempt, constructions.get(),
                fenced_before, before, fenced_after, after, undeclared, session, incoming,
                rows, queries, query_end, replies, ordinary, ordinary_tail, processing,
                processing_tail, stopped, report)
        }).unwrap_or_else(|_| panic!("actual Counter host entry"));
        let ((), outcome) = tokio::join!(execution, receiving);
        let (outcome, native, notifications, ()) = outcome.into_parts();
        let (output, cleanup) = outcome.try_into_completed()
            .unwrap_or_else(|_| panic!("original Counter Work"));
        let closed = node.close().await;
        let native = native.expect("original Counter retirement");
        let (target, denied, allowed, denied_duplicate, allowed_duplicate,
            denied_allocation, allowed_allocation, denied_recipient, allowed_recipient,
            entered, worker, prefix, first_denied, first_allowed, full_again, full_recipient,
            refused_full, refused_after, admitted, repeated, repeated_attempt, constructions,
            fenced_before, before, fenced_after, after, undeclared, session, _incoming,
            rows, queries, query_end, replies, _ordinary, ordinary_tail, _processing,
            processing_tail, stopped, report) = output;
        // All native owners and cleanup results have been acquired before these oracles.
        assert_completed(native, None);
        assert_notifications(&notifications);
        assert_report(report);
        assert!(cleanup.is_ok() && closed.is_ok() && undeclared.is_ok() && stopped.is_ok());
        assert!(entered.is_ok());
        let (announced, released) = worker.expect("actual gated worker joins");
        assert!(announced.is_ok() && released.is_err());
        assert_eq!(prefix.len(), 1_024);
        for original in &prefix { assert!(original.is_ok()); }
        assert!(first_denied.is_ok() && first_allowed.is_ok());
        let Err(permission_panic) = full_again else {
            panic!("the one permission-loan release must unwind");
        };
        assert_eq!(permission_panic.downcast_ref::<Result<(), u64>>(), Some(&Ok(())));
        assert_eq!(full_recipient.as_ref(), allowed_recipient.as_ref());
        assert!(matches!(refused_full, Err(RequestRefusal::Authority { cause: 47 })));
        assert!(matches!(refused_after, Err(RequestRefusal::Authority { cause: 47 })));
        assert!(admitted.is_ok() && repeated.is_ok() && repeated_attempt.is_ok());
        assert_eq!(constructions, 2);
        assert!(fenced_before.is_ok() && fenced_after.is_ok());
        assert_eq!(before.as_ref().map(|reply| (reply.from, reply.message)), Some((target, 1_024)));
        assert_eq!(after.as_ref().map(|reply| (reply.from, reply.message.request_sequence)), Some((target, 53)));
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0].0, denied);
        assert_eq!(rows[1].0, allowed);
        assert_eq!(rows[0].1.as_ptr(), denied_allocation);
        assert_eq!(rows[1].1.as_ptr(), allowed_allocation);
        assert_eq!(rows[0].1.as_ref(), [31, 37]);
        assert_eq!(rows[1].1.as_ref(), [41, 43]);
        assert_eq!((rows[0].2.as_ref(), rows[0].3), (&[5][..], 17));
        assert_eq!((rows[1].2.as_ref(), rows[1].3), (&[7][..], 19));
        assert!(matches!(&rows[0].4, Admission::Returned(Err(TrySendError::Full(CounterMessage::Read(reply_to))))
            if Some(reply_to) == denied_recipient.as_ref()));
        assert!(matches!(&rows[1].4, Admission::Returned(Ok(()))));
        assert!(rows.iter().all(|row| row.5.is_none()));
        let denied_endpoint = rows[0].6.recipient();
        let allowed_endpoint = rows[1].6.recipient();
        assert_eq!(Some(denied_endpoint), denied_recipient);
        assert_eq!(Some(allowed_endpoint), allowed_recipient);
        assert_eq!(replies[0].len(), 0);
        assert_eq!(replies[1].iter().map(|reply| (reply.from, reply.message)).collect::<Vec<_>>(), [(target, 1_024)]);
        assert!(denied_duplicate.is_none() && allowed_duplicate.is_none());
        assert!(queries.is_empty() && ordinary_tail.is_empty() && processing_tail.is_empty());
        assert!(matches!(query_end, mpsc::error::TryRecvError::Disconnected));
        assert!(session.is_closed() && refusals.lock().unwrap().is_empty());
    });
}

#[test]
#[expect(
    clippy::too_many_lines,
    reason = "one native pressure and retirement trace preserves the exact move-only Append through cleanup"
)]
fn ledger_full_retry_returns_the_same_allocation_when_the_exact_actor_has_retired() {
    Builder::new_multi_thread()
        .worker_threads(1)
        .enable_all()
        .build()
        .unwrap()
        .block_on(async {
            let address = reserved_address();
            let refusals = Arc::new(Mutex::new(Vec::<IngressError<Query>>::new()));
            let observed = Arc::clone(&refusals);
            let mut node = Application::new(
                MailLedger {
                    entries: Vec::new(),
                }
                .stop_on_shutdown(),
            )
            .open_node(configuration(&address, &EndpointRole::Receiver))
            .await
            .unwrap_or_else(|_| panic!("configured actual Ledger Node"));
            let (execution, receiving) = node
                .execute_with(|application| async move {
                    let target = application.root().established_recipient();
                    let target_address = application.root().address();
                    let mut ordinary = application
                        .export_client::<_, Receipts>(target.clone())
                        .unwrap_or_else(|_| panic!("independent actual Ledger client"));
                    let ordinary_address = ordinary.address();
                    let mut export = application
                        .export::<_, Receipts, u64>(
                            target,
                            "bombay/pressure/ledger",
                            ExportLimits {
                                queued_queries: NonZeroUsize::new(2).unwrap(),
                                retained_queries: NonZeroUsize::new(2).unwrap(),
                                retained_requests: NonZeroUsize::new(1).unwrap(),
                            },
                            u64::eq,
                            move |original| observed.lock().unwrap().push(original),
                        )
                        .await
                        .unwrap_or_else(|_| panic!("actual Node-associated Ledger export"));
                    let protected = Box::<[u8]>::from([59, 61]);
                    let protected_allocation = protected.as_ptr();
                    let (request, duplicate) = export
                        .retain(protected, Box::from([11]), 23)
                        .unwrap_or_else(|_| panic!("retained opaque Append request"));
                    let original = Box::<[u8]>::from([71, 73]);
                    let original_allocation = original.as_ptr();
                    let permission = RefCell::new(Ok::<(), u64>(()));
                    let (release_worker, released) = release::channel::<()>();
                    let (announced, entered) = oneshot::channel();
                    let worker = tokio::spawn(async move {
                        let announced = announced.send(());
                        let released = released.recv();
                        (announced, released)
                    });
                    let entered = entered.await;
                    let prefix = (0..1_024)
                        .map(|_| ordinary.try_send(LedgerCommand::Append(Box::from([13]))))
                        .collect::<Vec<_>>();
                    let first = export
                        .attempt(
                            &request,
                            |_| Ok::<_, u64>(permission.borrow()),
                            move |_| LedgerCommand::Append(original),
                        )
                        .map(|_| ());
                    let full_allocation = match export.request(&request).unwrap().3 {
                        Admission::Returned(Err(TrySendError::Full(LedgerCommand::Append(
                            original,
                        )))) => Some(original.as_ptr()),
                        _ => None,
                    };
                    *permission.borrow_mut() = Err(79);
                    let refused_full = export
                        .retry(&request, |_| {
                            let loan = permission.borrow();
                            match *loan {
                                Ok(()) => Ok(loan),
                                Err(cause) => Err(cause),
                            }
                        })
                        .map(|_| ());
                    drop(release_worker);
                    let worker = worker.await;
                    // Ledger's real Read drains and terminates; it is not a fabricated reusable fence.
                    let fenced = ordinary
                        .send(LedgerCommand::Read(ordinary.recipient()))
                        .await;
                    let reply = if fenced.is_ok() {
                        ordinary.receive().await
                    } else {
                        None
                    };
                    let report = application.lifecycle().retirement().await;
                    let refused_after = export
                        .retry(&request, |_| {
                            let loan = permission.borrow();
                            match *loan {
                                Ok(()) => Ok(loan),
                                Err(cause) => Err(cause),
                            }
                        })
                        .map(|_| ());
                    *permission.borrow_mut() = Ok(());
                    let retried = export
                        .retry(&request, |_| Ok::<_, u64>(permission.borrow()))
                        .map(|_| ());
                    let repeated = export
                        .retry(&request, |_| Ok::<_, u64>(permission.borrow()))
                        .map(|_| ());
                    let (undeclared, session, mut incoming, rows) = export.undeclare().await;
                    let mut rows = rows.collect::<Vec<_>>();
                    let mut queries = Vec::new();
                    let query_end = loop {
                        match incoming.try_recv() {
                            Ok(original) => queries.push(original),
                            Err(cause) => break cause,
                        }
                    };
                    let mut replies = Vec::new();
                    for row in &mut rows {
                        row.6.close_admission();
                        while let Some(original) = row.6.receive().await {
                            replies.push(original);
                        }
                    }
                    ordinary.close_admission();
                    let mut ordinary_tail = Vec::new();
                    while let Some(original) = ordinary.receive().await {
                        ordinary_tail.push(original);
                    }
                    (
                        target_address,
                        ordinary_address,
                        request,
                        duplicate,
                        protected_allocation,
                        original_allocation,
                        entered,
                        worker,
                        prefix,
                        first,
                        full_allocation,
                        refused_full,
                        refused_after,
                        retried,
                        repeated,
                        fenced,
                        reply,
                        report,
                        undeclared,
                        session,
                        incoming,
                        rows,
                        queries,
                        query_end,
                        replies,
                        ordinary,
                        ordinary_tail,
                    )
                })
                .unwrap_or_else(|_| panic!("actual Ledger host entry"));
            let ((), outcome) = tokio::join!(execution, receiving);
            let (outcome, native, notifications, ()) = outcome.into_parts();
            let (output, cleanup) = outcome
                .try_into_completed()
                .unwrap_or_else(|_| panic!("original Ledger Work"));
            let closed = node.close().await;
            let native = native.expect("original Ledger retirement");
            let (
                target,
                caller,
                request,
                duplicate,
                protected_allocation,
                original_allocation,
                entered,
                worker,
                prefix,
                first,
                full_allocation,
                refused_full,
                refused_after,
                retried,
                repeated,
                fenced,
                reply,
                report,
                undeclared,
                session,
                _incoming,
                rows,
                queries,
                query_end,
                replies,
                _ordinary,
                ordinary_tail,
            ) = output;
            assert_notifications(&notifications);
            assert_report(report);
            assert!(cleanup.is_ok() && closed.is_ok() && undeclared.is_ok());
            assert!(entered.is_ok());
            let (announced, released) = worker.expect("actual gated worker joins");
            assert!(announced.is_ok() && released.is_err());
            assert_eq!(prefix.len(), 1_024);
            for original in &prefix {
                assert!(original.is_ok());
            }
            assert!(first.is_ok());
            assert_eq!(full_allocation, Some(original_allocation));
            assert!(matches!(
                refused_full,
                Err(RequestRefusal::Authority { cause: 79 })
            ));
            assert!(matches!(
                refused_after,
                Err(RequestRefusal::Authority { cause: 79 })
            ));
            assert!(retried.is_ok() && repeated.is_ok() && fenced.is_ok());
            let reply = reply.expect("actual terminating Ledger reply");
            assert_eq!(reply.from, target);
            assert_eq!(reply.message, vec![caller; 1_024]);
            let ledger = native
                .1
                .custody()
                .expect("actual original Ledger state")
                .behavior
                .base();
            assert_eq!(ledger.entries.len(), 1_024);
            for (origin, original) in &ledger.entries {
                assert_eq!(*origin, caller);
                assert_eq!(original.as_ref(), [13]);
            }
            assert_completed(native, None);
            assert_eq!(rows.len(), 1);
            assert_eq!(rows[0].0, request);
            assert_eq!(rows[0].1.as_ptr(), protected_allocation);
            assert_eq!(rows[0].1.as_ref(), [59, 61]);
            assert_eq!((rows[0].2.as_ref(), rows[0].3), (&[11][..], 23));
            let Admission::Returned(Err(TrySendError::Closed(LedgerCommand::Append(original)))) =
                &rows[0].4
            else {
                panic!("retry after actual retirement must retain native Closed Append");
            };
            assert_eq!(original.as_ptr(), original_allocation);
            assert_eq!(original.as_ref(), [71, 73]);
            assert!(rows[0].5.is_none() && duplicate.is_none());
            assert!(queries.is_empty() && replies.is_empty() && ordinary_tail.is_empty());
            assert!(matches!(query_end, mpsc::error::TryRecvError::Disconnected));
            assert!(session.is_closed() && refusals.lock().unwrap().is_empty());
        });
}
