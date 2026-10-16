use super::super::{Counter, MailLedger};
use crate::actors::ActorExt;
use crate::application::execution::ApplicationWorkPresence;
use crate::behavior::Never;
use crate::{Application, ApplicationStagingError, WorkOutcome};
use core::future::{Future, Ready, poll_fn};
use core::pin::pin;
use core::task::Poll;
use std::cell::Cell;
use std::panic::{AssertUnwindSafe, catch_unwind, panic_any};
use std::ptr;
use std::rc::Rc;
use std::sync::Arc;
use tokio::runtime::Builder;

struct LedgerRole;

impl Drop for LedgerRole {
    fn drop(&mut self) {
        panic!("original ledger role disposal failure");
    }
}

struct InvocationDisposal {
    cause: Option<Arc<Vec<u8>>>,
}

impl Drop for InvocationDisposal {
    fn drop(&mut self) {
        if let Some(cause) = self.cause.take() {
            panic_any(cause);
        }
    }
}

crate::topology! {
    topology {
        root: Counter {}
        hosts { ledger: MailLedger; }
        members { Ledger(LedgerRole) => ledger; }
        ledger {}
    }
}

#[test]
fn invocation_disposal_keeps_original_staging_rejection() {
    let observations = {
        let host = Builder::new_current_thread().enable_all().build().unwrap();
        host.block_on(async {
            let calls = Rc::new(Cell::new(0));
            let invoked = Rc::clone(&calls);
            let work = Box::new(vec![139_u8, 149]);
            let original_work = ptr::from_ref(work.as_ref());
            let cause = Arc::new(vec![151_u8, 157]);
            let original_cause = Arc::as_ptr(&cause);
            let disposal = InvocationDisposal { cause: Some(cause) };
            let application = Application::with_topology(
                Counter::new().stop_on_shutdown(),
                topology::Topology::new(),
            )
            .child(
                LedgerRole,
                MailLedger {
                    entries: Vec::new(),
                },
            );
            let (execution, (receiving, received_families)) = application
                .execute_with_entities_invocation(
                    ApplicationWorkPresence::Supplied(work),
                    move |work: ApplicationWorkPresence<Box<Vec<u8>>, Never>, application| {
                        let _original_disposal = &disposal;
                        invoked.set(invoked.get() + 1);
                        (
                            ApplicationWorkPresence::Supplied(async move {
                                (work.into_supplied(), application.request_shutdown())
                            }),
                            None::<Ready<()>>,
                        )
                    },
                )
                .unwrap_or_else(|_| panic!("the actual host is entered"));
            let mut execution = Box::pin(execution);
            let fault = poll_fn(|context| {
                match catch_unwind(AssertUnwindSafe(|| execution.as_mut().poll(context))) {
                    Ok(Poll::Pending) => Poll::Pending,
                    actual => Poll::Ready(actual),
                }
            })
            .await;
            drop(execution);
            let (outcome, families) = {
                let mut receiving = pin!(receiving);
                let mut received_families = pin!(received_families);
                poll_fn(|context| {
                    Poll::Ready((
                        receiving.as_mut().poll(context),
                        received_families.as_mut().poll(context),
                    ))
                })
                .await
            };
            move || {
                let (Poll::Ready((work, root, notifications)), Poll::Ready(())) =
                    (outcome, families)
                else {
                    panic!("both original receivers must be ready after execution surrender");
                };
                let work = work.into_supplied_with_entities();
                assert!(root.is_err());
                assert!(notifications.is_err());
                let WorkOutcome::StagingRejected {
                    inputs: (_counter, _spaces, (), work, failure, _scope),
                } = work
                else {
                    panic!("staging rejection must own original Work before Invoke disposal");
                };
                assert_eq!(ptr::from_ref(work.as_ref()), original_work);
                assert_eq!(work.as_slice(), [139, 149]);
                assert_eq!(calls.get(), 0);
                let Err(cause) = fault else {
                    panic!("the original invocation disposal must unwind");
                };
                let cause = cause
                    .downcast_ref::<Arc<Vec<u8>>>()
                    .expect("original typed invocation cause");
                assert_eq!(Arc::as_ptr(cause), original_cause);
                assert_eq!(cause.as_slice(), [151, 157]);
                let ApplicationStagingError::RoleDisposalPanicked {
                    actor,
                    children: _children,
                    cause,
                    ..
                } = failure
                else {
                    panic!("the independently caught original role disposal survives");
                };
                assert_eq!(actor.entries, []);
                assert_eq!(
                    cause.downcast_ref::<&'static str>(),
                    Some(&"original ledger role disposal failure")
                );
            }
        })
    };
    observations();
}
