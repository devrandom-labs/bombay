//! Shared replay before concrete native construction; no provider/authentication claim.
//! Stop uses the actual same-handle root lifecycle; foreign/child denial is unproved.

#[path = "requests/keys.rs"]
mod keys;

use super::{Admission, ExportLimits};
use crate::actors::ActorExt;
use crate::application::{ActorInterface, ApplicationClient, ApplicationLifecycle};
use crate::behavior::{Behavior, EstablishedRecipient, Never, Protocol, User};
use crate::remote::counter::{Counter, CounterMessage, CounterProcessed, CounterValue};
use crate::remote::listening_counter_network;
use crate::{
    ActorRetirementReport, Application, Completion, ExternalActorError, MailAddr,
    RetirementAssessment,
};
use behavior_actors::{ShutdownRejection, StopOnShutdown};
use core::future::{Future, poll_fn};
use core::pin::pin;
use core::task::Poll;
use std::cell::Cell;
use std::num::NonZeroUsize;
use std::rc::Rc;
use std::time::Duration;
use tokio::runtime::Builder;
use tokio::time::timeout;
use zenoh::sample::Locality;

const WATCHDOG: Duration = Duration::from_secs(5);

enum ControlAdmission {
    Unattempted,
    Uncertain,
    Returned(Result<(), ShutdownRejection>),
}

#[derive(Clone, Copy, Debug)]
enum CounterOperation {
    Increment(u64),
    Read,
}

#[expect(
    clippy::option_option,
    reason = "distinguish no native acquisition from acquired EOF"
)]
enum CounterNative {
    Cold(CounterOperation),
    Entered(CounterOperation),
    Increment {
        client: ApplicationClient<EstablishedRecipient<Counter>, CounterProcessed>,
        sequence: u64,
        admission: Admission<CounterMessage>,
        processed: Option<Option<User<MailAddr, <CounterProcessed as Protocol>::Msg>>>,
    },
    Read {
        client: ApplicationClient<EstablishedRecipient<Counter>, CounterValue>,
        admission: Admission<CounterMessage>,
        value: Option<Option<User<MailAddr, u64>>>,
    },
    Stop {
        lifecycle: ApplicationLifecycle<Counter, <StopOnShutdown<Counter> as Behavior>::Event>,
        admission: ControlAdmission,
        report: Option<ActorRetirementReport>,
    },
    ConstructionRefused {
        operation: CounterOperation,
        target: EstablishedRecipient<Counter>,
        cause: ExternalActorError,
    },
}

#[expect(
    clippy::option_option,
    reason = "preserve every actual native acquisition after client retirement"
)]
enum CounterOutcome {
    Increment(
        Admission<CounterMessage>,
        Option<Option<User<MailAddr, <CounterProcessed as Protocol>::Msg>>>,
    ),
    Read(
        Admission<CounterMessage>,
        Option<Option<User<MailAddr, u64>>>,
    ),
    Stop(ControlAdmission, Option<ActorRetirementReport>),
    Uncompleted(CounterNative),
}

impl CounterNative {
    fn attempt<'receipt, Guard, Error>(
        &mut self,
        receipt: &'receipt Rc<u64>,
        interface: &ActorInterface<EstablishedRecipient<Counter>>,
        constructors: &Cell<usize>,
        current: impl FnOnce(&'receipt Rc<u64>) -> Result<Guard, Error>,
    ) -> Result<(), Error> {
        let permission = current(receipt)?;
        if let Self::Cold(operation) = self {
            let operation = *operation;
            *self = Self::Entered(operation);
            constructors.set(constructors.get() + 1);
            *self = match operation {
                CounterOperation::Increment(sequence) => {
                    match interface.client_to::<_, CounterProcessed>(interface.api().clone()) {
                        Ok(client) => Self::Increment {
                            client,
                            sequence,
                            admission: Admission::Unattempted,
                            processed: None,
                        },
                        Err((target, cause)) => Self::ConstructionRefused {
                            operation,
                            target,
                            cause,
                        },
                    }
                }
                CounterOperation::Read => {
                    match interface.client_to::<_, CounterValue>(interface.api().clone()) {
                        Ok(client) => Self::Read {
                            client,
                            admission: Admission::Unattempted,
                            value: None,
                        },
                        Err((target, cause)) => Self::ConstructionRefused {
                            operation,
                            target,
                            cause,
                        },
                    }
                }
            };
        }
        match self {
            Self::Increment {
                client,
                sequence,
                admission,
                ..
            } => {
                admission.attempt(
                    || CounterMessage::IncrementAndReply {
                        request_sequence: *sequence,
                        reply_to: client.recipient(),
                    },
                    |command| client.try_send(command),
                );
            }
            Self::Read {
                client, admission, ..
            } => {
                admission.attempt(
                    || CounterMessage::Read(client.recipient()),
                    |command| client.try_send(command),
                );
            }
            Self::Stop {
                lifecycle,
                admission,
                ..
            } => {
                if matches!(admission, ControlAdmission::Unattempted) {
                    *admission = ControlAdmission::Uncertain;
                    let original = lifecycle.request_shutdown();
                    *admission = ControlAdmission::Returned(original);
                }
            }
            Self::Cold(_) | Self::Entered(_) | Self::ConstructionRefused { .. } => {}
        }
        drop(permission);
        Ok(())
    }

    fn awaits_reply(&self) -> bool {
        match self {
            Self::Increment {
                admission,
                processed,
                ..
            } => {
                processed.is_none()
                    && matches!(
                        admission,
                        Admission::Uncertain | Admission::Returned(Ok(()))
                    )
            }
            Self::Read {
                admission, value, ..
            } => {
                value.is_none()
                    && matches!(
                        admission,
                        Admission::Uncertain | Admission::Returned(Ok(()))
                    )
            }
            Self::Stop {
                admission, report, ..
            } => {
                report.is_none()
                    && matches!(
                        admission,
                        ControlAdmission::Uncertain | ControlAdmission::Returned(Ok(()))
                    )
            }
            Self::Cold(_) | Self::Entered(_) | Self::ConstructionRefused { .. } => false,
        }
    }

    async fn receive(&mut self) {
        if !self.awaits_reply() {
            return;
        }
        match self {
            Self::Increment {
                client, processed, ..
            } => receive_reply(client, processed).await,
            Self::Read { client, value, .. } => receive_reply(client, value).await,
            Self::Stop {
                lifecycle, report, ..
            } => {
                let mut receiving = pin!(lifecycle.retirement());
                poll_fn(|context| {
                    receiving.as_mut().poll(context).map(|original| {
                        *report = Some(original);
                    })
                })
                .await;
            }
            Self::Cold(_) | Self::Entered(_) | Self::ConstructionRefused { .. } => {}
        }
    }
}

#[expect(
    clippy::option_option,
    reason = "native acquisition distinguishes no fact from acquired EOF"
)]
async fn receive_reply<Target, Replies>(
    client: &mut ApplicationClient<EstablishedRecipient<Target>, Replies>,
    acquired: &mut Option<Option<User<MailAddr, Replies::Msg>>>,
) where
    Target: Protocol<Addr = MailAddr>,
    Replies: Protocol<Addr = MailAddr>,
    Replies::Msg: Send,
{
    let mut receiving = pin!(client.receive());
    poll_fn(|context| match receiving.as_mut().poll(context) {
        Poll::Pending => Poll::Pending,
        Poll::Ready(original) => {
            *acquired = Some(original);
            Poll::Ready(())
        }
    })
    .await;
}

#[test]
#[expect(
    clippy::too_many_lines,
    reason = "one actual shared replay trace retires every native consumer before its independent oracles"
)]
fn counter_operations_share_replay_before_native_construction() {
    let receipt = Rc::new(11_u64);
    let receipt_allocation = Rc::as_ptr(&receipt);
    let protected: Box<[u8]> = Box::from([13_u8]);
    let proof: Box<[u8]> = Box::from([17_u8]);
    let protected_allocation = protected.as_ptr();
    let proof_allocation = proof.as_ptr();
    let duplicate: Box<[u8]> = Box::from([13_u8]);
    let duplicate_allocation = duplicate.as_ptr();
    let conflict: Box<[u8]> = Box::from([19_u8]);
    let conflict_allocation = conflict.as_ptr();
    let duplicate_proof: Box<[u8]> = Box::from([29_u8]);
    let conflict_proof: Box<[u8]> = Box::from([31_u8]);
    let duplicate_receipt = Rc::new(11_u64);
    let conflict_receipt = Rc::new(11_u64);
    let duplicate_originals = (duplicate_proof.as_ptr(), Rc::as_ptr(&duplicate_receipt));
    let conflict_originals = (conflict_proof.as_ptr(), Rc::as_ptr(&conflict_receipt));
    let read = (
        Box::<[u8]>::from([37]),
        Box::<[u8]>::from([41]),
        Rc::new(43),
    );
    let read_originals = (read.0.as_ptr(), read.1.as_ptr(), Rc::as_ptr(&read.2));
    let stop = (
        Box::<[u8]>::from([47]),
        Box::<[u8]>::from([53]),
        Rc::new(59),
    );
    let stop_originals = (stop.0.as_ptr(), stop.1.as_ptr(), Rc::as_ptr(&stop.2));
    let constructors = Cell::new(0);
    let host = Builder::new_multi_thread()
        .worker_threads(1)
        .enable_all()
        .build()
        .unwrap();
    let observations = host.block_on(async {
        let (execution, receiving, closing) = Application::new(Counter::new().stop_on_shutdown())
            .execute_remote(listening_counter_network(), move |application| async move {
                let target = application.root().address();
                let mut export = application.export::<_, CounterNative, Rc<u64>>(
                    application.root().established_recipient(), "bombay/requests/counter",
                    ExportLimits {
                        queued_queries: NonZeroUsize::new(1).unwrap(),
                        retained_queries: NonZeroUsize::new(1).unwrap(),
                        retained_requests: NonZeroUsize::new(3).unwrap(),
                    },
                    |left, right| **left == **right,
                    |original| panic!("the actual local query must not be refused: {original}"),
                ).await.unwrap_or_else(|_| panic!("actual associated Counter export"));
                let (increment, first) = export.retain(
                    protected, proof, receipt, CounterNative::Cold(CounterOperation::Increment(23)),
                ).unwrap_or_else(|_| panic!("first cold request fits"));
                let duplicate = export.retain(
                    duplicate, duplicate_proof, duplicate_receipt, CounterNative::Cold(CounterOperation::Read),
                );
                let conflict = export.retain(
                    conflict, conflict_proof, conflict_receipt, CounterNative::Cold(CounterOperation::Read),
                );
                let admitted = {
                    let (receipt, native, interface) = export.request_mut(&increment).unwrap();
                    native.attempt(receipt, interface, &constructors, |receipt| Ok::<_, Never>(receipt))
                };
                let mut query = None;
                let mut completed = None;
                timeout(WATCHDOG, export.receive(&mut query, &mut completed,
                    CounterNative::awaits_reply, async |native| native.receive().await))
                    .await.expect("actual processing receipt");
                let processed = completed.take();
                let repeated = {
                    let (receipt, native, interface) = export.request_mut(&increment).unwrap();
                    native.attempt(receipt, interface, &constructors, |receipt| Ok::<_, Never>(receipt))
                };
                let denied = {
                    let (receipt, native, interface) = export.request_mut(&increment).unwrap();
                    native.attempt(receipt, interface, &constructors, |receipt| Err::<(), _>(receipt))
                        .err().map(Rc::as_ptr)
                };
                let (read, read_duplicate) = export.retain(
                    read.0, read.1, read.2, CounterNative::Cold(CounterOperation::Read),
                ).unwrap_or_else(|_| panic!("distinct Read identity fits"));
                let read_admitted = {
                    let (receipt, native, interface) = export.request_mut(&read).unwrap();
                    native.attempt(receipt, interface, &constructors, |receipt| Ok::<_, Never>(receipt))
                };
                timeout(WATCHDOG, export.receive(&mut query, &mut completed,
                    CounterNative::awaits_reply, async |native| native.receive().await))
                    .await.expect("actual value receipt");
                let read_completed = completed.take();
                let issued = export.session.get("bombay/requests/counter")
                    .allowed_destination(Locality::SessionLocal).await;
                timeout(WATCHDOG, export.receive(&mut query, &mut completed,
                    CounterNative::awaits_reply, async |native| native.receive().await))
                    .await.expect("a new actual query progresses after both native acquisitions");
                let acquired_query = query.take();
                let query_completed = completed.take();
                let stop_native = CounterNative::Stop {
                    lifecycle: application.lifecycle(),
                    admission: ControlAdmission::Unattempted, report: None,
                };
                let (stop, stop_duplicate) = export.retain(
                    stop.0, stop.1, stop.2, stop_native,
                ).unwrap_or_else(|_| panic!("distinct exact root Stop identity fits"));
                let stop_admitted = {
                    let (receipt, native, interface) = export.request_mut(&stop).unwrap();
                    native.attempt(receipt, interface, &constructors, |receipt| Ok::<_, Never>(receipt))
                };
                timeout(WATCHDOG, export.receive(&mut query, &mut completed,
                    CounterNative::awaits_reply, async |native| native.receive().await))
                    .await.expect("actual joined-root report");
                let stop_completed = completed.take();
                let (undeclared, session, mut incoming, rows) = export.undeclare().await;
                let mut retained = Vec::new();
                for (key, protected, proof, receipt, native) in rows {
                    let mut tail = Vec::new();
                    let outcome = match native {
                        CounterNative::Increment { mut client, admission, processed, .. } => {
                            client.close_admission();
                            while let Some(original) = client.receive().await { tail.push(original.from); }
                            drop(client);
                            CounterOutcome::Increment(admission, processed)
                        }
                        CounterNative::Read { mut client, admission, value } => {
                            client.close_admission();
                            while let Some(original) = client.receive().await { tail.push(original.from); }
                            drop(client);
                            CounterOutcome::Read(admission, value)
                        }
                        CounterNative::Stop { admission, report, .. } => CounterOutcome::Stop(admission, report),
                        original => CounterOutcome::Uncompleted(original),
                    };
                    retained.push((key, protected, proof, receipt, outcome, tail));
                }
                let mut prefix = Vec::new();
                while let Some(original) = incoming.recv().await { prefix.push(original); }
                let query_observation = acquired_query.as_ref().map(|query| query.as_ref()
                    .map(|(query, permit)| (query.key_expr().as_str().to_owned(), permit.num_permits())));
                drop((acquired_query, issued, prefix, incoming, session));
                move || {
                    assert_eq!(constructors.get(), 2);
                    assert!(first.is_none() && admitted.is_ok() && repeated.is_ok());
                    let (duplicate_key, Some((bytes, proof, receipt, native))) = duplicate
                        .unwrap_or_else(|_| panic!("exact duplicate remains accepted")) else {
                        panic!("duplicate must return all of its originals");
                    };
                    assert!(duplicate_key == increment);
                    assert_eq!(bytes.as_ptr(), duplicate_allocation);
                    assert_eq!((proof.as_ptr(), Rc::as_ptr(&receipt)), duplicate_originals);
                    assert_eq!((bytes.as_ref(), proof.as_ref(), *receipt), (&[13][..], &[29][..], 11));
                    assert!(matches!(native, CounterNative::Cold(CounterOperation::Read)));
                    let Err(((bytes, proof, receipt, native), super::ExportError::Conflict)) = conflict else {
                        panic!("cross-operation reuse must retain its conflicting cold original");
                    };
                    assert_eq!(bytes.as_ptr(), conflict_allocation);
                    assert_eq!((proof.as_ptr(), Rc::as_ptr(&receipt)), conflict_originals);
                    assert_eq!((bytes.as_ref(), proof.as_ref(), *receipt), (&[19][..], &[31][..], 11));
                    assert!(matches!(native, CounterNative::Cold(CounterOperation::Read)));
                    assert!(processed.as_ref() == Some(&increment));
                    assert_eq!(denied, Some(receipt_allocation));
                    assert!(read_admitted.is_ok() && stop_admitted.is_ok());
                    assert!(read_duplicate.is_none() && stop_duplicate.is_none());
                    assert!(read_completed.as_ref() == Some(&read));
                    assert!(stop_completed.as_ref() == Some(&stop));
                    assert!(query.is_none() && completed.is_none() && undeclared.is_ok());
                    assert!(query_completed.is_none());
                    assert!(matches!(query_observation, Some(Some((ref key, 1))) if key == "bombay/requests/counter"));
                    assert!(retained.iter().map(|row| &row.0).eq([&increment, &read, &stop]));
                    for (key, protected, proof, receipt, native, tail) in retained {
                        assert!(tail.is_empty());
                        match native {
                            CounterOutcome::Increment(admission, processed) => {
                                assert!(key == increment);
                                assert_eq!(protected.as_ptr(), protected_allocation);
                                assert_eq!(proof.as_ptr(), proof_allocation);
                                assert_eq!(Rc::as_ptr(&receipt), receipt_allocation);
                                assert_eq!((protected.as_ref(), proof.as_ref(), *receipt), (&[13][..], &[17][..], 11));
                                assert!(matches!(admission, Admission::Returned(Ok(()))));
                                assert!(matches!(processed, Some(Some(ref reply)) if reply.from == target && reply.message.request_sequence == 23));
                            }
                            CounterOutcome::Read(admission, value) => {
                                assert!(key == read);
                                assert_eq!((protected.as_ptr(), proof.as_ptr(), Rc::as_ptr(&receipt)), read_originals);
                                assert_eq!((protected.as_ref(), proof.as_ref(), *receipt), (&[37][..], &[41][..], 43));
                                assert!(matches!(admission, Admission::Returned(Ok(()))));
                                assert!(matches!(value, Some(Some(ref reply)) if (reply.from, reply.message) == (target, 1)));
                            }
                            CounterOutcome::Stop(admission, report) => {
                                assert!(key == stop);
                                assert_eq!((protected.as_ptr(), proof.as_ptr(), Rc::as_ptr(&receipt)), stop_originals);
                                assert_eq!((protected.as_ref(), proof.as_ref(), *receipt), (&[47][..], &[53][..], 59));
                                assert!(matches!(admission, ControlAdmission::Returned(Ok(()))));
                                assert_eq!(report.unwrap().retirement(), RetirementAssessment::Established);
                            }
                            CounterOutcome::Uncompleted(CounterNative::ConstructionRefused { operation, target, cause }) => panic!("actual native construction refused {operation:?} for {target:?}: {cause}"),
                            CounterOutcome::Uncompleted(CounterNative::Cold(operation) | CounterNative::Entered(operation)) => panic!("native operation did not complete: {operation:?}"),
                            CounterOutcome::Uncompleted(other) => { drop(other); panic!("unexpected uncompleted native"); },
                        }
                    }
                }
            }).unwrap_or_else(|_| panic!("actual entered remote host"));
        execution.await;
        let (outcome, native, notifications, ()) = receiving.await;
        let closed = closing.await;
        let (observations, cleanup) = outcome.try_into_completed()
            .unwrap_or_else(|_| panic!("actual Work completes with its independent observations"));
        move || {
            assert!(cleanup.is_ok());
            let notifications = notifications.unwrap();
            assert!(notifications.termination.is_ok() && notifications.retirement.is_ok());
            let (_, retirement) = native.unwrap();
            assert_eq!(retirement.completion(), Some(&Completion::Stopped));
            let (joined, closed) = closed.unwrap().unwrap_or_else(|_| panic!("actual connection opens"));
            assert!(joined.is_ok() && matches!(closed, Ok(Ok(()))));
            observations();
        }
    });
    drop(host);
    observations();
}
