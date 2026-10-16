//! Unrelated send-only substitution and original custody, not business processing.
use super::{Admission, ExportLimits};
use crate::actors::ActorExt;
use crate::application::{ActorInterface, ExternalActor};
use crate::behavior::{BehaviorBase, EstablishedRecipient, MessageProtocol, Never};
use crate::remote::listening_counter_network;
use crate::{
    ActorCustody, Application, Completion, ExternalActorError, MailAddr, RetirementAssessment,
    ShutdownLedger,
};
use behavior_actors::StopOnShutdown;
use std::cell::Cell;
use std::num::NonZeroUsize;
use std::rc::Rc;
use std::time::Duration;
use tokio::runtime::Builder;
use tokio::time::timeout;

enum LedgerNative {
    Cold([u64; 2]),
    Entered([u64; 2]),
    Delivery {
        entries: [u64; 2],
        sender: ExternalActor<MessageProtocol<MailAddr, Never>>,
        admission: Admission<Vec<u64>>,
    },
    ConstructionRefused {
        entries: [u64; 2],
        cause: ExternalActorError,
    },
}

impl LedgerNative {
    fn attempt<'receipt, Guard, Error>(
        &mut self,
        receipt: &'receipt Rc<u64>,
        interface: &ActorInterface<EstablishedRecipient<ShutdownLedger>>,
        constructions: &Cell<usize>,
        allocation: &Cell<Option<*const u64>>,
        current: impl FnOnce(&'receipt Rc<u64>) -> Result<Guard, Error>,
    ) -> Result<(), Error> {
        let permission = current(receipt)?;
        if let Self::Cold(entries) = self {
            let entries = *entries;
            *self = Self::Entered(entries);
            constructions.set(constructions.get() + 1);
            *self = match interface.external::<MessageProtocol<MailAddr, Never>>() {
                Ok(sender) => Self::Delivery {
                    entries,
                    sender,
                    admission: Admission::Unattempted,
                },
                Err(cause) => Self::ConstructionRefused { entries, cause },
            };
        }
        if let Self::Delivery {
            entries,
            sender,
            admission,
        } = self
        {
            admission.attempt(
                || {
                    let original = entries.to_vec();
                    allocation.set(Some(original.as_ptr()));
                    original
                },
                |original| sender.try_send(interface.api(), original),
            );
        }
        drop(permission);
        Ok(())
    }
}

#[test]
#[expect(
    clippy::too_many_lines,
    reason = "retire the real send-only native and Application before checking original terminal custody"
)]
fn send_only_ledger_retains_one_original_in_actual_terminal_custody() {
    let protected: Box<[u8]> = Box::from([67_u8, 71]);
    let proof: Box<[u8]> = Box::from([73_u8]);
    let receipt = Rc::new(79_u64);
    let originals = (protected.as_ptr(), proof.as_ptr(), Rc::as_ptr(&receipt));
    let constructions = Cell::new(0);
    let allocation = Cell::new(None);
    let host = Builder::new_multi_thread()
        .worker_threads(1)
        .enable_all()
        .build()
        .unwrap();
    let observations = host.block_on(async {
        let root = ShutdownLedger {
            entries: vec![83],
            received: Vec::new(),
        }
        .stop_on_shutdown();
        let (execution, receiving, closing) = Application::new(root)
            .execute_remote(listening_counter_network(), move |application| async move {
                let mut export = application
                    .export::<_, LedgerNative, Rc<u64>>(
                        application.root().established_recipient(),
                        "bombay/requests/ledger",
                        ExportLimits {
                            queued_queries: NonZeroUsize::new(1).unwrap(),
                            retained_queries: NonZeroUsize::new(1).unwrap(),
                            retained_requests: NonZeroUsize::new(1).unwrap(),
                        },
                        |left, right| **left == **right,
                        |original| panic!("this send-only witness issues no queries: {original}"),
                    )
                    .await
                    .unwrap_or_else(|_| panic!("actual associated Ledger export"));
                let (issued, duplicate) = export
                    .retain(protected, proof, receipt, LedgerNative::Cold([89, 97]))
                    .unwrap_or_else(|_| panic!("one cold Ledger request fits"));
                let admitted = {
                    let (receipt, native, interface) = export.request_mut(&issued).unwrap();
                    native.attempt(receipt, interface, &constructions, &allocation, |receipt| {
                        Ok::<_, Never>(receipt)
                    })
                };
                let repeated = {
                    let (receipt, native, interface) = export.request_mut(&issued).unwrap();
                    native.attempt(receipt, interface, &constructions, &allocation, |receipt| {
                        Ok::<_, Never>(receipt)
                    })
                };
                let stopped = application.request_shutdown();
                let report = timeout(Duration::from_secs(5), application.lifecycle().retirement())
                    .await
                    .expect("actual joined Ledger retirement");
                let (undeclared, session, mut incoming, rows) = export.undeclare().await;
                let mut retained = Vec::new();
                for (key, protected, proof, receipt, native) in rows {
                    let disposition = match native {
                        LedgerNative::Delivery {
                            sender, admission, ..
                        } => {
                            let origin = sender.address();
                            drop(sender);
                            Ok((origin, admission))
                        }
                        original => Err(original),
                    };
                    retained.push((key, protected, proof, receipt, disposition));
                }
                let mut prefix = Vec::new();
                while let Some(original) = incoming.recv().await {
                    prefix.push(original);
                }
                let unread_queries = prefix.len();
                drop((prefix, incoming, session));
                move |custody: &ActorCustody<StopOnShutdown<ShutdownLedger>, Never, Vec<Never>>| {
                    assert!(admitted.is_ok() && repeated.is_ok() && stopped.is_ok());
                    assert!(duplicate.is_none() && undeclared.is_ok());
                    assert_eq!(constructions.get(), 1);
                    assert_eq!(unread_queries, 0);
                    assert_eq!(report.retirement(), RetirementAssessment::Established);
                    assert_eq!(retained.len(), 1);
                    let (key, protected, proof, receipt, disposition) =
                        retained.into_iter().next().unwrap();
                    assert!(key == issued);
                    assert_eq!(
                        (protected.as_ptr(), proof.as_ptr(), Rc::as_ptr(&receipt)),
                        originals
                    );
                    assert_eq!(
                        (protected.as_ref(), proof.as_ref(), *receipt),
                        (&[67, 71][..], &[73][..], 79)
                    );
                    let (origin, admission) = match disposition {
                        Ok(original) => original,
                        Err(LedgerNative::Cold(entries) | LedgerNative::Entered(entries)) => {
                            panic!("Ledger native remains unattempted or entered: {entries:?}")
                        }
                        Err(LedgerNative::ConstructionRefused { entries, cause }) => {
                            panic!("Ledger sending identity refused {entries:?}: {cause}")
                        }
                        Err(LedgerNative::Delivery { .. }) => {
                            panic!("native sender must already be disposed")
                        }
                    };
                    assert!(matches!(admission, Admission::Returned(Ok(()))));
                    assert_eq!(custody.behavior.base().entries.as_slice(), [83]);
                    let recorded = &custody.behavior.base().received;
                    assert_eq!(recorded.len() + custody.user.len(), 1);
                    let original = recorded.iter().chain(&custody.user).next().unwrap();
                    assert_eq!(original.from, origin);
                    assert_eq!(Some(original.message.as_ptr()), allocation.get());
                    assert_eq!(original.message.as_slice(), [89, 97]);
                }
            })
            .unwrap_or_else(|_| panic!("actual entered remote host"));
        execution.await;
        let (outcome, native, notifications, ()) = receiving.await;
        let closed = closing.await;
        let (observations, cleanup) = outcome
            .try_into_completed()
            .unwrap_or_else(|_| panic!("actual Ledger Work completes"));
        move || {
            assert!(cleanup.is_ok());
            let notifications = notifications.unwrap();
            assert!(notifications.termination.is_ok() && notifications.retirement.is_ok());
            let (_, retirement) = native.unwrap();
            assert_eq!(retirement.completion(), Some(&Completion::Stopped));
            let (joined, closed) = closed
                .unwrap()
                .unwrap_or_else(|_| panic!("actual connection opens"));
            assert!(joined.is_ok() && matches!(closed, Ok(Ok(()))));
            observations(
                retirement
                    .custody()
                    .expect("actual Ledger terminal custody"),
            );
        }
    });
    drop(host);
    observations();
}
