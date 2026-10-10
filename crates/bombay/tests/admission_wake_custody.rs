use bombay::behavior::{
    BehaviorBase, ClassifySettlement, EstablishedDelivery, MessageProtocol, NoSends,
    SettlementStatus, Step,
};
use bombay::prelude::*;
use bombay::{ActorNotificationReceipts, ApplicationOutcome, TrySendError};
use core::{
    future::Future,
    pin::pin,
    ptr,
    task::{Context, Poll},
};
use std::panic::{AssertUnwindSafe, catch_unwind, resume_unwind};
use std::sync::{Arc, Mutex};
use std::task::{Wake, Waker};
use tokio::runtime::Builder;

mod application_support;
use application_support::RootTerminal;

struct AdmissionWakeCause {
    retained: Arc<()>,
}

struct AdmissionWake {
    original: Mutex<Option<Box<AdmissionWakeCause>>>,
}

impl Wake for AdmissionWake {
    fn wake(self: Arc<Self>) {
        let original = self
            .original
            .lock()
            .expect("the notification cause remains exclusively owned")
            .take()
            .expect("the first notification transfers its original cause");
        resume_unwind(original);
    }
}

type Payloads = MessageProtocol<MailAddr, Box<[u8]>>;
type Replies = MessageProtocol<MailAddr, u64>;

enum CounterCommand {
    Read(EstablishedRecipient<Replies>),
}

#[derive(Default)]
struct Counter {
    reads: Vec<MailAddr>,
}

#[bombay::actor(sends = pub(crate) { replies: Vec<EstablishedDelivery<Replies>>, })]
#[allow(
    clippy::unnecessary_wraps,
    reason = "the pure owning Behavior contract returns typed Actions"
)]
impl Counter {
    fn receive(&mut self, from: MailAddr, command: CounterCommand) -> BehaviorActed<Self> {
        let CounterCommand::Read(reply_to) = command;
        self.reads.push(from);
        Ok(Actions::cont().send_replies(EstablishedDelivery::new(reply_to, 42)))
    }
}

#[test]
#[expect(
    clippy::too_many_lines,
    reason = "one temporal witness conserves the original notification, queued input and complete independent native product"
)]
fn notification_unwind_preserves_the_admitted_original_and_independent_actor_progress() {
    let host = Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("the ordinary public Application host");
    let original_cause = Box::new(AdmissionWakeCause {
        retained: Arc::new(()),
    });
    let cause_allocation = ptr::from_ref(original_cause.as_ref()).cast::<()>();
    let cause_custody = Arc::downgrade(&original_cause.retained);
    let notification = Arc::new(AdmissionWake {
        original: Mutex::new(Some(original_cause)),
    });
    let original_message = vec![17, 23].into_boxed_slice();
    let message_allocation = original_message.as_ptr();
    let receiving = host
        .block_on(
            Application::new(Counter::default().stop_on_shutdown()).run_with::<RootTerminal<
                StopOnShutdown<Counter>,
            >, _, _, _, _, _>(
                |application| async move {
                    let interface = application.interface(());
                    let source = interface
                        .external::<Replies>()
                        .expect("the real source actor");
                    let mut recipient = interface
                        .external::<Payloads>()
                        .expect("the real exact recipient actor");
                    let source_address = source.address();
                    let root_address = application.root().address();
                    let target = recipient.recipient();
                    let (original_cause, acquired) = {
                        let waker = Waker::from(notification);
                        let mut context = Context::from_waker(&waker);
                        let mut receive = pin!(recipient.receive());
                        let parked = receive.as_mut().poll(&mut context);
                        assert!(parked.is_pending(), "the actual empty user lane is parked");

                        let attempted = catch_unwind(AssertUnwindSafe(|| {
                            source.try_send(&target, original_message)
                        }));
                        let original_cause = match attempted {
                            Err(original) => original,
                            Ok(Ok(())) => {
                                panic!("the registered real consumer notification must unwind")
                            }
                            Ok(Err(TrySendError::Full(_))) => {
                                panic!("the empty exact mailbox has capacity")
                            }
                            Ok(Err(TrySendError::Closed(_))) => {
                                panic!("the exact recipient remains live")
                            }
                        };
                        assert_eq!(
                            ptr::from_ref(original_cause.as_ref()).cast::<()>(),
                            cause_allocation
                        );
                        let mut context = Context::from_waker(Waker::noop());
                        let acquired = receive.as_mut().poll(&mut context);
                        (original_cause, acquired)
                    };
                    recipient.close_admission();
                    let exhausted = recipient.receive().await;
                    assert!(
                        exhausted.is_none(),
                        "there is exactly one admitted original"
                    );

                    let mut source = source;
                    let admitted = source.try_send(
                        &application.root().established_recipient(),
                        CounterCommand::Read(source.recipient()),
                    );
                    match admitted {
                        Ok(()) => {}
                        Err(TrySendError::Full(_)) => panic!("the independent root has capacity"),
                        Err(TrySendError::Closed(_)) => panic!("the independent root remains live"),
                    }
                    let reply = source
                        .receive()
                        .await
                        .expect("the root's actual Actions reply");
                    assert_eq!(reply.from, root_address);
                    assert_eq!(reply.message, 42);
                    source.close_admission();
                    let exhausted = source.receive().await;
                    assert!(
                        exhausted.is_none(),
                        "the root emits exactly its one useful reply"
                    );
                    let shutdown = application.lifecycle().request_shutdown();
                    assert_eq!(shutdown, Ok(()));
                    (original_cause, acquired, root_address, source_address)
                },
            ),
        )
        .unwrap_or_else(|original| {
            drop(original);
            panic!("the Application retains its completed Work and root join");
        });
    drop(host);
    let (
        ApplicationOutcome::Completed {
            output: (original_cause, acquired, root_address, source_address),
            cleanup: Ok(()),
        },
        Ok((origin, native)),
        Ok(ActorNotificationReceipts {
            termination: Ok(()),
            retirement: Ok(()),
        }),
    ) = receiving
    else {
        panic!("Work, native root and both notification receipts remain independently acquired")
    };
    assert_eq!(
        ptr::from_ref(original_cause.as_ref()).cast::<()>(),
        cause_allocation
    );
    assert_eq!(cause_custody.strong_count(), 1);
    let ActorRetirement::Completed {
        behavior,
        settlements,
        control,
        user,
        descendants,
        child_failures: (),
        completion,
        capability_failures,
        unread_owner_cancellation,
        interpretation,
        source,
        additional_failures,
        received_interpretation,
        received_source,
        source_index,
        acquired_ingress,
        retirement_failures,
        terminal_report,
    } = native
    else {
        panic!("the independent actor's full original native result remains acquired")
    };
    assert_eq!(origin.address(), root_address);
    assert_eq!(behavior.base().reads, [source_address]);
    assert_eq!(completion, Completion::Stopped);
    assert!(control.is_empty() && user.is_empty());
    assert!(descendants.is_empty());
    assert!(capability_failures.is_empty() && unread_owner_cancellation.is_none());
    assert!(interpretation.is_none() && source.is_none() && additional_failures.is_empty());
    assert!(received_interpretation.is_none() && received_source.is_none());
    assert!(source_index.is_none() && acquired_ingress.is_none());
    assert!(retirement_failures.is_empty() && terminal_report.is_none());
    let [settlement] = settlements.as_slice() else {
        panic!("the complete final stopping Actions product remains retained")
    };
    assert_eq!(settlement.settlement_status(), SettlementStatus::Accepted);
    assert!(settlement.creations.is_empty());
    assert!(matches!(settlement.sends.owned, NoSends));
    assert_eq!(settlement.sends.inner.replies.len(), 0);
    assert!(matches!(settlement.become_, Step::Stop(_)));
    let original_message = match acquired {
        Poll::Ready(Some(original)) => original,
        Poll::Ready(None) => panic!("wake unwind does not close admission"),
        Poll::Pending => {
            panic!("publication precedes notification unwind: the original is queued")
        }
    };
    assert_eq!(original_message.from, source_address);
    assert_eq!(original_message.message.as_ptr(), message_allocation);
    assert_eq!(original_message.message.as_ref(), &[17, 23]);
    drop(original_cause);
    assert_eq!(cause_custody.strong_count(), 0);
}
