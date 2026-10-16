use bombay::behavior::{
    BehaviorBase, ItemSettlement, MessageProtocol, NoSends, SendLayer, SettledItem, Step,
};
use bombay::prelude::*;
use bombay::{ActorNotificationReceipts, ActorRetirement, ApplicationScope};
use std::time::{Duration, Instant};

mod application_support;

struct Booking {
    recipient: EstablishedRecipient<MessageProtocol<MailAddr, u32>>,
    version: u32,
    note: Box<[u8]>,
}

#[bombay::actor(message = Never, sends = pub(crate) {
    notices: Vec<EstablishedDelivery<MessageProtocol<MailAddr, u32>>>
})]
impl Booking {
    #[expect(
        clippy::unnecessary_wraps,
        reason = "the actor initializer uses the owning controlled-error Behavior result"
    )]
    fn init(&mut self) -> BehaviorActed<Self> {
        Ok(Actions::cont().with_send(EstablishedDelivery::new(
            self.recipient.clone(),
            self.version,
        )))
    }
}

#[tokio::test(flavor = "current_thread")]
async fn deadline_and_inactivity_keep_their_order_original_actor_and_complete_retirement() {
    let scope = ApplicationScope::new();
    let mut observer = scope
        .external::<MessageProtocol<MailAddr, u32>>()
        .expect("the booking notice recipient is established before construction");
    let note = Box::<[u8]>::from([149, 151, 157]);
    let allocation = note.as_ptr();
    let booking = Booking {
        recipient: observer.recipient(),
        version: 1,
        note,
    }
    .with_deadline(Some(Instant::now()), |booking| {
        booking.version += 10;
        Step::Continue
    })
    .with_receive_timeout(Duration::ZERO, |booking| {
        Actions::stop().with_send(EstablishedDelivery::new(
            booking.base().recipient.clone(),
            booking.base().version,
        ))
    })
    .stop_on_shutdown();
    let entered = scope
        .with_root(booking)
        .run_with(|application| async move {
            tokio::time::timeout(Duration::from_secs(5), application.termination())
                .await
                .expect("the two initialized timer requests complete within a finite watchdog")
        })
        .await
        .unwrap_or_else(|_| panic!("the real caller host interprets both timer requests"));
    let (work, native, notifications, ()) = entered.into_parts();
    let (termination, cleanup) = work
        .try_into_completed()
        .unwrap_or_else(|_| panic!("normal timer expiry retains the independent application work"));
    assert_eq!(termination, Ok(Exit::Normal));
    cleanup.expect("complete retirement is independently joined");
    let Ok(ActorNotificationReceipts {
        termination: Ok(()),
        retirement: Ok(()),
    }) = notifications
    else {
        panic!("both original notification results remain available");
    };
    let terminal = native.expect("the original native retirement remains owned");
    let retained = terminal
        .1
        .behavior()
        .expect("the original booking survives")
        .base();
    assert_eq!(retained.version, 11);
    assert_eq!(retained.note.as_ptr(), allocation);
    assert_eq!(&*retained.note, &[149, 151, 157]);
    assert_eq!(retained.recipient, observer.recipient());
    let ActorRetirement::Completed { settlements, .. } = &terminal.1 else {
        panic!("the original successful timer retirement keeps its complete last settlement");
    };
    let [settled] = settlements.as_slice() else {
        panic!("one final action settlement remains owned");
    };
    assert!(settled.creations.is_empty());
    assert!(matches!(settled.become_, Step::Stop(_)));
    let SendLayer {
        owned: NoSends,
        inner:
            SendLayer {
                owned: inactivity,
                inner:
                    SendLayer {
                        owned: deadline,
                        inner: notices,
                    },
            },
    } = &settled.sends;
    assert_eq!(inactivity.len(), 0);
    assert_eq!(deadline.len(), 0);
    assert!(matches!(
        notices.notices.as_slice(),
        [SettledItem::Attempted(ItemSettlement::Accepted(()))]
    ));
    observer.close_admission();
    let first = observer
        .receive()
        .await
        .expect("the accepted initialization notice is independently received");
    let second = observer
        .receive()
        .await
        .expect("inactivity receives the state changed by the earlier deadline");
    let exhausted = observer.receive().await;
    assert_eq!(first.from, terminal.0.address());
    assert_eq!(second.from, terminal.0.address());
    assert_eq!((first.message, second.message), (1, 11));
    assert!(exhausted.is_none());
    application_support::assert_completed(terminal, None);
}
