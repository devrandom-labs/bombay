use behavior_actors::{OwnedScheduleAfter, OwnedTimerElapsed, TimerGeneration};
use bombay::actors::ActorExt as _;
use bombay::behavior::{
    Actions, Behavior, BehaviorActed, BehaviorBase, Delivery, EventLayer, InterpreterRequests,
    MessageProtocol, Never, NoBirths, Recipient, SendLayer, Step, User,
};
use bombay::composition::{StashRoute, UnlimitedStash};
use bombay::prelude::{Activate as _, MailAddr};
use core::time::Duration;

#[derive(Debug)]
struct Invoice {
    number: u64,
    amount: u64,
    receipt: Box<[u8; 2]>,
}
enum Command {
    Invoice(Invoice),
    Open,
    Stop,
}
#[derive(Debug, PartialEq)]
enum Office {
    Closed,
    Open { balance: u64 },
}
#[derive(Debug)]
enum Reply {
    Reminder,
    Paid {
        invoice: Invoice,
        origin: MailAddr,
        balance: u64,
    },
    Rejected {
        invoice: Invoice,
        origin: MailAddr,
    },
}
struct Worker {
    customer: Recipient<MessageProtocol<MailAddr, Reply>>,
    office: Office,
}

#[bombay::actor(sends = { replies: Vec<Delivery<MessageProtocol<MailAddr, Reply>>> })]
impl Worker {
    fn receive(&mut self, from: MailAddr, command: Command) -> BehaviorActed<Self> {
        match command {
            Command::Invoice(invoice) => {
                let reply = match &mut self.office {
                    Office::Closed => Reply::Rejected {
                        invoice,
                        origin: from,
                    },
                    Office::Open { balance } => {
                        *balance += invoice.amount;
                        Reply::Paid {
                            invoice,
                            origin: from,
                            balance: *balance,
                        }
                    }
                };
                Ok(Actions::cont().with_send(Delivery::new(self.customer, reply)))
            }
            Command::Open => {
                if let Office::Closed = self.office {
                    self.office = Office::Open { balance: 0 };
                }
                Ok(Actions::cont())
            }
            Command::Stop => Ok(Actions::stop()),
        }
    }
}
fn route<B: BehaviorBase<Base = Worker>>(worker: &B, command: &Command) -> StashRoute {
    match command {
        Command::Invoice(_) if worker.base().office == Office::Closed => StashRoute::Stash,
        Command::Invoice(_) | Command::Stop => StashRoute::Deliver,
        Command::Open => StashRoute::Release,
    }
}
fn reminder<B: BehaviorBase<Base = Worker>>(
    worker: &mut B,
) -> Actions<MailAddr, Never, <Worker as Behavior>::Sends, NoBirths> {
    Actions::cont().with_send(Delivery::new(worker.base().customer, Reply::Reminder))
}
#[expect(
    clippy::too_many_lines,
    reason = "one complete ownership trace keeps the original invoices and replay oracle together"
)]
fn journey<B>(worker: B)
where
    B: Behavior<
            Protocol = Worker,
            Event = EventLayer<OwnedTimerElapsed, User<MailAddr, Command>>,
            Sends = SendLayer<InterpreterRequests<OwnedScheduleAfter>, <Worker as Behavior>::Sends>,
            Ph = Never,
            Error = Never,
            Birth = NoBirths,
        > + BehaviorBase<Base = Worker>,
{
    let initialized = worker.initialize().expect("infallible initialization");
    let actions = initialized.actions;
    assert!(actions.sends.inner.replies.is_empty());
    assert!(actions.creates.is_empty());
    assert!(matches!(actions.become_, Step::Continue));
    let [schedule]: [OwnedScheduleAfter; 1] = actions
        .sends
        .owned
        .into_requests()
        .try_into()
        .expect("one authored timer request");
    assert_eq!(
        schedule,
        OwnedScheduleAfter::new(TimerGeneration(0), Duration::from_secs(3))
    );
    let elapsed = OwnedTimerElapsed::new(schedule.generation);
    let mut active = initialized.behavior;
    let customer = active.base().customer;
    assert_eq!(customer.address(), MailAddr(59));
    let invoices = [
        Invoice {
            number: 11,
            amount: 13,
            receipt: Box::new([17, 19]),
        },
        Invoice {
            number: 23,
            amount: 29,
            receipt: Box::new([31, 37]),
        },
    ];
    let allocations = invoices
        .each_ref()
        .map(|invoice| core::ptr::from_ref(invoice.receipt.as_ref()));
    for (origin, invoice) in [MailAddr(41), MailAddr(43)].into_iter().zip(invoices) {
        let held = active
            .receive(origin, Command::Invoice(invoice))
            .expect("infallible hold");
        assert!(held.sends.owned.is_empty());
        assert!(held.sends.inner.replies.is_empty());
        assert!(held.creates.is_empty());
        assert!(matches!(held.become_, Step::Continue));
        assert_eq!(active.base().office, Office::Closed);
    }
    let reminder = active.on(elapsed).expect("matching authored timer");
    assert!(reminder.sends.owned.is_empty());
    assert!(reminder.creates.is_empty());
    assert!(matches!(reminder.become_, Step::Continue));
    let [reminder]: [Delivery<MessageProtocol<MailAddr, Reply>>; 1] = reminder
        .sends
        .inner
        .replies
        .try_into()
        .unwrap_or_else(|_| panic!("exactly one reminder"));
    assert_eq!(reminder.to, customer);
    assert!(matches!(reminder.message, Reply::Reminder));
    assert_eq!(active.base().office, Office::Closed);
    let duplicate = active.on(elapsed).expect("duplicate elapsed input");
    assert!(duplicate.sends.owned.is_empty());
    assert!(duplicate.sends.inner.replies.is_empty());
    assert!(duplicate.creates.is_empty());
    assert!(matches!(duplicate.become_, Step::Continue));
    assert_eq!(active.base().office, Office::Closed);
    let released = active
        .receive(MailAddr(47), Command::Open)
        .expect("open and replay");
    assert!(released.sends.owned.is_empty());
    assert!(released.creates.is_empty());
    assert!(matches!(released.become_, Step::Continue));
    assert_eq!(active.base().office, Office::Open { balance: 42 });
    let paid = released.sends.inner.replies;
    assert_eq!(paid.len(), 2);
    for (delivery, (number, amount, receipt, origin, balance, allocation)) in
        paid.into_iter().zip([
            (11, 13, [17, 19], MailAddr(41), 13, allocations[0]),
            (23, 29, [31, 37], MailAddr(43), 42, allocations[1]),
        ])
    {
        assert_eq!(delivery.to, customer);
        let Reply::Paid {
            invoice,
            origin: actual_origin,
            balance: actual_balance,
        } = delivery.message
        else {
            panic!("an opened office pays its original invoice");
        };
        assert_eq!(
            (
                invoice.number,
                invoice.amount,
                *invoice.receipt,
                actual_origin,
                actual_balance
            ),
            (number, amount, receipt, origin, balance)
        );
        assert_eq!(core::ptr::from_ref(invoice.receipt.as_ref()), allocation);
    }
    let stopped = active
        .receive(MailAddr(53), Command::Stop)
        .expect("business retirement");
    assert!(stopped.sends.owned.is_empty());
    assert!(stopped.sends.inner.replies.is_empty());
    assert!(stopped.creates.is_empty());
    assert!(matches!(stopped.become_, Step::Stop(_)));
    assert_eq!(active.base().office, Office::Open { balance: 42 });
}
fn worker() -> Worker {
    Worker {
        customer: Recipient::global(MailAddr(59)),
        office: Office::Closed,
    }
}
#[test]
fn stash_before_reminder_preserves_original_invoices_and_one_reminder() {
    journey(UnlimitedStash::new(worker(), route).with_one_shot(Duration::from_secs(3), reminder));
}
#[test]
fn reminder_before_stash_preserves_original_invoices_and_one_reminder() {
    journey(UnlimitedStash::new(
        worker().with_one_shot(Duration::from_secs(3), reminder),
        route,
    ));
}

#[test]
fn closed_worker_returns_the_original_invoice_without_stash() {
    let initialized = worker().initialize().expect("infallible initialization");
    assert!(initialized.actions.sends.replies.is_empty());
    assert!(initialized.actions.creates.is_empty());
    assert!(matches!(initialized.actions.become_, Step::Continue));
    let mut active = initialized.behavior;
    let invoice = Invoice {
        number: 61,
        amount: 67,
        receipt: Box::new([71, 73]),
    };
    let allocation = core::ptr::from_ref(invoice.receipt.as_ref());
    let rejected = active
        .receive(MailAddr(79), Command::Invoice(invoice))
        .expect("normal business rejection");
    assert!(rejected.creates.is_empty());
    assert!(matches!(rejected.become_, Step::Continue));
    let [delivery]: [Delivery<MessageProtocol<MailAddr, Reply>>; 1] = rejected
        .sends
        .replies
        .try_into()
        .unwrap_or_else(|_| panic!("exactly one original rejection"));
    assert_eq!(delivery.to, active.base().customer);
    let Reply::Rejected { invoice, origin } = delivery.message else {
        panic!("closed business returns its original invoice");
    };
    assert_eq!(
        (invoice.number, invoice.amount, *invoice.receipt, origin),
        (61, 67, [71, 73], MailAddr(79))
    );
    assert_eq!(core::ptr::from_ref(invoice.receipt.as_ref()), allocation);
    assert_eq!(active.base().office, Office::Closed);
}
