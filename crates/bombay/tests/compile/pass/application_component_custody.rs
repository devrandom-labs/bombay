use bombay::ActorCustody;
use bombay::actors::ActorExt;
use bombay::behavior::{
    ActiveTurn, Behavior, BehaviorActed, BehaviorBase, MessageProtocol, Never, NoBirths, NoSends,
    User,
};
use bombay::{ActorRetirement, Application, MailAddr};

#[path = "../../../../../examples/counter/src/counter.rs"]
mod counter;

struct JournalState {
    entries: Box<[u8]>,
}

struct Journal {
    state: JournalState,
}

impl BehaviorBase for Journal {
    type Base = JournalState;

    fn base(&self) -> &JournalState {
        &self.state
    }
}

impl Behavior for Journal {
    type Protocol = MessageProtocol<MailAddr, Never>;
    type Event = User<MailAddr, Never>;
    type Sends = NoSends;
    type Ph = Never;
    type Error = Never;
    type Birth = NoBirths;

    fn transition(&mut self, _: ActiveTurn, event: Self::Event) -> BehaviorActed<Self> {
        match event.message {}
    }
}

struct Audit;

bombay::topology! {
    journal_application {
        root: counter::Counter {}
        members { Audit(Audit) => journal; }
        journal: bombay::lifecycle::StopOnShutdown<Journal> {}
    }
}

#[tokio::main]
async fn main() {
    let entries: Box<[u8]> = Box::from([5, 11]);
    let allocation = entries.as_ptr();
    let (outcome, native, notifications, ()) = Application::with_topology(
        counter::Counter::new().stop_on_shutdown(),
        journal_application::Topology::new(),
    )
    .child(
        Audit,
        Journal {
            state: JournalState { entries },
        }
        .stop_on_shutdown(),
    )
    .run_with(|application| async move {
        let lifecycle = application.lifecycle();
        let interface = application.interface(application.root().established_recipient());
        let mut customer = interface
            .external::<counter::CounterValue>()
            .expect("the reply endpoint is established");
        customer
            .send(interface.api(), counter::CounterMessage::Increment)
            .await
            .expect("increment is admitted");
        customer
            .send(
                interface.api(),
                counter::CounterMessage::Read(customer.recipient()),
            )
            .await
            .expect("read is admitted");
        let reply = customer
            .receive()
            .await
            .expect("the actual counter replies");
        lifecycle
            .request_shutdown()
            .expect("the root admits shutdown");
        reply.message
    })
    .await
    .unwrap_or_else(|original| {
        drop(original);
        panic!("the component runs on the entered host");
    }) .into_parts();
    let (value, cleanup) = outcome.try_into_completed().unwrap_or_else(|original| {
        drop(original);
        panic!("the caller work completes independently of cleanup");
    });
    assert_eq!(value, 1);
    cleanup.expect("the actual cleanup producer completed independently");
    let notifications = notifications.expect("both original notification receipts survive");
    notifications.termination.expect("actual ordinary termination notification");
    notifications.retirement.expect("actual joined retirement notification");
    let (_, retirement) = native.expect("the complete component custody is joined");
    let ActorRetirement::Completed {
        custody: ActorCustody { descendants, .. },
        ..
    } = retirement
    else {
        panic!("the actual root retires");
    };
    assert_eq!(descendants.len(), 1);
    let report = descendants
        .into_iter()
        .next()
        .expect("the audit child retired");
    let journal_application::RootRetirements {
        origin: _,
        retirement,
    } = report;
    let ActorRetirement::OwnerCancelled {
        custody: ActorCustody { behavior, .. },
        ..
    } = *retirement
    else {
        panic!("the owner cancelled the component after startup");
    };
    assert_eq!(behavior.base().entries.as_ptr(), allocation);
    assert_eq!(&*behavior.base().entries, &[5, 11]);
}
