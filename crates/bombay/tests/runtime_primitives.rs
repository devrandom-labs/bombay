use std::convert::Infallible;

use behavior::{
    Actions, Behavior, BehaviorActed, InitializationTurn, MailAddr, Never, NoBirths, Step, User,
};
use bombay_address::{AddressSpace, ClaimError, Lease};
use bombay_engine::{ActionsOf, ActiveEnvironment, Completion, Driver, Environment};
use communication::{Config, Consumer, ControlSender, Received, UserSender, channel};
use observe::{ObservationSpace, Subject};
use timers::TimerQueue;

#[derive(Debug, PartialEq, Eq)]
struct Schedule {
    at: u64,
    message: u8,
}

struct RuntimeBehavior {
    seen: Vec<u8>,
}

impl Behavior for RuntimeBehavior {
    type Addr = MailAddr;
    type Msg = u8;
    type Event = User<MailAddr, u8>;
    type Sends = Vec<Schedule>;
    type Ph = Never;
    type Error = Infallible;
    type Birth = NoBirths;

    fn init(&mut self, _: InitializationTurn) -> BehaviorActed<Self> {
        Ok(Actions::new(
            vec![Schedule { at: 7, message: 1 }],
            Vec::new(),
            Step::Continue,
        ))
    }

    fn transition(&mut self, _: behavior::ActiveTurn, event: Self::Event) -> BehaviorActed<Self> {
        self.seen.push(event.message);
        if self.seen == [1, 2] {
            Ok(Actions::stop())
        } else {
            Ok(Actions::cont())
        }
    }
}

type Event = User<MailAddr, u8>;

struct Prepared {
    address: MailAddr,
    addresses: AddressSpace<MailAddr, UserSender<Event>>,
    endpoint: UserSender<Event>,
    control: ControlSender<Event>,
    consumer: Consumer<Event, Event>,
    timers: TimerQueue<u64, u8, Event>,
    subject: Subject<MailAddr, &'static str>,
}

struct Active {
    address: MailAddr,
    addresses: AddressSpace<MailAddr, UserSender<Event>>,
    lease: Lease<MailAddr, UserSender<Event>>,
    control: ControlSender<Event>,
    consumer: Consumer<Event, Event>,
    timers: TimerQueue<u64, u8, Event>,
    subject: Subject<MailAddr, &'static str>,
}

impl Environment<RuntimeBehavior> for Prepared {
    type Active = Active;
    type Error = ClaimError<MailAddr>;

    async fn activate(
        self,
        actions: ActionsOf<RuntimeBehavior>,
    ) -> Result<Self::Active, Self::Error> {
        let Self {
            address,
            addresses,
            endpoint,
            control,
            consumer,
            mut timers,
            subject,
        } = self;

        for schedule in actions.sends {
            timers.schedule(
                schedule.message,
                schedule.at,
                User::new(address, schedule.message),
            );
        }
        let lease = addresses.try_claim(address, endpoint)?;

        let published = addresses.resolve(&address).expect("published endpoint");
        published
            .try_send(User::new(address, 2))
            .expect("published user lane accepts input");

        Ok(Active {
            address,
            addresses,
            lease,
            control,
            consumer,
            timers,
            subject,
        })
    }
}

impl ActiveEnvironment<RuntimeBehavior> for Active {
    type Error = Infallible;

    async fn next(&mut self) -> Option<Event> {
        if let Some(expired) = self.timers.pop_due(7) {
            self.control
                .send(expired.value)
                .expect("live control lane accepts expiration");
        }
        match self.consumer.recv().await? {
            Received::Control(event) | Received::User(event) => Some(event),
            Received::UserLaneClosed => None,
        }
    }

    async fn apply(&mut self, actions: ActionsOf<RuntimeBehavior>) -> Result<(), Self::Error> {
        assert!(actions.sends.is_empty());
        Ok(())
    }

    async fn retire(mut self) {
        self.lease.release();
        assert!(self.addresses.resolve(&self.address).is_none());
        self.subject.complete("retired");
    }
}

#[tokio::test]
async fn prepared_environment_composes_all_runtime_primitives() {
    let addresses = AddressSpace::new();
    let observations = ObservationSpace::new();
    let address = MailAddr(7);
    let subject = observations.subject(address).unwrap();
    let observation = observations.observe(&address).unwrap();
    let (control, endpoint, consumer) = channel(Config::new(4).with_aging_cap(8));

    let environment = Prepared {
        address,
        addresses: addresses.clone(),
        endpoint,
        control,
        consumer,
        timers: TimerQueue::new(),
        subject,
    };

    assert_eq!(
        Driver::new(RuntimeBehavior { seen: Vec::new() }, environment,)
            .run()
            .await,
        Ok(Completion::Stopped)
    );
    assert!(addresses.resolve(&address).is_none());
    assert_eq!(observation.into_outcome(), Some("retired"));
}
