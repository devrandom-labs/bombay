use bombay::prelude::*;

struct Worker;

impl Behavior for Worker {
    type Protocol = behavior::MessageProtocol<MailAddr, Never>;
    type Event = User<MailAddr, Never>;
    type Sends = Vec<Never>;
    type Ph = Never;
    type Error = Never;
    type Birth = NoBirths;

    fn transition(&mut self, _: ActiveTurn, event: Self::Event) -> BehaviorActed<Self> {
        match event.message {}
    }
}

type ManagedWorker = StopOnShutdown<Worker>;
type ManagedReply = StopOnShutdown<SupervisorReply>;
type ParentPath = Inside<Here>;
type Workers = DynamicSupervisorWithParent<MailAddr, ManagedWorker, SupervisorReply, ParentPath>;
type ManagedWorkers = StopOnShutdown<Workers>;
type SystemProtocol = MessageProtocol<MailAddr, SystemMessage>;
type SupervisorReply =
    MessageAdapter<DynamicSupervisorOutcome<MailAddr, ManagedWorker>, SystemProtocol>;

struct ShutdownTimer;

impl Behavior for ShutdownTimer {
    type Protocol = behavior::MessageProtocol<MailAddr, Never>;
    type Event = User<MailAddr, Never>;
    type Sends = Vec<Delivery<SystemProtocol>>;
    type Ph = Never;
    type Error = Never;
    type Birth = NoBirths;

    fn transition(&mut self, _: ActiveTurn, event: Self::Event) -> BehaviorActed<Self> {
        match event.message {}
    }
}

#[allow(
    clippy::unnecessary_wraps,
    reason = "OneShot reactions use the exact fallible Behavior result signature"
)]
fn finish_system(_: &mut ShutdownTimer) -> BehaviorActed<ShutdownTimer> {
    Ok(Actions::new(
        vec![Delivery::new(
            Recipient::global(MailAddr(0)),
            SystemMessage::Finished,
        )],
        Vec::new(),
        Step::Stop(Stopped),
    ))
}

type SystemTimer = StopOnShutdown<OneShot<ShutdownTimer>>;

enum SystemMessage {
    SupervisorChanged,
    Finished,
}

fn supervisor_outcome(_: DynamicSupervisorOutcome<MailAddr, ManagedWorker>) -> SystemMessage {
    SystemMessage::SupervisorChanged
}

type SystemChildren =
    ChildChoice<SystemTimer, ChildChoice<ManagedReply, ChildChoice<ManagedWorkers, Never>>>;

struct System;

impl Behavior for System {
    type Protocol = behavior::MessageProtocol<MailAddr, SystemMessage>;
    type Event = User<MailAddr, SystemMessage>;
    type Sends = Vec<Delivery<Workers>>;
    type Ph = Never;
    type Error = ChildrenError<u64>;
    type Birth = Births<SystemChildren>;

    fn init(&mut self, _: InitializationTurn) -> BehaviorActed<Self> {
        let children = Children::<MailAddr>::new()
            .child(
                1,
                StopOnShutdown::new(Workers::with_parent(
                    ProxyParentIngress::<MailAddr, Here>::new().inside(),
                )),
            )
            .child(
                2,
                StopOnShutdown::new(MessageAdapter::new(
                    Recipient::<SystemProtocol>::global(MailAddr(0)),
                    supervisor_outcome,
                )),
            )
            .child(
                3,
                StopOnShutdown::new(OneShot::new(
                    ShutdownTimer,
                    TimerId(1),
                    std::time::Duration::from_millis(10),
                    finish_system,
                )),
            )
            .into_creates()?;
        let start = DynamicSupervisorMessage::Start {
            nonce: 7,
            child: StopOnShutdown::new(Worker),
            reply_to: Recipient::global(MailAddr(0).birth(2)),
        };
        Ok(Actions::new(
            vec![Delivery::local_child(ChildRecipient::new(1), start)],
            children,
            Step::Continue,
        ))
    }

    fn transition(&mut self, _: ActiveTurn, event: Self::Event) -> BehaviorActed<Self> {
        match event.message {
            SystemMessage::SupervisorChanged => Ok(Actions::cont()),
            SystemMessage::Finished => Ok(Actions::stop()),
        }
    }
}

application! {
    topology SupervisionTopology for System {
        hosted {
            Workers,
            SupervisorReply,
            DynamicProxy<ManagedWorker>,
            MessageProtocol<MailAddr, Never>,
        }
    }
}

#[bombay::main]
fn main() {
    System
}
