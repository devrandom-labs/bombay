//! Public Application trace: preload ingress before arming already-due timers.
use behavior_actors::{Exit, ScheduleAt, TimerElapsed, TimerGeneration, TimerId};
use bombay::behavior::{
    Actions, ActiveTurn, Behavior, BehaviorActed, BehaviorBase, Here, InjectEvent,
    InterpreterRequests, MessageProtocol, Never, NoBirths, User, UserEvent,
};
use bombay::{ActorRetirement, Application, MailAddr};
use std::time::{Duration, Instant};

#[derive(Debug)]
enum FloodInput {
    Arm,
    Message(Box<u64>),
}

enum FloodEvent {
    Input(User<MailAddr, FloodInput>),
    Timer(TimerElapsed),
}

impl UserEvent for FloodEvent {
    type Addr = MailAddr;
    type Message = FloodInput;
    fn user(from: MailAddr, message: FloodInput) -> Self {
        Self::Input(User::new(from, message))
    }
    fn into_user(self) -> Result<User<MailAddr, FloodInput>, Self> {
        match self {
            Self::Input(input) => Ok(input),
            event @ Self::Timer(_) => Err(event),
        }
    }
}

impl InjectEvent<TimerElapsed, Here> for FloodEvent {
    fn inject_at(event: TimerElapsed) -> Self {
        Self::Timer(event)
    }
}

#[derive(Clone, Copy, Debug)]
enum Service {
    Mailbox,
    Timer,
}

struct Flood {
    deadline: Instant,
    counts: [u64; 2],
    trace: Vec<Service>,
    #[expect(
        clippy::vec_box,
        reason = "retain independently allocated original messages as custody evidence"
    )]
    messages: Vec<Box<u64>>,
    timers: Vec<TimerElapsed>,
}

impl BehaviorBase for Flood {
    type Base = Self;
    fn base(&self) -> &Self {
        self
    }
}

impl Behavior for Flood {
    type Protocol = MessageProtocol<MailAddr, FloodInput>;
    type Event = FloodEvent;
    type Sends = InterpreterRequests<ScheduleAt>;
    type Ph = Never;
    type Birth = NoBirths;
    type Error = Never;

    fn transition(&mut self, _: ActiveTurn, event: FloodEvent) -> BehaviorActed<Self> {
        match event {
            FloodEvent::Input(User {
                message: FloodInput::Arm,
                ..
            }) => {
                let mut actions = Actions::cont();
                for sequence in 0..self.counts[1] {
                    actions = actions.with_send(ScheduleAt::new(
                        TimerId(sequence),
                        TimerGeneration(1),
                        self.deadline,
                    ));
                }
                return Ok(actions);
            }
            FloodEvent::Input(User {
                message: FloodInput::Message(message),
                ..
            }) => {
                self.trace.push(Service::Mailbox);
                self.messages.push(message);
            }
            FloodEvent::Timer(event) => {
                self.trace.push(Service::Timer);
                self.timers.push(event);
            }
        }
        Ok(
            if self.trace.len() as u64 == self.counts.iter().sum::<u64>() {
                Actions::stop()
            } else {
                Actions::cont()
            },
        )
    }
}

#[tokio::test(flavor = "current_thread")]
async fn actual_actor_bounds_both_sources_and_returns_every_original_payload() {
    for counts in [[256, 256], [512, 1], [1, 512]] {
        let messages = (0..counts[0]).map(Box::new).collect::<Vec<_>>();
        let allocations = messages
            .iter()
            .map(|message| (&raw const **message) as usize)
            .collect::<Vec<_>>();
        let flood = Flood {
            deadline: Instant::now().checked_sub(Duration::from_secs(1)).unwrap(),
            counts,
            trace: Vec::new(),
            messages: Vec::new(),
            timers: Vec::new(),
        };
        let (work, native, notifications) = Application::new(flood)
            .run_with(move |application| async move {
                let recipient = application.root().established_recipient();
                let interface = application.interface(());
                let sender = interface
                    .external::<MessageProtocol<MailAddr, Never>>()
                    .unwrap();
                // These synchronous admissions cannot yield to the actor on this
                // current-thread host. Arm is first; the full flood follows it.
                sender.try_send(&recipient, FloodInput::Arm).unwrap();
                for message in messages {
                    sender
                        .try_send(&recipient, FloodInput::Message(message))
                        .unwrap();
                }
                tokio::time::timeout(Duration::from_secs(5), application.termination()).await
            })
            .await
            .unwrap_or_else(|_| panic!("the entered application host is available"));
        let (termination, cleanup) = work
            .try_into_completed()
            .unwrap_or_else(|_| panic!("the finite workload completes"));
        assert_eq!(termination.unwrap(), Ok(Exit::Normal));
        cleanup.unwrap();
        let notifications = notifications.unwrap();
        notifications.termination.unwrap();
        notifications.retirement.unwrap();
        let (_, retirement) = native.unwrap();
        let ActorRetirement::Completed { custody, .. } = retirement else {
            panic!("the actual actor returns its complete trace and original messages");
        };
        let flood = custody.behavior;
        assert!(custody.control.is_empty());
        assert!(custody.user.is_empty());
        assert!(custody.capability_failures.is_empty());
        assert_eq!(flood.trace.len() as u64, counts.iter().sum::<u64>());
        // Independent oracle: measure distances in the completed actor trace,
        // without reproducing the selector's cursor or rotation order.
        for (source, count) in counts.into_iter().enumerate() {
            let positions = flood
                .trace
                .iter()
                .enumerate()
                .filter_map(|(position, service)| {
                    let observed = match service {
                        Service::Mailbox => 0,
                        Service::Timer => 1,
                    };
                    (observed == source).then_some(position)
                })
                .collect::<Vec<_>>();
            assert_eq!(positions.len() as u64, count);
            assert!(
                positions[0] < 3,
                "ready source {source} missed its first bound"
            );
            assert!(
                positions.windows(2).all(|pair| pair[1] - pair[0] <= 3),
                "ready source {source} exceeded two intervening actor turns"
            );
        }
        assert_eq!(
            flood
                .messages
                .iter()
                .map(|message| **message)
                .collect::<Vec<_>>(),
            (0..counts[0]).collect::<Vec<_>>()
        );
        assert_eq!(
            flood
                .messages
                .iter()
                .map(|message| (&raw const **message) as usize)
                .collect::<Vec<_>>(),
            allocations
        );
        assert_eq!(
            flood.timers,
            (0..counts[1])
                .map(|sequence| TimerElapsed::new(TimerId(sequence), TimerGeneration(1)))
                .collect::<Vec<_>>()
        );
    }
}
