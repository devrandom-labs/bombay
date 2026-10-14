use super::observation::TerminationEvent;
use super::*;
use crate::observe;
use behavior::{
    Actions, ActiveTurn, BehaviorActed, CreationSequence, Here, MessageProtocol, NoBirths, NoSends,
    User, UserEvent,
};
use behavior_actors::{ChildStopped, Exit, PeerStopped};
use communication::{Config, mailbox_channel};
use core::pin::pin;
use core::task::{Context, Poll, Waker};
use tokio::task;

#[derive(Debug, PartialEq, Eq)]
enum CompletionEvent {
    Observation(PeerStopped<MailAddr>),
    Startup(Box<u64>),
    Stopped(ChildStopped<MailAddr>),
}

impl InjectEvent<PeerStopped<MailAddr>, Here> for CompletionEvent {
    fn inject_at(event: PeerStopped<MailAddr>) -> Self {
        Self::Observation(event)
    }
}

impl InjectEvent<ChildStopped<MailAddr>, Here> for CompletionEvent {
    fn inject_at(event: ChildStopped<MailAddr>) -> Self {
        Self::Stopped(event)
    }
}

impl UserEvent for CompletionEvent {
    type Addr = MailAddr;
    type Message = Never;
    fn user(_: MailAddr, message: Never) -> Self {
        match message {}
    }
    fn into_user(self) -> Result<User<MailAddr, Never>, Self> {
        Err(self)
    }
}

struct CompletionActor;
impl Behavior for CompletionActor {
    type Protocol = MessageProtocol<MailAddr, Never>;
    type Event = CompletionEvent;
    type Sends = NoSends;
    type Ph = Never;
    type Birth = NoBirths;
    type Error = Never;
    fn transition(&mut self, _: ActiveTurn, _: Self::Event) -> BehaviorActed<Self> {
        Ok(Actions::cont())
    }
}

#[tokio::test]
async fn fairness_ready_observations_and_startup_results_alternate_without_loss() {
    let (control, owner, mailbox, consumer) =
        mailbox_channel::<CompletionEvent, ()>(Config::new(2));
    let (terminal, terminal_receiver) = oneshot::channel();
    let mut capabilities = ApplicationCapabilities::<CompletionActor, ()>::new_with_bindings(
        ApplicationCapabilityInputs {
            actor_spaces: Arc::new(()),
            allocations: ApplicationAddresses::new(),
            address: MailAddr(1),
            control,
            timers: LocalTimers::new(),
            observations: TerminationObservations::new(),
            terminal_reports: LocalTerminalReports::new(terminal),
        },
        NoChildBindings::default(),
    );
    let mut expected = Vec::new();
    for sequence in 0..256 {
        let peer = MailAddr(sequence);
        let (publisher, observation) = observe::pair();
        publisher.complete(Ok(Exit::Normal));
        capabilities
            .observations
            .insert_peer::<Here>(peer, observation);
        // Await a separate notification after spawning each task. A current-thread
        // executor finishes the original task before this controller resumes.
        let (ready, reached) = oneshot::channel();
        capabilities
            .activation_tasks
            .as_mut()
            .unwrap()
            .spawn(async move {
                ready.send(()).unwrap();
                Err(CompletionEvent::Startup(Box::new(sequence)))
            });
        reached.await.unwrap();
        expected.push(CompletionEvent::Observation(PeerStopped::new(
            peer,
            Ok(Exit::Normal),
        )));
        expected.push(CompletionEvent::Startup(Box::new(sequence)));
    }
    let mut acquired = Vec::new();
    for _ in 0..expected.len() {
        acquired.push(capabilities.next_local_event().await.unwrap());
    }
    assert_eq!(acquired, expected);
    let pending = {
        let mut receiving = pin!(capabilities.next_local_event());
        receiving
            .as_mut()
            .poll(&mut Context::from_waker(Waker::noop()))
    };
    assert!(
        matches!(pending, Poll::Pending),
        "each original result is acquired once"
    );
    drop((owner, mailbox, consumer, terminal_receiver));
}

#[tokio::test]
async fn fairness_delayed_startup_survives_losing_selection_and_pending_cancellation() {
    let (control, owner, mailbox, consumer) =
        mailbox_channel::<CompletionEvent, ()>(Config::new(2));
    let (terminal, terminal_receiver) = oneshot::channel();
    let mut capabilities = ApplicationCapabilities::<CompletionActor, ()>::new_with_bindings(
        ApplicationCapabilityInputs {
            actor_spaces: Arc::new(()),
            allocations: ApplicationAddresses::new(),
            address: MailAddr(1),
            control,
            timers: LocalTimers::new(),
            observations: TerminationObservations::new(),
            terminal_reports: LocalTerminalReports::new(terminal),
        },
        NoChildBindings::default(),
    );
    let (release, released) = oneshot::channel();
    let (started, entered) = oneshot::channel();
    let payload = Box::new(37);
    let identity = (&raw const *payload) as usize;
    capabilities
        .activation_tasks
        .as_mut()
        .unwrap()
        .spawn(async move {
            started.send(task::id()).unwrap();
            released.await.unwrap();
            Err(CompletionEvent::Startup(payload))
        });
    let task_id = entered.await.unwrap();
    for sequence in 0..8 {
        let waiting = {
            let mut receiving = pin!(capabilities.next_local_event());
            receiving
                .as_mut()
                .poll(&mut Context::from_waker(Waker::noop()))
        };
        assert!(matches!(waiting, Poll::Pending));
        let (publisher, observation) = observe::pair();
        capabilities
            .observations
            .insert_peer::<Here>(MailAddr(sequence), observation);
        publisher.complete(Ok(Exit::Normal));
        let event = capabilities.next_local_event().await.unwrap();
        assert_eq!(
            event,
            CompletionEvent::Observation(PeerStopped::new(MailAddr(sequence), Ok(Exit::Normal)))
        );
    }
    let release_result = release.send(());
    assert!(
        release_result.is_ok(),
        "the original task still owns its gate: {task_id:?}"
    );
    let event = capabilities.next_local_event().await.unwrap();
    let CompletionEvent::Startup(payload) = event else {
        panic!("the original startup result must survive")
    };
    assert_eq!((&raw const *payload) as usize, identity);
    assert_eq!(*payload, 37);
    let retirement = capabilities.retire().await;
    let (events, failures) = retirement.activation_tasks.settle().await;
    assert_eq!(events, []);
    assert!(failures.is_empty());
    drop((owner, mailbox, consumer, terminal_receiver));
}

#[tokio::test]
async fn fairness_observation_flood_yields_to_the_executor() {
    let (control, owner, mailbox, consumer) =
        mailbox_channel::<CompletionEvent, ()>(Config::new(2));
    let (terminal, terminal_receiver) = oneshot::channel();
    let mut capabilities = ApplicationCapabilities::<CompletionActor, ()>::new_with_bindings(
        ApplicationCapabilityInputs {
            actor_spaces: Arc::new(()),
            allocations: ApplicationAddresses::new(),
            address: MailAddr(1),
            control,
            timers: LocalTimers::new(),
            observations: TerminationObservations::new(),
            terminal_reports: LocalTerminalReports::new(terminal),
        },
        NoChildBindings::default(),
    );
    let mut expected = Vec::new();
    for sequence in 0..512 {
        let (publisher, observation) = observe::pair();
        publisher.complete(Ok(Exit::Normal));
        capabilities
            .observations
            .insert_peer::<Here>(MailAddr(sequence), observation);
        expected.push(CompletionEvent::Observation(PeerStopped::new(
            MailAddr(sequence),
            Ok(Exit::Normal),
        )));
    }
    let mut receiving = Box::pin(async {
        let mut acquired = Vec::new();
        for _ in 0..512 {
            acquired.push(capabilities.next_local_event().await.unwrap());
        }
        acquired
    });
    let first_poll = receiving
        .as_mut()
        .poll(&mut Context::from_waker(Waker::noop()));
    assert!(
        matches!(first_poll, Poll::Pending),
        "an observation flood must return control to the host"
    );
    let acquired = receiving.await;
    assert_eq!(acquired, expected);
    drop((owner, mailbox, consumer, terminal_receiver));
}

#[tokio::test]
async fn fairness_child_stop_follows_admitted_reports() {
    let (control, owner, mailbox, mut consumer) =
        mailbox_channel::<CompletionEvent, ()>(Config::new(2));
    let (terminal, terminal_receiver) = oneshot::channel();
    let mut capabilities = ApplicationCapabilities::<CompletionActor, ()>::new_with_bindings(
        ApplicationCapabilityInputs {
            actor_spaces: Arc::new(()),
            allocations: ApplicationAddresses::new(),
            address: MailAddr(1),
            control,
            timers: LocalTimers::new(),
            observations: TerminationObservations::new(),
            terminal_reports: LocalTerminalReports::new(terminal),
        },
        NoChildBindings::default(),
    );
    let mut creations = CreationSequence::new();
    let child = creations.issue().unwrap();
    let (publisher, observation) = observe::pair();
    capabilities
        .observations
        .insert_child::<Here>(child, observation);
    capabilities
        .control
        .send(CompletionEvent::Startup(Box::new(37)))
        .unwrap();
    publisher.complete(Ok(Exit::Normal));
    let before = Instant::now();
    let routed = {
        let mut receiving = pin!(capabilities.next_local_event());
        receiving
            .as_mut()
            .poll(&mut Context::from_waker(Waker::noop()))
    };
    let after = Instant::now();
    assert!(
        matches!(routed, Poll::Pending),
        "child termination cannot overtake its queued report"
    );
    let report = consumer.recv_control().await;
    assert_eq!(report, Some(CompletionEvent::Startup(Box::new(37))));
    let stopped = consumer.recv_control().await;
    let Some(CompletionEvent::Stopped(stopped)) = stopped else {
        panic!("the exact child fact follows its report");
    };
    assert_eq!(stopped.child, child);
    assert_eq!(stopped.outcome, Ok(Exit::Normal));
    assert!(before <= stopped.at && stopped.at <= after);
    let duplicate = {
        let mut receiving = pin!(consumer.recv_control());
        receiving
            .as_mut()
            .poll(&mut Context::from_waker(Waker::noop()))
    };
    assert!(matches!(duplicate, Poll::Pending));
    drop((owner, mailbox, terminal_receiver));
}

#[test]
fn fairness_closed_child_destination_retains_the_original_fact() {
    let (control, owner, mailbox, consumer) = mailbox_channel::<Box<[u8; 3]>, ()>(Config::new(1));
    drop(consumer);
    let original = Box::new([3, 7, 11]);
    let allocation = (&raw const *original) as usize;
    let recovered = TerminationEvent::Child(original).into_local(&control);
    let recovered = recovered.expect("closed control receiving returns exact local custody");
    assert_eq!((&raw const *recovered) as usize, allocation);
    assert_eq!(*recovered, [3, 7, 11]);
    drop((owner, mailbox));
}
