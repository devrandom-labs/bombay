//! Request/reply is ordinary user protocol composition.
//!
//! These oracles deliberately define no framework request, correlation, or
//! pending-operation type. An application message carries the existing typed
//! recipient; any correlation data belongs inside that application message.

use std::time::Duration;

use bombay::behavior::{
    Actions, Address, Behavior, Crash, Deadline, Delivery, Exit, Inner, Never, NoBirths,
    ObservePeer, Own, PeerStopped, Recipient, SendAlgebra, SendProduct, ServiceSends, Step, User,
    WatchEvent,
};
use bombay::{
    Actor, ActorRef, AddressInUse, AddressRouter, DeliveryRouter, EndpointRegistry,
    IncarnationEndpoint, MailboxAnchor, MailboxConfig, PeerObservationError, PeerObserver, System,
    TaskOutcome,
};
use observe::Observation;
use tokio::task::yield_now;
use tokio::time::{Instant, advance};

const REQUESTER: TestAddr = TestAddr(1);
const CALLEE: TestAddr = TestAddr(2);

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
struct TestAddr(u64);

impl Address for TestAddr {
    type Nonce = u64;

    fn birth(self, nonce: u64) -> Self {
        Self(self.0.wrapping_mul(257).wrapping_add(nonce))
    }
}

#[derive(Debug, Clone, Copy)]
enum CalleeAction {
    Reply(u8),
    Retire,
    Ignore,
}

#[derive(Debug)]
struct Call {
    reply_to: Recipient<TestAddr, u8>,
    action: CalleeAction,
}

struct Callee;

impl Behavior for Callee {
    type Addr = TestAddr;
    type Msg = Call;
    type Event = User<TestAddr, Call>;
    type Sends = Vec<Delivery<TestAddr, u8>>;
    type Ph = Never;
    type Error = Never;
    type Birth = NoBirths;

    fn init(&mut self) -> bombay::behavior::BehaviorActed<Self> {
        Ok(Actions::cont())
    }

    fn transition(&mut self, event: Self::Event) -> bombay::behavior::BehaviorActed<Self> {
        let Call { reply_to, action } = event.message;
        Ok(match action {
            CalleeAction::Reply(reply) => Actions::new(
                vec![Delivery::new(reply_to, reply)],
                Vec::new(),
                Step::Continue,
            ),
            CalleeAction::Retire => Actions::stop(Exit::Normal),
            CalleeAction::Ignore => Actions::cont(),
        })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ResultFact {
    Reply(u8),
    Timeout,
    LateReply(u8),
    CalleeRetired,
}

type RequestSends = SendProduct<ServiceSends<ObservePeer<TestAddr>>, Vec<Delivery<TestAddr, Call>>>;
type ObservePeerPath = Inner<Own>;

struct Requester {
    action: CalleeAction,
    deadline_elapsed: bool,
}

impl Requester {
    const fn new(action: CalleeAction) -> Self {
        Self {
            action,
            deadline_elapsed: false,
        }
    }
}

impl Behavior for Requester {
    type Addr = TestAddr;
    type Msg = u8;
    type Event = WatchEvent<User<TestAddr, u8>>;
    type Sends = RequestSends;
    type Ph = Never;
    type Error = ResultFact;
    type Birth = NoBirths;

    fn init(&mut self) -> bombay::behavior::BehaviorActed<Self> {
        let mut sends = RequestSends::empty();
        sends.send::<_, ObservePeerPath>(ObservePeer::new(CALLEE));
        sends.send::<_, Own>(Delivery::new(
            Recipient::global(CALLEE),
            Call {
                reply_to: Recipient::global(REQUESTER),
                action: self.action,
            },
        ));
        Ok(Actions::new(sends, Vec::new(), Step::Continue))
    }

    fn transition(&mut self, event: Self::Event) -> bombay::behavior::BehaviorActed<Self> {
        match event {
            WatchEvent::Inner(User { message, .. }) if self.deadline_elapsed => {
                Err(ResultFact::LateReply(message))
            }
            WatchEvent::Inner(User { message, .. }) => Err(ResultFact::Reply(message)),
            WatchEvent::PeerStopped(PeerStopped { peer, .. }) if peer == CALLEE => {
                Err(ResultFact::CalleeRetired)
            }
            WatchEvent::PeerStopped(_) => Ok(Actions::cont()),
        }
    }
}

fn timeout(requester: &mut Requester) -> Result<bombay::behavior::Become<TestAddr>, ResultFact> {
    let _ = requester;
    Err(ResultFact::Timeout)
}

#[allow(
    clippy::unnecessary_wraps,
    reason = "the deadline reaction shares the behavior's controlled error domain"
)]
fn remember_timeout(
    requester: &mut Requester,
) -> Result<bombay::behavior::Become<TestAddr>, ResultFact> {
    requester.deadline_elapsed = true;
    Ok(Step::Continue)
}

type RequesterBehavior = Deadline<Requester>;
type RequesterEndpoint = ActorRef<TestAddr, MailboxAnchor<<RequesterBehavior as Behavior>::Event>>;
type CalleeEndpoint = ActorRef<TestAddr, MailboxAnchor<<Callee as Behavior>::Event>>;
type RequesterIncarnation = IncarnationEndpoint<TestAddr, RequesterEndpoint>;
type CalleeIncarnation = IncarnationEndpoint<TestAddr, CalleeEndpoint>;

#[derive(Clone, Default)]
struct Routes {
    requesters: AddressRouter<TestAddr, RequesterIncarnation>,
    callees: AddressRouter<TestAddr, CalleeIncarnation>,
}

impl EndpointRegistry<TestAddr, u8, RequesterIncarnation> for Routes {
    type Error = AddressInUse<TestAddr>;
    type Registration = <AddressRouter<TestAddr, RequesterIncarnation> as EndpointRegistry<
        TestAddr,
        u8,
        RequesterIncarnation,
    >>::Registration;

    fn register(
        &self,
        address: TestAddr,
        endpoint: RequesterIncarnation,
    ) -> Result<Self::Registration, Self::Error> {
        <AddressRouter<TestAddr, RequesterIncarnation> as EndpointRegistry<
            TestAddr,
            u8,
            RequesterIncarnation,
        >>::register(&self.requesters, address, endpoint)
    }
}

impl EndpointRegistry<TestAddr, Call, CalleeIncarnation> for Routes {
    type Error = AddressInUse<TestAddr>;
    type Registration = <AddressRouter<TestAddr, CalleeIncarnation> as EndpointRegistry<
        TestAddr,
        Call,
        CalleeIncarnation,
    >>::Registration;

    fn register(
        &self,
        address: TestAddr,
        endpoint: CalleeIncarnation,
    ) -> Result<Self::Registration, Self::Error> {
        <AddressRouter<TestAddr, CalleeIncarnation> as EndpointRegistry<
            TestAddr,
            Call,
            CalleeIncarnation,
        >>::register(&self.callees, address, endpoint)
    }
}

impl DeliveryRouter<TestAddr, u8> for Routes {
    type Error =
        <AddressRouter<TestAddr, RequesterIncarnation> as DeliveryRouter<TestAddr, u8>>::Error;

    async fn deliver(
        &self,
        from: TestAddr,
        delivery: Delivery<TestAddr, u8>,
    ) -> Result<(), Self::Error> {
        self.requesters.deliver(from, delivery).await
    }
}

impl DeliveryRouter<TestAddr, Call> for Routes {
    type Error =
        <AddressRouter<TestAddr, CalleeIncarnation> as DeliveryRouter<TestAddr, Call>>::Error;

    async fn deliver(
        &self,
        from: TestAddr,
        delivery: Delivery<TestAddr, Call>,
    ) -> Result<(), Self::Error> {
        self.callees.deliver(from, delivery).await
    }
}

impl PeerObserver<TestAddr> for Routes {
    fn observe_peer(
        &self,
        peer: TestAddr,
    ) -> Result<Observation<Result<Exit<TestAddr>, Crash>>, PeerObservationError<TestAddr>> {
        self.callees.observe_peer(peer)
    }
}

macro_rules! result_fact {
    ($outcome:expr) => {
        match $outcome {
            TaskOutcome::Returned(Err(bombay::RunError::Behavior(fact))) => fact,
            other => panic!("requester returned an unexpected outcome: {other:?}"),
        }
    };
}

#[tokio::test(start_paused = true)]
async fn typed_reply_is_ordinary_delivery_to_the_user_supplied_recipient() {
    let system = System::new(MailboxConfig::bounded(4), Routes::default());
    let callee = system.spawn(Actor::new(CALLEE, Callee)).unwrap();
    let requester = system
        .spawn(Actor::new(
            REQUESTER,
            Deadline::new(
                Requester::new(CalleeAction::Reply(7)),
                bombay::behavior::TimerId(0),
                Some(Instant::now() + Duration::from_secs(5)),
                timeout,
            ),
        ))
        .unwrap();

    assert_eq!(
        result_fact!(requester.outcome().await),
        ResultFact::Reply(7)
    );
    callee.abort();
    let _ = callee.outcome().await;
}

#[tokio::test(start_paused = true)]
async fn deadline_is_the_timeout_and_a_late_reply_is_an_explicit_user_message() {
    let system = System::new(MailboxConfig::bounded(4), Routes::default());
    let callee = system.spawn(Actor::new(CALLEE, Callee)).unwrap();
    let requester = system
        .spawn(Actor::new(
            REQUESTER,
            Deadline::new(
                Requester::new(CalleeAction::Ignore),
                bombay::behavior::TimerId(0),
                Some(Instant::now() + Duration::from_secs(1)),
                remember_timeout,
            ),
        ))
        .unwrap();
    let outcome = tokio::spawn(async move { requester.outcome().await });

    yield_now().await;
    advance(Duration::from_secs(1)).await;
    yield_now().await;
    callee
        .actor_ref()
        .send(
            TestAddr(0),
            Call {
                reply_to: Recipient::global(REQUESTER),
                action: CalleeAction::Reply(9),
            },
        )
        .await
        .unwrap();

    assert_eq!(
        result_fact!(outcome.await.unwrap()),
        ResultFact::LateReply(9)
    );
    callee.abort();
    let _ = callee.outcome().await;
}

#[tokio::test]
async fn observation_installed_before_delivery_reports_exact_callee_retirement() {
    let system = System::new(MailboxConfig::bounded(4), Routes::default());
    let _callee = system.spawn(Actor::new(CALLEE, Callee)).unwrap();
    let requester = system
        .spawn(Actor::new(
            REQUESTER,
            Deadline::new(
                Requester::new(CalleeAction::Retire),
                bombay::behavior::TimerId(0),
                Some(Instant::now() + Duration::from_secs(5)),
                timeout,
            ),
        ))
        .unwrap();

    assert_eq!(
        result_fact!(requester.outcome().await),
        ResultFact::CalleeRetired
    );
}

#[tokio::test]
async fn retired_reply_target_is_an_existing_delivery_failure() {
    let system = System::new(MailboxConfig::bounded(4), Routes::default());
    let callee = system.spawn(Actor::new(CALLEE, Callee)).unwrap();
    let requester = system
        .spawn(Actor::new(
            REQUESTER,
            Deadline::new(
                Requester::new(CalleeAction::Ignore),
                bombay::behavior::TimerId(0),
                Some(Instant::now()),
                timeout,
            ),
        ))
        .unwrap();
    assert_eq!(result_fact!(requester.outcome().await), ResultFact::Timeout);

    let call = Call {
        reply_to: Recipient::global(REQUESTER),
        action: CalleeAction::Reply(11),
    };
    callee.actor_ref().send(TestAddr(0), call).await.unwrap();

    let undelivered = match callee.outcome().await {
        TaskOutcome::Returned(Err(bombay::RunError::Environment(
            bombay::RuntimeEffectError::Delivery(bombay::RoutingError::UnknownAddress {
                address,
                message,
            }),
        ))) => {
            assert_eq!(address, REQUESTER);
            message
        }
        other => panic!("retired target returned an unexpected failure: {other:?}"),
    };
    assert_eq!(undelivered, 11);
}
