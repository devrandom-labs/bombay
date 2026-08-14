//! Transactional root-activation ordering and rollback oracles.

use std::convert::Infallible;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use behavior::{Actions, Behavior, Delivery, MailAddr, Never, NoBirths, Recipient, Step, User};
use bombay::{
    Actor, AddressRouter, DeliveryEndpoint, DeliveryRouter, EndpointRegistry, MailboxConfig,
    RootEndpoint, System, SystemBirthError, TaskOutcome,
};

#[derive(Clone)]
struct ProbeRouter(Arc<Mutex<Vec<&'static str>>>);

struct Registration(Arc<Mutex<Vec<&'static str>>>);

impl Drop for Registration {
    fn drop(&mut self) {
        self.0.lock().unwrap().push("retired");
    }
}

impl<D> EndpointRegistry<MailAddr, u8, D> for ProbeRouter {
    type Error = Infallible;
    type Registration = Registration;

    fn register(&self, _: MailAddr, _: D) -> Result<Self::Registration, Self::Error> {
        self.0.lock().unwrap().push("registered");
        Ok(Registration(self.0.clone()))
    }
}

impl DeliveryRouter<MailAddr, u8> for ProbeRouter {
    type Error = Infallible;

    async fn deliver(&self, _: MailAddr, _: Delivery<MailAddr, u8>) -> Result<(), Self::Error> {
        self.0.lock().unwrap().push("init-effect");
        Ok(())
    }
}

struct OrderedInit(Arc<Mutex<Vec<&'static str>>>);

impl Behavior for OrderedInit {
    type Addr = MailAddr;
    type Msg = u8;
    type Event = User<MailAddr, u8>;
    type Sends = Vec<Delivery<MailAddr, u8>>;
    type Ph = Never;
    type Error = Never;
    type Birth = NoBirths;

    fn init(&mut self) -> behavior::BehaviorActed<Self> {
        self.0.lock().unwrap().push("init");
        Ok(Actions::new(
            vec![Delivery::new(Recipient::global(MailAddr(9)), 1)],
            Vec::new(),
            Step::Continue,
        ))
    }

    fn transition(&mut self, _: Self::Event) -> behavior::BehaviorActed<Self> {
        Ok(Actions::cont())
    }
}

#[derive(Debug, PartialEq, Eq)]
struct InitFailed;

struct FailingInit;

impl Behavior for FailingInit {
    type Addr = MailAddr;
    type Msg = u8;
    type Event = User<MailAddr, u8>;
    type Sends = Vec<Delivery<MailAddr, u8>>;
    type Ph = Never;
    type Error = InitFailed;
    type Birth = NoBirths;

    fn init(&mut self) -> behavior::BehaviorActed<Self> {
        Err(InitFailed)
    }

    fn transition(&mut self, _: Self::Event) -> behavior::BehaviorActed<Self> {
        unreachable!()
    }
}

struct DropProbe(Arc<AtomicUsize>);

impl Drop for DropProbe {
    fn drop(&mut self) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }
}

impl Behavior for DropProbe {
    type Addr = MailAddr;
    type Msg = u8;
    type Event = User<MailAddr, u8>;
    type Sends = Vec<Delivery<MailAddr, u8>>;
    type Ph = Never;
    type Error = Never;
    type Birth = NoBirths;

    fn init(&mut self) -> behavior::BehaviorActed<Self> {
        Ok(Actions::cont())
    }

    fn transition(&mut self, _: Self::Event) -> behavior::BehaviorActed<Self> {
        Ok(Actions::cont())
    }
}

#[derive(Debug, PartialEq, Eq)]
struct EffectFailed;

#[derive(Clone)]
struct FailingEffectRouter(Arc<Mutex<Vec<&'static str>>>);

impl<D> EndpointRegistry<MailAddr, u8, D> for FailingEffectRouter {
    type Error = Infallible;
    type Registration = ();

    fn register(&self, _: MailAddr, _: D) -> Result<(), Infallible> {
        self.0.lock().unwrap().push("registered");
        Ok(())
    }
}

impl DeliveryRouter<MailAddr, u8> for FailingEffectRouter {
    type Error = EffectFailed;

    async fn deliver(&self, _: MailAddr, _: Delivery<MailAddr, u8>) -> Result<(), Self::Error> {
        Err(EffectFailed)
    }
}

#[derive(Debug, PartialEq, Eq)]
struct RegistrationFailed;

#[derive(Clone)]
struct RejectingRegistrationRouter;

impl<D> EndpointRegistry<MailAddr, u8, D> for RejectingRegistrationRouter {
    type Error = RegistrationFailed;
    type Registration = ();

    fn register(&self, _: MailAddr, _: D) -> Result<(), Self::Error> {
        Err(RegistrationFailed)
    }
}

impl DeliveryRouter<MailAddr, u8> for RejectingRegistrationRouter {
    type Error = Infallible;

    async fn deliver(&self, _: MailAddr, _: Delivery<MailAddr, u8>) -> Result<(), Self::Error> {
        Ok(())
    }
}

fn assert_entity_seats<E: Clone + Send + Sync + 'static, L: Send + 'static>(_: &E, _: &L) {}

#[tokio::test]
async fn activation_completes_init_effects_before_registration_and_returns_separate_seats() {
    let events = Arc::new(Mutex::new(Vec::new()));
    let system = System::new(MailboxConfig::bounded(1), ProbeRouter(events.clone()));

    let activated = system
        .activate(Actor::new(MailAddr(1), OrderedInit(events.clone())))
        .await
        .unwrap();

    assert_eq!(
        &*events.lock().unwrap(),
        &["init", "init-effect", "registered"]
    );
    assert_entity_seats(&activated.endpoint, &activated.retirement);
    let clone = activated.endpoint.clone();
    DeliveryEndpoint::deliver(&clone, MailAddr(2), 7)
        .await
        .unwrap();

    activated.retirement.abort();
    assert!(matches!(
        activated.retirement.outcome().await,
        TaskOutcome::Cancelled
    ));
    assert_eq!(events.lock().unwrap().last(), Some(&"retired"));
}

#[tokio::test]
async fn failed_initialization_never_registers_or_returns_an_endpoint() {
    let events = Arc::new(Mutex::new(Vec::new()));
    let system = System::new(MailboxConfig::bounded(1), ProbeRouter(events.clone()));

    let failure = system.activate(Actor::new(MailAddr(1), FailingInit)).await;

    assert!(matches!(
        failure,
        Err(SystemBirthError::Initialization(InitFailed))
    ));
    assert!(events.lock().unwrap().is_empty());
}

#[tokio::test]
async fn failed_initialization_effect_never_reaches_registration() {
    let events = Arc::new(Mutex::new(Vec::new()));
    let system = System::new(
        MailboxConfig::bounded(1),
        FailingEffectRouter(events.clone()),
    );

    let failure = system
        .activate(Actor::new(MailAddr(1), OrderedInit(events.clone())))
        .await;

    assert!(matches!(failure, Err(SystemBirthError::Effects(_))));
    assert_eq!(&*events.lock().unwrap(), &["init"]);
}

#[tokio::test]
async fn registration_failure_stays_typed_after_successful_initialization() {
    let events = Arc::new(Mutex::new(Vec::new()));
    let system = System::new(MailboxConfig::bounded(1), RejectingRegistrationRouter);

    let failure = system
        .activate(Actor::new(MailAddr(1), OrderedInit(events.clone())))
        .await;

    assert!(matches!(
        failure,
        Err(SystemBirthError::Registration(RegistrationFailed))
    ));
    assert_eq!(&*events.lock().unwrap(), &["init"]);
}

#[tokio::test]
async fn registration_collision_drops_provisional_resources_once_and_address_reuses() {
    let router = AddressRouter::default();
    let system = System::new(MailboxConfig::bounded(1), router);
    let live_drops = Arc::new(AtomicUsize::new(0));
    let live = system
        .spawn(Actor::new(MailAddr(1), DropProbe(live_drops.clone())))
        .unwrap();
    let failed_drops = Arc::new(AtomicUsize::new(0));

    let collision = system
        .activate(Actor::new(MailAddr(1), DropProbe(failed_drops.clone())))
        .await;

    assert!(matches!(collision, Err(SystemBirthError::Registration(_))));
    assert_eq!(failed_drops.load(Ordering::SeqCst), 1);
    live.abort();
    assert!(matches!(live.outcome().await, TaskOutcome::Cancelled));
    assert_eq!(live_drops.load(Ordering::SeqCst), 1);

    let replacement_drops = Arc::new(AtomicUsize::new(0));
    let replacement = system
        .activate(Actor::new(
            MailAddr(1),
            DropProbe(replacement_drops.clone()),
        ))
        .await
        .unwrap();
    replacement.retirement.abort();
    assert!(matches!(
        replacement.retirement.outcome().await,
        TaskOutcome::Cancelled
    ));
    assert_eq!(replacement_drops.load(Ordering::SeqCst), 1);
}

const _: fn(RootEndpoint<OrderedInit>) = |_| {};
