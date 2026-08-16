#![no_main]

use std::collections::VecDeque;
use std::convert::Infallible;
use std::future::Future;
use std::pin::pin;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll, Waker};

use behavior::{Actions, Behavior, BehaviorActed, MailAddr, Never, NoBirths, Step, User};
use bombay_engine::{ActionsOf, ActiveEnvironment, Completion, Driver, Environment};
use libfuzzer_sys::fuzz_target;

#[derive(Debug, Clone, PartialEq, Eq)]
enum Fact {
    Commit(u8),
    Next(u8),
    Fold(u8),
    Retire,
}

struct FuzzBehavior(Arc<Mutex<Vec<Fact>>>);

impl Behavior for FuzzBehavior {
    type Addr = MailAddr;
    type Msg = u8;
    type Event = User<MailAddr, u8>;
    type Sends = Vec<u8>;
    type Ph = Never;
    type Error = Infallible;
    type Birth = NoBirths;

    fn init(&mut self, _: behavior::InitializationTurn) -> BehaviorActed<Self> {
        Ok(Actions::send(vec![0]))
    }

    fn transition(&mut self, _: behavior::ActiveTurn, event: Self::Event) -> BehaviorActed<Self> {
        let value = event.message;
        self.0.lock().unwrap().push(Fact::Fold(value));
        Ok(Actions::new(
            vec![value],
            Vec::new(),
            if value == u8::MAX {
                Step::Stop(behavior::Stopped)
            } else {
                Step::Continue
            },
        ))
    }
}

struct FuzzEnvironment {
    events: VecDeque<u8>,
    facts: Arc<Mutex<Vec<Fact>>>,
}

impl ActiveEnvironment<FuzzBehavior> for FuzzEnvironment {
    type Error = Infallible;

    async fn next(&mut self) -> Option<<FuzzBehavior as Behavior>::Event> {
        let value = self.events.pop_front()?;
        self.facts.lock().unwrap().push(Fact::Next(value));
        Some(User::new(MailAddr(1), value))
    }

    async fn apply(&mut self, actions: ActionsOf<FuzzBehavior>) -> Result<(), Self::Error> {
        for value in actions.sends {
            self.facts.lock().unwrap().push(Fact::Commit(value));
        }
        Ok(())
    }

    async fn retire(self) {
        self.facts.lock().unwrap().push(Fact::Retire);
    }
}

impl Environment<FuzzBehavior> for FuzzEnvironment {
    type Active = Self;
    type Error = Infallible;

    async fn activate(mut self, actions: ActionsOf<FuzzBehavior>) -> Result<Self, Self::Error> {
        self.apply(actions).await?;
        Ok(self)
    }
}

fn block_on(future: impl Future<Output = Completion>) -> Completion {
    let mut future = pin!(future);
    let waker = Waker::noop();
    let mut context = Context::from_waker(waker);
    match future.as_mut().poll(&mut context) {
        Poll::Ready(output) => output,
        Poll::Pending => panic!("fully local fuzz environment must not pend"),
    }
}

fuzz_target!(|input: &[u8]| {
    let events = input.iter().copied().take(1024).collect::<Vec<_>>();
    let facts = Arc::new(Mutex::new(Vec::new()));
    let completion = block_on(async {
        Driver::new(
            FuzzBehavior(facts.clone()),
            FuzzEnvironment {
                events: events.clone().into(),
                facts: facts.clone(),
            },
        )
        .run()
        .await
        .unwrap()
    });

    let mut expected = vec![Fact::Commit(0)];
    let mut expected_completion = Completion::Exhausted;
    for value in events {
        expected.push(Fact::Next(value));
        expected.push(Fact::Fold(value));
        expected.push(Fact::Commit(value));
        if value == u8::MAX {
            expected_completion = Completion::Stopped;
            break;
        }
    }
    expected.push(Fact::Retire);
    assert_eq!(completion, expected_completion);
    assert_eq!(*facts.lock().unwrap(), expected);
});
