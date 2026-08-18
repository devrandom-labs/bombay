use std::collections::VecDeque;
use std::convert::Infallible;
use std::sync::{Arc, Mutex};

use behavior::{Actions, Behavior, BehaviorActed, MailAddr, Never, NoBirths, Step, User};
use bombay_engine::{ActionsOf, ActiveEnvironment, Completion};
use proptest::prelude::*;

#[derive(Debug, Clone, PartialEq, Eq)]
enum Fact {
    Commit(u8),
    Next(u8),
    Fold(u8),
    Retire,
}

struct ModelBehavior(Arc<Mutex<Vec<Fact>>>);

impl Behavior for ModelBehavior {
    type Protocol = behavior::MessageProtocol<MailAddr, u8>;
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

struct ModelEnvironment {
    events: VecDeque<u8>,
    facts: Arc<Mutex<Vec<Fact>>>,
}

impl ActiveEnvironment<ModelBehavior> for ModelEnvironment {
    type Error = Infallible;

    async fn next(&mut self) -> Option<<ModelBehavior as Behavior>::Event> {
        let value = self.events.pop_front()?;
        self.facts.lock().unwrap().push(Fact::Next(value));
        Some(User::new(MailAddr(7), value))
    }

    async fn apply(&mut self, actions: ActionsOf<ModelBehavior>) -> Result<(), Self::Error> {
        for value in actions.sends {
            self.facts.lock().unwrap().push(Fact::Commit(value));
        }
        Ok(())
    }

    async fn retire(self) {
        self.facts.lock().unwrap().push(Fact::Retire);
    }
}

fn execute(events: Vec<u8>) -> (Completion, Vec<Fact>) {
    let facts = Arc::new(Mutex::new(Vec::new()));
    let driver = direct(
        ModelBehavior(facts.clone()),
        ModelEnvironment {
            events: events.into(),
            facts: facts.clone(),
        },
    );
    let runtime = tokio::runtime::Builder::new_current_thread()
        .build()
        .unwrap();
    let completion = runtime.block_on(driver.run()).unwrap();
    let transcript = facts.lock().unwrap().clone();
    (completion, transcript)
}

fn model(events: &[u8]) -> (Completion, Vec<Fact>) {
    let mut facts = vec![Fact::Commit(0)];
    for &value in events {
        facts.push(Fact::Next(value));
        facts.push(Fact::Fold(value));
        facts.push(Fact::Commit(value));
        if value == u8::MAX {
            facts.push(Fact::Retire);
            return (Completion::Stopped, facts);
        }
    }
    facts.push(Fact::Retire);
    (Completion::Exhausted, facts)
}

proptest! {
    #[test]
    fn generated_runs_match_the_causal_model(events in prop::collection::vec(any::<u8>(), 0..256)) {
        prop_assert_eq!(execute(events.clone()), model(&events));
    }

    #[test]
    fn deterministic_replay_is_exact(events in prop::collection::vec(any::<u8>(), 0..256)) {
        prop_assert_eq!(execute(events.clone()), execute(events));
    }
}

#[test]
fn zero_singleton_limit_and_post_stop_boundaries() {
    for events in [vec![], vec![1], vec![254], vec![255], vec![1, 255, 2]] {
        assert_eq!(execute(events.clone()), model(&events));
    }
}
mod support;

use support::direct;
