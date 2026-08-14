mod support;

use std::sync::{Arc, Mutex};
use std::time::Duration;

use bombay::behavior::{Actions, Compose, Exit, MailAddr, Never, NoBirths};
use bombay::{
    Actor, AddressRouter, LifecycleEvent, LifecycleSink, LifecycleTransition, MailboxConfig,
    RunExit, System, TaskOutcome,
};
use proptest::prelude::*;

#[test]
fn generated_send_orders_preserve_payload_and_generation_laws() {
    let mut runner = proptest::test_runner::TestRunner::deterministic();
    runner
        .run(&proptest::collection::vec(any::<u8>(), 0..64), |bytes| {
            let order = support::permutation(&bytes);
            let runtime = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .unwrap();
            runtime.block_on(support::accepted_payloads_are_processed_once(&order));
            Ok(())
        })
        .unwrap();
}

#[test]
fn miri_supported_payload_and_reuse_composition() {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    runtime.block_on(support::accepted_payloads_are_processed_once(&[3, 1, 0, 2]));
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct ProducerOrderViolation;

struct PerProducerOrder {
    next: [u8; 2],
    received: usize,
}

#[bombay::behavior::behavior(addr = MailAddr, message = (u8, u8), sends = Vec<Never>, births = NoBirths, error = ProducerOrderViolation)]
impl PerProducerOrder {
    fn receive(
        &mut self,
        from: MailAddr,
        (producer, sequence): (u8, u8),
    ) -> bombay::behavior::Acted<MailAddr, Never, Vec<Never>, NoBirths, ProducerOrderViolation>
    {
        let Some(expected) = self.next.get_mut(usize::from(producer)) else {
            return Err(ProducerOrderViolation);
        };
        if from != MailAddr(u64::from(producer)) || sequence != *expected {
            return Err(ProducerOrderViolation);
        }
        *expected += 1;
        self.received += 1;
        if self.received == 32 {
            Ok(Actions::stop(Exit::Normal))
        } else {
            Ok(Actions::cont())
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_producers_preserve_local_order_without_assuming_interleaving() {
    let system = System::new(MailboxConfig::bounded(1), AddressRouter::default());
    let actor = system
        .spawn(Actor::new(
            MailAddr(1),
            PerProducerOrder {
                next: [0; 2],
                received: 0,
            },
        ))
        .unwrap();

    let mut producers = tokio::task::JoinSet::new();
    for producer in 0..2_u8 {
        let actor_ref = actor.actor_ref().clone();
        producers.spawn(async move {
            for sequence in 0..16_u8 {
                actor_ref
                    .send(MailAddr(u64::from(producer)), (producer, sequence))
                    .await
                    .unwrap();
            }
        });
    }
    while let Some(result) = producers.join_next().await {
        result.unwrap();
    }

    assert!(matches!(
        tokio::time::timeout(Duration::from_secs(2), actor.outcome())
            .await
            .unwrap(),
        TaskOutcome::Returned(Ok(RunExit::Stopped(Exit::Normal)))
    ));
}

#[derive(Clone, Default)]
struct Events(Arc<Mutex<Vec<LifecycleEvent<MailAddr>>>>);

impl LifecycleSink<MailAddr, bombay_address::RegistrationId> for Events {
    fn record(&self, event: LifecycleEvent<MailAddr>) {
        self.0.lock().unwrap().push(event);
    }
}

struct Waiting;

#[bombay::behavior::behavior(addr = MailAddr, message = Never, sends = Vec<Never>, births = NoBirths, error = Never)]
impl Waiting {
    fn receive(
        &mut self,
        _from: MailAddr,
        message: Never,
    ) -> bombay::behavior::Acted<MailAddr, Never, Vec<Never>, NoBirths, Never> {
        match message {}
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn abort_and_shutdown_races_publish_one_terminal_generation_before_reuse() {
    for round in 0..64 {
        let events = Events::default();
        let system = System::with_lifecycle(
            MailboxConfig::bounded(1),
            AddressRouter::default(),
            events.clone(),
        );
        let actor = system
            .spawn(Actor::from_definition(
                MailAddr(7),
                Compose::new(Waiting).stop_on_shutdown(),
            ))
            .unwrap();
        let actor_ref = actor.actor_ref().clone();
        let shutdown = tokio::spawn(async move { actor_ref.request_shutdown() });
        if round % 2 == 0 {
            actor.abort();
        } else {
            tokio::task::yield_now().await;
            actor.abort();
        }
        let _ = shutdown.await.unwrap();
        assert!(matches!(
            tokio::time::timeout(Duration::from_secs(2), actor.outcome())
                .await
                .unwrap(),
            TaskOutcome::Cancelled | TaskOutcome::Returned(Ok(RunExit::Stopped(Exit::Normal)))
        ));

        let replacement = system
            .spawn(Actor::from_definition(
                MailAddr(7),
                Compose::new(Waiting).stop_on_shutdown(),
            ))
            .unwrap();
        replacement.abort();
        assert!(matches!(
            replacement.outcome().await,
            TaskOutcome::Cancelled
        ));

        let recorded = events.0.lock().unwrap();
        // Facts are grouped by exact incarnation: caller-thread facts
        // (ShutdownRequested) cannot be bounded by Completed position —
        // a late emission can land after its generation's Completed.
        let mut generations: Vec<Vec<&LifecycleEvent<MailAddr>>> = Vec::new();
        for event in recorded.iter() {
            match generations
                .iter_mut()
                .find(|group| group[0].incarnation == event.incarnation)
            {
                Some(group) => group.push(event),
                None => generations.push(vec![event]),
            }
        }
        assert_eq!(generations.len(), 2, "exactly two incarnations");
        assert_generation_order(&generations[0]);
        assert_generation_order(&generations[1]);
        assert_ne!(generations[0][0].incarnation, generations[1][0].incarnation);
    }
}

fn assert_generation_order(events: &[&LifecycleEvent<MailAddr>]) {
    assert!(!events.is_empty());
    // Backbone: facts emitted by the spawner and the actor task keep total
    // order among themselves. The caller-thread shutdown fact is excluded:
    // request_shutdown emits after control-lane publication on the caller's
    // thread, which cannot be totally ordered against the actor task's
    // Retired/Completed emissions.
    let backbone: Vec<_> = events
        .iter()
        .filter(|event| event.transition != LifecycleTransition::ShutdownRequested)
        .collect();
    assert_eq!(
        backbone.first().unwrap().transition,
        LifecycleTransition::Prepared
    );
    assert_eq!(
        backbone[backbone.len() - 2].transition,
        LifecycleTransition::Retired
    );
    assert_eq!(
        backbone.last().unwrap().transition,
        LifecycleTransition::Completed
    );
    // Caller-thread fact: never before preparation, because the shutdown
    // task can only start after spawn returns the reference.
    if let Some(shutdown) = events
        .iter()
        .position(|event| event.transition == LifecycleTransition::ShutdownRequested)
    {
        let prepared = events
            .iter()
            .position(|event| event.transition == LifecycleTransition::Prepared)
            .unwrap();
        assert!(shutdown > prepared, "shutdown fact precedes preparation");
    }
    assert!(
        events
            .iter()
            .all(|event| event.incarnation == events[0].incarnation)
    );
    for transition in [
        LifecycleTransition::Prepared,
        LifecycleTransition::Started,
        LifecycleTransition::ShutdownRequested,
        LifecycleTransition::Retired,
        LifecycleTransition::Completed,
    ] {
        assert!(
            events
                .iter()
                .filter(|event| event.transition == transition)
                .count()
                <= 1
        );
    }
}
