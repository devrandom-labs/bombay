use std::collections::BTreeSet;
use std::time::Duration;

use bombay::behavior::{Actions, Exit, Handler, MailAddr, Never, NoBirths, Pure};
use bombay::{Actor, AddressRouter, MailboxConfig, RunExit, System, TaskOutcome};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct InvalidPayload;

struct AccountFor {
    expected: BTreeSet<u8>,
    seen: BTreeSet<u8>,
}

impl Handler<u8, NoBirths, InvalidPayload> for AccountFor {
    type Addr = MailAddr;
    type Msg = u8;

    fn receive(
        &mut self,
        _from: MailAddr,
        message: u8,
    ) -> bombay::behavior::Acted<
        MailAddr,
        Never,
        Vec<bombay::behavior::Delivery<MailAddr, u8>>,
        NoBirths,
        InvalidPayload,
    > {
        if !self.expected.contains(&message) || !self.seen.insert(message) {
            return Err(InvalidPayload);
        }
        if self.seen == self.expected {
            Ok(Actions::stop(Exit::Normal))
        } else {
            Ok(Actions::cont())
        }
    }
}

pub fn permutation(bytes: &[u8]) -> Vec<u8> {
    let length = usize::from(bytes.first().copied().unwrap_or(0) % 16) + 1;
    let mut ranked: Vec<_> = (0..length)
        .map(|index| {
            (
                bytes.get(index + 1).copied().unwrap_or(0),
                u8::try_from(index).expect("permutation length is capped at sixteen"),
            )
        })
        .collect();
    ranked.sort_unstable();
    ranked.into_iter().map(|(_, value)| value).collect()
}

pub async fn accepted_payloads_are_processed_once(order: &[u8]) {
    let expected = order.iter().copied().collect::<BTreeSet<_>>();
    assert_eq!(expected.len(), order.len(), "oracle input must be unique");

    let system = System::new(MailboxConfig::bounded(1), AddressRouter::default());
    let actor = system
        .spawn(Actor::new(
            MailAddr(1),
            Pure::new(AccountFor {
                expected,
                seen: BTreeSet::new(),
            }),
        ))
        .expect("fresh address must be claimable");

    let mut sends = tokio::task::JoinSet::new();
    for message in order.iter().copied() {
        let actor_ref = actor.actor_ref().clone();
        sends.spawn(async move { actor_ref.send(MailAddr(0), message).await });
    }
    while let Some(result) = sends.join_next().await {
        result
            .expect("producer task must not panic")
            .expect("every generated payload must be accepted");
    }

    let outcome = tokio::time::timeout(Duration::from_secs(2), actor.outcome())
        .await
        .expect("all accepted payloads must reach the behavior");
    assert!(matches!(
        outcome,
        TaskOutcome::Returned(Ok(RunExit::Stopped(Exit::Normal)))
    ));

    let replacement = system
        .spawn(Actor::new(
            MailAddr(1),
            Pure::new(AccountFor {
                expected: [0].into(),
                seen: BTreeSet::new(),
            }),
        ))
        .expect("terminal publication must follow registration release");
    replacement.actor_ref().send(MailAddr(0), 0).await.unwrap();
    assert!(matches!(
        replacement.outcome().await,
        TaskOutcome::Returned(Ok(RunExit::Stopped(Exit::Normal)))
    ));
}
