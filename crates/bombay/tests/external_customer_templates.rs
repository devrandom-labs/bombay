use behavior_actors::{
    Barrier, BarrierGeneration, BarrierMembership, BarrierMessage, BarrierReleased, Cache,
    CacheConfiguration, CacheMessage, CacheResult, Latch, LatchMessage, LatchReleased,
};
use bombay::ProjectTerminal;
use bombay::behavior::MessageProtocol;
use bombay::prelude::*;

mod application_support;

use application_support::{RootTerminal, assert_completed};

type CacheReplies = MessageProtocol<MailAddr, CacheResult<u8, u16>>;

#[test]
fn cache_preserves_an_exact_external_customer() {
    let configuration = CacheConfiguration::new(2).expect("the cache capacity is positive");
    let cache = Cache::<MailAddr, u8, u16, EstablishedRecipient<CacheReplies>>::new(configuration);

    let ((), root_origin, joined_actor) = Application::new(cache.stop_on_shutdown())
        .run_with(|application| async move {
            let interface = application.interface(application.root().established_recipient());
            let lifecycle = application.lifecycle();
            let mut caller = interface
                .external::<CacheReplies>()
                .expect("the cache customer is established");

            caller
                .send(
                    interface.api(),
                    CacheMessage::Put {
                        key: 7,
                        value: 42,
                        reply_to: caller.recipient(),
                    },
                )
                .await
                .expect("the exact cache admits the put");
            let stored = caller.receive().await.map(|reply| reply.message);
            assert!(matches!(
                stored,
                Some(CacheResult::Stored {
                    key: 7,
                    replaced: None,
                    evicted: None,
                })
            ));

            caller
                .send(
                    interface.api(),
                    CacheMessage::Get {
                        key: 7,
                        reply_to: caller.recipient(),
                    },
                )
                .await
                .expect("the exact cache admits the get");
            let hit = caller.receive().await.map(|reply| reply.message);
            assert_eq!(hit, Some(CacheResult::Hit { key: 7, value: 42 }));
            let shutdown = lifecycle.request_shutdown();
            assert_eq!(shutdown, Ok(()));
            let repeated = lifecycle.request_shutdown();
            assert_eq!(repeated, Err(ShutdownRejection::AlreadyStopping));
        })
        .expect("the exact-customer cache application terminates normally");
    let terminal: RootTerminal<_> = ProjectTerminal::project(
        root_origin,
        joined_actor.unwrap_or_else(|failure| {
            panic!("the actual application actor task failed: {failure}")
        }),
    );
    assert_completed(terminal);
}

type BarrierReplies = MessageProtocol<MailAddr, BarrierReleased>;

#[test]
fn barrier_releases_two_exact_external_participants() {
    let membership =
        BarrierMembership::new(vec![1, 2]).expect("the barrier membership is distinct");
    let barrier = Barrier::<MailAddr, u8, EstablishedRecipient<BarrierReplies>>::new(membership);

    let ((), root_origin, joined_actor) = Application::new(barrier.stop_on_shutdown())
        .run_with(|application| async move {
            let interface = application.interface(application.root().established_recipient());
            let lifecycle = application.lifecycle();
            let mut first = interface
                .external::<BarrierReplies>()
                .expect("the first barrier participant is established");
            let mut second = interface
                .external::<BarrierReplies>()
                .expect("the second barrier participant is established");

            first
                .send(
                    interface.api(),
                    BarrierMessage {
                        generation: BarrierGeneration(0),
                        participant: 1,
                        reply_to: first.recipient(),
                    },
                )
                .await
                .expect("the barrier admits the first arrival");
            second
                .send(
                    interface.api(),
                    BarrierMessage {
                        generation: BarrierGeneration(0),
                        participant: 2,
                        reply_to: second.recipient(),
                    },
                )
                .await
                .expect("the barrier admits the second arrival");

            let first_release = first.receive().await.map(|reply| reply.message);
            assert_eq!(
                first_release,
                Some(BarrierReleased {
                    generation: BarrierGeneration(0),
                })
            );
            let second_release = second.receive().await.map(|reply| reply.message);
            assert_eq!(
                second_release,
                Some(BarrierReleased {
                    generation: BarrierGeneration(0),
                })
            );
            let shutdown = lifecycle.request_shutdown();
            assert_eq!(shutdown, Ok(()));
            let repeated = lifecycle.request_shutdown();
            assert_eq!(repeated, Err(ShutdownRejection::AlreadyStopping));
        })
        .expect("the exact-customer barrier application terminates normally");
    let terminal: RootTerminal<_> = ProjectTerminal::project(
        root_origin,
        joined_actor.unwrap_or_else(|failure| {
            panic!("the actual application actor task failed: {failure}")
        }),
    );
    assert_completed(terminal);
}

type LatchReplies = MessageProtocol<MailAddr, LatchReleased>;

#[test]
fn latch_releases_exact_external_participants() {
    let ((), root_origin, joined_actor) = Application::new(
        Latch::<MailAddr, EstablishedRecipient<LatchReplies>>::new(2).stop_on_shutdown(),
    )
    .run_with(|application| async move {
        let interface = application.interface(application.root().established_recipient());
        let lifecycle = application.lifecycle();
        let mut first = interface
            .external::<LatchReplies>()
            .expect("the first latch participant is established");
        let mut second = interface
            .external::<LatchReplies>()
            .expect("the second latch participant is established");

        first
            .send(interface.api(), LatchMessage::arrive(first.recipient()))
            .await
            .expect("the latch admits the first arrival");
        second
            .send(interface.api(), LatchMessage::arrive(second.recipient()))
            .await
            .expect("the latch admits the second arrival");

        let first_release = first.receive().await.map(|reply| reply.message);
        assert_eq!(first_release, Some(LatchReleased));
        let second_release = second.receive().await.map(|reply| reply.message);
        assert_eq!(second_release, Some(LatchReleased));
        let shutdown = lifecycle.request_shutdown();
        assert_eq!(shutdown, Ok(()));
        let repeated = lifecycle.request_shutdown();
        assert_eq!(repeated, Err(ShutdownRejection::AlreadyStopping));
    })
    .expect("the exact-route latch application terminates normally");
    let terminal: RootTerminal<_> = ProjectTerminal::project(
        root_origin,
        joined_actor.unwrap_or_else(|failure| {
            panic!("the actual application actor task failed: {failure}")
        }),
    );
    assert_completed(terminal);
}
