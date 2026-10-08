use core::time::Duration;
use std::time::Instant;

use behavior::{Behavior, BehaviorMessage, Never};
use behavior_actors::{
    Deadline, DeadlineReaction, OneShot, Periodic, ReceiveTimeout, Stash, StashRoute,
    StopOnShutdown, TimedReaction, TimerId,
};

/// Discover and compose reusable wrappers from any concrete actor behavior.
///
/// Every method returns the exact existing Behavior Actors wrapper named by
/// the method. The call order is therefore visible in the inferred concrete
/// type and remains semantically significant. No policy is selected unless it
/// is supplied explicitly by the application.
pub trait ActorExt: Behavior + Sized {
    /// Stash selected messages using a pure decision over the inner behavior and message.
    ///
    /// Replay observes the inner state returned by each preceding transition.
    #[must_use]
    fn with_stash(self, route: fn(&Self, &BehaviorMessage<Self>) -> StashRoute) -> Stash<Self>
    where
        Self: Behavior<Ph = Never>,
    {
        Stash::new(self, route)
    }

    /// React once after the relative delay.
    #[must_use]
    fn with_one_shot(
        self,
        id: TimerId,
        after: Duration,
        on_elapsed: TimedReaction<Self>,
    ) -> OneShot<Self> {
        OneShot::new(self, id, after, on_elapsed)
    }

    /// React after each relative interval while the behavior continues.
    #[must_use]
    fn with_periodic(
        self,
        id: TimerId,
        every: Duration,
        on_elapsed: TimedReaction<Self>,
    ) -> Periodic<Self> {
        Periodic::new(self, id, every, on_elapsed)
    }

    /// React once at an optional absolute deadline.
    #[must_use]
    fn with_deadline(
        self,
        id: TimerId,
        at: Option<Instant>,
        on_reached: DeadlineReaction<Self>,
    ) -> Deadline<Self> {
        Deadline::new(self, id, at, on_reached)
    }

    /// React after one idle period, rearmed by successful user messages.
    #[must_use]
    fn with_receive_timeout(
        self,
        id: TimerId,
        after: Duration,
        on_elapsed: TimedReaction<Self>,
    ) -> ReceiveTimeout<Self> {
        ReceiveTimeout::new(self, id, after, on_elapsed)
    }

    /// Stop normally when this wrapper receives the typed shutdown request.
    #[must_use]
    fn stop_on_shutdown(self) -> StopOnShutdown<Self> {
        StopOnShutdown::new(self)
    }
}

impl<B: Behavior> ActorExt for B {}
