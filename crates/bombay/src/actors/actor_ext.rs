use core::time::Duration;
use std::time::Instant;

use behavior::{
    Behavior, BehaviorAddr, BehaviorMessage, EndpointAddress, Never, Protocol, SendsFor, User,
};
use behavior_actors::{
    Deadline, DeadlineReaction, DeliveryRoute, EstablishedTerminationMonitor,
    EstablishedTerminationReaction, FinalizeOnShutdown, ObserveEstablished, OneShot, Periodic,
    ReceiveTimeout, ShutdownReaction, Stash, StashRoute, StaticallyInfallible, StopOnShutdown,
    TimedReaction,
};

/// Discover and compose reusable wrappers from any concrete actor behavior.
///
/// Every method returns the exact existing Behavior Actors wrapper named by
/// the method. The call order is therefore visible in the inferred concrete
/// type and remains semantically significant. No policy is selected unless it
/// is supplied explicitly by the application.
pub trait ActorExt: Behavior + Sized {
    /// Hold selected original inputs up to `limit`, returning overflow to `refusals`.
    ///
    /// Replay observes the inner state returned by each preceding transition.
    /// The inner transition must be statically infallible: replay cannot undo
    /// actions already produced by earlier messages. A refusal sends the whole
    /// original User, including its sender, and continues without changing the
    /// inner actor. Zero refuses all newly selected held inputs; delivery,
    /// release and non-user events remain available.
    #[must_use]
    fn with_stash<R>(
        self,
        limit: usize,
        refusals: R,
        route: fn(&Self, &BehaviorMessage<Self>) -> StashRoute,
    ) -> Stash<Self, R>
    where
        Self: Behavior<Ph = Never>,
        Self::Error: StaticallyInfallible,
        R: DeliveryRoute + Clone,
        R::Protocol: Protocol<
                Addr = BehaviorAddr<Self>,
                Msg = User<BehaviorAddr<Self>, BehaviorMessage<Self>>,
            >,
        R::Sends: SendsFor<Self::Event>,
    {
        Stash::new(self, limit, refusals, route)
    }

    /// React once after the relative delay.
    #[must_use]
    fn with_one_shot(self, after: Duration, on_elapsed: TimedReaction<Self>) -> OneShot<Self> {
        OneShot::new(self, after, on_elapsed)
    }

    /// React after each relative interval while the behavior continues.
    #[must_use]
    fn with_periodic(self, every: Duration, on_elapsed: TimedReaction<Self>) -> Periodic<Self> {
        Periodic::new(self, every, on_elapsed)
    }

    /// React once at an optional absolute deadline.
    #[must_use]
    fn with_deadline(
        self,
        at: Option<Instant>,
        on_reached: DeadlineReaction<Self>,
    ) -> Deadline<Self> {
        Deadline::new(self, at, on_reached)
    }

    /// React after one idle period, rearmed by successful user messages.
    #[must_use]
    fn with_receive_timeout(
        self,
        after: Duration,
        on_elapsed: TimedReaction<Self>,
    ) -> ReceiveTimeout<Self> {
        ReceiveTimeout::new(self, after, on_elapsed)
    }

    /// React once to the exact selected recipient's terminal report.
    ///
    /// The existing monitor retains the original request, observation authority,
    /// cancellation and rejection values. Its reaction owns the whole matching
    /// report and returns explicit Actions; construction starts no observation.
    /// Shutdown of this actor does not terminate the selected recipient.
    #[must_use]
    fn observe_termination<P>(
        self,
        request: ObserveEstablished<P>,
        reaction: EstablishedTerminationReaction<Self, P>,
    ) -> EstablishedTerminationMonitor<Self, P>
    where
        P: Protocol<Addr = BehaviorAddr<Self>>,
        BehaviorAddr<Self>: EndpointAddress,
    {
        EstablishedTerminationMonitor::established(self, request, reaction)
    }

    /// Stop normally when this wrapper receives the typed shutdown request.
    #[must_use]
    fn stop_on_shutdown(self) -> StopOnShutdown<Self> {
        StopOnShutdown::new(self)
    }

    /// Run a pure final transition on shutdown, then stop normally.
    ///
    /// The reaction's sends and fresh creations survive its continuation verdict.
    /// An outer shutdown wrapper takes precedence over this reaction.
    #[must_use]
    fn finalize_on_shutdown(self, reaction: ShutdownReaction<Self>) -> FinalizeOnShutdown<Self> {
        FinalizeOnShutdown::new(self, reaction)
    }
}

impl<B: Behavior> ActorExt for B {}
