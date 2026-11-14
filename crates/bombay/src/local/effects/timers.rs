use crate::address::MailAddr;
use crate::local::effects::ApplicationCapabilities;
use behavior::{
    ActionItem, Behavior, Here, InjectEvent, Inside, InterpretItem, ItemSettlement, Protocol,
};
use behavior_actors::{
    OwnedScheduleAfter, OwnedScheduleAt, OwnedTimerElapsed, ScheduleAfter, ScheduleAfterRejection,
    ScheduleAt, ScheduleAtRejection, TimerElapsed, TimerId, timer,
};
use core::future::Future;
use std::time::Instant;
use timers::{ScheduleError, TimerQueue};

impl<C, N, P, Bindings, Origins, Path, Services, Target>
    InterpretItem<timer::ScheduleAt<Target>, C::Event, Path>
    for ApplicationCapabilities<C, N, P, Bindings, Origins, Services>
where
    C: Behavior<Protocol: Protocol<Addr = MailAddr>>,
    Target: TimerDestination<Path>,
    C::Event: InjectEvent<timer::TimerElapsed<Target>, Path>,
    Self: Send,
{
    #[expect(
        clippy::manual_async_fn,
        reason = "retain the original receiving-loan opaque future; an async function captures unused route tags and would require new lifetime bounds"
    )]
    fn interpret_item<'a>(
        &'a mut self,
        input: <timer::ScheduleAt<Target> as ActionItem>::Input<'a>,
        received: &'a mut Option<<timer::ScheduleAt<Target> as ActionItem>::Reply>,
    ) -> impl Future<Output = ()> + Send + 'a
    where
        timer::ScheduleAt<Target>: 'a,
    {
        async move {
            if received.is_some() {
                return;
            }
            // Conversion may panic; retain the outside request until settlement.
            let Some(request) = *input else {
                return;
            };
            let settlement = {
                match Target::schedule_at(&mut self.timers, request) {
                    Ok(()) => ItemSettlement::Accepted(timer::TimerScheduled {
                        id: request.id,
                        generation: request.generation,
                    }),
                    Err(crate::local::effects::timers::TimerError::GenerationExhausted) => {
                        ItemSettlement::Rejected {
                            item: request,
                            reason: ScheduleAtRejection::QueueGenerationExhausted,
                        }
                    }
                    Err(crate::local::effects::timers::TimerError::SequenceExhausted) => {
                        ItemSettlement::Rejected {
                            item: request,
                            reason: ScheduleAtRejection::QueueSequenceExhausted,
                        }
                    }
                    Err(crate::local::effects::timers::TimerError::DeadlineOverflow) => {
                        unreachable!("absolute scheduling does not add a relative deadline")
                    }
                }
            };
            *input = None;
            *received = Some(settlement);
        }
    }
}

impl<C, N, P, Bindings, Origins, Path, Services, Target>
    InterpretItem<timer::ScheduleAfter<Target>, C::Event, Path>
    for ApplicationCapabilities<C, N, P, Bindings, Origins, Services>
where
    C: Behavior<Protocol: Protocol<Addr = MailAddr>>,
    Target: TimerDestination<Path>,
    C::Event: InjectEvent<timer::TimerElapsed<Target>, Path>,
    Self: Send,
{
    #[expect(
        clippy::manual_async_fn,
        reason = "retain the original receiving-loan opaque future; an async function captures unused route tags and would require new lifetime bounds"
    )]
    fn interpret_item<'a>(
        &'a mut self,
        input: <timer::ScheduleAfter<Target> as ActionItem>::Input<'a>,
        received: &'a mut Option<<timer::ScheduleAfter<Target> as ActionItem>::Reply>,
    ) -> impl Future<Output = ()> + Send + 'a
    where
        timer::ScheduleAfter<Target>: 'a,
    {
        async move {
            if received.is_some() {
                return;
            }
            // Conversion may panic; retain the outside request until settlement.
            let Some(request) = *input else {
                return;
            };
            let settlement = {
                match Target::schedule_after(&mut self.timers, request) {
                    Ok(()) => ItemSettlement::Accepted(timer::TimerScheduled {
                        id: request.id,
                        generation: request.generation,
                    }),
                    Err(crate::local::effects::timers::TimerError::DeadlineOverflow) => {
                        ItemSettlement::Rejected {
                            item: request,
                            reason: ScheduleAfterRejection::DeadlineOverflow,
                        }
                    }
                    Err(crate::local::effects::timers::TimerError::GenerationExhausted) => {
                        ItemSettlement::Rejected {
                            item: request,
                            reason: ScheduleAfterRejection::QueueGenerationExhausted,
                        }
                    }
                    Err(crate::local::effects::timers::TimerError::SequenceExhausted) => {
                        ItemSettlement::Rejected {
                            item: request,
                            reason: ScheduleAfterRejection::QueueSequenceExhausted,
                        }
                    }
                }
            };
            *input = None;
            *received = Some(settlement);
        }
    }
}

/// One actor has disjoint named and occurrence-owned replacement namespaces.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
enum TimerKey {
    Named(TimerId),
    Owned(usize),
}

trait TimerOccurrence {
    const POSITION: usize;
}

impl TimerOccurrence for Here {
    const POSITION: usize = 0;
}

impl<Path: TimerOccurrence> TimerOccurrence for Inside<Path> {
    // Constant evaluation rejects overflow on every supported pointer width.
    const POSITION: usize = Path::POSITION + 1;
}

/// Select the real scheduling port promised by the request's concrete target.
trait TimerDestination<Path>: timer::TimerTarget {
    fn schedule_at<Event>(
        timers: &mut LocalTimers<Event>,
        request: timer::ScheduleAt<Self>,
    ) -> Result<(), TimerError>
    where
        Event: InjectEvent<timer::TimerElapsed<Self>, Path>;

    fn schedule_after<Event>(
        timers: &mut LocalTimers<Event>,
        request: timer::ScheduleAfter<Self>,
    ) -> Result<(), TimerError>
    where
        Event: InjectEvent<timer::TimerElapsed<Self>, Path>;
}

impl<Path> TimerDestination<Path> for TimerId {
    fn schedule_at<Event>(
        timers: &mut LocalTimers<Event>,
        request: ScheduleAt,
    ) -> Result<(), TimerError>
    where
        Event: InjectEvent<TimerElapsed, Path>,
    {
        timers.schedule_at::<Path>(request)
    }

    fn schedule_after<Event>(
        timers: &mut LocalTimers<Event>,
        request: ScheduleAfter,
    ) -> Result<(), TimerError>
    where
        Event: InjectEvent<TimerElapsed, Path>,
    {
        timers.schedule_after::<Path>(request)
    }
}

impl<Path: TimerOccurrence> TimerDestination<Path> for Here {
    fn schedule_at<Event>(
        timers: &mut LocalTimers<Event>,
        request: OwnedScheduleAt,
    ) -> Result<(), TimerError>
    where
        Event: InjectEvent<OwnedTimerElapsed, Path>,
    {
        timers.schedule_owned_at::<Path>(request)
    }

    fn schedule_after<Event>(
        timers: &mut LocalTimers<Event>,
        request: OwnedScheduleAfter,
    ) -> Result<(), TimerError>
    where
        Event: InjectEvent<OwnedTimerElapsed, Path>,
    {
        timers.schedule_owned_after::<Path>(request)
    }
}

impl<Event> Default for LocalTimers<Event> {
    fn default() -> Self {
        Self::new()
    }
}

impl<Event> LocalTimers<Event> {
    pub(crate) fn new() -> Self {
        Self {
            queue: TimerQueue::new(),
        }
    }

    pub(crate) fn schedule_at<Path>(&mut self, schedule: ScheduleAt) -> Result<(), TimerError>
    where
        Event: InjectEvent<TimerElapsed, Path>,
    {
        self.queue
            .schedule(
                TimerKey::Named(schedule.id),
                schedule.at,
                Event::inject_at(TimerElapsed::new(schedule.id, schedule.generation)),
            )
            .map(|_token| ())
            .map_err(|error| match error {
                ScheduleError::GenerationExhausted { .. } => TimerError::GenerationExhausted,
                ScheduleError::SequenceExhausted { .. } => TimerError::SequenceExhausted,
            })
    }

    pub(crate) fn schedule_after<Path>(&mut self, schedule: ScheduleAfter) -> Result<(), TimerError>
    where
        Event: InjectEvent<TimerElapsed, Path>,
    {
        let deadline = Instant::now()
            .checked_add(schedule.after)
            .ok_or(TimerError::DeadlineOverflow)?;
        self.schedule_at::<Path>(ScheduleAt::new(schedule.id, schedule.generation, deadline))
    }

    fn schedule_owned_at<Path>(&mut self, schedule: OwnedScheduleAt) -> Result<(), TimerError>
    where
        Path: TimerOccurrence,
        Event: InjectEvent<OwnedTimerElapsed, Path>,
    {
        self.queue
            .schedule(
                TimerKey::Owned(Path::POSITION),
                schedule.at,
                Event::inject_at(OwnedTimerElapsed::new(schedule.generation)),
            )
            .map(|_token| ())
            .map_err(|error| match error {
                ScheduleError::GenerationExhausted { .. } => TimerError::GenerationExhausted,
                ScheduleError::SequenceExhausted { .. } => TimerError::SequenceExhausted,
            })
    }

    fn schedule_owned_after<Path>(&mut self, schedule: OwnedScheduleAfter) -> Result<(), TimerError>
    where
        Path: TimerOccurrence,
        Event: InjectEvent<OwnedTimerElapsed, Path>,
    {
        let deadline = Instant::now()
            .checked_add(schedule.after)
            .ok_or(TimerError::DeadlineOverflow)?;
        self.schedule_owned_at::<Path>(OwnedScheduleAt::new(schedule.generation, deadline))
    }

    pub(crate) fn next_deadline(&mut self) -> Option<Instant> {
        self.queue.next_deadline()
    }

    pub(crate) fn pop_due(&mut self, now: Instant) -> Option<Event> {
        self.queue.pop_due(now).map(|expired| expired.value)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub(crate) enum TimerError {
    #[error("the relative timer deadline cannot be represented")]
    DeadlineOverflow,
    #[error("the actor timer queue exhausted its generation domain")]
    GenerationExhausted,
    #[error("the actor timer queue exhausted its insertion sequence")]
    SequenceExhausted,
}

/// The exact timer queue owned by one actor's capability interpreter.
pub(crate) struct LocalTimers<Event> {
    queue: TimerQueue<Instant, TimerKey, Event>,
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use behavior::InjectEvent;
    use behavior_actors::{TimerGeneration, TimerId};

    use super::*;

    #[derive(Debug, PartialEq, Eq)]
    enum Event {
        Elapsed(TimerElapsed),
        Owned(OwnedTimerElapsed),
        Reminder(TimerElapsed),
        Warning(TimerElapsed),
    }

    struct ReminderPort;
    struct WarningPort;

    impl InjectEvent<TimerElapsed, ReminderPort> for Event {
        fn inject_at(event: TimerElapsed) -> Self {
            Self::Reminder(event)
        }
    }

    impl InjectEvent<TimerElapsed, WarningPort> for Event {
        fn inject_at(event: TimerElapsed) -> Self {
            Self::Warning(event)
        }
    }

    #[test]
    fn equal_named_timers_replace_across_distinct_custom_event_ports() {
        let mut timers = LocalTimers::<Event>::new();
        let at = Instant::now();
        let id = TimerId(23);
        timers
            .schedule_at::<ReminderPort>(ScheduleAt::new(id, TimerGeneration(11), at))
            .expect("the original named reminder is accepted");
        timers
            .schedule_at::<WarningPort>(ScheduleAt::new(id, TimerGeneration(12), at))
            .expect("the same actor-local name replaces across genuine custom ports");
        let replaced = timers.pop_due(at);
        assert_eq!(
            replaced,
            Some(Event::Warning(TimerElapsed::new(id, TimerGeneration(12))))
        );
        let exhausted = timers.pop_due(at);
        assert_eq!(exhausted, None);
    }

    impl InjectEvent<TimerElapsed, behavior::Here> for Event {
        fn inject_at(event: TimerElapsed) -> Self {
            Self::Elapsed(event)
        }
    }

    impl InjectEvent<OwnedTimerElapsed, Here> for Event {
        fn inject_at(event: OwnedTimerElapsed) -> Self {
            Self::Owned(event)
        }
    }

    #[test]
    fn relative_named_timers_replace_across_custom_destination_ports() {
        let mut timers = LocalTimers::<Event>::new();
        let id = TimerId(29);
        <TimerId as TimerDestination<ReminderPort>>::schedule_after(
            &mut timers,
            ScheduleAfter::new(id, TimerGeneration(13), Duration::ZERO),
        )
        .expect("the named relative reminder is accepted through its real destination port");
        <TimerId as TimerDestination<WarningPort>>::schedule_after(
            &mut timers,
            ScheduleAfter::new(id, TimerGeneration(14), Duration::ZERO),
        )
        .expect("the exact name replaces across a different custom destination port");
        let now = Instant::now();
        let latest = timers.pop_due(now);
        let exhausted = timers.pop_due(now);
        assert_eq!(
            latest,
            Some(Event::Warning(TimerElapsed::new(id, TimerGeneration(14))))
        );
        assert_eq!(exhausted, None);
    }

    #[test]
    fn named_and_owned_timers_coexist_at_the_same_numeric_position() {
        let mut timers = LocalTimers::<Event>::new();
        let at = Instant::now();
        let named = TimerElapsed::new(TimerId(0), TimerGeneration(7));
        let owned = OwnedTimerElapsed::new(TimerGeneration(9));
        timers
            .schedule_at::<Here>(ScheduleAt::new(named.id, named.generation, at))
            .expect("the named timer is accepted");
        timers
            .schedule_owned_at::<Here>(OwnedScheduleAt::new(owned.generation, at))
            .expect("the owned timer is independent of every numeric name");

        let first = timers.pop_due(at);
        let second = timers.pop_due(at);
        let exhausted = timers.pop_due(at);
        assert_eq!(first, Some(Event::Elapsed(named)));
        assert_eq!(second, Some(Event::Owned(owned)));
        assert_eq!(exhausted, None);
    }

    #[test]
    fn rejected_owned_deadline_preserves_the_previously_accepted_generation() {
        let mut timers = LocalTimers::<Event>::new();
        let at = Instant::now();
        let original = OwnedTimerElapsed::new(TimerGeneration(1));
        timers
            .schedule_owned_at::<Here>(OwnedScheduleAt::new(original.generation, at))
            .expect("the original owned schedule is accepted");
        let rejection = timers.schedule_owned_after::<Here>(OwnedScheduleAfter::new(
            TimerGeneration(2),
            Duration::MAX,
        ));
        let retained = timers.pop_due(at);
        let exhausted = timers.pop_due(at);
        assert_eq!(rejection, Err(TimerError::DeadlineOverflow));
        assert_eq!(retained, Some(Event::Owned(original)));
        assert_eq!(exhausted, None);
    }

    #[test]
    fn replacement_delivers_only_the_latest_behavior_generation() {
        let mut timers = LocalTimers::<Event>::new();
        let id = TimerId(4);
        let now = Instant::now();
        timers
            .schedule_at::<behavior::Here>(ScheduleAt::new(
                id,
                TimerGeneration(1),
                now + Duration::from_millis(50),
            ))
            .expect("the first bounded schedule is representable");
        timers
            .schedule_at::<behavior::Here>(ScheduleAt::new(
                id,
                TimerGeneration(2),
                now + Duration::from_millis(1),
            ))
            .expect("the replacement schedule is representable");

        let replacement = timers.pop_due(now + Duration::from_millis(1));
        assert_eq!(
            replacement,
            Some(Event::Elapsed(TimerElapsed::new(id, TimerGeneration(2))))
        );
        let later = timers.pop_due(now + Duration::from_secs(1));
        assert_eq!(later, None);
    }
}
