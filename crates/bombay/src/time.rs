//! Actor-local interpretation and acquisition of timer facts.

use std::time::Instant;

use behavior::InjectEvent;
use behavior_actors::{ScheduleAfter, ScheduleAt, TimerElapsed, TimerId};
use timers::{ScheduleError, TimerQueue};

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
    queue: TimerQueue<Instant, TimerId, Event>,
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
                schedule.id,
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

    pub(crate) fn next_deadline(&mut self) -> Option<Instant> {
        self.queue.next_deadline()
    }

    pub(crate) fn pop_due(&mut self, now: Instant) -> Option<Event> {
        self.queue.pop_due(now).map(|expired| expired.value)
    }
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
    }

    impl InjectEvent<TimerElapsed, behavior::Here> for Event {
        fn inject_at(event: TimerElapsed) -> Self {
            Self::Elapsed(event)
        }
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
