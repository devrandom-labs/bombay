//! Actor-local interpretation and acquisition of Behavior timer facts.

use std::time::Instant;

use behavior::{InjectEvent, ScheduleAfter, ScheduleAt, TimerElapsed};
use timers::TimerQueue;

#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub(crate) enum TimerError {
    #[error("timer deadline overflowed Instant")]
    DeadlineOverflow,
}

/// One shared view of the exact actor-owned timer queue.
///
/// The Environment polls deadlines while the action interpreter schedules
/// replacements. Both uses remain serialized by the Driver; the mutex merely
/// permits those two statically separate capability views to share one queue.
pub(crate) struct LocalTimers<E> {
    queue: std::sync::Arc<std::sync::Mutex<TimerQueue<Instant, behavior::TimerId, E>>>,
    event: core::marker::PhantomData<fn() -> E>,
}

impl<E> Clone for LocalTimers<E> {
    fn clone(&self) -> Self {
        Self {
            queue: self.queue.clone(),
            event: core::marker::PhantomData,
        }
    }
}

impl<E> Default for LocalTimers<E> {
    fn default() -> Self {
        Self::new()
    }
}

impl<E> LocalTimers<E> {
    pub(crate) fn new() -> Self {
        Self {
            queue: std::sync::Arc::new(std::sync::Mutex::new(TimerQueue::new())),
            event: core::marker::PhantomData,
        }
    }

    pub(crate) fn schedule_at<Path>(&self, schedule: ScheduleAt)
    where
        E: InjectEvent<TimerElapsed, Path>,
    {
        self.queue
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .schedule(
                schedule.id,
                schedule.at,
                E::inject_at(TimerElapsed::new(schedule.id, schedule.generation)),
            );
    }

    pub(crate) fn schedule_after<Path>(&self, schedule: ScheduleAfter) -> Result<(), TimerError>
    where
        E: InjectEvent<TimerElapsed, Path>,
    {
        let at = Instant::now()
            .checked_add(schedule.after)
            .ok_or(TimerError::DeadlineOverflow)?;
        self.schedule_at::<Path>(ScheduleAt::new(schedule.id, schedule.generation, at));
        Ok(())
    }

    pub(crate) fn next_deadline(&self) -> Option<Instant> {
        self.queue
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .next_deadline()
    }

    pub(crate) fn pop_due(&self, now: Instant) -> Option<E> {
        self.queue
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .pop_due(now)
            .map(|expired| expired.value)
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use behavior::{InjectEvent, TimerGeneration, TimerId};

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

    #[tokio::test]
    async fn replacement_delivers_only_the_latest_behavior_generation() {
        let timers = LocalTimers::<Event>::new();
        let id = TimerId(4);
        let now = Instant::now();
        timers.schedule_at::<behavior::Here>(ScheduleAt::new(
            id,
            TimerGeneration(1),
            now + Duration::from_millis(50),
        ));
        timers.schedule_at::<behavior::Here>(ScheduleAt::new(
            id,
            TimerGeneration(2),
            now + Duration::from_millis(1),
        ));

        assert_eq!(
            timers.pop_due(now + Duration::from_millis(1)),
            Some(Event::Elapsed(TimerElapsed::new(id, TimerGeneration(2),)))
        );
        assert_eq!(timers.pop_due(now + Duration::from_secs(1)), None);
    }
}
