//! Interpretation of Behavior timer requests through Bombay Timers and Tokio.

use core::convert::Infallible;

use behavior::{Address, ScheduleAfter, ScheduleAt, ServiceSends, TimerElapsed};
use tokio::time::Instant;

use super::IncarnationEffects;
use crate::RouteSends;

/// A relative timer request exceeded Tokio's representable instant range.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
#[error("a relative timer request exceeded Tokio's representable instant range")]
pub struct ScheduleAfterError;

impl<A: Address + Send, R: Send, N: Send, L: Send, S: Send, P: Send, PA: Send>
    RouteSends<A, IncarnationEffects<R, N, L, S, P, PA>> for ServiceSends<ScheduleAt>
{
    type Error = Infallible;

    async fn route(
        self,
        _from: A,
        services: &mut IncarnationEffects<R, N, L, S, P, PA>,
    ) -> Result<(), Self::Error> {
        for schedule in self {
            services.timers.schedule(
                schedule.id,
                schedule.at,
                TimerElapsed {
                    id: schedule.id,
                    generation: schedule.generation,
                },
            );
        }
        Ok(())
    }
}

impl<A: Address + Send, R: Send, N: Send, L: Send, S: Send, P: Send, PA: Send>
    RouteSends<A, IncarnationEffects<R, N, L, S, P, PA>> for ServiceSends<ScheduleAfter>
{
    type Error = ScheduleAfterError;

    async fn route(
        self,
        _from: A,
        services: &mut IncarnationEffects<R, N, L, S, P, PA>,
    ) -> Result<(), Self::Error> {
        for schedule in self {
            let deadline = Instant::now()
                .checked_add(schedule.after)
                .ok_or(ScheduleAfterError)?;
            services.timers.schedule(
                schedule.id,
                deadline,
                TimerElapsed {
                    id: schedule.id,
                    generation: schedule.generation,
                },
            );
        }
        Ok(())
    }
}
