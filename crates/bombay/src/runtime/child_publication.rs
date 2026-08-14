//! Creation and worker-terminal publication to typed child and parent lanes.

use core::convert::Infallible;

use behavior::{
    Address, CreationResolved, EventInput, ObserveCreation, ReportWorkerCreationResolved,
    ReportWorkerStopped, ServiceSends, WorkerCreationResolved, WorkerStopped,
};

use super::{IncarnationEffects, ParentReporter};
use crate::{EventSender, RouteSends};

impl<A, R, N, L, S, ParentSink>
    RouteSends<A, IncarnationEffects<R, N, L, S, ParentReporter<A, ParentSink>, A>>
    for ServiceSends<ReportWorkerStopped<A>>
where
    A: Address + Send,
    A::Nonce: Send,
    R: Send,
    N: Send,
    L: Send,
    S: Send,
    ParentSink: EventSender + Send + Sync,
    ParentSink::Event: EventInput<WorkerStopped<A>> + Send,
{
    type Error = Infallible;

    async fn route(
        self,
        _from: A,
        services: &mut IncarnationEffects<R, N, L, S, ParentReporter<A, ParentSink>, A>,
    ) -> Result<(), Self::Error> {
        for report in self {
            let event = ParentSink::Event::inject(WorkerStopped {
                proxy: services.parent.nonce,
                worker: report.worker,
                outcome: report.outcome,
                at: report.at,
            });
            let _ = services.parent.response.send(event).await;
        }
        Ok(())
    }
}

impl<A, R, L, S, P> RouteSends<A, IncarnationEffects<R, A::Nonce, L, S, P, A>>
    for ServiceSends<ObserveCreation<A::Nonce>>
where
    A: Address + Send + Sync + 'static,
    A::Nonce: Send + 'static,
    R: Send,
    L: Send,
    S: EventSender + Clone + Send + Sync + 'static,
    S::Event: EventInput<CreationResolved<A::Nonce>> + Send + 'static,
    P: Send,
{
    type Error = crate::runtime::CreationObservationError<A::Nonce>;

    async fn route(
        self,
        _from: A,
        services: &mut IncarnationEffects<R, A::Nonce, L, S, P, A>,
    ) -> Result<(), Self::Error> {
        for request in self {
            let resolved = services.take_creation(request.nonce)?;
            let _ = services.response.send(S::Event::inject(resolved)).await;
        }
        Ok(())
    }
}

impl<A, R, N, L, S, ParentSink>
    RouteSends<A, IncarnationEffects<R, N, L, S, ParentReporter<A, ParentSink>, A>>
    for ServiceSends<ReportWorkerCreationResolved<A::Nonce>>
where
    A: Address + Send,
    A::Nonce: Send,
    R: Send,
    N: Send,
    L: Send,
    S: Send,
    ParentSink: EventSender + Send + Sync,
    ParentSink::Event: EventInput<WorkerCreationResolved<A::Nonce>> + Send,
{
    type Error = Infallible;

    async fn route(
        self,
        _from: A,
        services: &mut IncarnationEffects<R, N, L, S, ParentReporter<A, ParentSink>, A>,
    ) -> Result<(), Self::Error> {
        for report in self {
            let event = ParentSink::Event::inject(WorkerCreationResolved {
                proxy: services.parent.nonce,
                worker: report.worker,
                kind: report.kind,
                result: report.result,
            });
            let _ = services.parent.response.send(event).await;
        }
        Ok(())
    }
}
