//! Creation and worker-terminal publication to typed child and parent lanes.

use core::convert::Infallible;

use behavior::{
    Address, CreationEvent, ObserveCreation, ReportWorkerCreationResolved, ReportWorkerStopped,
    ServiceSends, WorkerCreationEvent, WorkerCreationResolved, WorkerEvent, WorkerStopped,
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
    ParentSink::Event: WorkerEvent<Addr = A> + Send,
{
    type Error = Infallible;

    async fn route(
        self,
        _from: A,
        services: &mut IncarnationEffects<R, N, L, S, ParentReporter<A, ParentSink>, A>,
    ) -> Result<(), Self::Error> {
        for report in self {
            if let Some(event) = <ParentSink::Event as WorkerEvent>::worker_stopped(WorkerStopped {
                proxy: services.parent.nonce,
                worker: report.worker,
                outcome: report.outcome,
                at: report.at,
            }) {
                let _ = services.parent.response.send(event).await;
            }
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
    S::Event: CreationEvent<Addr = A> + Send + 'static,
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
            if let Some(event) = <S::Event as CreationEvent>::creation_resolved(resolved) {
                let _ = services.response.send(event).await;
            }
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
    ParentSink::Event: WorkerCreationEvent<Addr = A> + Send,
{
    type Error = Infallible;

    async fn route(
        self,
        _from: A,
        services: &mut IncarnationEffects<R, N, L, S, ParentReporter<A, ParentSink>, A>,
    ) -> Result<(), Self::Error> {
        for report in self {
            if let Some(event) =
                <ParentSink::Event as WorkerCreationEvent>::worker_creation_resolved(
                    WorkerCreationResolved {
                        proxy: services.parent.nonce,
                        worker: report.worker,
                        kind: report.kind,
                        result: report.result,
                    },
                )
            {
                let _ = services.parent.response.send(event).await;
            }
        }
        Ok(())
    }
}
