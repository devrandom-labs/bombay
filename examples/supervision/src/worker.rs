use bombay::actors::ActorExt as _;
use bombay::atomic::{ActivationPlan, WorkerSubmission};
use bombay::behavior::{Actions, BehaviorActed, Never};
use bombay::prelude::StopOnShutdown;
use tokio::sync::mpsc;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum WorkerRole {
    Primary,
}

pub(super) enum WorkerCommand {
    Stop,
}

pub(super) struct Worker;

#[bombay::actor]
impl Worker {
    fn receive(&mut self, message: WorkerCommand) -> BehaviorActed<Self> {
        match message {
            WorkerCommand::Stop => Ok(Actions::stop()),
        }
    }
}

type ManagedWorker = StopOnShutdown<Worker>;

pub(super) struct Startup {
    role: WorkerRole,
    activated: mpsc::UnboundedSender<WorkerRole>,
}

impl ActivationPlan for Startup {
    type Ready = ();
    type Rejection = Never;

    #[expect(
        clippy::unused_async_trait_impl,
        reason = "Defer trait-port work and owned inputs until the future is polled."
    )]
    async fn activate(self) -> Result<Self::Ready, Self::Rejection> {
        self.activated
            .send(self.role)
            .expect("the application observes each worker activation");
        Ok(())
    }
}

pub(super) fn worker_submission(
    role: WorkerRole,
    activated: &mpsc::UnboundedSender<WorkerRole>,
) -> WorkerSubmission<ManagedWorker, Startup> {
    WorkerSubmission::activated(
        Worker.stop_on_shutdown(),
        Startup {
            role,
            activated: activated.clone(),
        },
    )
}
