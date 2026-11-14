use bombay::actors::ActorExt as _;
use bombay::behavior::{Actions, BehaviorActed, Never};
use bombay::prelude::StopOnShutdown;
use bombay::worker::{StartupPlan, WorkerSubmission};
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

pub(super) fn worker_submission(
    role: WorkerRole,
    activated: &mpsc::UnboundedSender<WorkerRole>,
) -> WorkerSubmission<StopOnShutdown<Worker>, impl StartupPlan<Ready = (), Rejection = Never> + use<>>
{
    let activated = activated.clone();
    WorkerSubmission::with_startup(Worker.stop_on_shutdown(), move || async move {
        activated
            .send(role)
            .expect("the application observes each worker activation");
        Ok(())
    })
}
