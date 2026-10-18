struct Worker { count: u64 }
#[bombay::actor]
impl Worker {
    fn receive(&mut self, increment: u8) -> bombay::behavior::BehaviorActed<Self> {
        self.count = self.count.saturating_add(u64::from(increment));
        Ok(bombay::behavior::Actions::cont())
    }
}
fn worker(role: &u8) -> bombay::worker::WorkerSubmission<Worker, bombay::worker::ImmediateStartup> {
    bombay::worker::WorkerSubmission::immediate(Worker { count: u64::from(*role) })
}

async fn refuse_source() {
    let mut source = bombay::WorkerFactory::new(worker);
    let rejected = bombay::WorkerPreparationSource::prepare_first(&mut source, &3_u8).await;
    if let bombay::WorkerPreparationStart::SourceRejected(reason) = rejected {
        let _reason: String = reason;
    }
}
fn main() { let _future = refuse_source(); }
