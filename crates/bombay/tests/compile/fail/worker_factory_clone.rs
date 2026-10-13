struct Worker { count: u64 }
#[bombay::actor]
impl Worker {
    fn receive(&mut self, increment: u8) -> bombay::behavior::BehaviorActed<Self> {
        self.count = self.count.saturating_add(u64::from(increment));
        Ok(bombay::behavior::Actions::cont())
    }
}
fn worker(role: &u8) -> bombay::atomic::WorkerSubmission<Worker, bombay::atomic::ImmediateActivation> {
    bombay::atomic::WorkerSubmission::immediate(Worker { count: u64::from(*role) })
}

fn main() { let source = bombay::WorkerFactory::new(worker); let _duplicate = source.clone(); }
