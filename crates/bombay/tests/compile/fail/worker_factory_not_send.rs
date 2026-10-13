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

fn main() {
    let role = std::rc::Rc::new(3_u8);
    let _source = bombay::WorkerFactory::new(move |_: &u8| worker(role.as_ref()));
}
