use bombay::prelude::*;
use bombay::behavior::Behavior;

struct Subject;

fn require_behavior<B: Behavior>(_: B) {}

#[bombay::behavior::behavior(addr = MailAddr, message = Never)]
impl Subject {
    #[allow(
        clippy::unused_self,
        reason = "the owning Behavior fold requires the impossible receiver"
    )]
    fn receive(&mut self, _: MailAddr, message: Never) -> BehaviorActed<Self> {
        match message {}
    }
}

fn main() {
    let active = Subject.initialize().unwrap().behavior;
    require_behavior(active);
}
