use bombay::prelude::*;
use bombay::App;

struct Root;

#[bombay::behavior::behavior(addr = MailAddr, message = Never)]
impl Root {
    fn receive(&mut self, _: MailAddr, message: Never) -> BehaviorActed<Self> {
        match message {}
    }
}

fn main() {
    let _ = App::local(Root);
}
