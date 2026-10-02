use core::future::{Ready, ready};

use bombay::behavior::Behavior;
use bombay::prelude::*;

type RootEvent = <StopOnShutdown<Root> as Behavior>::Event;

struct Root;

#[bombay::behavior::behavior(addr = MailAddr, message = Never)]
impl Root {
    fn receive(&mut self, _: MailAddr, message: Never) -> BehaviorActed<Self> {
        match message {}
    }
}

struct Other;

impl Protocol for Other {
    type Addr = MailAddr;
    type Msg = ();
}

fn wrong_boundary(_: ApplicationHandle<Other, RootEvent>) -> Ready<()> {
    ready(())
}

fn main() {
    let _ = Application::new(Root.stop_on_shutdown()).run_with(wrong_boundary);
}
