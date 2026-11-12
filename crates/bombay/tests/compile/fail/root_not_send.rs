use bombay::behavior::{Actions, BehaviorActed};
use bombay::{Application, actor};
use core::future::ready;
use std::rc::Rc;

struct Counter {
    value: Rc<u8>,
}

#[actor]
impl Counter {
    fn receive(&mut self, value: u8) -> BehaviorActed<Self> {
        self.value = Rc::new(value);
        Ok(Actions::cont())
    }
}

fn main() {
    let application = Application::new(Counter { value: Rc::new(3) });
    let execution = application.execute_with(|_| ready(()));
    drop(execution);
}
