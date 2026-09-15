use bombay::prelude::*;

struct Invalid;

#[bombay::behavior::behavior(addr = MailAddr, message = u8)]
impl Invalid {
    fn receive(&mut self, _: MailAddr, _: u16) -> BehaviorActed<Self> {
        Ok(Actions::cont())
    }
}

fn main() {}
