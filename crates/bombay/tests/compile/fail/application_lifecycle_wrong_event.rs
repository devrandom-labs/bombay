use bombay::behavior::{Never, Protocol};
use bombay::prelude::ApplicationLifecycle;

fn cannot_change_event<P: Protocol>(lifecycle: ApplicationLifecycle<P, ()>) {
    let _other: ApplicationLifecycle<P, Never> = lifecycle;
}

fn main() {}
