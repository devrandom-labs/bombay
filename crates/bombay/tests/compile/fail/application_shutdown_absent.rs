use bombay::behavior::Behavior;
use bombay::ApplicationLifecycle;

#[path = "../../../../../examples/counter/src/counter.rs"]
mod counter;

fn request(lifecycle: ApplicationLifecycle<counter::Counter, <counter::Counter as Behavior>::Event>) {
    lifecycle.request_shutdown().expect("a shutdown lane must exist");
}

fn main() {}
