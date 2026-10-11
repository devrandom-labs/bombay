use bombay::behavior::Behavior;
use bombay::prelude::StopOnShutdown;
use bombay::ApplicationLifecycle;

#[path = "../../../../../examples/counter/src/counter.rs"]
mod counter;

fn request(
    lifecycle: ApplicationLifecycle<
        counter::Counter,
        <StopOnShutdown<StopOnShutdown<counter::Counter>> as Behavior>::Event,
    >,
) {
    lifecycle.request_shutdown().expect("a shutdown lane must be selected");
}

fn main() {}
