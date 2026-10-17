//! Select a standalone actor template and compose its timer and shutdown policies.

use core::time::Duration;

use bombay::prelude::*;
use processor::{ProcessorMessage, ProcessorPhase, ProcessorState, transition};

mod processor;

#[tokio::main(flavor = "current_thread")]
async fn main() {
    let actor = Machine::new(ProcessorState::new(), ProcessorPhase::Closed, transition)
        .with_receive_timeout(Duration::from_millis(50), |processor| {
            if processor.state().completed(7) {
                Actions::stop()
            } else {
                Actions::cont()
            }
        })
        .stop_on_shutdown();
    let outcome = Application::new(actor)
        .run_with(|application| async move {
            let sender = application.sender().expect("the sender is established");
            sender
                .send(ProcessorMessage::Work(7))
                .await
                .expect("deferred work admitted");
            sender
                .send(ProcessorMessage::Open)
                .await
                .expect("open admitted");
            application.retirement().await
        })
        .await
        .unwrap_or_else(|_| panic!("the processor application starts"));
    let (report, retirement) = outcome
        .try_into_completed()
        .unwrap_or_else(|_| panic!("the processor work completes"));
    println!("receive timeout: {report:?}");
    println!("cleanup: {:?}", retirement.cleanup());

    let outcome = Application::new(
        Machine::new(ProcessorState::new(), ProcessorPhase::Closed, transition).stop_on_shutdown(),
    )
    .run_with(|application| async move {
        application.shutdown().expect("shutdown admitted");
        application.retirement().await
    })
    .await
    .unwrap_or_else(|_| panic!("the processor application starts"));
    let (report, retirement) = outcome
        .try_into_completed()
        .unwrap_or_else(|_| panic!("the processor work completes"));
    println!("shutdown: {report:?}");
    println!("cleanup: {:?}", retirement.cleanup());
}
