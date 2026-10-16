//! Run one supervisor through worker startup, replacement and shutdown.
//! The supervisor owns recovery; Application owns execution and cleanup.

mod worker;

use bombay::actors::ActorExt as _;
use bombay::atomic::{
    ActivationPolicy, ActorDrainPolicy, CapabilityResult, DiagnosticDisposition, FailureReaction,
    FixedCommand, FixedSnapshot, OrderedRoles, Recovery, RestartLimit, RestartRelease, Strategy,
    fixed,
};
use bombay::behavior::MessageProtocol;
use bombay::prelude::MailAddr;
use bombay::{Application, WorkerFactory};
use core::time::Duration;
use tokio::sync::mpsc;
use worker::{Worker, WorkerCommand, WorkerRole, worker_submission};

type Status = MessageProtocol<MailAddr, FixedSnapshot<Worker>>;
type Capability = MessageProtocol<MailAddr, CapabilityResult<WorkerRole, Worker>>;

bombay::topology! {
    supervisor_application {
        root { Proxy => proxy; }
        proxy { Worker => worker; }
        worker {}
        hosts { status: Status; capability: Capability; }
    }
}

#[tokio::main(flavor = "current_thread")]
async fn main() {
    let (ready, mut startups) = mpsc::unbounded_channel();
    let roles = OrderedRoles::new(WorkerRole::Primary, []).expect("the worker role is unique");
    let initial_ready = ready.clone();
    let supervisor = fixed(
        move |role: &WorkerRole| worker_submission(*role, &initial_ready),
        roles,
        ActivationPolicy::new(1).expect("one worker has positive capacity"),
        Recovery::permanent(
            WorkerFactory::new(move |role| worker_submission(*role, &ready)),
            Strategy::OneForOne,
            RestartLimit::new(1, Duration::from_secs(30)),
            RestartRelease::immediate(),
        ),
        FailureReaction::StopSupervisor,
        ActorDrainPolicy::WaitForActorGraph,
        DiagnosticDisposition::terminate(),
    )
    .build()
    .unwrap_or_else(|_| panic!("the worker is prepared"));

    let outcome = Application::with_topology(
        supervisor.stop_on_shutdown(),
        supervisor_application::Topology::new(),
    )
    .run_with(move |application| async move {
        let first = startups.recv().await.expect("first worker startup");
        println!("worker ready: {first:?}");
        let target = application.root().established_recipient();
        let mut caller = application
            .external::<Capability>()
            .expect("the caller is established");
        caller
            .send(
                &target,
                FixedCommand::capability(WorkerRole::Primary, caller.recipient()),
            )
            .await
            .expect("capability query admitted");
        let reply = caller.receive().await.expect("worker capability");
        let CapabilityResult::Ready { proxy, .. } = reply.message else {
            panic!("the worker proxy is unavailable")
        };
        caller
            .send(&proxy, WorkerCommand::Stop)
            .await
            .expect("worker stop admitted");
        let replacement = startups.recv().await.expect("replacement worker startup");
        println!("replacement ready: {replacement:?}");
        caller
            .send(&target, FixedCommand::shutdown())
            .await
            .expect("supervisor shutdown admitted");
        application.retirement().await
    })
    .await
    .unwrap_or_else(|_| panic!("the supervisor application starts"));
    let (report, retirement) = outcome
        .try_into_completed()
        .unwrap_or_else(|_| panic!("the supervision work completes"));
    println!("actor retirement: {report:?}");
    // Execution already performed cleanup; these reports are optional inspection.
    println!(
        "cleanup: {:?}; notifications: {:?}",
        retirement.cleanup(),
        retirement.notifications()
    );
}
