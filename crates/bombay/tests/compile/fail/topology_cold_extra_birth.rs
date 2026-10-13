use bombay::actors::ActorExt;
use bombay::behavior::Never;

struct Root;
#[bombay::actor(message = Never)]
impl Root {}
struct ExtraWorker;
#[bombay::actor(message = Never)]
impl ExtraWorker {}
struct Worker;
#[bombay::actor(message = Never, births = { extra: ExtraWorker }, creation_settlements = retain_for_retirement)]
impl Worker {}
struct Primary;
struct ForeignRole;
bombay::topology! {
    services { root: Root {} members { Primary(Primary) => worker; } worker {} }
}
#[tokio::main(flavor = "current_thread")]
async fn main() {
    let outcome = bombay::Application::with_topology(Root.stop_on_shutdown(), services::Topology::new())
        .child(Primary, Worker.stop_on_shutdown())
        .run_with(|application| async move { application.request_shutdown().expect("admitted"); })
        .await;
    drop(outcome);
}
