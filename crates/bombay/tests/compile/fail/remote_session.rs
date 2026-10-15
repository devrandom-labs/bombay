use bombay::Application;
use bombay::actors::ActorExt;
use bombay::behavior::Never;

struct Worker;

#[bombay::actor(message = Never)]
impl Worker {}

async fn inspect() {
    let mut node = Application::new(Worker.stop_on_shutdown())
        .open_node(Default::default())
        .await
        .unwrap_or_else(|_| panic!("compile-only input"));
    let execution = node.execute_with(|_, session| async move { session.zid() });
    drop(execution);
}

fn main() {}
