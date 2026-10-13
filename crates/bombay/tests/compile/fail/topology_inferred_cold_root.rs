use bombay::actors::ActorExt;
struct Root;
#[bombay::actor(message = bombay::behavior::Never)]
impl Root {}
struct Worker;
#[bombay::actor(message = bombay::behavior::Never)]
impl Worker {}
struct Background;
bombay::topology! {
    application {
        root {}
        members { Background(Background) => worker; }
        worker: Worker {}
    }
}
fn main() {}
