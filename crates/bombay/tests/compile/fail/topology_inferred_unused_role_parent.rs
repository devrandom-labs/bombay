use bombay::behavior::Never;
struct Worker;
#[bombay::actor(message = Never)]
impl Worker {}
struct Root;
#[bombay::actor(message = Never, births = { worker: Worker }, creation_settlements = retain_for_retirement)]
impl Root {}
bombay::topology! {
    application {
        root: Root { Worker => worker; }
        worker for Root {}
    }
}
fn main() {}
