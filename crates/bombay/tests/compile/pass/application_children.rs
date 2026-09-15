use bombay::behavior::Never;
use bombay::prelude::Application;

struct Root;

#[bombay::actor(message = Never)]
impl Root {}

struct Worker;

#[bombay::actor(message = Never)]
impl Worker {}

struct Workers;

fn main() {
    let application = Application::new(Root).child(Workers, Worker);
    let _: &Root = application.root();
}
