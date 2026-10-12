struct Root;
#[bombay::actor(message = bombay::behavior::Never)]
impl Root {}
bombay::topology! {
    application {
        root: Root {}
        worker {}
    }
}

fn main() {}
