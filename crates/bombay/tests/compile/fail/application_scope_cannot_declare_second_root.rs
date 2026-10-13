struct Service;

#[bombay::actor(message = bombay::behavior::Never)]
impl Service {}

fn main() {
    let scope = bombay::ApplicationScope::new();
    let _first = scope.with_root(Service);
    let _second = scope.with_root(Service);
}
