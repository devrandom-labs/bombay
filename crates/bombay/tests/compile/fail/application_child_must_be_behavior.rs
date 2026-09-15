use bombay::behavior::Never;

struct Root;

#[bombay::actor(message = Never)]
impl Root {}

struct Workers;
struct NotBehavior;

fn main() {
    let _ = bombay::Application::new(Root).child(Workers, NotBehavior);
}
