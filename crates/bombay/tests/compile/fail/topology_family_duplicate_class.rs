struct Root;
#[bombay::actor(message = bombay::behavior::Never)]
impl Root {}

struct Profiles;

bombay::topology! {
    application {
        root: Root {}
        families { profile: Profiles; profile: Profiles; }
        profile {}
    }
}

fn main() {}
