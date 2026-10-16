fn retire(interface: bombay::ActorInterface<()>) {
    let _retirement = interface.retire_entity((), &7_u64);
}

fn main() {}
