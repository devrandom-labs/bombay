use bombay::ActorInterface;

fn shutdown(interface: ActorInterface<()>) {
    interface.shutdown();
}

fn main() {}
