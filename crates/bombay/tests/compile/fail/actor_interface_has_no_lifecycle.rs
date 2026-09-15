use bombay::ActorInterface;

fn request_shutdown(interface: ActorInterface<()>) {
    interface.request_shutdown();
}

fn main() {}
