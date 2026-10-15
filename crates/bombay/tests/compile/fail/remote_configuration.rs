use bombay::Application;

fn main() {
    let application = Application::with_topology((), ());
    let native_configuration = Default::default();
    let opening = application.open_node(native_configuration);
    drop(opening);
}
