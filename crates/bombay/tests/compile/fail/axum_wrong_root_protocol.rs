use axum::Router;
use bombay::prelude::*;

struct Root;

#[bombay::behavior::behavior(addr = MailAddr, message = Never)]
impl Root {
    fn receive(&mut self, _: MailAddr, message: Never) -> BehaviorActed<Self> {
        match message {}
    }
}

struct Other;

impl Protocol for Other {
    type Addr = MailAddr;
    type Msg = ();
}

fn wrong_router(_: ApplicationHandle<Other>) -> Router {
    Router::new()
}

fn main() {
    let _ = Application::new(Root.stop_on_shutdown()).run_axum(
        "127.0.0.1:0".parse().unwrap(),
        wrong_router,
    );
}
