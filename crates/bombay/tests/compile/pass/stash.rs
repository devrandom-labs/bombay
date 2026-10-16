struct Documents(Vec<Box<[u8]>>);

#[bombay::actor]
impl Documents {
    fn receive(&mut self, document: Box<[u8]>) -> bombay::behavior::BehaviorActed<Self> {
        self.0.push(document);
        Ok(bombay::behavior::Actions::cont())
    }
}

struct Clock;

#[bombay::actor(message = bombay::behavior::Never)]
impl Clock {}

fn main() {
    let scope = bombay::ApplicationScope::new();
    let refused = scope
        .external::<bombay::behavior::MessageProtocol<
            bombay::MailAddr,
            bombay::behavior::User<bombay::MailAddr, Box<[u8]>>,
        >>()
        .expect("the exact refusal recipient exists");
    let root = bombay::actors::ActorExt::with_stash(
        Documents(Vec::new()),
        2,
        refused.recipient(),
        |_, _| bombay::composition::StashRoute::Stash,
    );
    let root = bombay::actors::ActorExt::stop_on_shutdown(root);
    let _documents = scope.with_root(root).run();

    // No user input can enter this advanced composition. It needs no refusal
    // recipient, logical protocol host, cloned message or structural alias.
    let clock = bombay::composition::UnlimitedStash::new(Clock, |_, message| match *message {});
    let clock =
        bombay::actors::ActorExt::with_one_shot(clock, core::time::Duration::from_secs(1), |_| {
            bombay::behavior::Actions::stop()
        });
    let _clock = bombay::Application::new(clock).run();
}
