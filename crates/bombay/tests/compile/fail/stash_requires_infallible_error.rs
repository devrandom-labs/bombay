struct Documents;

#[bombay::actor(error = Box<[u8]>)]
impl Documents {
    fn receive(&mut self, document: Box<[u8]>) -> bombay::behavior::BehaviorActed<Self> {
        Err(document)
    }
}

fn main() {
    let refused = bombay::behavior::Recipient::<
        bombay::behavior::MessageProtocol<
            bombay::MailAddr,
            bombay::behavior::User<bombay::MailAddr, Box<[u8]>>,
        >,
    >::global(bombay::MailAddr::APPLICATION_ROOT);
    let _ = bombay::actors::ActorExt::with_stash(Documents, 2, refused, |_, _| {
        bombay::composition::StashRoute::Stash
    });
}
