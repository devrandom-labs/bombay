struct Documents(Vec<Box<[u8]>>);

#[bombay::actor]
impl Documents {
    fn receive(&mut self, document: Box<[u8]>) -> bombay::behavior::BehaviorActed<Self> {
        self.0.push(document);
        Ok(bombay::behavior::Actions::cont())
    }
}

fn main() {
    let refused = bombay::behavior::Recipient::<
        bombay::behavior::MessageProtocol<bombay::MailAddr, Box<[u8]>>,
    >::global(bombay::MailAddr::APPLICATION_ROOT);
    let _ = bombay::actors::ActorExt::with_stash(Documents(Vec::new()), 2, refused, |_, _| {
        bombay::composition::StashRoute::Stash
    });
}
