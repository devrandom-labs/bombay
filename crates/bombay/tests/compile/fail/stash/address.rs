#[derive(Clone, Copy, PartialEq, Eq)]
struct Office(u64);

impl bombay::behavior::Address for Office {
    type Nonce = u64;
}

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
        bombay::behavior::MessageProtocol<
            Office,
            bombay::behavior::User<bombay::MailAddr, Box<[u8]>>,
        >,
    >::global(Office(7));
    let _ = bombay::actors::ActorExt::with_stash(Documents(Vec::new()), 2, refused, |_, _| {
        bombay::composition::StashRoute::Stash
    });
}
