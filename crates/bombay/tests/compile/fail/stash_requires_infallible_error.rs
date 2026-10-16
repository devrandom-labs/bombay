struct Documents;

#[bombay::actor(error = Box<[u8]>)]
impl Documents {
    fn receive(&mut self, document: Box<[u8]>) -> bombay::behavior::BehaviorActed<Self> {
        Err(document)
    }
}

fn main() {
    let _ = bombay::actors::ActorExt::with_stash(Documents, |_, _| {
        bombay::composition::StashRoute::Stash
    });
}
