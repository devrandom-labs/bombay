use bombay::behavior::{
    ActionItem, Actions, BehaviorActed, InterpretItem, InterpreterRequests, Never,
};
use bombay::Application;
use core::future::Future;
use std::path::PathBuf;

#[path = "../../application_services/requests.rs"]
mod requests;
use requests::{FileAccess, OpenFile, Services};

impl bombay::ExternalRequest for OpenFile {
    type Interpreter = ();
}

impl<Event, Path> InterpretItem<OpenFile, Event, Path> for () {
    #[expect(
        clippy::manual_async_fn,
        reason = "the receiving loan captures no event or route lifetime"
    )]
    fn interpret_item<'a>(
        &'a mut self,
        input: <OpenFile as ActionItem>::Input<'a>,
        received: &'a mut Option<<OpenFile as ActionItem>::Reply>,
    ) -> impl Future<Output = ()> + Send + 'a
    where
        OpenFile: 'a,
    {
        async move {
            let mut files = Services;
            <Services as InterpretItem<OpenFile, Event, Path>>::interpret_item(
                &mut files, input, received,
            )
            .await;
        }
    }
}

struct Document {
    files: Vec<OpenFile>,
    original: Box<[u8; 2]>,
}

#[bombay::actor(message = Never, sends = { files: InterpreterRequests<OpenFile> })]
#[allow(
    clippy::unnecessary_wraps,
    reason = "the actor returns its complete typed Actions through the standard error carrier"
)]
impl Document {
    fn init(&mut self) -> BehaviorActed<Self> {
        let mut actions = Actions::stop();
        for file in self.files.drain(..) {
            actions = actions.with_send(file);
        }
        Ok(actions)
    }
}


fn main() {
    let host = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("the caller owns its host");
    let application = Application::new(Document {
        files: vec![OpenFile {
            path: PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("Cargo.toml"),
            access: FileAccess::Ready,
        }],
        original: Box::new([103, 107]),
    }).with_interpreter(|| Services);
    let outcome = host.block_on(application.run());
    drop(outcome);
}
