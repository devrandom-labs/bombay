use bombay::behavior::{CreateChild, CreationSequence, Creations, Never, NoSends, Step, Stopped};
use bombay::prelude::*;

const WORKER_NONCE: u64 = 1;

struct Worker;

#[bombay::actor(message = Never)]
impl Worker {}

struct Root;

#[bombay::actor(
    message = Never,
    births = { worker: StopOnShutdown<Worker> },
    creation_settlements = retain_for_retirement,
)]
impl Root {
    #[allow(
        clippy::unused_self,
        clippy::unnecessary_wraps,
        reason = "the generated foundational fold fixes the controlled-error boundary"
    )]
    fn init(&mut self) -> BehaviorActed<Self> {
        let mut creations = CreationSequence::new();
        let worker = creations.issue().expect("the first creation ID exists");
        Ok(Actions::new(
            NoSends,
            Creations::one(CreateChild::birth(worker, Worker.stop_on_shutdown())),
            Step::Stop(Stopped),
        ))
    }
}

#[derive(TerminalProjection)]
enum ApplicationTerminal {
    Root {
        origin: ActorOrigin<StopOnShutdown<Root>>,
        terminal: ActorRetirement<StopOnShutdown<Root>, Self>,
    },
    Worker {
        origin: ActorOrigin<Root, RootChildrenWorker>,
        terminal: ActorRetirement<StopOnShutdown<Worker>, Self>,
    },
}

#[test]
fn root_returns_only_after_owning_the_exact_direct_child_terminal() {
    let terminal: ApplicationTerminal = Application::new(Root.stop_on_shutdown())
        .run()
        .expect("the root and its declared child activate");

    let ApplicationTerminal::Root {
        origin,
        terminal:
            ActorRetirement::Completed {
                behavior: _,
                settlements,
                control,
                user,
                mut descendants,
                completion,
            },
    } = terminal
    else {
        panic!("the root must preserve its completed state and child custody")
    };
    assert_eq!(origin.address(), MailAddr::APPLICATION_ROOT);
    assert_eq!(origin.nonce(), None);
    assert!(control.is_empty());
    assert!(user.is_empty());
    assert_eq!(settlements.len(), 1);
    assert_eq!(completion, Completion::Stopped);
    assert_eq!(descendants.len(), 1);

    let ApplicationTerminal::Worker {
        origin,
        terminal:
            ActorRetirement::OwnerCancelled {
                behavior: _,
                settlements,
                control,
                user,
                descendants,
            },
    } = descendants
        .pop()
        .expect("the root owns its one child terminal")
    else {
        panic!("parent retirement must preserve the exact child cancellation custody")
    };
    assert!(control.is_empty());
    assert!(user.is_empty());
    assert_eq!(settlements.len(), 1);
    assert!(descendants.is_empty());
    assert_eq!(origin.address(), MailAddr(1));
    assert_eq!(origin.nonce(), Some(WORKER_NONCE));
}
