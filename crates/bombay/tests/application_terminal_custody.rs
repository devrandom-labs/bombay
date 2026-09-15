use bombay::behavior::{ChildChoice, Create, Never, NoSends, Step, Stopped};
use bombay::prelude::*;

const WORKER_NONCE: u64 = 7;

struct Worker;

#[bombay::actor(message = Never)]
impl Worker {}

struct Root;

#[bombay::actor(
    message = Never,
    births = { worker: StopOnShutdown<Worker> },
)]
impl Root {
    #[allow(
        clippy::unused_self,
        clippy::unnecessary_wraps,
        reason = "the generated foundational fold fixes the controlled-error boundary"
    )]
    fn init(&mut self) -> BehaviorActed<Self> {
        Ok(Actions::new(
            NoSends,
            vec![Create::birth(
                WORKER_NONCE,
                ChildChoice::Head(Worker.stop_on_shutdown()),
            )],
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

struct ApplicationRoot;

#[bombay::actor(message = Never)]
impl ApplicationRoot {
    #[allow(
        clippy::unused_self,
        clippy::unnecessary_wraps,
        reason = "the generated foundational fold fixes the controlled-error boundary"
    )]
    fn init(&mut self) -> BehaviorActed<Self> {
        Ok(Actions::stop())
    }
}

struct BackgroundWorker;

struct Auditor;

#[bombay::actor(message = Never)]
impl Auditor {}

struct AuditWorker;

#[derive(TerminalProjection)]
enum DeclaredApplicationTerminal {
    Root {
        origin: ActorOrigin<StopOnShutdown<ApplicationRoot>>,
        terminal: ActorRetirement<StopOnShutdown<ApplicationRoot>, Self>,
    },
    #[application_actor]
    BackgroundWorker {
        origin: ActorOrigin<StopOnShutdown<ApplicationRoot>, BackgroundWorker>,
        terminal: ActorRetirement<StopOnShutdown<Worker>, Self>,
    },
    #[application_actor]
    AuditWorker {
        origin: ActorOrigin<StopOnShutdown<ApplicationRoot>, AuditWorker>,
        terminal: ActorRetirement<StopOnShutdown<Auditor>, Self>,
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
    assert_eq!(completion, Completion::Stopped);
    assert_eq!(descendants.len(), 1);

    let ApplicationTerminal::Worker {
        origin,
        terminal:
            ActorRetirement::OwnerCancelled {
                behavior: _,
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
    assert!(descendants.is_empty());
    assert_eq!(origin.address(), MailAddr(1));
    assert_eq!(origin.nonce(), Some(WORKER_NONCE));
}

#[test]
fn heterogeneous_application_children_are_owned_by_their_declared_roles() {
    let terminal: DeclaredApplicationTerminal =
        Application::new(ApplicationRoot.stop_on_shutdown())
            .child(BackgroundWorker, Worker.stop_on_shutdown())
            .child(AuditWorker, Auditor.stop_on_shutdown())
            .run()
            .expect("the application root and heterogeneous declared children activate");

    let DeclaredApplicationTerminal::Root {
        origin,
        terminal:
            ActorRetirement::Completed {
                behavior: _,
                control,
                user,
                descendants,
                completion,
            },
    } = terminal
    else {
        panic!("the application root must retain its declared child")
    };
    assert_eq!(origin.address(), MailAddr::APPLICATION_ROOT);
    assert_eq!(origin.nonce(), None);
    assert!(control.is_empty());
    assert!(user.is_empty());
    assert_eq!(completion, Completion::Stopped);
    assert_eq!(descendants.len(), 2);

    let mut background = None;
    let mut audit = None;
    for descendant in descendants {
        match descendant {
            DeclaredApplicationTerminal::BackgroundWorker {
                origin,
                terminal:
                    ActorRetirement::OwnerCancelled {
                        behavior: _,
                        control,
                        user,
                        descendants,
                    },
            } => {
                assert!(control.is_empty());
                assert!(user.is_empty());
                assert!(descendants.is_empty());
                assert_eq!(background.replace(origin), None);
            }
            DeclaredApplicationTerminal::AuditWorker {
                origin,
                terminal:
                    ActorRetirement::OwnerCancelled {
                        behavior: _,
                        control,
                        user,
                        descendants,
                    },
            } => {
                assert!(control.is_empty());
                assert!(user.is_empty());
                assert!(descendants.is_empty());
                assert_eq!(audit.replace(origin), None);
            }
            _ => panic!("each application child must retain exact cancellation custody"),
        }
    }

    let background = background.expect("the root owns the background worker terminal");
    assert_eq!(background.address(), MailAddr(1));
    assert_eq!(background.nonce(), Some(0));

    let audit = audit.expect("the root owns the audit worker terminal");
    assert_eq!(audit.address(), MailAddr(2));
    assert_eq!(audit.nonce(), Some(1));
}
