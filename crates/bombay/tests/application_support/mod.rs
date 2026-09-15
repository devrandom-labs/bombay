use core::fmt;

use bombay::behavior::{Behavior, Never, Protocol};
use bombay::prelude::{ActorOrigin, ActorRetirement, Completion, MailAddr, TerminalProjection};

#[derive(TerminalProjection)]
pub(crate) enum RootTerminal<R>
where
    R: Behavior<Protocol: Protocol<Addr = MailAddr>, Ph = Never>,
{
    Root {
        origin: ActorOrigin<R>,
        terminal: ActorRetirement<R, Self>,
    },
}

impl<R> fmt::Debug for RootTerminal<R>
where
    R: Behavior<Protocol: Protocol<Addr = MailAddr>, Ph = Never>,
{
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("RootTerminal")
    }
}

pub(crate) fn into_root<R>(
    terminal: RootTerminal<R>,
) -> (ActorOrigin<R>, ActorRetirement<R, RootTerminal<R>>)
where
    R: Behavior<Protocol: Protocol<Addr = MailAddr>, Ph = Never>,
{
    let RootTerminal::Root { origin, terminal } = terminal;
    (origin, terminal)
}

pub(crate) fn assert_completed<R>(terminal: RootTerminal<R>)
where
    R: Behavior<Protocol: Protocol<Addr = MailAddr>, Ph = Never>,
{
    let (
        origin,
        ActorRetirement::Completed {
            behavior,
            control,
            user,
            descendants,
            completion,
        },
    ) = into_root(terminal)
    else {
        panic!("the application root must retain its completed terminal state")
    };
    assert_eq!(origin.address(), MailAddr::APPLICATION_ROOT);
    assert_eq!(origin.nonce(), None);
    drop(behavior);
    assert!(control.is_empty());
    assert!(user.is_empty());
    assert!(descendants.is_empty());
    assert_eq!(completion, Completion::Stopped);
}
