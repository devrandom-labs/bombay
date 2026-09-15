use bombay::ProjectTerminal;
use bombay::actors::ActorExt;
use bombay::behavior::{Actions, BehaviorActed, ChildRole, Never};
use bombay::prelude::{
    ActorOrigin, ActorRetirement, Application, Completion, MailAddr, StopOnShutdown,
    TerminalProjection,
};

struct Root;

#[bombay::actor(message = Never)]
impl Root {
    #[allow(
        clippy::unnecessary_wraps,
        clippy::unused_self,
        reason = "the generated foundational fold fixes the method and error boundary"
    )]
    fn init(&mut self) -> BehaviorActed<Self> {
        Ok(Actions::stop())
    }
}

#[derive(TerminalProjection)]
enum ApplicationTerminal {
    Root {
        origin: ActorOrigin<StopOnShutdown<Root>>,
        terminal: ActorRetirement<StopOnShutdown<Root>, Self>,
    },
}

struct Worker;

#[bombay::actor(message = Never)]
impl Worker {}

struct Parent;

#[bombay::actor(
    message = Never,
    births = {
        primary: Worker,
        replica: Worker,
    },
)]
impl Parent {}

#[allow(dead_code)]
#[derive(TerminalProjection)]
enum RoleTerminal {
    Primary {
        origin: ActorOrigin<Parent, ParentChildrenPrimary>,
        terminal: ActorRetirement<Worker, Self>,
    },
    Replica {
        origin: ActorOrigin<Parent, ParentChildrenReplica>,
        terminal: ActorRetirement<Worker, Self>,
    },
}

fn accepts_projection<Origin, Terminal, Root>()
where
    Root: ProjectTerminal<Origin, Terminal>,
{
}

#[test]
fn equal_child_types_project_from_their_distinct_generated_role_positions() {
    type PrimaryPosition = <ParentChildrenPrimary as ChildRole<Parent>>::Position;
    type ReplicaPosition = <ParentChildrenReplica as ChildRole<Parent>>::Position;

    accepts_projection::<
        ActorOrigin<Parent, PrimaryPosition>,
        ActorRetirement<Worker, RoleTerminal>,
        RoleTerminal,
    >();
    accepts_projection::<
        ActorOrigin<Parent, ReplicaPosition>,
        ActorRetirement<Worker, RoleTerminal>,
        RoleTerminal,
    >();
}

#[test]
fn derive_preserves_the_exact_runtime_origin_and_retirement() {
    let terminal: ApplicationTerminal = Application::new(Root.stop_on_shutdown())
        .run()
        .expect("the stopping root activates");
    let ApplicationTerminal::Root {
        origin,
        terminal:
            ActorRetirement::Completed {
                control,
                user,
                descendants,
                completion,
                ..
            },
    } = terminal
    else {
        panic!("the derive must preserve the exact root terminal")
    };

    assert_eq!(origin.address(), MailAddr::APPLICATION_ROOT);
    assert_eq!(origin.nonce(), None);
    assert!(control.is_empty());
    assert!(user.is_empty());
    assert!(descendants.is_empty());
    assert_eq!(completion, Completion::Stopped);
}

#[test]
fn projection_shape_is_compile_checked() {
    let cases = trybuild::TestCases::new();
    cases.compile_fail("tests/compile/fail/terminal_projection_incomplete_variant.rs");
    cases.compile_fail("tests/compile/fail/terminal_projection_duplicate_pair.rs");
    cases.compile_fail("tests/compile/fail/terminal_projection_wrong_role.rs");
    cases.compile_fail("tests/compile/fail/application_actor_projection_requires_attribute.rs");
    cases.compile_fail("tests/compile/fail/terminal_origin_cannot_change_role.rs");
}
