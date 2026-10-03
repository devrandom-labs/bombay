use bombay::ProjectTerminal;
use bombay::actors::ActorExt;
use bombay::behavior::{Actions, BehaviorActed, ChildRole, Never};
use bombay::prelude::{
    ActorRetirement, Application, ChildOrigin, Completion, MailAddr, RootOrigin, RunError,
    StopOnShutdown, TerminalProjection,
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

type RootLocation = RootOrigin<StopOnShutdown<Root>>;
type RootDeparture = ActorRetirement<StopOnShutdown<Root>, ApplicationTerminal>;

#[derive(TerminalProjection)]
enum ApplicationTerminal {
    Root {
        origin: RootLocation,
        terminal: RootDeparture,
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
    creation_settlements = retain_for_retirement,
)]
impl Parent {}

type PrimaryOrigin = ChildOrigin<Parent, ParentChildrenPrimary>;
type WorkerRetirement = ActorRetirement<Worker, RoleTerminal>;

#[allow(dead_code)]
#[derive(TerminalProjection)]
enum RoleTerminal {
    #[declared_child(Parent, ParentChildrenPrimary, Worker)]
    Primary {
        origin: PrimaryOrigin,
        terminal: WorkerRetirement,
    },
    #[declared_child(Parent, ParentChildrenReplica, Worker)]
    Replica {
        origin: ChildOrigin<Parent, ParentChildrenReplica>,
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
        ChildOrigin<Parent, PrimaryPosition>,
        ActorRetirement<Worker, RoleTerminal>,
        RoleTerminal,
    >();
    accepts_projection::<
        ChildOrigin<Parent, ReplicaPosition>,
        ActorRetirement<Worker, RoleTerminal>,
        RoleTerminal,
    >();
}

#[test]
fn derive_preserves_the_exact_runtime_origin_and_retirement() {
    let terminal: ApplicationTerminal = match Application::new(Root.stop_on_shutdown()).run() {
        Err(RunError::Unpublished(terminal)) => terminal,
        _ => panic!("the initialization stop must retain an unpublished terminal"),
    };
    let ApplicationTerminal::Root {
        origin,
        terminal:
            ActorRetirement::Completed {
                capability_failures,
                unread_owner_cancellation,
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
    assert!(capability_failures.is_empty());
    assert!(unread_owner_cancellation.is_none());

    assert_eq!(origin.address(), MailAddr::APPLICATION_ROOT);
    assert_eq!(control.len(), 0);
    assert_eq!(user.len(), 0);
    assert!(descendants.is_empty());
    assert_eq!(completion, Completion::Stopped);
}

#[test]
fn projection_shape_is_compile_checked() {
    let cases = trybuild::TestCases::new();
    cases.compile_fail("tests/compile/fail/terminal_projection_incomplete_variant.rs");
    cases.compile_fail("tests/compile/fail/terminal_projection_duplicate_pair.rs");
    cases.compile_fail("tests/compile/fail/terminal_projection_wrong_role.rs");
    cases.compile_fail("tests/compile/fail/terminal_projection_wrong_actor.rs");
    cases.compile_fail("tests/compile/fail/application_actor_projection_requires_attribute.rs");
    cases.compile_fail("tests/compile/fail/terminal_origin_cannot_change_role.rs");
    cases.compile_fail("tests/compile/fail/child_origin_cannot_be_root.rs");
    cases.compile_fail("tests/compile/fail/root_origin_cannot_be_child.rs");
}
