use bombay::{ChildOrigin, RootOrigin};

struct Parent;
struct Role;

fn project_root(child: ChildOrigin<Parent, Role>) -> RootOrigin<Parent> {
    child.into_declared_root()
}

fn main() {}
