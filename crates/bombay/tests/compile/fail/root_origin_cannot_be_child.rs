use bombay::{ChildOrigin, RootOrigin};

struct Parent;
struct Role;

fn project_child(root: RootOrigin<Parent>) -> ChildOrigin<Parent, Role> {
    root.into_declared_child::<Role>()
}

fn main() {}
