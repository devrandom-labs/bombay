struct Root;
#[bombay::actor(message = bombay::behavior::Never)]
impl Root {}

struct Profiles;
struct ProfilesRole;

bombay::topology! {
    application {
        root: Root {}
        families { profile: Profiles; }
        profile {}
    }
}

fn main() {
    let _application = bombay::Application::with_topology(
        bombay::actors::ActorExt::stop_on_shutdown(Root),
        application::Topology::new(),
    )
    .entity_family(
        ProfilesRole,
        Profiles,
        bombay::entity::DirectoryConfig::default(),
        bombay::entity::EntityCapacity::new(
            core::num::NonZeroUsize::new(1).unwrap(),
            core::num::NonZeroUsize::new(1).unwrap(),
        ),
    );
}
