use bombay::prelude::*;

type BasicMachine = Machine<MailAddr, (), (), (), Never>;
type Basic = OneShot<BasicMachine>;

#[allow(
    clippy::trivially_copy_pass_by_ref,
    clippy::unnecessary_wraps,
    reason = "Machine transitions use the template's exact fallible borrowed-message signature"
)]
fn stay(_: (), _: &mut (), _: &()) -> Result<Move<()>, Never> {
    Ok(Move::Stay)
}

#[allow(
    clippy::unnecessary_wraps,
    reason = "OneShot reactions use the wrapped Behavior's exact fallible signature"
)]
fn stop(_: &mut BasicMachine) -> BehaviorActed<BasicMachine> {
    Ok(Actions::stop())
}

#[derive(Default)]
struct AppActors {
    basic: ActorSpace<<Basic as Behavior>::Protocol>,
}

impl Hosts<<Basic as Behavior>::Protocol> for AppActors {
    fn space(&self) -> &ActorSpace<<Basic as Behavior>::Protocol> {
        &self.basic
    }
}

fn main() -> Result<(), RunError> {
    App::new(
        OneShot::new(
            Machine::new((), (), stay),
            TimerId(1),
            std::time::Duration::from_millis(1),
            stop,
        ),
        AppActors::default(),
    )
    .run()
}
