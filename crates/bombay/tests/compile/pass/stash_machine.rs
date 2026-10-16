#[derive(Clone, Copy, PartialEq, Eq)]
enum Door {
    Closed,
    Open,
}

#[derive(Clone)]
enum Parcel {
    Arrived(u64),
    Released,
}

fn receive(
    door: Door,
    entries: &mut Vec<u64>,
    parcel: &Parcel,
) -> Result<bombay::prelude::Move<Door>, bombay::behavior::Never> {
    Ok(match (door, parcel) {
        (Door::Closed, Parcel::Arrived(_)) => bombay::prelude::Move::Defer,
        (Door::Open, Parcel::Arrived(value)) => {
            entries.push(*value);
            bombay::prelude::Move::Stay
        }
        (_, Parcel::Released) => bombay::prelude::Move::Goto(Door::Open),
    })
}

fn main() {
    let machine = bombay::prelude::Machine::new(Vec::new(), Door::Closed, receive);
    let stash = bombay::actors::ActorExt::with_stash(machine, |machine, parcel| match parcel {
        Parcel::Arrived(_) if machine.phase() == Door::Closed => {
            bombay::composition::StashRoute::Stash
        }
        Parcel::Arrived(_) => bombay::composition::StashRoute::Deliver,
        Parcel::Released => bombay::composition::StashRoute::Release,
    });
    let _application = bombay::Application::new(bombay::actors::ActorExt::stop_on_shutdown(stash));
}
