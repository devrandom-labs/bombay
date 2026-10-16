use bombay::entity::{EntityDefinition, EntityRef};
use bombay::{ApplicationSender, ExternalTarget};

fn try_command<D>(
    sender: ApplicationSender<EntityRef<D>>,
    command: <EntityRef<D> as ExternalTarget>::Message,
) where
    D: EntityDefinition,
    EntityRef<D>: ExternalTarget,
{
    let _admission = sender.try_send(command);
}

fn main() {}
