use std::num::NonZeroUsize;

use bombay::entity::{
    EffectInterpreter, Entities, EntityCapacity, EntityDefinition, EntityId, EntityRef,
    EntityRuntime, EntitySlot, InstalledDispatch, InstalledSlotDecision, LifecycleEdge,
    LocalDirectory, LocalEntityRuntime, SlotEvent, TransitionEvidence,
};

fn ordinary_family<D: EntityDefinition>(entities: &Entities<D>, id: D::Id) -> EntityRef<D> {
    entities.entity(id)
}

fn advanced_runtime<I, C, R: LocalEntityRuntime<I, C>>() -> Option<EntityRuntime<I, C, R>> {
    None
}

fn advanced_directory<I, C, E, L, R: EffectInterpreter<I, C, E, L>>(
    _: &R,
) -> (Option<LocalDirectory<I, C, E, L>>, Option<InstalledSlotDecision<I, C, E, L>>, Option<InstalledDispatch<I, C, E, L>>) {
    (None, None, None)
}

fn pure_slot(event: SlotEvent<u8, u8, u8>) -> TransitionEvidence {
    EntitySlot::<u8, u8, u8>::Inactive.decide(event).evidence
}

fn claim_edge() -> LifecycleEdge {
    LifecycleEdge::ClaimActivation
}

fn main() {
    let _ = EntityId::new(7_u64);
    let one = NonZeroUsize::new(1).expect("one is nonzero");
    let _ = EntityCapacity::new(one, one);
}
