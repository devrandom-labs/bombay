//! Model-based differential fuzz of the `Registry` state machine.
//!
//! Drives `register` / `lookup` / `unregister` / drop (of an actor's handles)
//! against an oracle model and asserts the registry's two load-bearing
//! invariants (documented on `registry.rs`):
//!
//! * **register-once**: a name held by a *live* actor rejects re-registration
//!   with `NameTaken`, and the incumbent is untouched; a dead incumbent
//!   (channel closed) is reclaimed atomically.
//! * **dead reads absent**: a dropped actor's name resolves to `None` on every
//!   path — never a stale ref, never a type error.
//!
//! Sync (no threads) so the MIRI lane can run it, like the mailbox target. The
//! model tracks per-name the claimed `(id, incarnation)` and per-id liveness,
//! so a re-created actor (a dropped id registered again) is a *new*
//! incarnation the prior claim cannot see — exactly the weak-no-pin semantics
//! the registry is built on.

use std::collections::HashMap;

use bolero::{TypeGenerator, check};
use bombay_core::actor::{Actor, ActorRef};
use bombay_core::error::NameTaken;
use bombay_core::mailbox::{Capacity, Mailbox, Mailboxed, Signal};
use bombay_core::message::Msg;
use bombay_core::registry::Registry;
use bombay_core::test_support::unstarted_actor;

struct Probe;
#[derive(Debug, Clone, Copy, PartialEq)]
struct ProbeMsg(u64);
impl Msg for ProbeMsg {}
impl Mailboxed for Probe {
    type Msg = ProbeMsg;
}
impl Actor for Probe {
    type Args = ();
    type Error = core::convert::Infallible;
    async fn on_start(_: (), _: ActorRef<Self>) -> Result<Self, Self::Error> {
        Ok(Probe)
    }
    async fn handle(
        &mut self,
        _: ProbeMsg,
        _: ActorRef<Self>,
        _: &mut bool,
    ) -> Result<(), Self::Error> {
        Ok(())
    }
}

/// Per-id liveness + incarnation. Each (re)creation of an id bumps `incarn`; a
/// claim records the incarnation it was made against, so a later recreation
/// invalidates that claim (the registry still holds the old, dead weak handle).
struct ActorState {
    incarn: u64,
    live: bool,
    handle: Option<(ActorRef<Probe>, bombay_core::mailbox::MailboxReceiver<Probe>)>,
}

/// Returns a live `ActorRef` for `id`, creating (or recreating) one as needed.
fn live_ref(actors: &mut HashMap<u64, ActorState>, id: u64) -> ActorRef<Probe> {
    let state = actors.entry(id).or_insert_with(|| {
        let (r, rx) = new_actor();
        ActorState {
            incarn: 0,
            live: true,
            handle: Some((r.clone(), rx)),
        }
    });
    if !state.live {
        let (r, rx) = new_actor();
        state.incarn += 1;
        state.live = true;
        state.handle = Some((r.clone(), rx));
    }
    state.handle.as_ref().expect("live after (re)create").0.clone()
}

/// Drops the live handle for `id`, if any — the actor's mailbox channel closes.
fn drop_actor(actors: &mut HashMap<u64, ActorState>, id: u64) {
    if let Some(state) = actors.get_mut(&id) {
        if state.live {
            state.handle = None;
            state.live = false;
        }
    }
}

/// Whether the `(id, incarnation)` claim still names a live incarnation.
fn claim_is_live(actors: &HashMap<u64, ActorState>, id: u64, incarnation: u64) -> bool {
    actors
        .get(&id)
        .is_some_and(|s| s.live && s.incarn == incarnation)
}

fn new_actor() -> (ActorRef<Probe>, bombay_core::mailbox::MailboxReceiver<Probe>) {
    let cap = Capacity::try_from(4usize).expect("valid capacity");
    let (tx, rx) = Mailbox::<Probe>::bounded(cap);
    unstarted_actor::<Probe>((tx, rx))
}

#[derive(Debug, TypeGenerator)]
enum Op {
    Register { name: u8, id: u64 },
    Lookup { name: u8 },
    Unregister { name: u8 },
    DropActor { id: u64 },
}

#[test]
fn registry_state_machine() {
    check!()
        .with_type::<Vec<Op>>()
        .for_each(|ops| {
            let registry = Registry::new();
            let mut actors: HashMap<u64, ActorState> = HashMap::new();
            // name -> (id, incarnation) of the actor currently registered.
            let mut claims: HashMap<u8, (u64, u64)> = HashMap::new();

            for op in ops {
                match *op {
                    Op::Register { name, id } => {
                        let name_s = name.to_string();
                        let ref_ = live_ref(&mut actors, id);
                        let got = registry.register(name_s, &ref_);

                        match claims.get(&name).copied() {
                            Some((cid, cgen)) if claim_is_live(&actors, cid, cgen) => {
                                assert_eq!(
                                    got,
                                    Err(NameTaken),
                                    "a live incumbent blocks re-registration"
                                );
                                // incumbent untouched: claims unchanged.
                            }
                            Some(_) => {
                                // dead incumbent: reclaimed atomically.
                                assert!(
                                    got.is_ok(),
                                    "a dead incumbent's name is reclaimable"
                                );
                                claims.insert(name, (id, actors[&id].incarn));
                            }
                            None => {
                                assert!(got.is_ok(), "a free name is registrable");
                                claims.insert(name, (id, actors[&id].incarn));
                            }
                        }
                    }
                    Op::Lookup { name } => {
                        let name_s = name.to_string();
                        let got = registry
                            .lookup::<Probe>(&name_s)
                            .expect("same type — never a type conflict on lookup");

                        match claims.get(&name).copied() {
                            Some((cid, cgen)) if claim_is_live(&actors, cid, cgen) => {
                                let resolved = got
                                    .expect("a live incumbent resolves to a ref");
                                assert!(
                                    resolved.is_alive(),
                                    "lookup of a live incumbent returns a live handle"
                                );
                                // Identity: a message sent via the resolved ref must
                                // reach the registered slot's OWN receiver — never a
                                // torn or wrong channel (the registry's core invariant).
                                let m = ProbeMsg(0x9E37_79B9_2A17_3779);
                                resolved
                                    .tell(m)
                                    .try_send()
                                    .expect("a live incumbent's mailbox is open");
                                let landed = actors
                                    .get_mut(&cid)
                                    .expect("incumbent slot exists")
                                    .handle
                                    .as_mut()
                                    .expect("live slot holds a handle")
                                    .1
                                    .drain()
                                    .any(|s| matches!(s, Signal::Message { msg, .. } if msg == m));
                                assert!(
                                    landed,
                                    "lookup resolves THE registrant's channel, not a torn entry"
                                );
                            }
                            _ => {
                                assert!(
                                    got.is_none(),
                                    "a free or dead name reads as absent on every path"
                                );
                            }
                        }
                    }
                    Op::Unregister { name } => {
                        let name_s = name.to_string();
                        let removed = registry.unregister(&name_s);
                        let had = claims.remove(&name).is_some();
                        assert_eq!(
                            removed, had,
                            "unregister removes exactly the claimed entry"
                        );
                    }
                    Op::DropActor { id } => {
                        drop_actor(&mut actors, id);
                        // claims pointing at this id now read absent (oracle
                        // handles via claim_is_live); the claim entry stays so a
                        // later re-register recreates a new incarnation.
                    }
                }
            }
        });
}
