//! Zero-allocation guard for the `Phased` transition path (card #281,
//! plan S9 — the ADR-0024 "no allocation on the transition path"
//! constraint, re-proven on the caps surface).
//!
//! ONE test, in its OWN binary, on purpose — same rationale as
//! `alloc_exact.rs`: a `#[global_allocator]` counts every allocation in
//! its process, and only a single-test binary is process-isolated under
//! both harnesses. Don't add a second test here.
//!
//! What the spike measured as "Goto = Stay = 2 (both probe-channel;
//! transition adds 0)" is asserted here as absolute zeros — no probe
//! channel sits inside the measured windows. The deadline-arming claim is
//! the plane's improvement over the mock's measured 3: arming is
//! `entered_at + phase_deadline` arithmetic on the declarative slot — no
//! timer task, no allocation.

use std::alloc::System;

use bombay::{
    actor::{Flow, WeakActorRef},
    caps::{
        Actor, Admission, ByPhase, CapSet, Ctx, DeadlineHook, DeadlinePolicy, Disposition, NoDefer,
        PhasePolicy, PhaseView, Phased, Shell, Step,
    },
    error::Infallible,
    mailbox::Mailboxed,
    message::Msg,
    test_support::CountingAlloc,
};
use tokio::time::Instant;

use core::time::Duration;

#[global_allocator]
static COUNTER: CountingAlloc = CountingAlloc::new(System);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Phase {
    A,
    B,
    /// Carries a declared deadline — entering it ARMS the slot.
    Timed,
}

/// Non-ZST payload: a boxed message would be a real, countable alloc.
#[derive(Debug)]
struct Tick(u64);
impl Msg for Tick {}

struct P;
impl Mailboxed for P {
    type Msg = Tick;
}

struct Pol;
impl PhasePolicy for Pol {
    type Actor = P;
    type Phase = Phase;
    type Deferral = NoDefer;
    type Timeout = PolDl;
    fn initial((): &()) -> Phase {
        Phase::A
    }
    fn gate(_: Phase, _: &Tick) -> Disposition {
        Disposition::Deliver
    }
}

struct PolDl;
impl DeadlinePolicy<ByPhase<Pol>> for PolDl {
    fn build((): &()) -> Self {
        Self
    }
    fn next_deadline(&self, _: &P, view: PhaseView<Pol>) -> Option<Instant> {
        match view.phase {
            Phase::A | Phase::B => None,
            Phase::Timed => view.entered_at.checked_add(Duration::from_secs(30)),
        }
    }
    async fn on_deadline(
        &self,
        _: &mut P,
        _: PhaseView<Pol>,
        _: WeakActorRef<Shell<P>>,
    ) -> Result<Step<Phase>, Infallible> {
        Ok(Step::Stay)
    }
}

impl Actor for P {
    type Msg = Tick;
    type Args = ();
    type Error = Infallible;
    type Caps = PhasedOnly;
    async fn init((): (), _: Ctx<'_, Self>) -> Result<Self, Infallible> {
        Ok(Self)
    }
    async fn handle(&mut self, _: Tick, _: Ctx<'_, Self>) -> Result<Flow, Infallible> {
        Ok(Flow::Continue)
    }
}

#[derive(bombay_macros::Provide)]
struct PhasedOnly {
    phased: Phased<Pol>,
}
impl CapSet<P> for PhasedOnly {
    fn build(args: &()) -> Self {
        Self {
            phased: Phased::build(args),
        }
    }
}

#[test]
fn transitions_and_deadline_arming_allocate_nothing() {
    let mut actor = P;
    // Setup (outside every measured window): the machine, with its
    // bounded stash's storage already carved out.
    let mut caps = PhasedOnly::build(&());

    // Warm-up round of every measured operation.
    caps.phased.goto(Phase::B);
    Admission::commit(&mut caps.phased);
    let _ = DeadlineHook::next_deadline(&caps.phased, &actor);
    caps.phased.goto(Phase::A);
    Admission::commit(&mut caps.phased);

    // Stay floor: a commit with no pending transition.
    let before = COUNTER.gross_allocs();
    Admission::commit(&mut caps.phased);
    let stay = COUNTER.gross_allocs() - before;
    assert_eq!(stay, 0, "a Stay step allocates nothing");

    // Goto into a deadline-less phase: switch + anchor reset + (empty)
    // stash release.
    let before = COUNTER.gross_allocs();
    caps.phased.goto(Phase::B);
    Admission::commit(&mut caps.phased);
    let goto = COUNTER.gross_allocs() - before;
    assert_eq!(goto, 0, "a Goto transition adds zero allocations (= Stay)");

    // Goto INTO the phase WITH a declared deadline, then read the armed
    // slot: arming is `entered_at + phase_deadline` arithmetic — no timer
    // task, no allocation (the plane's win over the mock's measured 3).
    let before = COUNTER.gross_allocs();
    caps.phased.goto(Phase::Timed);
    Admission::commit(&mut caps.phased);
    let armed = DeadlineHook::next_deadline(&caps.phased, &actor);
    let arm = COUNTER.gross_allocs() - before;
    assert!(armed.is_some(), "the Timed phase declares a deadline");
    assert_eq!(arm, 0, "arming a phase deadline allocates nothing");

    let _ = &mut actor;
}
