//! O1 + O4 proofs: a user cap-set struct mixing a CORE cap and a
//! THIRD-PARTY cap, with hand-written impls standing in for the derive
//! (`#[derive(CapSet)]` emits exactly these).

use spike_278::core_caps::{OtpPropagation, StashPolicy, Stashing, Watching};
use spike_278::third_party::{RateLimited, RatePolicy};
use spike_278::{CapSet, Ctx, Provide};

struct WorkerArgs {
    stash_cap: usize,
}

// Policies: plugged types, required by construction (O4).
struct WorkerStash;
impl StashPolicy<WorkerArgs> for WorkerStash {
    fn capacity(args: &WorkerArgs) -> usize {
        args.stash_cap
    }
}

struct Burst3;
impl RatePolicy for Burst3 {
    fn burst() -> u32 {
        3
    }
}

/// The user's cap set: one core cap, one watch cap, one THIRD-PARTY cap.
/// Everything below `struct WorkerCaps` is what `#[derive(CapSet)]`
/// generates — one `Provide` impl per field + `build`.
struct WorkerCaps {
    stash: Stashing<u64>,
    watch: Watching<OtpPropagation>,
    rate: RateLimited<Burst3>,
}

impl CapSet<WorkerArgs> for WorkerCaps {
    fn build(args: &WorkerArgs) -> Self {
        Self {
            stash: Stashing::bounded::<WorkerArgs, WorkerStash>(args),
            watch: Watching {
                notices: Vec::new(),
                _p: std::marker::PhantomData,
            },
            rate: RateLimited::new(),
        }
    }
}

impl Provide<Stashing<u64>> for WorkerCaps {
    fn provide(&mut self) -> &mut Stashing<u64> {
        &mut self.stash
    }
}
impl Provide<Watching<OtpPropagation>> for WorkerCaps {
    fn provide(&mut self) -> &mut Watching<OtpPropagation> {
        &mut self.watch
    }
}
impl Provide<RateLimited<Burst3>> for WorkerCaps {
    fn provide(&mut self) -> &mut RateLimited<Burst3> {
        &mut self.rate
    }
}

#[test]
fn o1_third_party_cap_composes_with_core_caps() {
    let args = WorkerArgs { stash_cap: 2 };
    let mut cx = Ctx {
        caps: WorkerCaps::build(&args),
    };

    // Core cap through the one gated accessor:
    cx.cap::<Stashing<u64>>().held.push(7);
    assert_eq!(cx.cap::<Stashing<u64>>().cap, 2, "policy fed from args");

    // Third-party cap through the SAME accessor, zero core changes:
    assert!(cx.cap::<RateLimited<Burst3>>().try_take());
    assert!(cx.cap::<RateLimited<Burst3>>().try_take());
    assert!(cx.cap::<RateLimited<Burst3>>().try_take());
    assert!(
        !cx.cap::<RateLimited<Burst3>>().try_take(),
        "burst of 3 exhausted — third-party policy drove behavior"
    );

    // Watch cap present too: three capabilities composed on one struct —
    // the combination the shipped tier system cannot express at all.
    assert!(cx.cap::<Watching<OtpPropagation>>().notices.is_empty());
}
