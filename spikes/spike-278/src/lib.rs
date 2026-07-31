//! #278 stage-1 GATE: the OPEN capability encoding on stable Rust.
//!
//! ADR-0026 constraint 2 requires third-party capabilities; K3's HIGH
//! finding proved naive `Has<C>`-over-tuples is E0119-infeasible. This
//! spike proves the bevy-shaped encoding: **named-struct cap sets with
//! per-field `Provide<C>` impls** (derive-generated in the real build;
//! written by hand here — a derive can emit exactly what compiles here).
//!
//! Proof obligations:
//!   O1 openness    — a capability defined OUTSIDE core (module standing
//!                    in for a third-party crate) joins a user cap set
//!                    with zero core changes.
//!   O2 gating      — `cx.cap::<C>()` on a set not providing C is a
//!                    COMPILE error (doctest below).
//!   O3 duplicates  — two fields of the same cap type = overlapping
//!                    `Provide` impls = compile error (doctest below).
//!   O4 policies    — policies stay plugged types on the cap (strategy-
//!                    as-type survives the encoding).

use std::marker::PhantomData;

// ------------------------------------------------------- core machinery --

/// A capability set: built from spawn args. (Derive target in the build.)
pub trait CapSet<Args>: Sized {
    fn build(args: &Args) -> Self;
}

/// Plain actors: the unit set.
impl<Args> CapSet<Args> for () {
    fn build(_: &Args) -> Self {}
}

/// The open seam: "this cap set provides capability `C`". Implemented
/// per FIELD on the user's own struct (derive-generated) — impls live on
/// the user's type, so no coherence overlap is possible and any crate
/// can participate.
pub trait Provide<C> {
    fn provide(&mut self) -> &mut C;
}

/// The one typed window. Accessor is compile-gated by `Provide`.
pub struct Ctx<Caps> {
    pub caps: Caps,
}

impl<Caps> Ctx<Caps> {
    /// O2: exists only when the set provides `C`.
    ///
    /// ```compile_fail
    /// use spike_278::{CapSet, Ctx};
    /// use spike_278::core_caps::Stashing;
    /// // A plain cap set provides nothing:
    /// let mut cx: Ctx<()> = Ctx { caps: () };
    /// cx.cap::<Stashing<u32>>(); // COMPILE ERROR: `()` : !Provide<Stashing<u32>>
    /// ```
    pub fn cap<C>(&mut self) -> &mut C
    where
        Caps: Provide<C>,
    {
        self.caps.provide()
    }
}

/// O3: duplicate capability = overlapping impls = rejected at compile
/// time (the derive emits one `Provide<FieldType>` per field).
///
/// ```compile_fail
/// use spike_278::{Provide, core_caps::Stashing};
/// struct Dup { a: Stashing<u32>, b: Stashing<u32> }
/// impl Provide<Stashing<u32>> for Dup { fn provide(&mut self) -> &mut Stashing<u32> { &mut self.a } }
/// impl Provide<Stashing<u32>> for Dup { fn provide(&mut self) -> &mut Stashing<u32> { &mut self.b } }
/// // E0119: conflicting implementations — duplicates are unrepresentable.
/// ```
pub struct DuplicateRejectionProof;

// ----------------------------------------------- core-provided caps (O4) --

pub mod core_caps {
    use super::PhantomData;

    /// Bounded deferral cap; policy = plugged type (O4).
    pub struct Stashing<M> {
        pub held: Vec<M>,
        pub cap: usize,
    }

    pub trait StashPolicy<Args> {
        fn capacity(args: &Args) -> usize;
    }

    /// Constructor used by derive-generated `CapSet::build`.
    impl<M> Stashing<M> {
        pub fn bounded<Args, SP: StashPolicy<Args>>(args: &Args) -> Self {
            Self {
                held: Vec::new(),
                cap: SP::capacity(args),
            }
        }
    }

    /// Watch cap with a named policy type.
    pub struct Watching<WP> {
        pub notices: Vec<u64>,
        pub _p: PhantomData<WP>,
    }

    pub trait WatchPolicy {
        fn propagate(abnormal: bool, linked: bool) -> bool;
    }

    pub struct OtpPropagation;
    impl WatchPolicy for OtpPropagation {
        fn propagate(abnormal: bool, linked: bool) -> bool {
            abnormal && linked
        }
    }
}

// ------------------------------------- "third-party crate" module (O1) --

/// Stands in for an external crate: defines a NEW capability core has
/// never heard of, using only the public seam (`Provide` is implemented
/// by the USER's derive on the USER's struct — this module only defines
/// the cap type + policy).
pub mod third_party {
    /// A rate-limiter capability, entirely foreign to core.
    pub struct RateLimited<RP> {
        pub tokens: u32,
        pub _p: std::marker::PhantomData<RP>,
    }

    pub trait RatePolicy {
        fn burst() -> u32;
    }

    impl<RP: RatePolicy> RateLimited<RP> {
        pub fn new() -> Self {
            Self {
                tokens: RP::burst(),
                _p: std::marker::PhantomData,
            }
        }
        pub fn try_take(&mut self) -> bool {
            if self.tokens == 0 {
                return false;
            }
            self.tokens -= 1;
            true
        }
    }
}
