//! Restart policy & accounting (card #196).
//!
//! WHEN to rebuild a dead child, how to SPACE attempts, and when to GIVE UP.
//! Pure and synchronous on purpose — the async loop only consumes the verdicts
//! produced here, which keeps every restart decision unit- and
//! mutation-testable without a runtime.

use core::time::Duration;

use fastrand::Rng;

use crate::error::ActorStopReason;

/// Per-child restart policy — stated explicitly at every `supervise` call and
/// **never defaulted**.
///
/// Three mature supervisors disagree across the whole range on what the default
/// should be — OTP child specs default to `permanent`, Kubernetes pods to
/// `restartPolicy: Always`, Akka Typed to *stop* — which is evidence that the
/// choice belongs to the caller's semantics, not to a framework default. Hence
/// no `Default` impl anywhere in this module.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum RestartPolicy {
    /// Rebuild on every exit, normal or abnormal (a server: exiting is a bug).
    Permanent,
    /// Rebuild on abnormal exit only; a normal stop is the actor's own decision.
    Transient,
    /// Never rebuild; supervision is observation only.
    Never,
}

/// What the supervisor does with one death notice.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum RestartVerdict {
    /// Rebuild the child (subject to backoff and the give-up budgets).
    Restart,
    /// Leave the child dead; the supervisor keeps running.
    LeaveDead,
    /// A lifecycle-hook failure: restarting is a guaranteed crash loop —
    /// bypass backoff and counters, escalate now.
    Escalate,
}

/// The decision table: which deaths deserve a rebuild under `policy`.
///
/// A lifecycle-hook panic short-circuits *every* policy: the hook runs again on
/// the very next incarnation, so a restart is knowably a crash loop rather than
/// a gamble ([`PanicReason::is_lifecycle_hook`](crate::error::PanicReason::is_lifecycle_hook)).
/// A propagated [`LinkDied`](ActorStopReason::LinkDied) is classified by the
/// outer variant — the nested reason belongs to a *different* actor and is
/// diagnostic only.
#[must_use]
pub const fn should_restart(policy: RestartPolicy, reason: &ActorStopReason) -> RestartVerdict {
    if matches!(reason, ActorStopReason::Panicked(err) if err.reason().is_lifecycle_hook()) {
        return RestartVerdict::Escalate;
    }
    match policy {
        RestartPolicy::Transient if reason.is_normal() => RestartVerdict::LeaveDead,
        RestartPolicy::Permanent | RestartPolicy::Transient => RestartVerdict::Restart,
        RestartPolicy::Never => RestartVerdict::LeaveDead,
    }
}

/// Restart-delay jitter as an integer percent of the computed delay.
///
/// An integer percent rather than a float fraction keeps [`RestartConfig`]
/// `Eq`/`Hash` (no float comparison) and makes the config printable without
/// rounding noise. Construction clamps to `0..=100`: jitter is a magnitude, so
/// an over-large value has an obvious saturating meaning and no invalid state
/// needs representing.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct Jitter(u8);

impl Jitter {
    /// Builds a jitter magnitude, clamping `percent` into `0..=100`.
    #[must_use]
    pub const fn percent(percent: u8) -> Self {
        // `u8::min` is not `const`; the branch is the const-compatible clamp.
        Self(if percent > 100 { 100 } else { percent })
    }

    /// The clamped percent, `0..=100`.
    #[must_use]
    pub const fn as_percent(self) -> u8 {
        self.0
    }
}

/// Restart tuning for one supervised child.
///
/// Deliberately **no `Default` impl**: [`policy`](Self::policy) is
/// caller-stated semantics (see [`RestartPolicy`]), so a `Default` — or a later
/// `derive(Default)` — would silently invent it. The remaining fields are plain
/// tuning and carry documented starting values via [`new`](Self::new).
///
/// Fields are public because this is configuration, not an invariant-bearing
/// type: every combination is meaningful, including `min_backoff >
/// max_backoff` (which simply pins every delay at the cap).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct RestartConfig {
    /// Which deaths deserve a rebuild.
    pub policy: RestartPolicy,
    /// Consecutive failures tolerated before escalating — the fast trip, reset
    /// by `reset_after` of healthy uptime.
    pub max_restarts: u32,
    /// Lifetime rebuilds tolerated before escalating — the slow trip, never
    /// reset.
    pub max_total: u32,
    /// Delay before the first retry; doubled per consecutive failure.
    pub min_backoff: Duration,
    /// Ceiling on the exponential delay.
    pub max_backoff: Duration,
    /// Randomness added on top of the computed delay, de-synchronizing children
    /// that fail together.
    pub jitter: Jitter,
    /// Uptime after which an incarnation counts as healthy, zeroing the
    /// consecutive counter.
    pub reset_after: Duration,
    /// How long a child gets to shut down cleanly before it is killed.
    pub stop_grace: Duration,
}

impl RestartConfig {
    /// Documented tuning around an **explicit** policy.
    ///
    /// `stop_grace = 5s` is OTP's child-spec `shutdown` default
    /// (`supervisor:child_spec()`). The other values are unsourced starting
    /// points, to be re-tuned against the deterministic-simulation measurements
    /// of a later card — they are not claimed to be optimal.
    #[must_use]
    pub const fn new(policy: RestartPolicy) -> Self {
        Self {
            policy,
            max_restarts: 5,
            max_total: 100,
            min_backoff: Duration::from_millis(100),
            max_backoff: Duration::from_secs(30),
            jitter: Jitter::percent(20),
            reset_after: Duration::from_mins(1),
            stop_grace: Duration::from_secs(5),
        }
    }

    /// Sets [`max_restarts`](Self::max_restarts).
    #[must_use]
    pub const fn with_max_restarts(mut self, max_restarts: u32) -> Self {
        self.max_restarts = max_restarts;
        self
    }

    /// Sets [`max_total`](Self::max_total).
    #[must_use]
    pub const fn with_max_total(mut self, max_total: u32) -> Self {
        self.max_total = max_total;
        self
    }

    /// Sets [`min_backoff`](Self::min_backoff).
    #[must_use]
    pub const fn with_min_backoff(mut self, min_backoff: Duration) -> Self {
        self.min_backoff = min_backoff;
        self
    }

    /// Sets [`max_backoff`](Self::max_backoff).
    #[must_use]
    pub const fn with_max_backoff(mut self, max_backoff: Duration) -> Self {
        self.max_backoff = max_backoff;
        self
    }

    /// Sets [`jitter`](Self::jitter).
    #[must_use]
    pub const fn with_jitter(mut self, jitter: Jitter) -> Self {
        self.jitter = jitter;
        self
    }

    /// Sets [`reset_after`](Self::reset_after).
    #[must_use]
    pub const fn with_reset_after(mut self, reset_after: Duration) -> Self {
        self.reset_after = reset_after;
        self
    }

    /// Sets [`stop_grace`](Self::stop_grace).
    #[must_use]
    pub const fn with_stop_grace(mut self, stop_grace: Duration) -> Self {
        self.stop_grace = stop_grace;
        self
    }
}

impl From<RestartPolicy> for RestartConfig {
    fn from(policy: RestartPolicy) -> Self {
        Self::new(policy)
    }
}

/// The un-jittered delay before consecutive attempt `consecutive`:
/// `min_backoff · 2^(consecutive - 1)`, capped at
/// [`max_backoff`](RestartConfig::max_backoff).
///
/// The cap is reached by **explicit branch** on every route — a large exponent,
/// an exponent too large to shift, or a product too large to represent — never
/// by `saturating_*`, which would leave "hit the ceiling on purpose" and
/// "overflowed by accident" indistinguishable in the same value.
///
/// `consecutive` is 1-based — the give-up accounting hands out `attempt: 1` for
/// the first retry. A `0` is outside that contract and is treated as the first
/// attempt rather than underflowing.
#[must_use]
pub fn base_backoff(cfg: &RestartConfig, consecutive: u32) -> Duration {
    // Two ways the doubling factor fails to exist, both routed to the ceiling
    // below: a degenerate `consecutive == 0` (out of contract — read as the
    // first attempt, factor 1, NOT a saturating subtraction that would hide it)
    // and an exponent past 31, where `2^n` is not representable at all.
    let Some(factor) = consecutive
        .checked_sub(1)
        .map_or(Some(1), |exponent| 1_u32.checked_shl(exponent))
    else {
        return cfg.max_backoff;
    };
    match cfg.min_backoff.checked_mul(factor) {
        Some(delay) if delay < cfg.max_backoff => delay,
        _ => cfg.max_backoff,
    }
}

/// [`base_backoff`] lengthened by a random `0..=jitter%`, so children that fail
/// together do not retry together.
///
/// The generator is passed in and **seedable**: a deterministic simulation
/// fixes the seed and asserts exact delays, instead of setting jitter to zero
/// and leaving this path untested.
///
/// Divide-then-multiply (`base / 100 * percent`) is deliberate: `base *
/// percent` is un-representable for a near-[`Duration::MAX`] ceiling, while
/// `base / 100` never is. The cost is a truncation below 100 ns on a delay
/// measured in milliseconds.
///
/// Every step is checked. If the lengthened delay is un-representable, the base
/// delay is the answer: jitter only ever *adds*, so a delay already at the
/// ceiling has nothing left to receive — there is no failure to report, and
/// `base` is a real delay rather than a sentinel.
#[must_use]
pub fn jittered_backoff(cfg: &RestartConfig, consecutive: u32, rng: &mut Rng) -> Duration {
    let base = base_backoff(cfg, consecutive);
    let percent = u32::from(rng.u8(0..=cfg.jitter.as_percent()));
    base.checked_div(100)
        .and_then(|step| step.checked_mul(percent))
        .and_then(|extra| base.checked_add(extra))
        .unwrap_or(base)
}

#[cfg(test)]
mod tests {
    use proptest::{prop_assert, proptest};

    use super::*;
    use crate::{
        error::{ActorStopReason, PanicError, PanicReason},
        mailbox::ActorId,
    };

    fn panicked(reason: PanicReason) -> ActorStopReason {
        ActorStopReason::Panicked(PanicError::new(Box::new("boom"), reason))
    }

    /// `Permanent` means "this actor exiting is a bug" — every reason, normal
    /// or abnormal, is a rebuild.
    #[test]
    fn permanent_restarts_on_every_reason() {
        for reason in [
            ActorStopReason::Normal,
            ActorStopReason::SupervisorRestart,
            ActorStopReason::Killed,
            ActorStopReason::AlreadyDead,
            panicked(PanicReason::HandlerPanic),
        ] {
            assert_eq!(
                should_restart(RestartPolicy::Permanent, &reason),
                RestartVerdict::Restart,
                "Permanent must restart on {reason:?}",
            );
        }
    }

    /// A `Transient` child that stopped on purpose stays stopped — including the
    /// supervisor's own deliberate cycle, which is not a failure.
    #[test]
    fn transient_leaves_normal_and_supervisor_restart_dead() {
        assert_eq!(
            should_restart(RestartPolicy::Transient, &ActorStopReason::Normal),
            RestartVerdict::LeaveDead,
        );
        assert_eq!(
            should_restart(
                RestartPolicy::Transient,
                &ActorStopReason::SupervisorRestart
            ),
            RestartVerdict::LeaveDead,
        );
    }

    #[test]
    fn transient_restarts_on_abnormal() {
        for reason in [
            ActorStopReason::Killed,
            ActorStopReason::AlreadyDead,
            panicked(PanicReason::HandlerPanic),
        ] {
            assert_eq!(
                should_restart(RestartPolicy::Transient, &reason),
                RestartVerdict::Restart,
                "{reason:?}",
            );
        }
    }

    #[test]
    fn never_always_leaves_dead() {
        for reason in [
            ActorStopReason::Normal,
            ActorStopReason::Killed,
            panicked(PanicReason::HandlerPanic),
        ] {
            assert_eq!(
                should_restart(RestartPolicy::Never, &reason),
                RestartVerdict::LeaveDead,
                "{reason:?}",
            );
        }
    }

    /// #196: a lifecycle-hook panic (`on_start` above all) is a guaranteed crash
    /// loop — restart is knowably wrong; escalate regardless of policy.
    #[test]
    fn lifecycle_hook_panic_escalates_under_every_policy() {
        for policy in [
            RestartPolicy::Permanent,
            RestartPolicy::Transient,
            RestartPolicy::Never,
        ] {
            assert_eq!(
                should_restart(policy, &panicked(PanicReason::OnStart)),
                RestartVerdict::Escalate,
                "{policy:?}",
            );
        }
    }

    /// Every lifecycle hook escalates, not just `on_start` — the carve-out keys
    /// off [`PanicReason::is_lifecycle_hook`], never a single variant.
    #[test]
    fn every_lifecycle_hook_phase_escalates() {
        for reason in [
            PanicReason::OnStart,
            PanicReason::OnStop,
            PanicReason::OnPanic,
            PanicReason::OnLinkDied,
        ] {
            assert_eq!(
                should_restart(RestartPolicy::Permanent, &panicked(reason)),
                RestartVerdict::Escalate,
                "{reason:?}",
            );
        }
    }

    /// Jitter is a *magnitude*, not semantics: an out-of-range percent is
    /// clamped rather than rejected, so the type has no invalid state and the
    /// config stays `Eq`/`Hash`.
    #[test]
    fn jitter_clamps_to_percent() {
        assert_eq!(Jitter::percent(0).as_percent(), 0);
        assert_eq!(Jitter::percent(20).as_percent(), 20);
        assert_eq!(Jitter::percent(100).as_percent(), 100);
        assert_eq!(Jitter::percent(101).as_percent(), 100, "just past the edge");
        assert_eq!(
            Jitter::percent(u8::MAX).as_percent(),
            100,
            "clamped, not rejected"
        );
    }

    /// A bare policy converts into a config carrying the documented tuning —
    /// the policy itself is always the caller's, never a default.
    #[test]
    fn config_from_bare_policy_uses_documented_tuning() {
        let cfg: RestartConfig = RestartPolicy::Transient.into();
        assert_eq!(cfg.policy, RestartPolicy::Transient);
        assert_eq!(cfg.max_restarts, 5);
        assert_eq!(cfg.max_total, 100);
        assert_eq!(cfg.min_backoff, Duration::from_millis(100));
        assert_eq!(cfg.max_backoff, Duration::from_secs(30));
        assert_eq!(cfg.jitter, Jitter::percent(20));
        assert_eq!(
            cfg.reset_after,
            Duration::from_mins(1),
            "60s of healthy uptime"
        );
        assert_eq!(cfg.stop_grace, Duration::from_secs(5));
    }

    #[test]
    fn builder_overrides_stick() {
        let cfg = RestartConfig::new(RestartPolicy::Permanent)
            .with_max_backoff(Duration::from_secs(5))
            .with_max_restarts(2);
        assert_eq!(cfg.max_backoff, Duration::from_secs(5));
        assert_eq!(cfg.max_restarts, 2);
        assert_eq!(cfg.policy, RestartPolicy::Permanent);
    }

    /// Every builder method changes its OWN field and nothing else — a
    /// copy-paste slip that wired `with_max_total` to `max_restarts` would pass
    /// the two-override test above.
    #[test]
    fn each_builder_method_touches_only_its_own_field() {
        let base = RestartConfig::new(RestartPolicy::Never);
        for (label, changed, expected) in [
            (
                "max_restarts",
                base.with_max_restarts(1),
                RestartConfig {
                    max_restarts: 1,
                    ..base
                },
            ),
            (
                "max_total",
                base.with_max_total(2),
                RestartConfig {
                    max_total: 2,
                    ..base
                },
            ),
            (
                "min_backoff",
                base.with_min_backoff(Duration::from_millis(3)),
                RestartConfig {
                    min_backoff: Duration::from_millis(3),
                    ..base
                },
            ),
            (
                "max_backoff",
                base.with_max_backoff(Duration::from_millis(4)),
                RestartConfig {
                    max_backoff: Duration::from_millis(4),
                    ..base
                },
            ),
            (
                "jitter",
                base.with_jitter(Jitter::percent(5)),
                RestartConfig {
                    jitter: Jitter::percent(5),
                    ..base
                },
            ),
            (
                "reset_after",
                base.with_reset_after(Duration::from_millis(6)),
                RestartConfig {
                    reset_after: Duration::from_millis(6),
                    ..base
                },
            ),
            (
                "stop_grace",
                base.with_stop_grace(Duration::from_millis(7)),
                RestartConfig {
                    stop_grace: Duration::from_millis(7),
                    ..base
                },
            ),
        ] {
            assert_eq!(changed, expected, "with_{label} changed another field");
        }
    }

    /// A `LinkDied` death is classified by the OUTER variant (a propagated
    /// death is abnormal) — the nested reason is diagnostic, not a policy input.
    /// Were the inner reason consulted, a link death nesting `Normal` would read
    /// as a normal stop and leave a `Transient` child dead.
    #[test]
    fn nested_link_died_classified_by_outer_variant() {
        for inner in [ActorStopReason::Killed, ActorStopReason::Normal] {
            let reason = ActorStopReason::LinkDied {
                id: ActorId::new(3),
                reason: Box::new(inner),
            };
            assert_eq!(
                should_restart(RestartPolicy::Transient, &reason),
                RestartVerdict::Restart,
                "{reason:?}",
            );
        }
    }

    /// Attempt `n` waits `min_backoff · 2^(n-1)`.
    #[test]
    fn backoff_grows_exponentially_from_min() {
        let cfg = RestartConfig::new(RestartPolicy::Transient);
        assert_eq!(base_backoff(&cfg, 1), Duration::from_millis(100));
        assert_eq!(base_backoff(&cfg, 2), Duration::from_millis(200));
        assert_eq!(base_backoff(&cfg, 3), Duration::from_millis(400));
        assert_eq!(
            base_backoff(&cfg, 9),
            Duration::from_millis(25_600),
            "last uncapped step"
        );
    }

    /// The cap is a semantic ceiling, so *every* way of exceeding it — a normal
    /// large exponent, an un-shiftable one, an un-representable product —
    /// lands on `max_backoff` rather than wrapping or panicking.
    #[test]
    fn backoff_caps_at_max_and_survives_huge_n() {
        let cfg = RestartConfig::new(RestartPolicy::Transient);
        assert_eq!(
            base_backoff(&cfg, 10),
            Duration::from_secs(30),
            "past the cap"
        );
        assert_eq!(
            base_backoff(&cfg, 32),
            Duration::from_secs(30),
            "largest shiftable exponent"
        );
        assert_eq!(
            base_backoff(&cfg, 33),
            Duration::from_secs(30),
            "first unshiftable exponent"
        );
        assert_eq!(base_backoff(&cfg, u32::MAX - 1), Duration::from_secs(30));
        assert_eq!(
            base_backoff(&cfg, u32::MAX),
            Duration::from_secs(30),
            "overflow = explicit cap branch"
        );
        assert_eq!(
            base_backoff(&cfg, 0),
            cfg.min_backoff,
            "n=0 degenerate = first attempt"
        );
    }

    /// Defensive boundary: a config whose `min_backoff` is already the largest
    /// representable duration makes the doubling un-representable. A bare `*`
    /// would panic in debug and wrap in release; the checked branch must yield
    /// the ceiling instead.
    #[test]
    fn backoff_with_unrepresentable_product_yields_the_cap() {
        let cfg = RestartConfig::new(RestartPolicy::Transient)
            .with_min_backoff(Duration::MAX)
            .with_max_backoff(Duration::MAX);
        assert_eq!(base_backoff(&cfg, 2), Duration::MAX);
    }

    /// Zero jitter must be *exactly* the base — a supervisor that wants
    /// deterministic spacing gets it, and the jitter path cannot leak a stray
    /// nanosecond.
    #[test]
    fn zero_jitter_is_exactly_the_base() {
        let cfg = RestartConfig::new(RestartPolicy::Transient).with_jitter(Jitter::percent(0));
        let mut rng = Rng::with_seed(42);
        for attempt in 0..12_u32 {
            assert_eq!(
                jittered_backoff(&cfg, attempt, &mut rng),
                base_backoff(&cfg, attempt),
                "attempt {attempt}",
            );
        }
    }

    /// Jitter lengthens the delay by at most `jitter%`, and the *same seed
    /// produces the same delay* — the DST contract that lets a simulation assert
    /// exact deadlines instead of disabling jitter and leaving it untested.
    #[test]
    fn jittered_backoff_is_seeded_and_bounded() {
        let cfg = RestartConfig::new(RestartPolicy::Transient); // 20% jitter
        let base = base_backoff(&cfg, 3); // 400ms
        let mut rng = Rng::with_seed(42);
        let delay = jittered_backoff(&cfg, 3, &mut rng);
        assert!(
            delay >= base && delay <= base + base / 5,
            "within +20% of {base:?}: {delay:?}",
        );

        let mut same_seed = Rng::with_seed(42);
        assert_eq!(
            delay,
            jittered_backoff(&cfg, 3, &mut same_seed),
            "same seed ⇒ same delay (DST contract)",
        );
    }

    /// Jitter is actually *applied*: over a run of draws the delays vary and at
    /// least one exceeds the base. Without this, an implementation that ignored
    /// the rng entirely would satisfy the bounds and determinism assertions.
    #[test]
    fn jitter_varies_the_delay_across_draws() {
        let cfg = RestartConfig::new(RestartPolicy::Transient);
        let base = base_backoff(&cfg, 3);
        let mut rng = Rng::with_seed(7);
        let delays: Vec<Duration> = (0..64)
            .map(|_| jittered_backoff(&cfg, 3, &mut rng))
            .collect();

        assert!(
            delays.iter().any(|&d| d > base),
            "20% jitter must lengthen at least one of 64 draws",
        );
        let distinct: std::collections::BTreeSet<Duration> = delays.iter().copied().collect();
        assert!(distinct.len() > 1, "delays must vary, got {distinct:?}");
        assert!(
            delays.iter().all(|&d| d >= base && d <= base + base / 5),
            "every draw stays within +20%: {delays:?}",
        );
    }

    /// Defensive boundary: at the largest representable delay there is no room
    /// left to add jitter. The sum must not wrap or panic — the un-jittered base
    /// is the answer, since jitter only ever lengthens a delay.
    #[test]
    fn jitter_on_an_unextendable_delay_returns_the_base() {
        let cfg = RestartConfig::new(RestartPolicy::Transient)
            .with_min_backoff(Duration::MAX)
            .with_max_backoff(Duration::MAX)
            .with_jitter(Jitter::percent(100));
        let mut rng = Rng::with_seed(1);
        for attempt in 1..8_u32 {
            assert_eq!(
                jittered_backoff(&cfg, attempt, &mut rng),
                Duration::MAX,
                "attempt {attempt}",
            );
        }
    }

    proptest! {
        /// MIRI-skipped by prefix (the repo's `prop_` naming contract).
        #[test]
        fn prop_backoff_monotone_until_cap(n in 0_u32..64) {
            let cfg = RestartConfig::new(RestartPolicy::Transient);
            prop_assert!(base_backoff(&cfg, n) <= base_backoff(&cfg, n.saturating_add(1)));
            prop_assert!(base_backoff(&cfg, n) <= cfg.max_backoff);
        }

        /// Jitter never shortens a delay and never exceeds the configured
        /// percentage — over arbitrary attempts, seeds and jitter magnitudes.
        #[test]
        fn prop_jitter_stays_within_its_percentage(
            n in 0_u32..40,
            seed: u64,
            percent in 0_u8..=u8::MAX,
        ) {
            let jitter = Jitter::percent(percent);
            let cfg = RestartConfig::new(RestartPolicy::Transient).with_jitter(jitter);
            let base = base_backoff(&cfg, n);
            let delay = jittered_backoff(&cfg, n, &mut Rng::with_seed(seed));
            prop_assert!(delay >= base, "jitter must not shorten {base:?}: {delay:?}");
            let ceiling = base + base / 100 * u32::from(jitter.as_percent());
            prop_assert!(delay <= ceiling, "{delay:?} exceeds {ceiling:?}");
        }
    }
}
