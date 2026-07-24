//! Restart policy & accounting (card #196).
//!
//! WHEN to rebuild a dead child, how to SPACE attempts, and when to GIVE UP.
//! Pure and synchronous on purpose — the async loop only consumes the verdicts
//! produced here, which keeps every restart decision unit- and
//! mutation-testable without a runtime.

use core::time::Duration;

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

#[cfg(test)]
mod tests {
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
}
