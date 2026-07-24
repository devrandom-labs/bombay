//! Restart policy & accounting (card #196).
//!
//! WHEN to rebuild a dead child, how to SPACE attempts, and when to GIVE UP.
//! Pure and synchronous on purpose — the async loop only consumes the verdicts
//! produced here, which keeps every restart decision unit- and
//! mutation-testable without a runtime.

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
