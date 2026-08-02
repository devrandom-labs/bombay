//! The watching capability: [`WatchPolicy`] death reactions, the NAMED
//! [`OtpPropagation`] policy, and the [`HasWatching`] loop-participation
//! half (ADR-0026 stage 3, card #280).

use core::{future::Future, marker::PhantomData, ops::ControlFlow};

use crate::{error::ActorStopReason, mailbox::ActorId};

use super::Actor;

/// The death-reaction policy seat of the [`Watching`] capability — the
/// relocated `on_link_died` hook (ADR-0026 stage 3, card #280).
///
/// Parameterized by the actor so a policy can react through `&mut A`
/// (record, mutate state); the loop's delivery rules are unchanged (a
/// notice arriving after the loop's stop decision is dropped, #266).
/// Policies ride the [`Watching`] TYPE — chosen by name, never inherited.
pub trait WatchPolicy<A: Actor>: Send + 'static {
    /// Reacts to the death of a watched/linked actor. `Break(reason)`
    /// stops the watcher with that reason; `Continue` keeps it running.
    ///
    /// # Errors
    ///
    /// Returns [`A::Error`](Actor::Error) if the reaction fails — a
    /// controlled crash, exactly as a handler `Err`.
    fn on_link_died(
        actor: &mut A,
        id: ActorId,
        reason: ActorStopReason,
        linked: bool,
    ) -> impl Future<Output = Result<ControlFlow<ActorStopReason>, A::Error>> + Send;
}

/// The NAMED OTP propagation policy — semantics byte-identical to the
/// removed `Watch::on_link_died` default.
///
/// A **linked abnormal** death propagates
/// ([`LinkDied`](ActorStopReason::LinkDied) carrying the original reason);
/// a watch-only (`linked == false`) death, or any normal death, is
/// observed and the actor continues. Chosen by writing
/// `Watching<OtpPropagation>` — never inherited silently (card #280).
pub struct OtpPropagation;

impl<A: Actor> WatchPolicy<A> for OtpPropagation {
    async fn on_link_died(
        _actor: &mut A,
        id: ActorId,
        reason: ActorStopReason,
        linked: bool,
    ) -> Result<ControlFlow<ActorStopReason>, A::Error> {
        Ok(if linked && !reason.is_normal() {
            ControlFlow::Break(ActorStopReason::LinkDied {
                id,
                reason: Box::new(reason),
            })
        } else {
            ControlFlow::Continue(())
        })
    }
}

/// The watching capability (ADR-0026 stage 3).
///
/// Plugged as a cap-set field, it makes the actor **link-reactive** — the
/// loop drains its link channel and dispatches deaths to `WP`. Zero
/// runtime state (the watchers set stays loop-owned): the policy rides
/// the type, strategy-as-type (ADR-0026 constraint 5).
pub struct Watching<WP> {
    policy: PhantomData<WP>,
}

impl<WP> Watching<WP> {
    /// Builds the (stateless) watching capability.
    #[must_use]
    pub const fn new() -> Self {
        Self {
            policy: PhantomData,
        }
    }
}

impl<WP> Default for Watching<WP> {
    fn default() -> Self {
        Self::new()
    }
}

/// "This cap set watches, with policy [`Policy`](HasWatching::Policy)".
///
/// The loop-participation half of [`Watching`], as [`Replay`](super::Replay) is of
/// [`Stashing`](super::Stashing). Derive-emitted from a `Watching<WP>` field; the
/// associated type (never a free impl parameter) is what keeps the
/// [`Shell`](super::Shell)'s conditional impls coherent (spike-280, no E0207/E0119).
pub trait HasWatching<A: Actor> {
    /// The declared death-reaction policy.
    type Policy: WatchPolicy<A>;
}

#[cfg(test)]
mod tests {
    use core::ops::ControlFlow;

    use super::super::fixtures::Sup;
    use super::{OtpPropagation, WatchPolicy};
    use crate::{error::ActorStopReason, mailbox::ActorId};

    /// The NAMED OTP policy carries the exact semantics of the removed
    /// `Watch::on_link_died` default: a **linked abnormal** death
    /// propagates as `Break(LinkDied)` carrying the original reason;
    /// a watch-only (`linked == false`) abnormal death and a linked
    /// normal death are observed and continue. Port of the removed
    /// `default_hook_breaks_on_linked_abnormal_and_continues_otherwise`.
    #[tokio::test]
    async fn otp_propagation_breaks_on_linked_abnormal_and_continues_otherwise() {
        let mut sup = Sup;
        let id = ActorId::from_raw_for_test(1);

        let out = <OtpPropagation as WatchPolicy<Sup>>::on_link_died(
            &mut sup,
            id,
            ActorStopReason::Killed,
            true,
        )
        .await
        .expect("infallible policy");
        match out {
            ControlFlow::Break(ActorStopReason::LinkDied { id: died, reason }) => {
                assert_eq!(died, id, "the notice's id rides the stop reason");
                assert!(
                    matches!(*reason, ActorStopReason::Killed),
                    "the ORIGINAL reason is preserved, got {reason:?}",
                );
            }
            other => panic!("linked + abnormal must propagate, got {other:?}"),
        }

        let out = <OtpPropagation as WatchPolicy<Sup>>::on_link_died(
            &mut sup,
            id,
            ActorStopReason::Killed,
            false,
        )
        .await
        .expect("infallible policy");
        assert!(
            matches!(out, ControlFlow::Continue(())),
            "watch (linked=false) + abnormal is notify-only, got {out:?}",
        );

        let out = <OtpPropagation as WatchPolicy<Sup>>::on_link_died(
            &mut sup,
            id,
            ActorStopReason::Normal,
            true,
        )
        .await
        .expect("infallible policy");
        assert!(
            matches!(out, ControlFlow::Continue(())),
            "linked + normal does not propagate, got {out:?}",
        );
    }
}
