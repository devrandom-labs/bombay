//! Concise results for ordinary Bombay actors.

use behavior::{Actions, Address, MailAddr, Never, NoBirths, Step, Stopped};

/// Effects emitted by a common no-birth, no-phase actor turn.
pub struct Effect<S, A: Address = MailAddr> {
    actions: Actions<A, Never, Vec<S>, NoBirths>,
}

impl<S, A: Address> Effect<S, A> {
    /// Continue after emitting no effects.
    #[must_use]
    pub fn none() -> Self {
        Self {
            actions: Actions::cont(),
        }
    }

    /// Continue after emitting one effect.
    #[must_use]
    pub fn send(effect: S) -> Self {
        Self {
            actions: Actions::send(vec![effect]),
        }
    }

    /// Continue after emitting the supplied effects in order.
    #[must_use]
    pub fn send_all(effects: impl IntoIterator<Item = S>) -> Self {
        Self {
            actions: Actions::send(effects.into_iter().collect()),
        }
    }

    /// Stop after this turn while preserving its effects.
    #[must_use]
    pub fn stop(mut self) -> Self {
        self.actions.become_ = Step::Stop(Stopped);
        self
    }
}

impl<S, A: Address> From<Effect<S, A>> for Actions<A, Never, Vec<S>, NoBirths> {
    fn from(effect: Effect<S, A>) -> Self {
        effect.actions
    }
}

#[cfg(test)]
mod tests {
    use behavior::{Behavior, MailAddr, Never, Step, User, delegate_transition, initialize};

    use super::Effect;

    struct Printer(u64);

    #[crate::actor]
    impl Printer {
        fn receive(&mut self, from: MailAddr, message: String) -> Effect<String> {
            self.0 += 1;
            let message = message.into_boxed_str();
            Effect::send(format!("{} says {message} ({})", from.0, self.0)).stop()
        }
    }

    #[test]
    fn actor_macro_infers_the_common_behavior_algebra() {
        fn assert_protocol<B: Behavior<Addr = MailAddr, Msg = String, Error = Never>>(_: &B) {}

        let mut printer = Printer(0);
        assert_protocol(&printer);
        let initialized = initialize(&mut printer).unwrap();
        assert!(initialized.sends.is_empty());
        assert!(matches!(initialized.become_, Step::Continue));

        let acted =
            delegate_transition(&mut printer, User::new(MailAddr(7), "hello".to_owned())).unwrap();
        assert_eq!(acted.sends, ["7 says hello (1)"]);
        assert!(matches!(acted.become_, Step::Stop(_)));
    }
}
