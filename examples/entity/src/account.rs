use bombay::prelude::*;

#[derive(Debug)]
pub(super) enum AccountCommand {
    Deposit(u64),
}

pub(super) struct Account {
    pub(super) balance: u64,
}

#[bombay::actor]
#[allow(
    clippy::unnecessary_wraps,
    reason = "the Behavior fold returns the typed action algebra"
)]
impl Account {
    fn receive(&mut self, command: AccountCommand) -> BehaviorActed<Self> {
        let AccountCommand::Deposit(amount) = command;
        self.balance = self
            .balance
            .checked_add(amount)
            .expect("the example account balance remains bounded");
        Ok(Actions::cont())
    }
}
