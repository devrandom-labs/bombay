use bombay::behavior::{EstablishedDelivery, EstablishedRecipient, Protocol};
use bombay::prelude::{Actions, BehaviorActed, MailAddr};

pub(super) struct Receipts;

impl Protocol for Receipts {
    type Addr = MailAddr;
    type Msg = Vec<MailAddr>;
}

pub(super) enum LedgerCommand {
    Append(Box<[u8]>),
    Read(EstablishedRecipient<Receipts>),
}

pub(super) struct MailLedger {
    pub(super) entries: Vec<(MailAddr, Box<[u8]>)>,
}

#[bombay::actor(sends = pub(super) { receipts: Vec<EstablishedDelivery<Receipts>> })]
impl MailLedger {
    fn receive(&mut self, from: MailAddr, command: LedgerCommand) -> BehaviorActed<Self> {
        match command {
            LedgerCommand::Append(original) => {
                self.entries.push((from, original));
                Ok(Actions::cont())
            }
            LedgerCommand::Read(reply_to) => {
                let origins = self.entries.iter().map(|(origin, _)| *origin).collect();
                Ok(Actions::stop().send_receipts(EstablishedDelivery::new(reply_to, origins)))
            }
        }
    }
}
