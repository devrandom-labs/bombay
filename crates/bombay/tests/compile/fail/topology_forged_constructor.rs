use bombay::{EstablishNativeChild, PendingChild, ProjectTerminal};
use bombay::prelude::*;
use bombay::behavior::{Behavior, BehaviorSettlements, ChildCreationOutcome, CreationRejection, CreationId, CreationKind, ChildHead, ItemSettlement, Never, RoutedCreation};
use core::future::Future;
use std::rc::Rc;
use behavior::{InterpreterRequests, ReportToParent};
#[path = "../../../../../examples/counter/src/counter.rs"]
mod counter;
struct CounterScope;
struct CounterReport { value: Rc<u64> }
#[bombay::actor(message = Never, sends = { values: InterpreterRequests<ReportToParent<u64>> })]
impl CounterReport {
    fn init(&mut self) -> BehaviorActed<Self> {
        Ok(Actions::cont().with_send(ReportToParent::new(*self.value)))
    }
}
impl EstablishNativeChild<counter::Counter, CounterReport, Never> for CounterScope {
    fn establish_native_child<Position, Terminal, Origin, OriginFn>(
        pending: PendingChild<'_, counter::Counter, Self, Position, CounterReport, Terminal, Never, OriginFn>,
    ) -> impl Future<Output = ItemSettlement<RoutedCreation<bombay::MailAddr, CounterReport>, ChildCreationOutcome<CounterReport, Position>, CreationRejection, Never>> + Send
    where Position: 'static,
        Terminal: ProjectTerminal<Origin, CounterReport, Descendants = Never> + Send + 'static,
        Origin: Send + 'static,
        OriginFn: Fn(bombay::MailAddr, u64, CreationId, CreationKind) -> Origin + Send,
    { drop(pending); async { panic!("forged execution capability must be statically denied") } }
}
fn main() {}
