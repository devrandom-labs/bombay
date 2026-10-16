use std::collections::BTreeMap;

use bombay::prelude::*;

use crate::domain::PlaceOrder;

pub(crate) enum OrderMessage {
    Place(PlaceOrder),
}

#[derive(Default)]
pub(crate) struct OrderBook {
    orders: BTreeMap<u64, PlaceOrder>,
}

#[bombay::actor]
impl OrderBook {
    fn receive(&mut self, message: OrderMessage) -> BehaviorActed<Self> {
        match message {
            OrderMessage::Place(order) => {
                self.orders.insert(order.order_id, order);
                Ok(Actions::cont())
            }
        }
    }
}
