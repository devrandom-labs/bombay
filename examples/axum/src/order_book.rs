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

#[cfg(test)]
mod tests {
    use super::*;
    use bombay::testing::InfallibleResultExt;

    #[test]
    fn placing_an_order_changes_only_that_order() {
        let mut orders = OrderBook::default().initialize().infallible().behavior;
        let order = PlaceOrder {
            order_id: 41,
            customer_id: 7,
            sku: "COFFEE-1KG".into(),
            quantity: 2,
        };

        let actions = orders
            .receive(MailAddr(1), OrderMessage::Place(order.clone()))
            .infallible();

        assert_eq!(orders.orders, BTreeMap::from([(41, order)]));
        assert!(matches!(actions.become_, Step::Continue));
    }
}
