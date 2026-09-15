use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
pub(crate) struct PlaceOrder {
    pub(crate) order_id: u64,
    pub(crate) customer_id: u64,
    pub(crate) sku: String,
    pub(crate) quantity: u32,
}
