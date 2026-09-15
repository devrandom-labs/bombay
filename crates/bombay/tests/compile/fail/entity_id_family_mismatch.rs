use bombay::entity::EntityId;

struct AccountId(u64);
struct OrderId(u64);

fn dispatch_account(_: EntityId<AccountId>) {}

fn main() {
    dispatch_account(EntityId::new(OrderId(7)));
}
