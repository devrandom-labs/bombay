use bombay::prelude::*;
use bombay::{ActorSpace, ActorSpaces, Hosts};

struct Orders;

impl Protocol for Orders {
    type Addr = MailAddr;
    type Msg = ();
}

struct Payments;

impl Protocol for Payments {
    type Addr = MailAddr;
    type Msg = u64;
}

#[derive(Default, ActorSpaces)]
struct OrderPaymentSpaces {
    #[actor_space(Orders)]
    orders: ActorSpace<Orders>,
    label: &'static str,
    #[actor_space(Payments)]
    payments: ActorSpace<Payments>,
}

fn hosted_space<P>(spaces: &OrderPaymentSpaces) -> &ActorSpace<P>
where
    P: Protocol<Addr = MailAddr>,
    OrderPaymentSpaces: Hosts<P>,
{
    spaces.space()
}

#[test]
fn derive_selects_each_protocols_exact_named_space() {
    let spaces = OrderPaymentSpaces::default();

    assert!(core::ptr::eq(
        hosted_space::<Orders>(&spaces),
        core::ptr::from_ref(&spaces.orders)
    ));
    assert!(core::ptr::eq(
        hosted_space::<Payments>(&spaces),
        core::ptr::from_ref(&spaces.payments)
    ));
    assert_eq!(spaces.label, "");
}

#[test]
fn one_actor_space_is_its_own_exact_hosting_proof() {
    let space = ActorSpace::<Orders>::new();

    assert!(core::ptr::eq(
        core::ptr::from_ref(&space),
        <ActorSpace<Orders> as Hosts<Orders>>::space(&space),
    ));
}

#[test]
fn actor_spaces_compile_contract() {
    let cases = trybuild::TestCases::new();
    cases.pass("tests/compile/pass/actor_spaces_named.rs");
    cases.pass("tests/compile/pass/behavior_facade_named_children.rs");
    cases.pass("tests/compile/pass/behavior_root_authoring.rs");
    cases.compile_fail("tests/compile/fail/actor_spaces_duplicate.rs");
    cases.compile_fail("tests/compile/fail/actor_spaces_alias_duplicate.rs");
    cases.compile_fail("tests/compile/fail/actor_spaces_wrong_field.rs");
    cases.compile_fail("tests/compile/fail/actor_spaces_tuple.rs");
    cases.compile_fail("tests/compile/fail/actor_spaces_missing_host.rs");
}
