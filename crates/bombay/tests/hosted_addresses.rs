use bombay::prelude::*;
use bombay::{HostedAddresses, LocalAddresses};

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

#[derive(Default, HostedAddresses)]
struct LocalActors {
    orders: LocalAddresses<Orders>,
    label: &'static str,
    payments: LocalAddresses<Payments>,
}

fn hosted<P>(actors: &LocalActors) -> &LocalAddresses<P>
where
    P: Protocol<Addr = MailAddr>,
    LocalActors: HostedAddresses<P>,
{
    actors.addresses()
}

#[test]
fn derive_selects_each_protocols_exact_named_table() {
    let actors = LocalActors::default();

    assert!(core::ptr::eq(
        hosted::<Orders>(&actors),
        core::ptr::from_ref(&actors.orders)
    ));
    assert!(core::ptr::eq(
        hosted::<Payments>(&actors),
        core::ptr::from_ref(&actors.payments)
    ));
    assert_eq!(actors.label, "");
}

#[test]
fn one_address_table_is_its_own_exact_hosting_proof() {
    let space = LocalAddresses::<Orders>::new();

    assert!(core::ptr::eq(
        core::ptr::from_ref(&space),
        <LocalAddresses<Orders> as HostedAddresses<Orders>>::addresses(&space),
    ));
}

#[test]
fn hosted_addresses_compile_contract() {
    let cases = trybuild::TestCases::new();
    cases.pass("tests/compile/pass/hosted_addresses_named.rs");
    cases.pass("tests/compile/pass/behavior_facade_named_children.rs");
    cases.pass("tests/compile/pass/behavior_root_authoring.rs");
    cases.compile_fail("tests/compile/fail/hosted_addresses_duplicate.rs");
    cases.compile_fail("tests/compile/fail/hosted_addresses_tuple.rs");
    cases.compile_fail("tests/compile/fail/hosted_addresses_missing_host.rs");
}
