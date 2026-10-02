use std::future::Future;
use std::task::{Context, Poll, Waker};

use observe::affine_pair;

struct MoveOnly(String);

#[test]
fn affine_observation_moves_the_exact_published_outcome() {
    let (publisher, observation) = affine_pair();
    publisher.complete(MoveOnly(String::from("owned")));

    let mut observation = Box::pin(observation);
    let mut context = Context::from_waker(Waker::noop());
    let observed = match observation.as_mut().poll(&mut context) {
        Poll::Ready(outcome) => outcome,
        Poll::Pending => panic!("an already published affine outcome must be ready"),
    };
    assert_eq!(observed.0, "owned");
}

#[test]
fn observe_authority_is_affine_at_the_public_compilation_boundary() {
    let cases = trybuild::TestCases::new();
    cases.compile_fail("tests/compile/fail/publisher_cannot_clone.rs");
    cases.compile_fail("tests/compile/fail/publisher_cannot_complete_twice.rs");
    cases.compile_fail("tests/compile/fail/affine_observation_cannot_clone.rs");
}
