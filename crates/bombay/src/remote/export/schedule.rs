//! Incoming delivery and authority schedules through the real request interpreter.
use super::super::request::{Custody, Identity, Request};
use super::super::tests::counter::{Counter, CounterMessage, CounterValue};
use super::super::{Attempt, Export, ExportLimits, RefusalRetention, submission};
use super::Exports;
use crate::RetirementOutcome;
use crate::actors::ActorExt;
use crate::remote::json;
use crate::remote::verification::poll_verification;
use crate::{
    ActorFailureAssessment, ActorRetirement, Application, Completion, MailAddr,
    RetirementAssessment, TrySendError,
};
use behavior::{ClassifySettlement, SettlementStatus, User};
use core::future::poll_fn;
use core::pin::pin;
use core::task::Poll;
use futures_util::FutureExt;
use futures_util::future::Either;
use proptest::arbitrary::any;
use proptest::collection;
use proptest::sample;
use proptest::strategy::Strategy;
use proptest::test_runner::{Config, RngSeed, TestError, TestRunner};
use serde::Deserialize;
use std::array;
use std::cell::{Cell, RefCell};
use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::num::NonZeroUsize;
use std::rc::Rc;
use std::sync::Arc;
use std::time::Duration;
use tokio::runtime::Builder;
use tokio::sync::{Semaphore, oneshot};

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
enum Delivery {
    Initial,
    Repeat,
    Expiring,
    Revoked,
    ConstructorExpired,
    Fresh,
}

impl Delivery {
    const fn sequence(self) -> u64 {
        match self {
            Self::Initial | Self::Repeat => 101,
            Self::Expiring => 103,
            Self::Revoked => 107,
            Self::ConstructorExpired => 109,
            Self::Fresh => 113,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Scheduled {
    Deliver(Delivery),
    Hold(Delivery),
    Advance(Duration),
    Revoke(Delivery),
    Release(Delivery),
    ExpireConstruction(Duration),
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Grant {
    Current { until: Duration },
    Revoked,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Refusal {
    UnknownPair,
    UnknownGrant,
    Scope,
    Expired,
    Revoked,
}

#[derive(Debug, PartialEq, Eq)]
enum Event {
    Delivered(Delivery),
    VerificationPolled(Delivery, Poll<()>),
    Advanced(Duration),
    Revoked(Delivery),
    Released(Delivery),
    Verified(u64),
    Constructed(u64),
    RequestChecked(u64, Duration, Result<(), Refusal>),
    ReplyChecked(u64, Duration, Result<(), Refusal>),
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Increment {
    sequence: u64,
}

fn permission(
    sequence: u64,
    clock: Duration,
    grants: &BTreeMap<u64, Grant>,
) -> Result<(), Refusal> {
    match grants.get(&sequence) {
        Some(Grant::Current { until }) if clock < *until => Ok(()),
        Some(Grant::Current { .. }) => Err(Refusal::Expired),
        Some(Grant::Revoked) => Err(Refusal::Revoked),
        None => Err(Refusal::UnknownGrant),
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn scheduled_duplicates_and_authority_changes_preserve_native_admission() {
    let deliveries = [
        Delivery::Initial,
        Delivery::Repeat,
        Delivery::Expiring,
        Delivery::Revoked,
        Delivery::ConstructorExpired,
        Delivery::Fresh,
    ];
    let initial = Delivery::Initial.sequence();
    let expiring = Delivery::Expiring.sequence();
    let revoked = Delivery::Revoked.sequence();
    let constructed = Delivery::ConstructorExpired.sequence();
    let fresh = Delivery::Fresh.sequence();
    let zero = Duration::ZERO;
    let ten = Duration::from_secs(10);
    let twenty = Duration::from_secs(20);
    let expected = [
        Event::Delivered(Delivery::Initial),
        Event::Verified(initial),
        Event::RequestChecked(initial, zero, Ok(())),
        Event::Constructed(initial),
        Event::RequestChecked(initial, zero, Ok(())),
        Event::ReplyChecked(initial, zero, Ok(())),
        Event::Delivered(Delivery::Repeat),
        Event::Verified(initial),
        Event::RequestChecked(initial, zero, Ok(())),
        Event::ReplyChecked(initial, zero, Ok(())),
        Event::Delivered(Delivery::Expiring),
        Event::VerificationPolled(Delivery::Expiring, Poll::Pending),
        Event::Advanced(ten),
        Event::Released(Delivery::Expiring),
        Event::Verified(expiring),
        Event::RequestChecked(expiring, ten, Err(Refusal::Expired)),
        Event::Delivered(Delivery::Revoked),
        Event::VerificationPolled(Delivery::Revoked, Poll::Pending),
        Event::Revoked(Delivery::Revoked),
        Event::Released(Delivery::Revoked),
        Event::Verified(revoked),
        Event::RequestChecked(revoked, ten, Err(Refusal::Revoked)),
        Event::Delivered(Delivery::ConstructorExpired),
        Event::Verified(constructed),
        Event::RequestChecked(constructed, ten, Ok(())),
        Event::Advanced(twenty),
        Event::Constructed(constructed),
        Event::RequestChecked(constructed, twenty, Err(Refusal::Expired)),
        Event::Delivered(Delivery::Fresh),
        Event::Verified(fresh),
        Event::RequestChecked(fresh, twenty, Ok(())),
        Event::Constructed(fresh),
        Event::RequestChecked(fresh, twenty, Ok(())),
        Event::ReplyChecked(fresh, twenty, Ok(())),
    ];
    let derived = expected_trace(&deliveries);
    assert_eq!(derived.as_slice(), expected);
    tokio::time::timeout(
        Duration::from_secs(10),
        Box::pin(incoming_requests(&deliveries, &expected)),
    )
    .await
    .expect("watchdog supplies no delivery, expiry or cleanup evidence");
}

fn delivery_orders() -> Vec<[Delivery; 6]> {
    let mut orders = Vec::new();
    for initial in 0..6 {
        for repeat in initial + 1..6 {
            let remaining = (0..6)
                .filter(|&position| position != initial && position != repeat)
                .collect::<Vec<_>>();
            for expiry in 0..remaining.len() {
                for construction in expiry + 1..remaining.len() {
                    let expiring = remaining[expiry];
                    let constructed = remaining[construction];
                    let unassigned = remaining
                        .iter()
                        .copied()
                        .filter(|&position| position != expiring && position != constructed)
                        .collect::<Vec<_>>();
                    for [revoked, fresh] in [
                        [unassigned[0], unassigned[1]],
                        [unassigned[1], unassigned[0]],
                    ] {
                        let deliveries = array::from_fn(|position| match position {
                            position if position == initial => Delivery::Initial,
                            position if position == repeat => Delivery::Repeat,
                            position if position == expiring => Delivery::Expiring,
                            position if position == constructed => Delivery::ConstructorExpired,
                            position if position == revoked => Delivery::Revoked,
                            position if position == fresh => Delivery::Fresh,
                            _ => unreachable!("six distinct positions own the six deliveries"),
                        });
                        orders.push(deliveries);
                    }
                }
            }
        }
    }
    orders
}

#[test]
fn delivery_orders_cover_each_legal_six_label_word_once() {
    let orders = delivery_orders();
    let unique = orders.iter().copied().collect::<BTreeSet<_>>();
    assert_eq!(orders.len(), 180);
    assert_eq!(unique.len(), 180);
    let labels = BTreeSet::from([
        Delivery::Initial,
        Delivery::Repeat,
        Delivery::Expiring,
        Delivery::Revoked,
        Delivery::ConstructorExpired,
        Delivery::Fresh,
    ]);
    for deliveries in orders {
        let present = deliveries.into_iter().collect::<BTreeSet<_>>();
        assert_eq!(present, labels);
        let initial = deliveries
            .iter()
            .position(|&delivery| delivery == Delivery::Initial)
            .unwrap();
        let repeat = deliveries
            .iter()
            .position(|&delivery| delivery == Delivery::Repeat)
            .unwrap();
        let expiring = deliveries
            .iter()
            .position(|&delivery| delivery == Delivery::Expiring)
            .unwrap();
        let constructed = deliveries
            .iter()
            .position(|&delivery| delivery == Delivery::ConstructorExpired)
            .unwrap();
        assert!(initial < repeat);
        assert!(expiring < constructed);
    }
}

fn expected_trace(deliveries: &[Delivery]) -> Vec<Event> {
    let mut trace = Vec::new();
    let mut at = Duration::ZERO;
    for &delivery in deliveries {
        let sequence = delivery.sequence();
        trace.push(Event::Delivered(delivery));
        match delivery {
            Delivery::Initial | Delivery::Fresh => trace.extend([
                Event::Verified(sequence),
                Event::RequestChecked(sequence, at, Ok(())),
                Event::Constructed(sequence),
                Event::RequestChecked(sequence, at, Ok(())),
                Event::ReplyChecked(sequence, at, Ok(())),
            ]),
            Delivery::Repeat => trace.extend([
                Event::Verified(sequence),
                Event::RequestChecked(sequence, at, Ok(())),
                Event::ReplyChecked(sequence, at, Ok(())),
            ]),
            Delivery::Expiring => {
                at = Duration::from_secs(10);
                trace.extend([
                    Event::VerificationPolled(delivery, Poll::Pending),
                    Event::Advanced(at),
                    Event::Released(delivery),
                    Event::Verified(sequence),
                    Event::RequestChecked(sequence, at, Err(Refusal::Expired)),
                ]);
            }
            Delivery::Revoked => trace.extend([
                Event::VerificationPolled(delivery, Poll::Pending),
                Event::Revoked(delivery),
                Event::Released(delivery),
                Event::Verified(sequence),
                Event::RequestChecked(sequence, at, Err(Refusal::Revoked)),
            ]),
            Delivery::ConstructorExpired => {
                trace.extend([
                    Event::Verified(sequence),
                    Event::RequestChecked(sequence, at, Ok(())),
                ]);
                at = Duration::from_secs(20);
                trace.extend([
                    Event::Advanced(at),
                    Event::Constructed(sequence),
                    Event::RequestChecked(sequence, at, Err(Refusal::Expired)),
                ]);
            }
        }
    }
    trace
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn every_legal_six_label_order_preserves_native_admission() {
    let orders = delivery_orders();
    assert_eq!(orders.len(), 180);
    let mut completed = 0;
    for deliveries in orders {
        let expected = expected_trace(&deliveries);
        tokio::time::timeout(
            Duration::from_secs(10),
            Box::pin(incoming_requests(&deliveries, &expected)),
        )
        .await
        .expect("watchdog supplies no finite-order admission or cleanup evidence");
        completed += 1;
    }
    assert_eq!(completed, 180);
    println!("completed incoming delivery orders: {completed}");
}

#[test]
fn generated_delivery_orders_preserve_native_admission() {
    let orders = (
        sample::select(delivery_orders()),
        collection::vec(any::<sample::Index>(), 0..=2),
    )
        .prop_map(|(base, insertions)| {
            let initial = base
                .iter()
                .position(|&delivery| delivery == Delivery::Initial)
                .expect("each legal base word contains Initial");
            let gaps = base.len() - initial;
            let mut deliveries = Vec::with_capacity(base.len() + insertions.len());
            for (position, delivery) in base.into_iter().enumerate() {
                deliveries.push(delivery);
                if position >= initial {
                    for insertion in &insertions {
                        if insertion.index(gaps) == position - initial {
                            deliveries.push(Delivery::Repeat);
                        }
                    }
                }
            }
            deliveries
        });
    let mut configuration = Config::with_source_file(file!());
    if let RngSeed::Random = configuration.rng_seed {
        configuration.rng_seed = RngSeed::Fixed(20_261_112);
    }
    let cases = configuration.cases;
    let shrink_iterations = configuration.max_shrink_iters();
    println!(
        "generated incoming configuration: algorithm={:?}; seed={}; cases={cases}; shrink_iterations={} (effective {shrink_iterations}); shrink_time_ms={} (0 is unlimited); source_file={:?}; fork={}; timeout_ms={}; persistence={:?}",
        configuration.rng_algorithm,
        configuration.rng_seed,
        configuration.max_shrink_iters,
        configuration.max_shrink_time,
        configuration.source_file,
        configuration.fork(),
        configuration.timeout(),
        configuration.failure_persistence,
    );
    assert!(
        cases > 0,
        "the generated campaign must execute successful cases"
    );
    assert!(
        shrink_iterations > 0,
        "the generated campaign must permit shrinking"
    );
    assert!(
        !configuration.fork(),
        "this campaign requires in-process observations, without Proptest fork or timeout"
    );
    assert!(
        configuration.failure_persistence.is_some(),
        "this campaign requires standard failure persistence"
    );
    let mut runner = TestRunner::new(configuration);
    let invocations = Cell::new(0_u64);
    let completed = Cell::new(0_u64);
    let result = runner.run(&orders, |deliveries| {
        invocations.set(invocations.get() + 1);
        let expected = expected_trace(&deliveries);
        {
            let host = Builder::new_multi_thread()
                .worker_threads(2)
                .enable_all()
                .build()
                .expect("each generated case owns its ordinary Tokio host");
            host.block_on(async {
                tokio::time::timeout(
                    Duration::from_secs(10),
                    Box::pin(incoming_requests(&deliveries, &expected)),
                )
                .await
                .expect("watchdog supplies no generated admission or cleanup evidence");
            });
        }
        completed.set(completed.get() + 1);
        Ok(())
    });
    println!(
        "generated incoming observations: invocations={}; completed={}",
        invocations.get(),
        completed.get()
    );
    match result {
        Ok(()) => {
            assert_eq!(invocations.get(), completed.get());
            assert!(completed.get() >= u64::from(cases));
        }
        Err(TestError::Fail(reason, deliveries)) => {
            panic!(
                "generated incoming failure: {reason}; actual minimized deliveries: {deliveries:?}"
            );
        }
        Err(TestError::Abort(reason)) => panic!("generated incoming campaign aborted: {reason}"),
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn minimized_duplicate_and_expiry_order_preserves_native_admission() {
    let deliveries = [
        Delivery::Initial,
        Delivery::Repeat,
        Delivery::Expiring,
        Delivery::ConstructorExpired,
        Delivery::Revoked,
        Delivery::Fresh,
    ];
    let expected = expected_trace(&deliveries);
    tokio::time::timeout(
        Duration::from_secs(10),
        Box::pin(incoming_requests(&deliveries, &expected)),
    )
    .await
    .expect("watchdog supplies no literal-order admission or cleanup evidence");
}

#[expect(
    clippy::too_many_lines,
    reason = "one incoming schedule retains every original provider, native request and independent Counter result through cleanup"
)]
async fn incoming_requests(deliveries: &[Delivery], expected: &[Event]) {
    let maximum = NonZeroUsize::new(8).unwrap();
    let distinct = [
        Delivery::Initial,
        Delivery::Expiring,
        Delivery::Revoked,
        Delivery::ConstructorExpired,
        Delivery::Fresh,
    ];
    assert!((distinct.len() + 1..=maximum.get()).contains(&deliveries.len()));
    for delivery in distinct {
        let occurrences = deliveries
            .iter()
            .filter(|&&input| input == delivery)
            .count();
        assert_eq!(occurrences, 1, "each named base delivery occurs once");
    }
    let repeats = deliveries
        .iter()
        .filter(|&&input| input == Delivery::Repeat)
        .count();
    assert!((1..=maximum.get() - distinct.len()).contains(&repeats));
    let initial_position = deliveries
        .iter()
        .position(|&input| input == Delivery::Initial)
        .unwrap();
    let expiry_position = deliveries
        .iter()
        .position(|&input| input == Delivery::Expiring)
        .unwrap();
    let construction_position = deliveries
        .iter()
        .position(|&input| input == Delivery::ConstructorExpired)
        .unwrap();
    for (position, delivery) in deliveries.iter().enumerate() {
        if *delivery == Delivery::Repeat {
            assert!(initial_position < position, "Initial precedes each Repeat");
        }
    }
    assert!(
        expiry_position < construction_position,
        "expiry advances the clock before constructor expiry"
    );
    let expected_publications = deliveries
        .iter()
        .filter_map(|&delivery| match delivery {
            Delivery::Expiring | Delivery::Revoked => Some((delivery, Result::<(), ()>::Ok(()))),
            Delivery::Initial
            | Delivery::Repeat
            | Delivery::ConstructorExpired
            | Delivery::Fresh => None,
        })
        .collect::<Vec<_>>();
    let initial = Delivery::Initial.sequence();
    let expiring = Delivery::Expiring.sequence();
    let revoked = Delivery::Revoked.sequence();
    let constructed = Delivery::ConstructorExpired.sequence();
    let fresh = Delivery::Fresh.sequence();
    let clock = &Cell::new(Duration::ZERO);
    let grants = &RefCell::new(BTreeMap::from([
        (
            initial,
            Grant::Current {
                until: Duration::from_secs(100),
            },
        ),
        (
            expiring,
            Grant::Current {
                until: Duration::from_secs(5),
            },
        ),
        (
            revoked,
            Grant::Current {
                until: Duration::from_secs(100),
            },
        ),
        (
            constructed,
            Grant::Current {
                until: Duration::from_secs(15),
            },
        ),
        (
            fresh,
            Grant::Current {
                until: Duration::from_secs(100),
            },
        ),
    ]));
    let mut scheduled = VecDeque::new();
    for &delivery in deliveries {
        match delivery {
            Delivery::Initial | Delivery::Repeat | Delivery::Fresh => {
                scheduled.push_back(Scheduled::Deliver(delivery));
            }
            Delivery::Expiring => scheduled.extend([
                Scheduled::Hold(delivery),
                Scheduled::Advance(Duration::from_secs(10)),
                Scheduled::Release(delivery),
            ]),
            Delivery::Revoked => scheduled.extend([
                Scheduled::Hold(delivery),
                Scheduled::Revoke(delivery),
                Scheduled::Release(delivery),
            ]),
            Delivery::ConstructorExpired => scheduled.extend([
                Scheduled::Deliver(delivery),
                Scheduled::ExpireConstruction(Duration::from_secs(20)),
            ]),
        }
    }
    let scheduled = &RefCell::new(scheduled);
    let trace = &RefCell::new(Vec::new());
    let registered = &[initial, expiring, revoked, constructed, fresh]
        .map(|sequence| {
            (
                sequence,
                (
                    serde_json::to_vec(&serde_json::json!({"sequence": sequence}))
                        .unwrap()
                        .into_boxed_slice(),
                    Box::<[u8]>::from([
                        u8::try_from(sequence).expect("the fixed sequence fits its fixture proof"),
                        3,
                    ]),
                ),
            )
        })
        .into_iter()
        .collect::<BTreeMap<_, _>>();
    let (expiry_release, expiry_wait) = oneshot::channel();
    let (revocation_release, revocation_wait) = oneshot::channel();
    let releases = &RefCell::new(BTreeMap::from([
        (expiring, expiry_release),
        (revoked, revocation_release),
    ]));
    let waits = &RefCell::new(BTreeMap::from([
        (expiring, expiry_wait),
        (revoked, revocation_wait),
    ]));
    // Private provider-disposal input only; this unissued unit publication
    // cannot stand in for the actual Application cleanup inspected below.
    let (cancellation_owner, joined) = oneshot::channel::<()>();
    let joined = joined.shared();
    let issuer = &Arc::new(Semaphore::new(maximum.get()));
    let outcome = Application::new(Counter::new().stop_on_shutdown())
        .run_with(|application| async move {
            let target = application.root().established_recipient();
            let address = application.root().address();
            // Explicit deterministic fixture identity, never a native SDK ZID.
            let runtime = "incoming-schedule";
            let current_request = move |sequence: &u64, current_runtime: &str, actor: MailAddr| {
                let decision = if current_runtime == runtime && actor == address {
                    permission(*sequence, clock.get(), &grants.borrow())
                } else {
                    Err(Refusal::Scope)
                };
                trace
                    .borrow_mut()
                    .push(Event::RequestChecked(*sequence, clock.get(), decision));
                decision
            };
            let current_reply = move |sequence: &u64, current_runtime: &str, actor: MailAddr| {
                let decision = if current_runtime == runtime && actor == address {
                    permission(*sequence, clock.get(), &grants.borrow())
                } else {
                    Err(Refusal::Scope)
                };
                trace
                    .borrow_mut()
                    .push(Event::ReplyChecked(*sequence, clock.get(), decision));
                decision
            };
            let mut export = Export::new(
                "schedule/increment",
                ExportLimits {
                    queued_queries: maximum,
                    retained_queries: maximum,
                    retained_requests: maximum,
                    diagnostics: maximum,
                    refusals: RefusalRetention::Retain,
                },
                json::Limits {
                    input_bytes: NonZeroUsize::new(128).unwrap(),
                    proof_bytes: maximum,
                    token_bytes: NonZeroUsize::new(32).unwrap(),
                    nesting: maximum,
                    parser_states: NonZeroUsize::new(32).unwrap(),
                },
                |bytes: &[u8]| serde_json::from_slice::<Increment>(bytes),
                submission(
                    move |increment: Increment| {
                        Ok::<_, Increment>(move || {
                            if increment.sequence == constructed {
                                let event = scheduled.borrow_mut().pop_front();
                                let Some(Scheduled::ExpireConstruction(at)) = event else {
                                    panic!(
                                        "the fixed schedule supplies the constructor clock event"
                                    );
                                };
                                clock.set(at);
                                trace.borrow_mut().push(Event::Advanced(at));
                            }
                            trace
                                .borrow_mut()
                                .push(Event::Constructed(increment.sequence));
                            CounterMessage::Increment
                        })
                    },
                    |sequence: &u64, _: &Attempt<Result<(), TrySendError<CounterMessage>>>| {
                        serde_json::to_vec(&serde_json::json!({"sequence": sequence}))
                            .map(Vec::into_boxed_slice)
                    },
                ),
                async move |bytes: &[u8], proof: &[u8]| {
                    let sequence = registered
                        .iter()
                        .find_map(|(sequence, original)| {
                            (original.0.as_ref() == bytes && original.1.as_ref() == proof)
                                .then_some(*sequence)
                        })
                        .ok_or(Refusal::UnknownPair)?;
                    let waiting = waits.borrow_mut().remove(&sequence);
                    if let Some(waiting) = waiting {
                        waiting
                            .await
                            .expect("the fixed schedule releases this original verifier");
                    }
                    trace.borrow_mut().push(Event::Verified(sequence));
                    Ok::<_, Refusal>(sequence)
                },
                async |_: &[u8], _: &u64| Ok::<_, Refusal>(Box::<[u8]>::from([211])),
                |left: &u64, right: &u64| left == right,
                current_request,
                current_reply,
            );
            let mut addresses = Vec::new();
            let (interface, control, _lifecycle) = export
                .preflight(&application, &target, &mut addresses)
                .unwrap_or_else(|()| panic!("the actual Application owns this Counter"));
            let mut originals = Vec::new();
            let mut revocations = Vec::new();
            let mut publications = Vec::new();
            loop {
                let next = scheduled.borrow_mut().pop_front();
                let Some(next) = next else {
                    break;
                };
                let (Scheduled::Deliver(delivery) | Scheduled::Hold(delivery)) = next else {
                    panic!(
                        "every control event belongs to its scheduled verification or constructor"
                    );
                };
                trace.borrow_mut().push(Event::Delivered(delivery));
                let original = registered.get(&delivery.sequence()).unwrap();
                let original = (original.0.clone(), original.1.clone());
                originals.push((delivery, original.0.as_ptr(), original.1.as_ptr()));
                let row = export.entries.borrow().len();
                export.entries.borrow_mut().push(Some(Request {
                    custody: Rc::new(Custody {
                        identity: RefCell::new(Identity::Unassigned),
                        original,
                        query: RefCell::new(None),
                        verified: RefCell::new(None),
                        decoded: RefCell::new(None),
                        native: RefCell::new(None),
                        encoding: RefCell::new(None),
                        protection: RefCell::new(None),
                        publications: RefCell::new(Vec::new()),
                        request_refusal: RefCell::new(None),
                        reply_refusal: RefCell::new(None),
                        refusal: RefCell::new(None),
                        verification_factory: RefCell::new(None),
                        verification_poll: RefCell::new(None),
                        verification_disposal: RefCell::new(None),
                        protection_factory: RefCell::new(None),
                        protection_poll: RefCell::new(None),
                        protection_disposal: RefCell::new(None),
                        completed: RefCell::new(None),
                    }),
                }));
                let custody = Rc::clone(&export.entries.borrow()[row].as_ref().unwrap().custody);
                let operation = export.request(
                    row,
                    Arc::downgrade(issuer),
                    &interface,
                    &control,
                    runtime,
                    application.retirement(),
                    joined.clone(),
                );
                let mut operation = pin!(operation);
                if let Scheduled::Hold(_) = next {
                    let observed = poll_fn(|context| {
                        Poll::Ready(poll_verification(
                            operation.as_mut(),
                            &mut custody.completed.borrow_mut(),
                            context,
                        ))
                    })
                    .await;
                    trace
                        .borrow_mut()
                        .push(Event::VerificationPolled(delivery, observed));
                    let change = scheduled.borrow_mut().pop_front();
                    match change {
                        Some(Scheduled::Advance(at)) => {
                            clock.set(at);
                            trace.borrow_mut().push(Event::Advanced(at));
                        }
                        Some(Scheduled::Revoke(request)) => {
                            let prior = grants
                                .borrow_mut()
                                .insert(request.sequence(), Grant::Revoked);
                            trace.borrow_mut().push(Event::Revoked(request));
                            revocations.push((request, prior));
                        }
                        _ => panic!("a held verifier has one explicit scheduled authority change"),
                    }
                    let release = scheduled.borrow_mut().pop_front();
                    let Some(Scheduled::Release(request)) = release else {
                        panic!("the fixed schedule releases the held verifier");
                    };
                    let publication = releases
                        .borrow_mut()
                        .remove(&request.sequence())
                        .unwrap()
                        .send(());
                    publications.push((request, publication));
                    trace.borrow_mut().push(Event::Released(request));
                }
                poll_fn(|context| {
                    poll_verification(
                        operation.as_mut(),
                        &mut custody.completed.borrow_mut(),
                        context,
                    )
                })
                .await;
            }
            let mut reader = application.client::<CounterValue>().unwrap();
            let admitted = reader.send(CounterMessage::Read(reader.recipient())).await;
            let value = reader.receive().await;
            reader.close_admission();
            let tail = reader.receive().await;
            let shutdown = application.shutdown();
            (
                export,
                originals,
                revocations,
                publications,
                addresses,
                admitted,
                value,
                tail,
                shutdown,
            )
        })
        .await
        .unwrap_or_else(|_| panic!("the ordinary Application host is entered"));
    let (work, report, native, notifications, ()) = outcome.into_parts();
    let (
        (export, originals, revocations, publications, addresses, admitted, value, tail, shutdown),
        cleanup,
    ) = work
        .try_into_completed()
        .unwrap_or_else(|_| panic!("the complete fixed schedule and local Read remain owned"));
    let (origin, native) = native.expect("the actual native Counter result remains");
    let report = report.expect("the original joined Counter assessment remains");
    let notifications = notifications.expect("both original notification results remain");
    cleanup.expect("the ordinary Application cleanup joins before the oracle");
    notifications
        .termination
        .expect("actual termination notification");
    notifications
        .retirement
        .expect("actual joined-retirement notification");
    drop(cancellation_owner);
    println!("incoming deliveries: {deliveries:?}");
    println!(
        "incoming Counter: {value:?}; actual trace: {:?}",
        trace.borrow().as_slice()
    );
    assert_eq!(
        value,
        Some(User::new(origin.address(), 2)),
        "only initial and fresh increments may execute"
    );
    assert!(admitted.is_ok() && tail.is_none() && shutdown.is_ok());
    assert_eq!(report.retirement(), RetirementAssessment::Established);
    assert_eq!(report.failures(), ActorFailureAssessment::NoFailuresFound);
    assert_eq!(native.completion(), Some(&Completion::Stopped));
    let ActorRetirement::Retained {
        custody,
        outcome:
            RetirementOutcome::Completed {
                interpretation,
                source,
                settlements,
                ..
            },
    } = native
    else {
        panic!("the Counter retains its original normal native retirement");
    };
    assert!(interpretation.is_none() && source.is_none());
    assert_eq!(settlements.settlement_status(), SettlementStatus::Accepted);
    assert!(
        custody.user.is_empty() && custody.control.is_empty() && custody.descendants.is_empty()
    );
    assert!(custody.child_failures.is_empty() && custody.capability_failures.is_empty());
    assert!(custody.additional_failures.is_empty() && custody.retirement_failures.is_empty());
    assert!(custody.received_interpretation.is_none() && custody.received_source.is_none());
    assert!(custody.source_index.is_none() && custody.acquired_ingress.is_none());
    assert!(custody.terminal_report.is_none() && custody.unread_owner_cancellation.is_none());
    assert_eq!(addresses, [origin.address()]);
    assert_eq!(
        revocations,
        [(
            Delivery::Revoked,
            Some(Grant::Current {
                until: Duration::from_secs(100)
            })
        )]
    );
    assert_eq!(publications, expected_publications);
    assert!(
        scheduled.borrow().is_empty() && releases.borrow().is_empty() && waits.borrow().is_empty()
    );
    let (_, _, startup, withdrawal, ingress, rows) = export.into_parts();
    assert!(startup.is_none() && withdrawal.is_none() && ingress.is_empty());
    assert_eq!(rows.len(), deliveries.len());
    assert_eq!(originals.len(), rows.len());
    let mut keys = BTreeMap::new();
    for (row, (delivery, protected_allocation, proof_allocation)) in rows.into_iter().zip(originals)
    {
        assert_eq!(*row.custody.completed.borrow(), Some(()));
        assert!(row.custody.query.borrow().is_none());
        let (
            key,
            original,
            verified,
            decoded,
            native,
            encoding,
            protection,
            publications,
            refusals,
            panics,
        ) = row
            .try_into_parts()
            .unwrap_or_else(|_| panic!("every actual request producer has ended"));
        assert_eq!(original.0.as_ptr(), protected_allocation);
        assert_eq!(original.1.as_ptr(), proof_allocation);
        let expected = registered.get(&delivery.sequence()).unwrap();
        assert_eq!(
            (original.0.as_ref(), original.1.as_ref()),
            (expected.0.as_ref(), expected.1.as_ref())
        );
        assert_eq!(verified, Some(Ok(delivery.sequence())));
        assert!(decoded.is_none() && publications.is_empty());
        assert!(matches!(panics, (None, None, None, None, None, None)));
        match delivery {
            Delivery::Initial | Delivery::Fresh => {
                let key = key.expect("a real selected native attempt owns its canonical key");
                let previous = keys.insert(delivery, key);
                assert!(previous.is_none());
                assert!(matches!(refusals, (None, None, None)));
                let (command, sender, admission) =
                    native.expect("the actual native submission remains");
                assert!(command.is_none());
                assert!(matches!(sender, Attempt::Returned(Ok(_))));
                assert!(matches!(admission, Attempt::Returned(Ok(()))));
                let expected =
                    serde_json::to_vec(&serde_json::json!({"sequence": delivery.sequence()}))
                        .unwrap();
                assert!(
                    matches!(&encoding, Some(Ok(bytes)) if bytes.as_ref() == expected.as_slice())
                );
                assert_eq!(protection, Some(Ok(Box::<[u8]>::from([211]))));
            }
            Delivery::Repeat => {
                let initial_key = keys
                    .get(&Delivery::Initial)
                    .expect("Initial already owns its actual canonical key");
                assert_eq!(key.as_ref(), Some(initial_key));
                assert!(native.is_none() && encoding.is_none() && protection.is_none());
                assert!(matches!(refusals, (None, None, None)));
            }
            Delivery::Expiring | Delivery::Revoked => {
                assert!(
                    key.is_none() && native.is_none() && encoding.is_none() && protection.is_none()
                );
                let expected = if delivery == Delivery::Expiring {
                    Refusal::Expired
                } else {
                    Refusal::Revoked
                };
                assert!(matches!(refusals, (Some(cause), None, None) if cause == expected));
            }
            Delivery::ConstructorExpired => {
                let key = key.expect("construction preserves its actual canonical key");
                let previous = keys.insert(delivery, key);
                assert!(previous.is_none());
                let (command, sender, admission) =
                    native.expect("the unattempted original command remains");
                assert!(matches!(
                    command,
                    Some(Either::Right(CounterMessage::Increment))
                ));
                assert!(
                    matches!(sender, Attempt::Returned(Ok(_)))
                        && matches!(admission, Attempt::Unattempted)
                );
                assert!(encoding.is_none() && protection.is_none());
                assert!(matches!(refusals, (Some(Refusal::Expired), None, None)));
            }
        }
    }
    assert_eq!(keys.len(), 3);
    let initial_key = keys
        .get(&Delivery::Initial)
        .expect("Initial retains its actual key");
    let fresh_key = keys
        .get(&Delivery::Fresh)
        .expect("Fresh retains its actual key");
    let constructed_key = keys
        .get(&Delivery::ConstructorExpired)
        .expect("constructor expiry retains its actual key");
    assert_ne!(initial_key, fresh_key);
    assert_ne!(initial_key, constructed_key);
    assert_ne!(fresh_key, constructed_key);
    assert_eq!(clock.get(), Duration::from_secs(20));
    assert_eq!(trace.borrow().as_slice(), expected);
}
