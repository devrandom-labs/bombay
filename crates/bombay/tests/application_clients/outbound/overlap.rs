//! The caller consumes a checked processing receipt before acknowledging it.

use super::super::counter::CounterIncremented;
use super::custody::completed_actor;
use super::requester::{CounterProofPurpose, CounterRequests, CounterServiceInput};
use super::{Counter, CounterMessage, CounterValue};
use bombay::actors::ActorExt;
use bombay::behavior::{Step, User};
use bombay::composition::MessageAdapter;
use bombay::prelude::Activate;
use bombay::remote::{
    Attempt, Client, ClientLimits, Export, ExportLimits, Metadata, RefusalRetention, reply,
};
use bombay::{
    ActorFailureAssessment, Application, ApplicationScope, MailAddr, RetirementAssessment,
};
use futures_util::future::Either;
use proptest::arbitrary::any;
use proptest::strategy::Strategy;
use proptest::test_runner::{Config, FileFailurePersistence, RngSeed, TestError, TestRunner};
use proptest::{collection, sample};
use serde::{Deserialize, Serialize};
use std::cell::{Cell, RefCell};
use std::future::{Future, poll_fn};
use std::num::NonZeroUsize;
use std::pin::pin;
use std::task::Poll;
use std::time::{Duration, Instant};
use tokio::runtime::Builder;
use tokio::sync::{mpsc, oneshot};

#[path = "../protocol.rs"]
#[expect(
    clippy::duplicate_mod,
    reason = "independent public cohorts compile the same nominal provider and its existing TLS inputs"
)]
#[expect(
    dead_code,
    reason = "the same generic finite provider also owns read, Ledger and scheduled controls outside this fixed increment cohort"
)]
mod protocol;
use protocol::{
    Binding, EndpointRole, ProtectedMessage, ProviderProfile, Purpose, ReadReply, ReadReplyRef,
    ReadRequest, ReadRequestRef, VerificationError, configuration, current_scope, limits, metadata,
    recorded, reserved_address,
};

#[derive(Clone, Copy, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
enum Operation {
    Increment,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Delivery {
    Fresh(u64),
    Repeat,
}

fn consume(input: Either<u64, CounterIncremented>) -> CounterServiceInput {
    match input {
        Either::Left(correlation) => CounterServiceInput::Verify(correlation),
        Either::Right(receipt) => {
            let CounterIncremented { request_sequence } = receipt;
            CounterServiceInput::ProtectedConsumed {
                correlation: request_sequence,
                purpose: CounterProofPurpose::IncrementProcessed,
            }
        }
    }
}

#[test]
fn caller_actions_preserve_submitted_and_processed_correlations() {
    let scope = ApplicationScope::new();
    let requests = scope
        .external::<CounterRequests>()
        .expect("the actual outgoing source supplies its established recipient");
    let destination = requests.recipient();
    let sender = requests.address();
    let initialized = MessageAdapter::new(destination.clone(), consume)
        .initialize()
        .expect("the caller initializes without an effect");
    let mut caller = initialized.behavior;
    let initialization = initialized.actions;

    assert!(initialization.sends.is_empty());
    assert!(initialization.creates.is_empty());
    assert_eq!(initialization.become_, Step::Continue);

    // The returned sequence deliberately differs from the submitted sequence:
    // acknowledgement consumes the supplied receipt, never a cached request ID.
    for (submitted, processed) in [(23, 79), (24, 83), (u64::MAX, 0)] {
        let request = caller
            .receive(sender, Either::Left(submitted))
            .expect("the caller accepts one submitted correlation");
        let [delivery] = request
            .sends
            .try_into()
            .unwrap_or_else(|_| panic!("one typed request is the complete send lane"));
        let CounterServiceInput::Verify(correlation) = delivery.message else {
            panic!("submitting a request cannot acknowledge an unconsumed reply");
        };

        assert_eq!(delivery.to, destination);
        assert_eq!(correlation, submitted);
        assert!(request.creates.is_empty());
        assert_eq!(request.become_, Step::Continue);

        let receipt = CounterIncremented {
            request_sequence: processed,
        };
        let acknowledgement = caller
            .receive(sender, Either::Right(receipt))
            .expect("the caller consumes the complete checked processing receipt");
        let [delivery] = acknowledgement
            .sends
            .try_into()
            .unwrap_or_else(|_| panic!("one typed acknowledgement is the complete send lane"));
        let CounterServiceInput::ProtectedConsumed {
            correlation,
            purpose,
        } = delivery.message
        else {
            panic!("a consumed processing receipt must remain an acknowledgement");
        };

        assert_eq!(delivery.to, destination);
        assert_eq!(correlation, processed);
        assert_eq!(purpose, CounterProofPurpose::IncrementProcessed);
        assert!(acknowledgement.creates.is_empty());
        assert_eq!(acknowledgement.become_, Step::Continue);
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[expect(
    clippy::large_futures,
    reason = "the bounded two-Node trace retains concrete native originals without another allocation or erased future"
)]
async fn overlapping_processed_increments_reuse_the_original_admission() {
    let deliveries = [Delivery::Fresh(24), Delivery::Repeat];
    tokio::time::timeout(Duration::from_secs(15), increments(&deliveries))
        .await
        .expect("both real Nodes and their original native queries finish before native expiry");
}

fn delivery_orders() -> Vec<Vec<Delivery>> {
    let permutations: &[&[u64]] = &[
        &[24],
        &[24, 25],
        &[25, 24],
        &[24, 25, 26],
        &[24, 26, 25],
        &[25, 24, 26],
        &[25, 26, 24],
        &[26, 24, 25],
        &[26, 25, 24],
    ];
    let mut orders = Vec::new();
    for fresh in permutations {
        for gap in 0..=fresh.len() {
            let mut deliveries = fresh
                .iter()
                .copied()
                .map(Delivery::Fresh)
                .collect::<Vec<_>>();
            deliveries.insert(gap, Delivery::Repeat);
            orders.push(deliveries);
        }
    }
    orders
}

#[test]
fn delivery_orders_cover_each_bounded_word_once() {
    let orders = delivery_orders();
    assert_eq!(orders.len(), 32);
    let mut groups = [0_usize; 3];
    let mut unique: Vec<&[Delivery]> = Vec::new();
    for deliveries in &orders {
        assert!((2..=4).contains(&deliveries.len()));
        let mut fresh = Vec::new();
        let mut repeats = 0;
        for delivery in deliveries {
            match delivery {
                Delivery::Fresh(sequence) => fresh.push(*sequence),
                Delivery::Repeat => repeats += 1,
            }
        }
        fresh.sort_unstable();
        let expected: &[u64] = match deliveries.len() {
            2 => &[24],
            3 => &[24, 25],
            4 => &[24, 25, 26],
            _ => unreachable!("each bounded word has two to four deliveries"),
        };
        let duplicate = unique.contains(&deliveries.as_slice());
        assert!(!duplicate, "a bounded word appears twice: {deliveries:?}");
        assert_eq!(repeats, 1);
        assert_eq!(fresh.as_slice(), expected);
        unique.push(deliveries);
        groups[deliveries.len() - 2] += 1;
    }
    assert_eq!(groups, [2, 6, 24]);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn all_delivery_orders_preserve_processed_increments() {
    let orders = delivery_orders();
    let mut completed = 0;
    for deliveries in orders {
        println!("Client delivery input: {deliveries:?}");
        tokio::time::timeout(Duration::from_secs(15), Box::pin(increments(&deliveries)))
            .await
            .expect("each actual two-Node word finishes before native expiry");
        completed += 1;
        println!("Client delivery cases completed={completed}; input={deliveries:?}");
    }
    assert_eq!(completed, 32);
}

#[test]
fn generated_delivery_orders_preserve_processed_increments() {
    let orders = (
        sample::select(vec![
            vec![Delivery::Fresh(24), Delivery::Repeat],
            vec![Delivery::Repeat, Delivery::Fresh(24)],
        ]),
        collection::vec(any::<sample::Index>(), 0..=2),
    )
        .prop_map(|(mut deliveries, insertions)| {
            for (sequence, insertion) in [25, 26].into_iter().zip(insertions) {
                let gap = insertion.index(deliveries.len() + 1);
                deliveries.insert(gap, Delivery::Fresh(sequence));
            }
            deliveries
        });
    let mut configuration = Config::with_source_file(file!());
    if let RngSeed::Random = configuration.rng_seed {
        configuration.rng_seed = RngSeed::Fixed(20_261_113);
    }
    if configuration.failure_persistence.is_some() {
        configuration.failure_persistence = Some(Box::new(FileFailurePersistence::WithSource(
            "proptest-regressions",
        )));
    }
    let cases = configuration.cases;
    let shrink_iterations = configuration.max_shrink_iters();
    println!(
        "generated Client configuration: algorithm={:?}; seed={}; cases={cases}; shrink_iterations={} (effective {shrink_iterations}); shrink_time_ms={} (0 is unlimited); source_file={:?}; fork={}; timeout_ms={}; persistence={:?}",
        configuration.rng_algorithm,
        configuration.rng_seed,
        configuration.max_shrink_iters,
        configuration.max_shrink_time,
        configuration.source_file,
        configuration.fork(),
        configuration.timeout(),
        configuration.failure_persistence,
    );
    assert!(cases > 0, "the generated campaign requires positive cases");
    assert!(
        shrink_iterations > 0,
        "the generated campaign requires shrinking"
    );
    assert!(
        !configuration.fork(),
        "the generated campaign requires in-process observations"
    );
    assert!(
        configuration.failure_persistence.is_some(),
        "the generated campaign requires standard persistence"
    );
    let mut runner = TestRunner::new(configuration);
    let invocations = Cell::new(0_u64);
    let completed = Cell::new(0_u64);
    let result = runner.run(&orders, |deliveries| {
        invocations.set(invocations.get() + 1);
        {
            let host = Builder::new_multi_thread()
                .worker_threads(2)
                .enable_all()
                .build()
                .expect("each generated word owns its ordinary Tokio host");
            host.block_on(async {
                tokio::time::timeout(Duration::from_secs(15), Box::pin(increments(&deliveries)))
                    .await
                    .expect("watchdog supplies no generated admission or cleanup credit");
            });
        }
        completed.set(completed.get() + 1);
        Ok(())
    });
    println!(
        "generated Client observations: invocations={}; completed={}",
        invocations.get(),
        completed.get(),
    );
    match result {
        Ok(()) => {
            assert_eq!(invocations.get(), completed.get());
            assert!(completed.get() >= u64::from(cases));
        }
        Err(TestError::Fail(reason, deliveries)) => panic!(
            "generated Client failure: {reason}; actual minimized deliveries: {deliveries:?}"
        ),
        Err(TestError::Abort(reason)) => panic!("generated Client campaign aborted: {reason}"),
    }
}

#[expect(
    clippy::too_many_lines,
    reason = "the fixed public two-Node trace keeps all original inputs, independent Counter Read and joined custody together"
)]
#[expect(
    clippy::result_large_err,
    reason = "the owning command selector requires the complete original ReadRequest on rejection"
)]
async fn increments(deliveries: &[Delivery]) {
    let fresh = deliveries
        .iter()
        .filter_map(|delivery| match delivery {
            Delivery::Fresh(sequence) => Some(*sequence),
            Delivery::Repeat => None,
        })
        .collect::<Vec<_>>();
    let mut identities = fresh.clone();
    identities.sort_unstable();
    let repeats = deliveries
        .iter()
        .filter(|delivery| matches!(delivery, Delivery::Repeat))
        .count();
    assert!((1..=3).contains(&fresh.len()));
    assert_eq!(repeats, 1);
    assert_eq!(identities.as_slice(), &[24, 25, 26][..fresh.len()]);
    let repeat_row = 1 + deliveries
        .iter()
        .position(|delivery| matches!(delivery, Delivery::Repeat))
        .expect("the validated word contains one repeated request");
    let expected_requests = [23]
        .into_iter()
        .chain(deliveries.iter().map(|delivery| match delivery {
            Delivery::Fresh(sequence) => *sequence,
            Delivery::Repeat => 23,
        }))
        .collect::<Vec<_>>();
    let expected_effects = [23]
        .into_iter()
        .chain(fresh.iter().copied())
        .collect::<Vec<_>>();
    let expected_acknowledgements = fresh.iter().copied().chain([23, 23]).collect::<Vec<_>>();
    let expected_value = u64::try_from(expected_effects.len()).unwrap();
    let query_count = expected_requests.len();
    let address = reserved_address();
    let maximum = NonZeroUsize::new(query_count).unwrap();
    let records = &RefCell::new(Vec::<ProtectedMessage>::new());
    let claims = &RefCell::new(Vec::<Metadata>::new());
    let encoded = &RefCell::new(Vec::new());
    let protected = &RefCell::new(Vec::new());
    let verified = &RefCell::new(Vec::new());
    let checked = &RefCell::new(Vec::new());
    let consumed = &RefCell::new(Vec::new());
    let request_origins = &RefCell::new(Vec::new());
    let constructed = &Cell::new(0);
    let emitted = &RefCell::new(Vec::new());
    let reply_protections = &RefCell::new(Vec::new());
    let held_transfers = &RefCell::new(Vec::new());
    let reply_policy_transfers = &RefCell::new(Vec::new());
    let caller_address = &Cell::new(None);
    let (entered, entering) = oneshot::channel();
    let entered = &RefCell::new(Some(entered));
    let (release, released) = oneshot::channel();
    let released = &RefCell::new(Some(released));
    let reply_policy = &RefCell::new(None::<oneshot::Sender<(Binding, u64)>>);
    let (acknowledged, mut acknowledgements) = mpsc::channel(query_count);
    let acknowledgements_sent = &RefCell::new(Vec::new());
    let (configured, configuration_ready) = oneshot::channel();
    let (ready, receiver_ready) = oneshot::channel();
    let (finished, receiver_finished) = oneshot::channel();
    let deadline = Instant::now() + Duration::from_secs(60);
    let binding = Binding {
        deployment: "two-node-read-study",
        caller: "caller",
        controller: "owner",
        actor: "counter",
        host: "receiver",
        protocol: "counter",
        target_assignment: "overlapping-counter",
        logical_session: 317,
    };
    let mut receiver_network = configuration(&address, &EndpointRole::Receiver);
    receiver_network.max_retained_queries = maximum;
    let receiver = Application::new(Counter::new().stop_on_shutdown())
        .open_node(receiver_network)
        .await
        .unwrap_or_else(|_| panic!("the receiving Counter opens its actual TLS Session"));
    let receiver_id = receiver.runtime_id();
    let scope = ApplicationScope::new();
    let requests = scope.external::<CounterRequests>().unwrap();
    let source_address = requests.address();
    let caller = scope
        .with_root(MessageAdapter::new(requests.recipient(), consume).stop_on_shutdown())
        .open_node({
            let mut network = configuration(&address, &EndpointRole::Caller);
            network.max_retained_queries = maximum;
            network
        })
        .await
        .unwrap_or_else(|_| panic!("the caller opens a distinct actual TLS Session"));
    let caller_id = caller.runtime_id();
    let current = |receipt: &recorded::Receipt<Operation>, runtime: &str, target: MailAddr| {
        let actual = recorded::projection(receipt);
        let configured = claims.borrow();
        let expected = configured
            .iter()
            .find(|claim| claim.request_sequence == actual.bombay.request_sequence)
            .ok_or(VerificationError::Invalid)?;
        if recorded::identity(receipt).0 != &binding
            || runtime != receiver_id
            || expected.actor_generation != target.0
        {
            return Err(VerificationError::Invalid);
        }
        current_scope(
            actual,
            expected,
            &Operation::Increment,
            Purpose::Request,
            deadline,
        )
    };
    let export = Export::new(
        "bombay/overlap/increment",
        ExportLimits {
            queued_queries: maximum,
            retained_queries: maximum,
            retained_requests: maximum,
            diagnostics: maximum,
            refusals: RefusalRetention::Retain,
        },
        limits(),
        |bytes| serde_json::from_slice::<ReadRequest<Operation>>(bytes),
        reply(
            |request: ReadRequest<Operation>| match request.operation {
                Operation::Increment => Ok::<_, ReadRequest<Operation>>(move |reply_to| {
                    constructed.set(constructed.get() + 1);
                    CounterMessage::IncrementAndReply {
                        request_sequence: request.bombay.request_sequence,
                        reply_to,
                    }
                }),
            },
            |receipt: &recorded::Receipt<Operation>,
             original: &User<MailAddr, CounterIncremented>| {
                emitted
                    .borrow_mut()
                    .push((original.from, original.message.request_sequence));
                serde_json::to_vec(&ReadReplyRef {
                    bombay: &recorded::projection(receipt).bombay,
                    kind: Purpose::Processed,
                    operation: Operation::Increment,
                    value: &original.message.request_sequence,
                })
                .map(Vec::into_boxed_slice)
            },
        ),
        async |bytes: &[u8], proof: &[u8]| {
            let original = ProtectedMessage {
                protected: bytes.into(),
                proof: proof.into(),
            };
            let receipt = recorded::verify::<Operation>(&original, &records.borrow(), binding)
                .into_inner()?;
            let actual = recorded::projection(&receipt);
            request_origins.borrow_mut().push((
                actual.bombay.request_sequence,
                bytes.as_ptr(),
                proof.as_ptr(),
                actual.bombay.caller.as_ptr(),
                actual.bombay.runtime_generation.as_ptr(),
            ));
            Ok::<_, VerificationError>(receipt)
        },
        async |bytes: &[u8], receipt: &recorded::Receipt<Operation>| {
            let original: ReadReply<u64, Operation> =
                serde_json::from_slice(bytes).map_err(VerificationError::Declaration)?;
            if original.bombay != recorded::projection(receipt).bombay
                || original.kind != Purpose::Processed
                || original.operation != Operation::Increment
                || original.value != original.bombay.request_sequence
            {
                return Err(VerificationError::Invalid);
            }
            let proof = Box::<[u8]>::from([13, 17]);
            reply_protections
                .borrow_mut()
                .push((original.value, bytes.as_ptr(), proof.as_ptr()));
            records.borrow_mut().push(ProtectedMessage {
                protected: bytes.into(),
                proof: proof.clone(),
            });
            let gate = if original.value == 23 {
                released.borrow_mut().take()
            } else {
                None
            };
            if let Some(gate) = gate {
                let mut gate = pin!(gate);
                poll_fn(|context| {
                    let observation = gate.as_mut().poll(context);
                    if matches!(observation, Poll::Pending)
                        && let Some(entered) = entered.borrow_mut().take()
                    {
                        held_transfers
                            .borrow_mut()
                            .push(entered.send(original.value));
                    }
                    observation
                })
                .await
                .map_err(VerificationError::Unavailable)?;
            }
            Ok::<_, VerificationError>(proof)
        },
        |left, right| recorded::identity(left) == recorded::identity(right),
        &current,
        |receipt: &recorded::Receipt<Operation>, runtime: &str, target: MailAddr| {
            let permission = current(receipt, runtime, target);
            let sequence = recorded::identity(receipt).1;
            if sequence == 23
                && let Some(observed) = reply_policy.borrow_mut().take()
            {
                match &permission {
                    Ok(()) => reply_policy_transfers
                        .borrow_mut()
                        .push(observed.send((*recorded::identity(receipt).0, sequence))),
                    Err(_) => drop(observed),
                }
            }
            permission
        },
    );
    let mut receiver = receiver.exports(export, async |application| {
        let target = application.root().established_recipient();
        let target_address = application.actor_address(&target)?;
        for &sequence in &expected_effects {
            let mut declared = metadata(
                ProviderProfile::Recorded,
                "counter",
                caller_id.clone(),
                receiver_id.clone(),
            );
            declared.actor_generation = target_address.0;
            declared.request_sequence = sequence;
            claims.borrow_mut().push(declared);
        }
        let reader = application
            .client::<CounterValue>()
            .unwrap_or_else(|_| panic!("the independent FIFO Counter reader allocates"));
        configured
            .send(())
            .unwrap_or_else(|()| panic!("caller binding retains configuration receiving"));
        ready
            .send((reader, application.lifecycle()))
            .unwrap_or_else(|_| panic!("caller Work retains the actual independent reader"));
        Ok::<_, bombay::remote::ExportError>(target)
    });
    let client = Client::new(
        requests,
        "bombay/overlap/increment",
        ClientLimits {
            concurrent_requests: maximum,
            retained_requests: NonZeroUsize::new(2 * query_count + 1).unwrap(),
            retained_responses: NonZeroUsize::new(1).unwrap(),
        },
        limits(),
        Duration::from_secs(60),
        |original: &User<MailAddr, CounterServiceInput>, runtime: &str, target: MailAddr| {
            if original.from != target || runtime != caller_id {
                return Err(VerificationError::Invalid);
            }
            match &original.message {
                CounterServiceInput::Verify(sequence) => {
                    let configured = claims.borrow();
                    let declared = configured
                        .iter()
                        .find(|claim| claim.request_sequence == *sequence)
                        .ok_or(VerificationError::Invalid)?;
                    let bytes = serde_json::to_vec(&ReadRequestRef {
                        bombay: declared,
                        kind: Purpose::Request,
                        operation: Operation::Increment,
                    })
                    .map_err(VerificationError::Declaration)?
                    .into_boxed_slice();
                    encoded
                        .borrow_mut()
                        .push((original.from, *sequence, bytes.as_ptr()));
                    Ok(Some(bytes))
                }
                CounterServiceInput::ProtectedConsumed {
                    correlation,
                    purpose,
                } => {
                    if *purpose != CounterProofPurpose::IncrementProcessed {
                        return Err(VerificationError::Invalid);
                    }
                    // The original whole User remains in Client's row. Observe
                    // its actual actor-consumed payload before selecting no Get.
                    consumed
                        .borrow_mut()
                        .push((original.from, *correlation, *purpose));
                    acknowledgements_sent
                        .borrow_mut()
                        .push(acknowledged.try_send(*correlation));
                    Ok(None)
                }
                CounterServiceInput::ReportConsumed(_)
                | CounterServiceInput::ReadObserved { .. } => Err(VerificationError::Invalid),
            }
        },
        async |bytes: &[u8], original: &User<MailAddr, CounterServiceInput>| {
            let CounterServiceInput::Verify(sequence) = &original.message else {
                return Err(VerificationError::Invalid);
            };
            let proof = Box::<[u8]>::from([7, 11]);
            protected
                .borrow_mut()
                .push((*sequence, bytes.as_ptr(), proof.as_ptr()));
            records.borrow_mut().push(ProtectedMessage {
                protected: bytes.into(),
                proof: proof.clone(),
            });
            Ok::<_, VerificationError>(proof)
        },
        async |bytes: &[u8], proof: &[u8]| {
            let original = ProtectedMessage {
                protected: bytes.into(),
                proof: proof.into(),
            };
            let receipt = recorded::verify::<Operation>(&original, &records.borrow(), binding)
                .into_inner()?;
            let actual = recorded::projection(&receipt);
            verified.borrow_mut().push((
                actual.bombay.request_sequence,
                bytes.as_ptr(),
                proof.as_ptr(),
                actual.bombay.caller.as_ptr(),
            ));
            Ok::<_, VerificationError>(receipt)
        },
        |receipt: &recorded::Receipt<Operation>,
         bytes: &[u8],
         original: &User<MailAddr, CounterServiceInput>| {
            let CounterServiceInput::Verify(sequence) = &original.message else {
                return Err(VerificationError::Invalid);
            };
            let actual = recorded::projection(receipt);
            let reply: ReadReply<u64, Operation> =
                serde_json::from_slice(bytes).map_err(VerificationError::Declaration)?;
            if actual.bombay != reply.bombay
                || actual.kind != Purpose::Processed
                || reply.kind != Purpose::Processed
                || actual.operation != Operation::Increment
                || reply.operation != Operation::Increment
                || reply.value != *sequence
                || reply.bombay.request_sequence != *sequence
            {
                return Err(VerificationError::Invalid);
            }
            checked
                .borrow_mut()
                .push((original.from, *sequence, reply.value));
            Ok(Either::Right(CounterIncremented {
                request_sequence: reply.value,
            }))
        },
        |original: &User<MailAddr, CounterServiceInput>, runtime: &str, target: MailAddr| {
            if original.from == target && runtime == caller_id {
                Ok(())
            } else {
                Err(VerificationError::Invalid)
            }
        },
        |receipt: &recorded::Receipt<Operation>,
         original: &User<MailAddr, CounterServiceInput>,
         runtime: &str,
         target: MailAddr| {
            let CounterServiceInput::Verify(sequence) = &original.message else {
                return Err(VerificationError::Invalid);
            };
            let actual = recorded::projection(receipt);
            let configured = claims.borrow();
            let declared = configured
                .iter()
                .find(|claim| claim.request_sequence == *sequence)
                .ok_or(VerificationError::Invalid)?;
            if original.from != target
                || runtime != caller_id
                || recorded::identity(receipt).0 != &binding
            {
                return Err(VerificationError::Invalid);
            }
            current_scope(
                actual,
                declared,
                &Operation::Increment,
                Purpose::Processed,
                deadline,
            )
        },
    );
    let mut caller = caller.exports(client, async |application| {
        configuration_ready
            .await
            .map_err(VerificationError::Unavailable)?;
        let target = application.root().established_recipient();
        let actual = application
            .actor_address(&target)
            .map_err(|_| VerificationError::Invalid)?;
        caller_address.set(Some(actual));
        Ok::<_, VerificationError>(target)
    });
    let outgoing = caller.run_with(|application| async move {
        let (mut reader, receiver_lifecycle) = receiver_ready
            .await
            .expect("receiver binding supplies actual custody");
        let sender = application.sender().unwrap();
        let first = sender.send(Either::Left(23)).await;
        let held = entering.await;
        let mut submissions = Vec::new();
        let mut acknowledgements_received = Vec::new();
        let mut fresh_progress = Vec::new();
        let mut policy_observation = None;
        for delivery in deliveries {
            match *delivery {
                Delivery::Fresh(sequence) => {
                    submissions.push(sender.send(Either::Left(sequence)).await);
                    let arrived = loop {
                        match acknowledgements.recv().await {
                            Some(correlation) => {
                                // An omitted canonical lookup may already have
                                // delivered A. Retain it while waiting for this
                                // actual fresh correlation, without rejecting it.
                                acknowledgements_received.push(correlation);
                                if correlation == sequence {
                                    break Some(correlation);
                                }
                            }
                            None => break None,
                        }
                    };
                    fresh_progress.push(arrived);
                }
                Delivery::Repeat => {
                    let (observed, replay_observed) = oneshot::channel();
                    *reply_policy.borrow_mut() = Some(observed);
                    submissions.push(sender.send(Either::Left(23)).await);
                    policy_observation = Some(replay_observed.await);
                }
            }
        }
        let before_release = acknowledgements_received.clone();
        // Even an observation failure releases the sole first-A provider.
        let released = release.send(());
        while acknowledgements_received.len() < query_count {
            match acknowledgements.recv().await {
                Some(correlation) => acknowledgements_received.push(correlation),
                None => break,
            }
        }
        let read_admission = reader.send(CounterMessage::Read(reader.recipient())).await;
        let value = reader.receive().await;
        reader.close_admission();
        let read_tail = reader.receive().await;
        let receiver_shutdown = receiver_lifecycle.shutdown();
        let receiver_report = receiver_lifecycle.retirement().await;
        let finished = finished.send(());
        let caller_shutdown = application.shutdown();
        (
            first,
            held,
            submissions,
            fresh_progress,
            policy_observation,
            released,
            before_release,
            acknowledgements_received,
            read_admission,
            value,
            read_tail,
            receiver_shutdown,
            receiver_report,
            finished,
            caller_shutdown,
            sender.address(),
        )
    });
    let incoming = receiver.run_with(|_| receiver_finished);
    let (incoming, outgoing) = tokio::join!(incoming, outgoing);
    let incoming = incoming.unwrap_or_else(|_| panic!("actual receiving Node host entry"));
    let outgoing = outgoing.unwrap_or_else(|_| panic!("actual caller Node host entry"));
    let (export, receiver_bound, receiver_closed, receiver_poll, receiver_drop) = receiver
        .into_parts()
        .unwrap_or_else(|_| panic!("receiving Node owns actual withdrawal and close"));
    let (client, caller_bound, caller_closed, caller_poll, caller_drop) = caller
        .into_parts()
        .unwrap_or_else(|_| panic!("caller Node owns actual withdrawal and close"));
    let (incoming_work, incoming_report, incoming_native, incoming_notes, ()) =
        incoming.into_parts();
    let (outgoing_work, outgoing_report, outgoing_native, outgoing_notes, ()) =
        outgoing.into_parts();
    let (receiver_finished, incoming_cleanup) = incoming_work
        .try_into_completed()
        .unwrap_or_else(|_| panic!("receiving Work original survives its joined cleanup"));
    let (observations, outgoing_cleanup) = outgoing_work
        .try_into_completed()
        .unwrap_or_else(|_| panic!("all original caller observations survive joined cleanup"));
    let (
        first,
        held,
        submissions,
        fresh_progress,
        policy_observation,
        released,
        before_release,
        acknowledgements_received,
        read_admission,
        value,
        read_tail,
        receiver_shutdown,
        receiver_report,
        finished,
        caller_shutdown,
        work_sender,
    ) = observations;
    let value = value.expect("the independent actual FIFO Counter reply is acquired");

    // Both Node futures, original Work/native products and native closes are
    // acquired. This is the first semantic assertion, independent of replay labels.
    eprintln!(
        "deliveries={deliveries:?} acknowledgements_before_release={before_release:?} acknowledgements_received={acknowledgements_received:?} counter_read={} cleanup={}/{} close={}/{} poll_panic_absent={}/{} disposal_panic_absent={}/{} native_completed={}/{} reports={:?}/{:?} notifications={:?}/{:?}",
        value.message,
        incoming_cleanup.is_ok(),
        outgoing_cleanup.is_ok(),
        receiver_closed.is_ok(),
        caller_closed.is_ok(),
        receiver_poll.is_none(),
        caller_poll.is_none(),
        receiver_drop.is_none(),
        caller_drop.is_none(),
        incoming_native
            .as_ref()
            .is_ok_and(|(_, retirement)| retirement.completion().is_some()),
        outgoing_native
            .as_ref()
            .is_ok_and(|(_, retirement)| retirement.completion().is_some()),
        incoming_report
            .as_ref()
            .map(|report| (report.retirement(), report.failures())),
        outgoing_report
            .as_ref()
            .map(|report| (report.retirement(), report.failures())),
        incoming_notes
            .as_ref()
            .map(|notes| (notes.termination.is_ok(), notes.retirement.is_ok())),
        outgoing_notes
            .as_ref()
            .map(|notes| (notes.termination.is_ok(), notes.retirement.is_ok())),
    );
    assert_eq!(
        value.message, expected_value,
        "a replay cannot increment the actual Counter again"
    );

    assert!(incoming_cleanup.is_ok() && outgoing_cleanup.is_ok());
    assert!(receiver_closed.is_ok() && caller_closed.is_ok());
    assert!(
        receiver_poll.is_none()
            && receiver_drop.is_none()
            && caller_poll.is_none()
            && caller_drop.is_none()
    );
    assert!(first.is_ok() && read_admission.is_ok());
    assert_eq!(submissions.len(), deliveries.len());
    assert!(submissions.iter().all(Result::is_ok));
    assert_eq!(held, Ok(23));
    assert_eq!(
        fresh_progress,
        fresh.iter().copied().map(Some).collect::<Vec<_>>()
    );
    assert_eq!(before_release, fresh);
    assert_eq!(policy_observation, Some(Ok((binding, 23))));
    assert_eq!(acknowledgements_received, expected_acknowledgements);
    assert!(
        released.is_ok()
            && receiver_shutdown.is_ok()
            && caller_shutdown.is_ok()
            && finished.is_ok()
    );
    assert!(receiver_finished.is_ok() && read_tail.is_none());
    assert_ne!(caller_id, receiver_id);
    let incoming_report = incoming_report.expect("receiving report remains acquired");
    let outgoing_report = outgoing_report.expect("caller report remains acquired");
    assert_eq!(incoming_report, receiver_report);
    for report in [incoming_report, outgoing_report] {
        assert_eq!(report.retirement(), RetirementAssessment::Established);
        assert_eq!(report.failures(), ActorFailureAssessment::NoFailuresFound);
    }
    for notes in [incoming_notes, outgoing_notes] {
        let notes = notes.expect("both original notification receipts remain");
        assert!(notes.termination.is_ok() && notes.retirement.is_ok());
    }
    let (counter_origin, counter) = incoming_native.expect("the complete native Counter remains");
    let (adapter_origin, adapter) = outgoing_native.expect("the complete native caller remains");
    let _counter = completed_actor(counter_origin, counter);
    let _adapter = completed_actor(adapter_origin, adapter);
    assert_eq!(value.from, counter_origin.address());
    assert_eq!(caller_address.get(), Some(adapter_origin.address()));
    assert_ne!(work_sender, adapter_origin.address());
    assert!(matches!(receiver_bound, Some(Ok(_))) && matches!(caller_bound, Some(Ok(_))));
    let (_, _, startup, withdrawal, diagnostics, requests) = export.into_parts();
    assert!(matches!(startup, Some(Ok(()))) && matches!(withdrawal, Some(Ok(()))));
    assert!(diagnostics.is_empty());
    let rows = requests
        .into_iter()
        .map(|request| {
            request
                .try_into_parts()
                .unwrap_or_else(|_| panic!("joined producers relinquish each actual request"))
        })
        .collect::<Vec<_>>();
    assert_eq!(rows.len(), query_count);
    assert_eq!(rows[0].0, rows[repeat_row].0);
    for (index, row) in rows.iter().enumerate() {
        assert!(row.0.is_some());
        if index != repeat_row {
            for (previous, original) in rows[..index].iter().enumerate() {
                if previous != repeat_row {
                    assert_ne!(row.0, original.0);
                }
            }
        }
    }
    let request_origins = request_origins.borrow();
    let claims = claims.borrow();
    let reply_protections = reply_protections.borrow();
    assert_eq!(request_origins.len(), query_count);
    assert_eq!(constructed.get(), expected_effects.len());
    assert_eq!(
        emitted.borrow().as_slice(),
        expected_effects
            .iter()
            .map(|sequence| (counter_origin.address(), *sequence))
            .collect::<Vec<_>>()
    );
    assert_eq!(reply_protections.len(), expected_effects.len());
    let mut incoming_originals = Vec::new();
    let mut produced = Vec::new();
    let mut reply_addresses = Vec::new();
    for (index, row) in rows.into_iter().enumerate() {
        let (
            key,
            raw,
            receipt,
            decoded,
            native,
            encoding,
            protection,
            publications,
            refusals,
            panics,
        ) = row;
        let receipt = receipt
            .expect("actual verification remains")
            .unwrap_or_else(|_| panic!("finite exact provider accepts"));
        let actual = recorded::projection(&receipt);
        let sequence = expected_requests[index];
        let expected = claims
            .iter()
            .find(|claim| claim.request_sequence == sequence)
            .expect("the actual request retains its configured identity");
        let declared: ReadRequest<Operation> = serde_json::from_slice(&raw.0).unwrap();
        assert_eq!(actual.bombay, *expected);
        assert_eq!(actual.kind, Purpose::Request);
        assert_eq!(actual.operation, Operation::Increment);
        assert_eq!(recorded::identity(&receipt), (&binding, sequence));
        assert_eq!(declared.bombay, *expected);
        assert_eq!(declared.kind, Purpose::Request);
        assert_eq!(declared.operation, Operation::Increment);
        assert_eq!(raw.1.as_ref(), [7, 11]);
        assert_eq!(
            (
                sequence,
                raw.0.as_ptr(),
                raw.1.as_ptr(),
                actual.bombay.caller.as_ptr(),
                actual.bombay.runtime_generation.as_ptr()
            ),
            request_origins[index]
        );
        assert!(key.is_some() && decoded.is_none());
        assert!(matches!(refusals, (None, None, None)));
        assert!(matches!(panics, (None, None, None, None, None, None)));
        assert_eq!(publications.len(), 1);
        assert!(publications.iter().all(Result::is_ok));
        if index == repeat_row {
            assert!(native.is_none() && encoding.is_none() && protection.is_none());
        } else {
            let (constructor, client, admission, reply, tail) =
                native.expect("actual native increment owner remains");
            let reply = reply
                .expect("actual native receive returned")
                .expect("actual processed User remains");
            let Attempt::Returned(Ok(client)) = client else {
                panic!("native reply client construction succeeds");
            };
            let protected = protection
                .expect("actual reply protection remains")
                .unwrap_or_else(|_| panic!("finite protection succeeds"));
            let bytes = encoding
                .expect("actual encoding remains")
                .expect("processing receipt encodes");
            let encoded_reply: ReadReply<u64, Operation> = serde_json::from_slice(&bytes).unwrap();
            let snapshot = reply_protections
                .iter()
                .find(|entry| entry.0 == sequence)
                .expect("each actual processing receipt retains its own protection inputs");
            assert!(constructor.is_none() && matches!(admission, Attempt::Returned(Ok(()))));
            assert_eq!(
                (reply.from, reply.message.request_sequence),
                (counter_origin.address(), sequence)
            );
            assert!(matches!(tail.as_slice(), [None]));
            assert_ne!(client.address(), counter_origin.address());
            reply_addresses.push(client.address());
            assert_eq!(encoded_reply.bombay, *expected);
            assert_eq!(
                (
                    encoded_reply.kind,
                    encoded_reply.operation,
                    encoded_reply.value
                ),
                (Purpose::Processed, Operation::Increment, sequence)
            );
            assert_eq!((sequence, bytes.as_ptr(), protected.as_ptr()), *snapshot);
            assert_eq!(protected.as_ref(), [13, 17]);
            produced.push((sequence, bytes, protected));
        }
        incoming_originals.push(raw);
    }
    assert_eq!(reply_addresses.len(), expected_effects.len());
    for (index, address) in reply_addresses.iter().enumerate() {
        for previous in &reply_addresses[..index] {
            assert_ne!(address, previous);
        }
    }
    let (source, _, startup, withdrawal, joined, source_eof, rows, routes) = client.into_parts();
    let (listener, visible, listener_withdrawal) = routes;
    assert_eq!(source.address(), source_address);
    assert!(matches!(startup, Some(Ok(()))) && matches!(withdrawal, Some(Ok(()))));
    assert!(matches!(joined, Some(Ok(()))) && source_eof.is_some());
    assert!(
        matches!(listener, Some(Ok(())))
            && matches!(visible, Some(Ok(())))
            && matches!(listener_withdrawal, Some(Ok(())))
    );
    assert_eq!(rows.len(), 2 * query_count);
    assert_eq!(
        consumed.borrow().as_slice(),
        expected_acknowledgements
            .iter()
            .map(|correlation| (
                adapter_origin.address(),
                *correlation,
                CounterProofPurpose::IncrementProcessed,
            ))
            .collect::<Vec<_>>()
    );
    assert_eq!(
        checked.borrow().as_slice(),
        expected_acknowledgements
            .iter()
            .map(|correlation| (adapter_origin.address(), *correlation, *correlation,))
            .collect::<Vec<_>>()
    );
    assert_eq!(acknowledgements_sent.borrow().len(), query_count);
    assert!(acknowledgements_sent.borrow().iter().all(Result::is_ok));
    assert_eq!(held_transfers.borrow().len(), 1);
    assert!(held_transfers.borrow().iter().all(Result::is_ok));
    assert_eq!(reply_policy_transfers.borrow().len(), 1);
    assert!(reply_policy_transfers.borrow().iter().all(Result::is_ok));
    let mut requests = Vec::new();
    let mut acknowledged = Vec::new();
    for row in rows {
        let (
            original,
            refusal,
            encoding,
            protection,
            permission,
            capacity,
            query,
            callback,
            responses,
            eof,
            protection_panics,
            operation_panics,
            completed,
        ) = row;
        assert_eq!(original.from, adapter_origin.address());
        assert!(
            refusal.is_none() && permission.is_none() && capacity.is_none() && callback.is_none()
        );
        assert!(
            matches!(protection_panics, (None, None, None))
                && matches!(operation_panics, (None, None, None))
        );
        match original.message {
            CounterServiceInput::Verify(sequence) => {
                let encoding = encoding
                    .expect("actual selected encoding remains")
                    .unwrap_or_else(|_| panic!("request encodes"))
                    .expect("real Get selected");
                let protection = protection
                    .expect("actual acquired proof remains")
                    .unwrap_or_else(|_| panic!("request protects"));
                assert!(matches!(query, Attempt::Returned(Ok(()))) && eof.is_some());
                assert_eq!(responses.len(), 1);
                requests.push((sequence, encoding, protection, completed));
                for response in responses {
                    let raw = response.payload().flatten().copied().collect::<Vec<_>>();
                    let proof = response
                        .proof()
                        .expect("actual response attachment remains")
                        .flatten()
                        .copied()
                        .collect::<Vec<_>>();
                    let (cause, wire, shape, receipt, decoded, permission, panics) =
                        response.into_parts();
                    let (bytes, attachment) =
                        wire.expect("actual wire retained before verification");
                    let receipt = receipt
                        .expect("actual verified response remains")
                        .unwrap_or_else(|_| panic!("exact registered processed reply verifies"));
                    let actual = recorded::projection(&receipt);
                    let expected = claims
                        .iter()
                        .find(|claim| claim.request_sequence == sequence)
                        .expect("the actual response retains its configured identity");
                    let observed = verified.borrow();
                    let snapshot = observed
                        .iter()
                        .find(|entry| entry.1 == bytes.as_ptr())
                        .expect("the exact verification input was observed");
                    let reply: ReadReply<u64, Operation> = serde_json::from_slice(&bytes).unwrap();
                    let (_, emitted_bytes, emitted_proof) = produced
                        .iter()
                        .find(|(correlation, _, _)| *correlation == sequence)
                        .expect("the exact processed publication remains owned");
                    assert!(
                        cause.is_none() && matches!(shape, Some(Ok(()))) && permission.is_none()
                    );
                    assert!(matches!(panics, (None, None, None)));
                    assert!(matches!(
                        decoded,
                        Some(Ok(Either::Right(Attempt::Returned(Ok(())))))
                    ));
                    assert_eq!(bytes.as_ref(), raw);
                    assert_eq!(attachment.as_ref(), proof);
                    assert_eq!(attachment.as_ref(), [13, 17]);
                    assert_eq!(bytes, *emitted_bytes);
                    assert_eq!(attachment, *emitted_proof);
                    assert_eq!(actual.bombay, *expected);
                    assert_eq!(
                        (actual.kind, actual.operation),
                        (Purpose::Processed, Operation::Increment)
                    );
                    assert_eq!(
                        (
                            sequence,
                            bytes.as_ptr(),
                            attachment.as_ptr(),
                            actual.bombay.caller.as_ptr()
                        ),
                        *snapshot
                    );
                    assert_eq!(reply.bombay, *expected);
                    assert_eq!(
                        (reply.kind, reply.operation, reply.value),
                        (Purpose::Processed, Operation::Increment, sequence)
                    );
                    assert_eq!(recorded::identity(&receipt), (&binding, sequence));
                }
            }
            CounterServiceInput::ProtectedConsumed {
                correlation,
                purpose,
            } => {
                assert!(matches!(encoding, Some(Ok(None))));
                assert!(protection.is_none() && matches!(query, Attempt::Unattempted));
                assert!(responses.is_empty() && eof.is_none());
                acknowledged.push((correlation, purpose, completed));
            }
            CounterServiceInput::ReportConsumed(_) | CounterServiceInput::ReadObserved { .. } => {
                panic!("no unrelated requester protocol is introduced")
            }
        }
    }
    assert_eq!(requests.len(), query_count);
    assert_eq!(
        acknowledged
            .iter()
            .map(|(correlation, purpose, _completed)| (*correlation, *purpose))
            .collect::<Vec<_>>(),
        expected_acknowledgements
            .iter()
            .map(|correlation| (*correlation, CounterProofPurpose::IncrementProcessed,))
            .collect::<Vec<_>>()
    );
    assert_eq!(
        requests.iter().map(|request| request.0).collect::<Vec<_>>(),
        expected_requests
    );
    assert_eq!(requests[0].1, requests[repeat_row].1);
    assert_eq!(requests[0].2, requests[repeat_row].2);
    let encoded = encoded.borrow();
    let protected = protected.borrow();
    for (index, (sequence, bytes, proof, _completed)) in requests.iter().enumerate() {
        assert_eq!(
            (adapter_origin.address(), *sequence, bytes.as_ptr()),
            encoded[index]
        );
        assert_eq!(
            (*sequence, bytes.as_ptr(), proof.as_ptr()),
            protected[index]
        );
        assert_eq!(proof.as_ref(), [7, 11]);
        assert_eq!(*bytes, incoming_originals[index].0);
        assert_eq!(*proof, incoming_originals[index].1);
    }
}
