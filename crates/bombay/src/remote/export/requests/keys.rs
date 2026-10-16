//! Actual declaration identity, including redeclaration; no forged request keys.
use super::{CounterNative, CounterOperation, CounterOutcome};
use crate::actors::ActorExt;
use crate::behavior::Never;
use crate::remote::counter::{Counter, CounterMessage, CounterValue};
use crate::remote::{ExportLimits, listening_counter_network};
use crate::{Application, Completion, RetirementAssessment};
use std::cell::Cell;
use std::num::NonZeroUsize;
use std::rc::Rc;
use std::time::Duration;
use tokio::runtime::Builder;
use tokio::time::timeout;

fn request_sequence(native: &CounterNative) -> Option<u64> {
    match native {
        CounterNative::Cold(CounterOperation::Increment(request_sequence)) => {
            Some(*request_sequence)
        }
        _ => None,
    }
}

#[test]
#[expect(
    clippy::too_many_lines,
    reason = "three actual declarations preserve their exact originals and retire every consumer before key/source oracles"
)]
fn request_keys_reject_other_exports_and_redeclarations_before_native_effects() {
    let limits = ExportLimits {
        queued_queries: NonZeroUsize::new(1).unwrap(),
        retained_queries: NonZeroUsize::new(1).unwrap(),
        retained_requests: NonZeroUsize::new(1).unwrap(),
    };
    let originals = [101_u64, 103, 107].map(|identity| {
        (
            vec![u8::try_from(identity).expect("fixture identity fits one byte")]
                .into_boxed_slice(),
            vec![109_u8].into_boxed_slice(),
            Rc::new(identity),
        )
    });
    let allocations = originals.each_ref().map(|(protected, proof, receipt)| {
        (protected.as_ptr(), proof.as_ptr(), Rc::as_ptr(receipt))
    });
    let host = Builder::new_multi_thread()
        .worker_threads(1)
        .enable_all()
        .build()
        .unwrap();
    let observations = host.block_on(async {
        let (execution, receiving, closing) = Application::new(Counter::new().stop_on_shutdown())
            .execute_remote(listening_counter_network(), move |application| async move {
                let target = application.root().address();
                let mut issuer = application.export::<_, CounterNative, Rc<u64>>(
                    application.root().established_recipient(), "bombay/keys/issuer", limits,
                    |left, right| **left == **right, |original| panic!("no queries: {original}"),
                ).await.unwrap_or_else(|_| panic!("actual declaration issuer"));
                let mut neighbor = application.export::<_, CounterNative, Rc<u64>>(
                    application.root().established_recipient(), "bombay/keys/neighbor", limits,
                    |left, right| **left == **right, |original| panic!("no queries: {original}"),
                ).await.unwrap_or_else(|_| panic!("actual declaration neighbor"));
                let [issuer_original, neighbor_original, replacement_original] = originals;
                let (issuer_key, issuer_duplicate) = issuer.retain(issuer_original.0, issuer_original.1, issuer_original.2,
                    CounterNative::Cold(CounterOperation::Increment(101)))
                    .unwrap_or_else(|_| panic!("issuer's first actual row"));
                let (neighbor_key, neighbor_duplicate) = neighbor.retain(neighbor_original.0, neighbor_original.1, neighbor_original.2,
                    CounterNative::Cold(CounterOperation::Increment(103)))
                    .unwrap_or_else(|_| panic!("neighbor's first actual row"));
                let issuer_own = issuer.request(&issuer_key).map(|(protected, proof, receipt, native)|
                    (protected.as_ptr(), proof.as_ptr(), protected.to_vec(), proof.to_vec(), Rc::as_ptr(receipt), **receipt, request_sequence(native)));
                let issuer_mutable = issuer.request_mut(&issuer_key).map(|(receipt, native, _)| (Rc::as_ptr(receipt), request_sequence(native)));
                let neighbor_own = neighbor.request(&neighbor_key).map(|(protected, proof, receipt, native)|
                    (protected.as_ptr(), proof.as_ptr(), protected.to_vec(), proof.to_vec(), Rc::as_ptr(receipt), **receipt, request_sequence(native)));
                let neighbor_mutable = neighbor.request_mut(&neighbor_key).map(|(receipt, native, _)| (Rc::as_ptr(receipt), request_sequence(native)));
                let constructors = Cell::new(0);
                let foreign = neighbor.request(&issuer_key).map(|_| ());
                let foreign_mutable = neighbor.request_mut(&issuer_key).map(|(receipt, native, interface)|
                    native.attempt(receipt, interface, &constructors, |receipt| Ok::<_, Never>(receipt)).unwrap());
                let neighbor_after = neighbor.request(&neighbor_key).map(|(protected, proof, receipt, native)|
                    (protected.as_ptr(), proof.as_ptr(), protected.to_vec(), proof.to_vec(), Rc::as_ptr(receipt), **receipt, request_sequence(native)));
                let neighbor_mutable_after = neighbor.request_mut(&neighbor_key).map(|(receipt, native, _)| (Rc::as_ptr(receipt), request_sequence(native)));
                let (issuer_undeclared, issuer_session, issuer_incoming, issuer_requests) = issuer.undeclare().await;
                let mut replacement = application.export::<_, CounterNative, Rc<u64>>(
                    application.root().established_recipient(), "bombay/keys/issuer", limits,
                    |left, right| **left == **right, |original| panic!("no queries: {original}"),
                ).await.unwrap_or_else(|_| panic!("actual redeclaration of issuer"));
                let (replacement_key, replacement_duplicate) = replacement.retain(
                    replacement_original.0, replacement_original.1, replacement_original.2,
                    CounterNative::Cold(CounterOperation::Increment(107)))
                    .unwrap_or_else(|_| panic!("replacement's first actual row"));
                let replacement_own = replacement.request(&replacement_key).map(|(protected, proof, receipt, native)|
                    (protected.as_ptr(), proof.as_ptr(), protected.to_vec(), proof.to_vec(), Rc::as_ptr(receipt), **receipt, request_sequence(native)));
                let replacement_mutable = replacement.request_mut(&replacement_key)
                    .map(|(receipt, native, _)| (Rc::as_ptr(receipt), request_sequence(native)));
                let stale = replacement.request(&issuer_key).map(|_| ());
                let stale_mutable = replacement.request_mut(&issuer_key).map(|(receipt, native, interface)|
                    native.attempt(receipt, interface, &constructors, |receipt| Ok::<_, Never>(receipt)).unwrap());
                let replacement_after = replacement.request(&replacement_key).map(|(protected, proof, receipt, native)|
                    (protected.as_ptr(), proof.as_ptr(), protected.to_vec(), proof.to_vec(), Rc::as_ptr(receipt), **receipt, request_sequence(native)));
                let replacement_mutable_after = replacement.request_mut(&replacement_key)
                    .map(|(receipt, native, _)| (Rc::as_ptr(receipt), request_sequence(native)));
                let mut client = application.client::<CounterValue>().unwrap();
                let read = client.try_send(CounterMessage::Read(client.recipient()));
                let value = timeout(Duration::from_secs(5), client.receive()).await.expect("actual FIFO value fence");
                let stopped = application.request_shutdown();
                let report = timeout(Duration::from_secs(5), application.lifecycle().retirement())
                    .await.expect("actual joined root report");
                let (neighbor_undeclared, neighbor_session, neighbor_incoming, neighbor_requests) = neighbor.undeclare().await;
                let (replacement_undeclared, replacement_session, replacement_incoming, replacement_requests) = replacement.undeclare().await;
                let mut retained = Vec::new();
                for (key, protected, proof, receipt, native) in issuer_requests.chain(neighbor_requests).chain(replacement_requests) {
                    let mut tail = Vec::new();
                    let outcome = match native {
                        CounterNative::Increment { mut client, admission, processed, .. } => {
                            client.close_admission();
                            while let Some(original) = client.receive().await { tail.push(original); }
                            drop(client);
                            CounterOutcome::Increment(admission, processed)
                        }
                        original => CounterOutcome::Uncompleted(original),
                    };
                    retained.push((key, protected, proof, receipt, outcome, tail));
                }
                let mut prefixes = Vec::new();
                for mut incoming in [issuer_incoming, neighbor_incoming, replacement_incoming] {
                    while let Some(original) = incoming.recv().await { prefixes.push(original); }
                }
                let unread = prefixes.len();
                drop((prefixes, issuer_session, neighbor_session, replacement_session));
                client.close_admission();
                let mut tail = Vec::new();
                while let Some(original) = client.receive().await { tail.push(original); }
                drop(client);
                move || {
                    assert_eq!(([foreign, foreign_mutable, stale, stale_mutable], constructors.get(), value.as_ref().map(|reply| (reply.from, reply.message))), ([None; 4], 0, Some((target, 0))));
                    for ((own, mutable), (identity, allocation)) in [issuer_own, neighbor_own, replacement_own, neighbor_after, replacement_after].into_iter()
                        .zip([issuer_mutable, neighbor_mutable, replacement_mutable, neighbor_mutable_after, replacement_mutable_after])
                        .zip([101_u64, 103, 107, 103, 107].into_iter()
                            .zip([allocations[0], allocations[1], allocations[2], allocations[1], allocations[2]])) {
                        assert_eq!(own, Some((allocation.0, allocation.1, vec![u8::try_from(identity).expect("fixture identity fits one byte")], vec![109], allocation.2, identity, Some(identity))));
                        assert_eq!(mutable, Some((allocation.2, Some(identity))));
                    }
                    assert!(issuer_duplicate.is_none() && neighbor_duplicate.is_none() && replacement_duplicate.is_none());
                    assert!(read.is_ok() && stopped.is_ok() && tail.is_empty());
                    assert_eq!(report.retirement(), RetirementAssessment::Established);
                    assert!(issuer_undeclared.is_ok() && neighbor_undeclared.is_ok() && replacement_undeclared.is_ok());
                    assert_eq!(unread, 0);
                    assert!(retained.iter().map(|row| &row.0).eq([&issuer_key, &neighbor_key, &replacement_key]));
                    for ((_, protected, proof, receipt, native, tail), (identity, allocation)) in retained.into_iter().zip([101_u64, 103, 107].into_iter().zip(allocations)) {
                        assert_eq!((protected.as_ptr(), proof.as_ptr(), Rc::as_ptr(&receipt)), allocation);
                        assert_eq!((protected.as_ref(), proof.as_ref(), *receipt), (&[u8::try_from(identity).expect("fixture identity fits one byte")][..], &[109][..], identity));
                        assert!(tail.is_empty());
                        assert!(matches!(native, CounterOutcome::Uncompleted(CounterNative::Cold(CounterOperation::Increment(request_sequence))) if request_sequence == identity));
                    }
                }
            }).unwrap_or_else(|_| panic!("actual entered remote host"));
        execution.await;
        let (outcome, native, notifications, ()) = receiving.await;
        let closed = closing.await;
        let (observations, cleanup) = outcome.try_into_completed().unwrap_or_else(|_| panic!("actual key scope Work completes"));
        move || {
            assert!(cleanup.is_ok());
            let notifications = notifications.unwrap();
            assert!(notifications.termination.is_ok() && notifications.retirement.is_ok());
            assert_eq!(native.unwrap().1.completion(), Some(&Completion::Stopped));
            let (joined, closed) = closed.unwrap().unwrap_or_else(|_| panic!("actual connection opens"));
            assert!(joined.is_ok() && matches!(closed, Ok(Ok(()))));
            observations();
        }
    });
    drop(host);
    observations();
}
