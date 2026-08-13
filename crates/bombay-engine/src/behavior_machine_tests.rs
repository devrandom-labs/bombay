//! Compatibility tests proving `BehaviorMachine` composes with
//! `SerializedExecutor`. These are NON-PRODUCTION evidence: the production
//! seat is `ExclusiveExecutor`; `SerializedExecutor` is exercised here only
//! to prove the `Machine` adapter is executor-agnostic.
//!
//! Every test here exercises the production `bombay_transition::Machine` and
//! `bombay_machine_executor::SerializedExecutor` types through
//! [`BehaviorMachine`]. These are not toy replicas — they are the real
//! contracts the engine crate claims to compose.

#[cfg(test)]
mod serialized_executor {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};

    use behavior::{
        Actions, Behavior, BehaviorActed, Delivery, MailAddr, Never, NoBirths, SendAlgebra, User,
    };
    use bombay_machine_executor::{Machine, SerializedExecutor, TurnOutcome};
    use bombay_transition::{Topology, ValidatedTopology, Vertex, VertexId};

    use crate::BehaviorMachine;

    /// A counter behavior: each event increments the internal value.
    struct Counter {
        value: Arc<AtomicUsize>,
    }

    impl Behavior for Counter {
        type Addr = MailAddr;
        type Msg = usize;
        type Event = User<MailAddr, usize>;
        type Sends = Vec<Delivery<MailAddr, Never>>;
        type Ph = Never;
        type Error = Never;
        type Birth = NoBirths;

        fn init(&mut self) -> BehaviorActed<Self> {
            Ok(Actions::cont())
        }

        fn transition(&mut self, event: Self::Event) -> BehaviorActed<Self> {
            self.value.fetch_add(event.message, Ordering::Relaxed);
            Ok(Actions::cont())
        }
    }

    fn descriptive_topology() -> ValidatedTopology {
        const READY: VertexId = VertexId(0);
        static VERTICES: &[Vertex] = &[Vertex {
            id: READY,
            label: "ready",
        }];
        const TOPOLOGY: Topology = Topology {
            name: "counter",
            initial: READY,
            vertices: VERTICES,
            transitions: &[],
        };
        TOPOLOGY.validated().unwrap()
    }

    /// Prove: `BehaviorMachine` implements Machine and runs under
    /// `SerializedExecutor` with synchronous output handling.
    #[test]
    fn behavior_machine_runs_under_serialized_executor() {
        let value = Arc::new(AtomicUsize::new(0));
        let machine = BehaviorMachine::with_topology(
            Counter {
                value: Arc::clone(&value),
            },
            descriptive_topology(),
        );
        let executor = SerializedExecutor::new(machine);
        let handled = AtomicUsize::new(0);

        for message in [1, 2, 3] {
            let receipt = executor
                .submit(
                    User::new(MailAddr(7), message),
                    &|_actions: BehaviorActed<Counter>| {
                        handled.fetch_add(1, Ordering::Relaxed);
                    },
                )
                .unwrap();
            assert_eq!(receipt.wait(), TurnOutcome::Completed);
        }

        assert_eq!(handled.load(Ordering::Relaxed), 3);
        assert_eq!(value.load(Ordering::Relaxed), 6);
    }

    /// Prove: a stop decision is observable through the output.
    #[test]
    fn serialized_executor_observes_stop() {
        struct StopAfterOne {
            fired: bool,
        }

        impl Behavior for StopAfterOne {
            type Addr = MailAddr;
            type Msg = usize;
            type Event = User<MailAddr, usize>;
            type Sends = Vec<Delivery<MailAddr, Never>>;
            type Ph = Never;
            type Error = Never;
            type Birth = NoBirths;

            fn init(&mut self) -> BehaviorActed<Self> {
                Ok(Actions::cont())
            }

            fn transition(&mut self, _event: Self::Event) -> BehaviorActed<Self> {
                self.fired = true;
                Ok(Actions::new(
                    Self::Sends::empty(),
                    Vec::new(),
                    behavior::Step::Stop(behavior::Exit::Normal),
                ))
            }
        }

        let machine =
            BehaviorMachine::with_topology(StopAfterOne { fired: false }, descriptive_topology());
        let executor = SerializedExecutor::new(machine);

        let stopped = AtomicUsize::new(0);
        let receipt = executor
            .submit(User::new(MailAddr(0), 0), &|actions: BehaviorActed<
                StopAfterOne,
            >| {
                let actions = actions.unwrap();
                assert!(matches!(actions.become_, behavior::Step::Stop(_)));
                stopped.fetch_add(1, Ordering::Relaxed);
            })
            .unwrap();
        assert_eq!(receipt.wait(), TurnOutcome::Completed);
        assert_eq!(stopped.load(Ordering::Relaxed), 1);
    }

    /// Prove: `BehaviorMachine` preserves the declared topology through
    /// `Machine::describe`.
    #[test]
    fn behavior_machine_preserves_topology_through_describe() {
        use bombay_transition::Structure;

        struct NameCollector {
            names: Vec<&'static str>,
        }

        impl Structure for NameCollector {
            type Output = ();

            fn base(&mut self, topology: Topology) {
                self.names.push(topology.name);
            }

            fn then(&mut self, _first: (), _second: ()) {}
            fn product(&mut self, _left: (), _right: ()) {}
            fn routed(&mut self, _left: (), _right: ()) {}
        }

        let machine = BehaviorMachine::with_topology(
            Counter {
                value: Arc::new(AtomicUsize::new(0)),
            },
            descriptive_topology(),
        );
        let mut collector = NameCollector { names: Vec::new() };
        machine.describe(&mut collector);
        assert_eq!(collector.names, vec!["counter"]);
    }
}
