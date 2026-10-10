use behavior_actors::{
    EstablishedShutdownResolved, ShutdownEstablished, ShutdownId, ShutdownRequested,
};
use bombay::behavior::{
    ActiveTurn, Behavior, BehaviorBase, Births, ChildCreationOutcome, ChildHead,
    ClassifySettlement, CreateChild, CreationEvent, CreationSequence, CreationSettlement,
    CreationSettlements, Creations, EstablishedActor, EstablishedDelivery, EventLayer, Here,
    Ingress, InitializationTurn, InterpretInstalledActor, InterpreterRequests, ItemSettlement,
    MessageProtocol, NoSends, SendLayer, SettledItem, SettlementStatus,
};
use bombay::prelude::*;
use bombay::{
    ActorFailureAssessment, ActorNotificationReceipts, ApplicationOutcome, InstalledActor,
    ProjectTerminal, RetirementAssessment,
};
use core::{
    future::Future,
    task::{Context, Poll, Waker},
};
use tokio::runtime::Builder;

type Worker = StopOnShutdown<ChildLedger>;
type CreatorChildren = Worker;
type CreatorEvent = EventLayer<
    EstablishedShutdownResolved<ChildLedger>,
    CreationEvent<MailAddr, CreatorChildren, CreatorCommand>,
>;
type CreatorSends = SendLayer<
    InterpreterRequests<ShutdownEstablished<Worker, Here>>,
    Vec<EstablishedDelivery<ServiceReplies>>,
>;
type ChildCreations = <Births<CreatorChildren> as CreationSettlements<MailAddr>>::Settlements;
type ServiceReplies = MessageProtocol<MailAddr, ServiceReply>;

enum ServiceReply {
    Exported(EstablishedActor<Worker>, EstablishedActor<Worker>),
    Processed(u64),
    Shutdown(EstablishedShutdownResolved<ChildLedger>),
}
enum ChildCommand {
    Inspect(EstablishedRecipient<ServiceReplies>),
}
struct ChildLedger {
    amount: u64,
}
#[bombay::actor(sends = pub(crate) { replies: Vec<EstablishedDelivery<ServiceReplies>>, })]
impl ChildLedger {
    fn receive(&mut self, _: MailAddr, command: ChildCommand) -> BehaviorActed<Self> {
        let ChildCommand::Inspect(reply) = command;
        Ok(Actions::cont().send_replies(EstablishedDelivery::new(
            reply,
            ServiceReply::Processed(self.amount),
        )))
    }
}

enum CreatorCommand {
    Export(EstablishedRecipient<ServiceReplies>),
    Stop(ShutdownEstablished<Worker, Here>),
}
#[derive(Default)]
struct Creator {
    returned_creations: Vec<ChildCreations>,
    committed_children: Vec<EstablishedActor<Worker>>,
    service_reply: Option<EstablishedRecipient<ServiceReplies>>,
}
impl Protocol for Creator {
    type Addr = MailAddr;
    type Msg = CreatorCommand;
}
impl BehaviorBase for Creator {
    type Base = Self;
    fn base(&self) -> &Self {
        self
    }
}
impl Behavior for Creator {
    type Protocol = Self;
    type Event = CreatorEvent;
    type Sends = CreatorSends;
    type Ph = Never;
    type Error = Never;
    type Birth = Births<CreatorChildren>;
    fn init(&mut self, _: InitializationTurn) -> BehaviorActed<Self> {
        let mut creation_sequence = CreationSequence::new();
        let target_creation = creation_sequence
            .issue()
            .expect("the actual creator issues A's correlation");
        let sibling_creation = creation_sequence
            .issue()
            .expect("the actual creator issues B's correlation");
        let children = Creations::one(CreateChild::birth(
            target_creation,
            ChildLedger { amount: 11 }.stop_on_shutdown(),
        ))
        .and(CreateChild::birth(
            sibling_creation,
            ChildLedger { amount: 17 }.stop_on_shutdown(),
        ));
        Ok(Actions::create(children))
    }
    fn transition(&mut self, _: ActiveTurn, event: Self::Event) -> BehaviorActed<Self> {
        match event {
            EventLayer::Owned(resolved) => {
                let reply = self
                    .service_reply
                    .as_ref()
                    .expect("the actual service owns its reply capability")
                    .clone();
                let sends = SendLayer::new(
                    InterpreterRequests::new(Vec::new()),
                    vec![EstablishedDelivery::new(
                        reply,
                        ServiceReply::Shutdown(resolved),
                    )],
                );
                Ok(Actions::new(sends, Creations::empty(), Step::Continue))
            }
            EventLayer::Inner(CreationEvent::Settlements(settled)) => {
                let original = settled.into_settlement();
                let CreationSettlement::Settled(children) = &original else {
                    panic!("actual creation traversal")
                };
                for child in children {
                    let SettledItem::Attempted(ItemSettlement::Accepted(
                        ChildCreationOutcome::Established(committed),
                    )) = child
                    else {
                        panic!("actual established children")
                    };
                    self.committed_children.push(committed.actor());
                }
                self.returned_creations.push(original);
                Ok(Actions::cont())
            }
            EventLayer::Inner(CreationEvent::User(user)) => {
                let command = user.message;
                match command {
                    CreatorCommand::Export(reply) => {
                        let [target_actor, sibling_actor] = self.committed_children.as_slice()
                        else {
                            panic!("both actual child exports")
                        };
                        self.service_reply = Some(reply.clone());
                        let sends = SendLayer::new(
                            InterpreterRequests::new(Vec::new()),
                            vec![EstablishedDelivery::new(
                                reply,
                                ServiceReply::Exported(target_actor.clone(), sibling_actor.clone()),
                            )],
                        );
                        Ok(Actions::new(sends, Creations::empty(), Step::Continue))
                    }
                    CreatorCommand::Stop(request) => Ok(Actions::cont().with_send(request)),
                }
            }
        }
    }
}

struct ChildService {
    installed: Option<InstalledActor<Worker>>,
}
impl InterpretInstalledActor<Worker> for ChildService {
    type Output = ();
    fn interpret_actor(&mut self, installed: InstalledActor<Worker>) {
        let previous = self.installed.replace(installed);
        assert!(previous.is_none(), "one actual service export attachment");
    }
}

#[expect(
    clippy::large_enum_variant,
    clippy::type_complexity,
    reason = "the owning derive retains complete native actor products; this finite trace introduces no boxing or alias policy"
)]
#[derive(TerminalProjection)]
enum ChildTerminals {
    Root {
        origin: RootOrigin<StopOnShutdown<Creator>>,
        terminal: ActorRetirement<
            StopOnShutdown<Creator>,
            Self,
            (
                Vec<ChildFailure<ChildOrigin<Creator, ChildHead>, Worker>>,
                (),
            ),
        >,
    },
    #[structural_child]
    Child {
        origin: ChildOrigin<Creator, ChildHead>,
        terminal: ActorRetirement<Worker, Self, ()>,
    },
}

#[expect(
    clippy::too_many_lines,
    reason = "one actual Application trace and its complete native custody oracle remain contiguous"
)]
#[test]
fn service_requests_exact_child_stop_and_waits_for_joined_report() {
    let host = Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("the explicit local host");
    let originals = host
        .block_on(
            Application::new(Creator::default().stop_on_shutdown())
                .run_with::<ChildTerminals, _, _, _, _, _>(|application| async move {
                    let interface = application.interface(());
                    let mut replies = interface
                        .external::<ServiceReplies>()
                        .expect("the actual service mailbox");
                    let creator = application.root().established_recipient();
                    let creator_address = application.root().address();
                    let exported = replies
                        .send(&creator, CreatorCommand::Export(replies.recipient()))
                        .await;
                    assert!(exported.is_ok());
                    let exports = replies
                        .receive()
                        .await
                        .expect("real creator Actions exports");
                    assert_eq!(exports.from, creator_address);
                    let ServiceReply::Exported(target_actor, sibling_actor) = exports.message
                    else {
                        panic!("both full actor proofs")
                    };
                    let mut service = ChildService { installed: None };
                    target_actor.clone().interpret_actor(&mut service);
                    let installed = service
                        .installed
                        .as_ref()
                        .expect("the actual installed target");
                    let mut retirement = Box::pin(installed.retirement());
                    let mut borrowing = Box::pin(async { retirement.as_mut().await });
                    let pending = borrowing
                        .as_mut()
                        .poll(&mut Context::from_waker(Waker::noop()));
                    assert!(matches!(pending, Poll::Pending));
                    drop(borrowing);
                    let target_inspect = replies
                        .send(
                            &target_actor.recipient(),
                            ChildCommand::Inspect(replies.recipient()),
                        )
                        .await;
                    assert!(target_inspect.is_ok());
                    let target_before = replies
                        .receive()
                        .await
                        .expect("A performs useful typed work");
                    let target_address = target_before.from;
                    let ServiceReply::Processed(amount) = target_before.message else {
                        panic!("A's typed reply")
                    };
                    assert_eq!(amount, 11);
                    let sibling_recipient = sibling_actor.recipient();
                    let before = replies
                        .send(
                            &sibling_recipient,
                            ChildCommand::Inspect(replies.recipient()),
                        )
                        .await;
                    assert!(before.is_ok());
                    let before = replies.receive().await.expect("B progresses before A stop");
                    let ServiceReply::Processed(amount) = before.message else {
                        panic!("B's typed reply")
                    };
                    assert_eq!(amount, 17);
                    let sibling_address = before.from;
                    let stop = ShutdownEstablished::new(
                        ShutdownId(103),
                        target_actor,
                        Ingress::<ShutdownRequested, Here>::new(),
                    );
                    let submitted = replies.send(&creator, CreatorCommand::Stop(stop)).await;
                    assert!(submitted.is_ok());
                    let resolution = replies
                        .receive()
                        .await
                        .expect("the actual shutdown source returns through parent Actions");
                    assert_eq!(resolution.from, creator_address);
                    let ServiceReply::Shutdown(EstablishedShutdownResolved::Accepted {
                        id, ..
                    }) = resolution.message
                    else {
                        panic!("actual exact shutdown accepted")
                    };
                    assert_eq!(id, ShutdownId(103));
                    let report = retirement.await;
                    assert_eq!(report.retirement(), RetirementAssessment::Established);
                    assert_eq!(report.failures(), ActorFailureAssessment::NoFailuresFound);
                    let after = replies
                        .send(
                            &sibling_recipient,
                            ChildCommand::Inspect(replies.recipient()),
                        )
                        .await;
                    assert!(after.is_ok());
                    let after = replies
                        .receive()
                        .await
                        .expect("B progresses after A joined report");
                    let ServiceReply::Processed(amount) = after.message else {
                        panic!("B's typed reply")
                    };
                    assert_eq!(amount, 17);
                    assert_eq!(after.from, sibling_address);
                    let stopped = application.lifecycle().request_shutdown();
                    assert_eq!(stopped, Ok(()));
                    (target_address, sibling_address)
                }),
        )
        .unwrap_or_else(|failed| {
            drop(failed);
            panic!("the exact native Application remains available")
        });
    drop(host);
    let (
        ApplicationOutcome::Completed {
            output: (target_address, sibling_address),
            cleanup: Ok(()),
        },
        Ok((origin, native)),
        Ok(ActorNotificationReceipts {
            termination: Ok(()),
            retirement: Ok(()),
        }),
    ) = originals
    else {
        panic!("all original actor and notification products remain independent")
    };
    let terminal: ChildTerminals = ProjectTerminal::project(origin, native);
    let ChildTerminals::Root {
        origin,
        terminal:
            ActorRetirement::Completed {
                behavior,
                interpretation,
                source,
                settlements,
                control,
                user,
                descendants,
                child_failures: (child_failures, ()),
                capability_failures,
                additional_failures,
                received_interpretation,
                received_source,
                source_index,
                acquired_ingress,
                retirement_failures,
                terminal_report,
                unread_owner_cancellation,
                completion,
            },
    } = terminal
    else {
        panic!("the full original root completes")
    };
    assert_eq!(origin.address(), MailAddr::APPLICATION_ROOT);
    assert_eq!(behavior.base().committed_children.len(), 2);
    assert_eq!(behavior.base().returned_creations.len(), 1);
    let CreationSettlement::Settled(creation_batch) = &behavior.base().returned_creations[0] else {
        panic!("the exact full creation batch remains owned")
    };
    assert_eq!(creation_batch.len(), 2);
    assert!(interpretation.is_none() && source.is_none() && control.is_empty() && user.is_empty());
    assert!(
        child_failures.is_empty()
            && capability_failures.is_empty()
            && additional_failures.is_empty()
    );
    assert!(
        received_interpretation.is_none() && received_source.is_none() && source_index.is_none()
    );
    assert!(
        acquired_ingress.is_none()
            && retirement_failures.is_empty()
            && terminal_report.is_none()
            && unread_owner_cancellation.is_none()
    );
    assert_eq!(completion, Completion::Stopped);
    let [settlement] = settlements.as_slice() else {
        panic!("the final root Stop settlement")
    };
    assert_eq!(settlement.settlement_status(), SettlementStatus::Accepted);
    let CreationSettlement::Settled(creations) = &settlement.creations else {
        panic!("the final creation product remains total")
    };
    assert!(creations.is_empty());
    assert!(matches!(settlement.sends.owned, NoSends));
    assert!(settlement.sends.inner.owned.is_empty() && settlement.sends.inner.inner.is_empty());
    assert!(matches!(settlement.become_, Step::Stop(_)));
    assert_eq!(descendants.len(), 2);
    for child in &descendants {
        let ChildTerminals::Child { origin, terminal } = child else {
            panic!("both original children remain native")
        };
        match terminal {
            ActorRetirement::Completed {
                completion,
                behavior,
                ..
            } => {
                assert_eq!(behavior.base().amount, 11);
                assert_eq!(origin.address(), target_address);
                assert_eq!(*completion, Completion::Stopped);
            }
            ActorRetirement::OwnerCancelled { behavior, .. } => {
                assert_eq!(behavior.base().amount, 17);
                assert_eq!(origin.address(), sibling_address);
            }
            _ => panic!("A stopped; only B was cancelled during later parent cleanup"),
        }
        assert_child_native(terminal);
    }
}

fn assert_child_native(terminal: &ActorRetirement<Worker, ChildTerminals, ()>) {
    let (ActorRetirement::Completed {
        behavior: _,
        interpretation,
        source,
        settlements,
        control,
        user,
        descendants,
        child_failures: (),
        capability_failures,
        additional_failures,
        received_interpretation,
        received_source,
        source_index,
        acquired_ingress,
        retirement_failures,
        terminal_report,
        unread_owner_cancellation,
        completion: _,
    }
    | ActorRetirement::OwnerCancelled {
        behavior: _,
        interpretation,
        source,
        settlements,
        control,
        user,
        descendants,
        child_failures: (),
        capability_failures,
        additional_failures,
        received_interpretation,
        received_source,
        source_index,
        acquired_ingress,
        retirement_failures,
        terminal_report,
        unread_owner_cancellation,
    }) = terminal
    else {
        panic!("the acquired original child phase")
    };
    assert!(
        interpretation.is_none()
            && source.is_none()
            && control.is_empty()
            && user.is_empty()
            && descendants.is_empty()
    );
    assert!(capability_failures.is_empty() && additional_failures.is_empty());
    assert!(
        received_interpretation.is_none() && received_source.is_none() && source_index.is_none()
    );
    // The Driver moved the original cancellation into Completion before retirement.
    assert!(acquired_ingress.is_none());
    assert!(
        retirement_failures.is_empty()
            && terminal_report.is_none()
            && unread_owner_cancellation.is_none()
    );
    if matches!(terminal, ActorRetirement::Completed { .. }) {
        assert_eq!(settlements.len(), 1, "the final Stop product survives");
    }
    for settlement in settlements {
        assert_eq!(settlement.settlement_status(), SettlementStatus::Accepted);
        assert!(settlement.creations.is_empty());
        assert!(matches!(settlement.sends.owned, NoSends));
        assert!(settlement.sends.inner.replies.is_empty());
        match terminal {
            ActorRetirement::Completed { .. } => {
                assert!(matches!(settlement.become_, Step::Stop(_)));
            }
            ActorRetirement::OwnerCancelled { .. } => {
                assert!(matches!(settlement.become_, Step::Continue));
            }
            _ => unreachable!("only the acquired terminal phases"),
        }
    }
}
