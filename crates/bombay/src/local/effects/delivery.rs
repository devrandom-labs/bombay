use crate::address::MailAddr;
use crate::entity::{EntityAdmission, EntityDefinition, NativeEntityHost};
use crate::local::children::{ChildAuthority, ChildBindingAt};
use crate::local::effects::ApplicationCapabilities;
use crate::local::endpoint::{ExtractLocalEndpoint, InstalledActor};
use crate::topology::Hosts;
use behavior::{
    ActionItem, Behavior, BehaviorMessage, ChildDelivery, ChildDeliveryReason, ChildHead,
    ChildInput, ChildInputIngress, ChildInputReason, CreationCorrelation, CreationId, Delivery,
    EstablishedActor, EstablishedDelivery, EstablishedRecipient, ExactDeliveryReason, Here,
    InjectEvent, InterpretItem, ItemSettlement, LogicalDeliveryReason, Never, Protocol, Recipient,
    ResolveChildOccurrence, ResolvedChild, ResolvedChildPosition,
};
use behavior_actors::atomic::{
    ActivationPlan, AssignWorker, Assignment, CustomerDelivery, DiagnosticAccepted,
    DiagnosticAction, InitializeWorker, ProxyControl, ProxyControlAdmission, ProxyOperation,
    StableProxy, WorkerInitializationOutcome, WorkerInitializationReport,
};
use behavior_actors::{ChildStopped, ReplyDelivery};
use communication::ControlClosed;
use core::convert::Infallible;
use core::future::Future;
use std::time::Instant;

impl<C, N, P, Bindings, Origins, Target, RootEvent, Path>
    InterpretItem<Delivery<Target>, RootEvent, Path>
    for ApplicationCapabilities<C, N, P, Bindings, Origins>
where
    C: Behavior<Protocol: Protocol<Addr = MailAddr>>,
    Target: Protocol<Addr = MailAddr>,
    Target::Msg: Send,
    N: Hosts<Target> + Send + Sync,
    Self: Send,
{
    #[expect(
        clippy::manual_async_fn,
        reason = "retain the original receiving-loan opaque future; an async function captures unused route tags and would require new lifetime bounds"
    )]
    fn interpret_item<'a>(
        &'a mut self,
        input: <Delivery<Target> as ActionItem>::Input<'a>,
        received: &'a mut Option<<Delivery<Target> as ActionItem>::Reply>,
    ) -> impl Future<Output = ()> + Send + 'a
    where
        Delivery<Target>: 'a,
    {
        async move {
            if received.is_some() {
                return;
            }
            let Some(delivery) = input.take() else {
                return;
            };
            let settlement = 'settlement: {
                let address = delivery.to.address();
                let Some(actor) = self
                    .actor_spaces
                    .space()
                    .resolve(&address)
                    .map(|actor| actor.as_ref().clone())
                else {
                    break 'settlement ItemSettlement::Rejected {
                        item: delivery,
                        reason: LogicalDeliveryReason::UnknownAddress,
                    };
                };
                let Delivery { to, message } = delivery;
                match actor.send_from(self.address, message).await {
                    Ok(()) => ItemSettlement::Accepted(()),
                    Err(error) => ItemSettlement::Rejected {
                        item: Delivery::new(to, error.into_message()),
                        reason: LogicalDeliveryReason::ClosedRecipient,
                    },
                }
            };
            *received = Some(settlement);
        }
    }
}

impl<C, N, P, Bindings, Origins, Target, RootEvent, Path>
    InterpretItem<EstablishedDelivery<Target>, RootEvent, Path>
    for ApplicationCapabilities<C, N, P, Bindings, Origins>
where
    C: Behavior<Protocol: Protocol<Addr = MailAddr>>,
    Target: Protocol<Addr = MailAddr>,
    Target::Msg: Send,
    Self: Send,
{
    #[expect(
        clippy::manual_async_fn,
        reason = "retain the original receiving-loan opaque future; an async function captures unused route tags and would require new lifetime bounds"
    )]
    fn interpret_item<'a>(
        &'a mut self,
        input: <EstablishedDelivery<Target> as ActionItem>::Input<'a>,
        received: &'a mut Option<<EstablishedDelivery<Target> as ActionItem>::Reply>,
    ) -> impl Future<Output = ()> + Send + 'a
    where
        EstablishedDelivery<Target>: 'a,
    {
        async move {
            if received.is_some() {
                return;
            }
            let Some(delivery) = input.take() else {
                return;
            };
            let settlement = {
                let EstablishedDelivery { to, message } = delivery;
                let endpoint = to.clone().interpret(&mut ExtractLocalEndpoint);
                match endpoint.send_from(self.address, message).await {
                    Ok(()) => ItemSettlement::Accepted(()),
                    Err(error) => ItemSettlement::Rejected {
                        item: EstablishedDelivery::new(to, error.into_message()),
                        reason: ExactDeliveryReason::ClosedRecipient,
                    },
                }
            };
            *received = Some(settlement);
        }
    }
}

impl<C, N, Parent, Bindings, Origins, Target, RootEvent, Path>
    InterpretItem<CustomerDelivery<Target>, RootEvent, Path>
    for ApplicationCapabilities<C, N, Parent, Bindings, Origins>
where
    C: Behavior<Protocol: Protocol<Addr = MailAddr>>,
    Target: Protocol<Addr = MailAddr>,
    Target::Msg: Send,
    Self: InterpretItem<Delivery<Target>, RootEvent, Path>
        + InterpretItem<EstablishedDelivery<Target>, RootEvent, Path>
        + Send,
{
    #[expect(
        clippy::too_many_lines,
        reason = "one complete customer-delivery interpretation preserves all original source items, refused payloads and acquired typed replies across the owning lane"
    )]
    #[expect(
        clippy::manual_async_fn,
        reason = "retain the original receiving-loan opaque future; an async function captures unused route tags and would require new lifetime bounds"
    )]
    fn interpret_item<'a>(
        &'a mut self,
        input: <CustomerDelivery<Target> as ActionItem>::Input<'a>,
        received: &'a mut Option<<CustomerDelivery<Target> as ActionItem>::Reply>,
    ) -> impl Future<Output = ()> + Send + 'a
    where
        CustomerDelivery<Target>: 'a,
    {
        async move {
            if received.is_some() {
                return;
            }
            let Some(customer_delivery) = input.take() else {
                return;
            };
            match customer_delivery {
                CustomerDelivery::Logical { delivery } => {
                    let mut delivery = Some(delivery);
                    let mut delivery_received = None;
                    <Self as InterpretItem<Delivery<Target>, RootEvent, Path>>::interpret_item(
                        self,
                        &mut delivery,
                        &mut delivery_received,
                    )
                    .await;
                    match delivery_received {
                        Some(settlement) => {
                            *received = Some(reunite_customer_delivery(
                                settlement,
                                |delivery| CustomerDelivery::Logical { delivery },
                                ReplyDelivery::Logical,
                            ));
                        }
                        None => {
                            if let Some(delivery) = delivery {
                                *input = Some(CustomerDelivery::Logical { delivery });
                            }
                        }
                    }
                }
                CustomerDelivery::Established { delivery } => {
                    let mut delivery = Some(delivery);
                    let mut delivery_received = None;
                    <Self as InterpretItem<EstablishedDelivery<Target>, RootEvent, Path>>::interpret_item(
                        self, &mut delivery, &mut delivery_received,
                    ).await;
                    match delivery_received {
                        Some(settlement) => {
                            *received = Some(reunite_customer_delivery(
                                settlement,
                                |delivery| CustomerDelivery::Established { delivery },
                                ReplyDelivery::Established,
                            ));
                        }
                        None => {
                            if let Some(delivery) = delivery {
                                *input = Some(CustomerDelivery::Established { delivery });
                            }
                        }
                    }
                }
                CustomerDelivery::RejectedLogical { delivery, customer } => {
                    let mut delivery = Some(delivery);
                    let mut delivery_received = None;
                    <Self as InterpretItem<Delivery<Target>, RootEvent, Path>>::interpret_item(
                        self,
                        &mut delivery,
                        &mut delivery_received,
                    )
                    .await;
                    match delivery_received {
                        Some(settlement) => {
                            *received = Some(reunite_customer_delivery(
                                settlement,
                                |delivery| CustomerDelivery::RejectedLogical { delivery, customer },
                                ReplyDelivery::Logical,
                            ));
                        }
                        None => {
                            if let Some(delivery) = delivery {
                                *input =
                                    Some(CustomerDelivery::RejectedLogical { delivery, customer });
                            }
                        }
                    }
                }
                CustomerDelivery::RejectedEstablished { delivery, customer } => {
                    let mut delivery = Some(delivery);
                    let mut delivery_received = None;
                    <Self as InterpretItem<EstablishedDelivery<Target>, RootEvent, Path>>::interpret_item(
                        self, &mut delivery, &mut delivery_received,
                    ).await;
                    match delivery_received {
                        Some(settlement) => {
                            *received = Some(reunite_customer_delivery(
                                settlement,
                                |delivery| CustomerDelivery::RejectedEstablished {
                                    delivery,
                                    customer,
                                },
                                ReplyDelivery::Established,
                            ));
                        }
                        None => {
                            if let Some(delivery) = delivery {
                                *input = Some(CustomerDelivery::RejectedEstablished {
                                    delivery,
                                    customer,
                                });
                            }
                        }
                    }
                }
            }
        }
    }
}

impl<C, N, Parent, Bindings, Origins, Target, RootEvent, Path>
    InterpretItem<DiagnosticAction<Recipient<Target>, Target::Msg>, RootEvent, Path>
    for ApplicationCapabilities<C, N, Parent, Bindings, Origins>
where
    C: Behavior<Protocol: Protocol<Addr = MailAddr>>,
    Target: Protocol<Addr = MailAddr>,
    Target::Msg: Send,
    Self: InterpretItem<Delivery<Target>, RootEvent, Path> + Send,
{
    #[expect(
        clippy::manual_async_fn,
        reason = "retain the original receiving-loan opaque future; an async function captures unused route tags and would require new lifetime bounds"
    )]
    fn interpret_item<'a>(
        &'a mut self,
        input: <DiagnosticAction<Recipient<Target>, Target::Msg> as ActionItem>::Input<'a>,
        received: &'a mut Option<
            <DiagnosticAction<Recipient<Target>, Target::Msg> as ActionItem>::Reply,
        >,
    ) -> impl Future<Output = ()> + Send + 'a
    where
        DiagnosticAction<Recipient<Target>, Target::Msg>: 'a,
    {
        async move {
            if received.is_some() {
                return;
            }
            let Some(action) = input.take() else {
                return;
            };
            match action {
                DiagnosticAction::Terminal { diagnostic } => {
                    *received = Some(ItemSettlement::Accepted(DiagnosticAccepted::terminal(
                        diagnostic,
                    )));
                }
                DiagnosticAction::Deliver { route, diagnostic } => {
                    let mut delivery = Some(Delivery::new(route, diagnostic));
                    let mut delivery_received = None;
                    <Self as InterpretItem<Delivery<Target>, RootEvent, Path>>::interpret_item(
                        self,
                        &mut delivery,
                        &mut delivery_received,
                    )
                    .await;
                    if let Some(Delivery { to, message }) = delivery {
                        *input = Some(DiagnosticAction::deliver(to, message));
                    }
                    if let Some(settlement) = delivery_received {
                        *received = Some(match settlement {
                            ItemSettlement::Accepted(()) => {
                                ItemSettlement::Accepted(DiagnosticAccepted::delivered())
                            }
                            ItemSettlement::Rejected {
                                item: Delivery { to, message },
                                reason,
                            } => ItemSettlement::Rejected {
                                item: DiagnosticAction::deliver(to, message),
                                reason,
                            },
                            ItemSettlement::Blocked { prerequisite, .. } => match prerequisite {},
                            ItemSettlement::Corrupt {
                                item: Delivery { to, message },
                                fault,
                            } => ItemSettlement::Corrupt {
                                item: DiagnosticAction::deliver(to, message),
                                fault,
                            },
                        });
                    }
                }
            }
        }
    }
}

impl<C, N, Parent, Bindings, Origins, Target, RootEvent, Path>
    InterpretItem<DiagnosticAction<EstablishedRecipient<Target>, Target::Msg>, RootEvent, Path>
    for ApplicationCapabilities<C, N, Parent, Bindings, Origins>
where
    C: Behavior<Protocol: Protocol<Addr = MailAddr>>,
    Target: Protocol<Addr = MailAddr>,
    Target::Msg: Send,
    Self: InterpretItem<EstablishedDelivery<Target>, RootEvent, Path> + Send,
{
    #[expect(
        clippy::manual_async_fn,
        reason = "retain the original receiving-loan opaque future; an async function captures unused route tags and would require new lifetime bounds"
    )]
    fn interpret_item<'a>(
        &'a mut self,
        input: <DiagnosticAction<EstablishedRecipient<Target>, Target::Msg> as ActionItem>::Input<
            'a,
        >,
        received: &'a mut Option<
            <DiagnosticAction<EstablishedRecipient<Target>, Target::Msg> as ActionItem>::Reply,
        >,
    ) -> impl Future<Output = ()> + Send + 'a
    where
        DiagnosticAction<EstablishedRecipient<Target>, Target::Msg>: 'a,
    {
        async move {
            if received.is_some() {
                return;
            }
            let Some(action) = input.take() else {
                return;
            };
            match action {
                DiagnosticAction::Terminal { diagnostic } => {
                    *received = Some(ItemSettlement::Accepted(DiagnosticAccepted::terminal(
                        diagnostic,
                    )));
                }
                DiagnosticAction::Deliver { route, diagnostic } => {
                    let mut delivery = Some(EstablishedDelivery::new(route, diagnostic));
                    let mut delivery_received = None;
                    <Self as InterpretItem<EstablishedDelivery<Target>, RootEvent, Path>>::interpret_item(
                        self, &mut delivery, &mut delivery_received,
                    ).await;
                    if let Some(EstablishedDelivery { to, message }) = delivery {
                        *input = Some(DiagnosticAction::deliver(to, message));
                    }
                    if let Some(settlement) = delivery_received {
                        *received = Some(match settlement {
                            ItemSettlement::Accepted(()) => {
                                ItemSettlement::Accepted(DiagnosticAccepted::delivered())
                            }
                            ItemSettlement::Rejected {
                                item: EstablishedDelivery { to, message },
                                reason,
                            } => ItemSettlement::Rejected {
                                item: DiagnosticAction::deliver(to, message),
                                reason,
                            },
                            ItemSettlement::Blocked { prerequisite, .. } => match prerequisite {},
                            ItemSettlement::Corrupt {
                                item: EstablishedDelivery { to, message },
                                fault,
                            } => ItemSettlement::Corrupt {
                                item: DiagnosticAction::deliver(to, message),
                                fault,
                            },
                        });
                    }
                }
            }
        }
    }
}

impl<C, N, Parent, Bindings, Origins, Diagnostic, RootEvent, Path>
    InterpretItem<DiagnosticAction<Infallible, Diagnostic>, RootEvent, Path>
    for ApplicationCapabilities<C, N, Parent, Bindings, Origins>
where
    C: Behavior<Protocol: Protocol<Addr = MailAddr>>,
    Diagnostic: Send,
    Self: Send,
{
    #[expect(
        clippy::manual_async_fn,
        reason = "retain the original receiving-loan opaque future; an async function captures unused route tags and would require new lifetime bounds"
    )]
    fn interpret_item<'a>(
        &'a mut self,
        input: <DiagnosticAction<Infallible, Diagnostic> as ActionItem>::Input<'a>,
        received: &'a mut Option<<DiagnosticAction<Infallible, Diagnostic> as ActionItem>::Reply>,
    ) -> impl Future<Output = ()> + Send + 'a
    where
        DiagnosticAction<Infallible, Diagnostic>: 'a,
    {
        async move {
            if received.is_some() {
                return;
            }
            let Some(action) = input.take() else {
                return;
            };
            let settlement = {
                match action {
                    DiagnosticAction::Deliver { route, .. } => match route {},
                    DiagnosticAction::Terminal { diagnostic } => {
                        ItemSettlement::Accepted(DiagnosticAccepted::terminal(diagnostic))
                    }
                }
            };
            *received = Some(settlement);
        }
    }
}

impl<C, N, P, Bindings, Origins, Target, Occurrence, RootEvent, Path>
    InterpretItem<ChildDelivery<Target, Occurrence>, RootEvent, Path>
    for ApplicationCapabilities<C, N, P, Bindings, Origins>
where
    C: Behavior<Protocol: Protocol<Addr = MailAddr>> + ResolveChildOccurrence<Occurrence>,
    Target: Protocol<Addr = MailAddr>,
    Target::Msg: Send,
    ResolvedChild<C, Occurrence>: Behavior<Protocol = Target>,
    Bindings:
        ChildBindingAt<ResolvedChildPosition<C, Occurrence>, Child = ResolvedChild<C, Occurrence>>,
    Self: Send,
{
    #[expect(
        clippy::manual_async_fn,
        reason = "retain the original receiving-loan opaque future; an async function captures unused route tags and would require new lifetime bounds"
    )]
    fn interpret_item<'a>(
        &'a mut self,
        input: <ChildDelivery<Target, Occurrence> as ActionItem>::Input<'a>,
        received: &'a mut Option<<ChildDelivery<Target, Occurrence> as ActionItem>::Reply>,
    ) -> impl Future<Output = ()> + Send + 'a
    where
        ChildDelivery<Target, Occurrence>: 'a,
    {
        async move {
            if received.is_some() {
                return;
            }
            let Some(delivery) = input.take() else {
                return;
            };
            let settlement = 'settlement: {
                let actor = match self
                    .child_bindings
                    .as_ref()
                    .expect("live child bindings remain installed")
                    .creation(delivery.creation)
                {
                    Some(ChildAuthority::Established { endpoint, .. }) => endpoint.clone(),
                    Some(ChildAuthority::Unavailable(correlation)) => {
                        let prerequisite = CreationCorrelation::new(correlation);
                        break 'settlement ItemSettlement::Blocked {
                            item: delivery,
                            prerequisite,
                        };
                    }
                    None => {
                        break 'settlement ItemSettlement::Rejected {
                            item: delivery,
                            reason: ChildDeliveryReason::MissingBinding,
                        };
                    }
                };
                let ChildDelivery {
                    creation, message, ..
                } = delivery;
                match actor.send_from(self.address, message).await {
                    Ok(()) => ItemSettlement::Accepted(()),
                    Err(error) => ItemSettlement::Rejected {
                        item: ChildDelivery::after(creation, error.into_message()),
                        reason: ChildDeliveryReason::ClosedRecipient,
                    },
                }
            };
            *received = Some(settlement);
        }
    }
}

impl<C, N, P, Bindings, Origins, Child, Source, Input, Occurrence, RootEvent, Path>
    InterpretItem<ChildInput<Child, Source, Input, Occurrence>, RootEvent, Path>
    for ApplicationCapabilities<C, N, P, Bindings, Origins>
where
    C: Behavior<Protocol: Protocol<Addr = MailAddr>>
        + ResolveChildOccurrence<Occurrence, Child = Child>,
    Child: Behavior<Protocol: Protocol<Addr = MailAddr>>,
    Child::Event: ChildInputIngress<Source, Input> + Send,
    Input: Send,
    Bindings: ChildBindingAt<ResolvedChildPosition<C, Occurrence>, Child = Child> + Send,
    Self: Send,
{
    #[expect(
        clippy::manual_async_fn,
        reason = "retain the original receiving-loan opaque future; an async function captures unused route tags and would require new lifetime bounds"
    )]
    fn interpret_item<'a>(
        &'a mut self,
        input: <ChildInput<Child, Source, Input, Occurrence> as ActionItem>::Input<'a>,
        received: &'a mut Option<
            <ChildInput<Child, Source, Input, Occurrence> as ActionItem>::Reply,
        >,
    ) -> impl Future<Output = ()> + Send + 'a
    where
        ChildInput<Child, Source, Input, Occurrence>: 'a,
    {
        async move {
            if received.is_some() {
                return;
            }
            let Some(input) = input.take() else {
                return;
            };
            let settlement = 'settlement: {
                let control = match self
                    .child_bindings
                    .as_ref()
                    .expect("live child bindings remain installed")
                    .creation(input.creation)
                {
                    Some(ChildAuthority::Established { control, .. }) => control.clone(),
                    Some(ChildAuthority::Unavailable(correlation)) => {
                        let prerequisite = CreationCorrelation::new(correlation);
                        break 'settlement ItemSettlement::Blocked {
                            item: input,
                            prerequisite,
                        };
                    }
                    None => {
                        break 'settlement ItemSettlement::Rejected {
                            item: input,
                            reason: ChildInputReason::MissingBinding,
                        };
                    }
                };
                let creation = input.creation;
                let event =
                    <Child::Event as ChildInputIngress<Source, Input>>::child_input(input.input);
                match control.send(event) {
                    Ok(()) => ItemSettlement::Accepted(()),
                    Err(ControlClosed(event)) => {
                        let input = <Child::Event as ChildInputIngress<Source, Input>>::recover_child_input(event)
                            .unwrap_or_else(|_| unreachable!("the selected child-input port recovers the event it constructed"));
                        ItemSettlement::Rejected {
                            item: ChildInput::after(creation, input),
                            reason: ChildInputReason::ClosedControlLane,
                        }
                    }
                }
            };
            *received = Some(settlement);
        }
    }
}

impl<C, N, Parent, Bindings, Origins, Worker, Plan> ProxyControlAdmission<Worker, Plan>
    for ApplicationCapabilities<C, N, Parent, Bindings, Origins>
where
    C: Behavior<Protocol: Protocol<Addr = MailAddr>>
        + ResolveChildOccurrence<ChildHead, Child = StableProxy<Worker, Plan>>,
    Worker: Behavior<Protocol: Protocol<Addr = MailAddr>>,
    Plan: ActivationPlan,
    StableProxy<Worker, Plan>: Behavior<Protocol = Worker::Protocol>,
    <StableProxy<Worker, Plan> as Behavior>::Event:
        ChildInputIngress<StableProxy<Worker, Plan>, ProxyControl<Worker, Plan>>,
    Bindings: ChildBindingAt<ResolvedChildPosition<C, ChildHead>, Child = StableProxy<Worker, Plan>>
        + Send,
    Self: Send,
{
    fn admit_proxy_control(
        &mut self,
        creation: CreationId,
        request: ProxyControl<Worker, Plan>,
    ) -> ItemSettlement<
        ProxyControl<Worker, Plan>,
        EstablishedActor<StableProxy<Worker, Plan>>,
        ChildInputReason,
        Never,
    > {
        let Some(ChildAuthority::Established {
            endpoint, control, ..
        }) = self
            .child_bindings
            .as_ref()
            .expect("live child bindings remain installed")
            .creation(creation)
        else {
            return ItemSettlement::Rejected {
                item: request,
                reason: ChildInputReason::MissingBinding,
            };
        };
        let actor = EstablishedActor::<StableProxy<Worker, Plan>>::issued(InstalledActor::new(
            endpoint.clone(),
            control.clone(),
        ));
        let event = <StableProxy<Worker, Plan> as Behavior>::Event::child_input(request);
        match control.send(event) {
            Ok(()) => ItemSettlement::Accepted(actor),
            Err(ControlClosed(event)) => {
                let request =
                    <StableProxy<Worker, Plan> as Behavior>::Event::recover_child_input(event)
                        .unwrap_or_else(|_| {
                            unreachable!(
                                "the selected proxy-input port recovers the event it constructed"
                            )
                        });
                ItemSettlement::Rejected {
                    item: request,
                    reason: ChildInputReason::ClosedControlLane,
                }
            }
        }
    }
}

impl<C, N, Parent, Bindings, Origins, Worker, Plan, RootEvent, Path>
    InterpretItem<ProxyOperation<Here, Worker, Plan>, RootEvent, Path>
    for ApplicationCapabilities<C, N, Parent, Bindings, Origins>
where
    C: Behavior<Protocol: Protocol<Addr = MailAddr>>,
    Worker: Behavior<Protocol: Protocol<Addr = MailAddr>> + Send,
    Plan: ActivationPlan,
    StableProxy<Worker, Plan>: Behavior<Protocol = Worker::Protocol>,
    EstablishedActor<StableProxy<Worker, Plan>>: Send,
    Self: ProxyControlAdmission<Worker, Plan> + Send,
{
    #[expect(
        clippy::manual_async_fn,
        reason = "retain the original receiving-loan opaque future; an async function captures unused route tags and would require new lifetime bounds"
    )]
    fn interpret_item<'a>(
        &'a mut self,
        input: <ProxyOperation<Here, Worker, Plan> as ActionItem>::Input<'a>,
        received: &'a mut Option<<ProxyOperation<Here, Worker, Plan> as ActionItem>::Reply>,
    ) -> impl Future<Output = ()> + Send + 'a
    where
        ProxyOperation<Here, Worker, Plan>: 'a,
    {
        async move {
            if received.is_some() {
                return;
            }
            let (creation, control) = input;
            let Some(control) = control.take() else {
                return;
            };
            *received = Some(self.admit_proxy_control(creation, control));
        }
    }
}

impl<C, N, Parent, Bindings, Origins, Target, Job, RootEvent, Path>
    InterpretItem<AssignWorker<Target, Job>, RootEvent, Path>
    for ApplicationCapabilities<C, N, Parent, Bindings, Origins>
where
    C: Behavior<Protocol: Protocol<Addr = MailAddr>>,
    Target: Protocol<Addr = MailAddr, Msg = Assignment<Job>>,
    Job: Send,
    Self: InterpretItem<behavior::EstablishedDelivery<Target>, RootEvent, Path> + Send,
{
    #[expect(
        clippy::manual_async_fn,
        reason = "retain the original receiving-loan opaque future; an async function captures unused route tags and would require new lifetime bounds"
    )]
    fn interpret_item<'a>(
        &'a mut self,
        input: <AssignWorker<Target, Job> as ActionItem>::Input<'a>,
        received: &'a mut Option<<AssignWorker<Target, Job> as ActionItem>::Reply>,
    ) -> impl Future<Output = ()> + Send + 'a
    where
        AssignWorker<Target, Job>: 'a,
    {
        async move {
            <Self as InterpretItem<EstablishedDelivery<Target>, RootEvent, Path>>::interpret_item(
                self, input, received,
            )
            .await;
        }
    }
}

impl<C, N, Parent, Bindings, Origins, Worker, Plan, Path>
    InterpretItem<InitializeWorker<Worker, Plan>, C::Event, Path>
    for ApplicationCapabilities<C, N, Parent, Bindings, Origins>
where
    C: Behavior<Protocol: Protocol<Addr = MailAddr>>,
    C::Event: InjectEvent<WorkerInitializationReport<Worker, Plan>, Path>,
    Worker: Behavior<Protocol: Protocol<Addr = MailAddr>>,
    Worker::Protocol: Protocol<Addr = MailAddr>,
    BehaviorMessage<Worker>: Send,
    Plan: Send,
    Self: Send,
{
    #[expect(
        clippy::manual_async_fn,
        reason = "retain the original receiving-loan opaque future; an async function captures unused route tags and would require new lifetime bounds"
    )]
    fn interpret_item<'a>(
        &'a mut self,
        input: <InitializeWorker<Worker, Plan> as ActionItem>::Input<'a>,
        received: &'a mut Option<<InitializeWorker<Worker, Plan> as ActionItem>::Reply>,
    ) -> impl Future<Output = ()> + Send + 'a
    where
        InitializeWorker<Worker, Plan>: 'a,
    {
        async move {
            if received.is_some() {
                return;
            }
            let Some(request) = input.take() else {
                return;
            };
            let settlement = {
                let worker = request.target().interpret(&mut ExtractLocalEndpoint);
                let outcome = match worker.termination_observation().try_get() {
                    Some(termination) => WorkerInitializationOutcome::Stopped(ChildStopped::new(
                        request.worker().creation(),
                        termination,
                        Instant::now(),
                    )),
                    None => WorkerInitializationOutcome::ReadyForActivation,
                };
                self.inject_control_event::<_, Path>(request.resolve(outcome));
                ItemSettlement::Accepted(())
            };
            *received = Some(settlement);
        }
    }
}

impl<C, N, P, Bindings, Origins, D, Path> InterpretItem<EntityAdmission<D>, C::Event, Path>
    for ApplicationCapabilities<C, N, P, Bindings, Origins>
where
    C: Behavior<Protocol: Protocol<Addr = MailAddr>>,
    D: EntityDefinition,
    D::Hosts: NativeEntityHost<D::Behavior, D::Terminal, D::ChildFailures>,
    Self: Send,
{
    #[expect(
        clippy::manual_async_fn,
        reason = "retain the original receiving-loan opaque future; an async function captures unused route tags and would require new lifetime bounds"
    )]
    fn interpret_item<'a>(
        &'a mut self,
        input: <EntityAdmission<D> as ActionItem>::Input<'a>,
        received: &'a mut Option<<EntityAdmission<D> as ActionItem>::Reply>,
    ) -> impl Future<Output = ()> + Send + 'a
    where
        EntityAdmission<D>: 'a,
    {
        async move {
            if received.is_some() {
                return;
            }
            if input.is_none() {
                return;
            }
            EntityAdmission::interpret(input, self.address).await;
            *received = Some(ItemSettlement::Accepted(()));
        }
    }
}

fn reunite_customer_delivery<Target, Leaf, Reason>(
    settlement: ItemSettlement<Leaf, (), Reason, Never>,
    restore: impl FnOnce(Leaf) -> CustomerDelivery<Target>,
    classify: impl FnOnce(Reason) -> ReplyDelivery<LogicalDeliveryReason, ExactDeliveryReason>,
) -> ItemSettlement<
    CustomerDelivery<Target>,
    (),
    ReplyDelivery<LogicalDeliveryReason, ExactDeliveryReason>,
    Never,
>
where
    Target: Protocol<Addr = MailAddr>,
{
    match settlement {
        ItemSettlement::Accepted(()) => ItemSettlement::Accepted(()),
        ItemSettlement::Rejected { item, reason } => ItemSettlement::Rejected {
            item: restore(item),
            reason: classify(reason),
        },
        ItemSettlement::Blocked { prerequisite, .. } => match prerequisite {},
        ItemSettlement::Corrupt { item, fault } => ItemSettlement::Corrupt {
            item: restore(item),
            fault,
        },
    }
}

#[cfg(test)]
mod child_input_custody {
    use core::convert::Infallible;
    use core::pin::pin;
    use core::task::{Context, Poll, Waker};
    use std::future::Future;
    use std::sync::Arc;

    use behavior::{
        Behavior, BehaviorMessage, ChildHead, ChildInput, ChildInputReason, CreateChild,
        CreationId, CreationSequence, Creations, Here, InterpretItem, ItemSettlement,
        MessageProtocol, Recipient,
    };
    use behavior_actors::atomic::{
        DynamicLifecycle, DynamicSupervisor, ImmediateActivation, ProxyControl, StableProxy,
    };
    use behavior_actors::{Activate, StopOnShutdown};
    use communication::{Config, mailbox_channel};
    use tokio::sync::oneshot;

    use crate::ShutdownLedger;
    use crate::address::{ApplicationAddresses, MailAddr};
    use crate::local::children::{
        ChildAuthority, ChildBindingAt, ChildBindingAtCursor, ChildBindings, ChildStartupFailures,
        StructuralOrigins,
    };
    use crate::local::effects::observation::TerminationObservations;
    use crate::local::effects::reports::LocalTerminalReports;
    use crate::local::effects::timers::LocalTimers;
    use crate::local::effects::{
        ApplicationCapabilities, ApplicationCapabilityInputs, NoParent, RetireCapabilities,
    };
    use crate::local::endpoint::ExtractLocalEndpoint;
    use crate::terminal::{ActorRetirement, ChildOrigin, ProjectTerminal};

    type LedgerProxy = StableProxy<ShutdownLedger, ImmediateActivation>;
    type LedgerSupervisor = DynamicSupervisor<
        u64,
        ShutdownLedger,
        ImmediateActivation,
        Recipient<
            MessageProtocol<MailAddr, DynamicLifecycle<u64, ShutdownLedger, ImmediateActivation>>,
        >,
        Infallible,
    >;
    type LedgerBindings = ChildBindings<
        LedgerSupervisor,
        LedgerServiceRetirement,
        StructuralOrigins<LedgerSupervisor>,
    >;

    #[expect(
        clippy::large_enum_variant,
        reason = "the test retains complete retirements of two real actor roles; no ownership law requires additional indirection"
    )]
    enum LedgerServiceRetirement {
        Proxy {
            origin: ChildOrigin<LedgerSupervisor, ChildHead>,
            retirement: ActorRetirement<LedgerProxy, Self, ChildStartupFailures<LedgerProxy>>,
        },
        Worker {
            origin: ChildOrigin<LedgerProxy, ChildHead>,
            retirement: ActorRetirement<StopOnShutdown<ShutdownLedger>, Self, ()>,
        },
    }
    impl
        ProjectTerminal<
            ChildOrigin<LedgerSupervisor, ChildHead>,
            LedgerProxy,
            ChildStartupFailures<LedgerProxy>,
        > for LedgerServiceRetirement
    {
        type Descendants = Self;
        fn project(
            origin: ChildOrigin<LedgerSupervisor, ChildHead>,
            retirement: ActorRetirement<LedgerProxy, Self, ChildStartupFailures<LedgerProxy>>,
        ) -> Self {
            Self::Proxy { origin, retirement }
        }
    }
    impl ProjectTerminal<ChildOrigin<LedgerProxy, ChildHead>, StopOnShutdown<ShutdownLedger>, ()>
        for LedgerServiceRetirement
    {
        type Descendants = Self;
        fn project(
            origin: ChildOrigin<LedgerProxy, ChildHead>,
            retirement: ActorRetirement<StopOnShutdown<ShutdownLedger>, Self, ()>,
        ) -> Self {
            Self::Worker { origin, retirement }
        }
    }

    #[expect(
        private_interfaces,
        reason = "the test-only original registry loan implements the crate read port while its complete terminal owner remains private"
    )]
    impl ChildBindingAtCursor<ChildHead, ChildHead> for &mut LedgerBindings {
        type Child = LedgerProxy;
        fn creation_at(&self, id: CreationId) -> Option<ChildAuthority<'_, LedgerProxy>> {
            <LedgerBindings as ChildBindingAt<ChildHead>>::creation(&**self, id)
        }
    }

    #[tokio::test]
    #[expect(
        clippy::too_many_lines,
        reason = "one complete native allocation, admission, shutdown, replay and original non-Clone custody journey"
    )]
    async fn generic_proxy_child_input_returns_exact_non_clone_input_after_native_shutdown() {
        let mut sequence = CreationSequence::new();
        let pending = sequence.issue().expect("original pending proxy ID");
        let established = sequence.issue().expect("original established proxy ID");
        let missing = sequence.issue().expect("known unbound proxy ID");
        let (control, source_owner, source_mailbox, source_receiver) = mailbox_channel::<
            <LedgerSupervisor as Behavior>::Event,
            behavior::User<MailAddr, BehaviorMessage<LedgerSupervisor>>,
        >(Config::new(1));
        let (terminal_sender, terminal_receiver) = oneshot::channel();
        let mut creator = ApplicationCapabilities::<
            LedgerSupervisor,
            (),
            NoParent,
            LedgerBindings,
            StructuralOrigins<LedgerSupervisor>,
        >::new_with_bindings(
            ApplicationCapabilityInputs {
                address: MailAddr::APPLICATION_ROOT,
                actor_spaces: Arc::new(()),
                allocations: ApplicationAddresses::new(),
                control,
                timers: LocalTimers::new(),
                observations: TerminationObservations::new(),
                terminal_reports: LocalTerminalReports::new(terminal_sender),
            },
            LedgerBindings::default(),
        );
        let mut batch = Some(
            Creations::one(CreateChild::birth(pending, LedgerProxy::immediate()))
                .and(CreateChild::birth(established, LedgerProxy::immediate())),
        );
        let mut routed = None;
        <_ as InterpretItem<
            Creations<CreateChild<MailAddr, LedgerProxy>>,
            <LedgerSupervisor as Behavior>::Event,
            Here,
        >>::interpret_item(&mut creator, &mut batch, &mut routed)
        .await;
        let Some(ItemSettlement::Accepted(routed)) = routed else {
            panic!("actual creator reserves both routes");
        };
        let mut requests = routed.into_iter();
        let pending_request = requests.next().expect("pending original request");
        let established_request = requests.next().expect("established original request");
        assert!(requests.next().is_none());
        let pending_poll = {
            let mut establishing = pin!(
                <_ as behavior::EstablishChild<ChildHead, LedgerProxy>>::establish_child(
                    &mut creator,
                    pending_request
                )
            );
            establishing
                .as_mut()
                .poll(&mut Context::from_waker(Waker::noop()))
        };
        assert!(matches!(pending_poll, Poll::Pending));
        let reply = <_ as behavior::EstablishChild<ChildHead, LedgerProxy>>::establish_child(
            &mut creator,
            established_request,
        )
        .await;
        let ItemSettlement::Accepted(behavior::ChildCreationOutcome::Established(receipt)) = reply
        else {
            panic!("actual fresh proxy commits independently");
        };
        let endpoint = receipt
            .actor()
            .recipient()
            .interpret(&mut ExtractLocalEndpoint);
        let binding_owner = creator
            .child_bindings
            .as_mut()
            .expect("creator owns original registry");
        let (control, read_owner, read_mailbox, read_receiver) = mailbox_channel::<
            <LedgerSupervisor as Behavior>::Event,
            behavior::User<MailAddr, BehaviorMessage<LedgerSupervisor>>,
        >(Config::new(1));
        let (read_terminal, read_terminal_receiver) = oneshot::channel();
        let mut reader = ApplicationCapabilities::<
            LedgerSupervisor,
            (),
            NoParent,
            &mut LedgerBindings,
            StructuralOrigins<LedgerSupervisor>,
        >::new_with_bindings(
            ApplicationCapabilityInputs {
                address: MailAddr::APPLICATION_ROOT,
                actor_spaces: Arc::new(()),
                allocations: ApplicationAddresses::new(),
                control,
                timers: LocalTimers::new(),
                observations: TerminationObservations::new(),
                terminal_reports: LocalTerminalReports::new(read_terminal),
            },
            binding_owner,
        );
        let entries = vec![31, 37, 41];
        let pending_allocation = entries.as_ptr();
        let mut request = Some(ChildInput::<
            LedgerProxy,
            LedgerProxy,
            ProxyControl<ShutdownLedger, ImmediateActivation>,
            ChildHead,
        >::after(
            pending,
            ProxyControl::start(ShutdownLedger {
                entries,
                received: Vec::new(),
            }),
        ));
        let mut received = None;
        <_ as InterpretItem<
            ChildInput<
                LedgerProxy,
                LedgerProxy,
                ProxyControl<ShutdownLedger, ImmediateActivation>,
                ChildHead,
            >,
            (),
            Here,
        >>::interpret_item(&mut reader, &mut request, &mut received)
        .await;
        let Some(ItemSettlement::Blocked { item, prerequisite }) = received else {
            panic!("uncommitted proxy grants no control authority");
        };
        assert_eq!(prerequisite.id(), pending);
        assert_eq!(item.creation, pending);
        let mut pure = LedgerProxy::immediate()
            .initialize()
            .unwrap_or_else(|_| panic!("pure proxy init"))
            .behavior;
        let created = pure
            .on(item.input)
            .unwrap_or_else(|_| panic!("original blocked input starts its original worker"))
            .creates
            .into_one()
            .unwrap_or_else(|_| panic!("one original worker"));
        let (_, worker, _) = created.into_parts();
        assert_eq!(worker.into_inner().entries.as_ptr(), pending_allocation);
        let entries = vec![43, 47, 53];
        let missing_allocation = entries.as_ptr();
        let mut request = Some(ChildInput::<
            LedgerProxy,
            LedgerProxy,
            ProxyControl<ShutdownLedger, ImmediateActivation>,
            ChildHead,
        >::after(
            missing,
            ProxyControl::start(ShutdownLedger {
                entries,
                received: Vec::new(),
            }),
        ));
        let mut received = None;
        <_ as InterpretItem<
            ChildInput<
                LedgerProxy,
                LedgerProxy,
                ProxyControl<ShutdownLedger, ImmediateActivation>,
                ChildHead,
            >,
            (),
            Here,
        >>::interpret_item(&mut reader, &mut request, &mut received)
        .await;
        let Some(ItemSettlement::Rejected {
            item,
            reason: ChildInputReason::MissingBinding,
        }) = received
        else {
            panic!("missing proxy returns complete original request");
        };
        assert_eq!(item.creation, missing);
        let mut pure = LedgerProxy::immediate()
            .initialize()
            .unwrap_or_else(|_| panic!("pure proxy init"))
            .behavior;
        let created = pure
            .on(item.input)
            .unwrap_or_else(|_| panic!("original absent input starts its original worker"))
            .creates
            .into_one()
            .unwrap_or_else(|_| panic!("one original worker"));
        let (_, worker, _) = created.into_parts();
        assert_eq!(worker.into_inner().entries.as_ptr(), missing_allocation);
        let mut request = Some(ChildInput::<
            LedgerProxy,
            LedgerProxy,
            ProxyControl<ShutdownLedger, ImmediateActivation>,
            ChildHead,
        >::after(established, ProxyControl::shutdown()));
        let mut received = None;
        <_ as InterpretItem<
            ChildInput<
                LedgerProxy,
                LedgerProxy,
                ProxyControl<ShutdownLedger, ImmediateActivation>,
                ChildHead,
            >,
            (),
            Here,
        >>::interpret_item(&mut reader, &mut request, &mut received)
        .await;
        assert!(matches!(received, Some(ItemSettlement::Accepted(()))));
        assert!(endpoint.termination().await.is_ok());
        let entries = vec![59, 61, 67];
        let closed_allocation = entries.as_ptr();
        let mut request = Some(ChildInput::<
            LedgerProxy,
            LedgerProxy,
            ProxyControl<ShutdownLedger, ImmediateActivation>,
            ChildHead,
        >::after(
            established,
            ProxyControl::start(ShutdownLedger {
                entries,
                received: Vec::new(),
            }),
        ));
        let mut received = None;
        <_ as InterpretItem<
            ChildInput<
                LedgerProxy,
                LedgerProxy,
                ProxyControl<ShutdownLedger, ImmediateActivation>,
                ChildHead,
            >,
            (),
            Here,
        >>::interpret_item(&mut reader, &mut request, &mut received)
        .await;
        <_ as InterpretItem<
            ChildInput<
                LedgerProxy,
                LedgerProxy,
                ProxyControl<ShutdownLedger, ImmediateActivation>,
                ChildHead,
            >,
            (),
            Here,
        >>::interpret_item(&mut reader, &mut request, &mut received)
        .await;
        assert!(request.is_none());
        let Some(ItemSettlement::Rejected {
            item,
            reason: ChildInputReason::ClosedControlLane,
        }) = received
        else {
            panic!("closed receiver returns complete original non-Clone input");
        };
        assert_eq!(item.creation, established);
        let mut pure = LedgerProxy::immediate()
            .initialize()
            .unwrap_or_else(|_| panic!("pure proxy init"))
            .behavior;
        let created = pure
            .on(item.input)
            .unwrap_or_else(|_| panic!("returned original input starts its original worker"))
            .creates
            .into_one()
            .unwrap_or_else(|_| panic!("one original worker"));
        let (_, worker, _) = created.into_parts();
        let worker = worker.into_inner();
        assert_eq!(worker.entries.as_ptr(), closed_allocation);
        assert_eq!(worker.entries, [59, 61, 67]);
        drop(reader);
        let retirement = creator.retire().await;
        let (children, (failures, ())) = retirement.descendants;
        assert!(failures.is_empty());
        assert_eq!(children.len(), 2);
        for terminal in children {
            match terminal {
                LedgerServiceRetirement::Proxy { origin, retirement } => {
                    assert!(origin.creation_id() == pending || origin.creation_id() == established);
                    assert!(retirement.behavior().is_some());
                }
                LedgerServiceRetirement::Worker { origin, retirement } => {
                    drop((origin, retirement));
                    panic!("returned start commands were never admitted to the stopped proxy");
                }
            }
        }
        drop((
            source_owner,
            source_mailbox,
            source_receiver,
            terminal_receiver,
            read_owner,
            read_mailbox,
            read_receiver,
            read_terminal_receiver,
        ));
    }

    fn original_worker_survives_child_input_composition<Child>()
    where
        Child: Behavior<Protocol = ShutdownLedger>,
        Child::Event: behavior::ChildInputIngress<
                LedgerProxy,
                ProxyControl<ShutdownLedger, ImmediateActivation>,
            >,
    {
        let entries = vec![71, 73, 79];
        let original = entries.as_ptr();
        let event = <Child::Event as behavior::ChildInputIngress<
            LedgerProxy,
            ProxyControl<ShutdownLedger, ImmediateActivation>,
        >>::child_input(ProxyControl::start(ShutdownLedger {
            entries,
            received: Vec::new(),
        }));
        let input = <Child::Event as behavior::ChildInputIngress<
            LedgerProxy,
            ProxyControl<ShutdownLedger, ImmediateActivation>,
        >>::recover_child_input(event)
        .unwrap_or_else(|_| panic!("selected child input returns its original worker"));
        let mut proxy = LedgerProxy::immediate()
            .initialize()
            .unwrap_or_else(|_| panic!("pure proxy initialization"))
            .behavior;
        let request = proxy
            .on(input)
            .unwrap_or_else(|_| panic!("original worker is accepted by the pure proxy"))
            .creates
            .into_one()
            .unwrap_or_else(|_| panic!("one worker definition"));
        let (_, worker, _) = request.into_parts();
        let worker = worker.into_inner();
        assert_eq!(worker.entries.as_ptr(), original);
        assert_eq!(worker.entries, [71, 73, 79]);
        let entries = vec![83, 89, 97];
        let foreign = entries.as_ptr();
        let event = <Child::Event as behavior::UserEvent>::user(MailAddr(103), entries);
        let Err(event) = <Child::Event as behavior::ChildInputIngress<
            LedgerProxy,
            ProxyControl<ShutdownLedger, ImmediateActivation>,
        >>::recover_child_input(event) else {
            panic!("a public service message is not a private owner input");
        };
        let user = <Child::Event as behavior::UserEvent>::into_user(event)
            .unwrap_or_else(|_| panic!("original foreign service lane remains exact"));
        assert_eq!(user.from, MailAddr(103));
        assert_eq!(user.message.as_ptr(), foreign);
        assert_eq!(user.message, [83, 89, 97]);
    }

    fn stop_on_peer_exit<B: Behavior>(
        _: &mut B,
        _: behavior::BehaviorAddr<B>,
        _: &Result<behavior_actors::Exit<behavior::BehaviorAddr<B>>, behavior_actors::Crash>,
    ) -> behavior::Become {
        behavior::Step::Stop(behavior::Stopped)
    }

    #[test]
    fn private_input_recovery_preserves_watch_and_shutdown_in_both_orders() {
        original_worker_survives_child_input_composition::<LedgerProxy>();
        original_worker_survives_child_input_composition::<
            StopOnShutdown<behavior_actors::Watch<LedgerProxy>>,
        >();
        original_worker_survives_child_input_composition::<
            behavior_actors::Watch<StopOnShutdown<LedgerProxy>>,
        >();
        let mut watched = behavior_actors::Watch::new(
            StopOnShutdown::new(LedgerProxy::immediate()),
            MailAddr(107),
            stop_on_peer_exit::<StopOnShutdown<LedgerProxy>>,
        );
        let watched_initialization = behavior::initialize(&mut watched)
            .unwrap_or_else(|_| panic!("real watch emits its peer observation"));
        let event = behavior::EventLayer::Owned(behavior_actors::PeerStopped::new(
            MailAddr(107),
            Err(behavior_actors::Crash::Cancelled),
        ));
        let Err(event) = <<behavior_actors::Watch<StopOnShutdown<LedgerProxy>> as Behavior>::Event as behavior::ChildInputIngress<LedgerProxy, ProxyControl<ShutdownLedger, ImmediateActivation>>>::recover_child_input(event) else { panic!("owned peer lifecycle report remains outside private child input lane"); };
        let actions = behavior::delegate_transition(&mut watched, event)
            .unwrap_or_else(|_| panic!("exact peer report remains accepted by its owning watch"));
        assert!(matches!(actions.become_, behavior::Step::Stop(_)));
        drop((watched_initialization, actions));
        let mut shutdown = StopOnShutdown::new(behavior_actors::Watch::new(
            LedgerProxy::immediate(),
            MailAddr(109),
            stop_on_peer_exit::<LedgerProxy>,
        ));
        let shutdown_initialization = behavior::initialize(&mut shutdown)
            .unwrap_or_else(|_| panic!("real shutdown/watch composition initializes"));
        let event = behavior::EventLayer::Owned(behavior_actors::ShutdownRequested);
        let Err(event) = <<StopOnShutdown<behavior_actors::Watch<LedgerProxy>> as Behavior>::Event as behavior::ChildInputIngress<LedgerProxy, ProxyControl<ShutdownLedger, ImmediateActivation>>>::recover_child_input(event) else { panic!("outer owned shutdown remains outside private child input lane"); };
        let actions = behavior::delegate_transition(&mut shutdown, event)
            .unwrap_or_else(|_| panic!("exact shutdown remains accepted by its owning wrapper"));
        assert!(matches!(actions.become_, behavior::Step::Stop(_)));
        drop((shutdown_initialization, actions));
    }
}
