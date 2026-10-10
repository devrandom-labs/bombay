use crate::address::MailAddr;
use crate::entity::{EntityAdmission, EntityDefinition, NativeEntityHost};
use crate::local::children::{ChildBindingAt, CreationBinding};
use crate::local::effects::ApplicationCapabilities;
use crate::local::endpoint::{ExtractLocalEndpoint, InstalledActor};
use crate::topology::Hosts;
use behavior::{
    ActionItem, Behavior, BehaviorMessage, ChildDelivery, ChildDeliveryReason, ChildHead,
    ChildInput, ChildInputIngress, ChildInputReason, CreationCorrelation, CreationId, Delivery,
    EstablishedActor, EstablishedDelivery, EstablishedRecipient, ExactDeliveryReason, Here,
    InjectEvent, InterpretItem, ItemSettlement, LogicalDeliveryReason, Never, Protocol, Recipient,
    RecoverEvent, ResolveChildOccurrence, ResolvedChild, ResolvedChildPosition,
};
use behavior_actors::atomic::{
    ActivationPlan, AssignWorker, Assignment, CustomerDelivery, DiagnosticAccepted,
    DiagnosticAction, InitializeWorker, ProxyControl, ProxyControlAdmission, ProxyOperation,
    StableProxy, WorkerInitializationOutcome, WorkerInitializationReport,
};
use behavior_actors::{ChildStopped, ReplyDelivery, ShutdownRequested};
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
                    Some(CreationBinding::Established { endpoint, .. }) => endpoint.clone(),
                    Some(CreationBinding::Rejected | CreationBinding::StartupRejected { .. }) => {
                        let prerequisite = CreationCorrelation::new(delivery.creation);
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
    Child::Event: InjectEvent<ShutdownRequested, Here>,
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
                    Some(CreationBinding::Established { control, .. }) => control.clone(),
                    Some(CreationBinding::Rejected | CreationBinding::StartupRejected { .. }) => {
                        let prerequisite = CreationCorrelation::new(input.creation);
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
                let event = Child::Event::child_input(input.input);
                match control.send(event) {
                    Ok(()) => ItemSettlement::Accepted(()),
                    Err(ControlClosed(_)) => {
                        unreachable!("an owned child control lane outlives its parent binding")
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
    <StableProxy<Worker, Plan> as Behavior>::Event: ChildInputIngress<StableProxy<Worker, Plan>, ProxyControl<Worker, Plan>>
        + RecoverEvent<ProxyControl<Worker, Plan>, Here>,
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
        let Some(CreationBinding::Established {
            endpoint,
            control,
            retirement,
            ..
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
            retirement.clone(),
        ));
        let event = <StableProxy<Worker, Plan> as Behavior>::Event::child_input(request);
        match control.send(event) {
            Ok(()) => ItemSettlement::Accepted(actor),
            Err(ControlClosed(event)) => {
                let request = <StableProxy<Worker, Plan> as Behavior>::Event::recover(event)
                    .unwrap_or_else(|_| unreachable!("the proxy control event was just injected"));
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
