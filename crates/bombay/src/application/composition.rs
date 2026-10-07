use crate::address::MailAddr;
use crate::local::children::{ChildOriginAt, StructuralOrigins};
use crate::terminal::ChildOrigin;
use behavior::{
    Actions, Behavior, BehaviorBase, BirthMode, BirthNodeAppend, Births, ChildCons, ChildHead,
    ChildOccurrenceProduct, ChildOccurrenceShape, ChildOccurrences, ChildProduct, ChildTail,
    Children, CreationId, CreationSequence, Creations, Never, NoBirths, NoChildren, Protocol,
    RetirementBirths,
};
use core::fmt;
use core::marker::PhantomData;
use std::any::Any;
use std::error::Error;
use std::panic::{AssertUnwindSafe, catch_unwind};

pub trait AppendApplicationBirths<Tail>: BirthMode
where
    Self::Child: BirthNodeAppend<Tail>,
    Tail: BirthNodeAppend<Never>,
{
    type Output: BirthMode<Child = <Self::Child as BirthNodeAppend<Tail>>::Output>;
}

impl<Tail> AppendApplicationBirths<Tail> for NoBirths
where
    Never: BirthNodeAppend<Tail>,
    Tail: BirthNodeAppend<Never>,
{
    type Output = RetirementBirths<<Never as BirthNodeAppend<Tail>>::Output>;
}

impl<Head, Tail> AppendApplicationBirths<Tail> for Births<Head>
where
    Head: BirthNodeAppend<Tail>,
    Tail: BirthNodeAppend<Never>,
{
    type Output = Births<<Head as BirthNodeAppend<Tail>>::Output>;
}

impl<Head, Tail> AppendApplicationBirths<Tail> for RetirementBirths<Head>
where
    Head: BirthNodeAppend<Tail>,
    Tail: BirthNodeAppend<Never>,
{
    type Output = RetirementBirths<<Head as BirthNodeAppend<Tail>>::Output>;
}

pub(in crate::application) trait StageApplicationChildren: Sized {
    type Product: ChildProduct<MailAddr>;
    type Failure;

    fn stage(
        self,
        creations: &mut CreationSequence,
    ) -> Result<Children<MailAddr, Self::Product>, Self::Failure>;
}

trait DeclaredApplicationChildren: StageApplicationChildren {}

impl<Role, Actor, Tail> DeclaredApplicationChildren for (Role, Actor, Tail)
where
    Actor: Behavior<Protocol: Protocol<Addr = MailAddr>>,
    Tail: StageApplicationChildren,
{
}

impl StageApplicationChildren for () {
    type Product = NoChildren;
    type Failure = Never;

    fn stage(
        self,
        _: &mut CreationSequence,
    ) -> Result<Children<MailAddr, Self::Product>, Self::Failure> {
        Ok(Children::new())
    }
}

impl<Role, Actor, Tail> StageApplicationChildren for (Role, Actor, Tail)
where
    Actor: Behavior<Protocol: Protocol<Addr = MailAddr>>,
    Tail: StageApplicationChildren,
{
    type Product = ChildCons<MailAddr, Actor, Tail::Product>;
    type Failure = ApplicationStagingError<Role, Actor, Tail::Product, Tail::Failure>;

    fn stage(
        self,
        creations: &mut CreationSequence,
    ) -> Result<Children<MailAddr, Self::Product>, Self::Failure> {
        let (role, actor, tail) = self;
        let children = match tail.stage(creations) {
            Ok(children) => children,
            Err(tail) => return Err(ApplicationStagingError::TailRejected { role, actor, tail }),
        };
        let Some(id) = creations.issue() else {
            return Err(ApplicationStagingError::NonceExhausted {
                role,
                actor,
                children,
            });
        };
        match catch_unwind(AssertUnwindSafe(|| drop(role))) {
            Ok(()) => Ok(children.child(id, actor)),
            Err(cause) => Err(ApplicationStagingError::RoleDisposalPanicked {
                id,
                actor,
                children,
                cause,
            }),
        }
    }
}

impl<Role, Actor, Product, TailFailure> fmt::Debug
    for ApplicationStagingError<Role, Actor, Product, TailFailure>
where
    Product: ChildProduct<MailAddr>,
{
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::TailRejected { .. } => "TailRejected",
            Self::NonceExhausted { .. } => "NonceExhausted",
            Self::RoleDisposalPanicked { .. } => "RoleDisposalPanicked",
        })
    }
}

impl<Role, Actor, Product, TailFailure> fmt::Display
    for ApplicationStagingError<Role, Actor, Product, TailFailure>
where
    Product: ChildProduct<MailAddr>,
{
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::TailRejected { .. } => "the tail retains a stopped cold declaration",
            Self::NonceExhausted { .. } => "no creator-local ID remained for the cold declaration",
            Self::RoleDisposalPanicked { .. } => {
                "the cold declaration retains a role disposal panic"
            }
        })
    }
}

impl<Role, Actor, Product, TailFailure> Error
    for ApplicationStagingError<Role, Actor, Product, TailFailure>
where
    Product: ChildProduct<MailAddr>,
{
}

impl<Root, Product> ApplicationBehavior<Root, Product>
where
    Product: ChildProduct<MailAddr>,
{
    const fn new(root: Root, application: Children<MailAddr, Product>) -> Self {
        Self {
            root,
            application: Some(application),
        }
    }
}

impl<Root, Product> Behavior for ApplicationBehavior<Root, Product>
where
    Root: Behavior<Protocol: Protocol<Addr = MailAddr>, Ph = Never>,
    Product: ChildProduct<MailAddr>,
    RootBirthNode<Root>: BirthNodeAppend<Product::Choice>,
    Root::Birth: AppendApplicationBirths<Product::Choice>,
{
    type Protocol = Root::Protocol;
    type Event = Root::Event;
    type Sends = Root::Sends;
    type Ph = Never;
    type Error = ApplicationDefinitionError<Root::Error>;
    type Birth = <Root::Birth as AppendApplicationBirths<Product::Choice>>::Output;

    fn init(&mut self, _: behavior::InitializationTurn) -> behavior::BehaviorActed<Self> {
        let actions =
            behavior::initialize(&mut self.root).map_err(ApplicationDefinitionError::Root)?;
        let application = self
            .application
            .take()
            .ok_or(ApplicationDefinitionError::InitializedTwice)?;
        let application = application.into_creates();
        Ok(Actions {
            sends: actions.sends,
            creates: RootBirthNode::<Root>::append_creations(actions.creates, application),
            become_: actions.become_,
        })
    }

    fn transition(
        &mut self,
        _: behavior::ActiveTurn,
        event: Self::Event,
    ) -> behavior::BehaviorActed<Self> {
        let actions = behavior::delegate_transition(&mut self.root, event)
            .map_err(ApplicationDefinitionError::Root)?;
        Ok(Actions {
            sends: actions.sends,
            creates: RootBirthNode::<Root>::append_creations(actions.creates, Creations::empty()),
            become_: actions.become_,
        })
    }
}

impl<Root, Product> BehaviorBase for ApplicationBehavior<Root, Product>
where
    Root: BehaviorBase,
    Product: ChildProduct<MailAddr>,
{
    type Base = Root::Base;

    fn base(&self) -> &Self::Base {
        self.root.base()
    }
}

pub(in crate::application) trait ComposeApplication<Root>
where
    Root: Behavior<Protocol: Protocol<Addr = MailAddr>, Ph = Never> + BehaviorBase,
{
    type Actor: Behavior<Protocol = Root::Protocol, Ph = Never>;
    type Origins;
    type StagingFailure;

    fn compose(self, root: Root) -> Result<Self::Actor, (Root, Self::StagingFailure)>;
}

impl<Root> ComposeApplication<Root> for ()
where
    Root: Behavior<Protocol: Protocol<Addr = MailAddr>, Ph = Never> + BehaviorBase,
{
    type Actor = Root;
    type Origins = StructuralOrigins<Root::Base>;
    type StagingFailure = Never;

    fn compose(self, root: Root) -> Result<Self::Actor, (Root, Self::StagingFailure)> {
        Ok(root)
    }
}

impl<Root, Role, Actor, Tail> ComposeApplication<Root> for (Role, Actor, Tail)
where
    Root: Behavior<Protocol: Protocol<Addr = MailAddr>, Ph = Never> + BehaviorBase,
    (Role, Actor, Tail): DeclaredApplicationChildren,
    RootBirthNode<Root>: BirthNodeAppend<ApplicationBirthNode<(Role, Actor, Tail)>>,
    Root::Birth: AppendApplicationBirths<ApplicationBirthNode<(Role, Actor, Tail)>>,
{
    type Actor = ApplicationBehavior<Root, ApplicationProduct<(Role, Actor, Tail)>>;
    type Origins = ApplicationOrigins<Root, (Role, Actor, Tail)>;
    type StagingFailure = <Self as StageApplicationChildren>::Failure;

    fn compose(self, root: Root) -> Result<Self::Actor, (Root, Self::StagingFailure)> {
        stage_application(root, self)
    }
}

impl ChildOccurrenceShape for RootTerminalOriginMapper {
    type Empty = NoRootTerminalOrigins;
    type Member<Position, Child: Behavior, Tail> = RootTerminalOrigin<Child, Tail>;
}

pub(in crate::application) trait AppendedChildOriginAt<Target, Absolute, Root, Child: Behavior> {
    type Origin: Send + 'static;

    fn origin(address: MailAddr, nonce: u64) -> Self::Origin;
}

pub(in crate::application) trait DeclaredChildOriginAt<Target, Root, Child: Behavior> {
    type Origin: Send + 'static;

    fn origin(address: MailAddr, nonce: u64) -> Self::Origin;
}

impl<Members, Target, Absolute, Root, Child: Behavior>
    AppendedChildOriginAt<Target, Absolute, Root, Child>
    for AppendedOrigins<NoRootTerminalOrigins, Members>
where
    Members: DeclaredChildOriginAt<Target, Root, Child>,
{
    type Origin = Members::Origin;

    fn origin(address: MailAddr, nonce: u64) -> Self::Origin {
        Members::origin(address, nonce)
    }
}

impl<RootChild: Behavior, Rest, Members, Absolute: 'static, Root>
    AppendedChildOriginAt<ChildHead, Absolute, Root, RootChild>
    for AppendedOrigins<RootTerminalOrigin<RootChild, Rest>, Members>
where
    Root: BehaviorBase,
    Root::Base: 'static,
{
    type Origin = ChildOrigin<Root::Base, Absolute>;

    fn origin(address: MailAddr, nonce: u64) -> Self::Origin {
        ChildOrigin::new(address, nonce)
    }
}

impl<RootChild: Behavior, Rest, Members, Relative, Absolute, Root, Child: Behavior>
    AppendedChildOriginAt<ChildTail<Relative>, Absolute, Root, Child>
    for AppendedOrigins<RootTerminalOrigin<RootChild, Rest>, Members>
where
    AppendedOrigins<Rest, Members>: AppendedChildOriginAt<Relative, Absolute, Root, Child>,
{
    type Origin = <AppendedOrigins<Rest, Members> as AppendedChildOriginAt<
        Relative,
        Absolute,
        Root,
        Child,
    >>::Origin;

    fn origin(address: MailAddr, nonce: u64) -> Self::Origin {
        <AppendedOrigins<Rest, Members> as
            AppendedChildOriginAt<Relative, Absolute, Root, Child>>::origin(address, nonce)
    }
}

impl<Role: 'static, Child: Behavior, Tail, Root: 'static>
    DeclaredChildOriginAt<ChildHead, Root, Child> for (Role, Child, Tail)
{
    type Origin = ChildOrigin<Root, Role>;

    fn origin(address: MailAddr, nonce: u64) -> Self::Origin {
        ChildOrigin::new(address, nonce)
    }
}

impl<Role, Declared: Behavior, Tail, Relative, Root, Child: Behavior>
    DeclaredChildOriginAt<ChildTail<Relative>, Root, Child> for (Role, Declared, Tail)
where
    Tail: DeclaredChildOriginAt<Relative, Root, Child>,
{
    type Origin = Tail::Origin;

    fn origin(address: MailAddr, nonce: u64) -> Self::Origin {
        Tail::origin(address, nonce)
    }
}

impl<Root, Members, Position, Child: Behavior> ChildOriginAt<Position, Child>
    for ApplicationOrigins<Root, Members>
where
    Root: Behavior + BehaviorBase,
    RootBirthNode<Root>: ChildOccurrenceProduct<RootTerminalOriginMapper>,
    AppendedOrigins<RootOriginProduct<Root>, Members>:
        AppendedChildOriginAt<Position, Position, Root, Child>,
{
    type Origin = <AppendedOrigins<RootOriginProduct<Root>, Members> as AppendedChildOriginAt<
        Position,
        Position,
        Root,
        Child,
    >>::Origin;

    fn origin(address: MailAddr, nonce: u64) -> Self::Origin {
        <AppendedOrigins<RootOriginProduct<Root>, Members> as AppendedChildOriginAt<
            Position,
            Position,
            Root,
            Child,
        >>::origin(address, nonce)
    }
}

type RootBirthNode<Root> = <<Root as Behavior>::Birth as BirthMode>::Child;

type ApplicationProduct<Members> = <Members as StageApplicationChildren>::Product;

type ApplicationBirthNode<Members> =
    <ApplicationProduct<Members> as ChildProduct<MailAddr>>::Choice;

type RootOriginProduct<Root> = ChildOccurrences<RootBirthNode<Root>, RootTerminalOriginMapper>;

pub(in crate::application) struct ApplicationOrigins<Root, Members>(
    PhantomData<fn() -> (Root, Members)>,
);

/// Failure while initializing the exact composed application behavior.
pub enum ApplicationDefinitionError<RootError> {
    Root(RootError),
    InitializedTwice,
}

/// Exact available cold declarations when application staging stops before any actor starts.
///
/// A consumed role is not reconstructed. The original root and uninvoked caller work
/// remain in the enclosing run error, independently of this child product.
#[must_use = "original cold declarations and native causes require explicit custody"]
pub enum ApplicationStagingError<Role, Actor, Product, TailFailure>
where
    Product: ChildProduct<MailAddr>,
{
    /// The tail returned a partial; the current role and actor were never attempted.
    TailRejected {
        role: Role,
        actor: Actor,
        tail: TailFailure,
    },
    /// No current ID was issued; the current role, actor and completed tail survive.
    NonceExhausted {
        role: Role,
        actor: Actor,
        children: Children<MailAddr, Product>,
    },
    /// The current role was consumed; the actual ID, actor, completed tail and cause survive.
    RoleDisposalPanicked {
        id: CreationId,
        actor: Actor,
        children: Children<MailAddr, Product>,
        cause: Box<dyn Any + Send>,
    },
}

fn stage_application<Root, Members>(
    root: Root,
    members: Members,
) -> Result<ApplicationBehavior<Root, ApplicationProduct<Members>>, (Root, Members::Failure)>
where
    Root: Behavior,
    Members: StageApplicationChildren,
{
    let mut creations = CreationSequence::new();
    match members.stage(&mut creations) {
        Ok(application) => Ok(ApplicationBehavior::new(root, application)),
        Err(failure) => Err((root, failure)),
    }
}

/// Exact behavior produced by composing an application root with its declared actors.
///
/// This type is public because terminal custody retains the final concrete
/// behavior and its complete action settlements. Applications normally create
/// it through [`Application`](crate::Application) and only name it in terminal projections.
pub struct ApplicationBehavior<Root, Product>
where
    Product: ChildProduct<MailAddr>,
{
    root: Root,
    application: Option<Children<MailAddr, Product>>,
}

pub(in crate::application) struct RootTerminalOriginMapper;

pub(in crate::application) struct NoRootTerminalOrigins;

pub(in crate::application) struct RootTerminalOrigin<Child, Tail>(
    PhantomData<fn() -> (Child, Tail)>,
);

pub(in crate::application) struct AppendedOrigins<RootOrigins, Members>(
    PhantomData<fn() -> (RootOrigins, Members)>,
);
