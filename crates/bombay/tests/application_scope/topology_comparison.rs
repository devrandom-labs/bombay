//! Handwritten host/schema composition for the same hydrated and cold account journey.
use super::{
    Account, AccountTerminal, ColdAccountOrigin, ColdApplication, ColdMembers, ManagedAccount,
    ManagedRoot, Root, run_cold_account_family, topology,
};
use bombay::behavior::{
    Behavior, ChildCreationOutcome, ChildHead, CreationId, CreationKind, CreationRejection,
    ItemSettlement, Never, RoutedCreation,
};
use bombay::{
    ActorSpace, ApplicationTopology, ChildFailure, EstablishNativeChild, Hosts, MailAddr,
    NativeChild, PendingChild, ProjectTerminal, RetirementSchema, RootRegistration,
};
use core::future::Future;

type AccountProtocol = <Account as Behavior>::Protocol;
type RootProtocol = <Root as Behavior>::Protocol;
struct AccountHosts {
    root: ActorSpace<RootProtocol>,
    accounts: ActorSpace<AccountProtocol>,
}
impl AccountHosts {
    fn new() -> Self {
        Self {
            root: ActorSpace::new(),
            accounts: ActorSpace::new(),
        }
    }
}
impl Hosts<AccountProtocol> for AccountHosts {
    fn space(&self) -> &ActorSpace<AccountProtocol> {
        &self.accounts
    }
}
impl RootRegistration<RootProtocol> for AccountHosts {
    fn root_space(&self) -> &ActorSpace<RootProtocol> {
        &self.root
    }
}
struct AccountDescendant {
    origin: ColdAccountOrigin,
    retirement: Box<AccountTerminal>,
}
impl RetirementSchema for AccountDescendant {
    type Failure = ChildFailure<ColdAccountOrigin, ManagedAccount>;
}
impl ProjectTerminal<ColdAccountOrigin, ManagedAccount> for AccountDescendant {
    type Descendants = Never;
    fn project(origin: ColdAccountOrigin, retirement: AccountTerminal) -> Self {
        Self {
            origin,
            retirement: Box::new(retirement),
        }
    }
}
impl ApplicationTopology<ManagedRoot, ColdMembers> for AccountHosts {
    type Descendants = AccountDescendant;
}
impl NativeChild<ColdApplication, ChildHead, ManagedAccount, AccountDescendant, Never>
    for AccountHosts
{
    fn registration(&self) -> ActorSpace<AccountProtocol> {
        ActorSpace::new()
    }
    fn establish<Origin, OriginFn>(
        pending: PendingChild<
            '_,
            ColdApplication,
            Self,
            ChildHead,
            ManagedAccount,
            AccountDescendant,
            Never,
            OriginFn,
        >,
    ) -> impl Future<
        Output = ItemSettlement<
            RoutedCreation<MailAddr, ManagedAccount>,
            ChildCreationOutcome<ManagedAccount, ChildHead>,
            CreationRejection,
            Never,
        >,
    > + Send
    where
        AccountDescendant:
            ProjectTerminal<Origin, ManagedAccount, Descendants = Never> + Send + 'static,
        Origin: Send + 'static,
        OriginFn: Fn(MailAddr, u64, CreationId, CreationKind) -> Origin + Send,
    {
        <Self as EstablishNativeChild<ColdApplication, ManagedAccount, Never>>::establish_native_child::<ChildHead, AccountDescendant, Origin, OriginFn>(pending)
    }
}
#[test]
fn generated_and_handwritten_hosts_preserve_hydration_cold_custody_and_shutdown() {
    run_cold_account_family(
        topology::Topology::new(),
        |topology::RootRetirements { origin, retirement }| (origin, retirement),
    );
    run_cold_account_family(
        AccountHosts::new(),
        |AccountDescendant { origin, retirement }| (origin, retirement),
    );
}
