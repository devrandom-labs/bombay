//! For learning native Entity installation: `Account` owns domain state,
//! `StopOnShutdown` owns lifecycle policy, Behavior owns typed actions, and the
//! advanced Bombay `App` boundary owns hydration, stable identity, passivation,
//! reactivation, and root-first family shutdown.

use core::num::NonZeroUsize;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, PoisonError};

use bombay::actors::ActorExt;
use bombay::behavior::{
    Actions, BehaviorActed, BehaviorBase, BehaviorSettlements, ClassifySettlement, Never, Protocol,
    SettlementStatus,
};
use bombay::entity::{
    ActivationId, AdmissionFailure, DirectoryConfig, DrainFailure, EntityActivationError,
    EntityCapacity, EntityDefinition, EntityId, Passivation,
};
use bombay::prelude::{
    ActorOrigin, ActorRetirement, Completion, MailAddr, StopOnShutdown, TerminalProjection,
};
use bombay::{App, HostedAddresses, LocalAddresses};
use tokio::sync::Semaphore;

const ACCOUNT_ID: u64 = 7;

struct Root;

#[bombay::actor(message = Never)]
impl Root {}

#[derive(Debug)]
enum AccountCommand {
    Deposit(u64),
}

struct Account {
    balance: u64,
}

#[bombay::actor]
#[allow(
    clippy::unnecessary_wraps,
    reason = "the Behavior fold returns the typed action algebra"
)]
impl Account {
    fn receive(&mut self, command: AccountCommand) -> BehaviorActed<Self> {
        let AccountCommand::Deposit(amount) = command;
        self.balance = self
            .balance
            .checked_add(amount)
            .expect("the example account balance remains bounded");
        Ok(Actions::cont())
    }
}

type AccountRetirement = ActorRetirement<StopOnShutdown<Account>, Never>;

struct Accounts {
    retired: Arc<Semaphore>,
    retirements: Arc<Mutex<Vec<AccountRetirement>>>,
    unexpected_facts: Arc<AtomicUsize>,
}

impl EntityDefinition for Accounts {
    type Id = u64;
    type Behavior = StopOnShutdown<Account>;
    type HostedAddresses = Spaces;
    type HydrationError = Never;
    type Terminal = Never;

    async fn hydrate(&self, _: EntityId<Self::Id>) -> Result<Self::Behavior, Self::HydrationError> {
        Ok(Account { balance: 0 }.stop_on_shutdown())
    }

    fn activation_failed(
        &self,
        _: EntityId<Self::Id>,
        _: ActivationId,
        _: EntityActivationError<Self::HydrationError, Self::Behavior, Self::Terminal>,
    ) {
        self.unexpected_facts.fetch_add(1, Ordering::Relaxed);
    }

    fn admission_refused(&self, _: EntityId<Self::Id>, _: AdmissionFailure<AccountCommand>) {
        self.unexpected_facts.fetch_add(1, Ordering::Relaxed);
    }

    fn forced_retirement(&self, _: EntityId<Self::Id>, _: ActivationId, _: DrainFailure) {
        self.unexpected_facts.fetch_add(1, Ordering::Relaxed);
    }

    fn retired(&self, _: EntityId<Self::Id>, _: ActivationId, retirement: AccountRetirement) {
        self.retirements
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .push(retirement);
        self.retired.add_permits(1);
    }
}

#[derive(Clone, Copy)]
struct AccountsRole;

struct Replies;

impl Protocol for Replies {
    type Addr = MailAddr;
    type Msg = Never;
}

#[derive(HostedAddresses)]
struct Spaces {
    root: LocalAddresses<Root>,
    accounts: LocalAddresses<Account>,
}

#[derive(TerminalProjection)]
enum ApplicationTerminal<R>
where
    R: BehaviorSettlements<Protocol: Protocol<Addr = MailAddr>, Ph = Never>,
{
    Root {
        origin: ActorOrigin<R>,
        terminal: ActorRetirement<R, Self>,
    },
}

fn main() {
    let retired = Arc::new(Semaphore::new(0));
    let retirements = Arc::new(Mutex::new(Vec::new()));
    let unexpected_facts = Arc::new(AtomicUsize::new(0));
    let definition = Accounts {
        retired: Arc::clone(&retired),
        retirements: Arc::clone(&retirements),
        unexpected_facts: Arc::clone(&unexpected_facts),
    };
    let spaces = Spaces {
        root: LocalAddresses::new(),
        accounts: LocalAddresses::new(),
    };
    let capacity = EntityCapacity::new(
        NonZeroUsize::new(2).expect("two is non-zero"),
        NonZeroUsize::new(32).expect("32 is non-zero"),
    );
    let application = App::new(Root.stop_on_shutdown(), spaces)
        .entity_family(
            AccountsRole,
            definition,
            DirectoryConfig::default(),
            capacity,
        )
        .expect("the default directory configuration is valid");

    let ((), terminal, (AccountsRole, (shutdown, metrics), ())): (_, ApplicationTerminal<_>, _) =
        application
            .run_with_entities(move |application| async move {
                let accounts = application.entities(AccountsRole);
                let account = accounts.entity(ACCOUNT_ID);
                let interface = application.interface(account.clone());
                let caller = interface
                    .external::<Replies>()
                    .expect("the external caller is established");

                caller
                    .send(interface.api(), AccountCommand::Deposit(40))
                    .await
                    .expect("the first incarnation accepts its command");
                assert_eq!(
                    application.passivate_entity(AccountsRole, &ACCOUNT_ID),
                    Passivation::Begun
                );
                Arc::clone(&retired)
                    .acquire_owned()
                    .await
                    .expect("the first incarnation retires")
                    .forget();
                caller
                    .send(&account, AccountCommand::Deposit(2))
                    .await
                    .expect("the same stable reference activates a replacement");
                assert_eq!(application.lifecycle().request_shutdown(), Ok(()));
            })
            .expect("the root and Entity family settle");

    assert_eq!(shutdown.represented, 1);
    assert_eq!(metrics.activations, 2);
    assert_eq!(metrics.residents, 0);
    assert_eq!(unexpected_facts.load(Ordering::Relaxed), 0);
    assert_application_stopped(terminal);
    assert_retirements(&retirements);
}

fn assert_application_stopped<R>(terminal: ApplicationTerminal<R>)
where
    R: BehaviorSettlements<Protocol: Protocol<Addr = MailAddr>, Ph = Never>,
    R::Settlements: ClassifySettlement,
{
    let ApplicationTerminal::Root {
        origin,
        terminal:
            ActorRetirement::Completed {
                settlements,
                control,
                user,
                descendants,
                completion,
                ..
            },
    } = terminal
    else {
        panic!("the application root must stop normally")
    };
    assert_eq!(origin.address(), MailAddr::APPLICATION_ROOT);
    let settlement_status = settlements.settlement_status();
    assert_eq!(settlement_status, SettlementStatus::Accepted);
    assert!(control.is_empty());
    assert!(user.is_empty());
    assert!(descendants.is_empty());
    assert_eq!(completion, Completion::Stopped);
}

fn assert_retirements(retirements: &Mutex<Vec<AccountRetirement>>) {
    let retirements = retirements.lock().unwrap_or_else(PoisonError::into_inner);
    assert_eq!(retirements.len(), 2);
    let balances = retirements
        .iter()
        .map(|retirement| {
            let ActorRetirement::Completed {
                behavior,
                settlements,
                ..
            } = retirement
            else {
                panic!("each account incarnation must retire normally")
            };
            let settlement_status = settlements.settlement_status();
            assert_eq!(settlement_status, SettlementStatus::Accepted);
            behavior.base().balance
        })
        .collect::<Vec<_>>();
    assert_eq!(balances, [40, 2]);
}
