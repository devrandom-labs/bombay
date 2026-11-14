//! A native account family uses the same Application entry and joined outcomes.

use core::num::NonZeroUsize;
use std::sync::{Arc, Mutex, PoisonError};

use bombay::entity::{DirectoryConfig, EntityCapacity};
use bombay::prelude::*;

mod account;
mod accounts;
use account::{Account, AccountCommand};
use accounts::Accounts;

const ACCOUNT_ID: u64 = 7;

struct Root;

#[bombay::actor(message = Never)]
impl Root {}

#[derive(Clone, Copy)]
struct AccountsRole;

bombay::topology! {
    account_application {
        root: Root {}
        hosts { accounts: Account; }
    }
}

#[tokio::main(flavor = "current_thread")]
async fn main() {
    let reports = Arc::new(Mutex::new(Vec::new()));
    let definition = Accounts {
        reports: Arc::clone(&reports),
    };
    let capacity = EntityCapacity::new(
        NonZeroUsize::new(2).expect("two concurrent hydrations"),
        NonZeroUsize::new(32).expect("32 residents"),
    );
    let outcome = Application::with_topology(
        Root.stop_on_shutdown(),
        account_application::Topology::new(),
    )
    .entity_family(
        AccountsRole,
        definition,
        DirectoryConfig::default(),
        capacity,
    )
    .run_with_entities(|application| async move {
        let caller = application
            .sender_to(application.entities(AccountsRole).entity(ACCOUNT_ID))
            .unwrap_or_else(|_| panic!("the account sender is established"));
        caller
            .send(AccountCommand::Deposit(40))
            .await
            .expect("first deposit admitted");
        let mode = application
            .retire_entity(AccountsRole, &ACCOUNT_ID)
            .await
            .expect("the first incarnation retired");
        println!("account retirement: {mode:?}");
        caller
            .send(AccountCommand::Deposit(2))
            .await
            .expect("fresh incarnation admits deposit");
        application.shutdown().expect("root shutdown admitted");
    })
    .await
    .unwrap_or_else(|_| panic!("the application starts"));
    let ((), retirement) = outcome
        .try_into_completed()
        .unwrap_or_else(|_| panic!("the work completes"));
    println!(
        "cleanup: {:?}; notifications: {:?}",
        retirement.cleanup(),
        retirement.notifications()
    );
    let family = retirement
        .family(AccountsRole)
        .expect("the original family result is acquired");
    println!(
        "accounts: {:?}; metrics: {:?}; disposal: {:?}",
        family.shutdown(),
        family.metrics(),
        family.disposal_failure()
    );
    let reports = reports.lock().unwrap_or_else(PoisonError::into_inner);
    for report in reports.iter() {
        println!("{report}");
    }
}
