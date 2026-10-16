use core::fmt;
use std::sync::{Arc, Mutex, PoisonError};

use bombay::ActorRetirementReport;
use bombay::actors::ActorExt;
use bombay::behavior::{BehaviorBase, Never};
use bombay::entity::{
    ActivationId, AdmissionFailure, DrainFailure, EntityActivationError, EntityDefinition, EntityId,
};
use bombay::prelude::{ActorRetirement, StopOnShutdown};

use super::account::{Account, AccountCommand};
use super::account_application;

type AccountRetirement =
    Result<ActorRetirement<StopOnShutdown<Account>, Never, Vec<Never>>, tokio::task::JoinError>;

// The definition retains every original callback payload for application reporting.
// These records contain data; Application remains the lifecycle authority.
pub(super) enum AccountReport {
    Startup {
        id: EntityId<u64>,
        attempt: ActivationId,
        failure: EntityActivationError<Never, StopOnShutdown<Account>, Never, Vec<Never>>,
    },
    Admission {
        id: EntityId<u64>,
        rejection: AdmissionFailure<AccountCommand>,
    },
    Drain {
        id: EntityId<u64>,
        attempt: ActivationId,
        reason: DrainFailure,
    },
    Retirement {
        id: EntityId<u64>,
        attempt: ActivationId,
        result: AccountRetirement,
        report: ActorRetirementReport,
    },
}

pub(super) struct Accounts {
    pub(super) reports: Arc<Mutex<Vec<AccountReport>>>,
}

impl EntityDefinition for Accounts {
    type Id = u64;
    type Behavior = StopOnShutdown<Account>;
    type Hosts = account_application::Topology;
    type HydrationError = Never;
    type Terminal = Never;
    type ChildFailures = Vec<Never>;

    #[expect(
        clippy::unused_async_trait_impl,
        reason = "hydration work begins when the native port polls its future"
    )]
    async fn hydrate(&self, _: EntityId<u64>) -> Result<Self::Behavior, Never> {
        Ok(Account { balance: 0 }.stop_on_shutdown())
    }

    fn activation_failed(
        &self,
        id: EntityId<u64>,
        attempt: ActivationId,
        failure: EntityActivationError<Never, Self::Behavior, Never, Vec<Never>>,
    ) {
        self.reports
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .push(AccountReport::Startup {
                id,
                attempt,
                failure,
            });
    }

    fn admission_refused(&self, id: EntityId<u64>, rejection: AdmissionFailure<AccountCommand>) {
        self.reports
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .push(AccountReport::Admission { id, rejection });
    }

    fn forced_retirement(&self, id: &EntityId<u64>, attempt: ActivationId, reason: DrainFailure) {
        self.reports
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .push(AccountReport::Drain {
                id: *id,
                attempt,
                reason,
            });
    }

    fn retired(
        &self,
        id: &EntityId<u64>,
        attempt: ActivationId,
        result: AccountRetirement,
        report: ActorRetirementReport,
    ) {
        self.reports
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .push(AccountReport::Retirement {
                id: *id,
                attempt,
                result,
                report,
            });
    }
}

impl fmt::Display for AccountReport {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Retirement {
                id,
                attempt,
                result,
                report,
            } => match result {
                Ok(retirement) => {
                    let balance = retirement.behavior().map(|account| account.base().balance);
                    write!(
                        formatter,
                        "account {} attempt {}: balance {balance:?}; {report:?}",
                        id.get(),
                        attempt.get()
                    )
                }
                Err(error) => write!(
                    formatter,
                    "account {} attempt {}: join failed: {error}; {report:?}",
                    id.get(),
                    attempt.get()
                ),
            },
            Self::Startup {
                id,
                attempt,
                failure,
            } => {
                let cause = match failure {
                    EntityActivationError::ResidentCapacity => "resident capacity",
                    EntityActivationError::Hydration(never) => match *never {},
                    EntityActivationError::AllocationRejected { .. } => "address allocation",
                    EntityActivationError::Launch { .. } => "actor launch",
                };
                write!(
                    formatter,
                    "account {} attempt {}: startup rejected ({cause})",
                    id.get(),
                    attempt.get()
                )
            }
            Self::Admission { id, rejection } => write!(
                formatter,
                "account {}: admission rejected: {rejection:?}",
                id.get()
            ),
            Self::Drain {
                id,
                attempt,
                reason,
            } => write!(
                formatter,
                "account {} attempt {}: forced drain: {reason:?}",
                id.get(),
                attempt.get()
            ),
        }
    }
}
