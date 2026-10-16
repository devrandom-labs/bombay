use behavior::Never;
use tokio::sync::oneshot::error::RecvError;

use super::execution::{ApplicationCleanupError, ApplicationWorkPresence};

/// Caller-local work disposition alongside the actual unit application cleanup.
///
/// Work values never move into the executor-owned cleanup task. `Unstarted`
/// returns the original untouched application and callable. `Prepared` retains
/// the actual actor and Spaces before handoff; `NotInvoked` retains the callable
/// while setup or startup owns consumed inputs. `Interrupted` does not claim
/// recovery of a callable or work future consumed by invocation.
/// Cleanup failure retains its actual publication or cleanup-task failure owner.
/// Each original root task error remains `ActorRetirement::ActorTaskFailed` in its joined-root product.
/// Entity receiving additionally returns independently held original root and family receipts.
/// Those actual receiving errors never substitute a no-root or completed-family classification.
/// For genuine absent work, `execute` and async `run` retain `Option<Never>`:
/// `None` is no supplied callable/output, and Completed means actual startup grant.
/// Supplied `execute_with` retains bare original Work and Ready Output.
/// Cold HTTP has actual unit work absence; Prepared/NotInvoked owns the bare acquired pair.
#[must_use = "original application work inputs and cleanup require explicit custody"]
pub enum WorkOutcome<
    ApplicationInputs,
    Work,
    Output,
    Cleanup,
    StagingInputs = Never,
    PreparedInputs = Never,
    ColdWork = Work,
> {
    /// Staging stopped before actor startup; exact cold partial inputs and callable survive.
    StagingRejected { inputs: StagingInputs },
    Unstarted {
        application: ApplicationInputs,
        work: ColdWork,
    },
    /// Prepared originals remain available before this owner transfers its actor.
    /// The original native setup cause propagates to the unwinding caller.
    Prepared {
        inputs: PreparedInputs,
        work: Work,
        cleanup: Result<Cleanup, ApplicationCleanupError>,
    },
    NotInvoked {
        work: Work,
        startup_error: Option<RecvError>,
        cleanup: Result<Cleanup, ApplicationCleanupError>,
    },
    Completed {
        output: Output,
        cleanup: Result<Cleanup, ApplicationCleanupError>,
    },
    Interrupted {
        cleanup: Result<Cleanup, ApplicationCleanupError>,
    },
}

impl<Inputs, Work, Output, Cleanup, StagingInputs, PreparedInputs, ColdWork>
    WorkOutcome<Inputs, Work, Output, Cleanup, StagingInputs, PreparedInputs, ColdWork>
{
    /// Borrow the original completed work value, including a domain failure.
    ///
    /// Presence says only that work returned. Cleanup and actor retirement
    /// remain independent facts; inspection consumes neither.
    #[must_use]
    pub const fn work_output(&self) -> Option<&Output> {
        match self {
            Self::Completed { output, .. } => Some(output),
            Self::StagingRejected { .. }
            | Self::Unstarted { .. }
            | Self::Prepared { .. }
            | Self::NotInvoked { .. }
            | Self::Interrupted { .. } => None,
        }
    }

    /// Borrow the received cleanup result without consuming its evidence.
    ///
    /// Cold declaration and staging rejection have no cleanup result. An
    /// `Ok` cleanup value still owns the actor's actual retirement disposition;
    /// it does not itself certify successful actor initialization or execution.
    #[must_use]
    pub const fn cleanup(&self) -> Option<&Result<Cleanup, ApplicationCleanupError>> {
        match self {
            Self::Prepared { cleanup, .. }
            | Self::NotInvoked { cleanup, .. }
            | Self::Completed { cleanup, .. }
            | Self::Interrupted { cleanup } => Some(cleanup),
            Self::StagingRejected { .. } | Self::Unstarted { .. } => None,
        }
    }

    /// Consume completed work together with its independent cleanup result.
    ///
    /// Cleanup failure accompanies the original output on the successful side
    /// of this selection. The caller retains responsibility for both values.
    ///
    /// # Errors
    /// Returns the entire original outcome when work has not returned. No
    /// original declaration, callable, prepared input or cleanup is discarded.
    pub fn try_into_completed(
        self,
    ) -> Result<(Output, Result<Cleanup, ApplicationCleanupError>), Self> {
        match self {
            Self::Completed { output, cleanup } => Ok((output, cleanup)),
            outcome @ (Self::StagingRejected { .. }
            | Self::Unstarted { .. }
            | Self::Prepared { .. }
            | Self::NotInvoked { .. }
            | Self::Interrupted { .. }) => Err(outcome),
        }
    }
}

impl<Inputs, Work, Invoke, Output, Cleanup, StagingInputs, PreparedInputs>
    WorkOutcome<
        Inputs,
        (ApplicationWorkPresence<Work, Never>, Invoke),
        ApplicationWorkPresence<Output, Never>,
        Cleanup,
        StagingInputs,
        PreparedInputs,
        (Work, Invoke),
    >
{
    pub(in crate::application) fn into_supplied(
        self,
    ) -> WorkOutcome<Inputs, Work, Output, Cleanup, StagingInputs, PreparedInputs> {
        match self {
            Self::StagingRejected { inputs } => WorkOutcome::StagingRejected { inputs },
            Self::Unstarted {
                application,
                work: (work, invoke),
            } => {
                drop(invoke);
                WorkOutcome::Unstarted { application, work }
            }
            Self::Prepared {
                work,
                inputs,
                cleanup,
            } => {
                let (work, invoke) = work;
                let work = work.into_supplied();
                drop(invoke);
                WorkOutcome::Prepared {
                    work,
                    inputs,
                    cleanup,
                }
            }
            Self::NotInvoked {
                work,
                startup_error,
                cleanup,
            } => {
                let (work, invoke) = work;
                let work = work.into_supplied();
                drop(invoke);
                WorkOutcome::NotInvoked {
                    work,
                    startup_error,
                    cleanup,
                }
            }
            Self::Completed { output, cleanup } => WorkOutcome::Completed {
                output: output.into_supplied(),
                cleanup,
            },
            Self::Interrupted { cleanup } => WorkOutcome::Interrupted { cleanup },
        }
    }
}

#[cfg(feature = "axum")]
impl<Inputs, Work, Invoke, Output, Cleanup, StagingInputs, PreparedInputs>
    WorkOutcome<
        Inputs,
        (ApplicationWorkPresence<Work, Never>, Invoke),
        ApplicationWorkPresence<Output, Never>,
        Cleanup,
        StagingInputs,
        PreparedInputs,
        (),
    >
{
    pub(in crate::application) fn into_http(
        self,
    ) -> WorkOutcome<Inputs, Work, Output, Cleanup, StagingInputs, PreparedInputs, ()> {
        match self {
            Self::StagingRejected { inputs } => WorkOutcome::StagingRejected { inputs },
            Self::Unstarted {
                application,
                work: (),
            } => WorkOutcome::Unstarted {
                application,
                work: (),
            },
            Self::Prepared {
                work,
                inputs,
                cleanup,
            } => {
                let (work, invoke) = work;
                let work = work.into_supplied();
                drop(invoke);
                WorkOutcome::Prepared {
                    work,
                    inputs,
                    cleanup,
                }
            }
            Self::NotInvoked {
                work,
                startup_error,
                cleanup,
            } => {
                let (work, invoke) = work;
                let work = work.into_supplied();
                drop(invoke);
                WorkOutcome::NotInvoked {
                    work,
                    startup_error,
                    cleanup,
                }
            }
            Self::Completed { output, cleanup } => WorkOutcome::Completed {
                output: output.into_supplied(),
                cleanup,
            },
            Self::Interrupted { cleanup } => WorkOutcome::Interrupted { cleanup },
        }
    }
}

impl<Inputs, Invoke, Cleanup, StagingInputs, PreparedInputs>
    WorkOutcome<
        Inputs,
        (ApplicationWorkPresence<Never, ()>, Invoke),
        ApplicationWorkPresence<Never, ()>,
        Cleanup,
        StagingInputs,
        PreparedInputs,
        Invoke,
    >
{
    pub(in crate::application) fn into_absent(
        self,
    ) -> WorkOutcome<Inputs, Option<Never>, Option<Never>, Cleanup, StagingInputs, PreparedInputs>
    {
        match self {
            Self::StagingRejected { inputs } => WorkOutcome::StagingRejected { inputs },
            Self::Unstarted {
                application,
                work: invoke,
            } => {
                drop(invoke);
                WorkOutcome::Unstarted {
                    application,
                    work: None,
                }
            }
            Self::Prepared {
                work,
                inputs,
                cleanup,
            } => {
                let (work, invoke) = work;
                drop(invoke);
                match work {
                    ApplicationWorkPresence::Absent(()) => WorkOutcome::Prepared {
                        work: None,
                        inputs,
                        cleanup,
                    },
                    ApplicationWorkPresence::Supplied(never) => match never {},
                }
            }
            Self::NotInvoked {
                work,
                startup_error,
                cleanup,
            } => {
                let (work, invoke) = work;
                drop(invoke);
                match work {
                    ApplicationWorkPresence::Absent(()) => WorkOutcome::NotInvoked {
                        work: None,
                        startup_error,
                        cleanup,
                    },
                    ApplicationWorkPresence::Supplied(never) => match never {},
                }
            }
            Self::Completed { output, cleanup } => match output {
                ApplicationWorkPresence::Absent(()) => WorkOutcome::Completed {
                    output: None,
                    cleanup,
                },
                ApplicationWorkPresence::Supplied(never) => match never {},
            },
            Self::Interrupted { cleanup } => WorkOutcome::Interrupted { cleanup },
        }
    }
}
