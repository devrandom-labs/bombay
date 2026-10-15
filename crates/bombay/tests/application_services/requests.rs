use bombay::behavior::{
    ActionItem, InterpretItem, InterpretationProgress, InterpreterRequest, ItemSettlement, Never,
    NoBirthProtocols, NoReturnToEmitter, finish_item, prepare_item,
};
use core::future::Future;
use std::env::{self, VarError};
use std::fs::File;
use std::io;
use std::panic::resume_unwind;
use std::path::PathBuf;
use std::sync::Arc;
use tokio::sync::{Notify, oneshot};

pub(super) struct OpenFile {
    pub(super) path: PathBuf,
    pub(super) access: FileAccess,
}

#[allow(
    dead_code,
    reason = "independent test binaries reuse the same request; the native service target exercises every access alternative"
)]
pub(super) enum FileAccess {
    Ready,
    Waiting {
        entered: Arc<Notify>,
        permission: oneshot::Receiver<()>,
    },
    Panicking {
        entered: Arc<Notify>,
        cause: oneshot::Receiver<Box<[u8; 2]>>,
    },
}

pub(super) struct ReadEnvironment {
    pub(super) name: String,
}

pub(super) struct Services;

impl InterpreterRequest for OpenFile {
    type ReturnToEmitter = NoReturnToEmitter;
    type LogicalProtocols = NoBirthProtocols;
}

impl InterpreterRequest for ReadEnvironment {
    type ReturnToEmitter = NoReturnToEmitter;
    type LogicalProtocols = NoBirthProtocols;
}

impl ActionItem for OpenFile {
    type Accepted = File;
    type Rejection = io::Error;
    type Prerequisite = Never;
    type Custody = (Option<Self>, Option<Self::Reply>);
    type Input<'a>
        = &'a mut Option<Self>
    where
        Self: 'a;
    type Reply = ItemSettlement<Self, Self::Accepted, Self::Rejection, Self::Prerequisite>;

    fn prepare_interpretation(
        progress: &mut Option<InterpretationProgress<Self, Self::Custody, Self::Reply>>,
    ) {
        prepare_item::<Self>(progress);
    }

    fn interpretation_input<'a>(
        custody: &'a mut Self::Custody,
    ) -> Option<(Self::Input<'a>, &'a mut Option<Self::Reply>)>
    where
        Self: 'a,
    {
        match custody {
            (input @ Some(_), received @ None) => Some((input, received)),
            _ => None,
        }
    }

    fn finish_interpretation(
        progress: &mut Option<InterpretationProgress<Self, Self::Custody, Self::Reply>>,
    ) {
        finish_item::<Self>(progress);
    }

    fn retain_accepted(file: File) -> Option<File> {
        Some(file)
    }
}

impl ActionItem for ReadEnvironment {
    type Accepted = String;
    type Rejection = VarError;
    type Prerequisite = Never;
    type Custody = (Option<Self>, Option<Self::Reply>);
    type Input<'a>
        = &'a mut Option<Self>
    where
        Self: 'a;
    type Reply = ItemSettlement<Self, Self::Accepted, Self::Rejection, Self::Prerequisite>;

    fn prepare_interpretation(
        progress: &mut Option<InterpretationProgress<Self, Self::Custody, Self::Reply>>,
    ) {
        prepare_item::<Self>(progress);
    }

    fn interpretation_input<'a>(
        custody: &'a mut Self::Custody,
    ) -> Option<(Self::Input<'a>, &'a mut Option<Self::Reply>)>
    where
        Self: 'a,
    {
        match custody {
            (input @ Some(_), received @ None) => Some((input, received)),
            _ => None,
        }
    }

    fn finish_interpretation(
        progress: &mut Option<InterpretationProgress<Self, Self::Custody, Self::Reply>>,
    ) {
        finish_item::<Self>(progress);
    }

    fn retain_accepted(value: String) -> Option<String> {
        Some(value)
    }
}

impl<Event, Path> InterpretItem<OpenFile, Event, Path> for Services {
    #[expect(
        clippy::manual_async_fn,
        reason = "async fn captures Event and Path lifetimes that the receiving-loan contract does not require"
    )]
    fn interpret_item<'a>(
        &'a mut self,
        input: <OpenFile as ActionItem>::Input<'a>,
        received: &'a mut Option<<OpenFile as ActionItem>::Reply>,
    ) -> impl Future<Output = ()> + Send + 'a
    where
        OpenFile: 'a,
    {
        async move {
            if received.is_some() {
                return;
            }
            let Some(request) = input.as_mut() else {
                return;
            };
            let permission = match &mut request.access {
                FileAccess::Ready => Ok(()),
                FileAccess::Waiting {
                    entered,
                    permission,
                } => {
                    entered.notify_one();
                    permission.await.map_err(io::Error::other)
                }
                FileAccess::Panicking { entered, cause } => {
                    entered.notify_one();
                    let cause = cause
                        .await
                        .expect("the native test injects its actual cause");
                    resume_unwind(cause);
                }
            };
            let Some(request) = input.take() else {
                return;
            };
            *received = Some(match permission.and_then(|()| File::open(&request.path)) {
                Ok(file) => ItemSettlement::Accepted(file),
                Err(reason) => ItemSettlement::Rejected {
                    item: request,
                    reason,
                },
            });
        }
    }
}

impl<Event, Path> InterpretItem<ReadEnvironment, Event, Path> for Services {
    #[expect(
        clippy::manual_async_fn,
        reason = "async fn captures Event and Path lifetimes that the receiving-loan contract does not require"
    )]
    fn interpret_item<'a>(
        &'a mut self,
        input: <ReadEnvironment as ActionItem>::Input<'a>,
        received: &'a mut Option<<ReadEnvironment as ActionItem>::Reply>,
    ) -> impl Future<Output = ()> + Send + 'a
    where
        ReadEnvironment: 'a,
    {
        async move {
            if received.is_some() {
                return;
            }
            let Some(request) = input.take() else {
                return;
            };
            *received = Some(match env::var(&request.name) {
                Ok(value) => ItemSettlement::Accepted(value),
                Err(reason) => ItemSettlement::Rejected {
                    item: request,
                    reason,
                },
            });
        }
    }
}
