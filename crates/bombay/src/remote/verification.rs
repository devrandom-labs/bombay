use core::future::{Future, poll_fn};
use core::pin::{Pin, pin};
use core::task::{Context, Poll};
use std::any::Any;
use std::cell::RefCell;
use std::panic::{AssertUnwindSafe, catch_unwind};

type Panic = Box<dyn Any + Send>;

// Keep acquisition independent of producer ownership: a caller can catch polling
// while retaining the actual producer for a separate, individually caught disposal.
pub(super) fn poll_verification<Verification: Future>(
    verification: Pin<&mut Verification>,
    retained: &mut Option<Verification::Output>,
    context: &mut Context<'_>,
) -> Poll<()> {
    if retained.is_some() {
        return Poll::Ready(());
    }
    match verification.poll(context) {
        Poll::Pending => Poll::Pending,
        Poll::Ready(original) => {
            *retained = Some(original);
            Poll::Ready(())
        }
    }
}

/// Retain a provider's complete verification result before disposing its future.
///
/// The request stays borrowed from its caller. When `retained` is empty, polling
/// this operation invokes `provider` once and polls its actual returned future.
/// Its returned `Ok` or `Err` is stored directly in `retained` before that future
/// is dropped, including when its destructor panics. No conversion or caller
/// callback runs between receiving the result and storing it.
///
/// An occupied slot, including `Some(Err(_))`, suppresses provider invocation.
/// Evaluation and destruction of the caller's closure captures still follow
/// ordinary Rust rules. Completion returns unit; inspect the retained result to
/// learn the provider's outcome. This operation does not establish permission,
/// replay acceptance or actor-mailbox admission.
///
/// Keep this operation pinned outside a temporary borrowed wait to cancel that
/// wait without disposing the provider operation. Dropping this operation itself
/// after `Pending` drops its provider future; the outside request and slot remain.
/// Provider construction, polling and destruction can panic. This function does
/// not catch those panics or recover a result erased inside a provider adapter.
pub async fn verify<'request, Request: ?Sized, Receipt, Error, Verification>(
    request: &'request Request,
    retained: &mut Option<Result<Receipt, Error>>,
    provider: impl FnOnce(&'request Request) -> Verification,
) where
    Verification: Future<Output = Result<Receipt, Error>>,
{
    if retained.is_none() {
        let mut verification = pin!(provider(request));
        poll_fn(|context| poll_verification(verification.as_mut(), retained, context)).await;
    }
}

// Own the actual producer, not its result. Each instance catches its own poll
// and disposal separately, so one provider cannot preempt sibling disposal.
struct Provider<'request, Production: Future> {
    production: Option<Pin<Box<Production>>>,
    retained: &'request RefCell<Option<Production::Output>>,
    polling: &'request RefCell<Option<Panic>>,
    disposal: &'request RefCell<Option<Panic>>,
}
impl<Production: Future> Future for Provider<'_, Production> {
    type Output = ();
    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<()> {
        let Self {
            production,
            retained,
            polling,
            ..
        } = &mut *self;
        let result = catch_unwind(AssertUnwindSafe(|| {
            let Some(production) = production.as_mut() else {
                return Poll::Ready(());
            };
            poll_verification(production.as_mut(), &mut retained.borrow_mut(), cx)
        }));
        match result {
            Ok(original) => original,
            Err(original) => {
                *polling.borrow_mut() = Some(original);
                Poll::Ready(())
            }
        }
    }
}
impl<Production: Future> Drop for Provider<'_, Production> {
    fn drop(&mut self) {
        if let Some(production) = self.production.take()
            && let Err(original) = catch_unwind(AssertUnwindSafe(|| drop(production)))
        {
            *self.disposal.borrow_mut() = Some(original);
        }
    }
}
pub(super) async fn provide<Production: Future>(
    factory: impl FnOnce() -> Production,
    retained: &RefCell<Option<Production::Output>>,
    construction: &RefCell<Option<Panic>>,
    polling: &RefCell<Option<Panic>>,
    disposal: &RefCell<Option<Panic>>,
    joined: impl Future,
) {
    match catch_unwind(AssertUnwindSafe(factory)) {
        Ok(production) => {
            let mut provider = pin!(Provider {
                production: Some(Box::pin(production)),
                retained,
                polling,
                disposal,
            });
            tokio::select! {
                biased;
                () = &mut provider => {},
                _ = joined => {},
            }
        }
        Err(original) => *construction.borrow_mut() = Some(original),
    }
}

#[cfg(test)]
mod tests;
