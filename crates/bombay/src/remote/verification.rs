use core::future::{Future, poll_fn};
use core::pin::pin;
use core::task::Poll;

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
        poll_fn(|context| match verification.as_mut().poll(context) {
            Poll::Pending => Poll::Pending,
            Poll::Ready(original) => {
                *retained = Some(original);
                Poll::Ready(())
            }
        })
        .await;
    }
}
