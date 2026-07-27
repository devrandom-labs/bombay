mod derive_msg;

use derive_msg::DeriveMsg;
use proc_macro::TokenStream;
use quote::ToTokens;
use syn::parse_macro_input;

/// Derive the [`Msg`](https://docs.rs/bombay/latest/bombay/message/trait.Msg.html)
/// marker trait and emit a compile-time slot-size tripwire.
///
/// A mailbox queues messages by value, so a fat inline variant taxes every
/// queue slot. This derive trips the build when `size_of` of the message exceeds
/// its `Msg::SLOT_BUDGET` (default 256 B). Box the largest variant to fix it, or
/// raise the budget with `#[msg(budget = N)]`.
///
/// Within budget — compiles:
/// ```
/// use bombay::message::Msg;
/// #[derive(bombay_macros::Msg)]
/// enum Ok { Small(u64) }
/// ```
///
/// A fat inline variant trips the budget:
/// ```compile_fail
/// use bombay::message::Msg;
/// #[derive(bombay_macros::Msg)]
/// enum Bad { Bulk([u8; 4096]) }
/// ```
///
/// Boxing the fat variant fixes it (as `Signal` boxes `LinkDied`):
/// ```
/// use bombay::message::Msg;
/// #[derive(bombay_macros::Msg)]
/// enum Fixed { Bulk(Box<[u8; 4096]>) }
/// ```
///
/// Or raise the budget for a deliberately large message:
/// ```
/// use bombay::message::Msg;
/// #[derive(bombay_macros::Msg)]
/// #[msg(budget = 8192)]
/// enum Big { Bulk([u8; 4096]) }
/// ```
///
/// The derive needs a concrete type — a generic is rejected:
/// ```compile_fail
/// use bombay::message::Msg;
/// #[derive(bombay_macros::Msg)]
/// enum Generic<T> { A(T) }
/// ```
///
/// Unions are rejected (structs and enums only):
/// ```compile_fail
/// use bombay::message::Msg;
/// #[derive(bombay_macros::Msg)]
/// union U { a: u32, b: u64 }
/// ```
#[proc_macro_derive(Msg, attributes(msg))]
pub fn derive_msg(input: TokenStream) -> TokenStream {
    let derive_msg = parse_macro_input!(input as DeriveMsg);
    TokenStream::from(derive_msg.into_token_stream())
}
