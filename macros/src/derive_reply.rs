// --- #61 quarantine (vendored kameo, pre-god-level-bar) -------------------
// This file predates the workspace god-level clippy bar (root Cargo.toml).
// It is held at the prior standard and is cleaned or deleted file-by-file
// under M1/M7. NEW code is NOT exempt — remove this block when the file is
// brought up to the bar or dropped. De-quarantine checklist: issue #61.
#![allow(
    clippy::all,
    clippy::pedantic,
    clippy::nursery,
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::todo,
    clippy::unimplemented,
    clippy::dbg_macro,
    clippy::print_stdout,
    clippy::print_stderr,
    clippy::disallowed_methods,
    clippy::clone_on_ref_ptr,
    clippy::as_conversions,
    clippy::str_to_string,
    clippy::implicit_clone,
    clippy::shadow_reuse,
    clippy::shadow_same,
    clippy::shadow_unrelated,
    clippy::allow_attributes_without_reason,
    reason = "Vendored kameo predating the #61 god-level clippy bar; held at the prior standard, cleaned or deleted file-by-file under M1/M7. New code is not exempt. See #61."
)]
use quote::{ToTokens, quote};
use syn::{
    DeriveInput, Generics, Ident,
    parse::{Parse, ParseStream},
};

pub struct DeriveReply {
    ident: Ident,
    generics: Generics,
}

impl ToTokens for DeriveReply {
    fn to_tokens(&self, tokens: &mut proc_macro2::TokenStream) {
        let Self { ident, generics } = self;
        let (impl_generics, ty_generics, where_clause) = generics.split_for_impl();

        tokens.extend(quote! {
            #[automatically_derived]
            impl #impl_generics ::bombay::Reply for #ident #ty_generics #where_clause {
                type Ok = Self;
                type Error = ::bombay::error::Infallible;
                type Value = Self;

                #[inline]
                fn to_result(self) -> ::std::result::Result<Self::Ok, Self::Error> {
                    ::std::result::Result::Ok(self)
                }

                #[inline]
                fn into_any_err(self) -> ::std::option::Option<::std::boxed::Box<dyn ::bombay::reply::ReplyError>> {
                    ::std::option::Option::None
                }

                #[inline]
                fn into_value(self) -> Self::Value {
                    self
                }
            }
        });
    }
}

impl Parse for DeriveReply {
    fn parse(input: ParseStream) -> syn::Result<Self> {
        let input: DeriveInput = input.parse()?;
        let ident = input.ident;
        let generics = input.generics;

        Ok(DeriveReply { ident, generics })
    }
}
