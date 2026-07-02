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
    DeriveInput, Expr, ExprAssign, ExprLit, Generics, Ident, Lit, LitStr,
    parse::{Parse, ParseStream},
    spanned::Spanned,
};

pub struct DeriveRemoteActor {
    attrs: DeriveRemoteActorAttrs,
    generics: Generics,
    ident: Ident,
}

impl ToTokens for DeriveRemoteActor {
    fn to_tokens(&self, tokens: &mut proc_macro2::TokenStream) {
        let Self {
            attrs,
            generics,
            ident,
        } = self;
        let (impl_generics, ty_generics, where_clause) = generics.split_for_impl();

        let id = if let Some(id) = &attrs.id {
            quote! { #id }
        } else {
            quote! {
                ::std::concat!(::std::module_path!(), "::", ::std::stringify!(#ident))
            }
        };

        tokens.extend(quote! {
            #[automatically_derived]
            impl #impl_generics ::bombay::remote::RemoteActor for #ident #ty_generics #where_clause {
                const REMOTE_ID: &'static str = #id;
            }

            const _: () = {
                #[::bombay::remote::_internal::linkme::distributed_slice(
                    ::bombay::remote::_internal::REMOTE_ACTORS
                )]
                #[linkme(crate = ::bombay::remote::_internal::linkme)]
                static REG: (
                    &'static str,
                    ::bombay::remote::_internal::RemoteActorFns,
                ) = (
                    <#ident #ty_generics as ::bombay::remote::RemoteActor>::REMOTE_ID,
                    ::bombay::remote::_internal::RemoteActorFns {
                        link: (
                            |
                              actor_id: ::bombay::actor::ActorId,
                              sibling_id: ::bombay::actor::ActorId,
                              sibling_remote_id: ::std::borrow::Cow<'static, str>,
                            | {
                                ::std::boxed::Box::pin(::bombay::remote::_internal::link::<
                                    #ident #ty_generics,
                                >(
                                    actor_id,
                                    sibling_id,
                                    sibling_remote_id,
                                ))
                            }) as ::bombay::remote::_internal::RemoteLinkFn,
                        unlink: (
                            |
                              actor_id: ::bombay::actor::ActorId,
                              sibling_id: ::bombay::actor::ActorId,
                            | {
                                ::std::boxed::Box::pin(::bombay::remote::_internal::unlink::<
                                    #ident #ty_generics,
                                >(
                                    actor_id,
                                    sibling_id,
                                ))
                            }) as ::bombay::remote::_internal::RemoteUnlinkFn,
                        signal_link_died: (
                            |
                              dead_actor_id: ::bombay::actor::ActorId,
                              notified_actor_id: ::bombay::actor::ActorId,
                              stop_reason: bombay::error::ActorStopReason,
                            | {
                                ::std::boxed::Box::pin(::bombay::remote::_internal::signal_link_died::<
                                    #ident #ty_generics,
                                >(
                                    dead_actor_id,
                                    notified_actor_id,
                                    stop_reason,
                                ))
                            }) as ::bombay::remote::_internal::RemoteSignalLinkDiedFn,
                    },
                );
            };
        });
    }
}

impl Parse for DeriveRemoteActor {
    fn parse(input: ParseStream) -> syn::Result<Self> {
        let input: DeriveInput = input.parse()?;
        let mut attrs = None;
        for attr in input.attrs {
            if attr.path().is_ident("remote_actor") {
                if attrs.is_some() {
                    return Err(syn::Error::new(
                        attr.span(),
                        "remote_actor attribute already specified",
                    ));
                }
                attrs = Some(attr.parse_args_with(DeriveRemoteActorAttrs::parse)?);
            }
        }
        let ident = input.ident;

        Ok(DeriveRemoteActor {
            attrs: attrs.unwrap_or_default(),
            generics: input.generics,
            ident,
        })
    }
}

#[derive(Default)]
struct DeriveRemoteActorAttrs {
    id: Option<LitStr>,
}

impl Parse for DeriveRemoteActorAttrs {
    fn parse(input: ParseStream) -> syn::Result<Self> {
        let expr: ExprAssign = input
            .parse()
            .map_err(|_| syn::Error::new(input.span(), "expected id = \"...\" expression"))?;
        let Expr::Path(left_path) = expr.left.as_ref() else {
            return Err(syn::Error::new(expr.left.span(), "expected `id`"));
        };
        if !left_path.path.is_ident("id") {
            return Err(syn::Error::new(expr.left.span(), "expected `id`"));
        }
        let Expr::Lit(ExprLit {
            lit: Lit::Str(lit_str),
            ..
        }) = *expr.right
        else {
            return Err(syn::Error::new(
                expr.right.span(),
                "expected a string literal",
            ));
        };

        Ok(DeriveRemoteActorAttrs { id: Some(lit_str) })
    }
}
