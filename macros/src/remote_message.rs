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
use proc_macro2::TokenStream;
use quote::{ToTokens, quote};
use syn::{
    AngleBracketedGenericArguments, GenericArgument, Generics, ItemImpl, LitStr, PathArguments,
    PathSegment, Token, Type,
    parse::{Parse, ParseStream},
    punctuated::Punctuated,
    spanned::Spanned,
};

pub struct RemoteMessageAttrs {
    id: Option<LitStr>,
}

impl Parse for RemoteMessageAttrs {
    fn parse(input: ParseStream) -> syn::Result<Self> {
        if input.is_empty() {
            Ok(RemoteMessageAttrs { id: None })
        } else {
            Ok(RemoteMessageAttrs {
                id: Some(input.parse()?),
            })
        }
    }
}

pub struct RemoteMessage {
    item_impl: ItemImpl,
    actor_ty: Box<Type>,
    actor_generics: Generics,
    message_generics: Punctuated<GenericArgument, Token![,]>,
}

impl RemoteMessage {
    pub fn into_tokens(self, attrs: RemoteMessageAttrs) -> TokenStream {
        let Self {
            item_impl,
            actor_ty,
            actor_generics,
            message_generics,
        } = self;

        let RemoteMessageAttrs { id } = attrs;
        let id = id.map_or_else(
            || {
                let actor_ty = actor_ty.to_token_stream().to_string().replace(' ', "");
                let message_generics = message_generics
                    .to_token_stream()
                    .to_string()
                    .replace(' ', "");
                let actor_generics = actor_generics
                    .to_token_stream()
                    .to_string()
                    .replace(' ', "");
                quote! {
                    ::bombay::remote::_internal::const_str::format!(
                        "{:x}",
                        ::bombay::remote::_internal::const_fnv1a_hash::fnv1a_hash_str_64(concat!(
                            env!("CARGO_PKG_NAME"),
                            "::",
                            env!("CARGO_PKG_VERSION_MAJOR"),
                            "::",
                            module_path!(),
                            "::",
                            #message_generics,
                            "::",
                            #actor_ty,
                            #actor_generics,
                        ))
                    )
                }
            },
            quote::ToTokens::into_token_stream,
        );

        let (impl_generics, ty_generics, where_clause) = actor_generics.split_for_impl();

        quote! {
            #item_impl

            #[automatically_derived]
            impl #impl_generics ::bombay::remote::RemoteMessage<#message_generics> for #actor_ty #ty_generics #where_clause {
                const REMOTE_ID: &'static str = #id;
            }

            const _: () = {
                #[::bombay::remote::_internal::linkme::distributed_slice(
                    ::bombay::remote::_internal::REMOTE_MESSAGES
                )]
                #[linkme(crate = ::bombay::remote::_internal::linkme)]
                static REG: (
                    ::bombay::remote::_internal::RemoteMessageRegistrationID<'static>,
                    ::bombay::remote::_internal::RemoteMessageFns,
                ) = (
                    ::bombay::remote::_internal::RemoteMessageRegistrationID {
                        actor_remote_id: <#actor_ty as ::bombay::remote::RemoteActor>::REMOTE_ID,
                        message_remote_id: <#actor_ty #ty_generics as ::bombay::remote::RemoteMessage<#message_generics>>::REMOTE_ID,
                    },
                    ::bombay::remote::_internal::RemoteMessageFns {
                        ask: (|actor_id: ::bombay::actor::ActorId,
                              msg: ::std::vec::Vec<u8>,
                              mailbox_timeout: ::std::option::Option<::std::time::Duration>,
                              reply_timeout: ::std::option::Option<::std::time::Duration>| {
                                ::std::boxed::Box::pin(::bombay::remote::_internal::ask::<
                                    #actor_ty,
                                    #message_generics,
                                >(
                                    actor_id,
                                    msg,
                                    mailbox_timeout,
                                    reply_timeout,
                                ))
                            }) as ::bombay::remote::_internal::RemoteAskFn,
                        try_ask: (|actor_id: ::bombay::actor::ActorId,
                              msg: ::std::vec::Vec<u8>,
                              reply_timeout: ::std::option::Option<::std::time::Duration>| {
                                ::std::boxed::Box::pin(::bombay::remote::_internal::try_ask::<
                                    #actor_ty,
                                    #message_generics,
                                >(
                                    actor_id,
                                    msg,
                                    reply_timeout,
                                ))
                            }) as ::bombay::remote::_internal::RemoteTryAskFn,
                        tell: (|actor_id: ::bombay::actor::ActorId,
                              msg: ::std::vec::Vec<u8>,
                              mailbox_timeout: ::std::option::Option<::std::time::Duration>| {
                                ::std::boxed::Box::pin(::bombay::remote::_internal::tell::<
                                    #actor_ty,
                                    #message_generics,
                                >(
                                    actor_id,
                                    msg,
                                    mailbox_timeout,
                                ))
                            }) as ::bombay::remote::_internal::RemoteTellFn,
                        try_tell: (|actor_id: ::bombay::actor::ActorId,
                              msg: ::std::vec::Vec<u8>| {
                                ::std::boxed::Box::pin(::bombay::remote::_internal::try_tell::<
                                    #actor_ty,
                                    #message_generics,
                                >(
                                    actor_id,
                                    msg,
                                ))
                            }) as ::bombay::remote::_internal::RemoteTryTellFn,
                    },
                );
            };
        }
    }
}

impl Parse for RemoteMessage {
    fn parse(input: ParseStream) -> syn::Result<Self> {
        let item_impl: ItemImpl = input.parse()?;
        let input_span = item_impl.span();
        let actor_ty = item_impl.self_ty.clone();
        let actor_generics = item_impl.generics.clone();
        let (_, trait_path, _) = item_impl.trait_.as_ref().ok_or_else(|| {
            syn::Error::new(
                input_span,
                "remote message can only be used on an impl for bombay::message::Message",
            )
        })?;
        let trait_path_span = trait_path.span();
        let PathSegment {
            ident: _,
            arguments,
        } = trait_path
            .segments
            .last()
            .ok_or_else(|| syn::Error::new(trait_path_span, "expected trait path"))?
            .clone();
        let PathArguments::AngleBracketed(AngleBracketedGenericArguments {
            args: message_generics,
            ..
        }) = arguments
        else {
            return Err(syn::Error::new(
                trait_path_span,
                "expected angle bracket arguments",
            ));
        };

        Ok(RemoteMessage {
            item_impl,
            actor_ty,
            actor_generics,
            message_generics,
        })
    }
}
