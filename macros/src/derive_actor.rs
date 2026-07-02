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
    DeriveInput, Generics, Ident, LitStr, Token, custom_keyword,
    parse::{Parse, ParseStream},
    punctuated::Punctuated,
    spanned::Spanned,
};

pub struct DeriveActor {
    attrs: DeriveActorAttrs,
    ident: Ident,
    generics: Generics,
}

impl ToTokens for DeriveActor {
    fn to_tokens(&self, tokens: &mut proc_macro2::TokenStream) {
        let Self {
            attrs,
            ident,
            generics,
        } = self;
        let name = match &attrs.name {
            Some(s) => s.value(),
            None => ident.to_string(),
        };
        let (impl_generics, ty_generics, where_clause) = generics.split_for_impl();

        tokens.extend(quote! {
            #[automatically_derived]
            impl #impl_generics ::bombay::actor::Actor for #ident #ty_generics #where_clause {
                type Args = Self;
                type Error = ::bombay::error::Infallible;

                fn name() -> &'static str {
                    #name
                }

                async fn on_start(
                    state: Self::Args,
                    _actor_ref: ::bombay::actor::ActorRef<Self>,
                ) -> ::std::result::Result<Self, Self::Error> {
                    ::std::result::Result::Ok(state)
                }
            }
        });
    }
}

impl Parse for DeriveActor {
    fn parse(input: ParseStream) -> syn::Result<Self> {
        let input: DeriveInput = input.parse()?;
        let ident = input.ident;
        let generics = input.generics;
        let mut attrs = None;
        for attr in input.attrs {
            if attr.path().is_ident("actor") {
                if attrs.is_some() {
                    return Err(syn::Error::new(
                        attr.span(),
                        "actor attribute already specified",
                    ));
                }
                attrs = Some(attr.parse_args_with(DeriveActorAttrs::parse)?);
            }
        }

        Ok(DeriveActor {
            attrs: attrs.unwrap_or_default(),
            ident,
            generics,
        })
    }
}

#[derive(Default)]
struct DeriveActorAttrs {
    name: Option<LitStr>,
}

impl Parse for DeriveActorAttrs {
    fn parse(input: ParseStream) -> syn::Result<Self> {
        #[derive(Debug)]
        enum Attr {
            Name(name, LitStr),
        }
        let attrs: Punctuated<Attr, Token![,]> =
            Punctuated::parse_terminated_with(input, |input| {
                let lookahead = input.lookahead1();
                if lookahead.peek(name) {
                    let key: name = input.parse()?;
                    let _: Token![=] = input.parse()?;
                    let name: LitStr = input.parse()?;
                    Ok(Attr::Name(key, name))
                } else {
                    Err(lookahead.error())
                }
            })?;

        let mut name = None;

        for attr in attrs {
            match attr {
                Attr::Name(key, s) => {
                    if name.is_none() {
                        name = Some(s);
                    } else {
                        return Err(syn::Error::new(key.span, "name already set"));
                    }
                }
            }
        }

        Ok(DeriveActorAttrs { name })
    }
}

custom_keyword!(name);
