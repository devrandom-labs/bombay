//! Mechanical derives for Bombay's concrete local runtime composition.

use proc_macro::TokenStream;
use proc_macro_crate::{FoundCrate, crate_name};
use proc_macro2::{Span, TokenStream as TokenStream2};
use quote::quote;
use syn::parse::{Parse, ParseStream};
use syn::punctuated::Punctuated;
use syn::{
    Attribute, Data, DeriveInput, Error, Fields, FnArg, Ident, ImplItem, ItemImpl, Meta, Result,
    Token, Type, Variant, Visibility, braced, parse_macro_input, parse_quote,
};

fn crate_path(found: FoundCrate) -> TokenStream2 {
    match found {
        FoundCrate::Itself => quote!(crate),
        FoundCrate::Name(name) => {
            let name = if name == "bombay_rs" {
                "bombay".to_owned()
            } else {
                name
            };
            let name = syn::Ident::new(&name, Span::call_site());
            quote!(::#name)
        }
    }
}

fn bombay_crate() -> syn::Result<TokenStream2> {
    if std::env::var("CARGO_PKG_NAME").as_deref() == Ok("bombay-rs") {
        return if std::env::var("CARGO_CRATE_NAME").as_deref() == Ok("bombay") {
            Ok(quote!(crate))
        } else {
            Ok(quote!(::bombay))
        };
    }
    crate_name("bombay-rs").map(crate_path).map_err(|_| {
        Error::new(
            Span::call_site(),
            "could not resolve the `bombay-rs` facade crate",
        )
    })
}

struct ActorArgs {
    message: Option<Type>,
    forwarded: Vec<TokenStream2>,
}

impl Parse for ActorArgs {
    fn parse(input: ParseStream<'_>) -> Result<Self> {
        let mut message = None;
        let mut forwarded = Vec::new();
        let mut keys = std::collections::BTreeSet::new();

        while !input.is_empty() {
            let key: Ident = input.parse()?;
            let key_name = key.to_string();
            if !keys.insert(key_name.clone()) {
                return Err(Error::new_spanned(key, "duplicate actor argument"));
            }
            input.parse::<Token![=]>()?;

            match key_name.as_str() {
                "message" => message = Some(input.parse()?),
                "error" => {
                    let ty: Type = input.parse()?;
                    forwarded.push(quote!(error = #ty));
                }
                "sends" => {
                    let visibility: Visibility = input.parse()?;
                    if input.peek(syn::token::Brace) {
                        let content;
                        braced!(content in input);
                        let fields: TokenStream2 = content.parse()?;
                        forwarded.push(quote!(sends = #visibility { #fields }));
                    } else {
                        if !matches!(visibility, Visibility::Inherited) {
                            return Err(input.error("send-product visibility requires named lanes"));
                        }
                        let ty: Type = input.parse()?;
                        forwarded.push(quote!(sends = #ty));
                    }
                }
                "births" => {
                    if input.peek(syn::token::Brace) {
                        let content;
                        braced!(content in input);
                        let fields: TokenStream2 = content.parse()?;
                        forwarded.push(quote!(births = { #fields }));
                    } else {
                        let ty: Type = input.parse()?;
                        forwarded.push(quote!(births = #ty));
                    }
                }
                "creation_settlements" => {
                    let disposition: Ident = input.parse()?;
                    forwarded.push(quote!(creation_settlements = #disposition));
                }
                "addr" => {
                    return Err(Error::new_spanned(
                        key,
                        "Bombay infers the actor address; remove `addr`",
                    ));
                }
                _ => return Err(Error::new_spanned(key, "unknown actor argument")),
            }

            if input.peek(Token![,]) {
                input.parse::<Token![,]>()?;
            } else if !input.is_empty() {
                return Err(input.error("expected `,` between actor arguments"));
            }
        }

        Ok(Self { message, forwarded })
    }
}

fn typed_argument(argument: &FnArg, message: &str) -> Result<Type> {
    let FnArg::Typed(argument) = argument else {
        return Err(Error::new_spanned(argument, message));
    };
    Ok((*argument.ty).clone())
}

/// Turn an ordinary state fold into the exact Behavior-owned actor type.
///
/// This facade infers the message and optional sender types from `receive`,
/// supplies Bombay's `MailAddr` for a sender-free fold, and delegates all
/// protocol, effect, birth, error, and transition semantics to the selected
/// `#[behavior]` macro.
#[proc_macro_attribute]
pub fn actor(arguments: TokenStream, item: TokenStream) -> TokenStream {
    let arguments = parse_macro_input!(arguments as ActorArgs);
    let implementation = parse_macro_input!(item as ItemImpl);
    expand_actor(arguments, implementation)
        .unwrap_or_else(Error::into_compile_error)
        .into()
}

fn expand_actor(arguments: ActorArgs, mut implementation: ItemImpl) -> Result<TokenStream2> {
    if implementation.trait_.is_some() {
        return Err(Error::new_spanned(
            &implementation,
            "#[actor] applies to an inherent impl",
        ));
    }

    let bombay = bombay_crate()?;
    let (addr, message) = actor_signature(&arguments, &mut implementation, &bombay)?;
    let forwarded = arguments.forwarded;

    Ok(quote! {
        #[#bombay::behavior::behavior(
            addr = #addr,
            message = #message
            #(, #forwarded)*
        )]
        #implementation
    })
}

fn actor_signature(
    arguments: &ActorArgs,
    implementation: &mut ItemImpl,
    bombay: &TokenStream2,
) -> Result<(Type, Type)> {
    let receive_index = implementation
        .items
        .iter()
        .position(|item| matches!(item, ImplItem::Fn(method) if method.sig.ident == "receive"));

    if let Some(index) = receive_index {
        if arguments.message.is_some() {
            return Err(Error::new_spanned(
                &implementation.items[index],
                "`message` is inferred from `receive`; remove the actor argument",
            ));
        }
        let ImplItem::Fn(receive) = &mut implementation.items[index] else {
            unreachable!("the receive item was selected as a function");
        };
        let input_count = receive.sig.inputs.len();
        if input_count != 2 && input_count != 3 {
            return Err(Error::new_spanned(
                &receive.sig,
                "receive must accept &mut self and message, with an optional sender",
            ));
        }
        let message = match receive.sig.inputs.last() {
            Some(argument) => typed_argument(argument, "message requires an explicit type")?,
            None => unreachable!("the receive arity was validated"),
        };
        let addr = if input_count == 2 {
            receive
                .sig
                .inputs
                .insert(1, parse_quote!(_: #bombay::MailAddr));
            parse_quote!(#bombay::MailAddr)
        } else {
            match receive.sig.inputs.iter().nth(1) {
                Some(argument) => {
                    typed_argument(argument, "sender requires an explicit address type")?
                }
                None => unreachable!("the receive arity was validated"),
            }
        };
        receive.attrs.push(parse_quote! {
            #[allow(
                clippy::needless_pass_by_value,
                clippy::unnecessary_wraps,
                clippy::unused_self,
                reason = "Behavior requires an owned message, mutable receiver, and fallible fold signature"
            )]
        });
        Ok((addr, message))
    } else {
        let Some(message) = arguments.message.clone() else {
            return Err(Error::new_spanned(
                &implementation.self_ty,
                "#[actor] requires `receive` or an explicit uninhabited `message`",
            ));
        };
        let addr: Type = parse_quote!(#bombay::MailAddr);
        implementation.items.push(parse_quote! {
            #[allow(
                clippy::needless_pass_by_value,
                clippy::unnecessary_wraps,
                clippy::unused_self,
                reason = "an explicitly uninhabited protocol has one impossible fold"
            )]
            fn receive(
                &mut self,
                _: #addr,
                message: #message,
            ) -> #bombay::behavior::BehaviorActed<Self> {
                match message {}
            }
        });
        Ok((addr, message))
    }
}

/// Derive `Hosts<P>` for each field explicitly marked `#[actor_space(P)]`.
/// Rust checks the declared field type and rejects duplicate protocol impls.
#[proc_macro_derive(ActorSpaces, attributes(actor_space))]
pub fn actor_spaces(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);
    expand_actor_spaces(&input)
        .unwrap_or_else(Error::into_compile_error)
        .into()
}

/// Derive the exact terminal projection implemented by every enum variant.
///
/// Each variant must contain exactly the named fields `origin` and `terminal`.
/// The derive adds no terminal policy; it only maps each declared pair to its
/// owning variant through Bombay's `ProjectTerminal` trait.
#[proc_macro_derive(
    TerminalProjection,
    attributes(application_actor, structural_child, declared_child)
)]
pub fn terminal_projection(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);
    expand_terminal_projection(&input)
        .unwrap_or_else(Error::into_compile_error)
        .into()
}

fn expand_terminal_projection(input: &DeriveInput) -> syn::Result<TokenStream2> {
    let Data::Enum(data) = &input.data else {
        return Err(Error::new_spanned(
            input,
            "TerminalProjection can be derived only for an enum",
        ));
    };
    let bombay = bombay_crate()?;
    let name = &input.ident;
    let mut projections = Vec::with_capacity(data.variants.len());

    for variant in &data.variants {
        let (origin, terminal) = terminal_projection_fields(&variant.fields)?;
        let variant_name = &variant.ident;
        let mut generics = input.generics.clone();
        let (source, declared_origin, projected_terminal) = match origin_projection(variant)? {
            OriginProjection::ApplicationChild | OriginProjection::StructuralChild => (
                quote!(#origin),
                quote!({
                    let _: u64 = origin.nonce();
                    origin
                }),
                quote!(terminal),
            ),
            OriginProjection::DeclaredChild(attribute) => {
                let arguments =
                    attribute.parse_args_with(Punctuated::<Type, Token![,]>::parse_terminated)?;
                let [owner, role, actor] = arguments
                    .iter()
                    .collect::<Vec<_>>()
                    .try_into()
                    .map_err(|_| {
                        Error::new_spanned(
                            attribute,
                            "declared_child requires owner, role, and actor types",
                        )
                    })?;
                generics.make_where_clause().predicates.push(parse_quote!(
                    #role: #bombay::behavior::ChildRole<#owner, Child = #actor>
                ));
                (
                    quote!(#bombay::ChildOrigin<#owner, <#role as #bombay::behavior::ChildRole<#owner>>::Position>),
                    quote!({
                        let declared: #bombay::ChildOrigin<#owner, #role> = origin.into_declared_child();
                        let declared: #origin = declared;
                        declared
                    }),
                    quote!({
                        let exact: #bombay::ActorRetirement<#actor, Self> = terminal;
                        exact
                    }),
                )
            }
            OriginProjection::Root => (
                quote!(#origin),
                quote!({
                    let root: #bombay::RootOrigin<_> = origin;
                    let declared: #origin = root;
                    declared
                }),
                quote!(terminal),
            ),
        };
        let (impl_generics, type_generics, where_clause) = generics.split_for_impl();
        projections.push(quote! {
            impl #impl_generics #bombay::ProjectTerminal<#source, #terminal>
                for #name #type_generics #where_clause
            {
                fn project(origin: #source, terminal: #terminal) -> Self {
                    Self::#variant_name {
                        origin: #declared_origin,
                        terminal: #projected_terminal,
                    }
                }
            }
        });
    }

    Ok(quote!(#(#projections)*))
}

enum OriginProjection<'a> {
    Root,
    ApplicationChild,
    StructuralChild,
    DeclaredChild(&'a Attribute),
}

fn origin_projection(variant: &Variant) -> Result<OriginProjection<'_>> {
    let mut projection = None;
    for attribute in &variant.attrs {
        let selected = if attribute.path().is_ident("application_actor") {
            if !matches!(&attribute.meta, Meta::Path(_)) {
                return Err(Error::new_spanned(
                    attribute,
                    "application_actor takes no arguments",
                ));
            }
            OriginProjection::ApplicationChild
        } else if attribute.path().is_ident("structural_child") {
            if !matches!(&attribute.meta, Meta::Path(_)) {
                return Err(Error::new_spanned(
                    attribute,
                    "structural_child takes no arguments",
                ));
            }
            OriginProjection::StructuralChild
        } else if attribute.path().is_ident("declared_child") {
            OriginProjection::DeclaredChild(attribute)
        } else {
            continue;
        };
        if projection.replace(selected).is_some() {
            return Err(Error::new_spanned(
                attribute,
                "choose one child-origin source marker",
            ));
        }
    }
    Ok(projection.unwrap_or(OriginProjection::Root))
}

fn terminal_projection_fields(fields: &Fields) -> syn::Result<(&Type, &Type)> {
    let Fields::Named(fields) = fields else {
        return Err(Error::new_spanned(
            fields,
            "terminal variants require named `origin` and `terminal` fields",
        ));
    };
    let origin_name = Ident::new("origin", Span::call_site());
    let terminal_name = Ident::new("terminal", Span::call_site());
    let origin = fields
        .named
        .iter()
        .find(|field| field.ident.as_ref() == Some(&origin_name));
    let terminal = fields
        .named
        .iter()
        .find(|field| field.ident.as_ref() == Some(&terminal_name));

    match (origin, terminal, fields.named.len()) {
        (Some(origin), Some(terminal), 2) => Ok((&origin.ty, &terminal.ty)),
        _ => Err(Error::new_spanned(
            fields,
            "terminal variants require exactly named `origin` and `terminal` fields",
        )),
    }
}

fn expand_actor_spaces(input: &DeriveInput) -> syn::Result<TokenStream2> {
    let Data::Struct(data) = &input.data else {
        return Err(Error::new_spanned(
            &input.ident,
            "ActorSpaces can be derived only for a struct with named fields",
        ));
    };
    let Fields::Named(fields) = &data.fields else {
        return Err(Error::new_spanned(
            &input.ident,
            "ActorSpaces requires named fields",
        ));
    };
    let bombay = bombay_crate()?;
    let name = &input.ident;
    let (impl_generics, type_generics, where_clause) = input.generics.split_for_impl();
    let mut implementations = Vec::new();

    for field in &fields.named {
        let Some(attribute) = field
            .attrs
            .iter()
            .find(|attribute| attribute.path().is_ident("actor_space"))
        else {
            continue;
        };
        if field
            .attrs
            .iter()
            .filter(|attribute| attribute.path().is_ident("actor_space"))
            .count()
            != 1
        {
            return Err(Error::new_spanned(
                field,
                "one actor_space protocol per field",
            ));
        }
        let protocol: Type = attribute.parse_args()?;
        let field_name = field.ident.as_ref().expect("named fields have identifiers");
        implementations.push(quote! {
            impl #impl_generics #bombay::Hosts<#protocol> for #name #type_generics #where_clause {
                fn space(&self) -> &#bombay::ActorSpace<#protocol> {
                    let space: &#bombay::ActorSpace<#protocol> = &self.#field_name;
                    space
                }
            }
        });
    }

    Ok(quote!(#(#implementations)*))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_facade_library_name_and_explicit_rename_are_distinct() {
        assert_eq!(
            crate_path(FoundCrate::Name("bombay_rs".to_owned())).to_string(),
            ":: bombay"
        );
        assert_eq!(
            crate_path(FoundCrate::Name("actor_runtime".to_owned())).to_string(),
            ":: actor_runtime"
        );
    }
}
