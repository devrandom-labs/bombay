//! Mechanical derives for Bombay's concrete local runtime composition.

use std::collections::BTreeMap;

use proc_macro::TokenStream;
use proc_macro_crate::{FoundCrate, crate_name};
use proc_macro2::{Span, TokenStream as TokenStream2};
use quote::quote;
use syn::parse::{Parse, ParseStream};
use syn::{
    Data, DeriveInput, Error, Fields, FnArg, GenericArgument, Ident, ImplItem, ItemImpl,
    PathArguments, Result, Token, Type, braced, parse_macro_input, parse_quote,
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
                "creation_settlements" => {
                    let disposition: Type = input.parse()?;
                    forwarded.push(quote!(creation_settlements = #disposition));
                }
                "sends" | "births" => {
                    if input.peek(syn::token::Brace) {
                        let content;
                        braced!(content in input);
                        let fields: TokenStream2 = content.parse()?;
                        forwarded.push(quote!(#key = { #fields }));
                    } else {
                        let ty: Type = input.parse()?;
                        forwarded.push(quote!(#key = #ty));
                    }
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
                reason = "Bombay's actor facade supplies the owning Behavior fold boundary"
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

fn hosted_addresses_protocol(field: &syn::Field) -> syn::Result<Option<&Type>> {
    let Type::Path(field_type) = &field.ty else {
        return Ok(None);
    };
    let Some(segment) = field_type.path.segments.last() else {
        return Ok(None);
    };
    if segment.ident != "LocalAddresses" {
        return Ok(None);
    }
    let PathArguments::AngleBracketed(arguments) = &segment.arguments else {
        return Err(Error::new_spanned(
            &field.ty,
            "LocalAddresses must name exactly one hosted protocol",
        ));
    };
    let mut types = arguments.args.iter().filter_map(|argument| match argument {
        GenericArgument::Type(protocol) => Some(protocol),
        _ => None,
    });
    let Some(protocol) = types.next() else {
        return Err(Error::new_spanned(
            &field.ty,
            "LocalAddresses must name exactly one hosted protocol",
        ));
    };
    if types.next().is_some() || arguments.args.len() != 1 {
        return Err(Error::new_spanned(
            &field.ty,
            "LocalAddresses must name exactly one hosted protocol",
        ));
    }
    Ok(Some(protocol))
}

/// Derive the exact `HostedAddresses<P>` implementation for every named
/// `LocalAddresses<P>` field in an application-owned product.
#[proc_macro_derive(HostedAddresses)]
pub fn hosted_addresses(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);
    expand_hosted_addresses(&input)
        .unwrap_or_else(Error::into_compile_error)
        .into()
}

/// Derive the exact terminal projection implemented by every enum variant.
///
/// Each variant must contain exactly the named fields `origin` and `terminal`.
/// The derive adds no terminal policy; it only maps each declared pair to its
/// owning variant through Bombay's `ProjectTerminal` trait.
#[proc_macro_derive(TerminalProjection, attributes(application_actor))]
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
        let (owner, role) = actor_origin_arguments(origin)?;
        let retiring_actor = actor_retirement_actor(terminal)?;
        let variant_name = &variant.ident;
        let mut generics = input.generics.clone();
        let application_actor = variant
            .attrs
            .iter()
            .any(|attribute| attribute.path().is_ident("application_actor"));
        let (source, declared_origin) = if let Some(role) = role {
            if application_actor || is_structural_child(role) {
                (quote!(#bombay::ActorOrigin<#owner, #role>), quote!(origin))
            } else {
                generics.make_where_clause().predicates.push(parse_quote!(
                    #role: #bombay::behavior::ChildRole<#owner, Child = #retiring_actor>
                ));
                (
                    quote! {
                        #bombay::ActorOrigin<
                            #owner,
                            <#role as #bombay::behavior::ChildRole<#owner>>::Position
                        >
                    },
                    quote!(origin.into_declared_child()),
                )
            }
        } else {
            if application_actor {
                return Err(Error::new_spanned(
                    variant,
                    "application_actor requires an ActorOrigin<Owner, Role> field",
                ));
            }
            (
                quote!(#bombay::ActorOrigin<#owner, #bombay::behavior::Here>),
                quote!(origin.into_declared_root()),
            )
        };
        let (impl_generics, type_generics, where_clause) = generics.split_for_impl();
        projections.push(quote! {
            impl #impl_generics #bombay::ProjectTerminal<#source, #terminal>
                for #name #type_generics #where_clause
            {
                fn project(origin: #source, terminal: #terminal) -> Self {
                    Self::#variant_name {
                        origin: #declared_origin,
                        terminal,
                    }
                }
            }
        });
    }

    Ok(quote!(#(#projections)*))
}

fn is_structural_child(role: &Type) -> bool {
    let Type::Path(role) = role else {
        return false;
    };
    role.path
        .segments
        .last()
        .is_some_and(|segment| segment.ident == "ChildHead" || segment.ident == "ChildTail")
}

fn actor_retirement_actor(terminal: &Type) -> syn::Result<&Type> {
    let Type::Path(terminal) = terminal else {
        return Err(Error::new_spanned(
            terminal,
            "terminal must be ActorRetirement<Actor, Root, EffectError>",
        ));
    };
    let Some(segment) = terminal.path.segments.last() else {
        return Err(Error::new_spanned(terminal, "terminal type path is empty"));
    };
    if segment.ident != "ActorRetirement" {
        return Err(Error::new_spanned(
            terminal,
            "terminal must be ActorRetirement<Actor, Root, EffectError>",
        ));
    }
    let PathArguments::AngleBracketed(arguments) = &segment.arguments else {
        return Err(Error::new_spanned(
            terminal,
            "ActorRetirement requires an actor, root terminal, and optional effect error",
        ));
    };
    let types = arguments
        .args
        .iter()
        .filter_map(|argument| match argument {
            GenericArgument::Type(ty) => Some(ty),
            _ => None,
        })
        .collect::<Vec<_>>();
    match types.as_slice() {
        [actor, _] if arguments.args.len() == 2 => Ok(actor),
        [actor, _, _] if arguments.args.len() == 3 => Ok(actor),
        _ => Err(Error::new_spanned(
            terminal,
            "ActorRetirement requires an actor, root terminal, and optional effect error",
        )),
    }
}

fn actor_origin_arguments(origin: &Type) -> syn::Result<(&Type, Option<&Type>)> {
    let Type::Path(origin) = origin else {
        return Err(Error::new_spanned(
            origin,
            "origin must be ActorOrigin<Owner> or ActorOrigin<Owner, Role>",
        ));
    };
    let Some(segment) = origin.path.segments.last() else {
        return Err(Error::new_spanned(origin, "origin type path is empty"));
    };
    if segment.ident != "ActorOrigin" {
        return Err(Error::new_spanned(
            origin,
            "origin must be ActorOrigin<Owner> or ActorOrigin<Owner, Role>",
        ));
    }
    let PathArguments::AngleBracketed(arguments) = &segment.arguments else {
        return Err(Error::new_spanned(
            origin,
            "ActorOrigin requires an owner and optional role",
        ));
    };
    let types = arguments
        .args
        .iter()
        .filter_map(|argument| match argument {
            GenericArgument::Type(ty) => Some(ty),
            _ => None,
        })
        .collect::<Vec<_>>();
    match types.as_slice() {
        [owner] if arguments.args.len() == 1 => Ok((owner, None)),
        [owner, role] if arguments.args.len() == 2 => Ok((owner, Some(role))),
        _ => Err(Error::new_spanned(
            origin,
            "ActorOrigin requires an owner and optional role",
        )),
    }
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

fn expand_hosted_addresses(input: &DeriveInput) -> syn::Result<TokenStream2> {
    let Data::Struct(data) = &input.data else {
        return Err(Error::new_spanned(
            &input.ident,
            "HostedAddresses can be derived only for a struct with named fields",
        ));
    };
    let Fields::Named(fields) = &data.fields else {
        return Err(Error::new_spanned(
            &input.ident,
            "HostedAddresses requires named fields",
        ));
    };
    let bombay = bombay_crate()?;
    let name = &input.ident;
    let (impl_generics, type_generics, where_clause) = input.generics.split_for_impl();
    let mut protocols = BTreeMap::new();
    let mut implementations = Vec::new();

    for field in &fields.named {
        let Some(protocol) = hosted_addresses_protocol(field)? else {
            continue;
        };
        let field_name = field.ident.as_ref().expect("named fields have identifiers");
        let key = quote!(#protocol).to_string();
        if let Some(previous) = protocols.insert(key, field_name) {
            return Err(Error::new_spanned(
                field_name,
                format!("duplicate hosted protocol; it is already hosted by field `{previous}`"),
            ));
        }
        implementations.push(quote! {
            impl #impl_generics #bombay::HostedAddresses<#protocol> for #name #type_generics #where_clause {
                fn addresses(&self) -> &#bombay::LocalAddresses<#protocol> {
                    &self.#field_name
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
