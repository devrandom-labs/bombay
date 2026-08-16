//! Application authoring macros for Bombay.

use proc_macro::TokenStream;
use quote::quote;
use syn::{Error, FnArg, GenericArgument, ImplItem, ItemImpl, PathArguments, ReturnType, Type};

/// Define an ordinary no-birth, no-phase actor from its `receive` method.
#[allow(
    clippy::too_many_lines,
    reason = "the parser validates one indivisible actor declaration before emitting any code"
)]
#[proc_macro_attribute]
pub fn actor(arguments: TokenStream, item: TokenStream) -> TokenStream {
    if !arguments.is_empty() {
        return Error::new(
            proc_macro2::Span::call_site(),
            "#[actor] accepts no arguments",
        )
        .to_compile_error()
        .into();
    }

    let implementation = syn::parse_macro_input!(item as ItemImpl);
    if implementation.trait_.is_some() {
        return Error::new_spanned(&implementation, "#[actor] applies to an inherent impl")
            .to_compile_error()
            .into();
    }
    let Some(receive) = implementation.items.iter().find_map(|item| match item {
        ImplItem::Fn(method) if method.sig.ident == "receive" => Some(method),
        _ => None,
    }) else {
        return Error::new_spanned(
            &implementation.self_ty,
            "#[actor] requires receive(&mut self, from, message)",
        )
        .to_compile_error()
        .into();
    };
    if receive.sig.asyncness.is_some() || receive.sig.inputs.len() != 3 {
        return Error::new_spanned(
            &receive.sig,
            "receive must be synchronous and accept exactly &mut self, from, and message",
        )
        .to_compile_error()
        .into();
    }

    let Some(FnArg::Receiver(receiver)) = receive.sig.inputs.first() else {
        return Error::new_spanned(&receive.sig, "receive must begin with &mut self")
            .to_compile_error()
            .into();
    };
    if receiver.reference.is_none() || receiver.mutability.is_none() {
        return Error::new_spanned(receiver, "receive must begin with &mut self")
            .to_compile_error()
            .into();
    }
    let mut parameters = receive.sig.inputs.iter().skip(1);
    let Some(FnArg::Typed(from)) = parameters.next() else {
        return Error::new_spanned(&receive.sig, "from requires an explicit address type")
            .to_compile_error()
            .into();
    };
    let Some(FnArg::Typed(message)) = parameters.next() else {
        return Error::new_spanned(&receive.sig, "message requires an explicit type")
            .to_compile_error()
            .into();
    };
    let ReturnType::Type(_, result) = &receive.sig.output else {
        return Error::new_spanned(&receive.sig, "receive must return Effect<T>")
            .to_compile_error()
            .into();
    };
    let Type::Path(result) = result.as_ref() else {
        return Error::new_spanned(result, "receive must return Effect<T>")
            .to_compile_error()
            .into();
    };
    let Some(effect) = result.path.segments.last() else {
        return Error::new_spanned(result, "receive must return Effect<T>")
            .to_compile_error()
            .into();
    };
    if effect.ident != "Effect" {
        return Error::new_spanned(result, "receive must return Effect<T>")
            .to_compile_error()
            .into();
    }
    let PathArguments::AngleBracketed(arguments) = &effect.arguments else {
        return Error::new_spanned(result, "Effect requires its emitted value type")
            .to_compile_error()
            .into();
    };
    let Some(GenericArgument::Type(emitted)) = arguments.args.first() else {
        return Error::new_spanned(result, "Effect requires its emitted value type")
            .to_compile_error()
            .into();
    };

    let actor = &implementation.self_ty;
    let address = &from.ty;
    let message = &message.ty;
    let (impl_generics, _, where_clause) = implementation.generics.split_for_impl();

    quote! {
        #implementation

        impl #impl_generics ::bombay::behavior::Behavior for #actor #where_clause {
            type Addr = #address;
            type Msg = #message;
            type Event = ::bombay::behavior::User<#address, #message>;
            type Sends = ::std::vec::Vec<#emitted>;
            type Ph = ::bombay::behavior::Never;
            type Error = ::bombay::behavior::Never;
            type Birth = ::bombay::behavior::NoBirths;

            fn init(
                &mut self,
                _: ::bombay::behavior::InitializationTurn,
            ) -> ::bombay::behavior::BehaviorActed<Self> {
                ::core::result::Result::Ok(::bombay::behavior::Actions::cont())
            }

            fn transition(
                &mut self,
                _: ::bombay::behavior::ActiveTurn,
                event: Self::Event,
            ) -> ::bombay::behavior::BehaviorActed<Self> {
                ::core::result::Result::Ok(
                    ::core::convert::Into::into(
                        <#actor>::receive(self, event.from, event.message)
                    )
                )
            }
        }

        impl #impl_generics ::bombay::behavior::BehaviorBase for #actor #where_clause {
            type Base = Self;

            fn base(&self) -> &Self {
                self
            }
        }
    }
    .into()
}
