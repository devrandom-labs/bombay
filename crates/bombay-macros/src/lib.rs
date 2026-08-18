//! Transitional static local-hosting declarations for Bombay.

use std::collections::HashSet;

use proc_macro::TokenStream;
use proc_macro2::Span;
use quote::{format_ident, quote};
use syn::parse::{Parse, ParseStream};
use syn::spanned::Spanned;
use syn::{
    Error, Ident, ItemFn, Result, ReturnType, Token, Type, Visibility, braced, parse_macro_input,
    parse_quote,
};

mod keyword {
    syn::custom_keyword!(topology);
    syn::custom_keyword!(hosted);
}

struct Declaration {
    visibility: Visibility,
    topology: Ident,
    root: Type,
    hosted: Vec<Type>,
}

fn parse_types(input: ParseStream<'_>) -> Result<Vec<Type>> {
    let content;
    braced!(content in input);
    let mut entries = Vec::new();
    while !content.is_empty() {
        entries.push(content.parse()?);
        if content.peek(Token![,]) {
            content.parse::<Token![,]>()?;
        }
    }
    Ok(entries)
}

impl Parse for Declaration {
    fn parse(input: ParseStream<'_>) -> Result<Self> {
        let visibility = input.parse()?;
        input.parse::<keyword::topology>()?;
        let topology = input.parse()?;
        input.parse::<Token![for]>()?;
        let root = input.parse()?;
        let body;
        braced!(body in input);

        body.parse::<keyword::hosted>()?;
        let hosted = parse_types(&body)?;
        if !body.is_empty() {
            return Err(body.error("unexpected tokens after `hosted` section"));
        }

        Ok(Self {
            visibility,
            topology,
            root,
            hosted,
        })
    }
}

fn require_unique_types(types: &[Type]) -> Result<()> {
    let mut seen = HashSet::new();
    for ty in types {
        let spelling = quote!(#ty).to_string();
        if !seen.insert(spelling.clone()) {
            return Err(Error::new(
                ty.span(),
                format!("hosted protocol `{spelling}` is listed more than once"),
            ));
        }
    }
    Ok(())
}

/// Declare closed, pure local-hosting evidence.
///
/// This macro does not implement Behavior or construct a runtime. Each hosted
/// entry is a stable [`bombay::behavior::Protocol`] type, not a behavior
/// wrapper. Duplicate hosted protocol types are compile-time errors.
#[proc_macro]
pub fn application(input: TokenStream) -> TokenStream {
    let declaration = parse_macro_input!(input as Declaration);
    match expand(declaration) {
        Ok(tokens) => tokens.into(),
        Err(error) => error.to_compile_error().into(),
    }
}

/// Run the root application value returned by a synchronous `fn main()`.
///
/// This is syntax over `bombay::run`; it does not construct a separate
/// runtime path or infer the application's local-hosting topology.
#[proc_macro_attribute]
pub fn main(arguments: TokenStream, input: TokenStream) -> TokenStream {
    if !arguments.is_empty() {
        return Error::new(Span::call_site(), "`#[bombay::main]` accepts no arguments")
            .to_compile_error()
            .into();
    }

    let mut function = parse_macro_input!(input as ItemFn);
    if let Err(error) = validate_main(&function) {
        return error.to_compile_error().into();
    }

    let body = function.block;
    function.sig.output = parse_quote!(-> ::core::result::Result<(), ::bombay::RunError>);
    function.block = Box::new(parse_quote!({ ::bombay::run({ #body }) }));
    quote!(#function).into()
}

fn validate_main(function: &ItemFn) -> Result<()> {
    if function.sig.ident != "main" {
        return Err(Error::new_spanned(
            &function.sig.ident,
            "`#[bombay::main]` must annotate `fn main`",
        ));
    }
    if function.sig.asyncness.is_some() {
        return Err(Error::new_spanned(
            function.sig.asyncness,
            "Bombay owns asynchronous execution; remove `async` from `fn main`",
        ));
    }
    if !function.sig.inputs.is_empty() {
        return Err(Error::new_spanned(
            &function.sig.inputs,
            "`#[bombay::main]` requires an argument-free `fn main`",
        ));
    }
    if !function.sig.generics.params.is_empty() || function.sig.generics.where_clause.is_some() {
        return Err(Error::new_spanned(
            &function.sig.generics,
            "`#[bombay::main]` does not accept generics",
        ));
    }
    if !matches!(function.sig.output, ReturnType::Default) {
        return Err(Error::new_spanned(
            &function.sig.output,
            "the body of `#[bombay::main] fn main()` returns the root application value; remove the explicit return type",
        ));
    }
    if function.sig.constness.is_some()
        || function.sig.unsafety.is_some()
        || function.sig.abi.is_some()
        || function.sig.variadic.is_some()
    {
        return Err(Error::new_spanned(
            &function.sig,
            "`#[bombay::main]` requires an ordinary safe Rust function",
        ));
    }
    Ok(())
}

fn expand(declaration: Declaration) -> Result<proc_macro2::TokenStream> {
    require_unique_types(&declaration.hosted)?;
    let visibility = declaration.visibility;
    let topology = declaration.topology;
    let root = declaration.root;
    let module = format_ident!("__bombay_topology_{}", topology.to_string().to_lowercase());
    let namespaces = Ident::new("Namespaces", Span::call_site());
    let namespace_fields: Vec<_> = declaration
        .hosted
        .iter()
        .enumerate()
        .map(|(index, _)| format_ident!("hosted_{index}"))
        .collect();
    let namespace_declarations = declaration
        .hosted
        .iter()
        .zip(&namespace_fields)
        .map(|(entry, field)| quote!(#field: ::bombay::__private::LocalAddresses<#entry>));
    let namespace_initializers =
        declaration.hosted.iter().zip(&namespace_fields).map(
            |(entry, field)| quote!(#field: ::bombay::__private::LocalAddresses::<#entry>::new()),
        );
    let namespace_impls = declaration
        .hosted
        .iter()
        .zip(&namespace_fields)
        .map(|(entry, field)| {
            quote! {
                impl ::bombay::__private::Namespace<#entry> for #namespaces {
                    fn namespace(&self) -> ::bombay::__private::LocalAddresses<#entry> {
                        self.#field.clone()
                    }
                }
            }
        });

    Ok(quote! {
        #[doc(hidden)]
        #[allow(dead_code)]
        #visibility mod #module {
            use super::*;

            pub struct #namespaces {
                root: ::bombay::__private::LocalAddresses<<#root as ::bombay::behavior::Behavior>::Protocol>,
                #(#namespace_declarations,)*
            }

            impl ::bombay::__private::BuildNamespaces for #namespaces {
                fn build() -> Self {
                    Self {
                        root: ::bombay::__private::LocalAddresses::<
                            <#root as ::bombay::behavior::Behavior>::Protocol
                        >::new(),
                        #(#namespace_initializers,)*
                    }
                }
            }

            impl ::bombay::__private::Namespace<<#root as ::bombay::behavior::Behavior>::Protocol>
                for #namespaces
            {
                fn namespace(
                    &self,
                ) -> ::bombay::__private::LocalAddresses<<#root as ::bombay::behavior::Behavior>::Protocol> {
                    self.root.clone()
                }
            }

            #(#namespace_impls)*
        }

        #visibility struct #topology;

        impl ::bombay::__private::Topology for #topology {
            type Root = #root;
            type Namespaces = #module::#namespaces;
        }

        impl ::bombay::Application for #root {
            type Topology = #topology;
        }
    })
}
