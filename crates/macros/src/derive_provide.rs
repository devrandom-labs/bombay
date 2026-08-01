//! `#[derive(Provide)]` — emits one `bombay::caps::Provide<FieldTy>` impl
//! per named field of a capability-set struct (ADR-0026 Addendum: the
//! open seam of the caps encoding). See card #278.
//!
//! Deliberately NOT named `CapSet`: this derive does not (and cannot)
//! generate `CapSet::build` — building needs policy knowledge only the
//! user has; a build-generating derive is card #243. Duplicate field
//! TYPES produce overlapping `Provide` impls and are rejected by
//! coherence (E0119) — the intended duplicate-capability guard.
//!
//! It ALSO emits the loop-participation half (ADR-0026 stage 2, card #279):
//! one `bombay::caps::Replay<Msg>` impl so the `Shell` can drain in-step
//! replay uniformly. A `Stashing<M>` field forwards to its own `Replay`; a
//! set with no stash field yields `None`. This is the forget-proof wiring
//! for the `Stashing` capability — a stash you cannot forget to service.

use proc_macro2::TokenStream;
use quote::{ToTokens, quote};
use syn::{
    Data, DeriveInput, Fields, GenericArgument, Ident, PathArguments, Type, parse::Parse,
    parse::ParseStream,
};

/// If `ty` is written `…::<cap_ident><T>` (any leading path), returns `T`.
///
/// The deliberate core-type coupling of this derive: it recognizes the
/// core capability types **structurally** so it can emit each field's loop
/// participation alongside its `Provide` — `Stashing<M>` →
/// [`Replay`](bombay::caps::Replay), `Watching<WP>` →
/// [`HasWatching`](bombay::caps::HasWatching), `Supervising<SS>` →
/// [`HasSupervising`](bombay::caps::HasSupervising) (+ the
/// [`SelectRunner`](bombay::caps::SelectRunner) loop shape). Structural —
/// rather than attribute-driven — recognition is what keeps participation
/// forget-proof: you cannot hold a cap field the derive fails to service.
/// An *alias* (`type S = Stashing<X>`) is NOT recognized; write the type
/// directly.
fn cap_type_arg<'t>(ty: &'t Type, cap_ident: &str) -> Option<&'t Type> {
    let Type::Path(type_path) = ty else {
        return None;
    };
    let segment = type_path.path.segments.last()?;
    if segment.ident != cap_ident {
        return None;
    }
    let PathArguments::AngleBracketed(args) = &segment.arguments else {
        return None;
    };
    args.args.iter().find_map(|arg| match arg {
        GenericArgument::Type(inner) => Some(inner),
        _ => None,
    })
}

/// See [`cap_type_arg`]: `…::Stashing<M>` → `M`.
fn stash_message_ty(ty: &Type) -> Option<&Type> {
    cap_type_arg(ty, "Stashing")
}

/// A parsed `#[derive(Provide)]` input: the struct identifier and its
/// named fields.
#[derive(Debug)]
pub struct DeriveProvide {
    ident: Ident,
    fields: Vec<(Ident, Type)>,
}

impl Parse for DeriveProvide {
    fn parse(input: ParseStream) -> syn::Result<Self> {
        let derive: DeriveInput = input.parse()?;

        if let Some(param) = derive.generics.params.first() {
            return Err(syn::Error::new_spanned(
                param,
                "`#[derive(Provide)]` needs a concrete cap-set struct: a \
                 generic set cannot emit per-field `Provide` impls without \
                 repeating its generics on every impl",
            ));
        }
        let Data::Struct(data) = &derive.data else {
            return Err(syn::Error::new_spanned(
                &derive.ident,
                "`#[derive(Provide)]` supports structs with named fields \
                 only (a capability set is a named struct of capability \
                 fields)",
            ));
        };
        let Fields::Named(named) = &data.fields else {
            return Err(syn::Error::new_spanned(
                &derive.ident,
                "`#[derive(Provide)]` needs NAMED fields: each field is a \
                 capability; tuple/unit structs have nothing to provide",
            ));
        };
        let fields = named
            .named
            .iter()
            .filter_map(|f| f.ident.clone().map(|ident| (ident, f.ty.clone())))
            .collect::<Vec<_>>();
        if fields.is_empty() {
            return Err(syn::Error::new_spanned(
                &derive.ident,
                "`#[derive(Provide)]` on an empty struct is a no-op; a \
                 capability-less actor uses `type Caps = ()` instead",
            ));
        }
        // The composition law's friendly half (ADR-0026 stage 3, card #280):
        // a supervisor IS a watcher, so `Supervising` without a `Watching`
        // sibling is rejected here with a readable error. The type-level
        // supertrait (`HasSupervising: HasWatching`) remains the law for
        // hand-written sets.
        let has_watching = fields
            .iter()
            .any(|(_, ty)| cap_type_arg(ty, "Watching").is_some());
        if !has_watching
            && let Some((_, supervising)) = fields
                .iter()
                .find(|(_, ty)| cap_type_arg(ty, "Supervising").is_some())
        {
            return Err(syn::Error::new_spanned(
                supervising,
                "`Supervising` requires a `Watching` field in the same cap \
                 set: a supervisor watches its children's deaths (ADR-0026 \
                 stage 3 composition law)",
            ));
        }
        reject_phased_conflicts(&fields)?;
        Ok(Self {
            ident: derive.ident,
            fields,
        })
    }
}

/// Stage-4 composition laws (ADR-0026, card #281), friendly halves: at
/// most one phase machine per set, and `Phased` embeds its own deadline
/// seat and stash — sibling `Deadlined`/`Stashing` fields are rejected
/// readably (E0119/overlap remains the law for hand-written sets).
fn reject_phased_conflicts(fields: &[(Ident, Type)]) -> syn::Result<()> {
    let phased: Vec<&(Ident, Type)> = fields
        .iter()
        .filter(|(_, ty)| cap_type_arg(ty, "Phased").is_some())
        .collect();
    if let Some((_, second)) = phased.get(1) {
        return Err(syn::Error::new_spanned(
            second,
            "at most one `Phased` field per cap set: an actor has one \
             phase machine (and the loop has one deadline arm)",
        ));
    }
    let Some((_, phased_ty)) = phased.first() else {
        return Ok(());
    };
    if fields
        .iter()
        .any(|(_, ty)| cap_type_arg(ty, "Deadlined").is_some())
    {
        return Err(syn::Error::new_spanned(
            phased_ty,
            "`Phased` EMBEDS the deadline seat (its phase deadline rides \
             the ADR-0025 plane); a separate `Deadlined` field would be a \
             second deadline for the loop's one arm — drop it",
        ));
    }
    if fields
        .iter()
        .any(|(_, ty)| cap_type_arg(ty, "Stashing").is_some())
    {
        return Err(syn::Error::new_spanned(
            phased_ty,
            "`Phased` embeds its own bounded stash (the gate defers into \
             it, `Phased::stash` is the manual escape hatch); a separate \
             `Stashing` field would double-buffer deferral — drop it",
        ));
    }
    Ok(())
}

impl ToTokens for DeriveProvide {
    fn to_tokens(&self, tokens: &mut TokenStream) {
        let ident = &self.ident;
        for (field, ty) in &self.fields {
            tokens.extend(quote! {
                #[automatically_derived]
                impl ::bombay::caps::Provide<#ty> for #ident {
                    fn provide(&mut self) -> &mut #ty {
                        &mut self.#field
                    }
                }
            });
        }

        // The loop-participation half (ADR-0026 stage 2): every cap set is
        // `Replay<Msg>` so the `Shell` can drain in-step replay uniformly. A
        // `Stashing<M>` field forwards to its own `Replay` impl; a `Phased<P>`
        // field forwards to its embedded phase stash (Parse rejects the two
        // together); a set with neither yields `None` for any message type.
        // Emitting exactly one shape keeps the impls non-overlapping (no
        // E0119).
        let stash_fields: Vec<(&Ident, &Type)> = self
            .fields
            .iter()
            .filter_map(|(field, ty)| stash_message_ty(ty).map(|m| (field, m)))
            .collect();
        let phased_field: Option<(&Ident, &Type)> = self
            .fields
            .iter()
            .find_map(|(field, ty)| cap_type_arg(ty, "Phased").map(|p| (field, p)));
        if let Some((field, policy)) = phased_field {
            let actor = quote!(<#policy as ::bombay::caps::PhasePolicy>::Actor);
            let msg = quote!(<#actor as ::bombay::caps::Actor>::Msg);
            tokens.extend(quote! {
                #[automatically_derived]
                impl ::bombay::caps::Replay<#msg> for #ident {
                    fn next_replay(&mut self) -> ::core::option::Option<#msg> {
                        ::bombay::caps::Replay::next_replay(&mut self.#field)
                    }
                }
            });
        } else if stash_fields.is_empty() {
            tokens.extend(quote! {
                #[automatically_derived]
                impl<__CapsReplayMsg> ::bombay::caps::Replay<__CapsReplayMsg> for #ident {
                    fn next_replay(&mut self) -> ::core::option::Option<__CapsReplayMsg> {
                        ::core::option::Option::None
                    }
                }
            });
        } else {
            for (field, msg) in stash_fields {
                tokens.extend(quote! {
                    #[automatically_derived]
                    impl ::bombay::caps::Replay<#msg> for #ident {
                        fn next_replay(&mut self) -> ::core::option::Option<#msg> {
                            ::bombay::caps::Replay::next_replay(&mut self.#field)
                        }
                    }
                });
            }
        }

        emit_watch_supervise(&self.fields, ident, tokens);
        emit_deadline(&self.fields, phased_field, ident, tokens);
        emit_admission(phased_field, ident, tokens);
    }
}

/// Stage-4 admission participation (ADR-0026, card #281): a `Phased<P>`
/// field forwards `admit`/`commit` to the machine (concrete over the
/// policy's served actor); every other set delivers everything and
/// commits nothing.
fn emit_admission(phased: Option<(&Ident, &Type)>, ident: &Ident, tokens: &mut TokenStream) {
    if let Some((field, policy)) = phased {
        let actor = quote!(<#policy as ::bombay::caps::PhasePolicy>::Actor);
        let msg = quote!(<#actor as ::bombay::caps::Actor>::Msg);
        let err = quote!(<#actor as ::bombay::caps::Actor>::Error);
        tokens.extend(quote! {
            #[automatically_derived]
            impl ::bombay::caps::Admission<#actor> for #ident {
                async fn admit(
                    &mut self,
                    actor: &mut #actor,
                    msg: #msg,
                ) -> ::core::result::Result<::bombay::caps::Admitted<#msg>, #err> {
                    ::bombay::caps::Admission::admit(&mut self.#field, actor, msg).await
                }
                fn commit(&mut self) {
                    ::bombay::caps::Admission::commit(&mut self.#field);
                }
            }
        });
        return;
    }
    tokens.extend(quote! {
        #[automatically_derived]
        impl<__CapsActor: ::bombay::caps::Actor> ::bombay::caps::Admission<__CapsActor>
            for #ident
        {
            async fn admit(
                &mut self,
                _: &mut __CapsActor,
                msg: <__CapsActor as ::bombay::caps::Actor>::Msg,
            ) -> ::core::result::Result<
                ::bombay::caps::Admitted<<__CapsActor as ::bombay::caps::Actor>::Msg>,
                <__CapsActor as ::bombay::caps::Actor>::Error,
            > {
                ::core::result::Result::Ok(::bombay::caps::Admitted::Deliver(msg))
            }
            fn commit(&mut self) {}
        }
    });
}

/// Stage-4 participation (ADR-0026, card #281): the ADR-0025 deadline
/// plane's loop hook.
///
/// A `Deadlined<DP>` field emits a `DeadlineHook` impl forwarding to the
/// field (gated on `DP: DeadlinePolicy<A>`, exactly as `HasWatching` gates
/// its policy); a `Phased<P>` field emits one forwarding to the machine —
/// its phase deadline IS the set's deadline seat (Parse rejects the two
/// together); a set with neither emits the disabled blanket (`None`, arm
/// never polls). Emitting exactly one shape keeps the impls
/// non-overlapping (no E0119) — and two `Deadlined` fields emit two
/// forwarding impls, rejected by coherence like every duplicate cap.
fn emit_deadline(
    fields: &[(Ident, Type)],
    phased: Option<(&Ident, &Type)>,
    ident: &Ident,
    tokens: &mut TokenStream,
) {
    if let Some((field, policy)) = phased {
        let actor = quote!(<#policy as ::bombay::caps::PhasePolicy>::Actor);
        let err = quote!(<#actor as ::bombay::caps::Actor>::Error);
        tokens.extend(quote! {
            #[automatically_derived]
            impl ::bombay::caps::DeadlineHook<#actor> for #ident {
                fn next_deadline(&self, actor: &#actor) -> ::core::option::Option<::bombay::caps::DeadlineInstant> {
                    ::bombay::caps::DeadlineHook::next_deadline(&self.#field, actor)
                }
                async fn on_deadline(
                    &mut self,
                    actor: &mut #actor,
                    actor_ref: ::bombay::actor::WeakActorRef<::bombay::caps::Shell<#actor>>,
                ) -> ::core::result::Result<::bombay::actor::Flow, #err> {
                    ::bombay::caps::DeadlineHook::on_deadline(&mut self.#field, actor, actor_ref).await
                }
            }
        });
        return;
    }
    let deadlined: Vec<(&Ident, &Type)> = fields
        .iter()
        .filter_map(|(field, ty)| cap_type_arg(ty, "Deadlined").map(|dp| (field, dp)))
        .collect();
    if deadlined.is_empty() {
        tokens.extend(quote! {
            #[automatically_derived]
            impl<__CapsActor: ::bombay::caps::Actor> ::bombay::caps::DeadlineHook<__CapsActor>
                for #ident
            {
                fn next_deadline(&self, _: &__CapsActor) -> ::core::option::Option<::bombay::caps::DeadlineInstant> {
                    ::core::option::Option::None
                }
                async fn on_deadline(
                    &mut self,
                    _: &mut __CapsActor,
                    _: ::bombay::actor::WeakActorRef<::bombay::caps::Shell<__CapsActor>>,
                ) -> ::core::result::Result<::bombay::actor::Flow, <__CapsActor as ::bombay::caps::Actor>::Error> {
                    ::core::result::Result::Ok(::bombay::actor::Flow::Continue)
                }
            }
        });
        return;
    }
    for (field, policy) in deadlined {
        tokens.extend(quote! {
            #[automatically_derived]
            impl<__CapsActor: ::bombay::caps::Actor> ::bombay::caps::DeadlineHook<__CapsActor>
                for #ident
            where
                #policy: ::bombay::caps::DeadlinePolicy<::bombay::caps::ByState<__CapsActor>>,
            {
                fn next_deadline(&self, actor: &__CapsActor) -> ::core::option::Option<::bombay::caps::DeadlineInstant> {
                    ::bombay::caps::DeadlineHook::next_deadline(&self.#field, actor)
                }
                async fn on_deadline(
                    &mut self,
                    actor: &mut __CapsActor,
                    actor_ref: ::bombay::actor::WeakActorRef<::bombay::caps::Shell<__CapsActor>>,
                ) -> ::core::result::Result<::bombay::actor::Flow, <__CapsActor as ::bombay::caps::Actor>::Error> {
                    ::bombay::caps::DeadlineHook::on_deadline(&mut self.#field, actor, actor_ref).await
                }
            }
        });
    }
}

/// Stage-3 participation + loop selection (ADR-0026, card #280).
///
/// A `Watching<WP>` field emits `HasWatching` (policy as associated type;
/// gated on the policy serving the actor); a `Supervising<SS>` field emits
/// `HasSupervising` (strategy as associated type; the same policy gate
/// discharges the `HasWatching` supertrait). Duplicate cap fields emit
/// overlapping impls and are rejected by coherence (E0119), exactly as
/// `Provide`. Every derived set then names its loop shape exactly once
/// (spike-280): `Supervising` ⇒ the three-arm supervised loop, else
/// `Watching` ⇒ the two-arm linked loop, else the plain one-arm loop (a
/// stash-only set replays on any shape).
fn emit_watch_supervise(fields: &[(Ident, Type)], ident: &Ident, tokens: &mut TokenStream) {
    let mut first_policy = None;
    for policy in fields
        .iter()
        .filter_map(|(_, ty)| cap_type_arg(ty, "Watching"))
    {
        first_policy.get_or_insert(policy);
        tokens.extend(quote! {
            #[automatically_derived]
            impl<__CapsActor: ::bombay::caps::Actor> ::bombay::caps::HasWatching<__CapsActor>
                for #ident
            where
                #policy: ::bombay::caps::WatchPolicy<__CapsActor>,
            {
                type Policy = #policy;
            }
        });
    }
    let mut supervising = false;
    for strat in fields
        .iter()
        .filter_map(|(_, ty)| cap_type_arg(ty, "Supervising"))
    {
        supervising = true;
        // `Parse` guarantees a Watching sibling exists, so a policy is
        // always available to gate the supertrait for the generic actor.
        let policy = first_policy.iter();
        tokens.extend(quote! {
            #[automatically_derived]
            impl<__CapsActor: ::bombay::caps::Actor> ::bombay::caps::HasSupervising<__CapsActor>
                for #ident
            where
                #(#policy: ::bombay::caps::WatchPolicy<__CapsActor>,)*
            {
                type Strat = #strat;
            }
        });
    }
    let runner = if supervising {
        quote!(::bombay::caps::SupervisedRun)
    } else if first_policy.is_some() {
        quote!(::bombay::caps::LinkedRun)
    } else {
        quote!(::bombay::caps::PlainRun)
    };
    tokens.extend(quote! {
        #[automatically_derived]
        impl<__CapsActor: ::bombay::caps::Actor> ::bombay::caps::SelectRunner<__CapsActor>
            for #ident
        {
            type Runner = #runner;
        }
    });
}

#[cfg(test)]
mod tests {
    use super::DeriveProvide;

    #[test]
    fn generic_struct_is_rejected() {
        let err = syn::parse_str::<DeriveProvide>("struct S<T> { a: T }").unwrap_err();
        assert!(
            err.to_string().contains("concrete cap-set struct"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn enum_is_rejected() {
        let err = syn::parse_str::<DeriveProvide>("enum E { A }").unwrap_err();
        assert!(
            err.to_string().contains("structs with named fields"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn union_is_rejected() {
        let err = syn::parse_str::<DeriveProvide>("union U { a: u32 }").unwrap_err();
        assert!(
            err.to_string().contains("structs with named fields"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn tuple_struct_is_rejected() {
        let err = syn::parse_str::<DeriveProvide>("struct S(u32);").unwrap_err();
        assert!(
            err.to_string().contains("NAMED fields"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn unit_struct_is_rejected() {
        let err = syn::parse_str::<DeriveProvide>("struct S;").unwrap_err();
        assert!(
            err.to_string().contains("NAMED fields"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn empty_struct_is_rejected() {
        let err = syn::parse_str::<DeriveProvide>("struct S {}").unwrap_err();
        assert!(err.to_string().contains("no-op"), "unexpected error: {err}");
    }

    #[test]
    fn one_provide_impl_per_field_is_emitted() {
        use quote::ToTokens as _;
        let parsed =
            syn::parse_str::<DeriveProvide>("struct Caps { stash: Stashing<M>, rate: Limiter }")
                .expect("valid cap-set struct");
        let out = parsed.to_token_stream().to_string();
        assert_eq!(
            out.matches("impl :: bombay :: caps :: Provide <").count(),
            2,
            "exactly one Provide impl per field: {out}"
        );
        assert!(out.contains("& mut self . stash"), "field access: {out}");
        assert!(out.contains("& mut self . rate"), "field access: {out}");
    }

    #[test]
    fn a_stashing_field_emits_a_concrete_replay_forwarding_to_it() {
        use quote::ToTokens as _;
        let parsed = syn::parse_str::<DeriveProvide>("struct Caps { buf: Stashing<GateMsg> }")
            .expect("valid cap-set struct");
        let out = parsed.to_token_stream().to_string();
        assert_eq!(
            out.matches("impl :: bombay :: caps :: Replay < GateMsg > for Caps")
                .count(),
            1,
            "one concrete Replay<GateMsg>: {out}"
        );
        assert!(
            out.contains(":: bombay :: caps :: Replay :: next_replay (& mut self . buf)"),
            "forwards to the stash field's own Replay: {out}"
        );
        assert!(
            !out.contains("__CapsReplayMsg"),
            "a stash field means NO None-blanket (would overlap, E0119): {out}"
        );
    }

    #[test]
    fn no_stash_field_emits_the_none_blanket() {
        use quote::ToTokens as _;
        let parsed = syn::parse_str::<DeriveProvide>("struct Caps { rate: Limiter, tags: Tags }")
            .expect("valid cap-set struct");
        let out = parsed.to_token_stream().to_string();
        assert_eq!(
            out.matches(
                "impl < __CapsReplayMsg > :: bombay :: caps :: Replay < __CapsReplayMsg > for Caps"
            )
            .count(),
            1,
            "one None-blanket Replay for a stash-less set: {out}"
        );
        assert!(
            out.contains(":: core :: option :: Option :: None"),
            "the blanket yields None: {out}"
        );
    }

    /// Stage 3 (card #280): a `Watching<WP>` field emits the `HasWatching`
    /// participation impl — generic over the actor, gated on the policy
    /// actually serving that actor (the where-clause the spike proved
    /// resolves for both generic and concrete policies).
    #[test]
    fn a_watching_field_emits_has_watching_with_the_policy() {
        use quote::ToTokens as _;
        let parsed =
            syn::parse_str::<DeriveProvide>("struct Caps { watching: Watching<RecPolicy> }")
                .expect("valid cap-set struct");
        let out = parsed.to_token_stream().to_string();
        assert_eq!(
            out.matches(":: bombay :: caps :: HasWatching < __CapsActor > for Caps")
                .count(),
            1,
            "exactly one HasWatching impl: {out}"
        );
        assert!(
            out.contains("where RecPolicy : :: bombay :: caps :: WatchPolicy < __CapsActor >"),
            "the impl is gated on the policy serving the actor: {out}"
        );
        assert!(
            out.contains("type Policy = RecPolicy"),
            "the declared policy rides the associated type: {out}"
        );
    }

    /// Stage 3: a `Supervising<SS>` field (with a `Watching` sibling) emits
    /// `HasSupervising` with the strategy as the associated type, and the
    /// SAME policy where-clause (which discharges the `HasWatching`
    /// supertrait for the generic actor).
    #[test]
    fn a_supervising_field_emits_has_supervising_with_the_strategy() {
        use quote::ToTokens as _;
        let parsed = syn::parse_str::<DeriveProvide>(
            "struct Caps { watching: Watching<Otp>, supervising: Supervising<OneForAll> }",
        )
        .expect("valid cap-set struct");
        let out = parsed.to_token_stream().to_string();
        assert_eq!(
            out.matches(":: bombay :: caps :: HasSupervising < __CapsActor > for Caps")
                .count(),
            1,
            "exactly one HasSupervising impl: {out}"
        );
        assert!(
            out.contains("type Strat = OneForAll"),
            "the declared strategy rides the associated type: {out}"
        );
        assert_eq!(
            out.matches("where Otp : :: bombay :: caps :: WatchPolicy < __CapsActor >")
                .count(),
            2,
            "both participation impls carry the policy gate: {out}"
        );
    }

    /// Stage 3 loop selection: `Supervising` selects the supervised shape.
    #[test]
    fn a_supervising_set_selects_the_supervised_runner() {
        use quote::ToTokens as _;
        let parsed = syn::parse_str::<DeriveProvide>(
            "struct Caps { watching: Watching<Otp>, supervising: Supervising<OneForOne> }",
        )
        .expect("valid cap-set struct");
        let out = parsed.to_token_stream().to_string();
        assert_eq!(
            out.matches(":: bombay :: caps :: SelectRunner < __CapsActor > for Caps")
                .count(),
            1,
            "exactly one SelectRunner impl: {out}"
        );
        assert!(
            out.contains("type Runner = :: bombay :: caps :: SupervisedRun"),
            "Supervising selects the three-arm loop: {out}"
        );
    }

    /// Stage 3 loop selection: `Watching` alone selects the linked shape.
    #[test]
    fn a_watching_only_set_selects_the_linked_runner() {
        use quote::ToTokens as _;
        let parsed = syn::parse_str::<DeriveProvide>("struct Caps { watching: Watching<Otp> }")
            .expect("valid cap-set struct");
        let out = parsed.to_token_stream().to_string();
        assert!(
            out.contains("type Runner = :: bombay :: caps :: LinkedRun"),
            "Watching without Supervising selects the two-arm loop: {out}"
        );
    }

    /// Stage 3 loop selection: no watch/supervise cap — including a
    /// stash-only set — selects the plain shape (the stage-2 replay drain
    /// is loop-agnostic).
    #[test]
    fn a_stash_only_set_selects_the_plain_runner() {
        use quote::ToTokens as _;
        let parsed = syn::parse_str::<DeriveProvide>("struct Caps { buf: Stashing<GateMsg> }")
            .expect("valid cap-set struct");
        let out = parsed.to_token_stream().to_string();
        assert_eq!(
            out.matches(":: bombay :: caps :: SelectRunner < __CapsActor > for Caps")
                .count(),
            1,
            "every derived set names its loop shape exactly once: {out}"
        );
        assert!(
            out.contains("type Runner = :: bombay :: caps :: PlainRun"),
            "no watch/supervise cap: plain loop: {out}"
        );
    }

    /// Stage 3 composition law, derive-side friendly half: `Supervising`
    /// without a `Watching` sibling is rejected at expansion with a
    /// readable error (the type-level supertrait law remains the backstop
    /// for hand-written sets).
    #[test]
    fn supervising_without_watching_is_rejected() {
        let err =
            syn::parse_str::<DeriveProvide>("struct Caps { supervising: Supervising<OneForOne> }")
                .unwrap_err();
        assert!(
            err.to_string().contains("requires a `Watching`"),
            "unexpected error: {err}"
        );
    }

    /// Stage 4 (card #281): a `Deadlined<DP>` field emits the
    /// `DeadlineHook` participation impl — generic over the actor, gated
    /// on the policy serving that actor (the `HasWatching` shape).
    #[test]
    fn a_deadlined_field_emits_a_forwarding_deadline_hook() {
        use quote::ToTokens as _;
        let parsed =
            syn::parse_str::<DeriveProvide>("struct Caps { deadlined: Deadlined<IdlePolicy> }")
                .expect("valid cap-set struct");
        let out = parsed.to_token_stream().to_string();
        assert_eq!(
            out.matches(":: bombay :: caps :: DeadlineHook < __CapsActor > for Caps")
                .count(),
            1,
            "exactly one DeadlineHook impl: {out}"
        );
        assert!(
            out.contains(
                "where IdlePolicy : :: bombay :: caps :: DeadlinePolicy < :: bombay :: caps :: \
                 ByState < __CapsActor >>"
            ),
            "the impl is gated on the policy serving the actor's ByState context: {out}"
        );
        assert!(
            out.contains(
                ":: bombay :: caps :: DeadlineHook :: next_deadline (& self . deadlined , actor)"
            ),
            "forwards to the Deadlined field's own hook: {out}"
        );
        assert!(
            !out.contains("fn next_deadline (& self , _ : & __CapsActor)"),
            "a deadlined set must NOT emit the disabled DeadlineHook blanket: {out}"
        );
    }

    /// Stage 4: a set without a deadline-bearing cap emits the disabled
    /// blanket — `None`, so the loop arm never polls.
    #[test]
    fn no_deadline_field_emits_the_disabled_blanket() {
        use quote::ToTokens as _;
        let parsed = syn::parse_str::<DeriveProvide>("struct Caps { buf: Stashing<GateMsg> }")
            .expect("valid cap-set struct");
        let out = parsed.to_token_stream().to_string();
        assert_eq!(
            out.matches(":: bombay :: caps :: DeadlineHook < __CapsActor > for Caps")
                .count(),
            1,
            "every derived set gets exactly one DeadlineHook impl: {out}"
        );
        assert!(
            !out.contains("DeadlinePolicy"),
            "no policy gate on the disabled blanket: {out}"
        );
    }

    /// Stage 4 (card #281): a `Phased<P>` field is the set's admission,
    /// replay, AND deadline seat — three concrete forwarding impls over
    /// the policy's served actor, no blankets.
    #[test]
    fn a_phased_field_emits_admission_replay_and_deadline_forwarding() {
        use quote::ToTokens as _;
        let parsed =
            syn::parse_str::<DeriveProvide>("struct Caps { phased: Phased<WorkerPhases> }")
                .expect("valid cap-set struct");
        let out = parsed.to_token_stream().to_string();
        let actor = "< WorkerPhases as :: bombay :: caps :: PhasePolicy > :: Actor";
        assert_eq!(
            out.matches(&format!(
                ":: bombay :: caps :: Admission < {actor} > for Caps"
            ))
            .count(),
            1,
            "one concrete Admission impl over the served actor: {out}"
        );
        assert_eq!(
            out.matches(&format!(
                ":: bombay :: caps :: DeadlineHook < {actor} > for Caps"
            ))
            .count(),
            1,
            "the phase deadline IS the set's deadline seat: {out}"
        );
        assert!(
            out.contains(":: bombay :: caps :: Replay :: next_replay (& mut self . phased)"),
            "replay forwards to the embedded phase stash: {out}"
        );
        assert!(
            !out.contains("__CapsReplayMsg") && !out.contains("Admitted :: Deliver (msg) }"),
            "a phased set gets NO blankets: {out}"
        );
    }

    /// Stage 4: a non-phased set delivers everything and commits nothing.
    #[test]
    fn no_phased_field_emits_the_deliver_blanket() {
        use quote::ToTokens as _;
        let parsed = syn::parse_str::<DeriveProvide>("struct Caps { rate: Limiter }")
            .expect("valid cap-set struct");
        let out = parsed.to_token_stream().to_string();
        assert_eq!(
            out.matches(":: bombay :: caps :: Admission < __CapsActor > for Caps")
                .count(),
            1,
            "every derived set gets exactly one Admission impl: {out}"
        );
        assert!(
            out.contains(":: bombay :: caps :: Admitted :: Deliver (msg)"),
            "the blanket delivers everything: {out}"
        );
    }

    /// Stage-4 composition law: `Phased` embeds the deadline seat, so a
    /// sibling `Deadlined` is rejected readably.
    #[test]
    fn phased_with_deadlined_is_rejected() {
        let err = syn::parse_str::<DeriveProvide>(
            "struct Caps { phased: Phased<P>, deadlined: Deadlined<DP> }",
        )
        .unwrap_err();
        assert!(
            err.to_string().contains("EMBEDS the deadline seat"),
            "unexpected error: {err}"
        );
    }

    /// Stage-4 composition law: `Phased` embeds its own stash, so a
    /// sibling `Stashing` is rejected readably.
    #[test]
    fn phased_with_stashing_is_rejected() {
        let err =
            syn::parse_str::<DeriveProvide>("struct Caps { phased: Phased<P>, buf: Stashing<M> }")
                .unwrap_err();
        assert!(
            err.to_string().contains("double-buffer deferral"),
            "unexpected error: {err}"
        );
    }

    /// Stage-4 composition law: one phase machine per actor.
    #[test]
    fn two_phased_fields_are_rejected() {
        let err = syn::parse_str::<DeriveProvide>("struct Caps { a: Phased<P1>, b: Phased<P2> }")
            .unwrap_err();
        assert!(
            err.to_string().contains("at most one `Phased`"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn a_mixed_set_emits_exactly_one_concrete_replay_and_no_blanket() {
        use quote::ToTokens as _;
        let parsed = syn::parse_str::<DeriveProvide>(
            "struct Caps { buf: Stashing<GateMsg>, rate: Limiter }",
        )
        .expect("valid cap-set struct");
        let out = parsed.to_token_stream().to_string();
        assert_eq!(
            out.matches("impl :: bombay :: caps :: Replay <").count(),
            1,
            "only the stash field participates in replay: {out}"
        );
        assert!(
            !out.contains("__CapsReplayMsg"),
            "mixed set with a stash: concrete impl only, no blanket: {out}"
        );
    }
}
