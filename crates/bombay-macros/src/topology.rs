use proc_macro2::TokenStream;
use quote::{format_ident, quote};
use std::collections::{BTreeMap, BTreeSet};
use syn::parse::{Parse, ParseStream};
use syn::{Error, Ident, Result, Token, Type, Visibility, braced, parenthesized};

pub(crate) struct TopologyDeclaration {
    visibility: Visibility,
    name: Ident,
    actors: Vec<ActorClass>,
    hosts: Vec<LogicalHost>,
    members: Vec<TopologyChild>,
    families: Vec<EntityFamily>,
    interpreter: Option<Type>,
}

struct ActorClass {
    name: Ident,
    behavior: Option<Type>,
    role_parent: Option<Type>,
    host: Option<Ident>,
    children: Vec<TopologyChild>,
}

struct TopologyChild {
    name: Ident,
    role: Option<Type>,
    actor: Ident,
}

struct LogicalHost {
    name: Ident,
    protocol: Type,
}

struct EntityFamily {
    actor: Ident,
    definition: Type,
}

impl Parse for TopologyDeclaration {
    #[expect(
        clippy::too_many_lines,
        reason = "parse the closed actor, logical-host, cold-member and hydrated-family declaration without a second syntax model"
    )]
    fn parse(input: ParseStream<'_>) -> Result<Self> {
        let visibility = input.parse()?;
        let name = input.parse()?;
        let content;
        braced!(content in input);
        let mut actors = Vec::new();
        let mut hosts = Vec::new();
        let mut members = Vec::new();
        let mut families = Vec::new();
        let mut interpreter = None;
        while !content.is_empty() {
            let name: Ident = content.parse()?;
            if name == "families" && content.peek(syn::token::Brace) {
                let declarations;
                braced!(declarations in content);
                while !declarations.is_empty() {
                    let actor = declarations.parse()?;
                    declarations.parse::<Token![:]>()?;
                    let definition = declarations.parse()?;
                    declarations.parse::<Token![;]>()?;
                    families.push(EntityFamily { actor, definition });
                }
                continue;
            }
            if name == "hosts" && content.peek(syn::token::Brace) {
                let fields;
                braced!(fields in content);
                while !fields.is_empty() {
                    let name = fields.parse()?;
                    fields.parse::<Token![:]>()?;
                    let protocol = fields.parse()?;
                    fields.parse::<Token![;]>()?;
                    hosts.push(LogicalHost { name, protocol });
                }
                continue;
            }
            if name == "members" && content.peek(syn::token::Brace) {
                let declarations;
                braced!(declarations in content);
                while !declarations.is_empty() {
                    let name = declarations.parse()?;
                    let declared;
                    parenthesized!(declared in declarations);
                    let role = Some(declared.parse()?);
                    declarations.parse::<Token![=>]>()?;
                    let actor = declarations.parse()?;
                    declarations.parse::<Token![;]>()?;
                    members.push(TopologyChild { name, role, actor });
                }
                continue;
            }
            let behavior = if content.peek(Token![:]) {
                content.parse::<Token![:]>()?;
                Some(content.parse()?)
            } else {
                None
            };
            if name == "interpreter" && content.peek(Token![;]) {
                let Some(configuration) = behavior else {
                    return Err(Error::new_spanned(
                        name,
                        "interpreter requires its configuration type",
                    ));
                };
                if interpreter.replace(configuration).is_some() {
                    return Err(Error::new_spanned(
                        name,
                        "one interpreter configuration per topology",
                    ));
                }
                content.parse::<Token![;]>()?;
                continue;
            }
            let role_parent = if content.peek(Token![for]) {
                content.parse::<Token![for]>()?;
                Some(content.parse()?)
            } else {
                None
            };
            let host = if content.peek(Token![in]) {
                content.parse::<Token![in]>()?;
                Some(content.parse()?)
            } else {
                None
            };
            let edges;
            braced!(edges in content);
            let mut children = Vec::new();
            while !edges.is_empty() {
                let name = edges.parse()?;
                let role = if edges.peek(syn::token::Paren) {
                    let declared;
                    parenthesized!(declared in edges);
                    Some(declared.parse()?)
                } else {
                    None
                };
                edges.parse::<Token![=>]>()?;
                let actor = edges.parse()?;
                edges.parse::<Token![;]>()?;
                children.push(TopologyChild { name, role, actor });
            }
            actors.push(ActorClass {
                name,
                behavior,
                role_parent,
                host,
                children,
            });
        }
        let declaration = Self {
            visibility,
            name,
            actors,
            hosts,
            members,
            families,
            interpreter,
        };
        declaration.validate()?;
        Ok(declaration)
    }
}

impl TopologyDeclaration {
    #[expect(
        clippy::too_many_lines,
        reason = "validate the complete closed graph with ordinary, cold and hydrated roots before deriving its types"
    )]
    fn validate(&self) -> Result<()> {
        let mut classes = BTreeMap::new();
        let mut custody_names = BTreeMap::new();
        for actor in &self.actors {
            if let Some(parent) = &actor.role_parent {
                if actor.name == "root" {
                    return Err(Error::new_spanned(
                        parent,
                        "root domain actor uses root: Type",
                    ));
                }
                if !actor.children.iter().any(|child| child.role.is_some()) {
                    return Err(Error::new_spanned(
                        parent,
                        "nominal role parent requires a named child role",
                    ));
                }
            }
            if classes.insert(actor.name.to_string(), actor).is_some() {
                return Err(Error::new_spanned(
                    &actor.name,
                    "one declaration per actor class",
                ));
            }
            if let Some(previous) = custody_names.insert(actor_name(&actor.name), &actor.name) {
                return Err(Error::new_spanned(
                    &actor.name,
                    format!(
                        "actor classes {previous} and {} generate the same custody names",
                        actor.name,
                    ),
                ));
            }
            let mut roles = BTreeSet::new();
            for child in &actor.children {
                if !roles.insert(child.name.to_string()) {
                    return Err(Error::new_spanned(&child.name, "duplicate child role"));
                }
                if child.role.is_none() && actor.children.len() != 1 {
                    return Err(Error::new_spanned(
                        &child.name,
                        "multiple child edges require their existing semantic ChildRole types",
                    ));
                }
            }
        }
        let Some(root) = classes.get("root") else {
            return Err(Error::new_spanned(
                &self.name,
                "declare the root actor class",
            ));
        };
        let mut roles = root
            .children
            .iter()
            .map(|child| child.name.to_string())
            .collect::<BTreeSet<_>>();
        let mut pending = vec![*root];
        let mut family_classes = BTreeSet::new();
        for family in &self.families {
            let Some(actor) = classes.get(&family.actor.to_string()) else {
                return Err(Error::new_spanned(
                    &family.actor,
                    "Entity family requires a declared actor class",
                ));
            };
            if !family_classes.insert(family.actor.to_string()) {
                return Err(Error::new_spanned(
                    &family.actor,
                    "one Entity definition per actor class",
                ));
            }
            if actor.name == "root" || actor.behavior.is_some() {
                return Err(Error::new_spanned(
                    &family.actor,
                    "Entity actor type comes from its definition, independently of the application root",
                ));
            }
            pending.push(*actor);
        }
        for member in &self.members {
            if !roles.insert(member.name.to_string()) {
                return Err(Error::new_spanned(
                    &member.name,
                    "duplicate root or cold child role",
                ));
            }
            let Some(target) = classes.get(&member.actor.to_string()) else {
                return Err(Error::new_spanned(
                    &member.actor,
                    "cold target is not a declared actor class",
                ));
            };
            pending.push(*target);
        }
        let mut visited = BTreeSet::new();
        while let Some(actor) = pending.pop() {
            if !visited.insert(actor.name.to_string()) {
                continue;
            }
            for child in &actor.children {
                let Some(target) = classes.get(&child.actor.to_string()) else {
                    return Err(Error::new_spanned(
                        &child.actor,
                        "child target is not a declared actor class",
                    ));
                };
                pending.push(*target);
            }
        }
        for actor in &self.actors {
            if !visited.contains(&actor.name.to_string()) {
                return Err(Error::new_spanned(
                    &actor.name,
                    "actor class is unreachable from the root",
                ));
            }
        }
        self.validate_hosts()?;
        self.validate_inference()
    }

    fn validate_hosts(&self) -> Result<()> {
        for actor in &self.actors {
            if let Some(host) = &actor.host {
                if !self.hosts.iter().any(|declared| declared.name == *host) {
                    return Err(Error::new_spanned(
                        host,
                        "actor registration requires a declared logical host",
                    ));
                }
                if actor.name == "root" && host != "root" {
                    return Err(Error::new_spanned(
                        host,
                        "the root registers in its existing root family",
                    ));
                }
            }
        }
        let mut fields = BTreeSet::new();
        for host in &self.hosts {
            if !fields.insert(host.name.to_string()) {
                return Err(Error::new_spanned(
                    &host.name,
                    "duplicate logical hosting class",
                ));
            }
        }
        Ok(())
    }

    pub(crate) fn expand(&self, bombay: &TokenStream) -> TokenStream {
        let mut aliases = Vec::new();
        let actors = self
            .actors
            .iter()
            .map(|actor| {
                let behavior = actor.behavior.as_ref().map(|declared| {
                    let name = format_ident!("__BombayTopology_{}_{}", self.name, aliases.len());
                    aliases.push(quote!(#[allow(non_camel_case_types)] type #name = #declared;));
                    syn::parse_quote!(super::#name)
                });
                let role_parent = actor.role_parent.as_ref().map(|declared| {
                    let name = format_ident!("__BombayTopology_{}_{}", self.name, aliases.len());
                    aliases.push(quote!(#[allow(non_camel_case_types)] type #name = #declared;));
                    syn::parse_quote!(super::#name)
                });
                let children = actor
                    .children
                    .iter()
                    .map(|child| {
                        let role = child.role.as_ref().map(|role| {
                            let name =
                                format_ident!("__BombayTopology_{}_{}", self.name, aliases.len());
                            aliases
                                .push(quote!(#[allow(non_camel_case_types)] type #name = #role;));
                            syn::parse_quote!(super::#name)
                        });
                        TopologyChild {
                            name: child.name.clone(),
                            role,
                            actor: child.actor.clone(),
                        }
                    })
                    .collect();
                ActorClass {
                    name: actor.name.clone(),
                    behavior,
                    role_parent,
                    host: actor.host.clone(),
                    children,
                }
            })
            .collect();
        let hosts = self
            .hosts
            .iter()
            .map(|host| {
                let name = format_ident!("__BombayTopology_{}_{}", self.name, aliases.len());
                let protocol = &host.protocol;
                aliases.push(quote!(#[allow(non_camel_case_types)] type #name = #protocol;));
                LogicalHost {
                    name: host.name.clone(),
                    protocol: syn::parse_quote!(super::#name),
                }
            })
            .collect();
        let members = self
            .members
            .iter()
            .map(|member| {
                let role = member.role.as_ref().map(|role| {
                    let name = format_ident!("__BombayTopology_{}_{}", self.name, aliases.len());
                    aliases.push(quote!(#[allow(non_camel_case_types)] type #name = #role;));
                    syn::parse_quote!(super::#name)
                });
                TopologyChild {
                    name: member.name.clone(),
                    role,
                    actor: member.actor.clone(),
                }
            })
            .collect();
        let families = self
            .families
            .iter()
            .map(|family| {
                let name = format_ident!("__BombayTopology_{}_{}", self.name, aliases.len());
                let definition = &family.definition;
                aliases.push(quote!(#[allow(non_camel_case_types)] type #name = #definition;));
                EntityFamily {
                    actor: family.actor.clone(),
                    definition: syn::parse_quote!(super::#name),
                }
            })
            .collect();
        let interpreter = self.interpreter.as_ref().map(|declared| {
            let name = format_ident!("__BombayTopology_{}_{}", self.name, aliases.len());
            aliases.push(quote!(#[allow(non_camel_case_types)] type #name = #declared;));
            syn::parse_quote!(super::#name)
        });
        let resolved = Self {
            visibility: self.visibility.clone(),
            name: self.name.clone(),
            actors,
            hosts,
            members,
            families,
            interpreter,
        };
        let expansion = resolved.expand_resolved(bombay);
        quote!(#(#aliases)* #expansion)
    }

    fn runtime(&self, bombay: &TokenStream) -> (Ident, TokenStream, TokenStream, TokenStream) {
        let mut field = format_ident!("__bombay_runtime");
        while self.hosts.iter().any(|host| host.name == field) {
            field = format_ident!("_{}", field);
        }
        let (runtime, parameters, constructor) = match &self.interpreter {
            Some(configuration) => (
                quote!(#bombay::Runtime<#configuration>),
                quote!(configuration: #configuration),
                quote!(#bombay::Runtime::with_interpreter(configuration)),
            ),
            None => (
                quote!(#bombay::Runtime),
                TokenStream::new(),
                quote!(#bombay::Runtime::new()),
            ),
        };
        (field, runtime, parameters, constructor)
    }

    fn caller_visibility(&self) -> Visibility {
        match &self.visibility {
            Visibility::Inherited => syn::parse_quote!(pub(super)),
            Visibility::Restricted(visibility) if visibility.path.is_ident("self") => {
                syn::parse_quote!(pub(super))
            }
            Visibility::Restricted(visibility)
                if visibility
                    .path
                    .segments
                    .first()
                    .is_some_and(|segment| segment.ident == "super") =>
            {
                let path = &visibility.path;
                syn::parse_quote!(pub(in super::#path))
            }
            visibility => visibility.clone(),
        }
    }

    fn root_class(&self) -> &ActorClass {
        self.actors
            .iter()
            .find(|actor| actor.name == "root")
            .expect("validated root class")
    }

    fn scope(&self) -> TokenStream {
        if self.root_class().behavior.is_none() {
            quote!(Topology<ExecutedRoot>)
        } else {
            quote!(Topology)
        }
    }

    fn root_protocol_bounds(bombay: &TokenStream) -> TokenStream {
        quote!(ExecutedRoot: #bombay::behavior::BehaviorBase + #bombay::behavior::Behavior<Protocol: #bombay::behavior::Protocol<Addr = #bombay::MailAddr>>)
    }

    fn select_birth_types<'a>(
        &'a self,
        known: &mut BTreeSet<String>,
        selected: &mut Vec<(&'a ActorClass, &'a TopologyChild)>,
    ) {
        loop {
            let before = known.len();
            for parent in &self.actors {
                if !known.contains(&parent.name.to_string()) {
                    continue;
                }
                for child in &parent.children {
                    if child.actor == "root" {
                        continue;
                    }
                    let name = child.actor.to_string();
                    if known.insert(name) {
                        selected.push((parent, child));
                    }
                }
            }
            if known.len() == before {
                return;
            }
        }
    }

    fn incoming_occurrences(&self) -> Result<Vec<(&ActorClass, &TopologyChild)>> {
        let mut known = self
            .actors
            .iter()
            .filter(|actor| actor.behavior.is_some())
            .map(|actor| actor.name.to_string())
            .collect::<BTreeSet<_>>();
        let mut selected = Vec::new();
        known.extend(self.families.iter().map(|family| family.actor.to_string()));
        // Prefer existing concrete anchors before deriving opaque root projections.
        self.select_birth_types(&mut known, &mut selected);
        known.insert("root".into());
        self.select_birth_types(&mut known, &mut selected);
        known.extend(self.members.iter().map(|member| member.actor.to_string()));
        self.select_birth_types(&mut known, &mut selected);
        if let Some(actor) = self
            .actors
            .iter()
            .find(|actor| !known.contains(&actor.name.to_string()))
        {
            return Err(Error::new_spanned(
                &actor.name,
                "actor class requires a birth occurrence or an explicit actor type",
            ));
        }
        Ok(selected)
    }

    fn validate_inference(&self) -> Result<()> {
        let root = self.root_class();
        if root.behavior.is_none() && (!self.members.is_empty() || root.children.len() > 1) {
            return Err(Error::new_spanned(
                &root.name,
                "cold members or multiple birth roles require the root domain actor type",
            ));
        }
        self.incoming_occurrences()?;
        let (_, sources, _) = self.actor_types(&quote!(::bombay));
        let dependent = sources
            .iter()
            .filter(|(_, parameters)| !parameters.is_empty())
            .map(|(name, _)| name.clone())
            .collect();
        let schemas = self.schema_dependencies(&dependent);
        for actor in &self.actors {
            if !schemas.contains(&actor.name.to_string()) {
                continue;
            }
            let mut visited = BTreeSet::new();
            let mut pending = vec![actor];
            while let Some(parent) = pending.pop() {
                if !visited.insert(parent.name.to_string()) {
                    continue;
                }
                for child in &parent.children {
                    if child.actor == actor.name {
                        return Err(Error::new_spanned(
                            &actor.name,
                            "recursive actor classes require a concrete actor type anchor",
                        ));
                    }
                    if schemas.contains(&child.actor.to_string()) {
                        pending.push(
                            self.actors
                                .iter()
                                .find(|target| target.name == child.actor)
                                .expect("validated birth target"),
                        );
                    }
                }
            }
        }
        Ok(())
    }

    #[expect(
        clippy::type_complexity,
        reason = "the exact class types, root dependencies and defining incoming occurrences are direct compile-time collections; a wrapper would add no domain law"
    )]
    fn actor_types(
        &self,
        bombay: &TokenStream,
    ) -> (
        BTreeMap<String, Type>,
        BTreeMap<String, BTreeSet<Ident>>,
        BTreeMap<String, (String, String)>,
    ) {
        let mut types = self
            .actors
            .iter()
            .filter_map(|actor| {
                actor
                    .behavior
                    .as_ref()
                    .map(|ty| (actor.name.to_string(), ty.clone()))
            })
            .collect::<BTreeMap<_, _>>();
        for family in &self.families {
            let definition = &family.definition;
            types.insert(
                family.actor.to_string(),
                syn::parse_quote!(<#definition as #bombay::entity::EntityDefinition>::Behavior),
            );
        }
        let mut sources = BTreeMap::<String, BTreeSet<Ident>>::new();
        let mut selected = BTreeMap::new();
        let mut pending = self
            .actors
            .iter()
            .map(|actor| actor.name.to_string())
            .collect::<BTreeSet<_>>();
        for name in types.keys() {
            pending.remove(name);
            sources.insert(name.clone(), BTreeSet::new());
        }
        if self.root_class().behavior.is_none() {
            let root = format_ident!("ExecutedRoot");
            types.insert("root".into(), syn::parse_quote!(#root));
            sources.insert("root".into(), BTreeSet::from([root]));
            pending.remove("root");
        }
        loop {
            let before = pending.len();
            for parent in &self.actors {
                for child in &parent.children {
                    let name = child.actor.to_string();
                    if !pending.contains(&name) {
                        continue;
                    }
                    let (base, dependencies) = if let Some(base) = &parent.role_parent
                        && child.role.is_some()
                    {
                        (quote!(#base), BTreeSet::new())
                    } else if let Some(creator) = types.get(&parent.name.to_string()) {
                        (
                            role_parent(parent, creator, bombay),
                            sources[&parent.name.to_string()].clone(),
                        )
                    } else {
                        continue;
                    };
                    let position = child_position(child, &base, bombay);
                    let child_actor = if let Some(role) = &child.role {
                        syn::parse_quote!(<#role as #bombay::behavior::ChildRole<#base>>::Child)
                    } else {
                        let creator = &types[&parent.name.to_string()];
                        syn::parse_quote!(<<<#creator as #bombay::behavior::Behavior>::Birth as #bombay::behavior::BirthMode>::Child as #bombay::behavior::BirthNodeAt<#position>>::Child)
                    };
                    types.insert(name.clone(), child_actor);
                    sources.insert(name.clone(), dependencies);
                    pending.remove(&name);
                    selected.insert(name, (parent.name.to_string(), child.name.to_string()));
                }
            }
            if before == pending.len() {
                let mut seeded = Vec::new();
                for member in &self.members {
                    let name = member.actor.to_string();
                    if !pending.contains(&name) {
                        continue;
                    }
                    let parameter = format_ident!("{}Actor", actor_name(&member.actor));
                    types.insert(name.clone(), syn::parse_quote!(#parameter));
                    sources.insert(name.clone(), BTreeSet::from([parameter]));
                    seeded.push(name);
                }
                if seeded.is_empty() {
                    break;
                }
                for name in seeded {
                    pending.remove(&name);
                }
            }
        }
        assert!(pending.is_empty(), "validated actor class sources");
        (types, sources, selected)
    }

    fn schema_dependencies(&self, dependent: &BTreeSet<String>) -> BTreeSet<String> {
        let mut schemas = self
            .actors
            .iter()
            .filter(|actor| {
                !actor.children.is_empty()
                    && (dependent.contains(&actor.name.to_string())
                        || actor
                            .children
                            .iter()
                            .any(|child| dependent.contains(&child.actor.to_string())))
            })
            .map(|actor| actor.name.to_string())
            .collect::<BTreeSet<_>>();
        loop {
            let before = schemas.len();
            for actor in &self.actors {
                if actor
                    .children
                    .iter()
                    .any(|child| schemas.contains(&child.actor.to_string()))
                {
                    schemas.insert(actor.name.to_string());
                }
            }
            if before == schemas.len() {
                return schemas;
            }
        }
    }

    fn actor_bounds(
        &self,
        types: &BTreeMap<String, Type>,
        dependent: &BTreeSet<String>,
        selected: &BTreeMap<String, (String, String)>,
        bombay: &TokenStream,
    ) -> TokenStream {
        let root = Self::root_protocol_bounds(bombay);
        let mut bounds = vec![quote!(#root,)];
        for actor in &self.actors {
            if !dependent.contains(&actor.name.to_string()) {
                continue;
            }
            let creator = &types[&actor.name.to_string()];
            let parent_base = role_parent(actor, creator, bombay);
            bounds.push(role_parent_bounds(actor, creator, bombay));
            if actor.name != "root" {
                bounds.push(quote!(#creator: #bombay::behavior::BehaviorBase + #bombay::behavior::BehaviorSettlements<Protocol: #bombay::behavior::Protocol<Addr = #bombay::MailAddr>, Ph = #bombay::behavior::Never>,));
                if let Some(host) = &actor.host {
                    let protocol = &self
                        .hosts
                        .iter()
                        .find(|declared| declared.name == *host)
                        .expect("validated host")
                        .protocol;
                    bounds
                        .push(quote!(#creator: #bombay::behavior::Behavior<Protocol = #protocol>,));
                }
            }
            if actor.children.is_empty() {
                bounds.push(quote!(<#creator as #bombay::behavior::Behavior>::Birth: #bombay::behavior::BirthMode<Child = #bombay::behavior::Never>,));
            }
            for child in &actor.children {
                if let Some(role) = &child.role {
                    bounds.push(quote!(#parent_base: #bombay::behavior::Behavior, #role: #bombay::behavior::ChildRole<#parent_base>,));
                }
                let position = child_position(child, &parent_base, bombay);
                let target = &types[&child.actor.to_string()];
                let equality = if child.role.is_none()
                    && selected.get(&child.actor.to_string())
                        == Some(&(actor.name.to_string(), child.name.to_string()))
                {
                    TokenStream::new()
                } else {
                    quote!(, Child = #target)
                };
                bounds.push(role_consistency(
                    actor,
                    child,
                    &parent_base,
                    target,
                    selected,
                    bombay,
                ));
                bounds.push(quote!(<<#creator as #bombay::behavior::Behavior>::Birth as #bombay::behavior::BirthMode>::Child: #bombay::behavior::BirthNodeAt<#position #equality>,));
            }
        }
        quote!(#(#bounds)*)
    }

    #[expect(
        clippy::too_many_lines,
        reason = "derive one complete cold occurrence schema, its actor dependencies and exact native creation proofs"
    )]
    fn expand_members(&self, bombay: &TokenStream) -> TokenStream {
        let root = self.root_class();
        let root_actor = root.behavior.as_ref().expect("cold roots are nominal");
        let root_protocol = quote!(<#root_actor as #bombay::behavior::Behavior>::Protocol);
        let (types, sources, selected) = self.actor_types(bombay);
        let mut custody = self
            .actors
            .iter()
            .map(|actor| {
                let mut parameters = BTreeSet::new();
                if !actor.children.is_empty() && actor.role_parent.is_none() {
                    parameters.extend(sources[&actor.name.to_string()].clone());
                }
                if actor.name == "root" {
                    parameters.insert(format_ident!("OriginalRoot"));
                }
                for child in &actor.children {
                    parameters.extend(sources[&child.actor.to_string()].clone());
                }
                if actor.name == "root" {
                    for member in &self.members {
                        parameters.extend(sources[&member.actor.to_string()].clone());
                    }
                }
                (actor.name.to_string(), parameters)
            })
            .collect::<BTreeMap<_, _>>();
        loop {
            let before = custody.clone();
            for actor in &self.actors {
                if actor.children.is_empty() && actor.name != "root" {
                    continue;
                }
                let mut inherited = BTreeSet::new();
                for child in &actor.children {
                    inherited.extend(before[&child.actor.to_string()].clone());
                }
                if actor.name == "root" {
                    for member in &self.members {
                        inherited.extend(before[&member.actor.to_string()].clone());
                    }
                }
                custody
                    .get_mut(&actor.name.to_string())
                    .expect("declared custody")
                    .extend(inherited);
            }
            if custody == before {
                break;
            }
        }
        let generic_tokens = |parameters: &BTreeSet<Ident>| {
            if parameters.is_empty() {
                TokenStream::new()
            } else {
                let parameters = parameters.iter();
                quote!(<#(#parameters),*>)
            }
        };
        let schema_tokens = |name: &Ident| {
            let actor = self
                .actors
                .iter()
                .find(|actor| actor.name == *name)
                .expect("declared class");
            let schema = retirement_name(name);
            let parameters = generic_tokens(&custody[&name.to_string()]);
            if actor.children.is_empty() && actor.name != "root" {
                quote!(#schema)
            } else {
                quote!(#schema #parameters)
            }
        };
        let parameter_bounds = |parameters: &BTreeSet<Ident>| {
            let mut bounds = Vec::new();
            if parameters.contains(&format_ident!("OriginalRoot")) {
                bounds.push(quote!(OriginalRoot: #bombay::behavior::BehaviorBase<Base = <#root_actor as #bombay::behavior::BehaviorBase>::Base> + #bombay::behavior::Behavior<Protocol = #root_protocol>,));
            }
            for actor in &self.actors {
                let dependencies = &sources[&actor.name.to_string()];
                if dependencies.is_empty() || !dependencies.is_subset(parameters) {
                    continue;
                }
                let creator = &types[&actor.name.to_string()];
                bounds.push(quote!(#creator: #bombay::behavior::BehaviorSettlements<Protocol: #bombay::behavior::Protocol<Addr = #bombay::MailAddr>, Ph = #bombay::behavior::Never>,));
                if !actor.children.is_empty() {
                    bounds.push(quote!(#creator: #bombay::behavior::BehaviorBase,));
                }
                bounds.push(role_parent_bounds(actor, creator, bombay));
                if let Some(host) = &actor.host {
                    let protocol = &self
                        .hosts
                        .iter()
                        .find(|declared| declared.name == *host)
                        .expect("declared host")
                        .protocol;
                    bounds
                        .push(quote!(#creator: #bombay::behavior::Behavior<Protocol = #protocol>,));
                }
                if actor.children.is_empty() {
                    bounds.push(quote!(<#creator as #bombay::behavior::Behavior>::Birth: #bombay::behavior::BirthMode<Child = #bombay::behavior::Never>,));
                }
                for child in &actor.children {
                    let base = role_parent(actor, creator, bombay);
                    let position = child_position(child, &base, bombay);
                    let child_actor = &types[&child.actor.to_string()];
                    if let Some(role) = &child.role {
                        let equality = if selected.get(&child.actor.to_string())
                            == Some(&(actor.name.to_string(), child.name.to_string()))
                        {
                            TokenStream::new()
                        } else {
                            quote!(, Child = #child_actor)
                        };
                        bounds.push(quote!(#base: #bombay::behavior::Behavior, #role: #bombay::behavior::ChildRole<#base #equality>,));
                    }
                    let equality = if child.role.is_none()
                        && selected.get(&child.actor.to_string())
                            == Some(&(actor.name.to_string(), child.name.to_string()))
                    {
                        TokenStream::new()
                    } else {
                        quote!(, Child = #child_actor)
                    };
                    bounds.push(quote!(<<#creator as #bombay::behavior::Behavior>::Birth as #bombay::behavior::BirthMode>::Child: #bombay::behavior::BirthNodeAt<#position #equality>,));
                }
            }
            quote!(#(#bounds)*)
        };
        let caller_visibility = self.caller_visibility();
        let visibility = &self.visibility;
        let name = &self.name;
        let fields = self
            .hosts
            .iter()
            .filter(|host| host.name != "root")
            .map(|host| {
                let name = &host.name;
                let protocol = &host.protocol;
                quote!(#name: #bombay::ActorSpace<#protocol>)
            });
        let constructors = self
            .hosts
            .iter()
            .filter(|host| host.name != "root")
            .map(|host| {
                let name = &host.name;
                quote!(#name: #bombay::ActorSpace::new())
            });
        let hosting = self.hosts.iter().map(|host| { let name = &host.name; let protocol = &host.protocol; quote!(impl #bombay::Hosts<#protocol> for Topology { fn space(&self) -> &#bombay::ActorSpace<#protocol> { &self.#name } }) });
        let members = self.members.iter().fold(quote!(()), |tail, member| {
            let role = member.role.as_ref().expect("cold role");
            let actor = &types[&member.actor.to_string()];
            quote!((#role, #actor, #tail))
        });

        let mut expansions = Vec::new();
        for actor in &self.actors {
            let actor_type = &types[&actor.name.to_string()];
            let schema_name = retirement_name(&actor.name);
            if actor.children.is_empty() && actor.name != "root" {
                expansions
                    .push(quote!(#caller_visibility type #schema_name = #bombay::behavior::Never;));
                continue;
            }
            let parent_base = role_parent(actor, actor_type, bombay);
            let mut occurrences = actor
                .children
                .iter()
                .map(|child| {
                    let position = child_position(child, &parent_base, bombay);
                    (
                        child,
                        quote!(#bombay::ChildOrigin<#parent_base, #position>),
                        position,
                    )
                })
                .collect::<Vec<_>>();
            if actor.name == "root" {
                for (index, member) in self.members.iter().enumerate() {
                    let role = member.role.as_ref().expect("cold role");
                    let mut position = quote!(#bombay::behavior::ChildHead);
                    for _ in 0..actor.children.len() + self.members.len() - 1 - index {
                        position = quote!(#bombay::behavior::ChildTail<#position>);
                    }
                    occurrences.push((
                        member,
                        quote!(#bombay::ChildOrigin<OriginalRoot, #role>),
                        position,
                    ));
                }
            }
            let parameters = &custody[&actor.name.to_string()];
            let generics = generic_tokens(parameters);
            let bounds = parameter_bounds(parameters);
            let schema = schema_tokens(&actor.name);
            let mut retirement_rows = Vec::new();
            for (child, origin, position) in &occurrences {
                let target = self
                    .actors
                    .iter()
                    .find(|target| target.name == child.actor)
                    .expect("declared child");
                let child_actor = &types[&child.actor.to_string()];
                let descendants = schema_tokens(&child.actor);
                let creator = if actor.name == "root" {
                    quote!(ExecutedRoot)
                } else {
                    quote!(#actor_type)
                };
                let mut native_parameters = parameters.clone();
                native_parameters.extend(sources[&child.actor.to_string()].clone());
                native_parameters.extend(custody[&child.actor.to_string()].clone());
                native_parameters.extend(sources[&actor.name.to_string()].clone());
                if actor.name == "root" {
                    for member in &self.members {
                        native_parameters.extend(sources[&member.actor.to_string()].clone());
                    }
                }
                if actor.name == "root" {
                    native_parameters.insert(format_ident!("ExecutedRoot"));
                }
                let native_generics = generic_tokens(&native_parameters);
                let native_bounds = parameter_bounds(&native_parameters);
                let proof = if native_parameters.is_empty() {
                    TokenStream::new()
                } else {
                    quote!(Self: #bombay::EstablishNativeChild<#creator, #child_actor, #descendants>,)
                };
                let equality = if actor.name != "root"
                    && child.role.is_none()
                    && selected.get(&child.actor.to_string())
                        == Some(&(actor.name.to_string(), child.name.to_string()))
                {
                    TokenStream::new()
                } else {
                    quote!(, Child = #child_actor)
                };
                let eligible = quote!(#native_bounds
                    #creator: #bombay::behavior::Behavior<Protocol: #bombay::behavior::Protocol<Addr = #bombay::MailAddr>>,
                    <<#creator as #bombay::behavior::Behavior>::Birth as #bombay::behavior::BirthMode>::Child: #bombay::behavior::BirthNodeAt<#position #equality>,
                    #position: #bombay::behavior::ChildPosition<<<#creator as #bombay::behavior::Behavior>::Birth as #bombay::behavior::BirthMode>::Child, #child_actor> + 'static,
                    #proof
                );
                expansions.push(native_creation(
                    &creator,
                    position,
                    &quote!(#child_actor),
                    &descendants,
                    &schema,
                    &child_registration(target, bombay),
                    &native_generics,
                    &eligible,
                    &quote!(Topology),
                    bombay,
                ));
                retirement_rows.push((
                    child.name.clone(),
                    origin.clone(),
                    quote!(#child_actor),
                    descendants,
                ));
            }
            expansions.push(retirement_custody(
                &schema_name,
                &generics,
                &quote!(where #bounds),
                &retirement_rows,
                &caller_visibility,
                bombay,
            ));
        }
        let mut all_parameters = BTreeSet::from([format_ident!("OriginalRoot")]);
        for dependencies in sources.values() {
            all_parameters.extend(dependencies.clone());
        }
        let all_generics = generic_tokens(&all_parameters);
        let all_bounds = parameter_bounds(&all_parameters);
        let root_descendants = schema_tokens(&root.name);
        let (runtime_field, runtime, parameters, constructor) = self.runtime(bombay);
        quote!(#visibility mod #name {
            #caller_visibility struct Topology { root: #bombay::ActorSpace<#root_protocol>, #runtime_field: #runtime, #(#fields,)* }
            impl Topology { #caller_visibility fn new(#parameters) -> Self { Self { root: #bombay::ActorSpace::new(), #runtime_field: #constructor, #(#constructors,)* } } }
            impl #bombay::NativeHost for Topology {
                type Runtime = #runtime;
                fn runtime(&self) -> &Self::Runtime { &self.#runtime_field }
            }
            impl #bombay::RootRegistration<#root_protocol> for Topology { fn root_space(&self) -> &#bombay::ActorSpace<#root_protocol> { &self.root } }
            #(#hosting)* #(#expansions)*
            impl #all_generics #bombay::ApplicationTopology<OriginalRoot, #members> for Topology where #all_bounds { type Descendants = #root_descendants; }
        })
    }

    fn expand_resolved(&self, bombay: &TokenStream) -> TokenStream {
        if !self.members.is_empty() {
            return self.expand_members(bombay);
        }
        let root = self
            .actors
            .iter()
            .find(|actor| actor.name == "root")
            .expect("validated root class");
        let (types, sources, selected) = self.actor_types(bombay);
        let dependent = sources
            .iter()
            .filter(|(_, parameters)| !parameters.is_empty())
            .map(|(name, _)| name.clone())
            .collect();
        let schemas = self.schema_dependencies(&dependent);
        let root_actor = &types["root"];
        let scope = self.scope();
        let scope_generics = if root.behavior.is_none() {
            quote!(<ExecutedRoot>)
        } else {
            TokenStream::new()
        };
        let scope_bounds = if root.behavior.is_none() {
            let bounds = Self::root_protocol_bounds(bombay);
            quote!(where #bounds)
        } else {
            TokenStream::new()
        };
        let root_protocol = quote!(<#root_actor as #bombay::behavior::Behavior>::Protocol);
        let fields = self
            .hosts
            .iter()
            .filter(|host| host.name != "root")
            .map(|host| {
                let name = &host.name;
                let protocol = &host.protocol;
                quote!(#name: #bombay::ActorSpace<#protocol>)
            });
        let constructors = self
            .hosts
            .iter()
            .filter(|host| host.name != "root")
            .map(|host| {
                let name = &host.name;
                quote!(#name: #bombay::ActorSpace::new())
            });
        let hosting = self.hosts.iter().map(|host| {
            let name = &host.name;
            let protocol = &host.protocol;
            quote! {
                impl #scope_generics #bombay::Hosts<#protocol> for #scope #scope_bounds {
                    fn space(&self) -> &#bombay::ActorSpace<#protocol> { &self.#name }
                }
            }
        });
        let actors = self.actors.iter().map(|actor| {
            self.retirement_schema(actor, &types, &dependent, &selected, &schemas, bombay)
        });
        let visibility = &self.visibility;
        let name = &self.name;
        let caller_visibility = self.caller_visibility();
        let (runtime_field, runtime, parameters, constructor) = self.runtime(bombay);
        quote! {
            #visibility mod #name {
                #caller_visibility struct Topology #scope_generics #scope_bounds {
                    root: #bombay::ActorSpace<#root_protocol>,
                    #runtime_field: #runtime,
                    #(#fields,)*
                }
                impl #scope_generics #scope #scope_bounds {
                    #caller_visibility fn new(#parameters) -> Self {
                        Self { root: #bombay::ActorSpace::new(), #runtime_field: #constructor, #(#constructors,)* }
                    }
                }
                impl #scope_generics #bombay::NativeHost for #scope #scope_bounds {
                    type Runtime = #runtime;
                    fn runtime(&self) -> &Self::Runtime { &self.#runtime_field }
                }
                impl #scope_generics #bombay::RootRegistration<#root_protocol> for #scope #scope_bounds {
                    fn root_space(&self) -> &#bombay::ActorSpace<#root_protocol> { &self.root }
                }
                #(#hosting)*
                #(#actors)*
            }
        }
    }

    #[expect(
        clippy::too_many_lines,
        reason = "derive one complete occurrence schema, including custody, failures and exact native creation proofs"
    )]
    fn retirement_schema(
        &self,
        actor: &ActorClass,
        types: &BTreeMap<String, Type>,
        dependent: &BTreeSet<String>,
        selected: &BTreeMap<String, (String, String)>,
        schemas: &BTreeSet<String>,
        bombay: &TokenStream,
    ) -> TokenStream {
        let caller_visibility = self.caller_visibility();
        let descendants = retirement_name(&actor.name);
        let primary = &types[&actor.name.to_string()];
        let parent_base = role_parent(actor, primary, bombay);
        let scope = self.scope();
        let root_bounds = Self::root_protocol_bounds(bombay);
        let actor_bounds = self.actor_bounds(types, dependent, selected, bombay);
        let schema_bounds = if schemas.contains(&actor.name.to_string()) {
            quote!(where #actor_bounds)
        } else {
            TokenStream::new()
        };
        let schema_generics = if schemas.contains(&actor.name.to_string()) {
            quote!(<ExecutedRoot>)
        } else {
            TokenStream::new()
        };
        let parent_terminal = quote!(#descendants #schema_generics);
        let mut retirement_rows = Vec::new();
        let mut native = Vec::new();
        let mut metadata = Vec::new();

        let children = actor
            .children
            .iter()
            .map(|child| {
                let position = child_position(child, &parent_base, bombay);
                let origin = quote!(#bombay::ChildOrigin<#parent_base, #position>);
                (child, origin)
            })
            .collect::<Vec<_>>();
        for (child, origin) in &children {
            let target = self
                .actors
                .iter()
                .find(|actor| actor.name == child.actor)
                .expect("validated child class");
            let child_actor = &types[&target.name.to_string()];
            let selected_child = if child.role.is_none()
                && selected.get(&target.name.to_string())
                    == Some(&(actor.name.to_string(), child.name.to_string()))
            {
                TokenStream::new()
            } else {
                quote!(, Child = #child_actor)
            };
            let child_retirements = retirement_name(&target.name);
            let child_descendants = if schemas.contains(&target.name.to_string()) {
                quote!(#child_retirements<ExecutedRoot>)
            } else {
                quote!(#child_retirements)
            };
            retirement_rows.push((
                child.name.clone(),
                origin.clone(),
                quote!(#child_actor),
                child_descendants.clone(),
            ));
            if actor.name == "root" {
                let position = child_position(child, &parent_base, bombay);
                let roles =
                    role_consistency(actor, child, &parent_base, child_actor, selected, bombay);
                let eligibility = if actor.behavior.is_none() {
                    quote!(#actor_bounds)
                } else {
                    quote!(ExecutedRoot: #bombay::behavior::BehaviorBase<Base = <#primary as #bombay::behavior::BehaviorBase>::Base> + #bombay::behavior::Behavior<Protocol = <#primary as #bombay::behavior::Behavior>::Protocol>,)
                };
                native.push(native_creation(&quote!(ExecutedRoot), &position, &quote!(#child_actor), &quote!(#child_descendants), &quote!(#parent_terminal), &child_registration(target, bombay), &quote!(<ExecutedRoot>), &quote!(
                    #eligibility
                    #roles
                    <<ExecutedRoot as #bombay::behavior::Behavior>::Birth as #bombay::behavior::BirthMode>::Child:
                        #bombay::behavior::BirthNodeAt<#position #selected_child>,
                    #position: #bombay::behavior::ChildPosition<<<ExecutedRoot as #bombay::behavior::Behavior>::Birth as #bombay::behavior::BirthMode>::Child, #child_actor>,
                    Self: #bombay::EstablishNativeChild<ExecutedRoot, #child_actor, #child_descendants>,
                ), &scope, bombay));
            }
            if actor.name != "root" {
                let creator = primary;
                let native_generics = if self.root_class().behavior.is_none() {
                    quote!(<ExecutedRoot>)
                } else {
                    TokenStream::new()
                };
                let ancestor_registration = if self.root_class().behavior.is_none() {
                    quote!(#root_bounds + 'static, <ExecutedRoot as #bombay::behavior::Behavior>::Protocol: 'static, <<ExecutedRoot as #bombay::behavior::Behavior>::Protocol as #bombay::behavior::Protocol>::Msg: Send,)
                } else {
                    TokenStream::new()
                };
                let position = child_position(child, &parent_base, bombay);
                let parent = role_parent_bounds(actor, creator, bombay);
                let roles =
                    role_consistency(actor, child, &parent_base, child_actor, selected, bombay);
                let creation_proof = if schemas.contains(&actor.name.to_string()) {
                    quote!(#actor_bounds Self: #bombay::EstablishNativeChild<#creator, #child_actor, #child_descendants>,)
                } else {
                    TokenStream::new()
                };
                native.push(native_creation(&quote!(#creator), &position, &quote!(#child_actor), &quote!(#child_descendants), &quote!(#parent_terminal), &child_registration(target, bombay), &native_generics, &quote!(
                    #ancestor_registration
                    #parent
                    #roles
                    #creation_proof
                    <<#creator as #bombay::behavior::Behavior>::Birth as #bombay::behavior::BirthMode>::Child:
                        #bombay::behavior::BirthNodeAt<#position #selected_child>,
                    #position: #bombay::behavior::ChildPosition<<<#creator as #bombay::behavior::Behavior>::Birth as #bombay::behavior::BirthMode>::Child, #child_actor>,
                ), &scope, bombay));
            }
        }
        if actor.name == "root" {
            let births = if children.is_empty() {
                quote!(<ExecutedRoot as #bombay::behavior::Behavior>::Birth: #bombay::behavior::BirthMode<Child = #bombay::behavior::Never>,)
            } else {
                TokenStream::new()
            };
            let eligibility = if actor.behavior.is_none() {
                quote!(#actor_bounds)
            } else {
                quote!(ExecutedRoot: #bombay::behavior::BehaviorBase<Base = <#primary as #bombay::behavior::BehaviorBase>::Base> + #bombay::behavior::Behavior<Protocol = <#primary as #bombay::behavior::Behavior>::Protocol>,)
            };
            metadata.push(quote! {
                impl<ExecutedRoot> #bombay::ApplicationTopology<ExecutedRoot, ()> for #scope
                where #eligibility #births { type Descendants = #parent_terminal; }
            });
        }
        let custody = retirement_custody(
            &descendants,
            &schema_generics,
            &schema_bounds,
            &retirement_rows,
            &caller_visibility,
            bombay,
        );
        quote!(#custody #(#native)* #(#metadata)*)
    }
}

fn retirement_custody(
    name: &Ident,
    generics: &TokenStream,
    bounds: &TokenStream,
    occurrences: &[(Ident, TokenStream, TokenStream, TokenStream)],
    visibility: &Visibility,
    bombay: &TokenStream,
) -> TokenStream {
    let schema = quote!(#name #generics);
    let failure_sum = format_ident!(
        "{}ChildFailure",
        name.to_string().trim_end_matches("Retirements")
    );
    let mut variants = Vec::new();
    let mut failures = Vec::new();
    let mut conversions = Vec::new();
    for (role, origin, actor, descendants) in occurrences {
        let retirement = quote!(#bombay::ActorRetirement<#actor, #descendants, ::std::vec::Vec<<#descendants as #bombay::RetirementSchema>::Failure>>);
        let failure = quote!(#bombay::ChildFailure<#origin, #actor>);
        variants
            .push(quote!(#role { origin: #origin, retirement: ::std::boxed::Box<#retirement> }));
        failures.push(quote!(#role(#failure)));
        let projected = if occurrences.len() == 1 {
            quote!(Self {
                origin,
                retirement: ::std::boxed::Box::new(retirement)
            })
        } else {
            quote!(Self::#role { origin, retirement: ::std::boxed::Box::new(retirement) })
        };
        conversions.push(quote! {
            impl #generics #bombay::ProjectTerminal<#origin, #actor> for #schema #bounds {
                type Descendants = #descendants;
                fn project(origin: #origin, retirement: #retirement) -> Self { #projected }
            }
        });
        if occurrences.len() > 1 {
            conversions.push(quote! {
                impl #generics ::core::convert::From<#failure> for #failure_sum #generics #bounds {
                    fn from(failure: #failure) -> Self { Self::#role(failure) }
                }
            });
        }
    }
    let definition = match occurrences {
        [] => quote!(#visibility type #name = #bombay::behavior::Never;),
        [(_, origin, actor, descendants)] => quote! {
            #visibility struct #name #generics #bounds {
                #visibility origin: #origin,
                #visibility retirement: ::std::boxed::Box<#bombay::ActorRetirement<#actor, #descendants, ::std::vec::Vec<<#descendants as #bombay::RetirementSchema>::Failure>>>,
            }
            impl #generics #bombay::RetirementSchema for #schema #bounds {
                type Failure = #bombay::ChildFailure<#origin, #actor>;
            }
        },
        _ => quote! {
            #visibility enum #name #generics #bounds { #(#variants,)* }
            #visibility enum #failure_sum #generics #bounds { #(#failures,)* }
            impl #generics #bombay::RetirementSchema for #schema #bounds {
                type Failure = #failure_sum #generics;
            }
        },
    };
    quote!(#definition #(#conversions)*)
}

fn actor_name(name: &Ident) -> String {
    let spelling = name.to_string();
    spelling
        .strip_prefix("r#")
        .unwrap_or(&spelling)
        .split('_')
        .map(|part| {
            let mut letters = part.chars();
            letters.next().map_or_else(String::new, |first| {
                first.to_uppercase().collect::<String>() + letters.as_str()
            })
        })
        .collect()
}

fn retirement_name(name: &Ident) -> Ident {
    format_ident!("{}Retirements", actor_name(name))
}

fn role_parent(actor: &ActorClass, creator: &Type, bombay: &TokenStream) -> TokenStream {
    if let Some(parent) = &actor.role_parent {
        quote!(#parent)
    } else {
        quote!(<#creator as #bombay::behavior::BehaviorBase>::Base)
    }
}

fn role_parent_bounds(actor: &ActorClass, creator: &Type, bombay: &TokenStream) -> TokenStream {
    if let Some(parent) = &actor.role_parent {
        quote!(#creator: #bombay::behavior::BehaviorBase<Base = #parent>,)
    } else {
        TokenStream::new()
    }
}

fn role_consistency(
    parent: &ActorClass,
    child: &TopologyChild,
    parent_base: &TokenStream,
    actor: &Type,
    selected: &BTreeMap<String, (String, String)>,
    bombay: &TokenStream,
) -> TokenStream {
    let Some(role) = &child.role else {
        return TokenStream::new();
    };
    if selected.get(&child.actor.to_string())
        == Some(&(parent.name.to_string(), child.name.to_string()))
    {
        TokenStream::new()
    } else {
        quote!(#role: #bombay::behavior::ChildRole<#parent_base, Child = #actor>,)
    }
}

fn child_position(
    child: &TopologyChild,
    parent_base: &TokenStream,
    bombay: &TokenStream,
) -> TokenStream {
    if let Some(role) = &child.role {
        quote!(<#role as #bombay::behavior::ChildRole<#parent_base>>::Position)
    } else {
        quote!(#bombay::behavior::ChildHead)
    }
}

#[expect(
    clippy::too_many_arguments,
    reason = "each input is a distinct part of the existing NativeChild implementation, not a new runtime product"
)]
fn native_creation(
    creator: &TokenStream,
    position: &TokenStream,
    child_actor: &TokenStream,
    child_descendants: &TokenStream,
    parent_terminal: &TokenStream,
    registration: &TokenStream,
    generics: &TokenStream,
    eligibility: &TokenStream,
    scope: &TokenStream,
    bombay: &TokenStream,
) -> TokenStream {
    quote! {
        impl #generics #bombay::NativeChild<#creator, #position, #child_actor, #parent_terminal, #child_descendants> for #scope
        where #eligibility
        {
                        fn registration(&self) -> #bombay::ActorSpace<<#child_actor as #bombay::behavior::Behavior>::Protocol> {
                            #registration
                        }

                        fn establish<Origin, OriginFn>(
                            pending: #bombay::PendingChild<'_, #creator, Self, #position, #child_actor, #parent_terminal, #child_descendants, OriginFn>,
                        ) -> impl ::core::future::Future<Output = #bombay::behavior::ItemSettlement<
                            #bombay::behavior::RoutedCreation<#bombay::MailAddr, #child_actor>,
                            #bombay::behavior::ChildCreationOutcome<#child_actor, #position>,
                            #bombay::behavior::CreationRejection,
                            #bombay::behavior::Never,
                        >> + Send
                        where
                            #parent_terminal: #bombay::ProjectTerminal<Origin, #child_actor, Descendants = #child_descendants> + Send + 'static,
                            Origin: Send + 'static,
                            OriginFn: Fn(#bombay::MailAddr, u64, #bombay::behavior::CreationId, #bombay::behavior::CreationKind) -> Origin + Send,
                        {
                            <Self as #bombay::EstablishNativeChild<#creator, #child_actor, #child_descendants>>::establish_native_child::<#position, #parent_terminal, Origin, OriginFn>(pending)
                        }

        }
    }
}

fn child_registration(actor: &ActorClass, bombay: &TokenStream) -> TokenStream {
    if let Some(host) = &actor.host {
        quote!(self.#host.clone())
    } else {
        quote!(#bombay::ActorSpace::new())
    }
}
