//! Statically composed, structurally representable executable machines.

/// Compact identity of a topology vertex.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct VertexId(pub u8);

/// Compact identity of an input trigger.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct TriggerId(pub u8);

/// One named topology vertex.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Vertex {
    /// Identity used by execution and structural interpreters.
    pub id: VertexId,
    /// Human-readable label used only by renderers.
    pub label: &'static str,
}

/// One declared edge in a base machine's topology.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Transition {
    /// Source vertex identity.
    pub from: VertexId,
    /// Input identity selecting the edge.
    pub trigger: TriggerId,
    /// Destination vertex identity.
    pub to: VertexId,
    /// Human-readable trigger label used only by renderers.
    pub label: &'static str,
}

/// Inspectable metadata for one indivisible machine.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Topology {
    /// Stable component name.
    pub name: &'static str,
    /// Initial vertex.
    pub initial: VertexId,
    /// Declared vertices.
    pub vertices: &'static [Vertex],
    /// Declared transition graph.
    pub transitions: &'static [Transition],
}

/// Topology whose identities, references, and reachability were validated.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ValidatedTopology(Topology);

impl ValidatedTopology {
    /// Borrow the validated descriptive topology.
    #[must_use]
    pub const fn topology(self) -> Topology {
        self.0
    }
}

/// Structural defect found in a topology.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum TopologyError {
    /// Two vertices have the same identity.
    #[error("two vertices share an identity")]
    DuplicateVertex(VertexId),
    /// Two edges share a source vertex and trigger.
    #[error("two transitions share a source and trigger")]
    DuplicateTransition(Transition),
    /// The initial identity is not a declared vertex.
    #[error("initial identity is not a declared vertex")]
    UnknownInitial(VertexId),
    /// An edge refers to an undeclared vertex.
    #[error("transition refers to an undeclared vertex")]
    UnknownVertex(VertexId),
    /// A declared vertex cannot be reached from the initial vertex.
    #[error("declared vertex is unreachable from the initial vertex")]
    UnreachableVertex(VertexId),
}

impl Topology {
    /// Validate and retain this topology for executable machine construction.
    ///
    /// # Errors
    ///
    /// Returns the first structural defect in declaration order.
    pub fn validated(self) -> Result<ValidatedTopology, TopologyError> {
        self.validate()?;
        Ok(ValidatedTopology(self))
    }

    /// Validate identity uniqueness, references, initial state, and reachability.
    ///
    /// Validation uses fixed stack storage because vertex identities are bytes;
    /// it performs no allocation and is suitable for `no_std` interpreters.
    ///
    /// # Errors
    ///
    /// Returns the first uniqueness, reference, initial-state, or reachability
    /// defect encountered in deterministic declaration order.
    pub fn validate(self) -> Result<(), TopologyError> {
        for (index, vertex) in self.vertices.iter().enumerate() {
            if self.vertices[..index]
                .iter()
                .any(|known| known.id == vertex.id)
            {
                return Err(TopologyError::DuplicateVertex(vertex.id));
            }
        }
        if !self.vertices.iter().any(|vertex| vertex.id == self.initial) {
            return Err(TopologyError::UnknownInitial(self.initial));
        }
        for (index, edge) in self.transitions.iter().enumerate() {
            if self.transitions[..index]
                .iter()
                .any(|known| known.from == edge.from && known.trigger == edge.trigger)
            {
                return Err(TopologyError::DuplicateTransition(*edge));
            }
            for vertex in [edge.from, edge.to] {
                if !self.vertices.iter().any(|known| known.id == vertex) {
                    return Err(TopologyError::UnknownVertex(vertex));
                }
            }
        }
        let mut reachable = [false; 256];
        reachable[usize::from(self.initial.0)] = true;
        for _ in 0..self.vertices.len() {
            for edge in self.transitions {
                if reachable[usize::from(edge.from.0)] {
                    reachable[usize::from(edge.to.0)] = true;
                }
            }
        }
        self.vertices
            .iter()
            .find(|vertex| !reachable[usize::from(vertex.id.0)])
            .map_or(Ok(()), |vertex| {
                Err(TopologyError::UnreachableVertex(vertex.id))
            })
    }

    /// Render a deterministic Mermaid state diagram into any formatting sink.
    ///
    /// # Errors
    ///
    /// Returns a formatting error from the sink or when an edge references an
    /// unknown vertex. [`Self::validate`] diagnoses the latter precisely.
    pub fn write_mermaid(self, output: &mut impl core::fmt::Write) -> core::fmt::Result {
        writeln!(output, "stateDiagram-v2")?;
        let initial = self.label(self.initial).ok_or(core::fmt::Error)?;
        writeln!(output, "    [*] --> {initial}")?;
        self.transitions.iter().try_for_each(|edge| {
            let from = self.label(edge.from).ok_or(core::fmt::Error)?;
            let to = self.label(edge.to).ok_or(core::fmt::Error)?;
            writeln!(output, "    {from} --> {to}: {}", edge.label)
        })
    }

    fn label(self, id: VertexId) -> Option<&'static str> {
        self.vertices
            .iter()
            .find(|vertex| vertex.id == id)
            .map(|vertex| vertex.label)
    }
}

/// A stateful transducer whose composition remains structurally inspectable.
pub trait Machine {
    /// Input consumed by one step.
    type Input;

    /// Output produced for one input.
    type Output;

    /// Consume one input and return the output plus successor machine.
    fn step(self, input: Self::Input) -> (Self::Output, Self)
    where
        Self: Sized;

    /// Fold the retained composition tree with a structural interpreter.
    fn describe<V: Structure>(&self, visitor: &mut V) -> V::Output;
}

/// Structural composition operations available to every machine.
pub trait Compose: Machine + Sized {
    /// Compose this machine sequentially with another machine.
    fn then<N>(self, next: N) -> Then<Self, N> {
        Then(self, next)
    }

    /// Compose this machine in a product with another machine.
    fn product<N>(self, other: N) -> Product<Self, N> {
        Product(self, other)
    }

    /// Compose this machine as an alternative to another machine.
    fn routed<N>(self, other: N) -> Routed<Self, N> {
        Routed(self, other)
    }
}

impl<M: Machine> Compose for M {}

/// Interpreter for the composition structure of a machine.
pub trait Structure {
    /// Representation produced for each subtree.
    type Output;

    /// Interpret an indivisible machine.
    fn base(&mut self, topology: Topology) -> Self::Output;

    /// Interpret sequential composition.
    fn then(&mut self, first: Self::Output, second: Self::Output) -> Self::Output;

    /// Interpret product composition.
    fn product(&mut self, left: Self::Output, right: Self::Output) -> Self::Output;

    /// Interpret sum composition.
    fn routed(&mut self, left: Self::Output, right: Self::Output) -> Self::Output;
}

/// An indivisible stateful machine and its inspectable topology.
pub struct Base<S, F, I, O> {
    state: S,
    transition: F,
    topology: ValidatedTopology,
    signature: core::marker::PhantomData<fn(I) -> O>,
}

impl<S, F, I, O> Base<S, F, I, O> {
    /// Construct a base machine from state, topology, and transition function.
    pub const fn new(state: S, topology: ValidatedTopology, transition: F) -> Self {
        Self {
            state,
            transition,
            topology,
            signature: core::marker::PhantomData,
        }
    }

    /// Borrow the retained machine state.
    #[must_use]
    pub const fn state(&self) -> &S {
        &self.state
    }
}

impl<S, I, O, F> Machine for Base<S, F, I, O>
where
    F: FnMut(S, I) -> (O, S),
{
    type Input = I;
    type Output = O;

    fn step(self, input: Self::Input) -> (Self::Output, Self) {
        let Self {
            state,
            mut transition,
            topology,
            signature,
        } = self;
        let (output, state) = transition(state, input);
        (
            output,
            Self {
                state,
                transition,
                topology,
                signature,
            },
        )
    }

    fn describe<V: Structure>(&self, visitor: &mut V) -> V::Output {
        visitor.base(self.topology.topology())
    }
}

/// Sequential composition of two machines.
pub struct Then<A, B>(A, B);

impl<A, B> Machine for Then<A, B>
where
    A: Machine,
    B: Machine<Input = A::Output>,
{
    type Input = A::Input;
    type Output = B::Output;

    fn step(self, input: Self::Input) -> (Self::Output, Self) {
        let (middle, first) = self.0.step(input);
        let (output, second) = self.1.step(middle);
        (output, Self(first, second))
    }

    fn describe<V: Structure>(&self, visitor: &mut V) -> V::Output {
        let first = self.0.describe(visitor);
        let second = self.1.describe(visitor);
        visitor.then(first, second)
    }
}

/// Product composition of two independent machines.
pub struct Product<A, B>(A, B);

impl<A, B> Machine for Product<A, B>
where
    A: Machine,
    B: Machine,
{
    type Input = (A::Input, B::Input);
    type Output = (A::Output, B::Output);

    fn step(self, (left, right): Self::Input) -> (Self::Output, Self) {
        let (left_output, left_machine) = self.0.step(left);
        let (right_output, right_machine) = self.1.step(right);
        (
            (left_output, right_output),
            Self(left_machine, right_machine),
        )
    }

    fn describe<V: Structure>(&self, visitor: &mut V) -> V::Output {
        let left = self.0.describe(visitor);
        let right = self.1.describe(visitor);
        visitor.product(left, right)
    }
}

/// One of two alternatives.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Either<L, R> {
    /// Left alternative.
    Left(L),
    /// Right alternative.
    Right(R),
}

/// Sum composition routing each alternative to its corresponding machine.
pub struct Routed<A, B>(A, B);

impl<A, B> Machine for Routed<A, B>
where
    A: Machine,
    B: Machine,
{
    type Input = Either<A::Input, B::Input>;
    type Output = Either<A::Output, B::Output>;

    fn step(self, input: Self::Input) -> (Self::Output, Self) {
        match input {
            Either::Left(left) => {
                let (output, machine) = self.0.step(left);
                (Either::Left(output), Self(machine, self.1))
            }
            Either::Right(right) => {
                let (output, machine) = self.1.step(right);
                (Either::Right(output), Self(self.0, machine))
            }
        }
    }

    fn describe<V: Structure>(&self, visitor: &mut V) -> V::Output {
        let left = self.0.describe(visitor);
        let right = self.1.describe(visitor);
        visitor.routed(left, right)
    }
}

#[cfg(test)]
mod tests {
    use alloc::string::ToString;

    use super::{
        Base, Compose, Machine, Structure, Topology, TopologyError, Transition, TriggerId, Vertex,
        VertexId,
    };

    #[test]
    fn topology_error_reports_each_defect() {
        assert_eq!(
            TopologyError::DuplicateVertex(READY).to_string(),
            "two vertices share an identity"
        );
        assert_eq!(
            TopologyError::UnknownInitial(READY).to_string(),
            "initial identity is not a declared vertex"
        );
        assert_eq!(
            TopologyError::UnknownVertex(READY).to_string(),
            "transition refers to an undeclared vertex"
        );
        assert_eq!(
            TopologyError::UnreachableVertex(READY).to_string(),
            "declared vertex is unreachable from the initial vertex"
        );
        assert_eq!(
            TopologyError::DuplicateTransition(EDGES[0]).to_string(),
            "two transitions share a source and trigger"
        );
    }

    const READY: VertexId = VertexId(0);
    const VERTICES: &[Vertex] = &[Vertex {
        id: READY,
        label: "ready",
    }];

    const EDGES: &[Transition] = &[Transition {
        from: READY,
        trigger: TriggerId(0),
        to: READY,
        label: "advance",
    }];
    const UNKNOWN: VertexId = VertexId(9);
    const UNKNOWN_EDGE: &[Transition] = &[Transition {
        from: READY,
        trigger: TriggerId(1),
        to: UNKNOWN,
        label: "lost",
    }];
    const STRANDED: VertexId = VertexId(1);
    const STRANDED_VERTICES: &[Vertex] = &[
        VERTICES[0],
        Vertex {
            id: STRANDED,
            label: "stranded",
        },
    ];
    const DUPLICATE_EDGES: &[Transition] = &[EDGES[0], EDGES[0]];
    const AMBIGUOUS: &[Transition] = &[
        EDGES[0],
        Transition {
            from: READY,
            trigger: TriggerId(0),
            to: STRANDED,
            label: "different presentation",
        },
    ];

    fn topology(name: &'static str) -> Topology {
        Topology {
            name,
            initial: READY,
            vertices: VERTICES,
            transitions: EDGES,
        }
    }

    #[derive(Debug, PartialEq, Eq)]
    struct Shape {
        bases: usize,
        sequences: usize,
        products: usize,
        choices: usize,
    }

    struct Count;

    impl Structure for Count {
        type Output = Shape;

        fn base(&mut self, _topology: Topology) -> Self::Output {
            Shape {
                bases: 1,
                sequences: 0,
                products: 0,
                choices: 0,
            }
        }

        fn then(&mut self, first: Self::Output, second: Self::Output) -> Self::Output {
            Shape {
                bases: first.bases + second.bases,
                sequences: first.sequences + second.sequences + 1,
                products: first.products + second.products,
                choices: first.choices + second.choices,
            }
        }

        fn product(&mut self, left: Self::Output, right: Self::Output) -> Self::Output {
            Shape {
                bases: left.bases + right.bases,
                sequences: left.sequences + right.sequences,
                products: left.products + right.products + 1,
                choices: left.choices + right.choices,
            }
        }

        fn routed(&mut self, left: Self::Output, right: Self::Output) -> Self::Output {
            Shape {
                bases: left.bases + right.bases,
                sequences: left.sequences + right.sequences,
                products: left.products + right.products,
                choices: left.choices + right.choices + 1,
            }
        }
    }

    #[test]
    fn sequential_machine_executes_and_retains_its_structure() {
        let increment = Base::new(
            0_u8,
            topology("increment").validated().unwrap(),
            |state: u8, input: u8| {
                let state = state + input;
                (state, state)
            },
        );
        let double = Base::new(
            (),
            topology("double").validated().unwrap(),
            |(), input: u8| (input * 2, ()),
        );
        let machine = increment.then(double);

        let (output, machine) = machine.step(3);
        assert_eq!(output, 6);
        let (output, machine) = machine.step(1);
        assert_eq!(output, 8);
        assert_eq!(
            machine.describe(&mut Count),
            Shape {
                bases: 2,
                sequences: 1,
                products: 0,
                choices: 0,
            }
        );
    }

    #[test]
    fn topology_validation_rejects_each_structural_defect() {
        const DUPLICATE_VERTICES: &[Vertex] = &[
            Vertex {
                id: READY,
                label: "ready",
            },
            Vertex {
                id: READY,
                label: "again",
            },
        ];
        assert_eq!(
            Topology {
                name: "duplicate",
                initial: READY,
                vertices: DUPLICATE_VERTICES,
                transitions: &[]
            }
            .validate(),
            Err(super::TopologyError::DuplicateVertex(READY))
        );

        assert_eq!(
            Topology {
                name: "initial",
                initial: UNKNOWN,
                vertices: VERTICES,
                transitions: &[]
            }
            .validate(),
            Err(super::TopologyError::UnknownInitial(UNKNOWN))
        );

        assert_eq!(
            Topology {
                name: "reference",
                initial: READY,
                vertices: VERTICES,
                transitions: UNKNOWN_EDGE
            }
            .validate(),
            Err(super::TopologyError::UnknownVertex(UNKNOWN))
        );

        assert_eq!(
            Topology {
                name: "reachability",
                initial: READY,
                vertices: STRANDED_VERTICES,
                transitions: &[]
            }
            .validate(),
            Err(super::TopologyError::UnreachableVertex(STRANDED))
        );

        assert_eq!(
            Topology {
                name: "edge",
                initial: READY,
                vertices: VERTICES,
                transitions: DUPLICATE_EDGES
            }
            .validate(),
            Err(super::TopologyError::DuplicateTransition(EDGES[0]))
        );

        assert_eq!(
            Topology {
                name: "ambiguous",
                initial: READY,
                vertices: STRANDED_VERTICES,
                transitions: AMBIGUOUS,
            }
            .validate(),
            Err(super::TopologyError::DuplicateTransition(AMBIGUOUS[1]))
        );
    }

    #[test]
    fn product_and_routed_composition_preserve_untouched_affine_state() {
        struct Affine(u8);

        let left = Base::new(
            Affine(1),
            topology("left").validated().unwrap(),
            |state: Affine, input: u8| (state.0 + input, Affine(state.0 + input)),
        );
        let right = Base::new(
            Affine(10),
            topology("right").validated().unwrap(),
            |state: Affine, input: u8| (state.0 + input, Affine(state.0 + input)),
        );
        let (output, product) = left.product(right).step((2, 3));
        assert_eq!(output, (3, 13));

        let left = Base::new(
            Affine(1),
            topology("left").validated().unwrap(),
            |state: Affine, input: u8| (state.0 + input, Affine(state.0 + input)),
        );
        let right = Base::new(
            Affine(10),
            topology("right").validated().unwrap(),
            |state: Affine, input: u8| (state.0 + input, Affine(state.0 + input)),
        );
        let (output, routed) = left.routed(right).step(super::Either::Left(2));
        assert_eq!(output, super::Either::Left(3));
        let (output, _) = routed.step(super::Either::Right(3));
        assert_eq!(output, super::Either::Right(13));
        assert_eq!(product.describe(&mut Count).products, 1);
    }
}
