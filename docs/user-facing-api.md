# Bombay user-facing API

This is the target product contract. It deliberately separates the stable
experience from constructor names still owned by evolving Behavior Actors
templates.

## Entry boundary

A complete application supplies one ordinary actor-system value:

```rust,ignore
use bombay::prelude::*;

fn main() -> Result<(), RunError> {
    App::new(root(), AppActors::default()).run()
}
```

`run` owns asynchronous execution. Users do not write an auxiliary `async fn`,
construct a Tokio runtime, or construct a Guardian. Bombay provides no entry
or topology macros; the ordinary function is the only execution path.

## Application composition

The root is an ordinary Behavior composition. Application actors implement
Behavior explicitly; reusable roles are concrete Behavior Actors templates.
Values configure policy where it belongs:

```rust,ignore
let devices = DynamicSupervisor::new(DeviceGroup::new);
let queries = WorkerPool::new(TemperatureQuery::new, 8)?;
let root = IoTSystem::new(devices, queries);
```

This snippet is illustrative because exact template constructors continue to
belong to the Behavior Actors repository. Bombay must consume those concrete
values; it must not copy them into façade builders or macros.

The actor-system value packages the pure root separately from the named
product of runtime-owned local actor spaces:

```rust,ignore
#[derive(Default)]
struct AppActors {
    system: ActorSpace<SystemProtocol>,
    groups: ActorSpace<GroupProtocol>,
    devices: ActorSpace<DeviceProtocol>,
    queries: ActorSpace<QueryProtocol>,
}

impl Hosts<DeviceProtocol> for AppActors {
    fn space(&self) -> &ActorSpace<DeviceProtocol> {
        &self.devices
    }
}
```

Every locally hosted protocol has one ordinary `Hosts<P>` implementation of
the same form. These implementations are the static protocol-to-space mapping;
they are not generated, erased, or discovered at runtime. Bombay's generic
`App<Root, Actors>` retains the spaces while giving only the root to the
Behavior Driver.

The hierarchy remains explicit:

```text
Guardian<IoTSystem>                 supplied internally by run
├── DynamicSupervisor<DeviceGroup>  configured by IoTSystem
│   ├── DeviceGroup("kitchen")
│   │   └── DynamicSupervisor<Device>
│   │       ├── Device("thermometer")
│   │       └── Device("oven")
│   └── DeviceGroup("garage")
│       └── DynamicSupervisor<Device>
└── WorkerPool<TemperatureQuery>
    ├── worker
    ├── worker
    └── ...
```

Guardian is the root lifecycle boundary. It is not a supervisor. Supervision
is selected explicitly at the subtree whose failures and membership it owns.

## Behavior authoring

Applications first select and compose existing Behavior Actors templates. The
basic executable example uses `Machine` for state transitions and `OneShot`
for timed termination; application code implements no `Behavior`:

```rust,ignore
let root = OneShot::new(
    Machine::new(state, phase, transition),
    TimerId(1),
    Duration::from_secs(1),
    stop,
);

App::new(root, AppActors::default()).run()
```

Handwritten `Behavior` is the last resort for genuinely application-specific
state machines that the template catalogue cannot express. Such an actor is
deterministic state plus the exact Behavior algebra:

```rust,ignore
struct Device {
    temperature: Option<f64>,
}

enum DeviceMessage {
    Record(f64),
    Read(Recipient<Temperature>),
}

impl Behavior for Device {
    type Protocol = MessageProtocol<ApplicationAddress, DeviceMessage>;
    type Event = User<ApplicationAddress, DeviceMessage>;
    type Sends = DeviceSends;
    type Ph = Never;
    type Error = Never;
    type Birth = NoBirths;

    // init and transition return exact typed Actions.
}
```

Effects are authored with Behavior Actors' named semantic send products and
typed `SendEffects::send`. Heterogeneous children use Behavior's closed
`Children` / `ChildChoice` creation algebra. Application code does not
implement product traversal, runtime interpreters, or Bombay capability
protocols.

`Protocol` deliberately contains only the actor's address and message
signature. `Behavior` contains its executable event and effect algebra. This
separation lets `Recipient` and `Delivery` name a destination without
recursively proving the destination's entire actor tree—for example, a root
that sends to a supervisor whose `MessageAdapter` replies to that same root.

Bombay currently defines no actor-authoring macro and no reduced `Effect`
language. Any future Behavior authoring convenience must be owned and proven
by Behavior, not invented in the runtime façade.

## Runtime boundary

Inside actor protocols, destinations are pure typed `Recipient<P>` values.
`ActorRef<P>` is reserved for external/runtime boundaries that need a live
delivery capability. Neither value exposes receive authority, a mailbox,
runtime task ownership, or cancellation.

The intended ordinary public surface is:

- `App::new(root, actors).run()` and typed `RunError`;
- `ActorSpace<P>` and `Hosts<P>` for static local composition;
- a focused prelude for public Behavior and Behavior Actors vocabulary;
- pure `Recipient<P>` values in actor messages;
- non-owning `ActorRef<P>` values at external boundaries;
- retained terminal facts through `ActorRef::termination`;
- concrete Behavior Actors templates and their policy types;

Users never manually construct or implement:

- a runtime, System, Guardian, Driver, Environment, or Incarnation;
- mailboxes, channels, capacities, address spaces, claims, or leases;
- observation publishers, timer queues, executor tasks, or child-task owners;
- creation, delivery, observation, timer, report, or shutdown interpreters;
- supervision loops, worker scheduling, restart machinery, or shutdown
  traversal;
- `SendEffects`, `SendInput`, creation dispatch, or product-routing
  implementations already supplied generically.

## Local actor spaces

Behavior cannot infer whether every mentioned protocol is locally hosted,
remote, or externally supplied. The named `AppActors` product therefore owns
one exact `ActorSpace<P>` for each locally hosted protocol. `Hosts<P>` selects
that field statically. There is no manifest, namespace, runtime registry,
`TypeId`, `Any`, endpoint enum, HList, or positional witness in the public API.

`InterpretDelivery<P>` remains the broader routing capability: an interpreter
may route locally, remotely, or through a composition of both. `Hosts<P>` says
only that this actor system can install and resolve local incarnations of `P`.

Established destinations use absolute `Recipient<P>` values, while
creator-local delivery uses `ChildRecipient<P>` and cannot escape as a stable
identity. Dynamic-supervisor outcomes now return the established managed-child
recipient, and pool assignments carry their completion recipient. Bombay
interprets both generically; it does not conceal address arithmetic behind a
parallel reference abstraction.

## Acceptance application

Bombay is not complete when it can merely run a counter. The acceptance
application must prove all of these while retaining the tiny `main`:

- recursive heterogeneous creation;
- typed local delivery and rejected-payload recovery;
- dynamic supervised membership and explicit restart policy;
- backoff timers and stale-timer rejection;
- worker-pool scheduling;
- exact-incarnation observation and cleanup;
- orderly recursive shutdown;
- exact terminal failure reporting;
- no runtime plumbing in application code.

Graceful shutdown order is Behavior policy expressed by templates such as
`ShutdownCoordinator` and `TreeShutdown`. Independently of that policy,
Bombay cancels and joins any child still live when its owner terminates, so an
incomplete graceful protocol cannot leak a subtree or deadlock retirement.

Current implementation eligibility and upstream blockers are recorded in
[`open-design-ledger.md`](open-design-ledger.md).
