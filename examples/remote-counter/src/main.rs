//! One receiving Counter or one ordinary outgoing Counter customer.
use bombay::actors::ActorExt;
use bombay::behavior::User;
use bombay::prelude::*;
use bombay::remote::{
    Client, ClientLimits, Export, ExportLimits, NodeExecutionRefusal, NodeStartupRefusal,
    RefusalRetention, json, reply, stop,
};
use bombay::{
    ActorFailureAssessment, ActorRetirementReport, ApplicationScope, RetirementAssessment,
};
use std::any::Any;
use std::cell::RefCell;
use std::env;
use std::error::Error as StdError;
use std::ffi::OsString;
use std::fmt;
use std::num::{NonZeroU64, NonZeroUsize, ParseIntError};
use std::path::{Path, PathBuf};
use std::process::ExitCode;
use std::time::Duration;
use thiserror::Error;
use tokio::io::BufReader;
use tokio::net::UnixStream;
use tokio::runtime::TryCurrentError;

#[path = "../../counter/src/counter.rs"]
#[expect(
    dead_code,
    reason = "the remote application uses the shared counter's correlated increment rather than its local one-way increment"
)]
mod counter;
mod deployment;
mod network;
mod protocol;

use counter::Counter;
use deployment::{Binding, Deployment, Operation, Purpose, Receipt, Role};
use protocol::{Caller, Journey, Requests, Results};

// These are application dispositions, not new runtime or delivery outcomes.
enum Outcome {
    Stopped,
    Complete(u64),
    Uncertain(Operation),
}

enum Mode {
    Test,
    ProductionRequired,
}

#[derive(Error)]
enum Refusal {
    #[error(
        "usage: remote-counter serve MODE HOST:PORT CERTIFICATES ASSIGNMENT_SOCKET | remote-counter call MODE HOST:PORT CERTIFICATES ASSIGNMENT_SOCKET WAIT_SECONDS"
    )]
    Arguments,
    #[error("this finite test provider cannot satisfy a production deployment")]
    ProductionProvider,
    #[error("a command-line value is not supported")]
    Argument { original: OsString },
    #[error("the wait duration must be a positive integer")]
    Duration {
        #[source]
        cause: ParseIntError,
    },
    #[error("counter assignment IO failed")]
    Io {
        #[source]
        cause: std::io::Error,
    },
    #[error("counter deployment was refused")]
    Deployment {
        #[source]
        cause: deployment::Refusal,
    },
    #[error("counter JSON was refused")]
    Json {
        #[source]
        cause: serde_json::Error,
    },
    #[error("counter protocol was refused")]
    Protocol {
        #[source]
        cause: protocol::Refusal,
    },
    #[error("counter application endpoint was refused")]
    Endpoint {
        #[source]
        cause: ExternalActorError,
    },
    #[error("counter target is not owned by this application")]
    Target {
        #[source]
        cause: bombay::remote::ExportError,
    },
    #[error("counter deployment is not configured")]
    NotConfigured,
    #[error("counter authority scope differs")]
    Scope,
    #[error("counter Node could not open")]
    Startup {
        #[source]
        cause: NodeStartupRefusal,
    },
    #[error("counter execution has already been issued")]
    ExecutionIssued,
    #[error("counter Session was already closed")]
    SessionClosed,
    #[error("counter execution host is unavailable")]
    Host {
        #[source]
        cause: TryCurrentError,
    },
    #[error("counter joined execution did not return its inputs")]
    Handoff,
    #[error("counter binding result is absent")]
    Binding,
    #[error("counter native close failed")]
    Close {
        #[source]
        cause: Box<dyn StdError + Send + Sync>,
    },
    #[error("counter execution polling panicked")]
    Poll { original: Box<dyn Any + Send> },
    #[error("counter execution disposal panicked")]
    Disposal { original: Box<dyn Any + Send> },
    #[error("counter cleanup did not establish successful retirement")]
    Retirement {
        report: Option<ActorRetirementReport>,
    },
    #[error("counter customer did not return a domain result")]
    Result,
    #[error("counter customer refused an unexpected reply")]
    Reply {
        #[source]
        cause: protocol::Unexpected,
    },
}

impl fmt::Debug for Refusal {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_tuple("Refusal")
            .field(&format_args!("{self}"))
            .finish()
    }
}

struct Configuration {
    address: String,
    certificates: PathBuf,
    assignment: PathBuf,
}

enum Command {
    Serve(Configuration),
    Call {
        configuration: Configuration,
        wait: Duration,
    },
}

impl Command {
    fn read() -> Result<Self, Refusal> {
        let mut arguments = env::args_os().skip(1);
        let role = arguments.next().ok_or(Refusal::Arguments)?;
        let role = match role.to_str() {
            Some("serve") => Role::Serve,
            Some("call") => Role::Call,
            _ => return Err(Refusal::Argument { original: role }),
        };
        let mut arguments = arguments.collect::<Vec<_>>().into_iter();
        match (role, arguments.len()) {
            (Role::Serve, 4) | (Role::Call, 5) => {}
            _ => return Err(Refusal::Arguments),
        }
        let mode = arguments.next().ok_or(Refusal::Arguments)?;
        let mode = match mode.to_str() {
            Some("test") => Mode::Test,
            Some("production-required") => Mode::ProductionRequired,
            _ => return Err(Refusal::Argument { original: mode }),
        };
        match mode {
            Mode::Test => {}
            Mode::ProductionRequired => return Err(Refusal::ProductionProvider),
        }
        let address = arguments
            .next()
            .ok_or(Refusal::Arguments)?
            .into_string()
            .map_err(|original| Refusal::Argument { original })?;
        let configuration = Configuration {
            address,
            certificates: arguments.next().ok_or(Refusal::Arguments)?.into(),
            assignment: arguments.next().ok_or(Refusal::Arguments)?.into(),
        };
        match role {
            Role::Serve => Ok(Self::Serve(configuration)),
            Role::Call => {
                let wait = arguments
                    .next()
                    .ok_or(Refusal::Arguments)?
                    .into_string()
                    .map_err(|original| Refusal::Argument { original })?
                    .parse::<NonZeroU64>()
                    .map_err(|cause| Refusal::Duration { cause })?;
                Ok(Self::Call {
                    configuration,
                    wait: Duration::from_secs(wait.get()),
                })
            }
        }
    }
}

fn limits() -> json::Limits {
    json::Limits {
        input_bytes: NonZeroUsize::new(8192).unwrap(),
        proof_bytes: NonZeroUsize::new(32).unwrap(),
        token_bytes: NonZeroUsize::new(1024).unwrap(),
        nesting: NonZeroUsize::new(8).unwrap(),
        parser_states: NonZeroUsize::new(32).unwrap(),
    }
}

async fn assignment(socket: &Path, actual: &Binding) -> Result<Deployment, Refusal> {
    let stream = UnixStream::connect(socket)
        .await
        .map_err(|cause| Refusal::Io { cause })?;
    let (input, output) = stream.into_split();
    deployment::configure(actual, BufReader::new(input), output, &limits())
        .await
        .map_err(|cause| Refusal::Deployment { cause })
}

#[tokio::main(flavor = "multi_thread", worker_threads = 2)]
async fn main() -> ExitCode {
    match execute().await {
        Ok(Outcome::Stopped) => {
            println!("counter stopped");
            ExitCode::SUCCESS
        }
        Ok(Outcome::Complete(value)) => {
            println!("counter value: {value}");
            println!("counter stopped");
            ExitCode::SUCCESS
        }
        Ok(Outcome::Uncertain(operation)) => {
            println!("counter uncertain: {operation:?}");
            ExitCode::from(2)
        }
        Err(cause) => {
            eprintln!("{cause}");
            ExitCode::FAILURE
        }
    }
}

async fn execute() -> Result<Outcome, Refusal> {
    match Command::read()? {
        Command::Serve(configuration) => serve(configuration).await,
        Command::Call {
            configuration,
            wait,
        } => call(configuration, wait).await,
    }
}

#[expect(
    clippy::too_many_lines,
    reason = "one receiving application composition preserves declaration, execution and joined cleanup together"
)]
async fn serve(configuration: Configuration) -> Result<Outcome, Refusal> {
    let identity = RefCell::new(None::<Deployment>);
    let network = network::configuration(
        configuration.address,
        &configuration.certificates,
        Role::Serve,
    );
    let node = Application::new(Counter::new().stop_on_shutdown())
        .open_node(network)
        .await
        .map_err(|(_application, _network, cause)| Refusal::Startup { cause })?;
    let runtime = node.runtime_id();
    let current = |receipt: &Receipt, runtime: &str, target: MailAddr| {
        let identity = identity.borrow();
        let identity = identity.as_ref().ok_or(Refusal::NotConfigured)?;
        let configured = identity
            .metadata(receipt.operation())
            .map_err(|cause| Refusal::Deployment { cause })?;
        if receipt.metadata() == configured
            && configured.runtime_generation.as_ref() == runtime
            && configured.actor_generation == target.0
        {
            Ok(())
        } else {
            Err(Refusal::Scope)
        }
    };
    let maximum = NonZeroUsize::new(3).unwrap();
    let export = Export::new(
        protocol::KEY,
        ExportLimits {
            refusals: RefusalRetention::Discharge,
            queued_queries: maximum,
            retained_queries: maximum,
            retained_requests: maximum,
            diagnostics: maximum,
        },
        limits(),
        |bytes: &[u8]| serde_json::from_slice::<protocol::Request>(bytes),
        (
            reply(protocol::increment, protocol::processed),
            (
                reply(protocol::read, protocol::value),
                stop(protocol::stop, protocol::stopped),
            ),
        ),
        async |bytes: &[u8], proof: &[u8]| {
            identity
                .borrow()
                .as_ref()
                .ok_or(Refusal::NotConfigured)?
                .verify(Purpose::Request, bytes, proof)
                .map_err(|cause| Refusal::Deployment { cause })
        },
        async |bytes: &[u8], receipt: &Receipt| {
            let parsed: protocol::Response =
                serde_json::from_slice(bytes).map_err(|cause| Refusal::Json { cause })?;
            if parsed.bombay != *receipt.metadata()
                || parsed.operation != receipt.operation()
                || parsed.kind != Purpose::Reply
            {
                return Err(Refusal::Scope);
            }
            identity
                .borrow()
                .as_ref()
                .ok_or(Refusal::NotConfigured)?
                .protect(Purpose::Reply, bytes)
                .map_err(|cause| Refusal::Deployment { cause })
        },
        |left: &Receipt, right: &Receipt| {
            left.operation() == right.operation() && left.metadata() == right.metadata()
        },
        &current,
        &current,
    );
    let mut node = node.exports(export, async |application| {
        let target = application.root().established_recipient();
        let address = application
            .actor_address(&target)
            .map_err(|cause| Refusal::Target { cause })?;
        let actual = Binding {
            role: Role::Serve,
            runtime,
            actor: address.0,
        };
        *identity.borrow_mut() = Some(assignment(&configuration.assignment, &actual).await?);
        Ok::<_, Refusal>(target)
    });
    let outcome = node.run().await.map_err(|cause| match cause {
        NodeExecutionRefusal::ExecutionIssued { .. } => Refusal::ExecutionIssued,
        NodeExecutionRefusal::SessionClosed { .. } => Refusal::SessionClosed,
        NodeExecutionRefusal::Host { cause, .. } => Refusal::Host { cause },
    })?;
    let (_export, binding, closed, poll, disposal) =
        node.into_parts().map_err(|_| Refusal::Handoff)?;
    binding.ok_or(Refusal::Binding)??;
    closed.map_err(|cause| Refusal::Close { cause })?;
    if let Some(original) = poll {
        return Err(Refusal::Poll { original });
    }
    if let Some(original) = disposal {
        return Err(Refusal::Disposal { original });
    }
    let report = outcome.root_report();
    if !outcome.cleanup().is_some_and(Result::is_ok)
        || !matches!(report, Some(report) if report.retirement() == RetirementAssessment::Established && report.failures() == ActorFailureAssessment::NoFailuresFound)
    {
        return Err(Refusal::Retirement { report });
    }
    Ok(Outcome::Stopped)
}

#[expect(
    clippy::too_many_lines,
    reason = "one outgoing application composition preserves the domain result and independent joined cleanup facts"
)]
async fn call(configuration: Configuration, wait: Duration) -> Result<Outcome, Refusal> {
    let identity = RefCell::new(None::<Deployment>);
    let scope = ApplicationScope::new();
    let requests = scope
        .external::<Requests>()
        .map_err(|cause| Refusal::Endpoint { cause })?;
    let mut results = scope
        .external::<Results>()
        .map_err(|cause| Refusal::Endpoint { cause })?;
    let caller = Caller {
        service: requests.recipient(),
        results: results.recipient(),
        journey: Journey::Increment,
    }
    .with_receive_timeout(wait, Caller::expire);
    let network = network::configuration(
        configuration.address,
        &configuration.certificates,
        Role::Call,
    );
    let node = scope
        .with_root(caller)
        .open_node(network)
        .await
        .map_err(|(_application, _network, cause)| Refusal::Startup { cause })?;
    let runtime = node.runtime_id();
    let client = Client::new(
        requests,
        protocol::KEY,
        ClientLimits {
            concurrent_requests: NonZeroUsize::new(1).unwrap(),
            retained_requests: NonZeroUsize::new(4).unwrap(),
            retained_responses: NonZeroUsize::new(1).unwrap(),
        },
        limits(),
        wait,
        |original: &User<MailAddr, Operation>, _: &str, _: MailAddr| {
            let identity = identity.borrow();
            let identity = identity.as_ref().ok_or(Refusal::NotConfigured)?;
            let metadata = identity
                .metadata(original.message)
                .map_err(|cause| Refusal::Deployment { cause })?;
            protocol::request(metadata, original.message)
                .map(Some)
                .map_err(|cause| Refusal::Protocol { cause })
        },
        async |bytes: &[u8], _: &User<MailAddr, Operation>| {
            identity
                .borrow()
                .as_ref()
                .ok_or(Refusal::NotConfigured)?
                .protect(Purpose::Request, bytes)
                .map_err(|cause| Refusal::Deployment { cause })
        },
        async |bytes: &[u8], proof: &[u8]| {
            identity
                .borrow()
                .as_ref()
                .ok_or(Refusal::NotConfigured)?
                .verify(Purpose::Reply, bytes, proof)
                .map_err(|cause| Refusal::Deployment { cause })
        },
        |receipt: &Receipt, bytes: &[u8], original: &User<MailAddr, Operation>| {
            let parsed: protocol::Response =
                serde_json::from_slice(bytes).map_err(|cause| Refusal::Json { cause })?;
            if parsed.bombay != *receipt.metadata()
                || parsed.kind != Purpose::Reply
                || parsed.operation != receipt.operation()
                || original.message != parsed.operation
            {
                return Err(Refusal::Scope);
            }
            Ok(protocol::Reply {
                operation: parsed.operation,
                sequence: parsed.bombay.request_sequence,
                value: parsed.value,
            })
        },
        |original: &User<MailAddr, Operation>, runtime: &str, target: MailAddr| {
            let identity = identity.borrow();
            let identity = identity.as_ref().ok_or(Refusal::NotConfigured)?;
            if original.from == target
                && identity.caller().runtime == runtime
                && identity.caller().actor == target.0
            {
                Ok(())
            } else {
                Err(Refusal::Scope)
            }
        },
        |receipt: &Receipt,
         original: &User<MailAddr, Operation>,
         runtime: &str,
         target: MailAddr| {
            let identity = identity.borrow();
            let identity = identity.as_ref().ok_or(Refusal::NotConfigured)?;
            let metadata = identity
                .metadata(receipt.operation())
                .map_err(|cause| Refusal::Deployment { cause })?;
            if original.from == target
                && identity.caller().runtime == runtime
                && identity.caller().actor == target.0
                && receipt.metadata() == metadata
                && original.message == receipt.operation()
            {
                Ok(())
            } else {
                Err(Refusal::Scope)
            }
        },
    );
    let mut node = node.exports(client, async |application| {
        let target = application.root().established_recipient();
        let address = application
            .actor_address(&target)
            .map_err(|cause| Refusal::Target { cause })?;
        let actual = Binding {
            role: Role::Call,
            runtime,
            actor: address.0,
        };
        *identity.borrow_mut() = Some(assignment(&configuration.assignment, &actual).await?);
        Ok::<_, Refusal>(target)
    });
    // The only Work is useful domain output. Node owns joined cleanup; no
    // explicit drop, notification bookkeeping or extra retirement barrier.
    let outcome = node
        .run_with(move |_| async move { results.receive().await.map(|result| result.message) })
        .await
        .map_err(|cause| match cause {
            NodeExecutionRefusal::ExecutionIssued { .. } => Refusal::ExecutionIssued,
            NodeExecutionRefusal::SessionClosed { .. } => Refusal::SessionClosed,
            NodeExecutionRefusal::Host { cause, .. } => Refusal::Host { cause },
        })?;
    let (_client, binding, closed, poll, disposal) =
        node.into_parts().map_err(|_| Refusal::Handoff)?;
    binding.ok_or(Refusal::Binding)??;
    closed.map_err(|cause| Refusal::Close { cause })?;
    if let Some(original) = poll {
        return Err(Refusal::Poll { original });
    }
    if let Some(original) = disposal {
        return Err(Refusal::Disposal { original });
    }
    let report = outcome.root_report();
    if !outcome.cleanup().is_some_and(Result::is_ok)
        || !matches!(report, Some(report) if report.retirement() == RetirementAssessment::Established && report.failures() == ActorFailureAssessment::NoFailuresFound)
    {
        return Err(Refusal::Retirement { report });
    }
    match outcome
        .try_into_completed()
        .map_err(|_| Refusal::Result)?
        .0
        .ok_or(Refusal::Result)?
    {
        protocol::Outcome::Complete(value) => Ok(Outcome::Complete(value)),
        protocol::Outcome::Uncertain(operation) => Ok(Outcome::Uncertain(operation)),
        protocol::Outcome::Failed(cause) => Err(Refusal::Reply { cause }),
    }
}
