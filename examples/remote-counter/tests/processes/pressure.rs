//! Actual held-reply pressure across two independent public Nodes and OS processes.
use super::{Program, ReplyProof, counter, deployment, network, protocol, provision};
use bombay::actors::ActorExt;
use bombay::behavior::{BehaviorBase, Step, Stopped, User};
use bombay::prelude::*;
use bombay::remote::{Attempt, Client, ClientLimits, Export, ExportLimits, json, reply};
use bombay::{ActorFailureAssessment, ApplicationScope, RetirementAssessment};
use counter::{Counter, CounterMessage, CounterValue};
use deployment::{Binding, Deployment, Operation, Purpose, Receipt, Role};
use futures_util::future::Either;
use serde::{Deserialize, Serialize};
use std::cell::RefCell;
use std::env;
use std::fmt;
use std::fs::{File, Permissions};
use std::io;
use std::net::TcpListener;
use std::num::NonZeroUsize;
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};
use std::pin::pin;
use std::process::{self, Command, Stdio};
use std::time::{Duration, Instant};
use tempfile::Builder;
use thiserror::Error;
use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};
use tokio::net::unix::{OwnedReadHalf, OwnedWriteHalf};
use tokio::net::{UnixListener, UnixStream};
use tokio::sync::oneshot;

#[path = "../../../../crates/bombay/tests/protected_counter/custody.rs"]
mod custody;

#[derive(Debug, Deserialize, Serialize, PartialEq, Eq)]
enum Control {
    Held {
        response: Box<[u8]>,
        proof: Box<[u8]>,
        value: Option<u64>,
    },
    Unavailable,
    BorrowEnded,
    CompletedBeforeHeld,
    Release,
    Shutdown,
}

#[derive(Error)]
enum Refusal {
    #[error("the actual finite deployment has not been acquired")]
    NotConfigured,
    #[error("the actual counter operation is outside the declared scope")]
    Scope,
    #[error("the actual finite deployment refused its input")]
    Deployment {
        #[source]
        cause: deployment::Refusal,
    },
    #[error("the actual protected response JSON was refused")]
    Json {
        #[source]
        cause: serde_json::Error,
    },
    #[error("the actual counter request encoding was refused")]
    Protocol {
        #[source]
        cause: protocol::Refusal,
    },
    #[error("the held native reply lost its release owner")]
    Release {
        #[source]
        cause: oneshot::error::RecvError,
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

fn limits() -> json::Limits {
    json::Limits {
        input_bytes: NonZeroUsize::new(8192).unwrap(),
        proof_bytes: NonZeroUsize::new(32).unwrap(),
        token_bytes: NonZeroUsize::new(1024).unwrap(),
        nesting: NonZeroUsize::new(8).unwrap(),
        parser_states: NonZeroUsize::new(32).unwrap(),
    }
}

// Concrete bounded test control, never called from a Behavior fold. Keep this
// SAME reader across configuration and subsequent notices: it may prefetch.
async fn read_control(input: &mut BufReader<OwnedReadHalf>) -> io::Result<Control> {
    let mut original = Vec::new();
    input.take(8193).read_until(b'\n', &mut original).await?;
    if original.len() > 8192 || original.last() != Some(&b'\n') {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "one bounded control notice is required",
        ));
    }
    // Explicit fixture disposition: surrender unverified framing after parsing.
    serde_json::from_slice(&original).map_err(io::Error::other)
}

async fn write_control(output: &mut OwnedWriteHalf, control: &Control) -> io::Result<()> {
    let mut original = serde_json::to_vec(control).map_err(io::Error::other)?;
    original.push(b'\n');
    if original.len() > 8192 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "control notice exceeds its finite limit",
        ));
    }
    output.write_all(&original).await?;
    output.flush().await
}

async fn binding(
    listener: &UnixListener,
) -> io::Result<(Binding, BufReader<OwnedReadHalf>, OwnedWriteHalf)> {
    let (stream, _) = listener.accept().await?;
    let (input, output) = stream.into_split();
    let mut input = BufReader::new(input);
    let mut original = Vec::new();
    (&mut input)
        .take(8193)
        .read_until(b'\n', &mut original)
        .await?;
    if original.len() > 8192 || original.last() != Some(&b'\n') {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "one bounded actual binding is required",
        ));
    }
    let actual = serde_json::from_slice(&original).map_err(io::Error::other)?;
    Ok((actual, input, output))
}

struct Burst {
    requests: EstablishedRecipient<protocol::Requests>,
    replies: Vec<User<MailAddr, protocol::Reply>>,
}

#[bombay::actor(sends = {
    requests: Vec<EstablishedDelivery<protocol::Requests>>,
})]
impl Burst {
    #[expect(
        clippy::unnecessary_wraps,
        reason = "the owning Behavior initialization contract returns Result even with an uninhabited error"
    )]
    fn init(&mut self) -> BehaviorActed<Self> {
        Ok(Actions::cont()
            .send_requests(EstablishedDelivery::new(
                self.requests.clone(),
                Operation::Increment,
            ))
            .send_requests(EstablishedDelivery::new(
                self.requests.clone(),
                Operation::Read,
            )))
    }

    fn receive(&mut self, from: MailAddr, reply: protocol::Reply) -> BehaviorActed<Self> {
        self.replies.push(User {
            from,
            message: reply,
        });
        Ok(if self.replies.len() == 2 {
            Actions::stop()
        } else {
            Actions::cont()
        })
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "executed only by the exact-named pressure parent"]
#[expect(
    clippy::too_many_lines,
    reason = "one actual receiver trace retains both original requests and held proof through release and joined native cleanup"
)]
async fn held_receiver() {
    let address =
        env::var("BOMBAY_COUNTER_PRESSURE_ADDRESS").expect("the parent's reserved native endpoint");
    let certificates = PathBuf::from(
        env::var_os("BOMBAY_COUNTER_PRESSURE_CERTIFICATES").expect("actual TLS credentials"),
    );
    let socket = PathBuf::from(
        env::var_os("BOMBAY_COUNTER_PRESSURE_ASSIGNMENT")
            .expect("the parent's private assignment socket"),
    );
    let identity = RefCell::new(None::<Deployment>);
    let (processed, acknowledged) = oneshot::channel();
    let processed = RefCell::new(Some(processed));
    let (release, released) = oneshot::channel();
    let released = RefCell::new(Some(released));
    let (handoff, control) = oneshot::channel();
    let handoff = RefCell::new(Some(handoff));
    let node = Application::new(Counter::new().stop_on_shutdown())
        .open_node(network::configuration(address, &certificates, Role::Serve))
        .await
        .unwrap_or_else(|_| panic!("the actual receiving Node opens"));
    let runtime = node.runtime_id();
    let current = |receipt: &Receipt, actual_runtime: &str, target: MailAddr| {
        let configured = identity.borrow();
        let configured = configured
            .as_ref()
            .ok_or(Refusal::NotConfigured)?
            .metadata(receipt.operation())
            .map_err(|cause| Refusal::Deployment { cause })?;
        if receipt.metadata() == configured
            && configured.runtime_generation.as_ref() == actual_runtime
            && configured.actor_generation == target.0
        {
            Ok(())
        } else {
            Err(Refusal::Scope)
        }
    };
    let maximum = NonZeroUsize::new(2).unwrap();
    let export = Export::new(
        protocol::KEY,
        ExportLimits {
            queued_queries: maximum,
            retained_queries: maximum,
            retained_requests: maximum,
            diagnostics: maximum,
        },
        limits(),
        |bytes: &[u8]| serde_json::from_slice::<protocol::Request>(bytes),
        (
            reply(protocol::increment, protocol::processed),
            reply(protocol::read, protocol::value),
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
                || parsed.kind != Purpose::Reply
                || parsed.operation != receipt.operation()
            {
                return Err(Refusal::Scope);
            }
            let proof = identity
                .borrow()
                .as_ref()
                .ok_or(Refusal::NotConfigured)?
                .protect(Purpose::Reply, bytes)
                .map_err(|cause| Refusal::Deployment { cause })?;
            match parsed.operation {
                Operation::Increment => {
                    let observation = (
                        bytes.to_vec().into_boxed_slice(),
                        proof.as_ptr(),
                        proof.to_vec().into_boxed_slice(),
                    );
                    let publication = processed.borrow_mut().take().ok_or(Refusal::Scope)?;
                    publication.send(observation).unwrap_or_else(|original| {
                        // Fixture setup failure: explicitly dispose this copied observation.
                        drop(original);
                        panic!("the actual Work observation receiver remains owned");
                    });
                    let released = released.borrow_mut().take().ok_or(Refusal::Scope)?;
                    released.await.map_err(|cause| Refusal::Release { cause })?;
                }
                Operation::Read => {}
                Operation::Stop => return Err(Refusal::Scope),
            }
            // The SAME actual allocated proof leaves this producer after release.
            Ok(proof)
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
            .expect("the actual Counter is locally owned");
        let actual = Binding {
            role: Role::Serve,
            runtime,
            actor: address.0,
        };
        let stream = UnixStream::connect(&socket)
            .await
            .expect("the parent accepts this native binding");
        let (input, mut output) = stream.into_split();
        let mut input = BufReader::new(input);
        let configured = deployment::configure(&actual, &mut input, &mut output, &limits()).await?;
        *identity.borrow_mut() = Some(configured);
        handoff
            .borrow_mut()
            .take()
            .expect("one concrete control handoff")
            .send((input, output))
            .unwrap_or_else(|original| {
                drop(original);
                panic!("the receiving Work retains its control receiver");
            });
        Ok::<_, deployment::Refusal>(target)
    });
    let outcome = node
        .run_with(move |application| async move {
            let (mut input, mut output) = control
                .await
                .expect("binding handed off its original control stream");
            let acknowledged = acknowledged.await;
            let mut reader = application
                .client::<CounterValue>()
                .expect("a real independent local Read client");
            let admitted = reader.send(CounterMessage::Read(reader.recipient())).await;
            let before = reader.receive().await;
            let notice = match &acknowledged {
                Ok((response, _, proof)) => Control::Held {
                    response: response.to_vec().into_boxed_slice(),
                    proof: proof.to_vec().into_boxed_slice(),
                    value: before.as_ref().map(|original| original.message),
                },
                Err(_) => Control::Unavailable,
            };
            let notified = write_control(&mut output, &notice).await;
            let release_notice = read_control(&mut input).await;
            // This release is unconditional with respect to the saved observations.
            let released = release.send(());
            let shutdown_notice = read_control(&mut input).await;
            let admitted_after = reader.send(CounterMessage::Read(reader.recipient())).await;
            let after = reader.receive().await;
            let control_closed = output.shutdown().await;
            let shutdown = application.shutdown();
            (
                acknowledged,
                admitted,
                before,
                notice,
                notified,
                release_notice,
                released,
                shutdown_notice,
                admitted_after,
                after,
                control_closed,
                shutdown,
            )
        })
        .await
        .unwrap_or_else(|_| panic!("the actual receiving application executes"));
    let (export, binding, closed, poll, disposal) = node
        .into_parts()
        .unwrap_or_else(|_| panic!("receiver joins native cleanup before extraction"));
    let (work, report, native, notes, ()) = outcome.into_parts();
    let (
        (
            acknowledged,
            admitted,
            before,
            notice,
            notified,
            release_notice,
            released,
            shutdown_notice,
            admitted_after,
            after,
            control_closed,
            shutdown,
        ),
        cleanup,
    ) = work
        .try_into_completed()
        .unwrap_or_else(|_| panic!("the real release and shutdown Work completed"));
    let (origin, native) = native.expect("the actual original receiving Counter");
    let report = report.expect("the independent Counter retirement assessment");
    let notes = notes.expect("both actual Counter notifications");
    let (_inputs, _declarations, declared, withdrawn, diagnostics, rows) = export.into_parts();
    let acknowledged = acknowledged.expect("actual processing/proof reached the held producer");
    let before = before.expect("the independent pre-release Read returns an original User");
    let after = after.expect("the independent terminal Read returns an original User");
    let configured = identity.borrow();
    let configured = configured
        .as_ref()
        .expect("the original finite configuration remains owned");
    // All original provider/native/Work/Node resources are joined before oracles.
    assert!(cleanup.is_ok() && closed.is_ok() && poll.is_none() && disposal.is_none());
    assert!(notes.termination.is_ok() && notes.retirement.is_ok());
    assert!(matches!(binding, Some(Ok(_))));
    assert!(matches!(declared, Some(Ok(()))) && matches!(withdrawn, Some(Ok(()))));
    assert!(diagnostics.is_empty());
    assert_eq!(report.retirement(), RetirementAssessment::Established);
    assert_eq!(report.failures(), ActorFailureAssessment::NoFailuresFound);
    let _counter = custody::completed_actor(origin, native);
    assert!(admitted.is_ok() && admitted_after.is_ok() && shutdown.is_ok());
    assert!(notified.is_ok() && released.is_ok() && control_closed.is_ok());
    assert!(
        matches!(release_notice, Ok(Control::Release))
            && matches!(shutdown_notice, Ok(Control::Shutdown))
    );
    let increment_grant = configured
        .metadata(Operation::Increment)
        .expect("actual Increment grant");
    let address = MailAddr(increment_grant.actor_generation);
    assert_eq!((before.from, before.message), (address, 1));
    assert_eq!((after.from, after.message), (address, 1));
    assert!(matches!(notice, Control::Held { value: Some(1), .. }));
    assert_eq!(rows.len(), 2);
    let mut operations = Vec::new();
    for row in rows {
        let (
            key,
            raw,
            verified,
            decoded,
            native_reply,
            encoding,
            protection,
            publications,
            refusals,
            panics,
        ) = row
            .try_into_parts()
            .unwrap_or_else(|_| panic!("both original request rows surrender joined custody"));
        let verified = verified
            .expect("actual request verification returned")
            .unwrap_or_else(|_| panic!("the exact configured pair verifies"));
        let operation = verified.operation();
        operations.push(operation);
        let grant = configured
            .metadata(operation)
            .expect("the actual row's finite grant");
        let expected_request = protocol::request(grant, operation)
            .unwrap_or_else(|_| panic!("the exact request encodes"));
        let expected_proof = configured
            .protect(Purpose::Request, &expected_request)
            .unwrap_or_else(|_| panic!("the exact request proof is configured"));
        let encoding = encoding
            .expect("actual native reply encoding returned")
            .unwrap_or_else(|_| panic!("the actual reply encodes"));
        let protection = protection
            .expect("actual protection returned")
            .unwrap_or_else(|_| panic!("the full exact reply pair is configured"));
        let expected_reply_proof = configured
            .protect(Purpose::Reply, &encoding)
            .unwrap_or_else(|_| panic!("the actual full reply pair matches"));
        assert!(key.is_some() && decoded.is_none());
        assert_eq!(raw, (expected_request, expected_proof));
        assert_eq!(verified.metadata(), grant);
        assert_eq!(protection.as_ref(), expected_reply_proof.as_ref());
        assert!(matches!(publications.as_slice(), [Ok(())]));
        assert!(matches!(refusals, (None, None, None)));
        assert!(matches!(panics, (None, None, None, None, None, None)));
        match native_reply.expect("the complete actual native reply lane") {
            Either::Left((constructor, client, admission, received, tail)) => {
                let received = received
                    .expect("actual Increment receive")
                    .expect("the actual processed User");
                assert_eq!(operation, Operation::Increment);
                assert!(constructor.is_none() && matches!(client, Attempt::Returned(Ok(_))));
                assert!(matches!(admission, Attempt::Returned(Ok(()))));
                assert_eq!(
                    (received.from, received.message.request_sequence),
                    (address, 101)
                );
                assert!(matches!(tail.as_slice(), [None]));
                assert_eq!(encoding.as_ref(), acknowledged.0.as_ref());
                assert_eq!(protection.as_ptr(), acknowledged.1);
                assert_eq!(protection.as_ref(), acknowledged.2.as_ref());
            }
            Either::Right((constructor, client, admission, received, tail)) => {
                let received = received
                    .expect("actual Read receive")
                    .expect("the actual value User");
                assert_eq!(operation, Operation::Read);
                assert!(constructor.is_none() && matches!(client, Attempt::Returned(Ok(_))));
                assert!(matches!(admission, Attempt::Returned(Ok(()))));
                assert_eq!((received.from, received.message), (address, 1));
                assert!(matches!(tail.as_slice(), [None]));
            }
        }
    }
    operations.sort_unstable();
    assert_eq!(operations, [Operation::Increment, Operation::Read]);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "executed only by the exact-named pressure parent"]
#[expect(
    clippy::too_many_lines,
    reason = "one actual caller retains a pending borrowed Node wait, complete public rows and original actor replies through joined cleanup"
)]
async fn waiting_caller() {
    let address = env::var("BOMBAY_COUNTER_PRESSURE_ADDRESS")
        .expect("the parent's actual listening endpoint");
    let certificates = PathBuf::from(
        env::var_os("BOMBAY_COUNTER_PRESSURE_CERTIFICATES").expect("actual TLS credentials"),
    );
    let socket = PathBuf::from(
        env::var_os("BOMBAY_COUNTER_PRESSURE_ASSIGNMENT").expect("the private assignment socket"),
    );
    let identity = RefCell::new(None::<Deployment>);
    let (handoff, control) = oneshot::channel();
    let handoff = RefCell::new(Some(handoff));
    let scope = ApplicationScope::new();
    let requests = scope
        .external::<protocol::Requests>()
        .expect("the actual outgoing Source");
    let source_address = requests.address();
    let actor = Burst {
        requests: requests.recipient(),
        replies: Vec::new(),
    }
    .with_deadline(Some(Instant::now() + Duration::from_secs(10)), |_| {
        Step::Stop(Stopped)
    });
    let node = scope
        .with_root(actor)
        .open_node(network::configuration(address, &certificates, Role::Call))
        .await
        .unwrap_or_else(|_| panic!("the actual calling Node opens"));
    let runtime = node.runtime_id();
    let client = Client::new(
        requests,
        protocol::KEY,
        ClientLimits {
            concurrent_requests: NonZeroUsize::new(1).unwrap(),
            retained_requests: NonZeroUsize::new(2).unwrap(),
            retained_responses: NonZeroUsize::new(1).unwrap(),
        },
        limits(),
        Duration::from_secs(20),
        |original: &User<MailAddr, Operation>, _: &str, _: MailAddr| {
            let configured = identity.borrow();
            let configured = configured.as_ref().ok_or(Refusal::NotConfigured)?;
            let metadata = configured
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
        |original: &User<MailAddr, Operation>, actual_runtime: &str, target: MailAddr| {
            let configured = identity.borrow();
            let configured = configured.as_ref().ok_or(Refusal::NotConfigured)?;
            if original.from == target
                && configured.caller().runtime == actual_runtime
                && configured.caller().actor == target.0
            {
                Ok(())
            } else {
                Err(Refusal::Scope)
            }
        },
        |receipt: &Receipt,
         original: &User<MailAddr, Operation>,
         actual_runtime: &str,
         target: MailAddr| {
            let configured = identity.borrow();
            let configured = configured.as_ref().ok_or(Refusal::NotConfigured)?;
            let metadata = configured
                .metadata(receipt.operation())
                .map_err(|cause| Refusal::Deployment { cause })?;
            if original.from == target
                && configured.caller().runtime == actual_runtime
                && configured.caller().actor == target.0
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
            .expect("the actual Burst is locally owned");
        let actual = Binding {
            role: Role::Call,
            runtime,
            actor: address.0,
        };
        let stream = UnixStream::connect(&socket)
            .await
            .expect("the parent receives this actual binding");
        let (input, mut output) = stream.into_split();
        let mut input = BufReader::new(input);
        let configured = deployment::configure(&actual, &mut input, &mut output, &limits()).await?;
        *identity.borrow_mut() = Some(configured);
        handoff
            .borrow_mut()
            .take()
            .expect("one original control handoff")
            .send((input, output))
            .unwrap_or_else(|original| {
                drop(original);
                panic!("the calling test retains its control receiver");
            });
        Ok::<_, deployment::Refusal>(target)
    });
    let (outcome, selected) = {
        // Keep BOTH original futures outside select's disposable borrowed waits.
        let mut running = pin!(node.run());
        let mut notice = pin!(async {
            let (mut input, output) = control
                .await
                .expect("binding handed off its SAME buffered stream");
            let notice = read_control(&mut input).await;
            (input, output, notice)
        });
        let selected = tokio::select! {
            biased;
            actual = running.as_mut() => Either::Left(actual),
            actual = notice.as_mut() => Either::Right(actual),
        };
        match selected {
            Either::Left(outcome) => {
                // Unexpected early completion remains owned, and is never repolled.
                // Still tell the parent to release the actual held receiver.
                let (_input, mut output, held) = notice.await;
                let notified = write_control(&mut output, &Control::CompletedBeforeHeld).await;
                let control_closed = output.shutdown().await;
                (outcome, Either::Left((held, notified, control_closed)))
            }
            Either::Right((_input, mut output, held)) => {
                // Biased select polled the Node branch Pending before this actual
                // held notice won. That borrowed wait has now been cancelled;
                // the SAME pinned original Node future is still owned above.
                let notified = write_control(&mut output, &Control::BorrowEnded).await;
                let outcome = running.await;
                let control_closed = output.shutdown().await;
                (outcome, Either::Right((held, notified, control_closed)))
            }
        }
    };
    let outcome = outcome.unwrap_or_else(|_| panic!("the actual no-Work caller executes"));
    let (client, bound, closed, poll, disposal) = node
        .into_parts()
        .unwrap_or_else(|_| panic!("caller joins every native owner before extraction"));
    let (work, report, native, notes, ()) = outcome.into_parts();
    let (_absence, cleanup) = work
        .try_into_completed()
        .unwrap_or_else(|_| panic!("the actual caller has genuine Work absence"));
    let report = report.expect("the independent caller retirement assessment");
    let notes = notes.expect("both actual caller notifications");
    let (origin, native) = native.expect("the actual original Burst actor");
    let (mut source, _inputs, startup, withdrawal, joined, source_eof, rows, routes) =
        client.into_parts();
    let mut unread = Vec::new();
    let mut acquired_eof = None;
    if source_eof.is_none() {
        while let Some(original) = source.receive().await {
            unread.push(original);
        }
        acquired_eof = Some(());
    }
    let configured = identity.borrow();
    let configured = configured
        .as_ref()
        .expect("the complete original finite deployment remains owned");
    // No semantic assertion happened before A's release.
    // These are the whole original Node/native/actor/Source owners, not a report callback.
    assert!(cleanup.is_ok() && closed.is_ok() && poll.is_none() && disposal.is_none());
    assert!(notes.termination.is_ok() && notes.retirement.is_ok());
    assert!(matches!(bound, Some(Ok(_))));
    assert_eq!(report.retirement(), RetirementAssessment::Established);
    assert_eq!(report.failures(), ActorFailureAssessment::NoFailuresFound);
    let retired = custody::completed_actor(origin, native);
    let burst = retired.base();
    let actual = burst
        .replies
        .iter()
        .map(|original| {
            (
                original.from,
                original.message.operation,
                original.message.sequence,
                &original.message.value,
            )
        })
        .collect::<Vec<_>>();
    let expected = [
        (
            source_address,
            Operation::Increment,
            101,
            &protocol::Value::Processed(101),
        ),
        (
            source_address,
            Operation::Read,
            102,
            &protocol::Value::Read(1),
        ),
    ];
    // Primary end-to-end delivery oracle; the retained-message/no-native-send
    // inverse reaches this AFTER actual deadline retirement and all cleanup.
    assert_eq!(actual.as_slice(), expected);
    match selected {
        Either::Right((
            Ok(Control::Held {
                response,
                proof,
                value,
            }),
            notified,
            control_closed,
        )) => {
            let receipt = configured
                .verify(Purpose::Reply, &response, &proof)
                .unwrap_or_else(|_| panic!("the actual held pair verifies independently"));
            let actual: protocol::Response =
                serde_json::from_slice(&response).expect("the actual held response parses");
            assert!(notified.is_ok() && control_closed.is_ok());
            assert_eq!(value, Some(1));
            assert_eq!(actual.operation, Operation::Increment);
            assert_eq!(actual.bombay, *receipt.metadata());
            assert_eq!(actual.value, protocol::Value::Processed(101));
        }
        Either::Right(_) => {
            panic!("the borrowed Node wait selected its actual held-response notice")
        }
        Either::Left(_) => panic!("the actual Node must remain Pending while the reply is held"),
    }
    assert!(matches!(startup, Some(Ok(()))) && matches!(withdrawal, Some(Ok(()))));
    assert!(matches!(routes, (Some(Ok(())), Some(Ok(())), Some(Ok(())))));
    assert!(matches!(joined, Some(Ok(()))));
    assert!(source_eof.is_some() || acquired_eof.is_some());
    assert_eq!(unread, []);
    assert_eq!(rows.len(), 2);
    let mut operations = Vec::new();
    for row in rows {
        let (
            original,
            refusal,
            encoded,
            protected,
            permission,
            capacity,
            query,
            callback,
            replies,
            eof,
            protection_panics,
            operation_panics,
            _completed,
        ) = row;
        let operation = original.message;
        operations.push(operation);
        let grant = configured
            .metadata(operation)
            .expect("the original operation's configured metadata");
        let expected_request = protocol::request(grant, operation)
            .unwrap_or_else(|_| panic!("the configured original request encodes"));
        let expected_proof = configured
            .protect(Purpose::Request, &expected_request)
            .unwrap_or_else(|_| panic!("the exact original request proof is configured"));
        let encoded = encoded
            .expect("actual encoding returned")
            .unwrap_or_else(|_| panic!("the actual request encoding succeeded"))
            .expect("these two original Actions select actual Gets");
        let protected = protected
            .expect("actual protection returned")
            .unwrap_or_else(|_| panic!("the actual full pair matched"));
        assert_eq!(original.from, MailAddr(configured.caller().actor));
        assert_eq!(encoded.as_ref(), expected_request.as_ref());
        assert_eq!(protected.as_ref(), expected_proof.as_ref());
        assert!(
            refusal.is_none() && permission.is_none() && capacity.is_none() && callback.is_none()
        );
        assert!(matches!(query, Attempt::Returned(Ok(()))) && eof.is_some());
        assert!(
            matches!(protection_panics, (None, None, None))
                && matches!(operation_panics, (None, None, None))
        );
        assert_eq!(replies.len(), 1);
        for response in replies {
            let payload = response.payload().flatten().copied().collect::<Vec<_>>();
            let proof = response
                .proof()
                .expect("actual native proof attachment")
                .flatten()
                .copied()
                .collect::<Vec<_>>();
            let (cause, wire, shape, verified, decoded, permission, panics) = response.into_parts();
            let wire = wire.expect("the actual processed protected pair remains owned");
            let receipt = verified
                .expect("actual response verification returned")
                .unwrap_or_else(|_| panic!("the exact finite response pair verifies"));
            let independently_verified = configured
                .verify(Purpose::Reply, &payload, &proof)
                .unwrap_or_else(|_| {
                    panic!("the borrowed actual native bytes match the configured pair")
                });
            assert_eq!(wire.0.as_ref(), payload.as_slice());
            assert_eq!(wire.1.as_ref(), proof.as_slice());
            assert_eq!(receipt.operation(), operation);
            assert_eq!(receipt.metadata(), grant);
            assert_eq!(independently_verified.metadata(), receipt.metadata());
            assert!(cause.is_none() && permission.is_none() && matches!(shape, Some(Ok(()))));
            assert!(matches!(
                decoded,
                Some(Ok(Either::Right(Attempt::Returned(Ok(())))))
            ));
            assert!(matches!(panics, (None, None, None)));
        }
    }
    operations.sort_unstable();
    assert_eq!(operations, [Operation::Increment, Operation::Read]);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[expect(
    clippy::too_many_lines,
    reason = "the complete separate-process pressure trace retains both actual bindings, finite controls and child reaps before its oracles"
)]
async fn queued_requests_resume_after_held_reply_and_borrowed_wait() {
    let directory = Builder::new()
        .prefix("counter-")
        .permissions(Permissions::from_mode(0o700))
        .tempdir()
        .expect("this trace owns a private bounded assignment directory");
    let socket = directory.path().join("p.sock");
    let certificates = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../../docs/prds/static-remote-actors/transport-custody/certificates");
    let trace = tokio::time::timeout(Duration::from_secs(40), async {
        let listener = UnixListener::bind(&socket)?;
        let port = {
            let reservation = TcpListener::bind(("localhost", 0))?;
            reservation.local_addr()?.port()
        };
        let address = format!("localhost:{port}");
        let mut children = Vec::new();
        for (leaf, role) in [
            ("pressure::held_receiver", Role::Serve),
            ("pressure::waiting_caller", Role::Call),
        ] {
            let name = match role {
                Role::Serve => "pressure-receiver",
                Role::Call => "pressure-caller",
            };
            let output = directory.path().join(format!("{name}.stdout"));
            let diagnostics = directory.path().join(format!("{name}.stderr"));
            let child = Command::new(env::current_exe()?)
                .args(["--ignored", "--exact", leaf])
                .env("BOMBAY_COUNTER_PRESSURE_ADDRESS", &address)
                .env("BOMBAY_COUNTER_PRESSURE_CERTIFICATES", &certificates)
                .env("BOMBAY_COUNTER_PRESSURE_ASSIGNMENT", &socket)
                .stdin(Stdio::null())
                .stdout(File::create(&output)?)
                .stderr(File::create(&diagnostics)?)
                .spawn()?;
            let program = Program {
                child,
                output,
                diagnostics,
            };
            let actual = binding(&listener).await?;
            children.push((program, actual));
        }
        let [
            (mut receiver, (receiver_binding, mut receiver_control, mut receiver_input)),
            (mut caller, (caller_binding, mut caller_control, mut caller_input)),
        ]: [_; 2] = children
            .try_into()
            .unwrap_or_else(|_| panic!("exactly two actual child owners were constructed"));
        let receiver_pid = receiver.child.id();
        let caller_pid = caller.child.id();
        let configuration = provision(&caller_binding, &receiver_binding, ReplyProof::Matching)?;
        receiver_input.write_all(&configuration).await?;
        receiver_input.flush().await?;
        caller_input.write_all(&configuration).await?;
        caller_input.flush().await?;
        let held = read_control(&mut receiver_control).await;
        let forwarded = match &held {
            Ok(held) => write_control(&mut caller_input, held).await,
            Err(_) => write_control(&mut caller_input, &Control::Unavailable).await,
        };
        let borrowed = read_control(&mut caller_control).await;
        // Parent release never depends on a law assertion or predicted delay.
        let released = write_control(&mut receiver_input, &Control::Release).await;
        let caller_control_closed = caller_input.shutdown().await;
        let caller_output = caller.finish().await?;
        let shutdown = write_control(&mut receiver_input, &Control::Shutdown).await;
        let receiver_control_closed = receiver_input.shutdown().await;
        let receiver_output = receiver.finish().await?;
        Ok::<_, io::Error>((
            receiver_pid,
            caller_pid,
            receiver_binding,
            caller_binding,
            held,
            forwarded,
            borrowed,
            released,
            shutdown,
            caller_control_closed,
            receiver_control_closed,
            receiver_output,
            caller_output,
        ))
    })
    .await;
    // Timeout/error cancellation also invokes the existing kill-AND-reap guards.
    let removed = directory.close();
    let (
        receiver_pid,
        caller_pid,
        receiver_binding,
        caller_binding,
        held,
        forwarded,
        borrowed,
        released,
        shutdown,
        caller_control_closed,
        receiver_control_closed,
        receiver_output,
        caller_output,
    ) = trace
        .expect("the complete process/control/native trace remains finite")
        .expect("both actual native programs finish");
    removed.expect("both children have been reaped before private path removal");
    let receiver_stdout =
        String::from_utf8(receiver_output.stdout).expect("bounded child output is UTF-8");
    let caller_stdout =
        String::from_utf8(caller_output.stdout).expect("bounded child output is UTF-8");
    let parent_pid = process::id();
    assert_ne!(receiver_pid, caller_pid);
    assert_ne!(receiver_pid, parent_pid);
    assert_ne!(caller_pid, parent_pid);
    assert_eq!(receiver_binding.role, Role::Serve);
    assert_eq!(caller_binding.role, Role::Call);
    assert!(!receiver_binding.runtime.is_empty() && !caller_binding.runtime.is_empty());
    assert_ne!(receiver_binding.runtime, caller_binding.runtime);
    assert!(matches!(held, Ok(Control::Held { value: Some(1), .. })));
    assert!(forwarded.is_ok() && released.is_ok() && shutdown.is_ok());
    assert!(matches!(borrowed, Ok(Control::BorrowEnded)));
    assert!(caller_control_closed.is_ok() && receiver_control_closed.is_ok());
    assert!(
        receiver_output.status.success(),
        "receiver stdout: {receiver_stdout}\nreceiver stderr: {:?}",
        receiver_output.stderr
    );
    assert!(
        caller_output.status.success(),
        "caller stdout: {caller_stdout}\ncaller stderr: {:?}",
        caller_output.stderr
    );
    let receiver_name = receiver_stdout
        .matches("test pressure::held_receiver ... ok")
        .count();
    let caller_name = caller_stdout
        .matches("test pressure::waiting_caller ... ok")
        .count();
    assert_eq!(receiver_name, 1);
    assert_eq!(caller_name, 1);
    assert!(receiver_stdout.contains("test result: ok. 1 passed; 0 failed; 0 ignored;"));
    assert!(caller_stdout.contains("test result: ok. 1 passed; 0 failed; 0 ignored;"));
}
