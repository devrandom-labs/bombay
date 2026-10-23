//! Proposed child of tests/processes.rs; fault custody belongs only to this test.
use super::{
    Mode, Program, ProgramRole, ReplyProof, assignment, counter, deployment, protocol, provision,
};
use bombay::actors::ActorExt;
use bombay::prelude::*;
use bombay::remote::{Attempt, Export, ExportLimits, json, reply, stop};
use bombay::{ActorFailureAssessment, RetirementAssessment};
use counter::{Counter, CounterMessage, CounterValue};
use deployment::{Binding, Deployment, Operation, Purpose, Receipt, Role};
use futures_util::future::Either;
use std::cell::RefCell;
use std::env;
use std::fs::{File, Permissions};
use std::future::pending;
use std::io;
use std::net::TcpListener;
use std::num::NonZeroUsize;
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};
use std::process::{self, Command, Stdio};
use std::time::Duration;
use tempfile::Builder;
use thiserror::Error;
use tokio::io::{AsyncReadExt, AsyncWriteExt, BufReader};
use tokio::net::{UnixListener, UnixStream};
use tokio::sync::oneshot;

// Relative to the proposed tests/processes/lost_reply.rs destination. This is
// the application's exact Network input construction, not another transport.
#[path = "../../src/network.rs"]
mod network;

#[derive(Debug, Error)]
enum Refusal {
    #[error("the held reply is outside the configured operation")]
    Scope,
    #[error("the actual reply JSON was refused")]
    Json {
        #[source]
        cause: serde_json::Error,
    },
    #[error("the exact finite pair was refused")]
    Deployment {
        #[source]
        cause: deployment::Refusal,
    },
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

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "executed only by the exact-named parent process test"]
#[expect(
    clippy::too_many_lines,
    reason = "one native loss trace retains complete original request and processing facts through joined cleanup"
)]
async fn held_increment_reply_is_withdrawn_after_independent_processing() {
    let address =
        env::var("BOMBAY_COUNTER_LOSS_ADDRESS").expect("the parent supplies its reserved endpoint");
    let certificates = PathBuf::from(
        env::var_os("BOMBAY_COUNTER_LOSS_CERTIFICATES").expect("actual TLS credentials"),
    );
    let socket = PathBuf::from(
        env::var_os("BOMBAY_COUNTER_LOSS_ASSIGNMENT").expect("the private assignment socket"),
    );
    let identity = RefCell::new(None::<Deployment>);
    let (processed, acknowledged) = oneshot::channel();
    let processed = RefCell::new(Some(processed));
    let node = Application::new(Counter::new().stop_on_shutdown())
        .open_node(network::configuration(address, &certificates, Role::Serve))
        .await
        .unwrap_or_else(|_| panic!("the native receiving Node opens"));
    let runtime = node.runtime_id();
    let current = |receipt: &Receipt, runtime: &str, target: MailAddr| {
        let identity = identity.borrow();
        let configured = identity
            .as_ref()
            .expect("binding precedes declaration")
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
                .expect("actual bound finite configuration")
                .verify(Purpose::Request, bytes, proof)
                .map_err(|cause| Refusal::Deployment { cause })
        },
        async |bytes: &[u8], receipt: &Receipt| {
            let parsed: protocol::Response =
                serde_json::from_slice(bytes).map_err(|cause| Refusal::Json { cause })?;
            if parsed.bombay != *receipt.metadata()
                || parsed.operation != receipt.operation()
                || parsed.kind != Purpose::Reply
                || parsed.operation != Operation::Increment
            {
                return Err(Refusal::Scope);
            }
            let proof = identity
                .borrow()
                .as_ref()
                .expect("actual bound finite configuration")
                .protect(Purpose::Reply, bytes)
                .map_err(|cause| Refusal::Deployment { cause })?;
            let sender = processed.borrow_mut().take().ok_or(Refusal::Scope)?;
            sender.send((parsed, proof)).map_err(|_| Refusal::Scope)?;
            // The actual processed response and its valid finite proof have
            // been acquired. No proof is returned to Export for publication.
            pending::<Result<Box<[u8]>, Refusal>>().await
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
            .expect("the actual local target is owned");
        let actual = Binding {
            role: Role::Serve,
            runtime,
            actor: address.0,
        };
        let stream = UnixStream::connect(&socket)
            .await
            .expect("the parent accepts this actual binding");
        let (input, output) = stream.into_split();
        let configured =
            deployment::configure(&actual, BufReader::new(input), output, &limits()).await?;
        *identity.borrow_mut() = Some(configured);
        Ok::<_, deployment::Refusal>(target)
    });
    let outcome = node
        .run_with(move |application| async move {
            let acknowledged = acknowledged.await;
            let mut reader = application
                .client::<CounterValue>()
                .expect("a real local Read endpoint");
            let admitted = reader.send(CounterMessage::Read(reader.recipient())).await;
            let value = reader.receive().await;
            let shutdown = application.shutdown();
            (acknowledged, admitted, value, shutdown)
        })
        .await
        .unwrap_or_else(|_| panic!("the actual Node execution starts"));
    let (export, binding, closed, poll, disposal) = node
        .into_parts()
        .unwrap_or_else(|_| panic!("joined execution returns its owning Export"));
    let (work, report, native, notes, ()) = outcome.into_parts();
    let ((acknowledged, admitted, value, shutdown), cleanup) = work
        .try_into_completed()
        .unwrap_or_else(|_| panic!("the independent local processing fence completed"));
    let (_, _, declared, withdrawn, diagnostics, rows) = export.into_parts();
    let row_count = rows.len();
    let mut rows = rows.into_iter();
    let row = rows.next().expect("one actual received Increment");
    let extra = rows.next();
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
        .unwrap_or_else(|_| panic!("joined providers surrender original request custody"));
    let acknowledged =
        acknowledged.expect("the actual processed reply reached the test-owned hold");
    let value = value.expect("the independent local Read returns an original User");
    let configured = identity.borrow();
    let configured = configured
        .as_ref()
        .expect("the actual finite configuration remains owned");
    let grant = configured
        .metadata(Operation::Increment)
        .expect("the configured Increment grant");
    let verified = verified
        .expect("the real request was verified")
        .expect("the complete configured request pair");
    let encoding = encoding
        .expect("the actual processing reply encoded")
        .expect("the protocol encoded its native original");
    let expected_encoding = protocol::response(
        grant,
        Operation::Increment,
        protocol::Value::Processed(protocol::sequence(Operation::Increment)),
    )
    .expect("the exact configured response");
    let expected_request =
        protocol::request(grant, Operation::Increment).expect("the exact configured request");
    let expected_proof = configured
        .protect(Purpose::Request, &expected_request)
        .expect("the configured request proof");
    let expected_reply_proof = configured
        .protect(Purpose::Reply, &encoding)
        .expect("the configured reply proof");
    let report = report.expect("the actual Counter retirement report");
    let notes = notes.expect("the actual retirement notification receipts remain owned");
    let origin = MailAddr(grant.actor_generation);
    // All provider/native/Work/Node cleanup and original extraction precedes
    // the semantic observations. No transport wait is used as processing proof.
    assert!(cleanup.is_ok() && closed.is_ok() && native.is_ok());
    assert!(notes.termination.is_ok() && notes.retirement.is_ok());
    assert!(matches!(binding, Some(Ok(_))));
    assert!(poll.is_none() && disposal.is_none());
    assert!(matches!(declared, Some(Ok(()))) && matches!(withdrawn, Some(Ok(()))));
    assert!(diagnostics.is_empty());
    assert_eq!(report.retirement(), RetirementAssessment::Established);
    assert_eq!(report.failures(), ActorFailureAssessment::NoFailuresFound);
    assert!(admitted.is_ok() && shutdown.is_ok());
    assert_eq!((value.from, value.message), (origin, 1));
    assert_eq!(row_count, 1);
    assert!(extra.is_none());
    assert!(key.is_some() && decoded.is_none());
    assert_eq!(raw, (expected_request, expected_proof));
    assert_eq!(verified.operation(), Operation::Increment);
    assert_eq!(verified.metadata(), grant);
    assert_eq!(encoding, expected_encoding);
    assert_eq!(acknowledged.0.operation, Operation::Increment);
    assert_eq!(acknowledged.0.bombay, *grant);
    assert_eq!(acknowledged.0.value, protocol::Value::Processed(101));
    assert_eq!(acknowledged.1, expected_reply_proof);
    assert!(protection.is_none() && publications.is_empty());
    assert!(matches!(refusals, (None, None, None)));
    assert!(matches!(panics, (None, None, None, None, None, None)));
    match native_reply.expect("the complete native Increment remains owned") {
        Either::Left((constructor, client, admission, received, tail)) => {
            let received = received
                .expect("actual native receive result")
                .expect("actual processed User");
            assert!(constructor.is_none());
            assert!(matches!(client, Attempt::Returned(Ok(_))));
            assert!(matches!(admission, Attempt::Returned(Ok(()))));
            assert_eq!(received.from, origin);
            assert_eq!(received.message.request_sequence, 101);
            assert!(matches!(tail.as_slice(), [None]));
        }
        Either::Right(_) => panic!("the processed operation is the actual Increment leaf"),
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[expect(
    clippy::too_many_lines,
    reason = "the exact native child and ordinary caller are both reaped before the complete uncertainty observations"
)]
async fn processed_increment_without_publication_is_uncertain() {
    let directory = Builder::new()
        .prefix("counter-")
        .permissions(Permissions::from_mode(0o700))
        .tempdir()
        .expect("a private assignment directory");
    let socket = directory.path().join("a.sock");
    let certificates = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../../docs/prds/static-remote-actors/transport-custody/certificates");
    let trace = tokio::time::timeout(Duration::from_secs(40), async {
        let listener = UnixListener::bind(&socket)?;
        let port = {
            let reservation = TcpListener::bind(("localhost", 0))?;
            reservation.local_addr()?.port()
        };
        let address = format!("localhost:{port}");
        let output = directory.path().join("held.stdout");
        let diagnostics = directory.path().join("held.stderr");
        let child = Command::new(env::current_exe()?)
            .args([
                "--ignored",
                "--exact",
                "lost_reply::held_increment_reply_is_withdrawn_after_independent_processing",
            ])
            .env("BOMBAY_COUNTER_LOSS_ADDRESS", &address)
            .env("BOMBAY_COUNTER_LOSS_CERTIFICATES", &certificates)
            .env("BOMBAY_COUNTER_LOSS_ASSIGNMENT", &socket)
            .stdin(Stdio::null())
            .stdout(File::create(&output)?)
            .stderr(File::create(&diagnostics)?)
            .spawn()?;
        let mut receiver = Program {
            child,
            output,
            diagnostics,
        };
        let receiver_pid = receiver.child.id();
        let (receiver_binding, mut receiver_input) = assignment(&listener).await?;
        if receiver_binding.role != Role::Serve {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "the held server binding must be Serve",
            ));
        }
        let mut caller = Program::start(
            ProgramRole::Call(Duration::from_secs(20)),
            Mode::Test,
            &address,
            &certificates,
            &socket,
            directory.path(),
        )?;
        let caller_pid = caller.child.id();
        let (caller_binding, mut caller_input) = assignment(&listener).await?;
        if caller_binding.role != Role::Call {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "the ordinary caller binding must be Call",
            ));
        }
        let configuration = provision(&caller_binding, &receiver_binding, ReplyProof::Matching)?;
        receiver_input.write_all(&configuration).await?;
        receiver_input.shutdown().await?;
        caller_input.write_all(&configuration).await?;
        caller_input.shutdown().await?;
        let receiver_output = receiver.finish().await?;
        if !receiver_output.status.success() {
            // Preserve a concrete failed child result while preventing the
            // unrelated caller deadline from concealing the child assertion.
            drop(caller.child.kill());
        }
        let caller_output = caller.finish().await?;
        Ok::<_, io::Error>((
            receiver_pid,
            caller_pid,
            receiver_binding,
            caller_binding,
            receiver_output,
            caller_output,
        ))
    })
    .await;
    let removed = directory.close();
    let (
        receiver_pid,
        caller_pid,
        receiver_binding,
        caller_binding,
        receiver_output,
        caller_output,
    ) = trace
        .expect("the complete native process trace remains finite")
        .expect("both actual child exits are acquired");
    removed.expect("both children are reaped before removing assignment/output files");
    let receiver_stdout =
        String::from_utf8(receiver_output.stdout).expect("libtest output is UTF-8");
    let caller_stdout =
        String::from_utf8(caller_output.stdout).expect("ordinary caller output is UTF-8");
    assert_ne!(receiver_pid, caller_pid);
    assert_ne!(receiver_pid, process::id());
    assert_ne!(caller_pid, process::id());
    assert_eq!(receiver_binding.role, Role::Serve);
    assert_eq!(caller_binding.role, Role::Call);
    assert_ne!(receiver_binding.runtime, caller_binding.runtime);
    assert!(
        receiver_output.status.success(),
        "native held child: {:?}",
        receiver_output.stderr
    );
    assert_eq!(receiver_stdout.matches("test lost_reply::held_increment_reply_is_withdrawn_after_independent_processing ... ok").count(), 1);
    assert!(receiver_stdout.contains("test result: ok. 1 passed; 0 failed; 0 ignored;"));
    assert_eq!(
        caller_output.status.code(),
        Some(2),
        "ordinary caller: {:?}",
        caller_output.stderr
    );
    assert_eq!(caller_stdout, "counter uncertain: Increment\n");
}

#[derive(Clone, Copy)]
enum StopProof {
    Matching,
    Different,
}

// The controller changes one receiver-owned request proof, retaining every
// actual binding, grant, protected byte, correlation and other proof.
fn stop_configuration(
    caller: &Binding,
    receiver: &Binding,
    proof: StopProof,
) -> io::Result<Vec<u8>> {
    let original = provision(caller, receiver, ReplyProof::Matching)?;
    let mut configuration: serde_json::Value =
        serde_json::from_slice(&original).map_err(io::Error::other)?;
    let records = configuration["records"]
        .as_array_mut()
        .ok_or_else(|| io::Error::other("the real assignment has six records"))?;
    let mut changed = 0;
    for record in records {
        let bytes: Vec<u8> =
            serde_json::from_value(record["protected"].clone()).map_err(io::Error::other)?;
        let claims: serde_json::Value = serde_json::from_slice(&bytes).map_err(io::Error::other)?;
        let operation: Operation =
            serde_json::from_value(claims["operation"].clone()).map_err(io::Error::other)?;
        let purpose: Purpose =
            serde_json::from_value(claims["kind"].clone()).map_err(io::Error::other)?;
        if operation == Operation::Stop && purpose == Purpose::Request {
            record["proof"] = match proof {
                StopProof::Matching => serde_json::json!([Operation::Stop as u8, 0]),
                StopProof::Different => serde_json::json!([Operation::Stop as u8, 97]),
            };
            changed += 1;
        }
    }
    if changed != 1 {
        return Err(io::Error::other("exactly one Stop request is configured"));
    }
    let mut configuration = serde_json::to_vec(&configuration).map_err(io::Error::other)?;
    configuration.push(b'\n');
    Ok(configuration)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "executed only by the exact-named Stop parent controls"]
#[expect(
    clippy::too_many_lines,
    reason = "complete native operation originals and both Stop outcomes remain outside producers until joined cleanup"
)]
async fn stop_proof_preserves_the_counter_and_original_request() {
    let address = env::var("BOMBAY_COUNTER_STOP_ADDRESS").expect("actual reserved endpoint");
    let certificates = PathBuf::from(
        env::var_os("BOMBAY_COUNTER_STOP_CERTIFICATES").expect("actual TLS credentials"),
    );
    let socket =
        PathBuf::from(env::var_os("BOMBAY_COUNTER_STOP_ASSIGNMENT").expect("assignment socket"));
    let expectation = match env::var("BOMBAY_COUNTER_STOP_PROOF").as_deref() {
        Ok("matching") => StopProof::Matching,
        Ok("different") => StopProof::Different,
        _ => panic!("the exact parent selects the Stop proof"),
    };
    let identity = RefCell::new(None::<Deployment>);
    let (input_sender, input_receiver) = oneshot::channel();
    let input_sender = RefCell::new(Some(input_sender));
    let node = Application::new(Counter::new().stop_on_shutdown())
        .open_node(network::configuration(address, &certificates, Role::Serve))
        .await
        .unwrap_or_else(|_| panic!("the receiving Node opens"));
    let runtime = node.runtime_id();
    let current = |receipt: &Receipt, runtime: &str, target: MailAddr| {
        let identity = identity.borrow();
        let grant = identity
            .as_ref()
            .expect("bound deployment")
            .metadata(receipt.operation())
            .map_err(|cause| Refusal::Deployment { cause })?;
        if receipt.metadata() == grant
            && grant.runtime_generation.as_ref() == runtime
            && grant.actor_generation == target.0
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
                .expect("bound deployment")
                .verify(Purpose::Request, bytes, proof)
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
                .expect("bound deployment")
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
            .expect("actual local Counter");
        let actual = Binding {
            role: Role::Serve,
            runtime,
            actor: address.0,
        };
        let stream = UnixStream::connect(&socket)
            .await
            .expect("actual assignment connection");
        let (input, output) = stream.into_split();
        let mut input = BufReader::new(input);
        let configured = deployment::configure(&actual, &mut input, output, &limits()).await?;
        *identity.borrow_mut() = Some(configured);
        let sent = input_sender
            .borrow_mut()
            .take()
            .expect("one binding")
            .send(input);
        sent.unwrap_or_else(|_| panic!("Work retains the actual assignment reader"));
        Ok::<_, deployment::Refusal>(target)
    });
    let outcome = node
        .run_with(move |application| async move {
            let mut input = input_receiver
                .await
                .expect("binding retained the control reader");
            let mut release = [0_u8; 1];
            let released = input.read_exact(&mut release).await;
            let read = match expectation {
                StopProof::Different => {
                    let mut reader = application
                        .client::<CounterValue>()
                        .expect("actual local Read endpoint");
                    let admitted = reader.send(CounterMessage::Read(reader.recipient())).await;
                    let value = match &admitted {
                        Ok(()) => Attempt::Returned(reader.receive().await),
                        Err(_) => Attempt::Unattempted,
                    };
                    let shutdown = application.shutdown();
                    Some((admitted, value, shutdown))
                }
                StopProof::Matching => None,
            };
            (released, release, read)
        })
        .await
        .unwrap_or_else(|_| panic!("the public Node executes"));
    let (export, binding, closed, poll, disposal) = node
        .into_parts()
        .unwrap_or_else(|_| panic!("joined Node returns its owning Export"));
    let (work, report, native, notes, ()) = outcome.into_parts();
    // Root retirement and caller Work completion are independent. Both actual
    // callers earn release; only a denied Stop requires the independent Read.
    let audit = work.try_into_completed();
    let report = report.expect("actual Counter retirement report");
    let notes = notes.expect("actual retirement notification receipts");
    let (_, _, declared, withdrawn, diagnostics, rows) = export.into_parts();
    let count = rows.len();
    let configured = identity.borrow();
    let configured = configured.as_ref().expect("original finite configuration");
    let origin = MailAddr(
        configured
            .metadata(Operation::Stop)
            .expect("Stop grant")
            .actor_generation,
    );
    let rows = rows
        .into_iter()
        .map(|row| {
            row.try_into_parts()
                .unwrap_or_else(|_| panic!("all original rows return after cleanup"))
        })
        .collect::<Vec<_>>();
    assert!(closed.is_ok() && native.is_ok());
    assert!(notes.termination.is_ok() && notes.retirement.is_ok());
    assert!(matches!(binding, Some(Ok(_))));
    assert!(poll.is_none() && disposal.is_none());
    assert!(matches!(declared, Some(Ok(()))) && matches!(withdrawn, Some(Ok(()))));
    assert!(diagnostics.is_empty());
    assert_eq!(report.retirement(), RetirementAssessment::Established);
    assert_eq!(report.failures(), ActorFailureAssessment::NoFailuresFound);
    assert_eq!(count, 3);
    let mut rows = rows.into_iter();
    for operation in [Operation::Increment, Operation::Read, Operation::Stop] {
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
        ) = rows.next().expect("one complete row per actual operation");
        let grant = configured
            .metadata(operation)
            .expect("original operation grant");
        let expected_request =
            protocol::request(grant, operation).expect("original request encoding");
        let expected_proof: Box<[u8]> = vec![operation as u8, 0].into_boxed_slice();
        assert_eq!(raw, (expected_request, expected_proof));
        assert!(decoded.is_none());
        assert!(matches!(refusals, (None, None, None)));
        assert!(matches!(panics, (None, None, None, None, None, None)));
        if let (Operation::Stop, StopProof::Different) = (operation, expectation) {
            // Primary denial oracle precedes any denied-only Work unwrap.
            assert!(matches!(verified, Some(Err(deployment::Refusal::Proof))));
            assert!(key.is_none() && native_reply.is_none());
            assert!(encoding.is_none() && protection.is_none() && publications.is_empty());
        } else {
            let verified = verified
                .expect("verification was attempted")
                .expect("exact configured pair");
            assert_eq!(verified.operation(), operation);
            assert_eq!(verified.metadata(), grant);
            assert!(key.is_some());
            let encoded = encoding
                .expect("actual native output encoded")
                .expect("encoding succeeded");
            let expected_value = match operation {
                Operation::Increment => protocol::Value::Processed(101),
                Operation::Read => protocol::Value::Read(1),
                Operation::Stop => protocol::Value::Stopped,
            };
            let expected_encoding =
                protocol::response(grant, operation, expected_value).expect("exact wire response");
            let proof = configured
                .protect(Purpose::Reply, &encoded)
                .expect("configured reply proof");
            let protected = protection
                .expect("reply protection attempted")
                .expect("reply protection succeeded");
            assert_eq!(encoded, expected_encoding);
            assert_eq!(protected, proof);
            assert_eq!(publications.len(), 1);
            assert!(publications.iter().all(Result::is_ok));
            match (
                operation,
                native_reply.expect("actual native operation remains owned"),
            ) {
                (
                    Operation::Increment,
                    Either::Left((constructor, client, admission, received, tail)),
                ) => {
                    let received = received
                        .expect("native receive completed")
                        .expect("actual processed User");
                    assert!(constructor.is_none());
                    assert!(matches!(client, Attempt::Returned(Ok(_))));
                    assert!(matches!(admission, Attempt::Returned(Ok(()))));
                    assert_eq!(received.from, origin);
                    assert_eq!(received.message.request_sequence, 101);
                    assert!(matches!(tail.as_slice(), [None]));
                }
                (
                    Operation::Read,
                    Either::Right(Either::Left((constructor, client, admission, received, tail))),
                ) => {
                    let received = received
                        .expect("native receive completed")
                        .expect("actual value User");
                    assert!(constructor.is_none());
                    assert!(matches!(client, Attempt::Returned(Ok(_))));
                    assert!(matches!(admission, Attempt::Returned(Ok(()))));
                    assert_eq!((received.from, received.message), (origin, 1));
                    assert!(matches!(tail.as_slice(), [None]));
                }
                (Operation::Stop, Either::Right(Either::Right((admission, stopped)))) => {
                    assert!(matches!(admission, Attempt::Returned(Ok(()))));
                    assert_eq!(stopped, Some(report));
                }
                _ => panic!("native leaf must match the exact operation"),
            }
        }
    }
    let ((released, release, read), cleanup) =
        audit.unwrap_or_else(|_| panic!("caller release completes the actual audit Work"));
    assert!(released.is_ok() && cleanup.is_ok());
    assert_eq!(release, [b'R']);
    match expectation {
        StopProof::Different => {
            let (admitted, value, shutdown) =
                read.expect("denied Stop leaves the actor alive for the audit Read");
            let Attempt::Returned(Some(value)) = value else {
                panic!("actual local Read reply");
            };
            assert!(admitted.is_ok() && shutdown.is_ok());
            assert_eq!((value.from, value.message), (origin, 1));
        }
        StopProof::Matching => assert!(read.is_none()),
    }
}

#[expect(
    clippy::too_many_lines,
    reason = "both actual process exits and control release are retained before semantic assertions"
)]
async fn observe_counter_stop(proof: StopProof) {
    let directory = Builder::new()
        .prefix("counter-stop-")
        .permissions(Permissions::from_mode(0o700))
        .tempdir()
        .expect("private test directory");
    let socket = directory.path().join("a.sock");
    let certificates = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../../docs/prds/static-remote-actors/transport-custody/certificates");
    let trace = tokio::time::timeout(Duration::from_secs(40), async {
        let listener = UnixListener::bind(&socket)?;
        let port = {
            let reservation = TcpListener::bind(("localhost", 0))?;
            reservation.local_addr()?.port()
        };
        let address = format!("localhost:{port}");
        let output = directory.path().join("stop.stdout");
        let diagnostics = directory.path().join("stop.stderr");
        let expected = match proof {
            StopProof::Matching => "matching",
            StopProof::Different => "different",
        };
        let child = Command::new(env::current_exe()?)
            .args([
                "--ignored",
                "--exact",
                "lost_reply::stop_proof_preserves_the_counter_and_original_request",
            ])
            .env("BOMBAY_COUNTER_STOP_ADDRESS", &address)
            .env("BOMBAY_COUNTER_STOP_CERTIFICATES", &certificates)
            .env("BOMBAY_COUNTER_STOP_ASSIGNMENT", &socket)
            .env("BOMBAY_COUNTER_STOP_PROOF", expected)
            .stdin(Stdio::null())
            .stdout(File::create(&output)?)
            .stderr(File::create(&diagnostics)?)
            .spawn()?;
        let mut receiver = Program {
            child,
            output,
            diagnostics,
        };
        let receiver_pid = receiver.child.id();
        let (receiver_binding, mut receiver_input) = assignment(&listener).await?;
        let mut caller = Program::start(
            ProgramRole::Call(Duration::from_secs(2)),
            Mode::Test,
            &address,
            &certificates,
            &socket,
            directory.path(),
        )?;
        let caller_pid = caller.child.id();
        let (caller_binding, mut caller_input) = assignment(&listener).await?;
        let receiving = stop_configuration(&caller_binding, &receiver_binding, proof)?;
        let calling = provision(&caller_binding, &receiver_binding, ReplyProof::Matching)?;
        receiver_input.write_all(&receiving).await?;
        // Keep this half open until the ordinary caller has actually exited.
        caller_input.write_all(&calling).await?;
        caller_input.shutdown().await?;
        let caller_output = caller.finish().await?;
        let release = receiver_input.write_all(b"R").await;
        let receiver_output = receiver.finish().await?;
        Ok::<_, io::Error>((
            receiver_pid,
            caller_pid,
            receiver_binding,
            caller_binding,
            caller_output,
            release,
            receiver_output,
        ))
    })
    .await;
    let removed = directory.close();
    let (
        receiver_pid,
        caller_pid,
        receiver_binding,
        caller_binding,
        caller_output,
        release,
        receiver_output,
    ) = trace
        .expect("complete two-process Stop trace is finite")
        .expect("both actual children are reaped");
    removed.expect("both native children are reaped before directory removal");
    let receiver_stdout = String::from_utf8(receiver_output.stdout).expect("libtest UTF-8");
    let caller_stdout = String::from_utf8(caller_output.stdout).expect("ordinary domain UTF-8");
    assert_ne!(receiver_pid, caller_pid);
    assert_ne!(receiver_pid, process::id());
    assert_ne!(caller_pid, process::id());
    assert_eq!(receiver_binding.role, Role::Serve);
    assert_eq!(caller_binding.role, Role::Call);
    assert_ne!(receiver_binding.runtime, caller_binding.runtime);
    assert!(
        receiver_output.status.success(),
        "native Stop child stdout:\n{receiver_stdout}\nnative Stop child stderr:\n{}",
        String::from_utf8_lossy(&receiver_output.stderr)
    );
    assert_eq!(
        receiver_stdout
            .matches(
                "test lost_reply::stop_proof_preserves_the_counter_and_original_request ... ok"
            )
            .count(),
        1
    );
    assert!(receiver_stdout.contains("test result: ok. 1 passed; 0 failed; 0 ignored;"));
    assert_eq!(caller_output.stderr, [] as [u8; 0]);
    assert!(release.is_ok());
    match proof {
        StopProof::Different => {
            assert_eq!(caller_output.status.code(), Some(2));
            assert_eq!(caller_stdout, "counter uncertain: Stop\n");
        }
        StopProof::Matching => {
            assert_eq!(caller_output.status.code(), Some(0));
            assert_eq!(caller_stdout, "counter value: 1\ncounter stopped\n");
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn denied_stop_preserves_a_live_counter_in_a_separate_program() {
    observe_counter_stop(StopProof::Different).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn matching_stop_retires_the_same_separate_counter() {
    observe_counter_stop(StopProof::Matching).await;
}
