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
use tokio::io::{AsyncWriteExt, BufReader};
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
