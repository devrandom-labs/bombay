//! Two ordinary programs use real TLS and one bounded assignment each.
use bombay::remote::Metadata;
use serde_json::json;
use std::fs::{File, Permissions};
use std::future::poll_fn;
use std::io::{self, Read};
use std::net::TcpListener;
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};
use std::process::{self, Child, Command, Output, Stdio};
use std::task::Poll;
use std::time::Duration;
use tempfile::Builder;
use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};
use tokio::net::UnixListener;
use tokio::net::unix::OwnedWriteHalf;

#[path = "../../counter/src/counter.rs"]
#[expect(
    dead_code,
    reason = "the controller reuses the application's actual wire functions, not its separately executed actor"
)]
mod counter;
#[path = "../src/deployment.rs"]
#[expect(
    dead_code,
    reason = "the controller shares actual binding and operation types; each program executes validation and providers"
)]
mod deployment;
#[path = "../src/protocol.rs"]
#[expect(
    dead_code,
    reason = "the controller configures exact pairs through the actual wire owner; the child program executes Caller"
)]
mod protocol;
use deployment::{Binding, Operation, Role};
use protocol::Value;

#[path = "processes/arguments.rs"]
mod arguments;
#[path = "processes/lost_reply.rs"]
mod lost_reply;

#[derive(Clone, Copy)]
enum Mode {
    Test,
    ProductionRequired,
}

#[derive(Clone, Copy)]
enum ProgramRole {
    Serve,
    Call(Duration),
}

#[derive(Clone, Copy)]
enum ReplyProof {
    Matching,
    WrongRead,
}

// The only extra owner is a native child guard. Its failure policy is kill AND
// reap, including cancellation of the outer timeout or setup unwinding.
struct Program {
    child: Child,
    output: PathBuf,
    diagnostics: PathBuf,
}

impl Program {
    fn start(
        role: ProgramRole,
        mode: Mode,
        address: &str,
        certificates: &Path,
        socket: &Path,
        directory: &Path,
    ) -> io::Result<Self> {
        let name = match role {
            ProgramRole::Serve => "serve",
            ProgramRole::Call(_) => "call",
        };
        let mode = match mode {
            Mode::Test => "test",
            Mode::ProductionRequired => "production-required",
        };
        let output = directory.join(format!("{name}.stdout"));
        let diagnostics = directory.join(format!("{name}.stderr"));
        let mut command = Command::new(env!("CARGO_BIN_EXE_bombay-example-remote-counter"));
        command
            .arg(name)
            .arg(mode)
            .arg(address)
            .arg(certificates)
            .arg(socket);
        match role {
            ProgramRole::Serve => {}
            ProgramRole::Call(wait) => {
                command.arg(wait.as_secs().to_string());
            }
        }
        let child = command
            .stdin(Stdio::null())
            .stdout(File::create(&output)?)
            .stderr(File::create(&diagnostics)?)
            .spawn()?;
        Ok(Self {
            child,
            output,
            diagnostics,
        })
    }

    async fn finish(&mut self) -> io::Result<Output> {
        loop {
            if let Some(status) = self.child.try_wait()? {
                return Ok(Output {
                    status,
                    stdout: bounded_output(&self.output)?,
                    stderr: bounded_output(&self.diagnostics)?,
                });
            }
            // This timer waits only for actual OS exit; it never makes a route
            // ready or earns network/lifecycle acceptance from elapsed time.
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }
}

impl Drop for Program {
    fn drop(&mut self) {
        if !matches!(self.child.try_wait(), Ok(Some(_))) {
            // Emergency cleanup results are explicitly discharged. A test
            // succeeds only through finish's acquired real exit status.
            drop(self.child.kill());
            drop(self.child.wait());
        }
    }
}

fn bounded_output(path: &Path) -> io::Result<Vec<u8>> {
    let mut bytes = Vec::new();
    File::open(path)?.take(8193).read_to_end(&mut bytes)?;
    if bytes.len() > 8192 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "program output exceeded the finite test limit",
        ));
    }
    Ok(bytes)
}

async fn assignment(listener: &UnixListener) -> io::Result<(Binding, OwnedWriteHalf)> {
    let (stream, _) = listener.accept().await?;
    let (input, output) = stream.into_split();
    let mut input = BufReader::new(input.take(8193));
    let mut original = Vec::new();
    input.read_until(b'\n', &mut original).await?;
    if original.len() > 8192 || original.last() != Some(&b'\n') {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "one bounded actual binding is required",
        ));
    }
    // Unverified framing is explicitly discharged after ordinary DTO parsing.
    let binding = serde_json::from_slice(&original).map_err(io::Error::other)?;
    Ok((binding, output))
}

fn provision(caller: &Binding, receiver: &Binding, proof: ReplyProof) -> io::Result<Vec<u8>> {
    let mut grants = Vec::new();
    let mut records = Vec::new();
    for operation in [Operation::Increment, Operation::Read, Operation::Stop] {
        let grant = Metadata {
            wire_version: 1,
            schema_version: 1,
            provider_profile: "remote-counter-finite-pairs-v1".into(),
            deployment: "finite-counter-test".into(),
            caller: "customer".into(),
            caller_session: caller.runtime.clone().into(),
            controller: "test-controller".into(),
            actor: "counter".into(),
            host: "recipient".into(),
            protocol: "counter".into(),
            grant_issuer: "finite-test-provider".into(),
            runtime_generation: receiver.runtime.clone().into(),
            actor_generation: receiver.actor,
            authority_generation: 1,
            request_sequence: protocol::sequence(operation),
        };
        let request = protocol::request(&grant, operation).map_err(io::Error::other)?;
        let value = match operation {
            Operation::Increment => Value::Processed(protocol::sequence(operation)),
            Operation::Read => Value::Read(1),
            Operation::Stop => Value::Stopped,
        };
        let response = protocol::response(&grant, operation, value).map_err(io::Error::other)?;
        records.push(json!({"protected":request.as_ref(), "proof":[operation as u8, 0]}));
        let reply_proof = match (proof, operation) {
            (ReplyProof::WrongRead, Operation::Read) => [operation as u8, 97],
            (ReplyProof::Matching, _)
            | (ReplyProof::WrongRead, Operation::Increment | Operation::Stop) => {
                [operation as u8, 1]
            }
        };
        records.push(json!({"protected":response.as_ref(), "proof":reply_proof}));
        grants.push(grant);
    }
    let mut configuration = serde_json::to_vec(&json!({
        "profile":"remote-counter-finite-pairs-v1", "revision":1,
        "caller":caller, "receiver":receiver, "grants":grants, "records":records,
    }))
    .map_err(io::Error::other)?;
    configuration.push(b'\n');
    if configuration.len() > 262_144 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "the complete assignment exceeds the application limit",
        ));
    }
    Ok(configuration)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[expect(
    clippy::too_many_lines,
    reason = "the complete two-program trace keeps real exits and cleanup before its independent domain oracles"
)]
async fn separate_programs_increment_read_and_stop_through_real_tls() {
    let directory = Builder::new()
        .prefix("counter-")
        .permissions(Permissions::from_mode(0o700))
        .tempdir()
        .expect("create this test's private assignment directory");
    let socket = directory.path().join("a.sock");
    let certificates = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../../docs/prds/static-remote-actors/transport-custody/certificates");
    let trace = tokio::time::timeout(Duration::from_secs(40), async {
        let listener = UnixListener::bind(&socket)?;
        // Reservation only avoids ordinary test-port collisions. The actual
        // server binding below, not elapsed time, gates caller startup.
        let port = {
            let reservation = TcpListener::bind(("localhost", 0))?;
            reservation.local_addr()?.port()
        };
        let address = format!("localhost:{port}");
        let mut receiver = Program::start(
            ProgramRole::Serve,
            Mode::Test,
            &address,
            &certificates,
            &socket,
            directory.path(),
        )?;
        let receiver_pid = receiver.child.id();
        let (receiver_binding, mut receiver_input) = assignment(&listener).await?;
        match receiver_binding.role {
            Role::Serve => {}
            Role::Call => {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "the listening program supplied the wrong binding role",
                ));
            }
        }
        // Binding occurs after the server's actual Node open. The native Client
        // owns subsequent Queryable visibility; this test adds no matching loop.
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
        match caller_binding.role {
            Role::Call => {}
            Role::Serve => {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "the calling program supplied the wrong binding role",
                ));
            }
        }
        let configuration = provision(&caller_binding, &receiver_binding, ReplyProof::Matching)?;
        receiver_input.write_all(&configuration).await?;
        receiver_input.shutdown().await?;
        caller_input.write_all(&configuration).await?;
        caller_input.shutdown().await?;
        // A failed caller may truthfully leave the service alive. Acquire that
        // actual result first, then reap the server before judging the journey.
        let caller_output = caller.finish().await?;
        let receiver_kill = if caller_output.status.success() {
            None
        } else {
            Some(receiver.child.kill())
        };
        let receiver_output = receiver.finish().await?;
        Ok::<_, io::Error>((
            receiver_pid,
            caller_pid,
            receiver_binding,
            caller_binding,
            receiver_output,
            caller_output,
            receiver_kill,
        ))
    })
    .await;
    // This occurs after normal return or after cancellation has killed/reaped
    // both children. Cleanup precedes every semantic success assertion.
    let removed = directory.close();
    let (
        receiver_pid,
        caller_pid,
        receiver_binding,
        caller_binding,
        receiver_output,
        caller_output,
        receiver_kill,
    ) = trace
        .expect("the whole process/assignment/native trace stays bounded")
        .expect("both actual programs and their controller finish");
    removed.expect("release the test's assignment socket and output files");
    let receiver_stdout =
        String::from_utf8(receiver_output.stdout).expect("the ordinary server output is UTF-8");
    let caller_stdout =
        String::from_utf8(caller_output.stdout).expect("the ordinary caller output is UTF-8");
    let parent_pid = process::id();
    assert_ne!(receiver_pid, caller_pid);
    assert_ne!(receiver_pid, parent_pid);
    assert_ne!(caller_pid, parent_pid);
    assert_eq!(receiver_binding.role, Role::Serve);
    assert_eq!(caller_binding.role, Role::Call);
    assert!(!receiver_binding.runtime.is_empty() && !caller_binding.runtime.is_empty());
    assert_ne!(receiver_binding.runtime, caller_binding.runtime);
    // Primary no-transport oracle precedes the expected clean server exit: a
    // real Uncertain caller is not hidden by the parent's necessary server kill.
    assert_eq!(caller_stdout, "counter value: 1\ncounter stopped\n");
    assert!(
        caller_output.status.success(),
        "caller: {:?}",
        caller_output.stderr
    );
    assert!(receiver_kill.is_none());
    assert!(
        receiver_output.status.success(),
        "server: {:?}",
        receiver_output.stderr
    );
    assert_eq!(receiver_stdout, "counter stopped\n");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn production_required_refuses_before_tls_or_assignment() {
    let directory = Builder::new()
        .prefix("counter-")
        .permissions(Permissions::from_mode(0o700))
        .tempdir()
        .expect("the refusal controller owns a private directory");
    let socket = directory.path().join("a.sock");
    let trace = tokio::time::timeout(Duration::from_secs(10), async {
        let listener = UnixListener::bind(&socket)?;
        let certificates = directory.path().join("absent-certificates");
        let mut outputs = Vec::new();
        for role in [
            ProgramRole::Serve,
            ProgramRole::Call(Duration::from_secs(2)),
        ] {
            let mut program = Program::start(
                role,
                Mode::ProductionRequired,
                "localhost:9?priority=1",
                &certificates,
                &socket,
                directory.path(),
            )?;
            outputs.push(program.finish().await?);
        }
        let accepted = poll_fn(|context| Poll::Ready(listener.poll_accept(context))).await;
        // Dispose any unexpected unverified connection before inspecting the
        // observation. Preserve its actual peer address or native accept error.
        let assignment = match accepted {
            Poll::Pending => Poll::Pending,
            Poll::Ready(Ok((stream, address))) => {
                drop(stream);
                Poll::Ready(Ok(address))
            }
            Poll::Ready(Err(cause)) => Poll::Ready(Err(cause)),
        };
        Ok::<_, io::Error>((outputs, assignment))
    })
    .await;
    let removed = directory.close();
    let (outputs, assignment) = trace
        .expect("the early CLI refusal stays finite")
        .expect("both production-required programs finish before configuration IO");
    removed.expect("release the refusal controller's socket and output files");
    let [serve, call]: [_; 2] = outputs
        .try_into()
        .unwrap_or_else(|_| panic!("both actual CLI roles were executed"));
    assert!(matches!(&assignment, Poll::Pending));
    for output in [serve, call] {
        let code = output.status.code();
        let diagnostics = String::from_utf8(output.stderr).expect("bounded refusal is UTF-8");
        assert_eq!(code, Some(1));
        assert_eq!(output.stdout.as_slice(), b"");
        // The existing Network owner rejects endpoint metadata before SDK
        // startup, producing a different refusal if the provider guard moves.
        assert_eq!(
            diagnostics,
            "this finite test provider cannot satisfy a production deployment\n"
        );
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[expect(
    clippy::too_many_lines,
    reason = "the wrong-proof trace preserves both actual child exits and server liveness before judging uncertainty"
)]
async fn wrong_caller_reply_proof_reports_uncertain_read_without_stopping_server() {
    let directory = Builder::new()
        .prefix("counter-")
        .permissions(Permissions::from_mode(0o700))
        .tempdir()
        .expect("the proof controller owns a private directory");
    let socket = directory.path().join("a.sock");
    let certificates = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../../docs/prds/static-remote-actors/transport-custody/certificates");
    let trace = tokio::time::timeout(Duration::from_secs(15), async {
        let listener = UnixListener::bind(&socket)?;
        let port = {
            let reservation = TcpListener::bind(("localhost", 0))?;
            reservation.local_addr()?.port()
        };
        let address = format!("localhost:{port}");
        let mut receiver = Program::start(
            ProgramRole::Serve,
            Mode::Test,
            &address,
            &certificates,
            &socket,
            directory.path(),
        )?;
        let receiver_pid = receiver.child.id();
        let (receiver_binding, mut receiver_input) = assignment(&listener).await?;
        match receiver_binding.role {
            Role::Serve => {}
            Role::Call => {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "expected actual serve binding",
                ));
            }
        }
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
        match caller_binding.role {
            Role::Call => {}
            Role::Serve => {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "expected actual call binding",
                ));
            }
        }
        let receiving = provision(&caller_binding, &receiver_binding, ReplyProof::Matching)?;
        let calling = provision(&caller_binding, &receiver_binding, ReplyProof::WrongRead)?;
        receiver_input.write_all(&receiving).await?;
        receiver_input.shutdown().await?;
        caller_input.write_all(&calling).await?;
        caller_input.shutdown().await?;
        let caller_output = caller.finish().await?;
        let receiver_before_kill = receiver.child.try_wait();
        let killed = receiver.child.kill();
        let receiver_output = receiver.finish().await?;
        Ok::<_, io::Error>((
            receiver_pid,
            caller_pid,
            receiver_binding,
            caller_binding,
            caller_output,
            receiver_before_kill,
            killed,
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
        receiver_before_kill,
        killed,
        receiver_output,
    ) = trace
        .expect("the actual proof refusal reaches application uncertainty before the watchdog")
        .expect("the caller exits and the parent reaps the still-live service");
    removed.expect("release the proof controller's socket and output files");
    let caller_code = caller_output.status.code();
    let caller_stdout = String::from_utf8(caller_output.stdout).expect("domain output is UTF-8");
    assert_ne!(receiver_pid, caller_pid);
    assert_eq!(receiver_binding.role, Role::Serve);
    assert_eq!(caller_binding.role, Role::Call);
    assert!(!receiver_binding.runtime.is_empty() && !caller_binding.runtime.is_empty());
    assert_ne!(receiver_binding.runtime, caller_binding.runtime);
    // A no-query shortcut remains at Increment; an unconditional success print
    // is also rejected. Neither can substitute for uncertainty at checked Read.
    assert_eq!(caller_stdout, "counter uncertain: Read\n");
    assert_eq!(caller_code, Some(2));
    assert_eq!(caller_output.stderr.as_slice(), b"");
    assert!(matches!(&receiver_before_kill, Ok(None)));
    assert!(killed.is_ok());
    assert!(!receiver_output.status.success());
    assert_eq!(receiver_output.stdout.as_slice(), b"");
    // Parent-forced server exit is NOT clean Node/Session shutdown evidence.
}
