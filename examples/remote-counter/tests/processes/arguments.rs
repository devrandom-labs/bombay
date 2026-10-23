//! CLI-only controls; append under the existing processes test owner.
//! Reuses that owner's Program kill-and-reap guard, not another runner.
use super::Program;
use std::fs::{File, Permissions};
use std::io;
use std::os::unix::fs::PermissionsExt;
use std::process::{Command, Stdio};
use std::time::Duration;
use tempfile::Builder;

enum Refusal {
    Arguments,
    Production,
    Duration,
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[expect(
    clippy::too_many_lines,
    reason = "all five actual CLI commands are reaped before their complete argument and refusal observations"
)]
async fn each_role_accepts_only_its_actual_configuration_inputs() {
    let directory = Builder::new()
        .prefix("counter-")
        .permissions(Permissions::from_mode(0o700))
        .tempdir()
        .expect("the CLI controller owns a private directory");
    let trace = tokio::time::timeout(Duration::from_secs(10), async {
        let cases: [(&str, &[&str], Refusal); 5] = [
            (
                "serve-production",
                &[
                    "serve",
                    "production-required",
                    "localhost:9",
                    "absent-certificates",
                    "unused.sock",
                ],
                Refusal::Production,
            ),
            (
                "call-production",
                &[
                    "call",
                    "production-required",
                    "localhost:9",
                    "absent-certificates",
                    "unused.sock",
                    "20",
                ],
                Refusal::Production,
            ),
            (
                "serve-extra-wait",
                &[
                    "serve",
                    "test",
                    "localhost:9",
                    "absent-certificates",
                    "unused.sock",
                    "20",
                ],
                Refusal::Arguments,
            ),
            (
                "call-missing-wait",
                &[
                    "call",
                    "test",
                    "localhost:9",
                    "absent-certificates",
                    "unused.sock",
                ],
                Refusal::Arguments,
            ),
            (
                "call-zero-wait",
                &[
                    "call",
                    "test",
                    "localhost:9",
                    "absent-certificates",
                    "unused.sock",
                    "0",
                ],
                Refusal::Duration,
            ),
        ];
        let mut results = Vec::new();
        for (name, arguments, expected) in cases {
            let output = directory.path().join(format!("{name}.stdout"));
            let diagnostics = directory.path().join(format!("{name}.stderr"));
            let child = Command::new(env!("CARGO_BIN_EXE_bombay-example-remote-counter"))
                .args(arguments)
                .current_dir(directory.path())
                .stdin(Stdio::null())
                .stdout(File::create(&output)?)
                .stderr(File::create(&diagnostics)?)
                .spawn()?;
            let mut program = Program {
                child,
                output,
                diagnostics,
            };
            results.push((expected, program.finish().await?));
        }
        Ok::<_, io::Error>(results)
    })
    .await;
    // Cancellation unwinds each real Program guard before removing its files.
    let removed = directory.close();
    let results = trace
        .expect("all five actual argument controls stay bounded")
        .expect("all five ordinary program statuses and outputs are acquired");
    removed.expect("release the CLI controller directory after all child reaps");
    assert_eq!(results.len(), 5);
    for (expected, output) in results {
        let diagnostics =
            String::from_utf8(output.stderr).expect("bounded CLI diagnostics are UTF-8");
        assert_eq!(output.status.code(), Some(1));
        assert_eq!(output.stdout.as_slice(), b"");
        match expected {
            Refusal::Arguments => assert!(diagnostics.starts_with("usage: remote-counter ")),
            Refusal::Production => assert_eq!(
                diagnostics,
                "this finite test provider cannot satisfy a production deployment\n"
            ),
            Refusal::Duration => assert_eq!(
                diagnostics,
                "the wait duration must be a positive integer\n"
            ),
        }
    }
}
