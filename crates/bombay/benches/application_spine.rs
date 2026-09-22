//! End-to-end benchmark over the composed local Environment spine.
//!
//! One invocation measures the complete application lifecycle through the
//! public `Application` boundary — spawn, tell-shaped send, turn, shutdown,
//! and retirement — plus the tell-and-reply round trip, with a realistic
//! ~40 B command payload so the by-value copy a real slot pays is measured
//! honestly. The mailbox-layer lane equivalents live in `mailbox_lanes.rs`.
//! The retired old-runtime oracle (`benches/mailbox.rs`: `tell` ≈ 5.7 ns,
//! send+recv ≈ 18.4 ns, recorded in the `b0c212a` history) is a labeled
//! baseline only and is never compared as a current number.

use std::hint::black_box;
use std::time::{Duration, Instant};

use bombay::behavior::{BehaviorSettlements, ClassifySettlement, SettlementStatus};
use bombay::prelude::*;

/// Application runs per measured lifecycle sample.
const RUNS_PER_SAMPLE: u32 = 25;
/// Tell-shaped sends per application run.
const TELLS_PER_RUN: u64 = 1_000;
/// Tell-and-reply round trips per application run.
const ROUNDS_PER_RUN: u64 = 1_000;
/// Warm-up sends before each measured window, excluding lazy platform
/// initialization from the steady-state cost.
const WARMUP_SENDS: u64 = 128;
/// Repetitions per scenario; the minimum and median are reported.
const REPETITIONS: usize = 7;

/// A realistically-sized actor command (~40 bytes), matching the retired
/// old-runtime bench's payload shape.
#[derive(Clone, Copy)]
#[allow(
    dead_code,
    reason = "the payload models a realistic command size; the spine measures transport cost, not content"
)]
struct Command {
    id: u64,
    correlation: u64,
    kind: u32,
    amount: i64,
    flags: u64,
}

fn command(index: u64) -> Command {
    let low = u32::try_from(index & 0xff).expect("the masked index fits u32");
    Command {
        id: index,
        correlation: index ^ 0x5555_5555,
        kind: low,
        amount: i64::from(low),
        flags: index.rotate_left(7),
    }
}

/// The external customer's inbound value protocol: the tally replies the
/// exact accumulated count through an established recipient.
struct TallyValue;

impl Protocol for TallyValue {
    type Addr = MailAddr;
    type Msg = u64;
}

enum TallyMessage {
    #[allow(
        dead_code,
        reason = "the payload models a realistic command size; the tally counts arrivals, not content"
    )]
    Command(Command),
    Read(EstablishedRecipient<TallyValue>),
}

#[derive(Debug, PartialEq, Eq)]
enum TallyError {
    Overflow,
}

/// Counts every accepted command and replies the exact count on demand.
struct Tally {
    value: u64,
}

impl Tally {
    const fn new() -> Self {
        Self { value: 0 }
    }
}

#[bombay::actor(
    sends = { values: Vec<EstablishedDelivery<TallyValue>> },
    error = TallyError,
)]
impl Tally {
    fn receive(&mut self, message: TallyMessage) -> BehaviorActed<Self> {
        match message {
            TallyMessage::Command(_) => {
                self.value = self.value.checked_add(1).ok_or(TallyError::Overflow)?;
                Ok(Actions::cont())
            }
            TallyMessage::Read(reply_to) => {
                Ok(Actions::cont().send_values(EstablishedDelivery::new(reply_to, self.value)))
            }
        }
    }
}

#[derive(TerminalProjection)]
enum ApplicationTerminal<R>
where
    R: BehaviorSettlements<Protocol: Protocol<Addr = MailAddr>, Ph = Never>,
{
    Root {
        origin: ActorOrigin<R>,
        terminal: ActorRetirement<R, Self>,
    },
}

type TallyRunError = RunError<TallyError>;

struct Api {
    tally: EstablishedRecipient<Tally>,
}

fn assert_application_stopped<R>(terminal: ApplicationTerminal<R>)
where
    R: BehaviorSettlements<Protocol: Protocol<Addr = MailAddr>, Ph = Never>,
    R::Settlements: ClassifySettlement,
{
    let ApplicationTerminal::Root {
        origin,
        terminal:
            ActorRetirement::Completed {
                behavior,
                settlements,
                control,
                user,
                descendants,
                completion,
            },
    } = terminal
    else {
        panic!("the application must preserve the root's completed terminal state")
    };
    assert_eq!(origin.address(), MailAddr::APPLICATION_ROOT);
    assert_eq!(origin.nonce(), None);
    drop(behavior);
    let settlement_status = settlements.settlement_status();
    assert_eq!(settlement_status, SettlementStatus::Accepted);
    assert!(control.is_empty());
    assert!(user.is_empty());
    assert!(descendants.is_empty());
    assert_eq!(completion, Completion::Stopped);
}

/// One full application lifecycle with `tells` tell-shaped commands. Returns
/// the elapsed steady send window inside the live application, excluding the
/// warm-up sends that precede it.
fn run_tell_spine(tells: u64) -> Result<Duration, TallyRunError> {
    let (steady, terminal) =
        Application::new(Tally::new().stop_on_shutdown()).run_with(|application| async move {
            let lifecycle = application.lifecycle();
            let interface = application.interface(Api {
                tally: application.root().established_recipient(),
            });
            let caller = interface
                .external::<TallyValue>()
                .expect("the tally customer is established");
            for index in 0..WARMUP_SENDS {
                caller
                    .send(
                        &interface.api().tally,
                        TallyMessage::Command(command(index)),
                    )
                    .await
                    .expect("the tally accepts the warm-up command");
            }
            let started = Instant::now();
            for index in WARMUP_SENDS..(WARMUP_SENDS + tells) {
                caller
                    .send(
                        &interface.api().tally,
                        TallyMessage::Command(command(index)),
                    )
                    .await
                    .expect("the tally accepts the command");
            }
            let steady = started.elapsed();
            assert_eq!(lifecycle.request_shutdown(), Ok(()));
            assert_eq!(lifecycle.termination().await, Ok(Exit::Normal));
            black_box(steady)
        })?;
    assert_application_stopped(terminal);
    Ok(steady)
}

/// One full application lifecycle with `rounds` command-plus-reply round
/// trips. Each round tells one command, then sends the exact reply
/// capability and receives the accumulated count. Returns the elapsed
/// steady window inside the live application.
fn run_reply_spine(rounds: u64) -> Result<Duration, TallyRunError> {
    let (window, terminal) =
        Application::new(Tally::new().stop_on_shutdown()).run_with(|application| async move {
            let lifecycle = application.lifecycle();
            let interface = application.interface(Api {
                tally: application.root().established_recipient(),
            });
            let mut caller = interface
                .external::<TallyValue>()
                .expect("the tally customer is established");
            let mut window = Duration::ZERO;
            for index in 0..(WARMUP_SENDS + rounds) {
                let started = Instant::now();
                caller
                    .send(
                        &interface.api().tally,
                        TallyMessage::Command(command(index)),
                    )
                    .await
                    .expect("the tally accepts the command");
                caller
                    .send(
                        &interface.api().tally,
                        TallyMessage::Read(caller.recipient()),
                    )
                    .await
                    .expect("the tally accepts the exact reply capability");
                let value = caller
                    .receive()
                    .await
                    .expect("the tally replies to the exact customer");
                assert_eq!(value.message, index + 1);
                let elapsed = started.elapsed();
                if index >= WARMUP_SENDS {
                    window += elapsed;
                }
            }
            assert_eq!(lifecycle.request_shutdown(), Ok(()));
            assert_eq!(lifecycle.termination().await, Ok(Exit::Normal));
            black_box(window)
        })?;
    assert_application_stopped(terminal);
    Ok(window)
}

/// Measure repeatedly, returning (minimum, median). See the entity benches
/// for the rationale.
fn repeat(mut operation: impl FnMut() -> Duration) -> (Duration, Duration) {
    let mut samples = [Duration::ZERO; REPETITIONS];
    for sample in &mut samples {
        *sample = operation();
    }
    samples.sort_unstable();
    (samples[0], samples[REPETITIONS / 2])
}

fn main() {
    println!("runs_per_sample={RUNS_PER_SAMPLE}");
    println!("tells_per_run={TELLS_PER_RUN}");
    println!("rounds_per_run={ROUNDS_PER_RUN}");
    println!("warmup_sends={WARMUP_SENDS}");
    println!("repetitions={REPETITIONS}");

    // Warm-up invocation outside the measured samples: the first application
    // boot pays platform lazy initialization the steady runs do not.
    run_tell_spine(TELLS_PER_RUN).expect("the tell spine completes");

    let (lifecycle_min, lifecycle_median) = repeat(|| {
        let started = Instant::now();
        for _ in 0..RUNS_PER_SAMPLE {
            run_tell_spine(TELLS_PER_RUN).expect("the tell spine completes");
        }
        started.elapsed() / RUNS_PER_SAMPLE
    });
    let (steady_min, steady_median) =
        repeat(|| run_tell_spine(TELLS_PER_RUN).expect("the tell spine completes"));
    let (reply_min, reply_median) =
        repeat(|| run_reply_spine(ROUNDS_PER_RUN).expect("the reply spine completes"));

    println!("lifecycle_tell_run_min={lifecycle_min:?}");
    println!("lifecycle_tell_run_median={lifecycle_median:?}");
    println!(
        "lifecycle_tell_ns_per_tell_median={}",
        lifecycle_median.as_nanos() / u128::from(TELLS_PER_RUN)
    );
    println!("steady_tell_window_min={steady_min:?}");
    println!("steady_tell_window_median={steady_median:?}");
    println!(
        "steady_tell_ns_per_tell_median={}",
        steady_median.as_nanos() / u128::from(TELLS_PER_RUN)
    );
    println!("reply_window_min={reply_min:?}");
    println!("reply_window_median={reply_median:?}");
    println!(
        "reply_ns_per_round_median={}",
        reply_median.as_nanos() / u128::from(ROUNDS_PER_RUN)
    );
}
