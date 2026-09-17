//! Semantic-equivalent mailbox-lane benchmark over the owning two-lane
//! channel.
//!
//! The retired old-runtime bench (`benches/mailbox.rs`, Criterion, recorded
//! in the `b0c212a` history) measured `tell` ≈ 5.7 ns and send+recv
//! ≈ 18.4 ns for a realistic ~40 B command, and `benches/channels.rs`
//! (ADR-0001) selected flume over the tokio channel for both `u64` and
//! ~40 B payloads. This suite keeps the equivalent lanes maintained over the
//! current owning transport: the Communication `mailbox_channel` user lane
//! (strong owner tell, weak reference tell), the full send+recv round trip,
//! and the tokio mpsc baseline at both payload sizes. The historical
//! figures are a labeled oracle baseline only and are never compared as
//! current numbers.

use std::hint::black_box;
use std::time::{Duration, Instant};

use behavior::Never;
use communication::{Config, Received, channel, mailbox_channel};
use tokio::runtime::Builder;
use tokio::sync::mpsc;

/// Fresh channels per measured batch, matching the retired bench's shape.
const BATCHES: u64 = 1_000;
/// Commands sent per fresh channel. With the bounded capacity below, no
/// batch fills its channel, isolating the move-into-slot cost of a tell.
const COMMANDS_PER_BATCH: u64 = 1_000;
/// Bounded user-lane capacity, matching the retired bench's bounded(1024).
const CAPACITY: usize = 1024;
/// Repetitions per scenario; the minimum and median are reported.
const REPETITIONS: usize = 7;

/// A realistically-sized actor command (~40 bytes) — a handful of fields,
/// closer to a real closed-enum `Msg` variant than a bare `u64`.
#[derive(Clone, Copy, Debug)]
#[allow(
    dead_code,
    reason = "the payload models a realistic command size; the lanes measure admission cost, not content"
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

fn value(index: u64) -> u64 {
    index
}

/// Owner tell: the strong user-lane `try_send` admission cost.
fn owner_tell<T: Debug>(payload: fn(u64) -> T) -> Duration {
    let started = Instant::now();
    for _ in 0..BATCHES {
        let (_control, sender, mut consumer) = channel::<Never, T>(Config::new(CAPACITY));
        for index in 0..COMMANDS_PER_BATCH {
            sender.try_send(payload(index)).expect("capacity available");
        }
        black_box(&sender);
        black_box(&mut consumer);
    }
    started.elapsed()
}

/// Reference tell: the weak `MailboxRef::try_send` admission cost, the shape
/// a typed `ActorRef` tell pays.
fn reference_tell<T: Debug>(payload: fn(u64) -> T) -> Duration {
    let started = Instant::now();
    for _ in 0..BATCHES {
        let (_control, owner, reference, mut consumer) =
            mailbox_channel::<Never, T>(Config::new(CAPACITY));
        for index in 0..COMMANDS_PER_BATCH {
            reference
                .try_send(payload(index))
                .expect("capacity available");
        }
        black_box(&owner);
        black_box(&reference);
        black_box(&mut consumer);
    }
    started.elapsed()
}

/// Tokio mpsc tell baseline: `try_send` into a bounded channel with spare
/// capacity.
fn tokio_tell<T: Debug>(payload: fn(u64) -> T) -> Duration {
    let started = Instant::now();
    for _ in 0..BATCHES {
        let (sender, mut receiver) = mpsc::channel::<T>(CAPACITY);
        for index in 0..COMMANDS_PER_BATCH {
            sender.try_send(payload(index)).expect("capacity available");
        }
        black_box(&sender);
        black_box(&mut receiver);
    }
    started.elapsed()
}

/// Full round trip over the two-lane channel: a producer task sends
/// `COMMANDS_PER_BATCH` commands and the consumer receives every one, on a
/// current-thread runtime.
fn owner_roundtrip<T: Debug + Send + 'static>(payload: fn(u64) -> T) -> Duration {
    let runtime = Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("current-thread runtime");
    runtime.block_on(async {
        let started = Instant::now();
        for _ in 0..BATCHES {
            let (_control, sender, mut consumer) = channel::<Never, T>(Config::new(CAPACITY));
            let producer = tokio::spawn(async move {
                for index in 0..COMMANDS_PER_BATCH {
                    sender
                        .send(payload(index))
                        .await
                        .expect("capacity available");
                }
            });
            for _ in 0..COMMANDS_PER_BATCH {
                match consumer.recv().await {
                    Some(Received::User(received)) => {
                        black_box(received);
                    }
                    Some(Received::Control(never)) => match never {},
                    Some(Received::UserLaneClosed) => {
                        panic!("the producer must outlive the measured round")
                    }
                    None => panic!("the channel must stay open during the measured round"),
                }
            }
            producer.await.expect("the producer completes");
        }
        started.elapsed()
    })
}

/// Tokio mpsc round-trip baseline: the same producer-consumer shape over the
/// tokio channel.
fn tokio_roundtrip<T: Debug + Send + 'static>(payload: fn(u64) -> T) -> Duration {
    let runtime = Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("current-thread runtime");
    runtime.block_on(async {
        let started = Instant::now();
        for _ in 0..BATCHES {
            let (sender, mut receiver) = mpsc::channel::<T>(CAPACITY);
            let producer = tokio::spawn(async move {
                for index in 0..COMMANDS_PER_BATCH {
                    sender
                        .send(payload(index))
                        .await
                        .expect("capacity available");
                }
            });
            for _ in 0..COMMANDS_PER_BATCH {
                let received = receiver.recv().await;
                black_box(received);
            }
            producer.await.expect("the producer completes");
        }
        started.elapsed()
    })
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

/// Emit a scenario's raw windows and its median cost per operation.
fn emit(name: &str, operations: u64, minimum: Duration, median: Duration) {
    println!("{name}_min={minimum:?}");
    println!("{name}_median={median:?}");
    println!(
        "{name}_ns_per_operation_median={}",
        median.as_nanos() / u128::from(operations)
    );
}

fn main() {
    println!("batches={BATCHES}");
    println!("commands_per_batch={COMMANDS_PER_BATCH}");
    println!("capacity={CAPACITY}");
    println!("repetitions={REPETITIONS}");
    let operations = BATCHES * COMMANDS_PER_BATCH;

    let (owner_tell_min, owner_tell_median) = repeat(|| owner_tell(command));
    emit(
        "owner_tell_command",
        operations,
        owner_tell_min,
        owner_tell_median,
    );

    let (reference_tell_min, reference_tell_median) = repeat(|| reference_tell(command));
    emit(
        "reference_tell_command",
        operations,
        reference_tell_min,
        reference_tell_median,
    );

    let (tokio_tell_command_min, tokio_tell_command_median) = repeat(|| tokio_tell(command));
    emit(
        "tokio_mpsc_tell_command",
        operations,
        tokio_tell_command_min,
        tokio_tell_command_median,
    );

    let (tokio_tell_u64_min, tokio_tell_u64_median) = repeat(|| tokio_tell(value));
    emit(
        "tokio_mpsc_tell_u64",
        operations,
        tokio_tell_u64_min,
        tokio_tell_u64_median,
    );

    let (roundtrip_command_min, roundtrip_command_median) = repeat(|| owner_roundtrip(command));
    emit(
        "owner_roundtrip_command",
        operations,
        roundtrip_command_min,
        roundtrip_command_median,
    );

    let (roundtrip_u64_min, roundtrip_u64_median) = repeat(|| owner_roundtrip(value));
    emit(
        "owner_roundtrip_u64",
        operations,
        roundtrip_u64_min,
        roundtrip_u64_median,
    );

    let (tokio_roundtrip_command_min, tokio_roundtrip_command_median) =
        repeat(|| tokio_roundtrip(command));
    emit(
        "tokio_mpsc_roundtrip_command",
        operations,
        tokio_roundtrip_command_min,
        tokio_roundtrip_command_median,
    );

    let (tokio_roundtrip_u64_min, tokio_roundtrip_u64_median) = repeat(|| tokio_roundtrip(value));
    emit(
        "tokio_mpsc_roundtrip_u64",
        operations,
        tokio_roundtrip_u64_min,
        tokio_roundtrip_u64_median,
    );
}
