//! Property tests P1-P5 for the deadline-arm loop model (paused clock,
//! current_thread — deterministic virtual time throughout).

use std::time::Duration;

use spike_274_loop::{ArmOrder, ModelActor, run_loop};
use tokio::sync::mpsc;
use tokio::time::{Instant, sleep};

#[derive(Debug, Clone, PartialEq, Eq)]
enum Event {
    Handled(u64),
    HandlerDone(u64),
    Fired,
}

/// Test actor: fixed or sliding deadline, per-message virtual work,
/// event log for order assertions.
struct Probe {
    log: Vec<Event>,
    fired_at: Vec<Instant>,
    /// `Some(at)` = fixed deadline; else `idle: Some(t)` = sliding.
    fixed: Option<Instant>,
    idle: Option<Duration>,
    last_activity: Instant,
    per_msg_work: Duration,
    stop_on_fire: bool,
}

impl Probe {
    fn new() -> Self {
        Self {
            log: Vec::new(),
            fired_at: Vec::new(),
            fixed: None,
            idle: None,
            last_activity: Instant::now(),
            per_msg_work: Duration::ZERO,
            stop_on_fire: false,
        }
    }
}

impl ModelActor for Probe {
    async fn handle(&mut self, msg: u64) {
        self.log.push(Event::Handled(msg));
        if self.per_msg_work > Duration::ZERO {
            sleep(self.per_msg_work).await;
        }
        self.last_activity = Instant::now();
        self.log.push(Event::HandlerDone(msg));
    }

    fn next_deadline(&self) -> Option<Instant> {
        match (self.fixed, self.idle) {
            (Some(at), _) => Some(at),
            (None, Some(t)) => Some(self.last_activity + t),
            (None, None) => None,
        }
    }

    async fn on_deadline(&mut self) -> bool {
        self.log.push(Event::Fired);
        self.fired_at.push(Instant::now());
        // Deliberately does NOT clear `fixed` — models the pathological
        // hook that leaves its deadline unchanged (P3's spin hazard).
        !self.stop_on_fire
    }
}

fn msgs_before_fire(log: &[Event]) -> usize {
    log.iter()
        .take_while(|e| **e != Event::Fired)
        .filter(|e| matches!(e, Event::Handled(_)))
        .count()
}

/// P1a: arm ABOVE the mailbox — a due deadline fires promptly even though
/// the mailbox never empties. Handler costs 1 ms virtual each; deadline at
/// +10 ms; 50 messages pre-queued. Fire lands after ~10 handled, not 50.
#[tokio::test(start_paused = true)]
async fn p1a_above_mailbox_fires_promptly_under_saturation() {
    let (tx, mut rx) = mpsc::channel(64);
    for i in 0..50 {
        tx.try_send(i).expect("pre-queue");
    }
    drop(tx);
    let mut actor = Probe::new();
    actor.fixed = Some(Instant::now() + Duration::from_millis(10));
    actor.per_msg_work = Duration::from_millis(1);
    actor.stop_on_fire = true;
    run_loop(&mut actor, &mut rx, ArmOrder::AboveMailbox).await;

    let before = msgs_before_fire(&actor.log);
    assert!(
        (9..=11).contains(&before),
        "deadline must fire when due (~10 messages in), not after the \
         backlog; fired after {before}"
    );
}

/// P1b (counter-model): arm BELOW the mailbox — biased select starves the
/// deadline until the backlog is fully drained. Same setup as P1a; the
/// fire lands only after all 50. This is what makes the arm placement a
/// structural requirement, not a style choice.
#[tokio::test(start_paused = true)]
async fn p1b_below_mailbox_starves_until_backlog_drains() {
    let (tx, mut rx) = mpsc::channel(64);
    for i in 0..50 {
        tx.try_send(i).expect("pre-queue");
    }
    let mut actor = Probe::new();
    actor.fixed = Some(Instant::now() + Duration::from_millis(10));
    actor.per_msg_work = Duration::from_millis(1);
    actor.stop_on_fire = true;
    let driver = async {
        run_loop(&mut actor, &mut rx, ArmOrder::BelowMailbox).await;
    };
    driver.await;
    drop(tx);

    assert_eq!(
        msgs_before_fire(&actor.log),
        50,
        "below the mailbox arm, the fire cannot preempt a saturated backlog"
    );
}

/// P2: no deadline declared — the arm is disabled: no fire, no spin, the
/// loop drains messages and terminates on channel close.
#[tokio::test(start_paused = true)]
async fn p2_none_deadline_never_fires() {
    let (tx, mut rx) = mpsc::channel(8);
    for i in 0..5 {
        tx.try_send(i).expect("queue");
    }
    drop(tx);
    let mut actor = Probe::new();
    run_loop(&mut actor, &mut rx, ArmOrder::AboveMailbox).await;

    assert_eq!(actor.log.iter().filter(|e| **e == Event::Fired).count(), 0);
    assert_eq!(msgs_before_fire(&actor.log), 5, "all messages handled");
}

/// P3: fires-once-per-value — a hook that leaves its (already-due)
/// deadline unchanged fires exactly once and the loop keeps serving
/// messages afterwards. Without the guard this is a busy loop.
#[tokio::test(start_paused = true)]
async fn p3_unchanged_deadline_fires_exactly_once() {
    let (tx, mut rx) = mpsc::channel(8);
    let mut actor = Probe::new();
    actor.fixed = Some(Instant::now()); // due immediately, never cleared
    let driver = tokio::spawn(async move {
        run_loop(&mut actor, &mut rx, ArmOrder::AboveMailbox).await;
        actor
    });
    // The loop must remain live for ordinary traffic after the one fire.
    tx.send(7).await.expect("loop still serving");
    drop(tx);
    let actor = driver.await.expect("loop task");

    assert_eq!(
        actor.log.iter().filter(|e| **e == Event::Fired).count(),
        1,
        "fires-once guard: one fire per deadline value"
    );
    assert!(
        actor.log.contains(&Event::Handled(7)),
        "loop still serves messages after the guarded fire"
    );
}

/// P4: sliding deadline (receive-timeout emulation) — each handled message
/// defers the fire; the fire lands exactly at last_activity + T.
#[tokio::test(start_paused = true)]
async fn p4_sliding_deadline_resets_on_activity() {
    let (tx, mut rx) = mpsc::channel(8);
    let mut actor = Probe::new();
    actor.idle = Some(Duration::from_millis(20));
    actor.stop_on_fire = true;
    let start = Instant::now();
    let driver = tokio::spawn(async move {
        run_loop(&mut actor, &mut rx, ArmOrder::AboveMailbox).await;
        actor
    });
    // Two touches inside the window, then silence.
    sleep(Duration::from_millis(15)).await;
    tx.send(1).await.expect("send 1"); // t=15, defers fire to 35
    sleep(Duration::from_millis(15)).await;
    tx.send(2).await.expect("send 2"); // t=30, defers fire to 50
    let actor = driver.await.expect("loop task");

    assert_eq!(msgs_before_fire(&actor.log), 2, "both touches handled first");
    let fired = actor.fired_at.first().expect("fired once");
    assert_eq!(
        fired.duration_since(start),
        Duration::from_millis(50),
        "fire lands at last_activity + T exactly (virtual clock)"
    );
}

/// P5: run-to-completion — a deadline coming due MID-handler is observed
/// only after the handler returns; the hook never interleaves.
#[tokio::test(start_paused = true)]
async fn p5_deadline_never_fires_mid_handler() {
    let (tx, mut rx) = mpsc::channel(8);
    tx.try_send(1).expect("queue");
    drop(tx);
    let mut actor = Probe::new();
    actor.fixed = Some(Instant::now() + Duration::from_millis(10));
    actor.per_msg_work = Duration::from_millis(30); // due mid-handler
    actor.stop_on_fire = true;
    run_loop(&mut actor, &mut rx, ArmOrder::AboveMailbox).await;

    assert_eq!(
        actor.log,
        vec![Event::Handled(1), Event::HandlerDone(1), Event::Fired],
        "expiry mid-handler is delivered only at the step boundary"
    );
}
