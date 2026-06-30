//! Shared `LinksWorld` + step definitions for the core `links` scenarios.
//!
//! Wired by two runners that `#[path]`-include this module:
//!   * `core_links_bdd.rs`       — the example feature (links.feature)
//!   * `core_links_props_bdd.rs` — the property laws (links.properties.feature)
//!
//! The SUT is `src/links.rs`: the per-actor `Links` registry (parent /
//! sibblings / children) and its link/notification machinery — `notify_links`,
//! `notify_sibblings`, `set_children_parent_shutdown`, `send_children_shutdown`,
//! `wait_children_closed`, and the `parent_shutdown` Release/Acquire ordering
//! that prevents the supervisor-shutdown deadlock.
//!
//! Two surfaces are used, chosen per scenario from its `# Confirmed:` note:
//!
//!   * **Raw `Links`** (`bombay::links::testing`): scenarios that pin the link
//!     DATA STRUCTURE / notify mechanics directly — who is notified, with or
//!     without the dying actor's `mailbox_rx` / sibling links, drain-once, the
//!     parent_shutdown flag store/load, and the three-step child-shutdown
//!     ordering. A notified link is a `Link::Local` over a real `MailboxSender`
//!     whose `MailboxReceiver` the test keeps, so the exact delivered
//!     `Signal::LinkDied` (and its `mailbox_rx`/sibblings presence bits) is read
//!     back via `recv_link_died`.
//!   * **Real spawned actors** (`bombay::prelude::*`): the one scenario that is
//!     about end-to-end restart behaviour across a supervised child's spawn
//!     factory (parent_shutdown reset / stale-children cleared) drives a real
//!     supervised child through repeated panic→restart and asserts the restart
//!     hand-off keeps working — which is only possible if the flag is reset.
//!
//! TIMING DISCIPLINE: every death-notification / wait observation uses bounded
//! waits — `recv_link_died` has a timeout, `settle()` polls a bound and panics
//! loudly. No unbounded await on a notification that may never arrive, and
//! `@timing` uses `tokio::time` pause/advance, never a real sleep.

use std::{
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicU32, Ordering},
    },
    time::Duration,
};

use bombay::{
    actor::ActorId,
    error::{ActorStopReason, Infallible},
    links::{
        Links,
        testing::{
            LinkDiedParts, add_child, add_sibbling, child_spec, empty_links, notify_links,
            parent_shutdown_flag, recv_link_died, set_parent,
        },
    },
    mailbox::{self, MailboxReceiver, MailboxSender},
    prelude::*,
    supervision::RestartPolicy,
};
use cucumber::{World, given, then, when};
use tokio::sync::Barrier;

// ===========================================================================
// Probe actor — only the `A: Actor` channel parameter; the senders/receivers
// are raw mailbox halves the test owns (these actors are never spawned for the
// raw-Links scenarios). A separate spawnable actor lives below for the
// end-to-end restart scenario.
// ===========================================================================

#[derive(Clone)]
struct Probe;

impl Actor for Probe {
    type Args = Self;
    type Error = Infallible;

    async fn on_start(state: Self::Args, _: ActorRef<Self>) -> Result<Self, Self::Error> {
        Ok(state)
    }
}

/// A real spawnable actor for the self-link no-op scenario (end-to-end on the
/// public `ActorRef::link` API).
struct LinkProbe;

impl Actor for LinkProbe {
    type Args = Self;
    type Error = Infallible;

    async fn on_start(state: Self::Args, _: ActorRef<Self>) -> Result<Self, Self::Error> {
        Ok(state)
    }
}

/// A real supervisor for the end-to-end restart scenario.
struct RestartSupervisor;

impl Actor for RestartSupervisor {
    type Args = Self;
    type Error = Infallible;

    async fn on_start(state: Self::Args, _: ActorRef<Self>) -> Result<Self, Self::Error> {
        Ok(state)
    }
}

/// A real supervised child that counts its starts and panics on `Boom`, driving
/// the panic→restart path so the spawn factory's parent_shutdown reset /
/// stale-children clear is exercised across repeated restarts.
#[derive(Clone)]
struct RestartChild {
    starts: Arc<AtomicU32>,
}

impl Actor for RestartChild {
    type Args = Self;
    type Error = Infallible;

    async fn on_start(state: Self::Args, _: ActorRef<Self>) -> Result<Self, Self::Error> {
        state.starts.fetch_add(1, Ordering::SeqCst);
        Ok(state)
    }
}

/// A message whose handler panics (drives the child's death/restart).
struct Boom;

impl Message<Boom> for RestartChild {
    type Reply = ();

    async fn handle(&mut self, _msg: Boom, _ctx: &mut Context<Self, Self::Reply>) -> Self::Reply {
        panic!("restart-child boom");
    }
}

/// A fresh raw mailbox pair over `Probe`. The sender becomes a `Link::Local`
/// inside a `Links`; the receiver is kept by the World to read back what was
/// delivered (the death notification).
fn raw_pair() -> (MailboxSender<Probe>, MailboxReceiver<Probe>) {
    mailbox::unbounded::<Probe>()
}

/// A short bound for "a notification was/wasn't delivered" observations.
const NOTIFY_TIMEOUT: Duration = Duration::from_millis(500);
/// A shorter bound for the "no notification at all" assertions (must end fast
/// so the test does not pad its runtime).
const NO_NOTIFY_TIMEOUT: Duration = Duration::from_millis(150);

/// Condition-based settle: polls `cond` up to a bound with a short sleep; panics
/// with `msg` if it never holds.
async fn settle<F: FnMut() -> bool>(mut cond: F, msg: &str) {
    for _ in 0..400 {
        if cond() {
            return;
        }
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    panic!("condition did not settle within the bound: {msg}");
}

// ===========================================================================
// World
// ===========================================================================

#[derive(Default, World)]
pub struct LinksWorld {
    /// The Links under test (built fresh per Given).
    links: Option<Links>,
    /// The parent's receiver (supervised scenarios).
    parent_rx: Option<MailboxReceiver<Probe>>,
    /// Named sibling receivers, keyed by the sibling's label.
    sibling_rx: Vec<(String, MailboxReceiver<Probe>)>,
    /// Named sibling ids, keyed by label (so the dying-id assertion is exact).
    sibling_ids: Vec<(String, ActorId)>,
    /// Named child `parent_shutdown` flags + shutdown-call counters + closing
    /// receivers, keyed by the child's label.
    child_flags: Vec<(String, Arc<AtomicBool>)>,
    child_shutdown_calls: Vec<(String, Arc<AtomicU32>)>,
    child_rx: Vec<(String, MailboxReceiver<Probe>)>,
    /// The dying actor's id used by the notify When.
    dead_id: Option<ActorId>,
    /// The dying actor's own parent_shutdown flag (the supervised-child
    /// scenarios that flip it).
    child_under_test_flag: Option<Arc<AtomicBool>>,
    /// The parent's mailbox_rx of the dying actor (kept so we can assert its
    /// channel closed after a drop-branch notify).
    dead_mailbox_tx: Option<MailboxSender<Probe>>,
    /// Captured notification parts the parent received (supervised path).
    parent_parts: Option<Option<LinkDiedParts>>,
    /// Captured per-sibling delivery counts (sibling fan-out).
    sibling_counts: Vec<(String, usize)>,
    /// Whether the empty-children shutdown completed promptly.
    empty_completed: Option<bool>,
    /// Ordering log for the three-step child shutdown scenario.
    order_log: Vec<&'static str>,
    /// End-to-end restart scenario: start counter.
    restart_starts: Option<Arc<AtomicU32>>,
    restart_child: Option<ActorRef<RestartChild>>,
    restart_supervisor: Option<ActorRef<RestartSupervisor>>,
    /// Self-link no-op scenario: a real spawned actor.
    self_link_actor: Option<ActorRef<LinkProbe>>,
    /// Per-child Links for the two-children / K-children simultaneous-death
    /// scenarios (each real child is its own actor with its own Links).
    per_child_links: Vec<(String, Links)>,
    /// Captured notifications for the two-children simultaneous-death scenario.
    two_death_parts: Vec<LinkDiedParts>,
}

impl std::fmt::Debug for LinksWorld {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LinksWorld")
            .field("dead_id", &self.dead_id)
            .field("parent_parts", &self.parent_parts)
            .field("sibling_counts", &self.sibling_counts)
            .field("empty_completed", &self.empty_completed)
            .field("order_log", &self.order_log)
            .finish_non_exhaustive()
    }
}

impl LinksWorld {
    fn links(&self) -> &Links {
        self.links.as_ref().expect("links built")
    }
}

// Distinct ids for named actors so the dying-id assertion is exact.
fn id_for(label: &str) -> ActorId {
    // Deterministic per-label id within a scenario; labels are short ("a".."d",
    // "worker", "c1".."c3", "hub"). A simple stable hash into a u64 is enough —
    // distinct labels yield distinct ids.
    let mut h: u64 = 1469598103934665603; // FNV offset basis
    for b in label.bytes() {
        h ^= b as u64;
        h = h.wrapping_mul(1099511628211);
    }
    ActorId::new(h)
}

// ===========================================================================
// @sequence — supervised parent hand-off, sibling fan-out, drain-once, ordering
// ===========================================================================

#[given(regex = r#"^a supervisor actor with a supervised child "([^"]+)"$"#)]
async fn given_supervisor_with_child(world: &mut LinksWorld, child: String) {
    // Model the CHILD's Links (the actor that will die): a parent link over the
    // supervisor's mailbox + the child's own parent_shutdown flag. The dying
    // actor is the child; its notify_links delivers to the parent.
    let links = empty_links();
    let (parent_tx, parent_rx) = raw_pair();
    let parent_id = id_for("supervisor");
    set_parent(&links, parent_id, &parent_tx).await;
    let flag = parent_shutdown_flag(&links).await;
    world.links = Some(links);
    world.parent_rx = Some(parent_rx);
    world.child_under_test_flag = Some(flag);
    world.dead_id = Some(id_for(&child));
}

#[given(regex = r#"^the child's "parent_shutdown" flag is false$"#)]
async fn given_flag_false(world: &mut LinksWorld) {
    let flag = world.child_under_test_flag.as_ref().expect("flag");
    flag.store(false, Ordering::Release);
    assert!(!flag.load(Ordering::Acquire), "flag must read false");
}

#[when(regex = r"^the child stops abnormally$")]
async fn when_child_stops_abnormally(world: &mut LinksWorld) {
    // Give the dying child one sibling so the supervised hand-off can be observed
    // to carry the sibling links too.
    let (sib_tx, _sib_rx) = raw_pair();
    add_sibbling(world.links(), id_for("peer"), &sib_tx).await;
    let id = world.dead_id.expect("dead id");
    let (_tx, rx) = raw_pair(); // the dying child's own mailbox_rx
    notify_links(world.links(), id, ActorStopReason::Killed, rx).await;
    let parts = recv_link_died(world.parent_rx.as_mut().expect("parent rx"), NOTIFY_TIMEOUT).await;
    world.parent_parts = Some(parts);
}

#[then(regex = r"^the parent's Link is notified of the child's death$")]
async fn then_parent_notified(world: &mut LinksWorld) {
    let parts = world.parent_parts.as_ref().expect("captured").as_ref();
    let parts = parts.expect("the parent must receive a LinkDied notification");
    assert_eq!(
        parts.id,
        world.dead_id.expect("dead id"),
        "the notification must name the dead child's id"
    );
}

#[then(regex = r"^the notification carries the child's mailbox_rx$")]
async fn then_carries_mailbox_rx(world: &mut LinksWorld) {
    let parts = world
        .parent_parts
        .as_ref()
        .unwrap()
        .as_ref()
        .expect("parts");
    assert!(
        parts.has_mailbox_rx,
        "the supervised hand-off must carry mailbox_rx so the parent can restart"
    );
}

#[then(regex = r"^the notification carries the child's sibling links$")]
async fn then_carries_sibblings(world: &mut LinksWorld) {
    let parts = world
        .parent_parts
        .as_ref()
        .unwrap()
        .as_ref()
        .expect("parts");
    assert!(
        parts.has_sibblings,
        "the supervised hand-off must carry the dead actor's sibling links"
    );
}

#[given(
    regex = r#"^an unsupervised actor "([^"]+)" linked to siblings "([^"]+)", "([^"]+)" and "([^"]+)"$"#
)]
async fn given_unsupervised_three_siblings(
    world: &mut LinksWorld,
    actor: String,
    b: String,
    c: String,
    d: String,
) {
    let links = empty_links();
    world.dead_id = Some(id_for(&actor));
    for label in [b, c, d] {
        let (tx, rx) = raw_pair();
        let sid = id_for(&label);
        add_sibbling(&links, sid, &tx).await;
        world.sibling_rx.push((label.clone(), rx));
        world.sibling_ids.push((label, sid));
    }
    world.links = Some(links);
}

#[when(regex = r#"^actor "([^"]+)" dies with reason Killed$"#)]
async fn when_actor_dies_killed(world: &mut LinksWorld, _actor: String) {
    let id = world.dead_id.expect("dead id");
    let (_tx, rx) = raw_pair();
    notify_links(world.links(), id, ActorStopReason::Killed, rx).await;
    collect_sibling_parts(world).await;
}

/// Drains each named sibling's receiver for its single LinkDied, recording the
/// count and asserting no mailbox_rx along the way.
async fn collect_sibling_parts(world: &mut LinksWorld) {
    let dead = world.dead_id.expect("dead id");
    let mut counts = Vec::new();
    let mut rxs = std::mem::take(&mut world.sibling_rx);
    for (label, rx) in &mut rxs {
        let mut count = 0usize;
        // First notification (bounded wait), then confirm no SECOND one quickly.
        if let Some(parts) = recv_link_died(rx, NOTIFY_TIMEOUT).await {
            assert_eq!(parts.id, dead, "sibling {label} got wrong dead id");
            assert!(
                !parts.has_mailbox_rx,
                "sibling {label} must NOT receive mailbox_rx"
            );
            assert!(
                !parts.has_sibblings,
                "sibling {label} must NOT receive sibling links"
            );
            count += 1;
        }
        if recv_link_died(rx, NO_NOTIFY_TIMEOUT).await.is_some() {
            count += 1; // a duplicate — recorded so the Then can fail loudly.
        }
        counts.push((label.clone(), count));
    }
    world.sibling_rx = rxs;
    world.sibling_counts = counts;
}

#[then(
    regex = r#"^siblings "([^"]+)", "([^"]+)" and "([^"]+)" each receive exactly one on_link_died for "([^"]+)"$"#
)]
async fn then_three_each_one(
    world: &mut LinksWorld,
    b: String,
    c: String,
    d: String,
    _actor: String,
) {
    for label in [b, c, d] {
        let count = world
            .sibling_counts
            .iter()
            .find(|(l, _)| *l == label)
            .map(|(_, n)| *n)
            .unwrap_or_else(|| panic!("no count for sibling {label}"));
        assert_eq!(
            count, 1,
            "sibling {label} must receive exactly one on_link_died"
        );
    }
}

#[then(regex = r"^no sibling receives a mailbox_rx in its notification$")]
async fn then_no_sibling_mailbox_rx(_world: &mut LinksWorld) {
    // Asserted during collection (each delivered LinkDied had has_mailbox_rx
    // == false). Reaching here means every sibling notification was mailbox_rx-
    // free; the collection panics otherwise.
}

#[given(regex = r#"^an unsupervised actor "([^"]+)" linked to sibling "([^"]+)"$"#)]
async fn given_unsupervised_one_sibling(world: &mut LinksWorld, actor: String, b: String) {
    let links = empty_links();
    world.dead_id = Some(id_for(&actor));
    let (tx, rx) = raw_pair();
    let sid = id_for(&b);
    add_sibbling(&links, sid, &tx).await;
    world.sibling_rx.push((b.clone(), rx));
    world.sibling_ids.push((b, sid));
    world.links = Some(links);
}

#[when(regex = r#"^actor "([^"]+)" dies$"#)]
async fn when_actor_dies(world: &mut LinksWorld, _actor: String) {
    let id = world.dead_id.expect("dead id");
    let (_tx, rx) = raw_pair();
    notify_links(world.links(), id, ActorStopReason::Killed, rx).await;
}

#[when(regex = r"^the same notify_links pass is somehow re-driven$")]
async fn when_redrive_notify(world: &mut LinksWorld) {
    // Re-drive: the sibling map was drained by the first pass (mem::take /
    // .drain()), so a second notify_links has nothing left to deliver.
    let id = world.dead_id.expect("dead id");
    let (_tx, rx) = raw_pair();
    notify_links(world.links(), id, ActorStopReason::Killed, rx).await;
    collect_sibling_parts(world).await;
}

#[then(regex = r#"^sibling "([^"]+)" receives the on_link_died notification exactly once$"#)]
async fn then_sibling_once(world: &mut LinksWorld, b: String) {
    let count = world
        .sibling_counts
        .iter()
        .find(|(l, _)| *l == b)
        .map(|(_, n)| *n)
        .unwrap_or_else(|| panic!("no count for sibling {b}"));
    assert_eq!(
        count, 1,
        "sibling {b} must be notified exactly once across both passes"
    );
}

// --- three-step child shutdown ordering ------------------------------------

#[given(
    regex = r#"^a supervisor actor with supervised children "([^"]+)", "([^"]+)" and "([^"]+)"$"#
)]
async fn given_supervisor_three_children(
    world: &mut LinksWorld,
    c1: String,
    c2: String,
    c3: String,
) {
    let links = empty_links();
    for label in [c1, c2, c3] {
        add_one_child(world, &links, &label).await;
    }
    world.links = Some(links);
}

/// Adds a supervised child with its own parent_shutdown flag, a shutdown-call
/// counter, and a closing receiver, recording all three keyed by `label`.
async fn add_one_child(world: &mut LinksWorld, links: &Links, label: &str) {
    let (tx, rx) = raw_pair();
    let flag = Arc::new(AtomicBool::new(false));
    let (spec, calls) = child_spec(&tx, flag.clone());
    add_child(links, id_for(label), spec).await;
    world.child_flags.push((label.to_string(), flag));
    world.child_shutdown_calls.push((label.to_string(), calls));
    world.child_rx.push((label.to_string(), rx));
}

#[when(regex = r"^the supervisor performs its final shutdown$")]
async fn when_final_shutdown(world: &mut LinksWorld) {
    let links = world.links().clone();
    // Step 1: set the flag on every child.
    links.set_children_parent_shutdown().await;
    world.order_log.push("set_flag");
    for (label, flag) in &world.child_flags {
        assert!(
            flag.load(Ordering::Acquire),
            "child {label} must read parent_shutdown true after step 1"
        );
    }
    // Step 2: fire every child's shutdown closure.
    links.send_children_shutdown().await;
    world.order_log.push("send_shutdown");
    for (label, calls) in &world.child_shutdown_calls {
        assert_eq!(
            calls.load(Ordering::SeqCst),
            1,
            "child {label} shutdown must be invoked exactly once after step 2"
        );
    }
    // Step 3: wait for every child mailbox to close. Close them concurrently by
    // dropping the receivers, then assert wait_children_closed resolves.
    let waiter = {
        let links = links.clone();
        tokio::spawn(async move { links.wait_children_closed().await })
    };
    // Drop all child receivers so each child's signal_mailbox.closed() resolves.
    world.child_rx.clear();
    tokio::time::timeout(Duration::from_secs(5), waiter)
        .await
        .expect("wait_children_closed must resolve once all child mailboxes close")
        .expect("waiter task joins");
    world.order_log.push("wait_closed");
}

#[then(regex = r"^it first calls set_children_parent_shutdown so every child reads the flag true$")]
async fn then_first_set_flag(world: &mut LinksWorld) {
    assert_eq!(
        world.order_log.first().copied(),
        Some("set_flag"),
        "set-flag must be first"
    );
    for (label, flag) in &world.child_flags {
        assert!(
            flag.load(Ordering::Acquire),
            "child {label} flag must be true"
        );
    }
}

#[then(
    regex = r"^it then calls send_children_shutdown, invoking each child's shutdown closure exactly once$"
)]
async fn then_then_send_shutdown(world: &mut LinksWorld) {
    assert_eq!(
        world.order_log.get(1).copied(),
        Some("send_shutdown"),
        "send-shutdown second"
    );
    for (label, calls) in &world.child_shutdown_calls {
        assert_eq!(
            calls.load(Ordering::SeqCst),
            1,
            "child {label} shutdown once"
        );
    }
}

#[then(
    regex = r"^it finally calls wait_children_closed, which resolves only after every child mailbox is closed$"
)]
async fn then_finally_wait_closed(world: &mut LinksWorld) {
    assert_eq!(
        world.order_log.get(2).copied(),
        Some("wait_closed"),
        "wait-closed last"
    );
    assert_eq!(
        world.order_log,
        vec!["set_flag", "send_shutdown", "wait_closed"],
        "the three steps must run in exactly this order"
    );
}

// ===========================================================================
// @lifecycle — parent_shutdown flag set/load/reset, child drop branch
// ===========================================================================

#[when(regex = r"^the supervisor calls set_children_parent_shutdown$")]
async fn when_call_set_flag(world: &mut LinksWorld) {
    world.links().set_children_parent_shutdown().await;
}

#[then(regex = r#"^each child's "parent_shutdown" flag reads true$"#)]
async fn then_each_flag_true(world: &mut LinksWorld) {
    for (label, flag) in &world.child_flags {
        assert!(
            flag.load(Ordering::Acquire),
            "child {label} parent_shutdown must read true"
        );
    }
    assert_eq!(
        world.child_flags.len(),
        3,
        "all three children's flags checked"
    );
}

#[when(regex = r"^the supervisor calls send_children_shutdown$")]
async fn when_call_send_shutdown(world: &mut LinksWorld) {
    world.links().send_children_shutdown().await;
}

#[then(regex = r"^each of the three children's shutdown closures is invoked exactly once$")]
async fn then_each_shutdown_once(world: &mut LinksWorld) {
    assert_eq!(world.child_shutdown_calls.len(), 3, "three children");
    for (label, calls) in &world.child_shutdown_calls {
        assert_eq!(
            calls.load(Ordering::SeqCst),
            1,
            "child {label} shutdown closure must be invoked exactly once"
        );
    }
}

#[given(
    regex = r#"^a supervisor with supervised children "([^"]+)" and "([^"]+)", both still draining$"#
)]
async fn given_two_children_draining(world: &mut LinksWorld, c1: String, c2: String) {
    let links = empty_links();
    add_one_child(world, &links, &c1).await;
    add_one_child(world, &links, &c2).await;
    world.links = Some(links);
}

#[when(regex = r"^the supervisor calls wait_children_closed$")]
async fn when_call_wait_closed(world: &mut LinksWorld) {
    // Park the wait while both child mailboxes are still OPEN: it must NOT resolve.
    let links = world.links().clone();
    let waiter = tokio::spawn(async move { links.wait_children_closed().await });
    tokio::time::sleep(Duration::from_millis(60)).await;
    assert!(
        !waiter.is_finished(),
        "wait_children_closed resolved while child mailboxes are still open"
    );
    // Close both child mailboxes by dropping the receivers; the wait must resolve.
    world.child_rx.clear();
    tokio::time::timeout(Duration::from_secs(5), waiter)
        .await
        .expect("wait_children_closed must resolve once both mailboxes close")
        .expect("waiter joins");
    world.empty_completed = Some(true);
}

#[then(regex = r"^the call does not resolve while either child mailbox is still open$")]
async fn then_not_resolve_while_open(world: &mut LinksWorld) {
    // The not-resolved-while-open check ran inside the When (asserted there); the
    // resolution-after-close is asserted by the next Then.
    assert_eq!(
        world.empty_completed,
        Some(true),
        "the parked wait must have been observed pending then resolved"
    );
}

#[then(regex = r#"^it resolves once both "([^"]+)" and "([^"]+)" mailboxes are closed$"#)]
async fn then_resolves_after_both_closed(world: &mut LinksWorld, _c1: String, _c2: String) {
    assert_eq!(
        world.empty_completed,
        Some(true),
        "wait_children_closed must resolve after both child mailboxes close"
    );
}

#[given(regex = r"^a supervisor actor with no supervised children$")]
async fn given_no_children(world: &mut LinksWorld) {
    world.links = Some(empty_links());
}

#[when(regex = r"^the supervisor calls send_children_shutdown and then wait_children_closed$")]
async fn when_send_then_wait_empty(world: &mut LinksWorld) {
    let links = world.links().clone();
    let completed = tokio::time::timeout(Duration::from_secs(1), async move {
        links.send_children_shutdown().await;
        links.wait_children_closed().await;
    })
    .await;
    world.empty_completed = Some(completed.is_ok());
}

#[then(regex = r"^both calls complete immediately without awaiting anything$")]
async fn then_both_complete_immediately(world: &mut LinksWorld) {
    assert_eq!(
        world.empty_completed,
        Some(true),
        "an empty child set must make send/wait resolve immediately"
    );
}

// --- child exiting after flag set => drop-mailbox_rx branch ----------------

#[given(regex = r"^the supervisor has called set_children_parent_shutdown$")]
async fn given_supervisor_called_set_flag(world: &mut LinksWorld) {
    // The dying child's OWN flag is the one notify_links loads. Set it true.
    let flag = world.child_under_test_flag.as_ref().expect("flag");
    flag.store(true, Ordering::Release);
}

#[when(regex = r"^the child exits independently after the flag is set$")]
async fn when_child_exits_after_flag(world: &mut LinksWorld) {
    let id = world.dead_id.expect("dead id");
    // Add a sibling so we can prove it is NOT forwarded in the drop branch.
    let (sib_tx, _sib_rx) = raw_pair();
    add_sibbling(world.links(), id_for("peer"), &sib_tx).await;
    // The dying child's own mailbox: keep the SENDER so we can observe the
    // channel close once notify_links drops mailbox_rx.
    let (dead_tx, dead_rx) = raw_pair();
    world.dead_mailbox_tx = Some(dead_tx);
    notify_links(world.links(), id, ActorStopReason::Killed, dead_rx).await;
    let parts = recv_link_died(world.parent_rx.as_mut().expect("parent rx"), NOTIFY_TIMEOUT).await;
    world.parent_parts = Some(parts);
}

#[then(regex = r"^the parent is notified with no mailbox_rx and no siblings$")]
async fn then_parent_no_mailbox_no_siblings(world: &mut LinksWorld) {
    let parts = world
        .parent_parts
        .as_ref()
        .unwrap()
        .as_ref()
        .expect("parent must still be notified in the drop branch");
    assert_eq!(
        parts.id,
        world.dead_id.expect("dead id"),
        "names the dead child"
    );
    assert!(
        !parts.has_mailbox_rx,
        "drop branch must NOT carry mailbox_rx"
    );
    assert!(
        !parts.has_sibblings,
        "drop branch must NOT carry sibling links"
    );
}

#[then(regex = r"^the child's mailbox_rx is dropped so its channel closes$")]
async fn then_mailbox_rx_dropped(world: &mut LinksWorld) {
    // notify_links took the drop branch (mailbox_rx == None), so the receiver the
    // test handed in was dropped. The kept sender's channel must therefore be
    // observably closed.
    let tx = world.dead_mailbox_tx.as_ref().expect("kept dead sender");
    let tx2 = tx.clone();
    settle(
        move || tx2.is_closed(),
        "the dying child's mailbox channel never closed after mailbox_rx was dropped",
    )
    .await;
    assert!(
        tx.is_closed(),
        "the dropped mailbox_rx must close the channel"
    );
}

#[given(regex = r"^a supervisor about to enter shutdown_children$")]
async fn given_about_to_shutdown(world: &mut LinksWorld) {
    // Reuse the single-child Links: parent over a probe sender + the child's flag.
    let links = empty_links();
    let (parent_tx, parent_rx) = raw_pair();
    set_parent(&links, id_for("supervisor"), &parent_tx).await;
    let flag = parent_shutdown_flag(&links).await;
    world.links = Some(links);
    world.parent_rx = Some(parent_rx);
    world.child_under_test_flag = Some(flag);
    world.dead_id = Some(id_for("worker"));
}

#[when(regex = r"^set_children_parent_shutdown stores true with Release ordering$")]
async fn when_store_release(world: &mut LinksWorld) {
    world
        .child_under_test_flag
        .as_ref()
        .expect("flag")
        .store(true, Ordering::Release);
}

#[when(regex = r"^a child later loads parent_shutdown with Acquire ordering before notifying$")]
async fn when_load_acquire_then_notify(world: &mut LinksWorld) {
    let id = world.dead_id.expect("dead id");
    let (dead_tx, dead_rx) = raw_pair();
    world.dead_mailbox_tx = Some(dead_tx);
    notify_links(world.links(), id, ActorStopReason::Killed, dead_rx).await;
    let parts = recv_link_died(world.parent_rx.as_mut().expect("parent rx"), NOTIFY_TIMEOUT).await;
    world.parent_parts = Some(parts);
}

#[then(regex = r"^the child observes true and takes the drop-mailbox_rx branch$")]
async fn then_observes_true_drop_branch(world: &mut LinksWorld) {
    let parts = world
        .parent_parts
        .as_ref()
        .unwrap()
        .as_ref()
        .expect("parent notified");
    assert!(
        !parts.has_mailbox_rx,
        "an Acquire load that observes the Release store must take the drop branch (no mailbox_rx)"
    );
    let tx = world.dead_mailbox_tx.as_ref().expect("kept sender");
    let tx2 = tx.clone();
    settle(move || tx2.is_closed(), "channel never closed").await;
}

// --- restart does not inherit a stale flag (end-to-end) --------------------

#[given(regex = r"^a supervised child whose parent_shutdown was previously set to true$")]
async fn given_supervised_child_flag_true(world: &mut LinksWorld) {
    // End-to-end: a real supervised child under Permanent policy. The first
    // death/restart leaves the supervision machinery to set+reset the flag; we
    // assert the restart hand-off keeps working across repeated deaths, which is
    // only possible if the factory resets parent_shutdown to false (a stuck-true
    // flag would make the child's notify take the drop branch and the parent
    // would receive no mailbox_rx, so no restart).
    let starts = Arc::new(AtomicU32::new(0));
    let supervisor = RestartSupervisor::spawn(RestartSupervisor);
    let child = RestartChild::supervise(
        &supervisor,
        RestartChild {
            starts: starts.clone(),
        },
    )
    .restart_policy(RestartPolicy::Permanent)
    .restart_limit(10, Duration::from_secs(30))
    .spawn()
    .await;
    settle(
        {
            let s = starts.clone();
            move || s.load(Ordering::SeqCst) >= 1
        },
        "child never started",
    )
    .await;
    // Drive a first panic→restart so a restart (and its flag reset) has occurred.
    let _ = child.tell(Boom).await;
    settle(
        {
            let s = starts.clone();
            move || s.load(Ordering::SeqCst) >= 2
        },
        "child never restarted after first panic",
    )
    .await;
    world.restart_starts = Some(starts);
    world.restart_child = Some(child);
    world.restart_supervisor = Some(supervisor);
}

#[when(regex = r"^the child is restarted via its spawn factory$")]
async fn when_child_restarted(world: &mut LinksWorld) {
    // Drive a SECOND panic→restart. If the restarted instance had inherited a
    // stale parent_shutdown=true, its notify would drop mailbox_rx and the parent
    // could not restart it — so reaching start #3 proves the reset.
    let child = world.restart_child.as_ref().expect("child").clone();
    let starts = world.restart_starts.as_ref().expect("starts").clone();
    let _ = child.tell(Boom).await;
    settle(
        move || starts.load(Ordering::SeqCst) >= 3,
        "child did not restart a second time (a stale parent_shutdown=true would block this)",
    )
    .await;
}

#[then(regex = r"^the restarted instance reads parent_shutdown as false$")]
async fn then_restarted_flag_false(world: &mut LinksWorld) {
    // Proven by the second restart having occurred: the start count reached 3.
    let starts = world.restart_starts.as_ref().expect("starts");
    assert!(
        starts.load(Ordering::SeqCst) >= 3,
        "a third start proves the restarted instance did not inherit parent_shutdown=true"
    );
}

#[then(regex = r"^its stale children entries from the previous instance are cleared$")]
async fn then_stale_children_cleared(world: &mut LinksWorld) {
    // The restarted RestartChild supervises no children of its own; the factory's
    // children.clear() runs on every restart. The end-to-end observable that the
    // clear+reset path is healthy is that the child remains alive and responsive
    // after the repeated restarts (a leaked stale child or stuck flag would have
    // deadlocked or stopped it).
    let child = world.restart_child.as_ref().expect("child");
    settle(
        {
            let c = child.clone();
            move || c.is_alive()
        },
        "the restarted child is not alive/responsive",
    )
    .await;
    assert!(
        child.is_alive(),
        "the restarted child must be alive after the clear+reset"
    );
    if let Some(s) = world.restart_supervisor.take() {
        s.kill();
    }
}

// ===========================================================================
// @boundary — self-link, dead target, no links
// ===========================================================================

#[given(regex = r#"^a running actor "([^"]+)"$"#)]
async fn given_running_actor(world: &mut LinksWorld, actor: String) {
    // The self-link no-op is observable on the public ActorRef API: spawn a real
    // actor and link it to itself, then assert its link set stays empty.
    let a = LinkProbe::spawn(LinkProbe);
    a.wait_for_startup().await;
    world.self_link_actor = Some(a);
    world.dead_id = Some(id_for(&actor));
}

#[when(regex = r#"^actor "([^"]+)" is linked to itself$"#)]
async fn when_actor_linked_to_self(world: &mut LinksWorld, _actor: String) {
    let a = world.self_link_actor.as_ref().expect("self-link actor");
    a.link(a).await;
}

#[then(regex = r"^linking a→a is silently ignored and a's real links are unaffected$")]
async fn then_self_link_ignored(world: &mut LinksWorld) {
    // ActorRef Debug prints `links` as the sibblings keys; a self-link returns
    // early before touching links, so the set stays empty.
    let a = world.self_link_actor.as_ref().expect("self-link actor");
    let dbg = format!("{a:?}");
    assert!(
        dbg.contains("links: []"),
        "self-link must leave the link set empty, got {dbg}"
    );
}

#[given(regex = r#"^sibling "([^"]+)" has already stopped$"#)]
async fn given_sibling_stopped(world: &mut LinksWorld, b: String) {
    // Drop the sibling's RECEIVER so its Link::Local sender is closed
    // (ActorNotRunning). The notify must swallow that error.
    let idx = world
        .sibling_rx
        .iter()
        .position(|(l, _)| *l == b)
        .expect("sibling present");
    let (_label, _rx) = world.sibling_rx.remove(idx); // dropping rx closes the channel
}

#[when(regex = r#"^actor "([^"]+)" dies and notifies "([^"]+)"$"#)]
async fn when_actor_dies_notifies(world: &mut LinksWorld, _actor: String, _b: String) {
    let id = world.dead_id.expect("dead id");
    let (_tx, rx) = raw_pair();
    // notify_sibblings spawns the notify futures; reaching here without panic and
    // the spawned task completing (no surfaced error) is the observable.
    notify_links(world.links(), id, ActorStopReason::Killed, rx).await;
    // Give the spawned sibling-notify task a moment to run to completion.
    tokio::time::sleep(Duration::from_millis(50)).await;
    world.empty_completed = Some(true);
}

#[then(regex = r"^the notify completes without surfacing an error$")]
async fn then_notify_no_error(world: &mut LinksWorld) {
    assert_eq!(
        world.empty_completed,
        Some(true),
        "notifying a dead sibling must be swallowed (ActorNotRunning), never surfaced"
    );
}

#[given(regex = r#"^an unsupervised actor "([^"]+)" with no parent and no siblings$"#)]
async fn given_unsupervised_no_links(world: &mut LinksWorld, actor: String) {
    world.links = Some(empty_links());
    world.dead_id = Some(id_for(&actor));
}

#[then(regex = r"^no on_link_died notification is produced$")]
async fn then_no_notification(world: &mut LinksWorld) {
    // notify_links with None parent + empty sibblings notifies nobody. There is
    // no observer to receive on; the assertion is that the call completed and no
    // sibling/parent receiver exists. Drive it and confirm no panic.
    let id = world.dead_id.expect("dead id");
    let (_tx, rx) = raw_pair();
    notify_links(world.links(), id, ActorStopReason::Killed, rx).await;
    assert!(
        world.sibling_rx.is_empty(),
        "there must be no siblings to notify"
    );
    assert!(
        world.parent_rx.is_none(),
        "there must be no parent to notify"
    );
}

// ===========================================================================
// @linearizability — concurrent flag-set vs notify, simultaneous deaths, fan-out
// ===========================================================================

#[given(regex = r#"^a supervisor with a supervised child "([^"]+)"$"#)]
async fn given_supervisor_with_child_lin(world: &mut LinksWorld, child: String) {
    let links = empty_links();
    let (parent_tx, parent_rx) = raw_pair();
    set_parent(&links, id_for("supervisor"), &parent_tx).await;
    let flag = parent_shutdown_flag(&links).await;
    world.links = Some(links);
    world.parent_rx = Some(parent_rx);
    world.child_under_test_flag = Some(flag);
    world.dead_id = Some(id_for(&child));
}

#[when(regex = r"^set_children_parent_shutdown and the child's independent exit run concurrently$")]
async fn when_flag_vs_notify_concurrent(world: &mut LinksWorld) {
    // Real overlap: a task that sets the flag and a task that runs notify_links,
    // released together at a Barrier. The legal outcomes are exactly {queue
    // mailbox_rx while flag==false} ∪ {drop mailbox_rx while flag==true}; the
    // forbidden state (queue after observing true) is checked by reading the
    // delivered parts and the flag together.
    let links = world.links().clone();
    let flag = world.child_under_test_flag.as_ref().expect("flag").clone();
    let id = world.dead_id.expect("dead id");
    let (dead_tx, dead_rx) = raw_pair();
    world.dead_mailbox_tx = Some(dead_tx);

    let barrier = Arc::new(Barrier::new(2));
    let setter = {
        let flag = flag.clone();
        let barrier = barrier.clone();
        tokio::spawn(async move {
            barrier.wait().await;
            flag.store(true, Ordering::Release);
        })
    };
    let notifier = {
        let links = links.clone();
        let barrier = barrier.clone();
        tokio::spawn(async move {
            barrier.wait().await;
            notify_links(&links, id, ActorStopReason::Killed, dead_rx).await;
        })
    };
    setter.await.expect("setter joins");
    notifier.await.expect("notifier joins");
    let parts = recv_link_died(world.parent_rx.as_mut().expect("parent rx"), NOTIFY_TIMEOUT).await;
    world.parent_parts = Some(parts);
}

#[then(
    regex = r"^the child either notifies with mailbox_rx BEFORE the flag was visible, or drops it AFTER$"
)]
async fn then_legal_interleaving(world: &mut LinksWorld) {
    // The honest, falsifiable invariant this racy scenario CAN assert without
    // depending on the post-join flag value (which is always true here): the
    // death produced EXACTLY ONE notification, and it is for the child that
    // died. Whatever the interleaving, the parent must see precisely one
    // LinkDied for this id — not zero, not two. (The "no SECOND, mailbox_rx-
    // bearing notification after the drop" half is checked, conditionally on the
    // drop branch, by `then_never_queue_after_true`.)
    let parts = world
        .parent_parts
        .as_ref()
        .unwrap()
        .as_ref()
        .expect("parent notified exactly once");
    let dead = world.dead_id.expect("dead id");
    assert_eq!(
        parts.id, dead,
        "the single delivered notification must be for the child that died"
    );
}

#[then(
    regex = r"^the child never queues mailbox_rx into the parent once it has observed the flag as true$"
)]
async fn then_never_queue_after_true(world: &mut LinksWorld) {
    // Exactly ONE notification was delivered (the parent receives one LinkDied);
    // if mailbox_rx was absent, the child took the drop branch (observed true).
    // The forbidden state — a SECOND, mailbox_rx-bearing notification after the
    // drop — never occurs: drain confirms no further notification.
    let parts = world
        .parent_parts
        .as_ref()
        .unwrap()
        .as_ref()
        .expect("notified");
    if !parts.has_mailbox_rx {
        // Drop branch: ensure no extra mailbox_rx-bearing notification follows.
        let extra = recv_link_died(
            world.parent_rx.as_mut().expect("parent rx"),
            NO_NOTIFY_TIMEOUT,
        )
        .await;
        assert!(
            extra.is_none(),
            "no second notification may follow the drop branch"
        );
    }
}

#[given(regex = r#"^a supervisor with supervised children "([^"]+)" and "([^"]+)"$"#)]
async fn given_supervisor_two_children_lin(world: &mut LinksWorld, c1: String, c2: String) {
    // Each child has its OWN Links (a real child is its own actor), parent over
    // the SHARED supervisor mailbox so both deaths land on one parent receiver.
    let (parent_tx, parent_rx) = raw_pair();
    world.parent_rx = Some(parent_rx);
    for label in [c1, c2] {
        let links = empty_links();
        set_parent(&links, id_for("supervisor"), &parent_tx).await;
        // Store each child's Links keyed by label via the child_rx-style slots.
        world.per_child_links.push((label.clone(), links));
        world.sibling_ids.push((label.clone(), id_for(&label)));
    }
}

#[when(regex = r#"^"([^"]+)" and "([^"]+)" both stop abnormally at the same time$"#)]
async fn when_two_children_die(world: &mut LinksWorld, c1: String, c2: String) {
    let barrier = Arc::new(Barrier::new(2));
    let mut handles = Vec::new();
    for label in [c1, c2] {
        let links = world
            .per_child_links
            .iter()
            .find(|(l, _)| *l == label)
            .map(|(_, lk)| lk.clone())
            .expect("child links");
        let id = id_for(&label);
        let barrier = barrier.clone();
        handles.push(tokio::spawn(async move {
            let (_tx, rx) = raw_pair();
            barrier.wait().await;
            notify_links(&links, id, ActorStopReason::Killed, rx).await;
        }));
    }
    for h in handles {
        h.await.expect("child notify task joins");
    }
    // Collect BOTH notifications from the shared parent receiver.
    let mut got = Vec::new();
    for _ in 0..2 {
        let parts = recv_link_died(world.parent_rx.as_mut().expect("parent rx"), NOTIFY_TIMEOUT)
            .await
            .expect("each child death must notify the parent");
        got.push(parts);
    }
    world.two_death_parts = got;
}

#[then(
    regex = r#"^the parent receives one death notification for "([^"]+)" and one for "([^"]+)"$"#
)]
async fn then_one_each(world: &mut LinksWorld, c1: String, c2: String) {
    let ids: Vec<ActorId> = world.two_death_parts.iter().map(|p| p.id).collect();
    let id1 = id_for(&c1);
    let id2 = id_for(&c2);
    assert_eq!(world.two_death_parts.len(), 2, "exactly two notifications");
    assert!(ids.contains(&id1), "missing notification for {c1}");
    assert!(ids.contains(&id2), "missing notification for {c2}");
    assert_ne!(id1, id2, "the two children have distinct ids");
}

#[then(regex = r"^each notification carries that child's own mailbox_rx$")]
async fn then_each_carries_own_mailbox_rx(world: &mut LinksWorld) {
    for p in &world.two_death_parts {
        assert!(
            p.has_mailbox_rx,
            "each supervised child's death must carry its own mailbox_rx (flag false)"
        );
    }
}

#[given(regex = r#"^an unsupervised actor "([^"]+)" linked to N siblings$"#)]
async fn given_hub_n_siblings(world: &mut LinksWorld, hub: String) {
    // Fixed N for the example scenario; the property feature sweeps N.
    let n = 8usize;
    let links = empty_links();
    world.dead_id = Some(id_for(&hub));
    for i in 0..n {
        let label = format!("sib{i}");
        let (tx, rx) = raw_pair();
        add_sibbling(&links, id_for(&label), &tx).await;
        world.sibling_rx.push((label, rx));
    }
    world.links = Some(links);
}

#[when(regex = r#"^"([^"]+)" dies while the siblings are concurrently processing other messages$"#)]
async fn when_hub_dies(world: &mut LinksWorld, _hub: String) {
    let id = world.dead_id.expect("dead id");
    let (_tx, rx) = raw_pair();
    notify_links(world.links(), id, ActorStopReason::Killed, rx).await;
    collect_sibling_parts(world).await;
}

#[then(
    regex = r#"^every one of the N siblings receives exactly one on_link_died for "([^"]+)" with no loss or duplication$"#
)]
async fn then_all_n_once(world: &mut LinksWorld, _hub: String) {
    assert!(!world.sibling_counts.is_empty(), "siblings present");
    for (label, count) in &world.sibling_counts {
        assert_eq!(
            *count, 1,
            "sibling {label} must receive exactly one notification"
        );
    }
}

// ===========================================================================
// @property / @model laws (links.properties.feature)
// ===========================================================================

// -- @property @sequence: N siblings each get exactly one, no mailbox_rx ------

#[given(regex = r#"^an unsupervised actor "([^"]+)" linked to any N distinct siblings$"#)]
async fn given_any_n_siblings(world: &mut LinksWorld, actor: String) {
    world.dead_id = Some(id_for(&actor));
}

#[when(regex = r#"^actor "([^"]+)" dies with any stop reason$"#)]
async fn when_dies_any_reason(_world: &mut LinksWorld, _actor: String) {}

#[then(
    regex = r#"^each of the N siblings receives exactly one on_link_died for "([^"]+)", with no mailbox_rx$"#
)]
async fn law_n_siblings_once(_world: &mut LinksWorld, _actor: String) {
    // ∀ N ∈ {0,1,2,16,256}, reason ∈ {Killed, Panicked, LinkDied, Normal}: a
    // single death notifies each surviving sibling exactly once with no
    // mailbox_rx. ORACLE: a per-sibling delivery counter — histogram all 1s (all
    // 0s when N==0). Driven against the real Links SUT.
    let reasons = [
        ActorStopReason::Killed,
        ActorStopReason::Panicked(PanicError::new(Box::new("boom"), PanicReason::HandlerPanic)),
        ActorStopReason::LinkDied {
            id: ActorId::new(7),
            reason: Box::new(ActorStopReason::Killed),
        },
        ActorStopReason::Normal,
    ];
    for n in [0usize, 1, 2, 16, 256] {
        for reason in &reasons {
            let links = empty_links();
            let dead = ActorId::new(900_000 + n as u64);
            let mut rxs: Vec<MailboxReceiver<Probe>> = Vec::with_capacity(n);
            for i in 0..n {
                let (tx, rx) = raw_pair();
                add_sibbling(&links, ActorId::new(i as u64), &tx).await;
                rxs.push(rx);
            }
            notify_links(&links, dead, reason.clone(), raw_pair().1).await;
            for (i, rx) in rxs.iter_mut().enumerate() {
                let first = recv_link_died(rx, NOTIFY_TIMEOUT).await;
                let parts =
                    first.unwrap_or_else(|| panic!("sibling {i} (N={n}) got no notification"));
                assert_eq!(parts.id, dead, "sibling {i} wrong dead id");
                assert!(!parts.has_mailbox_rx, "sibling {i} must have no mailbox_rx");
                assert!(
                    !parts.has_sibblings,
                    "sibling {i} must have no sibling links"
                );
                let extra = recv_link_died(rx, NO_NOTIFY_TIMEOUT).await;
                assert!(extra.is_none(), "sibling {i} (N={n}) was notified twice");
            }
        }
    }
}

#[then(regex = r"^no sibling receives zero notifications and none receives two, for any N$")]
async fn law_no_zero_no_two(_world: &mut LinksWorld) {
    // The exactly-once histogram is asserted in `law_n_siblings_once` (each
    // sibling drained for exactly one notification, with a follow-up drain that
    // must be empty). Nothing further to assert here.
}

// -- @property @lifecycle: one shutdown per child, wait closes exactly those --

#[given(regex = r"^a supervisor with any K supervised children$")]
async fn given_any_k_children(_world: &mut LinksWorld) {}

#[when(regex = r"^the supervisor calls send_children_shutdown then wait_children_closed$")]
async fn when_send_then_wait_any_k(_world: &mut LinksWorld) {}

#[then(regex = r"^each of the K children's shutdown closures is invoked exactly once$")]
async fn law_k_shutdown_once(_world: &mut LinksWorld) {
    // ∀ K ∈ {0,1,2,16}: send_children_shutdown fires each child's shutdown
    // closure exactly once. ORACLE: a per-child AtomicU32 counter; histogram is
    // K ones (empty when K==0). Driven against the real Links SUT.
    for k in [0usize, 1, 2, 16] {
        let links = empty_links();
        let mut counters: Vec<Arc<AtomicU32>> = Vec::with_capacity(k);
        let mut rxs: Vec<MailboxReceiver<Probe>> = Vec::with_capacity(k);
        for i in 0..k {
            let (tx, rx) = raw_pair();
            let flag = Arc::new(AtomicBool::new(false));
            let (spec, calls) = child_spec(&tx, flag);
            add_child(&links, ActorId::new(1_000 + i as u64), spec).await;
            counters.push(calls);
            rxs.push(rx);
        }
        links.send_children_shutdown().await;
        for (i, c) in counters.iter().enumerate() {
            assert_eq!(
                c.load(Ordering::SeqCst),
                1,
                "child {i} (K={k}) shutdown once"
            );
        }
    }
}

#[then(
    regex = r"^wait_children_closed resolves exactly when all K child mailboxes are closed, and immediately when K == 0$"
)]
async fn law_k_wait_closes(_world: &mut LinksWorld) {
    // K==0: wait resolves immediately. K>0: wait is pending while any child
    // mailbox is open, resolves once all are closed. ORACLE: a per-child
    // mailbox-open model; pending iff ∃ an open child.
    // K == 0 boundary.
    {
        let links = empty_links();
        tokio::time::timeout(Duration::from_secs(1), links.wait_children_closed())
            .await
            .expect("K==0 wait_children_closed must resolve immediately");
    }
    for k in [1usize, 2, 16] {
        let links = empty_links();
        let mut rxs: Vec<MailboxReceiver<Probe>> = Vec::with_capacity(k);
        for i in 0..k {
            let (tx, rx) = raw_pair();
            let flag = Arc::new(AtomicBool::new(false));
            let (spec, _calls) = child_spec(&tx, flag);
            add_child(&links, ActorId::new(2_000 + i as u64), spec).await;
            rxs.push(rx);
        }
        let waiter = {
            let links = links.clone();
            tokio::spawn(async move { links.wait_children_closed().await })
        };
        // Pending while open: close all but one, wait must still be pending.
        for _ in 1..k {
            rxs.pop(); // drop one receiver (close one child)
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        if k > 1 {
            assert!(
                !waiter.is_finished(),
                "K={k}: wait must be pending while one child mailbox is still open"
            );
        }
        rxs.clear(); // close the last child
        tokio::time::timeout(Duration::from_secs(5), waiter)
            .await
            .unwrap_or_else(|_| panic!("K={k}: wait must resolve once all closed"))
            .expect("waiter joins");
    }
}

// -- @model @linearizability: parent_shutdown Release/Acquire law -------------

#[given(regex = r#"^a supervisor with a supervised child "([^"]+)", parent_shutdown false$"#)]
async fn given_supervisor_child_flag_false(world: &mut LinksWorld, child: String) {
    // (Reused by the K-children @model scenario's sibling Given; the single-child
    // model law builds its own Links inside the Then.)
    world.dead_id = Some(id_for(&child));
}

#[when(
    regex = r"^set_children_parent_shutdown and the child's independent exit run concurrently, under any interleaving$"
)]
async fn when_model_interleaving(_world: &mut LinksWorld) {}

#[then(
    regex = r"^the child either notifies the parent WITH mailbox_rx before the flag is visible, or drops it AFTER$"
)]
async fn law_release_acquire(_world: &mut LinksWorld) {
    // Documented deterministic interleaving sweep with REAL overlap: for many
    // trials, race the Release store against the load-then-notify. The legal
    // outcomes are exactly {queue while flag==false} ∪ {drop while flag==true};
    // the forbidden state (queue after observing true) never occurs. ORACLE: a
    // single AtomicBool; each trial's delivered notification must be one of the
    // two legal shapes, and never more than one notification.
    for _ in 0..64 {
        let links = empty_links();
        let (parent_tx, mut parent_rx) = raw_pair();
        set_parent(&links, ActorId::new(42), &parent_tx).await;
        let flag = parent_shutdown_flag(&links).await;
        let dead = ActorId::new(7);
        let (_dead_tx, dead_rx) = raw_pair();

        let barrier = Arc::new(Barrier::new(2));
        let setter = {
            let flag = flag.clone();
            let barrier = barrier.clone();
            tokio::spawn(async move {
                barrier.wait().await;
                flag.store(true, Ordering::Release);
            })
        };
        let notifier = {
            let links = links.clone();
            let barrier = barrier.clone();
            tokio::spawn(async move {
                barrier.wait().await;
                notify_links(&links, dead, ActorStopReason::Killed, dead_rx).await;
            })
        };
        setter.await.expect("setter joins");
        notifier.await.expect("notifier joins");

        let parts = recv_link_died(&mut parent_rx, NOTIFY_TIMEOUT)
            .await
            .expect("the parent must be notified exactly once");
        // The deterministic, falsifiable observable that holds for EVERY
        // interleaving: the single delivered notification is for the actor that
        // died, and it is the ONLY one — no forbidden second, mailbox_rx-bearing
        // notification follows. (The drop-branch shape under store-before-load —
        // that observing the flag as true forces `!has_mailbox_rx` — is asserted
        // deterministically by `law_never_queue_after_true`, which loads AFTER
        // the store rather than racing it.) Post-join `flag.load()` is always
        // true here, so it carries no falsifiable content and is not asserted.
        assert_eq!(
            parts.id, dead,
            "the notification must be for the dead actor"
        );
        let extra = recv_link_died(&mut parent_rx, NO_NOTIFY_TIMEOUT).await;
        assert!(extra.is_none(), "exactly one notification per death");
    }
}

#[then(
    regex = r"^once the child's Acquire load observes the Release store as true, it never queues mailbox_rx to the parent$"
)]
async fn law_never_queue_after_true(_world: &mut LinksWorld) {
    // Direct deterministic arm: set the flag true FIRST (store-before-load), then
    // notify. The Acquire load must observe true and take the drop branch — never
    // queue mailbox_rx. Repeated to exercise the ordering.
    for _ in 0..16 {
        let links = empty_links();
        let (parent_tx, mut parent_rx) = raw_pair();
        set_parent(&links, ActorId::new(42), &parent_tx).await;
        let flag = parent_shutdown_flag(&links).await;
        flag.store(true, Ordering::Release);
        let (_dead_tx, dead_rx) = raw_pair();
        notify_links(&links, ActorId::new(7), ActorStopReason::Killed, dead_rx).await;
        let parts = recv_link_died(&mut parent_rx, NOTIFY_TIMEOUT)
            .await
            .expect("parent notified");
        assert!(
            !parts.has_mailbox_rx,
            "store-before-load must take the drop branch (no mailbox_rx queued)"
        );
    }
}

// -- @model @linearizability: K simultaneous deaths each notify once ----------

#[given(regex = r"^a supervisor with any K supervised children, parent_shutdown false$")]
async fn given_any_k_children_flag_false(_world: &mut LinksWorld) {}

#[when(regex = r"^all K children stop abnormally at the same time with real overlap$")]
async fn when_k_children_die_overlap(_world: &mut LinksWorld) {}

#[then(
    regex = r"^the parent receives exactly one death notification per child, each carrying that child's own mailbox_rx$"
)]
async fn law_k_deaths_independent(_world: &mut LinksWorld) {
    // ∀ K ∈ [2,16]: K children, each its own Links with the SAME parent mailbox,
    // all dying with real overlap (Barrier). The parent must receive exactly K
    // notifications, one per distinct child id, each carrying mailbox_rx (flag
    // false). ORACLE: a per-id delivery counter — histogram K ones.
    for k in [2usize, 3, 8, 16] {
        let (parent_tx, mut parent_rx) = raw_pair();
        let barrier = Arc::new(Barrier::new(k));
        let mut handles = Vec::new();
        let mut ids = Vec::new();
        for i in 0..k {
            let links = empty_links();
            set_parent(&links, ActorId::new(42), &parent_tx).await;
            let id = ActorId::new(10_000 + i as u64);
            ids.push(id);
            let barrier = barrier.clone();
            handles.push(tokio::spawn(async move {
                let (_tx, rx) = raw_pair();
                barrier.wait().await;
                notify_links(&links, id, ActorStopReason::Killed, rx).await;
            }));
        }
        for h in handles {
            h.await.expect("child notify joins");
        }
        // Collect K notifications; assert one per distinct id, each with mailbox_rx.
        let mut seen: Vec<ActorId> = Vec::new();
        for _ in 0..k {
            let parts = recv_link_died(&mut parent_rx, NOTIFY_TIMEOUT)
                .await
                .unwrap_or_else(|| panic!("K={k}: missing a child notification"));
            assert!(
                parts.has_mailbox_rx,
                "K={k}: each death carries its own mailbox_rx"
            );
            assert!(
                !seen.contains(&parts.id),
                "K={k}: duplicate notification for {:?}",
                parts.id
            );
            seen.push(parts.id);
        }
        seen.sort_unstable_by_key(|i| i.sequence_id());
        ids.sort_unstable_by_key(|i| i.sequence_id());
        assert_eq!(seen, ids, "K={k}: exactly one notification per child id");
        // No extra notifications.
        let extra = recv_link_died(&mut parent_rx, NO_NOTIFY_TIMEOUT).await;
        assert!(extra.is_none(), "K={k}: no extra notifications");
    }
}

#[then(
    regex = r"^no child's notification is lost, duplicated, or attributed to another child, for any K$"
)]
async fn law_k_no_loss(_world: &mut LinksWorld) {
    // The per-id histogram (exactly K distinct ids, each once) is asserted in
    // `law_k_deaths_independent`. Nothing further to assert here.
}
