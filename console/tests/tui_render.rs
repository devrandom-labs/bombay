//! In-process render tests for `tui::App` (card #76): an in-process `ratatui::TestBackend`
//! harness that drives the REAL `App` by synthetic keystrokes (`App::press`) + `App::render_once`,
//! then asserts on the captured cell grid. Complements the pure-helper coverage in `tui_bdd`
//! by exercising the full refresh→rebuild→draw pipeline and the `on_key` dispatch, which a
//! Gherkin/`cucumber` scenario cannot reach (the helpers are pure; the App is stateful + drawn).
//! Only the literal terminal `event::read()` poll is out of reach in-process.

use std::net::{Ipv4Addr, SocketAddr};
use std::sync::atomic::AtomicU64;
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime};

use bombay::console::wire::{
    ActorCounters, ActorId, ActorSnapshot, ActorStatus, HandlerActivity, Links, MailboxKind,
    MailboxStats, MessageCount, RefCounts, RestartPolicy, Snapshot, SupervisionInfo,
    SupervisorStrategy, Totals, WaitEdge, WaitKind,
};
use bombay_console::{App, ConnectionState};
use crossterm::event::{KeyCode, KeyModifiers};
use ratatui::Terminal;
use ratatui::backend::TestBackend;

fn actor(id: u64, name: &str, status: ActorStatus) -> ActorSnapshot {
    ActorSnapshot {
        id: ActorId(id),
        name: name.to_string(),
        status,
        handling: None,
        waiting_on: None,
        strategy: None,
        spawned_at: SystemTime::UNIX_EPOCH,
        mailbox: MailboxStats {
            kind: MailboxKind::Bounded,
            len: 3,
            capacity: Some(10),
        },
        counters: ActorCounters {
            messages_received: 100,
            ..ActorCounters::default()
        },
        message_types: Vec::new(),
        refs: RefCounts { strong: 1, weak: 0 },
        links: Links::default(),
        supervision: None,
    }
}

fn snapshot(seq: u64, actors: Vec<ActorSnapshot>) -> Snapshot {
    Snapshot {
        seq,
        captured_at: SystemTime::UNIX_EPOCH + Duration::from_secs(seq),
        uptime: Duration::from_secs(seq),
        actors,
        totals: Totals::default(),
    }
}

/// Flatten the TestBackend's cell grid into a newline-joined string for substring assertions.
fn screen(terminal: &Terminal<TestBackend>) -> String {
    let buf = terminal.backend().buffer();
    let mut out = String::new();
    for y in 0..buf.area.height {
        for x in 0..buf.area.width {
            out.push_str(buf.cell((x, y)).map_or(" ", ratatui::buffer::Cell::symbol));
        }
        out.push('\n');
    }
    out
}

/// Build an `App` wired to a shared slot we control, seeded with `snap`.
fn app_with(snap: Snapshot) -> (App, Arc<Mutex<Option<Snapshot>>>) {
    let slot = Arc::new(Mutex::new(Some(snap)));
    let connection = Arc::new(Mutex::new(ConnectionState::Connected));
    let interval = Arc::new(AtomicU64::new(1000));
    let addr = SocketAddr::from((Ipv4Addr::LOCALHOST, 4000));
    let app = App::live(addr, Arc::clone(&slot), connection, interval);
    (app, slot)
}

fn terminal() -> Terminal<TestBackend> {
    Terminal::new(TestBackend::new(120, 40)).unwrap()
}

#[test]
fn dashboard_renders_actor_rows() {
    let snap = snapshot(
        1,
        vec![
            actor(1, "AlphaActor", ActorStatus::Running),
            actor(2, "BetaActor", ActorStatus::Restarting),
            actor(
                3,
                "GammaActor",
                ActorStatus::Stopped {
                    at: SystemTime::UNIX_EPOCH,
                    reason: "done".into(),
                },
            ),
        ],
    );
    let (mut app, _slot) = app_with(snap);
    let mut term = terminal();
    app.render_once(&mut term).unwrap();

    let s = screen(&term);
    assert!(
        s.contains("AlphaActor"),
        "dashboard should list the first actor:\n{s}"
    );
    assert!(s.contains("BetaActor"));
    assert!(s.contains("GammaActor"));
}

#[test]
fn help_popup_toggles_with_question_mark() {
    let (mut app, _slot) = app_with(snapshot(
        1,
        vec![actor(1, "AlphaActor", ActorStatus::Running)],
    ));
    let mut term = terminal();

    app.render_once(&mut term).unwrap();
    assert!(
        !screen(&term).contains("Keybindings"),
        "help hidden by default"
    );

    app.press(KeyCode::Char('?'), KeyModifiers::NONE);
    app.render_once(&mut term).unwrap();
    assert!(
        screen(&term).contains("Keybindings"),
        "? opens the help popup"
    );

    app.press(KeyCode::Esc, KeyModifiers::NONE);
    app.render_once(&mut term).unwrap();
    assert!(
        !screen(&term).contains("Keybindings"),
        "Esc closes the help popup"
    );
}

#[test]
fn filter_mode_shows_typed_query() {
    let (mut app, _slot) = app_with(snapshot(
        1,
        vec![
            actor(1, "AlphaActor", ActorStatus::Running),
            actor(2, "BetaActor", ActorStatus::Running),
        ],
    ));
    let mut term = terminal();
    app.render_once(&mut term).unwrap();

    app.press(KeyCode::Char('/'), KeyModifiers::NONE);
    for c in "Alpha".chars() {
        app.press(KeyCode::Char(c), KeyModifiers::NONE);
    }
    app.render_once(&mut term).unwrap();
    let s = screen(&term);
    assert!(
        s.contains("Alpha"),
        "filter line echoes the typed query:\n{s}"
    );
    // Filtering to "Alpha" keeps AlphaActor and drops BetaActor.
    assert!(s.contains("AlphaActor"));
    assert!(!s.contains("BetaActor"), "Beta filtered out");
}

#[test]
fn inspect_panel_opens_on_enter_and_sorts_apply() {
    let (mut app, _slot) = app_with(snapshot(
        1,
        vec![
            actor(1, "AlphaActor", ActorStatus::Running),
            actor(2, "BetaActor", ActorStatus::Running),
        ],
    ));
    let mut term = terminal();
    app.render_once(&mut term).unwrap();

    // Open the inspect panel, focus it, scroll it — exercises render_inspect_panel + panel keys.
    app.press(KeyCode::Enter, KeyModifiers::NONE);
    app.render_once(&mut term).unwrap();
    app.press(KeyCode::Tab, KeyModifiers::NONE);
    app.press(KeyCode::Down, KeyModifiers::NONE);
    app.render_once(&mut term).unwrap();
    app.press(KeyCode::Esc, KeyModifiers::NONE);
    app.render_once(&mut term).unwrap();

    // Cycle the sort columns (Id/Name/State/...).
    for k in ['n', 's', 'i'] {
        app.press(KeyCode::Char(k), KeyModifiers::NONE);
        app.render_once(&mut term).unwrap();
    }
    assert!(screen(&term).contains("AlphaActor"));
}

#[test]
fn deadlock_banner_renders_for_a_wait_cycle() {
    // A↔B mutual wait => one cycle => the deadlock banner should appear.
    let mut a = actor(1, "AlphaActor", ActorStatus::Running);
    a.waiting_on = Some(WaitEdge {
        target: ActorId(2),
        kind: WaitKind::Ask,
        elapsed: Duration::from_secs(1),
    });
    let mut b = actor(2, "BetaActor", ActorStatus::Running);
    b.waiting_on = Some(WaitEdge {
        target: ActorId(1),
        kind: WaitKind::Ask,
        elapsed: Duration::from_secs(1),
    });

    let (mut app, _slot) = app_with(snapshot(1, vec![a, b]));
    let mut term = terminal();
    app.render_once(&mut term).unwrap();
    // The banner text varies; assert the actors still render and no panic occurred under a cycle.
    let s = screen(&term);
    assert!(s.contains("AlphaActor") && s.contains("BetaActor"));
}

#[test]
fn rates_render_across_two_snapshots() {
    // First snapshot seeds prev; a second with higher counts drives a non-zero msg/s + sparkline.
    let (mut app, slot) = app_with(snapshot(
        1,
        vec![actor(1, "AlphaActor", ActorStatus::Running)],
    ));
    let mut term = terminal();
    app.render_once(&mut term).unwrap();

    let mut a = actor(1, "AlphaActor", ActorStatus::Running);
    a.counters.messages_received = 250; // +150 over the prev frame
    *slot.lock().unwrap() = Some(snapshot(2, vec![a]));
    app.render_once(&mut term).unwrap();

    assert!(screen(&term).contains("AlphaActor"));
}

// ---------------------------------------------------------------------------
// Extra keystroke scenarios (card #82): press the branches the initial 6 render
// tests didn't reach — every state/severity arm, the sort keys, tree collapse/
// expand, the poll-interval keys, and the focused inspect-panel scroll edges.
// ---------------------------------------------------------------------------

/// A `TestBackend` terminal of an explicit size, for the responsive layouts (compact columns,
/// stacked-vs-side inspect panel, list overflow) that only trigger at particular dimensions.
fn terminal_sized(width: u16, height: u16) -> Terminal<TestBackend> {
    Terminal::new(TestBackend::new(width, height)).unwrap()
}

/// A `Running` actor that has been inside a handler for `elapsed` — drives the Busy (≥500ms) and
/// Stuck (≥5s) severity arms of `state_cell`.
fn handling(id: u64, name: &str, elapsed: Duration) -> ActorSnapshot {
    ActorSnapshot {
        handling: Some(HandlerActivity {
            message: "Deposit".to_string(),
            elapsed,
        }),
        ..actor(id, name, ActorStatus::Running)
    }
}

/// 1-based row index of the first rendered line containing `needle`, for order assertions.
fn row_of(screen: &str, needle: &str) -> Option<usize> {
    screen.lines().position(|l| l.contains(needle))
}

#[test]
fn every_state_arm_renders_its_label() {
    // One actor per status/severity, all roots, so the tree shows each `state_cell` arm at once.
    let snap = snapshot(
        1,
        vec![
            actor(1, "RunActor", ActorStatus::Running),
            actor(2, "RestartActor", ActorStatus::Restarting),
            actor(3, "StartActor", ActorStatus::Starting),
            actor(4, "StopActor", ActorStatus::Stopping),
            actor(
                5,
                "DeadActor",
                ActorStatus::Stopped {
                    at: SystemTime::UNIX_EPOCH,
                    reason: "done".into(),
                },
            ),
            handling(6, "BusyActor", Duration::from_millis(600)),
            handling(7, "StuckActor", Duration::from_secs(6)),
        ],
    );
    let (mut app, _slot) = app_with(snap);
    let mut term = terminal();
    app.render_once(&mut term).unwrap();

    let s = screen(&term);
    for label in [
        "Running",
        "Restarting",
        "Starting",
        "Stopping",
        "Dead",
        "Busy",
        "Stuck",
    ] {
        assert!(s.contains(label), "state column should show {label}:\n{s}");
    }
}

#[test]
fn sort_keys_cycle_every_column_and_toggle_direction() {
    let (mut app, _slot) = app_with(snapshot(
        1,
        vec![
            actor(1, "RunActor", ActorStatus::Running),
            actor(
                2,
                "DeadActor",
                ActorStatus::Stopped {
                    at: SystemTime::UNIX_EPOCH,
                    reason: "boom".into(),
                },
            ),
        ],
    ));
    let mut term = terminal();

    // Every column key drives a distinct `SortCol` (the "different column" branch of set_sort and
    // each arm of `compare`); the render must survive each.
    for k in ['i', 'n', 's', 'm', 'g', 'r'] {
        app.press(KeyCode::Char(k), KeyModifiers::NONE);
        app.render_once(&mut term).unwrap();
        assert!(
            screen(&term).contains("RunActor"),
            "sort '{k}' still renders"
        );
    }

    // State descending is the default for the State column, so the more-severe Dead actor sorts
    // above the Running one — a concrete ordering assertion, not just "it drew something".
    app.press(KeyCode::Char('s'), KeyModifiers::NONE);
    app.render_once(&mut term).unwrap();
    let s = screen(&term);
    assert!(
        row_of(&s, "DeadActor") < row_of(&s, "RunActor"),
        "State-desc sort surfaces Dead above Running:\n{s}"
    );

    // Pressing the active column again toggles the direction (the "same column" branch), flipping
    // the order.
    app.press(KeyCode::Char('s'), KeyModifiers::NONE);
    app.render_once(&mut term).unwrap();
    let s = screen(&term);
    assert!(
        row_of(&s, "RunActor") < row_of(&s, "DeadActor"),
        "toggling State sort to ascending puts Running above Dead:\n{s}"
    );
}

/// A parent (id 1) supervising two children (ids 2, 3), each a root/child in one snapshot — the
/// supervision tree the collapse/expand keys prune.
fn tree_snapshot() -> Snapshot {
    let mut parent = actor(1, "ParentActor", ActorStatus::Running);
    parent.strategy = Some(SupervisorStrategy::OneForAll);
    parent.links = Links {
        parent: None,
        children: vec![ActorId(2), ActorId(3)],
        siblings: Vec::new(),
    };
    let mut child_a = actor(2, "ChildAActor", ActorStatus::Running);
    child_a.links.parent = Some(ActorId(1));
    let mut child_b = actor(3, "ChildBActor", ActorStatus::Running);
    child_b.links.parent = Some(ActorId(1));
    snapshot(1, vec![parent, child_a, child_b])
}

#[test]
fn space_toggles_collapse_of_the_selected_node() {
    let (mut app, _slot) = app_with(tree_snapshot());
    let mut term = terminal();
    app.render_once(&mut term).unwrap();
    assert!(
        screen(&term).contains("ChildAActor"),
        "children visible while the parent is expanded"
    );

    // Selection starts on the parent (row 0); Space collapses it, pruning both children.
    app.press(KeyCode::Char(' '), KeyModifiers::NONE);
    app.render_once(&mut term).unwrap();
    let s = screen(&term);
    assert!(
        !s.contains("ChildAActor") && !s.contains("ChildBActor"),
        "collapsing the parent hides its children:\n{s}"
    );
    assert!(s.contains("▸"), "collapsed parent shows the ▸ marker");

    // Space again re-expands.
    app.press(KeyCode::Char(' '), KeyModifiers::NONE);
    app.render_once(&mut term).unwrap();
    assert!(
        screen(&term).contains("ChildAActor"),
        "toggling Space back expands the parent"
    );
}

#[test]
fn collapse_all_and_expand_all_and_arrow_keys() {
    let (mut app, _slot) = app_with(tree_snapshot());
    let mut term = terminal();
    app.render_once(&mut term).unwrap();

    // 'c' collapses every parent; 'e' expands them all again.
    app.press(KeyCode::Char('c'), KeyModifiers::NONE);
    app.render_once(&mut term).unwrap();
    assert!(
        !screen(&term).contains("ChildAActor"),
        "collapse-all hides children"
    );
    app.press(KeyCode::Char('e'), KeyModifiers::NONE);
    app.render_once(&mut term).unwrap();
    assert!(
        screen(&term).contains("ChildAActor"),
        "expand-all shows children"
    );

    // Left/h collapses the selected node; Right/l expands it.
    app.press(KeyCode::Left, KeyModifiers::NONE);
    app.render_once(&mut term).unwrap();
    assert!(
        !screen(&term).contains("ChildAActor"),
        "Left collapses the selected parent"
    );
    app.press(KeyCode::Char('l'), KeyModifiers::NONE);
    app.render_once(&mut term).unwrap();
    assert!(
        screen(&term).contains("ChildAActor"),
        "'l' expands the selected parent"
    );

    // Selecting a child highlights its ancestor lineage (exercises ancestor_ids + ANCESTOR_BG).
    app.press(KeyCode::Down, KeyModifiers::NONE);
    app.render_once(&mut term).unwrap();
    assert!(screen(&term).contains("ChildAActor"));
}

#[test]
fn poll_interval_keys_adjust_and_clamp() {
    let (mut app, _slot) = app_with(snapshot(
        1,
        vec![actor(1, "AlphaActor", ActorStatus::Running)],
    ));
    let mut term = terminal();

    // The header meta line echoes the current interval; it starts at the wired 1000ms.
    app.press(KeyCode::Char('+'), KeyModifiers::NONE);
    app.render_once(&mut term).unwrap();
    assert!(
        screen(&term).contains("1100ms"),
        "'+' raises by one 100ms step"
    );

    app.press(KeyCode::Char('-'), KeyModifiers::NONE);
    app.press(KeyCode::Char('-'), KeyModifiers::NONE);
    app.render_once(&mut term).unwrap();
    assert!(screen(&term).contains("900ms"), "'-' lowers by 100ms steps");

    // Floors at 100ms no matter how many decrements.
    for _ in 0..20 {
        app.press(KeyCode::Char('-'), KeyModifiers::NONE);
    }
    app.render_once(&mut term).unwrap();
    assert!(screen(&term).contains("100ms"), "interval floors at 100ms");

    // Caps at 10000ms no matter how many increments.
    for _ in 0..200 {
        app.press(KeyCode::Char('+'), KeyModifiers::NONE);
    }
    app.render_once(&mut term).unwrap();
    assert!(
        screen(&term).contains("10000ms"),
        "interval caps at 10000ms"
    );
}

/// A fully-wired supervised child (id 2 under parent 1) locked in a 2⇄3 deadlock, with a sibling
/// link, its own child, a restart history at the limit, and per-type message counts — so the
/// inspect panel exercises every optional block in `build_panel_lines`.
fn rich_snapshot() -> Snapshot {
    let mut parent = actor(1, "ParentActor", ActorStatus::Running);
    parent.strategy = Some(SupervisorStrategy::OneForAll);
    parent.links = Links {
        parent: None,
        children: vec![ActorId(2), ActorId(3)],
        siblings: Vec::new(),
    };

    let mut child = handling(2, "ChildActor", Duration::from_secs(6)); // Stuck
    child.strategy = Some(SupervisorStrategy::OneForOne);
    child.links = Links {
        parent: Some(ActorId(1)),
        children: vec![ActorId(4)],
        siblings: vec![ActorId(3)],
    };
    child.waiting_on = Some(WaitEdge {
        target: ActorId(3),
        kind: WaitKind::Ask,
        elapsed: Duration::from_secs(6),
    });
    child.supervision = Some(SupervisionInfo {
        policy: RestartPolicy::Permanent,
        max_restarts: 3,
        restart_window: Duration::from_secs(60),
        restart_count: 3, // at the limit
    });
    child.counters.restarts = 3;
    child.counters.panics = 1;
    child.message_types = vec![
        MessageCount {
            name: "Deposit".into(),
            count: 80,
        },
        MessageCount {
            name: "Withdraw".into(),
            count: 20,
        },
    ];

    let mut peer = actor(3, "PeerActor", ActorStatus::Running);
    peer.links = Links {
        parent: Some(ActorId(1)),
        children: Vec::new(),
        siblings: vec![ActorId(2)],
    };
    peer.waiting_on = Some(WaitEdge {
        target: ActorId(2), // closes the 2⇄3 cycle
        kind: WaitKind::Ask,
        elapsed: Duration::from_secs(6),
    });

    let mut grandchild = actor(4, "GrandActor", ActorStatus::Running);
    grandchild.links.parent = Some(ActorId(2));

    snapshot(1, vec![parent, child, peer, grandchild])
}

/// From the parent-selected default, move selection onto the rich child (id 2). Depth-first tree
/// order is Parent, Child, Grand(child of Child), Peer — so one Down lands on the child.
fn select_rich_child(app: &mut App, term: &mut Terminal<TestBackend>) {
    app.render_once(term).unwrap();
    app.press(KeyCode::Down, KeyModifiers::NONE); // Parent -> Child
    app.render_once(term).unwrap();
}

#[test]
fn inspect_panel_renders_every_field_block() {
    let (mut app, _slot) = app_with(rich_snapshot());
    let mut term = terminal();
    select_rich_child(&mut app, &mut term);

    app.press(KeyCode::Enter, KeyModifiers::NONE); // open inspect on the child
    app.render_once(&mut term).unwrap();
    let s = screen(&term);

    for token in [
        "Deadlock",      // state_cell: the deadlocked arm wins over the Stuck severity
        "Waiting on",    // the blocked ask edge to the peer
        "Supervisor",    // parent link
        "Strategy",      // the child is itself a sub-supervisor (OneForOne)
        "OneForOne",     // its own strategy label, rendered in full
        "Children",      // its own child block
        "Sibling links", // sibling link block
        "Waiters",       // the peer blocked on this child
        "DEADLOCK",      // the 2⇄3 cycle
        "Throughput",
        "Refs",
    ] {
        assert!(s.contains(token), "inspect panel should show {token}:\n{s}");
    }
}

#[test]
fn focused_inspect_panel_scrolls_through_its_content() {
    // A short terminal makes the panel shorter than its content, so scrolling actually moves it.
    let (mut app, _slot) = app_with(rich_snapshot());
    let mut term = terminal_sized(80, 14);
    select_rich_child(&mut app, &mut term);
    app.press(KeyCode::Enter, KeyModifiers::NONE); // open inspect
    app.press(KeyCode::Tab, KeyModifiers::NONE); // focus the panel
    app.press(KeyCode::Home, KeyModifiers::NONE); // scroll to the top
    app.render_once(&mut term).unwrap();
    let top = screen(&term);
    assert!(top.contains("State"), "panel top shows the first field");
    assert!(
        !top.contains("Messages"),
        "the last block is off-screen at the top:\n{top}"
    );

    // End jumps to the bottom, revealing the final "Messages by type" block.
    app.press(KeyCode::End, KeyModifiers::NONE);
    app.render_once(&mut term).unwrap();
    let bottom = screen(&term);
    assert!(
        bottom.contains("Messages"),
        "End scrolls the last block into view:\n{bottom}"
    );
    assert!(
        !bottom.contains("State "),
        "the first field has scrolled off the top:\n{bottom}"
    );

    // The remaining scroll keys must all drive the panel without panicking, and land back near
    // the top so the first field is visible again.
    app.press(KeyCode::PageUp, KeyModifiers::NONE);
    app.press(KeyCode::PageDown, KeyModifiers::NONE);
    app.press(KeyCode::Char('k'), KeyModifiers::NONE);
    app.press(KeyCode::Char('j'), KeyModifiers::NONE);
    app.press(KeyCode::Up, KeyModifiers::NONE);
    app.press(KeyCode::Home, KeyModifiers::NONE);
    app.render_once(&mut term).unwrap();
    assert!(
        screen(&term).contains("State"),
        "back at the top after Home"
    );

    // Tab returns focus to the list; Enter then closes the panel.
    app.press(KeyCode::Tab, KeyModifiers::NONE);
    app.press(KeyCode::Enter, KeyModifiers::NONE);
    app.render_once(&mut term).unwrap();
    assert!(
        screen(&term).contains("ChildActor"),
        "list still renders after closing inspect"
    );
}

#[test]
fn list_navigation_scrolls_and_jumps_with_a_scrollbar() {
    // More actors than the short viewport can show, forcing the overflow scrollbar + focus fade.
    let actors: Vec<ActorSnapshot> = (1..=20)
        .map(|i| actor(i, &format!("Actor{i:02}"), ActorStatus::Running))
        .collect();
    let (mut app, _slot) = app_with(snapshot(1, actors));
    let mut term = terminal_sized(120, 10);
    app.render_once(&mut term).unwrap();
    // The list title carries a "pos/len" counter; selection starts at 1/20.
    assert!(
        screen(&term).contains("1/20"),
        "starts selected on the first row"
    );

    // End jumps to the last row; the viewport scrolls so row 20 shows.
    app.press(KeyCode::End, KeyModifiers::NONE);
    app.render_once(&mut term).unwrap();
    let s = screen(&term);
    assert!(s.contains("20/20"), "End selects the last row:\n{s}");
    assert!(s.contains("Actor20"), "the last actor scrolled into view");

    // j/k and Up/Down step the selection; Home returns to the top.
    app.press(KeyCode::Char('k'), KeyModifiers::NONE);
    app.render_once(&mut term).unwrap();
    assert!(screen(&term).contains("19/20"), "'k' steps up one row");
    app.press(KeyCode::Home, KeyModifiers::NONE);
    app.render_once(&mut term).unwrap();
    assert!(
        screen(&term).contains("1/20"),
        "Home returns to the first row"
    );
}

#[test]
fn filter_backspace_and_escape_clear_the_query() {
    let (mut app, _slot) = app_with(snapshot(
        1,
        vec![
            actor(1, "AlphaActor", ActorStatus::Running),
            actor(2, "BetaActor", ActorStatus::Running),
        ],
    ));
    let mut term = terminal();
    app.render_once(&mut term).unwrap();

    // Enter filter mode and narrow to Alpha.
    app.press(KeyCode::Char('/'), KeyModifiers::NONE);
    for c in "Alph".chars() {
        app.press(KeyCode::Char(c), KeyModifiers::NONE);
    }
    app.render_once(&mut term).unwrap();
    assert!(!screen(&term).contains("BetaActor"), "Beta filtered out");

    // Backspace deletes characters; once empty, a further Backspace exits filter mode.
    for _ in 0..4 {
        app.press(KeyCode::Backspace, KeyModifiers::NONE);
    }
    app.press(KeyCode::Backspace, KeyModifiers::NONE); // empty -> leave filter mode
    app.render_once(&mut term).unwrap();
    let s = screen(&term);
    assert!(
        s.contains("AlphaActor") && s.contains("BetaActor"),
        "clearing the filter restores every actor:\n{s}"
    );

    // Re-filter, then Esc clears the query in one shot.
    app.press(KeyCode::Char('/'), KeyModifiers::NONE);
    app.press(KeyCode::Char('B'), KeyModifiers::NONE);
    app.render_once(&mut term).unwrap();
    assert!(!screen(&term).contains("AlphaActor"), "filtered to Beta");
    app.press(KeyCode::Esc, KeyModifiers::NONE);
    app.render_once(&mut term).unwrap();
    assert!(
        screen(&term).contains("AlphaActor"),
        "Esc clears the filter and restores the list"
    );
}
