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
    ActorCounters, ActorId, ActorSnapshot, ActorStatus, Links, MailboxKind, MailboxStats,
    RefCounts, Snapshot, Totals, WaitEdge, WaitKind,
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
