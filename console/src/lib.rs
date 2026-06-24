mod poller;
mod tui;

use std::time::Instant;

pub use poller::spawn_poller;
pub use tui::App;

#[derive(Debug, Clone)]
pub enum ConnectionState {
    Connecting,
    Connected,
    Disconnected { error: String, since: Instant },
}

/// Test-only access to the crate's private helpers, for the cucumber harness.
/// Gated so normal builds never expose it (CLAUDE.md rule 4).
#[cfg(any(test, feature = "testing"))]
pub mod testing {
    pub use crate::poller::{
        MAX_FRAME_BYTES, check_frame_len, decode_frame, poll_once_over,
        poll_once_over_with_read_timeout,
    };
    pub use crate::tui::{
        STUCK_THRESHOLD, SortCol, actor_rate, backpressure_style, braille, centered_rect,
        color_rgb, compare, detect_deadlocks, fade_toward_bg, fmt_ago, fmt_short, fmt_uptime,
        mailbox_bar, rate_context, severity, short_type_name, sort_actors, spark_height,
        sparkline_line,
    };
    pub use kameo::console::wire::{
        ActorCounters, ActorId, ActorSnapshot, ActorStatus, HandlerActivity, Links, MailboxKind,
        MailboxStats, MessageCount, RefCounts, Snapshot, Totals, WaitEdge, WaitKind,
    };
}
