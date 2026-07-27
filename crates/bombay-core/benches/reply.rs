//! Criterion bench: the #115 typed reply port vs. kameo's erased reply.
//!
//! #115 deletes kameo's `Box<dyn Any>` reply erasure. The justification is that a
//! typed `oneshot<Result<R, E>>` avoids the two costs kameo's
//! `oneshot<Result<Box<dyn Any>, _>>` pays per reply: a **heap `Box`** on send
//! and a **`downcast`** on recv. This bench *measures* that claim (CLAUDE rule 0)
//! rather than asserting it.
//!
//! The `erased` arm models kameo's mechanism faithfully: kameo's
//! `ReplySender::send` does `Box::new(value) as BoxReply` and the caller recovers
//! it with `*ok.downcast().unwrap()` (see the vendored `src/reply.rs`). We model
//! the mechanism rather than invoking kameo's `ReplySender` directly because the
//! latter is coupled to its `Reply` trait + `Context` dispatch; the isolated
//! oneshot roundtrip is the honest apples-to-apples comparison of the erasure cost.
//!
//! Both arms do the same work an `ask` does: one fresh channel + one send + one
//! recv, ×1_000, so the per-`ask` cost (channel alloc dominates) is measured, not
//! a reused channel.

use std::any::Any;

use bombay_core::error::Infallible;
use bombay_core::reply::reply_channel;
use criterion::{Criterion, black_box, criterion_group, criterion_main};
use tokio::sync::oneshot;

const N: u64 = 1_000;

/// Bombay's typed reply: fresh channel, `send`, `recv` — no box, no downcast.
fn typed_roundtrip(c: &mut Criterion) {
    let rt = tokio::runtime::Builder::new_current_thread()
        .build()
        .expect("current-thread runtime");

    c.bench_function("reply_typed_roundtrip_1k", |b| {
        b.iter(|| {
            rt.block_on(async {
                let mut sink = 0u64;
                for i in 0..N {
                    let (tx, rx) = reply_channel::<u64, Infallible>();
                    let _ = tx.send(black_box(i));
                    if let Ok(v) = rx.recv::<()>().await {
                        sink = sink.wrapping_add(v);
                    }
                }
                black_box(sink)
            });
        });
    });
}

/// The kameo-shaped erased reply: the value is boxed to `Box<dyn Any>` on send and
/// recovered by `downcast` on recv — the two costs #115 removes.
fn erased_roundtrip(c: &mut Criterion) {
    let rt = tokio::runtime::Builder::new_current_thread()
        .build()
        .expect("current-thread runtime");

    c.bench_function("reply_erased_roundtrip_1k", |b| {
        b.iter(|| {
            rt.block_on(async {
                let mut sink = 0u64;
                for i in 0..N {
                    let (tx, rx) =
                        oneshot::channel::<Result<Box<dyn Any + Send>, Box<dyn Any + Send>>>();
                    let _ = tx.send(Ok(Box::new(black_box(i)) as Box<dyn Any + Send>));
                    if let Ok(Ok(boxed)) = rx.await {
                        let v = *boxed.downcast::<u64>().expect("reply is a u64");
                        sink = sink.wrapping_add(v);
                    }
                }
                black_box(sink)
            });
        });
    });
}

criterion_group!(benches, typed_roundtrip, erased_roundtrip);
criterion_main!(benches);
