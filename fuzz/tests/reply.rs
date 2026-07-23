//! Model-based fuzz of the single-shot reply channel (`reply_channel`, card
//! #115 / #118). The response port is the typed end of every `ask`; it is
//! currently only covered by `proptest` in `bombay-core`, never by a fuzzer.
//!
//! Drives `send` / `send_err` / receiver-drop / sender-drop and asserts the
//! exact outcome matrix:
//!
//! * `send(v)` to a live receiver → the receiver yields `Ok(v)`;
//! * `send_err(e)` to a live receiver → the receiver yields `Err(Handler(e))`;
//! * `send` after the receiver is dropped → `Err(AskerGone)` (reply discarded);
//! * `recv` after the sender is dropped without replying → `Err(Interrupted)`.
//!
//! Runs the one-shot `recv` on a current-thread tokio runtime, like the
//! `actor_loop` target. Bolero's corpus replay (CI `bombay-fuzz-replay`) is
//! sync, so the MIRI lane cannot run this one — it is fuzz-only, matching the
//! other targets in this workspace.

use bolero::{TypeGenerator, check};
use bombay_core::error::AskError;
use bombay_core::reply::reply_channel;

#[derive(Debug, TypeGenerator)]
enum Op {
    Send(u64),
    SendErr(String),
    SendAfterReceiverDrop(u64),
    RecvAfterSenderDrop,
}

#[test]
fn reply_channel_state_machine() {
    check!().with_type::<Vec<Op>>().for_each(|ops| {
        let rt = tokio::runtime::Builder::new_current_thread()
            .build()
            .expect("current-thread runtime");

        for op in ops {
            match op {
                Op::Send(v) => {
                    let (tx, rx) = reply_channel::<u64, std::convert::Infallible>();
                    assert!(tx.send(*v).is_ok(), "send to a live receiver succeeds");
                    let got = rt.block_on(rx.recv::<()>());
                    match got {
                        Ok(x) => assert_eq!(x, *v, "the receiver gets the sent reply"),
                        Err(e) => panic!("Send: expected Ok({v}), got Err({e:?})"),
                    }
                }
                Op::SendErr(e) => {
                    let (tx, rx) = reply_channel::<u64, String>();
                    assert!(
                        tx.send_err(e.clone()).is_ok(),
                        "send_err to a live receiver succeeds"
                    );
                    let got = rt.block_on(rx.recv::<()>());
                    match got {
                        Err(AskError::Handler(actual)) => {
                            assert_eq!(actual, *e, "the receiver gets the typed handler error")
                        }
                        other => panic!("SendErr: expected Handler({e:?}), got {other:?}"),
                    }
                }
                Op::SendAfterReceiverDrop(v) => {
                    let (tx, rx) = reply_channel::<u64, std::convert::Infallible>();
                    drop(rx);
                    assert!(
                        tx.send(*v).is_err(),
                        "send to a dropped receiver is AskerGone"
                    );
                }
                Op::RecvAfterSenderDrop => {
                    let (tx, rx) = reply_channel::<u64, std::convert::Infallible>();
                    drop(tx);
                    let got = rt.block_on(rx.recv::<()>());
                    match got {
                        Err(AskError::Interrupted) => {}
                        other => {
                            panic!("RecvAfterSenderDrop: expected Interrupted, got {other:?}")
                        }
                    }
                }
            }
        }
    });
}
