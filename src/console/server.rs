//! TCP server that streams snapshots to a connected console client.
//!
//! Serving is explicit and pull-based: the client opens a connection and sends a one-byte
//! request whenever it wants a fresh snapshot; the server responds with a length-prefixed,
//! MessagePack-encoded [`wire::Message`]. Nothing is collected until a client asks, so an
//! idle server does no periodic work.

use std::{io, net::SocketAddr, time::Duration};

use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream, ToSocketAddrs},
    task::JoinHandle,
};

use super::{registry, wire};

const DEFAULT_GRAVE_WINDOW: Duration = Duration::from_secs(5);

/// Builder for a console server.
#[derive(Debug, Clone)]
pub struct Console {
    grave_window: Duration,
}

impl Default for Console {
    fn default() -> Self {
        Console {
            grave_window: DEFAULT_GRAVE_WINDOW,
        }
    }
}

impl Console {
    /// Creates a console builder with default settings.
    pub fn builder() -> Console {
        Console::default()
    }

    /// Sets how long a stopped actor lingers in snapshots before being dropped.
    ///
    /// Defaults to 5 seconds. Supervised actors that restart keep their id and never appear
    /// as stopped, so this only affects actors that truly terminate.
    pub fn grave_window(mut self, grave_window: Duration) -> Self {
        self.grave_window = grave_window;
        self
    }

    /// Binds the given address and starts serving snapshots in a background task.
    pub async fn serve(self, addr: impl ToSocketAddrs) -> io::Result<ConsoleHandle> {
        let listener = TcpListener::bind(addr).await?;
        let local_addr = listener.local_addr()?;
        let grave_window = self.grave_window;
        let task = tokio::spawn(async move {
            loop {
                match listener.accept().await {
                    Ok((stream, _peer)) => {
                        tokio::spawn(serve_client(stream, grave_window));
                    }
                    Err(_err) => {
                        #[cfg(feature = "tracing")]
                        tracing::warn!("console failed to accept connection: {_err}");
                    }
                }
            }
        });

        Ok(ConsoleHandle { task, local_addr })
    }
}

/// Binds `addr` and serves console snapshots with default settings.
pub async fn serve(addr: impl ToSocketAddrs) -> io::Result<ConsoleHandle> {
    Console::builder().serve(addr).await
}

/// A running console server. The server keeps running until [`ConsoleHandle::shutdown`] is
/// called or the process exits; dropping the handle leaves it running.
#[derive(Debug)]
#[must_use = "keep the handle to later call shutdown(); dropping it detaches the server, which keeps running"]
pub struct ConsoleHandle {
    task: JoinHandle<()>,
    local_addr: SocketAddr,
}

impl ConsoleHandle {
    /// The address the server is bound to. Useful when binding to port 0.
    pub fn local_addr(&self) -> SocketAddr {
        self.local_addr
    }

    /// Stops the console server.
    pub fn shutdown(self) {
        self.task.abort();
    }
}

async fn serve_client(mut stream: TcpStream, grave_window: Duration) {
    let mut request = [0u8; 1];
    loop {
        if stream.read_exact(&mut request).await.is_err() {
            break;
        }

        let message = wire::Message::Snapshot(registry::snapshot(grave_window).await);
        let bytes = match encode_snapshot(&message) {
            Ok(bytes) => bytes,
            Err(_err) => {
                #[cfg(feature = "tracing")]
                tracing::error!("console failed to encode snapshot: {_err}");
                break;
            }
        };

        let len = (bytes.len() as u32).to_be_bytes();
        if stream.write_all(&len).await.is_err() || stream.write_all(&bytes).await.is_err() {
            break;
        }
    }
}

/// Encodes a wire message to MessagePack, returning the same `rmp_serde` error the serve loop
/// treats as an encode failure (break before any write).
///
/// With the `testing` feature, [`testing::fail_next_encode`] arms a one-shot flag that makes the
/// next call return that error path without producing bytes, so a test can drive the
/// encode-failure teardown (no length prefix, no partial frame) without constructing an
/// otherwise-unencodable `Snapshot`.
fn encode_snapshot(message: &wire::Message) -> Result<Vec<u8>, rmp_serde::encode::Error> {
    #[cfg(feature = "testing")]
    if testing::take_fail_next_encode() {
        return Err(rmp_serde::encode::Error::Syntax(
            "console: encode failure injected by testing hook".to_owned(),
        ));
    }
    rmp_serde::to_vec_named(message)
}

/// Test-only hooks for driving the serve loop's encode-failure teardown path.
#[cfg(any(test, feature = "testing"))]
pub mod testing {
    use std::sync::atomic::{AtomicBool, Ordering};

    static FAIL_NEXT_ENCODE: AtomicBool = AtomicBool::new(false);

    /// Arms a one-shot flag so the next snapshot encode in any serve-client task fails, exercising
    /// the "encode error closes the connection without a partial frame" path. The flag is consumed
    /// by the first encode after it is set (see [`take_fail_next_encode`]).
    pub fn fail_next_encode() {
        FAIL_NEXT_ENCODE.store(true, Ordering::SeqCst);
    }

    /// Returns whether the one-shot encode-failure flag was armed, clearing it. Used by the serve
    /// loop; `SeqCst` pairs the cross-task arm/observe so the very next encode sees the arm.
    pub(super) fn take_fail_next_encode() -> bool {
        FAIL_NEXT_ENCODE.swap(false, Ordering::SeqCst)
    }
}
