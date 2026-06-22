//! Per-connection plumbing for the stream driver: the split-half socket tasks and the channel
//! frames they exchange with the run loop.

use std::sync::{
  Arc,
  atomic::{AtomicUsize, Ordering},
};

use bytes::Bytes;
use compio::{
  buf::BufResult,
  io::{AsyncRead, AsyncWrite, AsyncWriteExt},
  net::TcpStream,
};
use sailing_proto::ConnId;

/// One read buffer's size per connection-reader task.
const RECV_BUF_LEN: usize = 64 * 1024;

/// A connection task → the run loop (ONE shared channel; the tag carries which [`ConnId`]).
pub(crate) enum BridgeInbound {
  /// Received wire bytes.
  Bytes {
    /// The tagged connection.
    id: ConnId,
    /// The received chunk (exact-sized copy; the reader's buffer is re-lent).
    bytes: Vec<u8>,
  },
  /// The peer closed its write side (a clean EOF).
  Eof {
    /// The tagged connection.
    id: ConnId,
  },
  /// The socket failed (read or write half).
  Error {
    /// The tagged connection.
    id: ConnId,
  },
}

/// The run loop → ONE connection's writer task (a per-conn FIFO so queued bytes flush before a
/// close). A graceful close is signalled by DROPPING the sender: the writer's `recv` resolves
/// `None` only after draining the bytes queued ahead of it. One frame type keeps the queue's byte
/// accounting unambiguous.
pub(crate) struct BridgeOut(pub(crate) Bytes);

/// A dial completion handed back to the run loop. The connection was REGISTERED with the
/// coordinator at dial time (its queued handshake bytes must not be lost while the socket
/// connects); on success the writer-side receiver and the shared byte counter ride back so the
/// bridge — not the connect task — owns them.
pub(crate) struct DialReady {
  /// The coordinator-assigned id this dial was registered under.
  pub(crate) id: ConnId,
  /// The connected socket, or the dial failure (the run loop closes + schedules the redial).
  pub(crate) result: std::io::Result<TcpStream>,
  /// The outbound receiver the writer drains (moved into the connect task at dial time, shipped
  /// back on completion). A `!Send` lochan receiver, carried on-thread through the flume
  /// `DialReady` channel.
  pub(crate) out_rx: lochan::mpsc::Receiver<BridgeOut>,
  /// The same byte counter the run loop's `Conn` holds: the run loop adds on enqueue, the writer
  /// subtracts as it writes.
  pub(crate) queued_bytes: Arc<AtomicUsize>,
}

/// Read one socket half until EOF/error, tagging every chunk onto the shared inbound channel.
/// Runs on its own task so a concurrent large write cannot stop this half from reading. One
/// buffer for the task's lifetime (each completed read hands it back in the `BufResult`). A full
/// inbound channel parks `send`, which stops reading — kernel TCP backpressure on exactly the
/// flooding peer.
pub(crate) async fn bridge_read(
  mut half: impl AsyncRead,
  id: ConnId,
  inbound: lochan::mpsc::Sender<BridgeInbound>,
) {
  let mut buf = vec![0u8; RECV_BUF_LEN];
  loop {
    let BufResult(res, returned) = half.read(buf).await;
    buf = returned;
    match res {
      Ok(0) => {
        let _ = inbound.send(BridgeInbound::Eof { id }).await;
        return;
      }
      Ok(n) => {
        if inbound
          .send(BridgeInbound::Bytes {
            id,
            bytes: buf[..n].to_vec(),
          })
          .await
          .is_err()
        {
          return; // the run loop dropped its receiver: teardown
        }
      }
      Err(_) => {
        let _ = inbound.send(BridgeInbound::Error { id }).await;
        return;
      }
    }
  }
}

/// Drain the per-conn FIFO into one socket half. Exits when the sender side drops (draining
/// what was queued ahead, BEST-EFFORT: the run loop's close path cancels this task outright —
/// queued frames are then discarded, which is safe because consensus retransmission re-drives
/// them; a guaranteed drain would have unbounded lifetime under a peer that never reads) or
/// when the socket fails (reported tagged; the run loop tears the connection down).
/// `queued_bytes` is decremented as bytes are WRITTEN, so the run loop's enqueue-side budget
/// tracks genuinely-unwritten bytes.
pub(crate) async fn bridge_write(
  mut half: impl AsyncWrite,
  id: ConnId,
  mut out: lochan::mpsc::Receiver<BridgeOut>,
  queued_bytes: Arc<AtomicUsize>,
  inbound: lochan::mpsc::Sender<BridgeInbound>,
) {
  while let Some(BridgeOut(bytes)) = out.recv().await {
    let len = bytes.len();
    let BufResult(res, _) = half.write_all(bytes).await;
    queued_bytes.fetch_sub(len, Ordering::AcqRel);
    if res.is_err() {
      let _ = inbound.send(BridgeInbound::Error { id }).await;
      return;
    }
  }
  // Sender dropped: the run loop closed this connection gracefully; flush and exit.
  let _ = half.flush().await;
}
