//! Per-connection plumbing for the stream driver: the split-half socket tasks and the channel
//! frames they exchange with the run loop.
//!
//! The readiness sibling of sailing-compio's bridge — same SHAPE (one shared inbound channel tagged
//! by [`ConnId`], a per-conn outbound FIFO, a dial completion carrying the writer receiver back), but
//! the `Send` work-stealing types: `flume` channels and an `Arc<AtomicUsize>` byte counter, never
//! compio's thread-per-core `lochan` + `Rc<Cell>`, and borrowed-buffer readiness I/O
//! (`futures_util::io`) rather than compio's owned-buffer completion.

use std::sync::{
  Arc,
  atomic::{AtomicUsize, Ordering},
};

use agnostic::{Runtime, net::Net};
use bytes::Bytes;
use futures_util::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use sailing_proto::ConnId;

/// One read buffer's size per connection-reader task.
const RECV_BUF_LEN: usize = 64 * 1024;

/// The connected TCP stream type of the runtime `R`'s network.
pub(crate) type StreamOf<R> = <<R as Runtime>::Net as Net>::TcpStream;

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
/// close). A graceful close is signalled by DROPPING the sender: the writer's `recv_async` resolves
/// `Err` only after draining the bytes queued ahead of it. One frame type keeps the queue's byte
/// accounting unambiguous.
pub(crate) struct BridgeOut(pub(crate) Bytes);

/// A dial completion handed back to the run loop. The connection was REGISTERED with the
/// coordinator at dial time (its queued handshake bytes must not be lost while the socket
/// connects); on success the writer-side receiver and the shared byte counter ride back so the
/// bridge — not the connect task — owns them.
pub(crate) struct DialReady<R: Runtime> {
  /// The coordinator-assigned id this dial was registered under.
  pub(crate) id: ConnId,
  /// The connected socket, or the dial failure (the run loop closes + schedules the redial).
  pub(crate) result: std::io::Result<StreamOf<R>>,
  /// The outbound receiver the writer drains (moved into the connect task at dial time, shipped
  /// back on completion so the bridge — not the connect task — owns it).
  pub(crate) out_rx: flume::Receiver<BridgeOut>,
  /// The same byte counter the run loop's `Conn` holds: the run loop adds on enqueue, the writer
  /// subtracts as it writes.
  pub(crate) queued_bytes: Arc<AtomicUsize>,
}

/// The live task(s) the driver holds for one connection. Dropping it ABORTS whatever is running:
/// every handle is an [`AbortOnDrop`], so dropping a `Connecting` aborts the dial task and dropping
/// a `Bridged` aborts BOTH split-half tasks — preempting a stuck write and releasing the socket
/// halves the tasks own.
///
/// The handles are held ONLY so their drop performs that abort; they are never read again (the
/// `#[allow(dead_code)]` records that drop is the load-bearing use, not the value).
///
/// [`AbortOnDrop`]: crate::task::AbortOnDrop
pub(crate) enum ConnTask<R: Runtime> {
  /// The dial/connect task, until it completes.
  #[allow(dead_code)]
  Connecting(crate::task::AbortOnDrop<R>),
  /// The two independent split-half tasks (each owns one half via `into_split`, so a large write
  /// never starves the reader). Either half's EOF/error leads the run loop to `close_conn`, which
  /// drops this `Bridged` and so aborts the OTHER half.
  #[allow(dead_code)]
  Bridged {
    /// The reader half task.
    read: crate::task::AbortOnDrop<R>,
    /// The writer half task.
    write: crate::task::AbortOnDrop<R>,
  },
}

/// Everything the driver owns for one connection. Dropping it tears the connection down:
/// the task drop aborts the live task(s), and dropping `out_tx` signals a still-running
/// writer to flush-then-exit.
pub(crate) struct Conn<R: Runtime, I> {
  /// The connection's live task(s): the dial task until it succeeds, then the two bridge halves.
  pub(crate) tasks: ConnTask<R>,
  /// Outbound wire bytes to the writer (the per-conn FIFO).
  pub(crate) out_tx: flume::Sender<BridgeOut>,
  /// Bytes enqueued toward the socket and not yet written — the per-conn memory bound.
  pub(crate) queued_bytes: Arc<AtomicUsize>,
  /// `Some(peer)` for a dialed conn — the reconciler's dial-in-flight marker — and `None` for
  /// an accepted one. Repair scheduling itself lives in the reconciler, never on the conn.
  pub(crate) dialed_to: Option<I>,
}

/// Read one socket half until EOF/error, tagging every chunk onto the shared inbound channel.
/// Runs on its own task so a concurrent large write cannot stop this half from reading. One
/// buffer for the task's lifetime: a readiness read borrows it per call, so every iteration
/// re-lends the same 64 KiB allocation instead of zeroing a fresh one per read. A full inbound
/// channel parks `send_async`, which stops reading — kernel TCP backpressure on exactly the
/// flooding peer.
pub(crate) async fn bridge_read(
  mut read_half: impl AsyncRead + Unpin,
  id: ConnId,
  inbound: flume::Sender<BridgeInbound>,
) {
  let mut buf = vec![0u8; RECV_BUF_LEN];
  loop {
    match read_half.read(&mut buf).await {
      Ok(0) => {
        let _ = inbound.send_async(BridgeInbound::Eof { id }).await;
        return;
      }
      Ok(n) => {
        if inbound
          .send_async(BridgeInbound::Bytes {
            id,
            // Only the exactly-`n`-sized copy is allocated per chunk (the channel needs an owned
            // `Vec`; an oversize forward would carry the buffer's stale tail).
            bytes: buf[..n].to_vec(),
          })
          .await
          .is_err()
        {
          return; // the run loop dropped its receiver: teardown
        }
      }
      Err(_) => {
        let _ = inbound.send_async(BridgeInbound::Error { id }).await;
        return;
      }
    }
  }
}

/// Drain the per-conn FIFO into one socket half. Exits when the sender side drops (draining what
/// was queued ahead, BEST-EFFORT: the run loop's close path aborts this task outright — queued
/// frames are then discarded, which is safe because consensus retransmission re-drives them; a
/// guaranteed drain would have unbounded lifetime under a peer that never reads) or when the socket
/// fails (reported tagged; the run loop tears the connection down).
///
/// Each chunk is written whole via `write_all`, then `queued_bytes` is released by the chunk's FULL
/// length — exactly once, on completion or on a terminal error. The `Bytes` stays resident for the
/// entire write, so the charge must track RETAINED bytes: releasing per partial write would
/// under-count resident memory and let the gate admit another full backlog while this chunk is still
/// in memory. This mirrors the compio writer's whole-chunk `len` release (only the buffer ownership
/// model differs: borrowed slice vs owned buffer).
pub(crate) async fn bridge_write(
  mut write_half: impl AsyncWrite + Unpin,
  id: ConnId,
  out: flume::Receiver<BridgeOut>,
  queued_bytes: Arc<AtomicUsize>,
  inbound: flume::Sender<BridgeInbound>,
) {
  loop {
    match out.recv_async().await {
      Ok(BridgeOut(bytes)) => {
        // Drop the allocation BEFORE the decrement: the error report below can PARK on a full inbound
        // channel, so a charge released while `bytes` were still resident would let `pump` admit another
        // backlog against this conn over the cap. `write_all` maps a no-progress write to an error, so
        // Ok(0) needs no case.
        let total = bytes.len();
        let res = write_half.write_all(&bytes).await;
        drop(bytes);
        queued_bytes.fetch_sub(total, Ordering::Relaxed);
        if res.is_err() {
          let _ = inbound.send_async(BridgeInbound::Error { id }).await;
          return;
        }
      }
      // The run loop dropped `out_tx` (it dropped this conn's `Conn`): the FIFO has flushed, so
      // flush the socket and exit.
      Err(_) => {
        let _ = write_half.flush().await;
        return;
      }
    }
  }
}

#[cfg(test)]
mod tests {
  use std::{
    pin::Pin,
    sync::{
      Arc, Mutex,
      atomic::{AtomicBool, AtomicUsize, Ordering},
    },
    task::{Context, Poll},
    time::Duration,
  };

  use bytes::Bytes;
  use futures_util::io::{AsyncRead, AsyncWrite};
  use sailing_proto::ConnId;

  use super::{BridgeInbound, BridgeOut, bridge_read, bridge_write};

  /// An `AsyncRead` whose every read reports a clean EOF (`Ok(0)`).
  struct EofReader;
  impl AsyncRead for EofReader {
    fn poll_read(
      self: Pin<&mut Self>,
      _cx: &mut Context<'_>,
      _buf: &mut [u8],
    ) -> Poll<std::io::Result<usize>> {
      Poll::Ready(Ok(0))
    }
  }

  /// An `AsyncRead` whose every read fails.
  struct ErrReader;
  impl AsyncRead for ErrReader {
    fn poll_read(
      self: Pin<&mut Self>,
      _cx: &mut Context<'_>,
      _buf: &mut [u8],
    ) -> Poll<std::io::Result<usize>> {
      Poll::Ready(Err(std::io::Error::other("read boom")))
    }
  }

  /// An `AsyncRead` that always yields a few bytes — to drive the chunk-forward path.
  struct DataReader;
  impl AsyncRead for DataReader {
    fn poll_read(
      self: Pin<&mut Self>,
      _cx: &mut Context<'_>,
      buf: &mut [u8],
    ) -> Poll<std::io::Result<usize>> {
      let n = buf.len().min(4);
      buf[..n].fill(7);
      Poll::Ready(Ok(n))
    }
  }

  /// A clean peer EOF (`read` → `Ok(0)`) is reported as a tagged `Eof` frame, then the reader returns.
  #[tokio::test]
  async fn bridge_read_reports_eof_then_returns() {
    let (tx, rx) = flume::unbounded();
    bridge_read(EofReader, ConnId(7), tx).await;
    assert!(matches!(
      rx.try_recv(),
      Ok(BridgeInbound::Eof { id: ConnId(7) })
    ));
  }

  /// A read error is reported as a tagged `Error` frame, then the reader returns.
  #[tokio::test]
  async fn bridge_read_reports_a_read_error_then_returns() {
    let (tx, rx) = flume::unbounded();
    bridge_read(ErrReader, ConnId(9), tx).await;
    assert!(matches!(
      rx.try_recv(),
      Ok(BridgeInbound::Error { id: ConnId(9) })
    ));
  }

  /// When the run loop has dropped its inbound receiver, the reader's tagged-chunk `send_async` fails
  /// and the reader returns (teardown) rather than spinning — even though the socket keeps yielding
  /// bytes.
  #[tokio::test]
  async fn bridge_read_returns_when_the_run_loop_dropped_the_receiver() {
    let (tx, rx) = flume::unbounded::<BridgeInbound>();
    drop(rx); // the run loop is gone: the very first chunk-forward fails
    // Returns promptly (a `timeout` would fire if it spun); the unit return type asserts completion.
    tokio::time::timeout(
      std::time::Duration::from_secs(5),
      bridge_read(DataReader, ConnId(1), tx),
    )
    .await
    .expect("bridge_read returns once its inbound receiver is gone");
  }

  /// An `AsyncWrite` that accepts ONE byte per poll and records the `queued_bytes` value observed on
  /// each write, so the test can prove the per-connection byte budget is charged WHOLE-CHUNK (constant
  /// until the frame finishes) rather than released per partial write.
  struct OneBytePartialWriter {
    queued: Arc<AtomicUsize>,
    samples: Arc<Mutex<Vec<usize>>>,
  }

  impl AsyncWrite for OneBytePartialWriter {
    fn poll_write(
      self: Pin<&mut Self>,
      _cx: &mut Context<'_>,
      buf: &[u8],
    ) -> Poll<std::io::Result<usize>> {
      self
        .samples
        .lock()
        .unwrap()
        .push(self.queued.load(Ordering::Relaxed));
      Poll::Ready(Ok(buf.len().min(1)))
    }
    fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
      Poll::Ready(Ok(()))
    }
    fn poll_close(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
      Poll::Ready(Ok(()))
    }
  }

  /// Under a slow socket that accepts one byte at a time, the frame's whole `Bytes` allocation stays
  /// resident, so `queued_bytes` must stay at the full chunk size for the entire drain and drop to zero
  /// only on completion. A per-partial-write release would under-count resident memory and let `pump`
  /// admit another full backlog while the chunk is still in memory.
  #[tokio::test]
  async fn outbound_bytes_are_charged_whole_chunk_until_the_frame_completes() {
    let total = 64usize;
    // The run loop charges the frame on enqueue; the writer is what releases it.
    let queued = Arc::new(AtomicUsize::new(total));
    let samples = Arc::new(Mutex::new(Vec::new()));
    let (out_tx, out_rx) = flume::unbounded();
    let (inbound_tx, _inbound_rx) = flume::unbounded();

    out_tx
      .send(BridgeOut(Bytes::from(vec![7u8; total])))
      .unwrap();
    drop(out_tx); // the writer exits once the single frame drains

    let writer = OneBytePartialWriter {
      queued: queued.clone(),
      samples: samples.clone(),
    };
    bridge_write(writer, ConnId(0), out_rx, queued.clone(), inbound_tx).await;

    let samples = samples.lock().unwrap();
    assert_eq!(
      samples.len(),
      total,
      "one byte per write means `total` partial writes"
    );
    assert!(
      samples.iter().all(|&s| s == total),
      "queued_bytes must stay at the full chunk size while the frame drains, not decrement per partial \
       write: {samples:?}"
    );
    assert_eq!(
      queued.load(Ordering::Relaxed),
      0,
      "the whole charge is released once the frame completes"
    );
  }

  /// A FAILED write must drop the chunk before releasing its charge, even when the error report itself
  /// parks on a saturated inbound channel: a chunk left resident but uncharged would let `pump` admit
  /// another backlog against the same connection, exceeding the per-connection memory cap.
  #[tokio::test]
  async fn a_failed_write_drops_the_chunk_before_releasing_its_charge() {
    // A buffer that flips a flag when its backing allocation is dropped.
    struct TrackedBuf {
      data: Vec<u8>,
      dropped: Arc<AtomicBool>,
    }
    impl AsRef<[u8]> for TrackedBuf {
      fn as_ref(&self) -> &[u8] {
        &self.data
      }
    }
    impl Drop for TrackedBuf {
      fn drop(&mut self) {
        self.dropped.store(true, Ordering::Relaxed);
      }
    }

    // A writer whose every write fails.
    struct FailingWriter;
    impl AsyncWrite for FailingWriter {
      fn poll_write(
        self: Pin<&mut Self>,
        _cx: &mut Context<'_>,
        _buf: &[u8],
      ) -> Poll<std::io::Result<usize>> {
        Poll::Ready(Err(std::io::Error::other("boom")))
      }
      fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        Poll::Ready(Ok(()))
      }
      fn poll_close(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        Poll::Ready(Ok(()))
      }
    }

    let total = 32usize;
    let queued = Arc::new(AtomicUsize::new(total));
    let dropped = Arc::new(AtomicBool::new(false));
    let (out_tx, out_rx) = flume::unbounded();
    // A saturated inbound channel: a zero-capacity rendezvous no one receives, so the error report parks.
    let (inbound_tx, inbound_rx) = flume::bounded(0);

    let bytes = Bytes::from_owner(TrackedBuf {
      data: vec![0u8; total],
      dropped: dropped.clone(),
    });
    out_tx.send(BridgeOut(bytes)).unwrap();
    drop(out_tx);

    let q = queued.clone();
    let writer = tokio::spawn(async move {
      bridge_write(FailingWriter, ConnId(0), out_rx, q, inbound_tx).await;
    });

    // Let the writer reach its parked error report (the rendezvous has no receiver yet).
    tokio::time::sleep(Duration::from_millis(100)).await;

    assert!(
      dropped.load(Ordering::Relaxed),
      "the failed chunk must be dropped before the parked error report, not retained while uncharged"
    );
    assert_eq!(
      queued.load(Ordering::Relaxed),
      0,
      "the whole charge is released on the error path"
    );

    drop(inbound_rx); // unpark the error report so the writer can exit
    let _ = writer.await;
  }
}
