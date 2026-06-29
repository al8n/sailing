//! Per-connection plumbing for the stream driver: the split-half socket tasks and the channel
//! frames they exchange with the run loop.

use std::{cell::Cell, rc::Rc};

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
  pub(crate) queued_bytes: Rc<Cell<usize>>,
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
  queued_bytes: Rc<Cell<usize>>,
  inbound: lochan::mpsc::Sender<BridgeInbound>,
) {
  while let Some(BridgeOut(bytes)) = out.recv().await {
    let len = bytes.len();
    let BufResult(res, _) = half.write_all(bytes).await;
    queued_bytes.set(queued_bytes.get() - len);
    if res.is_err() {
      let _ = inbound.send(BridgeInbound::Error { id }).await;
      return;
    }
  }
  // Sender dropped: the run loop closed this connection gracefully; flush and exit.
  let _ = half.flush().await;
}

#[cfg(test)]
mod tests {
  use std::{cell::Cell, rc::Rc};

  use bytes::Bytes;
  use compio::{
    buf::{BufResult, IoBuf, IoBufMut},
    io::{AsyncRead, AsyncWrite},
  };
  use sailing_proto::ConnId;

  use super::{BridgeInbound, BridgeOut, bridge_read, bridge_write};

  /// A reader whose every read reports a clean EOF (`Ok(0)`). The owned buffer rides back untouched —
  /// the EOF arm never inspects it.
  struct EofReader;
  impl AsyncRead for EofReader {
    async fn read<B: IoBufMut>(&mut self, buf: B) -> BufResult<usize, B> {
      BufResult(Ok(0), buf)
    }
  }

  /// A reader whose every read fails.
  struct ErrReader;
  impl AsyncRead for ErrReader {
    async fn read<B: IoBufMut>(&mut self, buf: B) -> BufResult<usize, B> {
      BufResult(Err(std::io::Error::other("read boom")), buf)
    }
  }

  /// A reader that always yields a few bytes (the run loop's recv buffer is pre-zeroed, so the
  /// chunk-forward path needs no buffer fill here).
  struct DataReader;
  impl AsyncRead for DataReader {
    async fn read<B: IoBufMut>(&mut self, buf: B) -> BufResult<usize, B> {
      BufResult(Ok(4), buf)
    }
  }

  /// A writer that either accepts every byte at once (`fail = false`) or fails every write
  /// (`fail = true`). One fixture covers both the graceful-drain path (`write` + the `flush` a dropped
  /// sender drives) and the error path, so the trait's `shutdown` — which `bridge_write` never calls —
  /// is the only uncovered stub, counted once.
  struct TestWriter {
    fail: bool,
  }
  impl AsyncWrite for TestWriter {
    async fn write<T: IoBuf>(&mut self, buf: T) -> BufResult<usize, T> {
      if self.fail {
        BufResult(Err(std::io::Error::other("write boom")), buf)
      } else {
        let n = buf.buf_len();
        BufResult(Ok(n), buf)
      }
    }
    async fn flush(&mut self) -> std::io::Result<()> {
      Ok(())
    }
    async fn shutdown(&mut self) -> std::io::Result<()> {
      Ok(())
    }
  }

  /// A clean peer EOF is reported as a tagged `Eof` frame, then the reader returns.
  #[compio::test]
  async fn bridge_read_reports_eof_then_returns() {
    let (tx, mut rx) = lochan::mpsc::unbounded();
    bridge_read(EofReader, ConnId(7), tx).await;
    assert!(matches!(
      rx.recv().await,
      Some(BridgeInbound::Eof { id: ConnId(7) })
    ));
  }

  /// A read error is reported as a tagged `Error` frame, then the reader returns.
  #[compio::test]
  async fn bridge_read_reports_a_read_error_then_returns() {
    let (tx, mut rx) = lochan::mpsc::unbounded();
    bridge_read(ErrReader, ConnId(9), tx).await;
    assert!(matches!(
      rx.recv().await,
      Some(BridgeInbound::Error { id: ConnId(9) })
    ));
  }

  /// When the run loop dropped its inbound receiver, the reader's tagged-chunk `send` fails and the
  /// reader returns (teardown) rather than spinning.
  #[compio::test]
  async fn bridge_read_returns_when_the_run_loop_dropped_the_receiver() {
    let (tx, rx) = lochan::mpsc::unbounded::<BridgeInbound>();
    drop(rx); // the run loop is gone: the first chunk-forward fails
    compio::time::timeout(
      std::time::Duration::from_secs(5),
      bridge_read(DataReader, ConnId(1), tx),
    )
    .await
    .expect("bridge_read returns once its inbound receiver is gone");
  }

  /// The whole-chunk byte release: the writer drains one frame, `queued_bytes` returns to zero, and a
  /// dropped sender drives the graceful flush-and-exit.
  #[compio::test]
  async fn bridge_write_releases_the_whole_chunk_then_exits_on_sender_drop() {
    let total = 64usize;
    let queued = Rc::new(Cell::new(total));
    let (out_tx, out_rx) = lochan::mpsc::unbounded();
    let (inbound_tx, _inbound_rx) = lochan::mpsc::unbounded();
    out_tx
      .send(BridgeOut(Bytes::from(vec![7u8; total])))
      .await
      .expect("enqueue");
    drop(out_tx); // the writer exits once the single frame drains
    bridge_write(
      TestWriter { fail: false },
      ConnId(0),
      out_rx,
      queued.clone(),
      inbound_tx,
    )
    .await;
    assert_eq!(
      queued.get(),
      0,
      "the whole chunk's bytes are released once the frame is written"
    );
  }

  /// A failed write reports a tagged `Error` frame and releases the chunk's bytes before returning.
  #[compio::test]
  async fn bridge_write_reports_an_error_and_releases_the_charge() {
    let total = 32usize;
    let queued = Rc::new(Cell::new(total));
    let (out_tx, out_rx) = lochan::mpsc::unbounded();
    let (inbound_tx, mut inbound_rx) = lochan::mpsc::unbounded();
    out_tx
      .send(BridgeOut(Bytes::from(vec![0u8; total])))
      .await
      .expect("enqueue");
    bridge_write(
      TestWriter { fail: true },
      ConnId(3),
      out_rx,
      queued.clone(),
      inbound_tx,
    )
    .await;
    assert_eq!(queued.get(), 0, "the charge is released on the error path");
    assert!(matches!(
      inbound_rx.recv().await,
      Some(BridgeInbound::Error { id: ConnId(3) })
    ));
  }
}
