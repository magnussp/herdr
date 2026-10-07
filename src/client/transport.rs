use super::*;

pub(super) fn start_endpoint_transport(
    stream: LocalStream,
    lifetime: impl Send + 'static,
    event_tx: &tokio::sync::mpsc::Sender<ClientLoopEvent>,
    endpoint_id: endpoint::ClientEndpointId,
    generation: u64,
    max_frame_size: usize,
    surface_decoder: Option<protocol::surface_reuse::Decoder>,
) -> Result<endpoint::NativeEndpointTransport, ClientError> {
    let reader = stream.try_clone().map_err(ClientError::ConnectionFailed)?;
    let transport = endpoint::NativeEndpointTransport::with_lifetime(stream, lifetime)
        .map_err(ClientError::ConnectionFailed)?;
    let stopped = transport.stop_handle();
    let event_tx = event_tx.clone();
    std::thread::Builder::new()
        .name("endpoint-reader".into())
        .spawn(move || {
            server_reader_thread(
                reader,
                event_tx,
                &stopped,
                max_frame_size,
                endpoint_id,
                generation,
                surface_decoder,
            );
        })
        .map_err(ClientError::ConnectionFailed)?;
    Ok(transport)
}

/// Wire bytes of decoded server messages that the client loop may leave
/// unhandled before the reader stops draining the socket. Handling a frame can
/// block on the host terminal. Reading further ahead would queue superseded
/// frames here instead of letting the server's latest-wins render slot drop them.
const READ_AHEAD_BYTES: usize = 1024 * 1024;
const READ_AHEAD_STOP_POLL: Duration = Duration::from_millis(50);

#[derive(Default)]
struct ReadAhead {
    unhandled: std::sync::Mutex<usize>,
    handled: std::sync::Condvar,
}

/// Charges a server message's wire bytes to its reader until the client loop
/// drops the message event.
pub(super) struct ReadAheadCredit {
    budget: Arc<ReadAhead>,
    bytes: usize,
}

impl Drop for ReadAheadCredit {
    fn drop(&mut self) {
        let mut unhandled = self.budget.lock();
        *unhandled = unhandled.saturating_sub(self.bytes);
        self.budget.handled.notify_all();
    }
}

impl ReadAhead {
    fn lock(&self) -> std::sync::MutexGuard<'_, usize> {
        self.unhandled
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    /// Waits until the unhandled bytes fit the budget; false once stopped.
    fn wait_for_room(&self, stopped: &AtomicBool) -> bool {
        let mut unhandled = self.lock();
        while *unhandled >= READ_AHEAD_BYTES {
            if stopped.load(Ordering::Acquire) {
                return false;
            }
            unhandled = match self.handled.wait_timeout(unhandled, READ_AHEAD_STOP_POLL) {
                Ok((guard, _)) => guard,
                Err(poisoned) => poisoned.into_inner().0,
            };
        }
        true
    }

    fn credit(self: &Arc<Self>, bytes: usize) -> ReadAheadCredit {
        *self.lock() += bytes;
        ReadAheadCredit {
            budget: Arc::clone(self),
            bytes,
        }
    }
}

/// Reads complete frames while retaining partial-read progress across nonblocking polls.
pub(super) fn server_reader_thread(
    mut stream: LocalStream,
    event_tx: tokio::sync::mpsc::Sender<ClientLoopEvent>,
    should_quit: &Arc<AtomicBool>,
    max_frame_size: usize,
    endpoint_id: endpoint::ClientEndpointId,
    generation: u64,
    mut surface_decoder: Option<protocol::surface_reuse::Decoder>,
) {
    if stream.set_nonblocking(true).is_err() {
        let _ = event_tx.blocking_send(ClientLoopEvent::ServerDisconnected {
            endpoint_id,
            generation,
        });
        return;
    }

    let mut stream = EndpointReader {
        stream: &mut stream,
        stopped: should_quit,
        read: 0,
    };
    let read_ahead = Arc::new(ReadAhead::default());
    loop {
        if should_quit.load(Ordering::Acquire) || !read_ahead.wait_for_room(should_quit) {
            break;
        }

        let message = protocol::read_message(&mut stream, max_frame_size).and_then(|message| {
            match &mut surface_decoder {
                Some(decoder) => decoder.decode(message).map_err(|error| {
                    protocol::FramingError::Io(io::Error::new(io::ErrorKind::InvalidData, error))
                }),
                None => Ok(message),
            }
        });
        match message {
            Ok(msg) => {
                let credit = read_ahead.credit(std::mem::take(&mut stream.read));
                if event_tx
                    .blocking_send(ClientLoopEvent::ServerMessage {
                        endpoint_id: endpoint_id.clone(),
                        generation,
                        message: Box::new(msg),
                        credit,
                    })
                    .is_err()
                {
                    break;
                }
            }
            Err(protocol::FramingError::UnexpectedEof) => {
                let _ = event_tx.blocking_send(ClientLoopEvent::ServerDisconnected {
                    endpoint_id: endpoint_id.clone(),
                    generation,
                });
                break;
            }
            Err(protocol::FramingError::Io(err)) if err.kind() == io::ErrorKind::WouldBlock => {
                std::thread::sleep(Duration::from_millis(1));
            }
            Err(err) => {
                warn!(err = %err, "server read error");
                let _ = event_tx.blocking_send(ClientLoopEvent::ServerDisconnected {
                    endpoint_id: endpoint_id.clone(),
                    generation,
                });
                break;
            }
        }
    }
}

struct EndpointReader<'a> {
    stream: &'a mut LocalStream,
    stopped: &'a AtomicBool,
    /// Bytes read since the last complete message was charged.
    read: usize,
}

impl io::Read for EndpointReader<'_> {
    fn read(&mut self, buffer: &mut [u8]) -> io::Result<usize> {
        loop {
            if self.stopped.load(Ordering::Acquire) {
                return Ok(0);
            }
            match crate::ipc::poll_local_stream_read_count(self.stream, buffer)? {
                crate::ipc::LocalStreamReadCount::Data(count) => {
                    self.read = self.read.saturating_add(count);
                    return Ok(count);
                }
                crate::ipc::LocalStreamReadCount::Closed => return Ok(0),
                crate::ipc::LocalStreamReadCount::Pending => {
                    crate::platform::wait_client_stream_readable(self.stream)?;
                }
            }
        }
    }
}

pub(in crate::client) fn write_to_local_server(
    stream: &mut LocalStream,
    msg: &ClientMessage,
) -> io::Result<()> {
    protocol::write_message(stream, msg).map_err(|error| io::Error::other(error.to_string()))
}

pub(super) trait ClientMessageSink {
    fn send_client_message(&mut self, message: &ClientMessage) -> io::Result<()>;
}

impl ClientMessageSink for LocalStream {
    fn send_client_message(&mut self, message: &ClientMessage) -> io::Result<()> {
        write_to_local_server(self, message)
    }
}

impl ClientMessageSink for endpoint::EndpointRegistry {
    fn send_client_message(&mut self, message: &ClientMessage) -> io::Result<()> {
        // The lifecycle loop consumes failures for every endpoint, including Local. A send
        // failure must not bypass that transition or tear down unrelated connections.
        self.send(message);
        Ok(())
    }
}

pub(super) fn write_to_server(
    stream: &mut impl ClientMessageSink,
    msg: &ClientMessage,
) -> io::Result<()> {
    stream.send_client_message(msg)
}

#[cfg(all(test, unix))]
mod tests {
    use super::*;
    use crate::client::endpoint::EndpointTransport as _;
    use interprocess::local_socket::traits::Listener as _;
    use std::io::{Read as _, Write as _};
    use std::time::Instant;

    #[test]
    fn read_ahead_budget_waits_until_handled_messages_release_bytes() {
        let budget = Arc::new(ReadAhead::default());
        let stopped = AtomicBool::new(true); // Turns a wait into an observable refusal.
        let small = budget.credit(READ_AHEAD_BYTES - 1);
        assert!(budget.wait_for_room(&stopped));
        let large = budget.credit(1);
        assert!(!budget.wait_for_room(&stopped));
        drop(small);
        assert!(budget.wait_for_room(&stopped));
        drop(large);
        assert_eq!(*budget.lock(), 0);
    }

    #[test]
    fn reader_stops_draining_while_large_frames_are_unhandled() {
        let path = std::env::temp_dir().join(format!(
            "herdr-read-ahead-{}-{}.sock",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        let listener = crate::ipc::bind_private_local_listener(&path).unwrap();
        let client = crate::ipc::connect_local_stream(&path).unwrap();
        let mut server = listener.accept().unwrap();
        std::fs::remove_file(path).unwrap();
        drop(listener);
        // Each frame is over half the budget: two unhandled frames exhaust it.
        let frame_len = READ_AHEAD_BYTES / 2 + 1;
        let writer = std::thread::spawn(move || {
            for fill in 0..3u8 {
                let message = ServerMessage::Graphics {
                    bytes: vec![fill; frame_len],
                };
                protocol::write_message(&mut server, &message).unwrap();
            }
        });
        let (event_tx, mut event_rx) = tokio::sync::mpsc::channel(256);
        let quit = Arc::new(AtomicBool::new(false));
        let reader_quit = Arc::clone(&quit);
        let reader = std::thread::spawn(move || {
            server_reader_thread(
                client,
                event_tx,
                &reader_quit,
                protocol::MAX_GRAPHICS_FRAME_SIZE,
                endpoint::ClientEndpointId::Local,
                1,
                None,
            );
        });
        fn next_graphics(
            events: &mut tokio::sync::mpsc::Receiver<ClientLoopEvent>,
        ) -> (u8, ReadAheadCredit) {
            match events.blocking_recv() {
                Some(ClientLoopEvent::ServerMessage {
                    message, credit, ..
                }) => match *message {
                    ServerMessage::Graphics { bytes } => (bytes[0], credit),
                    _ => panic!("unexpected server message"),
                },
                _ => panic!("expected a server message event"),
            }
        }
        let first = next_graphics(&mut event_rx);
        let second = next_graphics(&mut event_rx);
        assert_eq!((first.0, second.0), (0, 1));
        // Without the budget the third frame, already written, arrives here.
        std::thread::sleep(Duration::from_millis(200));
        assert!(event_rx.try_recv().is_err());
        drop(first);
        let third = next_graphics(&mut event_rx);
        assert_eq!(third.0, 2);
        writer.join().unwrap();
        quit.store(true, Ordering::Release);
        drop((second, third));
        reader.join().unwrap();
    }

    #[test]
    fn upload_cancellation_preserves_pending_endpoint_download() {
        let path = std::env::temp_dir().join(format!(
            "herdr-cancel-{}-{}.sock",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        let listener = crate::ipc::bind_private_local_listener(&path).unwrap();
        let client = crate::ipc::connect_local_stream(&path).unwrap();
        let mut bridge = listener.accept().unwrap();
        std::fs::remove_file(path).unwrap();
        drop(listener);
        let mut reader_stream = client.try_clone().unwrap();
        let mut writer = endpoint::NativeEndpointTransport::with_lifetime(client, ()).unwrap();
        let stopped = writer.stop_handle();
        struct ForwardedInput(std::sync::mpsc::Sender<Vec<u8>>);
        impl io::Write for ForwardedInput {
            fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
                self.0.send(bytes.to_vec()).unwrap();
                Ok(bytes.len())
            }

            fn flush(&mut self) -> io::Result<()> {
                Ok(())
            }
        }
        let (forwarded_tx, forwarded_rx) = std::sync::mpsc::channel();
        let cancel = crate::remote::bridge_upload_cancellation_for_test(
            bridge.try_clone().unwrap(),
            ForwardedInput(forwarded_tx),
        );
        let message = ClientMessage::ClientShellFocus { focused: false };
        let mut expected = Vec::new();
        protocol::write_message(&mut expected, &message).unwrap();
        writer.send(&message).unwrap();
        let mut forwarded = Vec::new();
        while forwarded.len() < expected.len() {
            forwarded.extend(forwarded_rx.recv_timeout(Duration::from_secs(3)).unwrap());
        }
        assert_eq!(forwarded, expected);
        cancel();

        // A client write after upload cancellation must not stop the download reader.
        writer
            .send(&ClientMessage::ClientShellFocus { focused: true })
            .unwrap();
        let flushed = writer.flush(Instant::now() + Duration::from_secs(3));
        if flushed.is_ok() {
            let received: ClientMessage =
                protocol::read_message(&mut bridge, protocol::MAX_FRAME_SIZE).unwrap();
            assert_eq!(received, ClientMessage::ClientShellFocus { focused: true });
        }
        const FINAL: &[u8] = b"pending-download: FINAL OUTPUT\n";
        bridge.write_all(FINAL).unwrap();
        drop(bridge);
        let mut output = Vec::new();
        EndpointReader {
            stream: &mut reader_stream,
            stopped: &stopped,
            read: 0,
        }
        .read_to_end(&mut output)
        .unwrap();
        assert_eq!(output, FINAL);
        assert!(flushed.is_ok(), "client write failed: {flushed:?}");
        assert!(!stopped.load(Ordering::Acquire));
        assert!(writer.take_error().is_none());
    }
}
