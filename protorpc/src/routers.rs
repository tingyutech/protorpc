//! Handles routing RPC requests and responses
//!
//! A router can manage multiple services (both client and server) and multiple
//! transport layer streams at the same time.
//!
//! ### Service
//!
//! ```rust,ignore
//! let routes = Routes::new();
//!
//! routes.add_stream(stream).await;
//! routes.make_service::<proto::server::EchoServer<EchoService>>(EchoService).await;
//! ```
//!
//! For the server, routes is required, and the rpc server implementation must
//! be managed through routes. But that's not all, the client can also be
//! managed through routes, in which case the client uses the streams in the
//! transport stream pool.
//!
//! ```rust,ignore
//! let client = routes.make_service::<proto::client::EchoClient>(()).await;
//! ```
//!
//! ### Transport Layer
//!
//! routes can unconditionally accept any stream. You don't need to worry about
//! anything else, just add the stream to routes.
//!
//! For example, you can add a TCP stream to routes.
//!
//! ```rust,ignore
//! routes.add_stream(TcpStream::connect("127.0.0.1:8080")
//!               .await?
//!               .into()).await;
//! ```
//!
//! For the server:
//!
//! ```rust,ignore
//! let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
//!
//! while let Ok((socket, _)) = listener.accept().await {
//!     routes.add_stream(socket.into()).await;
//! }
//! ```

use std::{
    collections::HashMap,
    sync::{
        Arc, LazyLock,
        atomic::{AtomicU32, Ordering},
    },
};

use tokio::sync::{
    RwLock,
    mpsc::{UnboundedReceiver, UnboundedSender, unbounded_channel},
};

use crate::{
    NamedPayload, RpcServiceBuilder, proto, result::IoResult, task::spawn, transport::IOStream,
};

// Transport layer sequence number cursor
static TRASNPORT_NUMBER: LazyLock<AtomicU32> = LazyLock::new(|| AtomicU32::new(0));

/// Request/Response Router
pub struct Routes {
    drop_notify_sender: tokio::sync::broadcast::Sender<()>,
    drop_notify_receiver: tokio::sync::broadcast::Receiver<()>,

    // Frames that need to be sent to a specific transport layer can be delivered
    // through this channel
    transport_senders: Arc<RwLock<HashMap<u32, UnboundedSender<proto::Frame>>>>,
    // Frames that need to be sent to a specific service implementation can be
    // delivered through this channel
    service_senders:
        Arc<RwLock<HashMap<String, UnboundedSender<NamedPayload<IoResult<proto::Frame>>>>>>,
}

impl Routes {
    pub fn new() -> Self {
        let (drop_notify_sender, drop_notify_receiver) = tokio::sync::broadcast::channel::<()>(10);

        Self {
            transport_senders: Default::default(),
            service_senders: Default::default(),
            drop_notify_receiver,
            drop_notify_sender,
        }
    }

    /// Add a transport layer stream, used to receive and send data.
    pub async fn add_stream(
        &self,
        IOStream {
            receiver: mut readable_stream,
            sender: writable_stream,
        }: IOStream,
    ) {
        let sequence = TRASNPORT_NUMBER.fetch_add(1, Ordering::Relaxed);

        let transport_senders = self.transport_senders.clone();
        let service_senders = self.service_senders.clone();

        transport_senders
            .write()
            .await
            .insert(sequence, writable_stream);

        #[cfg(feature = "log")]
        log::info!(
            "added a transport layer, assigned sequence number = {}",
            sequence
        );

        let mut drop_notify_receiver_ = self.drop_notify_receiver.resubscribe();

        spawn(async move {
            let mut closed_service = None;
            
            'a: loop {
                tokio::select! {
                    ret = readable_stream.recv() => {
                        if let Some(Ok(frame)) = ret {
                            #[cfg(feature = "log")]
                            log::debug!("transport received a frame, number = {}, frame = {:?}", sequence, frame);

                            {
                                if let Some(sender) = service_senders.read().await.get(&frame.service) {
                                    if !sender.is_closed() {
                                        let _ = sender.send(NamedPayload {
                                            transport: sequence,
                                            payload: Ok(frame)
                                        });
                                    } else {
                                        closed_service = Some(frame.service);
                                    }
                                }
                            }

                            if let Some(service) = closed_service.take() {
                                let _ = service_senders.write().await.remove(&service);
                            }
                        } else {
                            break 'a;
                        }
                    }
                    _ = drop_notify_receiver_.recv() => {
                        #[cfg(feature = "log")]
                        log::warn!("transport exited for rpc dropped, number = {}", sequence);

                        break 'a;
                    }
                }
            }

            #[cfg(feature = "log")]
            log::warn!("routers transport closed, number = {}", sequence);

            let _ = transport_senders.write().await.remove(&sequence);

            for item in service_senders.write().await.values() {
                let _ = item.send(NamedPayload {
                    transport: sequence,
                    payload: Err(std::io::Error::new(
                        std::io::ErrorKind::ConnectionAborted,
                        "",
                    )),
                });
            }
        });
    }

    /// Start a service.
    pub async fn start_service<S: RpcServiceBuilder + Send + Sync + 'static>(
        &self,
        ctx: S::Context,
    ) -> S::Output {
        let (sender, receiver) = unbounded_channel();
        self.service_senders
            .write()
            .await
            .insert(S::NAME.to_string(), sender);

        #[cfg(feature = "log")]
        log::info!("routers added a service, service = {}", S::NAME);

        S::build(
            ctx,
            MessageStream {
                transport_senders: self.transport_senders.clone(),
                receiver,
            },
        )
        .await
    }
}

impl Drop for Routes {
    fn drop(&mut self) {
        let _ = self.drop_notify_sender.send(());

        #[cfg(feature = "log")]
        log::info!("routers dropped");
    }
}

pub struct MessageStream {
    receiver: UnboundedReceiver<NamedPayload<IoResult<proto::Frame>>>,
    transport_senders: Arc<RwLock<HashMap<u32, UnboundedSender<proto::Frame>>>>,
}

impl MessageStream {
    pub fn split(self) -> (MessageStreamSender, MessageStreamReceiver) {
        (
            MessageStreamSender(self.transport_senders),
            MessageStreamReceiver(self.receiver),
        )
    }
}

#[derive(Clone)]
pub struct MessageStreamSender(Arc<RwLock<HashMap<u32, UnboundedSender<proto::Frame>>>>);

impl MessageStreamSender {
    pub async fn send(&self, message: NamedPayload<proto::Frame>) -> IoResult<()> {
        if let Some(sender) = self.0.read().await.get(&message.transport) {
            if sender.send(message.payload).is_ok() {
                return Ok(());
            }
        }

        Err(std::io::Error::new(
            std::io::ErrorKind::ConnectionAborted,
            "",
        ))
    }
}

pub struct MessageStreamReceiver(UnboundedReceiver<NamedPayload<IoResult<proto::Frame>>>);

impl MessageStreamReceiver {
    pub async fn recv(&mut self) -> Option<NamedPayload<IoResult<proto::Frame>>> {
        self.0.recv().await
    }
}
