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

use lru::LruCache;

use std::{
    collections::HashMap,
    num::NonZeroUsize,
    sync::{
        Arc, LazyLock,
        atomic::{AtomicU32, Ordering},
    },
};

use tokio::sync::{
    Mutex, RwLock,
    mpsc::{UnboundedSender, unbounded_channel},
};

use crate::{RpcServiceBuilder, proto, task::spawn, transport::IOStream};

// Transport layer sequence number cursor
static TRASNPORT_NUMBER: LazyLock<AtomicU32> = LazyLock::new(|| AtomicU32::new(0));

/// Request/Response Router
pub struct Routes {
    drop_notify_sender: tokio::sync::broadcast::Sender<()>,
    drop_notify_receiver: tokio::sync::broadcast::Receiver<()>,

    // All frames that need to be sent to the peer through the transport layer
    // are sent to this channel; this is a bus for output frames
    output_bus_sender: UnboundedSender<proto::Frame>,
    // Frames that need to be sent to a specific transport layer can be delivered
    // through this channel
    transport_senders: Arc<RwLock<HashMap<u32, UnboundedSender<proto::Frame>>>>,
    // Frames that need to be sent to a specific service implementation can be
    // delivered through this channel
    service_senders: Arc<RwLock<HashMap<String, UnboundedSender<proto::Frame>>>>,
    // Associate the transport layer sequence number with the message transaction number
    //
    // The response to a request received by a certain transport layer must also
    // be sent to this transport layer
    lru: Arc<Mutex<LruCache<u128 /* frame order id */, u32 /* transport sequence */>>>,
}

impl Routes {
    pub fn new() -> Self {
        let (drop_notify_sender, drop_notify_receiver) = tokio::sync::broadcast::channel::<()>(10);
        let (output_bus_sender, mut output_bus_receiver) = unbounded_channel::<proto::Frame>();

        let transport_senders: Arc<RwLock<HashMap<u32, UnboundedSender<proto::Frame>>>> =
            Default::default();

        let lru = Arc::new(Mutex::new(LruCache::<u128, u32>::new(
            NonZeroUsize::new(100).unwrap(),
        )));

        // This worker is dedicated to message routing
        //
        // Uniformly handle frames that need to be sent out, then look up the
        // message routing table to forward them to the corresponding transport layer
        {
            let lru_ = lru.clone();
            let transport_senders_ = transport_senders.clone();
            let mut drop_notify_receiver_ = drop_notify_receiver.resubscribe();

            spawn(async move {
                loop {
                    tokio::select! {
                        Some(frame) = output_bus_receiver.recv() => {
                            let id = frame.order_number();

                            #[cfg(feature = "log")]
                            log::debug!("routers bus forwarding task received a frame, frame = {:?}", frame);

                            let sequence = {
                                let mut lru = lru_.lock().await;

                                if let Some(sequence) = lru.get(&id).copied() {
                                    sequence
                                } else {
                                    if let Some(proto::frame::Payload::RequestHeader(_)) = frame.payload
                                    {
                                        // If this frame is a request header and
                                        // there is no record in the lru, randomly
                                        // select a transport layer
                                        //
                                        // Because this means the request header
                                        // was sent by the client and is not recorded.
                                        let index = {
                                            let transport_senders = transport_senders_.read().await;
                                            let mut keys = transport_senders.keys();

                                            let offset = if keys.len() == 0 {
                                                continue;
                                            } else {
                                                if keys.len() == 1 {
                                                    0
                                                } else {
                                                    fastrand::usize(0..keys.len())
                                                }
                                            };

                                            keys.nth(offset).copied().unwrap_or(0)
                                        };

                                        lru.push(id, index);

                                        #[cfg(feature = "log")]
                                        log::debug!(
                                            "request has no bound transport layer, randomly assigned to = {}, service = {:?}",
                                            index,
                                            frame.service,
                                        );

                                        index
                                    } else {
                                        continue;
                                    }
                                }
                            };

                            let mut is_closed = false;

                            {
                                if let Some(sender) = transport_senders_.read().await.get(&sequence) {
                                    if sender.send(frame).is_err() {
                                        is_closed = true;
                                    }
                                }
                            }

                            // The transport layer is invalid, clean it up
                            if is_closed {
                                let _ = transport_senders_.write().await.remove(&sequence);

                                #[cfg(feature = "log")]
                                log::info!("transport layer invalid, removing transport layer = {}", sequence);
                            }
                        }
                        _ = drop_notify_receiver_.recv() => {
                            #[cfg(feature = "log")]
                            log::info!("routers bus forwarding task exited for routers dropped");

                            break;
                        }
                        else => {
                            break;
                        }
                    }
                }

                #[cfg(feature = "log")]
                log::info!("routers bus forwarding task closed");
            });
        }

        Self {
            service_senders: Default::default(),
            drop_notify_receiver,
            drop_notify_sender,
            transport_senders,
            output_bus_sender,
            lru,
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

        let lru = self.lru.clone();
        let transport_senders = self.transport_senders.clone();
        let service_senders = self.service_senders.clone();
        let mut drop_notify_receiver_ = self.drop_notify_receiver.resubscribe();

        transport_senders
            .write()
            .await
            .insert(sequence, writable_stream);

        #[cfg(feature = "log")]
        log::info!(
            "added a transport layer, assigned sequence number = {}",
            sequence
        );

        spawn(async move {
            loop {
                tokio::select! {
                    Some(frame) = readable_stream.recv() => {
                        #[cfg(feature = "log")]
                        log::debug!("transport received a frame, number = {}, frame = {:?}", sequence, frame);

                        // Received a request header from the remote, need
                        // to record which transport layer sent this request
                        //
                        // Here, associate the current request's transaction
                        // number with the current transport layer and record
                        // it in the lru
                        if let Some(proto::frame::Payload::RequestHeader(_)) = frame.payload {
                            lru.lock().await.push(frame.order_number(), sequence);
                        }

                        let mut closed_service = None;

                        {
                            if let Some(sender) = service_senders.read().await.get(&frame.service) {
                                if !sender.is_closed() {
                                    let _ = sender.send(frame);
                                }
                            } else {
                                closed_service = Some(frame.service);
                            }
                        }

                        if let Some(service) = closed_service {
                            let _ = service_senders.write().await.remove(&service);
                        }
                    }
                    _ = drop_notify_receiver_.recv() => {
                        #[cfg(feature = "log")]
                        log::info!("transport exited for rpc dropped, number = {}", sequence);

                        break;
                    }
                    else => {
                        break;
                    }
                }
            }

            #[cfg(feature = "log")]
            log::info!("transport closed, number = {}", sequence);

            let _ = transport_senders.write().await.remove(&sequence);
        });
    }

    /// Build a service.
    pub async fn make_service<S: RpcServiceBuilder + Send + Sync + 'static>(
        &self,
        ctx: S::Context,
    ) -> S::Output {
        let (input_sender, input_receiver) = unbounded_channel();

        self.service_senders
            .write()
            .await
            .insert(S::NAME.to_string(), input_sender);

        #[cfg(feature = "log")]
        log::info!("routers added a service, service = {}", S::NAME);

        S::build(
            ctx,
            IOStream {
                sender: self.output_bus_sender.clone(),
                receiver: input_receiver,
            },
        )
    }
}

impl Drop for Routes {
    fn drop(&mut self) {
        let _ = self.drop_notify_sender.send(());

        #[cfg(feature = "log")]
        log::info!("routers dropped");
    }
}
