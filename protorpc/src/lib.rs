pub mod client;
pub mod request;
pub mod response;
pub mod server;

pub use async_trait::async_trait;
pub use futures_core;
pub use tokio_stream;

use lru::LruCache;
use rand::seq::IteratorRandom;

use bytes::{Buf, BufMut, BytesMut};
use prost::Message;

use std::{
    collections::HashMap,
    num::NonZeroUsize,
    pin::Pin,
    sync::{
        Arc, LazyLock,
        atomic::{AtomicU32, Ordering},
    },
};

use tokio::{
    io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt},
    sync::{
        Mutex, RwLock,
        mpsc::{UnboundedReceiver, UnboundedSender, unbounded_channel},
    },
};

#[macro_export]
macro_rules! include_proto {
    ($package: tt) => {
        include!(concat!(env!("OUT_DIR"), concat!("/", $package, ".rs")));
    };
}

mod proto {
    include_proto!("airport.core");
}

// Transport layer sequence number cursor
static TRASNPORT_NUMBER: LazyLock<AtomicU32> = LazyLock::new(|| AtomicU32::new(0));

pub struct Stream<T: Send>(Box<dyn futures_core::Stream<Item = T> + Send + Unpin>);

impl<T: Send + 'static> Stream<T> {
    pub fn once(value: T) -> Self {
        Self(Box::new(tokio_stream::once(value)))
    }

    pub fn from<S>(stream: S) -> Self
    where
        T: Send + 'static,
        S: futures_core::Stream<Item = T> + Send + Unpin + 'static + Sized,
    {
        Self(Box::new(stream))
    }
}

impl<T: Send> futures_core::Stream for Stream<T> {
    type Item = T;

    fn poll_next(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        Pin::new(&mut self.as_mut().0).poll_next(cx)
    }
}

pub struct RpcTransport {
    pub sender: UnboundedSender<proto::Frame>,
    pub receiver: UnboundedReceiver<proto::Frame>,
}

pub trait RpcService {
    const NAME: &'static str;

    type Context;
    type Output;

    fn with_transport(ctx: Self::Context, transport: RpcTransport) -> Self::Output;
}

pub struct Rpc {
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
    lru: Arc<Mutex<LruCache<String /* frame order id */, u32 /* transport sequence */>>>,
}

impl Rpc {
    pub fn new() -> Self {
        let (drop_notify_sender, drop_notify_receiver) = tokio::sync::broadcast::channel::<()>(10);
        let (output_bus_sender, mut output_bus_receiver) = unbounded_channel::<proto::Frame>();

        let transport_senders: Arc<RwLock<HashMap<u32, UnboundedSender<proto::Frame>>>> =
            Default::default();

        let lru = Arc::new(Mutex::new(LruCache::<String, u32>::new(
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

            tokio::spawn(async move {
                loop {
                    tokio::select! {
                        Some(frame) = output_bus_receiver.recv() => {
                            #[cfg(feature = "log")]
                            log::debug!("rpc bus forwarding task received a frame, frame = {:?}", frame);

                            let sequence = {
                                let mut lru = lru_.lock().await;

                                if let Some(sequence) = lru.get(&frame.order_id).copied() {
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
                                        let index = transport_senders_
                                            .read()
                                            .await
                                            .keys()
                                            .choose(&mut rand::rng())
                                            .copied()
                                            .unwrap_or(0);

                                        lru.push(frame.order_id.clone(), index);

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
                            log::info!("rpc bus forwarding task exited for rpc dropped");

                            break;
                        }
                        else => {
                            break;
                        }
                    }
                }

                #[cfg(feature = "log")]
                log::info!("rpc bus forwarding task closed");
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

    pub async fn add_transport<T>(&self, mut transport: T)
    where
        T: AsyncRead + AsyncWrite + Unpin + Send + 'static,
    {
        let sequence = TRASNPORT_NUMBER.fetch_add(1, Ordering::Relaxed);

        let lru = self.lru.clone();
        let transport_senders = self.transport_senders.clone();
        let service_senders = self.service_senders.clone();
        let mut drop_notify_receiver_ = self.drop_notify_receiver.resubscribe();

        let (sender, mut receiver) = unbounded_channel::<proto::Frame>();
        transport_senders.write().await.insert(sequence, sender);

        #[cfg(feature = "log")]
        log::info!(
            "added a transport layer, assigned sequence number = {}",
            sequence
        );

        tokio::spawn(async move {
            let mut receiver_buffer = BytesMut::new();
            let mut sender_buffer = BytesMut::new();

            loop {
                tokio::select! {
                    Ok(size) = transport.read_buf(&mut receiver_buffer) => {
                        if size == 0 {
                            #[cfg(feature = "log")]
                            log::info!(
                                "transport closed because it cannot continue reading data, number = {}",
                                sequence,
                            );

                            break;
                        }

                        loop {
                            // There are at least 4 bytes, because there must be a
                            // length field
                            if receiver_buffer.len() < 4 {
                                break;
                            }

                            let content_len =
                                u32::from_be_bytes(receiver_buffer[..4].try_into().unwrap()) as usize;

                            if content_len + 4 > receiver_buffer.len() {
                                break;
                            }

                            // skip len
                            receiver_buffer.advance(4);

                            #[cfg(feature = "log")]
                            log::debug!(
                                "transport received a data packet, number = {}, size = {}",
                                sequence,
                                content_len,
                            );

                            match proto::Frame::decode(&mut receiver_buffer.split_to(content_len)) {
                                Ok(frame) => {
                                    #[cfg(feature = "log")]
                                    log::debug!("transport received a frame, number = {}, frame = {:?}", sequence, frame);

                                    // Received a request header from the remote, need
                                    // to record which transport layer sent this request
                                    //
                                    // Here, associate the current request's transaction
                                    // number with the current transport layer and record
                                    // it in the lru
                                    if let Some(proto::frame::Payload::RequestHeader(_)) = frame.payload {
                                        lru.lock().await.push(frame.order_id.clone(), sequence);
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
                                #[allow(unused_variables)]
                                Err(e) => {
                                    #[cfg(feature = "log")]
                                    log::error!(
                                        "transport decode data to frame failed, number = {}, size = {}, error = {}",
                                        sequence,
                                        content_len,
                                        e,
                                    );
                                }
                            }
                        }
                    }
                    Some(frame) = receiver.recv() => {
                        sender_buffer.clear();
                        sender_buffer.put_u32(0);

                        #[cfg(feature = "log")]
                        log::debug!(
                            "transport send frame to remote, number = {}, frame = {:?}",
                            sequence,
                            frame,
                        );

                        #[allow(unused_variables)]
                        if let Err(e) = frame.encode(&mut sender_buffer) {
                            #[cfg(feature = "log")]
                            log::error!(
                                "failed to encode frame, number = {}, frame = {:?}, error = {:?}",
                                sequence,
                                frame,
                                e,
                            );
                        } else {
                            {
                                let size = sender_buffer.len() as u32 - 4;
                                sender_buffer[..4].copy_from_slice(size.to_be_bytes().as_ref());
                            }

                            if let Err(e) = transport.write_all(&sender_buffer).await {
                                #[cfg(feature = "log")]
                                log::error!(
                                    "transport send data to remote failed, number = {}, error = {:?}",
                                    sequence,
                                    e,
                                );

                                // Can't send out the data packet, this transport
                                // layer is already broken, just discard it.
                                break;
                            } else {
                                let _ = transport.flush().await;
                            }
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

    pub async fn make_service<S: RpcService + Send + Sync + 'static>(
        &self,
        ctx: S::Context,
    ) -> S::Output {
        let (input_sender, input_receiver) = unbounded_channel();

        self.service_senders
            .write()
            .await
            .insert(S::NAME.to_string(), input_sender);

        #[cfg(feature = "log")]
        log::info!("rpc added a service, service = {}", S::NAME);

        S::with_transport(
            ctx,
            RpcTransport {
                sender: self.output_bus_sender.clone(),
                receiver: input_receiver,
            },
        )
    }
}

impl Drop for Rpc {
    fn drop(&mut self) {
        let _ = self.drop_notify_sender.send(());

        #[cfg(feature = "log")]
        log::info!("rpc dropped");
    }
}
