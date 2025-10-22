use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
};

use async_trait::async_trait;
use prost::Message;
use tokio::sync::mpsc::{UnboundedSender, unbounded_channel};
use tokio_stream::{StreamExt, wrappers::UnboundedReceiverStream};

use crate::{
    OrderNumber, Stream,
    helper::{FrameBuilder, NamedPayload, spawn},
    proto::{self, EndOfStream},
    request::Request,
    response::Response,
    result::{IoResult, RpcError},
    routers::MessageStream,
};

pub struct Session<T> {
    pub transport: u32,
    pub order_number: OrderNumber,
    pub service: String,
    pub method: String,
    pub metadata: HashMap<String, String>,
    pub payload: T,
}

impl Session<Stream<Result<Vec<u8>, RpcError>>> {
    pub async fn into_once<T: Message + Default>(mut self) -> Result<Request<T>, RpcError> {
        Ok(Request {
            metadata: self.metadata,
            timeout: Default::default(),
            payload: self
                .payload
                .next()
                .await
                .ok_or_else(|| RpcError::invalid_stream_with_message("session is closed"))?
                .map(|it| {
                    T::decode(it.as_ref()).map_err(|e| RpcError::invalid_data(&e.to_string()))
                })??,
        })
    }

    pub fn into_stream<T: Message + Unpin + Default + 'static>(
        mut self,
    ) -> Request<Stream<Result<T, RpcError>>> {
        let (tx, rx) = unbounded_channel::<Result<T, RpcError>>();

        spawn(async move {
            while let Some(buf) = self.payload.next().await {
                match buf {
                    Ok(buf) => {
                        if let Ok(message) = T::decode(buf.as_ref()) {
                            if tx.send(Ok(message)).is_err() {
                                break;
                            }
                        } else {
                            break;
                        }
                    }
                    Err(e) => {
                        let _ = tx.send(Err(RpcError::from(e)));

                        break;
                    }
                }
            }

            #[cfg(feature = "log")]
            log::info!(
                "session stream closed, transport={}, service={}, method={}, order_number={:?}",
                self.transport,
                self.service,
                self.method,
                self.order_number,
            );
        });

        Request {
            metadata: self.metadata,
            timeout: Default::default(),
            payload: Stream::from(UnboundedReceiverStream::from(rx)),
        }
    }
}

struct Channel {
    transport: u32,
    order_number: OrderNumber,
    service: String,
    method: String,
    sender: UnboundedSender<Result<Vec<u8>, RpcError>>,
}

impl std::fmt::Debug for Channel {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Session")
            .field("transport", &self.transport)
            .field("order_number", &self.order_number)
            .field("service", &self.service)
            .field("method", &self.method)
            .finish()
    }
}

#[derive(Default)]
struct Remuxer {
    transport_bound: HashMap<u32, HashSet<OrderNumber>>,
    channels: HashMap<OrderNumber, Channel>,
}

impl Remuxer {
    fn insert_channel(
        &mut self,
        transport: u32,
        order_number: OrderNumber,
        service: String,
        method: String,
        sender: UnboundedSender<Result<Vec<u8>, RpcError>>,
    ) {
        self.channels.insert(
            order_number,
            Channel {
                transport,
                order_number,
                service,
                method,
                sender,
            },
        );
    }

    fn remove_channel(
        &mut self,
        order_number: OrderNumber,
        transport: u32,
        frame: Option<EndOfStream>,
    ) {
        if let Some(channel) = self.channels.remove(&order_number) {
            if let Some(frame) = frame {
                if let Some(e) = frame.error {
                    let _ = channel.sender.send(Err(RpcError::from(e)));
                }
            }

            #[cfg(feature = "log")]
            log::info!("session closed: session = {channel:?}");
        }

        if let Some(items) = self.transport_bound.get_mut(&transport) {
            items.remove(&order_number);
        }
    }

    fn accept(
        &mut self,
        NamedPayload { transport, payload }: NamedPayload<IoResult<proto::Frame>>,
    ) -> Option<Session<Stream<Result<Vec<u8>, RpcError>>>> {
        match payload {
            Ok(frame) => {
                let order_number = frame.order_number();

                if let Some(payload) = frame.payload {
                    match payload {
                        proto::frame::Payload::RequestHeader(header) => {
                            let (tx, rx) = unbounded_channel::<Result<Vec<u8>, RpcError>>();
                            self.insert_channel(
                                transport,
                                order_number,
                                frame.service.clone(),
                                frame.method.clone(),
                                tx,
                            );

                            self.transport_bound
                                .entry(transport)
                                .or_default()
                                .insert(order_number);

                            #[cfg(feature = "log")]
                            log::info!(
                                "server recv a new session, transport={transport}, service={}, method={}, order_number={order_number:?}",
                                frame.service,
                                frame.method
                            );

                            return Some(Session {
                                payload: Stream::from(UnboundedReceiverStream::from(rx)),
                                service: frame.service,
                                method: frame.method,
                                metadata: header.metadata,
                                order_number,
                                transport,
                            });
                        }
                        proto::frame::Payload::Request(request) => {
                            if let Some(channel) = self.channels.get(&order_number).as_ref() {
                                let _ = channel.sender.send(Ok(request.payload));
                            }
                        }
                        proto::frame::Payload::EndOfStream(frame) => {
                            self.remove_channel(order_number, transport, Some(frame));
                        }
                        proto::frame::Payload::Close(_) => {
                            self.remove_channel(order_number, transport, None);
                        }
                        _ => (),
                    }
                }
            }
            Err(e) => {
                if let Some(items) = self.transport_bound.remove(&transport) {
                    for order_number in items {
                        if let Some(channel) = self.channels.remove(&order_number) {
                            let _ = channel.sender.send(Err(RpcError::from(std::io::Error::new(
                                e.kind(),
                                e.to_string(),
                            ))));

                            #[cfg(feature = "log")]
                            log::warn!("session recv a error = {e:?}, session = {channel:?}");
                        }
                    }
                }
            }
        }

        None
    }
}

#[cfg_attr(target_family = "wasm", async_trait(?Send))]
#[cfg_attr(not(target_family = "wasm"), async_trait)]
pub trait ServerService {
    const NAME: &'static str;

    async fn handle(
        &self,
        session: Session<Stream<Result<Vec<u8>, RpcError>>>,
    ) -> Result<Response<Stream<Result<Vec<u8>, RpcError>>>, RpcError>;
}

pub async fn startup_server<T>(service: T, stream: MessageStream)
where
    T: ServerService + Sync + Send + 'static,
{
    let (writable_stream, mut readable_stream) = stream.split();
    let service = Arc::new(service);

    let mut remuxer = Remuxer::default();

    while let Some(payload) = readable_stream.recv().await {
        if let Some(session) = remuxer.accept(payload) {
            let service = service.clone();
            let writable_stream_ = writable_stream.clone();

            spawn(async move {
                let transport = session.transport;

                let mut frame_builder = FrameBuilder::new(
                    T::NAME.to_string(),
                    session.method.clone(),
                    session.order_number,
                );

                match service.handle(session).await {
                    Ok(mut response) => {
                        {
                            if writable_stream_
                                .send(NamedPayload {
                                    payload: frame_builder
                                        .response_header(response.metadata, &Ok(())),
                                    transport,
                                })
                                .await
                                .is_err()
                            {
                                return;
                            }
                        }

                        let mut result = Ok(());

                        {
                            let mut serial_number = 0;

                            while let Some(payload) = response.payload.next().await {
                                let payload = match payload {
                                    Ok(it) => it,
                                    Err(e) => {
                                        result = Err(e);

                                        break;
                                    }
                                };

                                if writable_stream_
                                    .send(NamedPayload {
                                        payload: frame_builder.response(serial_number, payload),
                                        transport,
                                    })
                                    .await
                                    .is_err()
                                {
                                    break;
                                }

                                serial_number += 1;
                            }
                        }

                        {
                            let _ = writable_stream_
                                .send(NamedPayload {
                                    payload: frame_builder.end_of_stream(&result),
                                    transport,
                                })
                                .await;
                        }
                    }
                    Err(e) => {
                        let _ = writable_stream_
                            .send(NamedPayload {
                                payload: frame_builder
                                    .response_header::<()>(HashMap::default(), &Err(e)),
                                transport,
                            })
                            .await;
                    }
                }
            });
        }
    }

    #[cfg(feature = "log")]
    log::warn!("service closed, service = {}", T::NAME);
}
