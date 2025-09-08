use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
};

use async_trait::async_trait;
use prost::Message;
use tokio::sync::mpsc::{UnboundedSender, unbounded_channel};
use tokio_stream::{StreamExt, wrappers::UnboundedReceiverStream};

use crate::{
    NamedPayload, OrderNumber, Stream, proto,
    request::Request,
    response::Response,
    result::{IoResult, RpcError},
    routers::MessageStream,
    task::spawn,
};

pub struct Session<T> {
    pub transport: u32,
    pub order_number: OrderNumber,
    pub service: String,
    pub method: String,
    pub metadata: HashMap<String, String>,
    pub payload: T,
}

impl<T> std::fmt::Debug for Session<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BaseRequest")
            .field("service", &self.service)
            .field("method", &self.method)
            .field("metadata", &self.metadata)
            .finish()
    }
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
                .ok_or_else(|| RpcError::invalid_stream())?
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
        });

        Request {
            metadata: self.metadata,
            timeout: Default::default(),
            payload: Stream::from(UnboundedReceiverStream::from(rx)),
        }
    }
}

#[derive(Default)]
struct SessionsManager {
    transport_bound: HashMap<u32, HashSet<OrderNumber>>,
    request_senders: HashMap<OrderNumber, UnboundedSender<Result<Vec<u8>, RpcError>>>,
}

impl SessionsManager {
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
                            self.request_senders.insert(order_number, tx);

                            self.transport_bound
                                .entry(transport)
                                .or_default()
                                .insert(order_number);

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
                            if let Some(tx) = self.request_senders.get(&order_number).as_ref() {
                                let _ = tx.send(Ok(request.payload));
                            }
                        }
                        proto::frame::Payload::EndOfStream(frame) => {
                            if let Some(tx) = self.request_senders.remove(&order_number) {
                                if let Some(e) = frame.error {
                                    let _ = tx.send(Err(RpcError::from(e)));
                                }
                            }

                            if let Some(items) = self.transport_bound.get_mut(&transport) {
                                items.remove(&order_number);
                            }
                        }
                        _ => (),
                    }
                }
            }
            Err(e) => {
                if let Some(items) = self.transport_bound.remove(&transport) {
                    for order_number in items {
                        if let Some(tx) = self.request_senders.remove(&order_number) {
                            let _ = tx.send(Err(RpcError::from(std::io::Error::new(
                                e.kind(),
                                e.to_string(),
                            ))));
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

    let mut sessions_manager = SessionsManager::default();

    while let Some(payload) = readable_stream.recv().await {
        if let Some(session) = sessions_manager.accept(payload) {
            let service = service.clone();
            let writable_stream_ = writable_stream.clone();

            spawn(async move {
                let transport = session.transport;

                let mut frame = proto::Frame {
                    service: T::NAME.to_string(),
                    method: session.method.clone(),
                    payload: None,
                    ..Default::default()
                };

                frame.set_order_number(session.order_number);

                match service.handle(session).await {
                    Ok(mut response) => {
                        {
                            frame.payload = Some(proto::frame::Payload::ResponseHeader(
                                proto::ResponseHeader {
                                    success: true,
                                    error: None,
                                    metadata: response.metadata,
                                },
                            ));

                            if writable_stream_
                                .send(NamedPayload {
                                    payload: frame.clone(),
                                    transport,
                                })
                                .await
                                .is_err()
                            {
                                return;
                            }
                        }

                        let mut result = None;

                        {
                            let mut serial_number = 0;

                            while let Some(payload) = response.payload.next().await {
                                let payload = match payload {
                                    Ok(it) => it,
                                    Err(e) => {
                                        result = Some(e);

                                        break;
                                    }
                                };

                                frame.payload =
                                    Some(proto::frame::Payload::Response(proto::Response {
                                        serial_number,
                                        payload,
                                    }));

                                serial_number += 1;

                                if writable_stream_
                                    .send(NamedPayload {
                                        payload: frame.clone(),
                                        transport,
                                    })
                                    .await
                                    .is_err()
                                {
                                    break;
                                }
                            }
                        }

                        {
                            frame.payload = Some(proto::frame::Payload::EndOfStream(
                                result
                                    .map(|it| proto::EndOfStream {
                                        success: false,
                                        error: Some(it.to_string()),
                                    })
                                    .unwrap_or_else(|| proto::EndOfStream {
                                        success: true,
                                        error: None,
                                    }),
                            ));

                            let _ = writable_stream_
                                .send(NamedPayload {
                                    payload: frame,
                                    transport,
                                })
                                .await;
                        }
                    }
                    Err(e) => {
                        frame.payload = Some(proto::frame::Payload::ResponseHeader(
                            proto::ResponseHeader {
                                success: false,
                                error: Some(e.to_string()),
                                metadata: HashMap::default(),
                            },
                        ));

                        let _ = writable_stream_.send(NamedPayload {
                            payload: frame,
                            transport,
                        }).await;
                    }
                }
            });
        }
    }

    #[cfg(feature = "log")]
    log::info!("service closed, service = {}", T::NAME);
}
