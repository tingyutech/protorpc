use std::{collections::HashMap, sync::Arc};

use async_trait::async_trait;
use prost::Message;
use tokio::sync::mpsc::{UnboundedSender, unbounded_channel};
use tokio_stream::{StreamExt, wrappers::UnboundedReceiverStream};

use crate::{
    OrderNumber, Stream, proto, request::Request, response::Response, result::RpcError,
    task::spawn, transport::IOStream,
};

pub struct BaseRequest<T> {
    pub order_number: OrderNumber,
    pub service: String,
    pub method: String,
    pub metadata: HashMap<String, String>,
    pub payload: T,
}

impl<T> std::fmt::Debug for BaseRequest<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BaseRequest")
            .field("service", &self.service)
            .field("method", &self.method)
            .field("metadata", &self.metadata)
            .finish()
    }
}

impl BaseRequest<Stream<Result<Vec<u8>, RpcError>>> {
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
struct RequestFrameAdapter {
    stream_senders: HashMap<OrderNumber, UnboundedSender<Result<Vec<u8>, RpcError>>>,
}

impl RequestFrameAdapter {
    fn accept(
        &mut self,
        frame: proto::Frame,
    ) -> Option<BaseRequest<Stream<Result<Vec<u8>, RpcError>>>> {
        let order_number = frame.order_number();

        if let Some(payload) = frame.payload {
            match payload {
                proto::frame::Payload::RequestHeader(header) => {
                    let (tx, rx) = unbounded_channel::<Result<Vec<u8>, RpcError>>();
                    self.stream_senders.insert(order_number, tx);

                    return Some(BaseRequest {
                        payload: Stream::from(UnboundedReceiverStream::from(rx)),
                        service: frame.service,
                        method: frame.method,
                        metadata: header.metadata,
                        order_number,
                    });
                }
                proto::frame::Payload::Request(request) => {
                    if let Some(tx) = self.stream_senders.get(&order_number).as_ref() {
                        let _ = tx.send(Ok(request.payload));
                    }
                }
                proto::frame::Payload::EndOfStream(frame) => {
                    if let Some(tx) = self.stream_senders.remove(&order_number) {
                        if let Some(e) = frame.error {
                            let _ = tx.send(Err(RpcError::from(e)));
                        }
                    }
                }
                _ => (),
            }
        }

        None
    }

    fn error(&mut self, err: std::io::Error) {
        for tx in self.stream_senders.values() {
            let _ = tx.send(Err(RpcError::from(std::io::Error::new(
                err.kind(),
                err.to_string(),
            ))));
        }
    }
}

#[cfg_attr(target_family = "wasm", async_trait(?Send))]
#[cfg_attr(not(target_family = "wasm"), async_trait)]
pub trait ServerService {
    const NAME: &'static str;

    async fn handle(
        &self,
        request: BaseRequest<Stream<Result<Vec<u8>, RpcError>>>,
    ) -> Result<Response<Stream<Result<Vec<u8>, RpcError>>>, RpcError>;
}

pub fn startup_server<T>(
    service: T,
    IOStream {
        receiver: mut readable_stream,
        sender: writable_stream,
    }: IOStream,
) where
    T: ServerService + Sync + Send + 'static,
{
    let service = Arc::new(service);
    let (frame_sender, mut frame_receiver) = unbounded_channel::<proto::Frame>();
    spawn(async move {
        while let Some(frame) = frame_receiver.recv().await {
            if writable_stream.send(frame).is_err() {
                break;
            }
        }
    });

    spawn(async move {
        let mut adapter = RequestFrameAdapter::default();

        while let Some(frame) = readable_stream.recv().await {
            match frame {
                Ok(frame) => {
                    if let Some(request) = adapter.accept(frame) {
                        let service = service.clone();
                        let frame_sender_ = frame_sender.clone();

                        spawn(async move {
                            let mut frame = proto::Frame {
                                service: T::NAME.to_string(),
                                method: request.method.clone(),
                                payload: None,
                                ..Default::default()
                            };

                            frame.set_order_number(request.order_number);

                            match service.handle(request).await {
                                Ok(mut response) => {
                                    {
                                        frame.payload =
                                            Some(proto::frame::Payload::ResponseHeader(
                                                proto::ResponseHeader {
                                                    success: true,
                                                    error: None,
                                                    metadata: response.metadata,
                                                },
                                            ));

                                        if frame_sender_.send(frame.clone()).is_err() {
                                            return;
                                        }
                                    }

                                    let mut result = None;
                                    let mut serial_number = 0;
                                    while let Some(payload) = response.payload.next().await {
                                        let payload = match payload {
                                            Ok(it) => it,
                                            Err(e) => {
                                                result = Some(e);

                                                break;
                                            }
                                        };

                                        frame.payload = Some(proto::frame::Payload::Response(
                                            proto::Response {
                                                serial_number,
                                                payload,
                                            },
                                        ));

                                        serial_number += 1;

                                        if frame_sender_.send(frame.clone()).is_err() {
                                            break;
                                        }
                                    }

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

                                    let _ = frame_sender_.send(frame);
                                }
                                Err(e) => {
                                    frame.payload = Some(proto::frame::Payload::ResponseHeader(
                                        proto::ResponseHeader {
                                            success: false,
                                            error: Some(e.to_string()),
                                            metadata: HashMap::default(),
                                        },
                                    ));

                                    let _ = frame_sender_.send(frame);
                                }
                            }
                        });
                    }
                }
                Err(e) => {
                    adapter.error(e);

                    break;
                }
            }
        }
    });
}
