use std::{collections::HashMap, fmt::Debug, sync::Arc};

use async_trait::async_trait;
use prost::Message;
use tokio::sync::mpsc::{UnboundedSender, unbounded_channel};
use tokio_stream::{StreamExt, wrappers::UnboundedReceiverStream};

use crate::{RpcTransport, Stream, proto, request::Request, response::Response};

#[derive(Debug)]
pub struct BaseRequest<T> {
    pub order_id: String,
    pub service: String,
    pub method: String,
    pub metadata: HashMap<String, String>,
    pub payload: T,
}

impl BaseRequest<Stream<Vec<u8>>> {
    pub async fn into_once<T: Message + Default>(mut self) -> Option<Request<T>> {
        Some(Request {
            metadata: self.metadata,
            timeout: Default::default(),
            payload: T::decode(self.payload.next().await?.as_ref()).ok()?,
        })
    }

    pub fn into_stream<T: Message + Unpin + Default + 'static>(mut self) -> Request<Stream<T>> {
        let (tx, rx) = unbounded_channel::<T>();

        tokio::spawn(async move {
            while let Some(buf) = self.payload.next().await {
                if let Ok(message) = T::decode(buf.as_ref()) {
                    if tx.send(message).is_err() {
                        break;
                    }
                } else {
                    break;
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
    stream_senders: HashMap<String, UnboundedSender<Vec<u8>>>,
}

impl RequestFrameAdapter {
    fn accept(&mut self, frame: proto::Frame) -> Option<BaseRequest<Stream<Vec<u8>>>> {
        if (frame.flags & proto::FrameFlags::EndOfStream as u32) != 0 {
            let _ = self.stream_senders.remove(&frame.order_id);
        }

        if let Some(payload) = frame.payload {
            match payload {
                proto::frame::Payload::RequestHeader(header) => {
                    let (tx, rx) = unbounded_channel::<Vec<u8>>();
                    self.stream_senders.insert(frame.order_id.clone(), tx);

                    return Some(BaseRequest {
                        payload: Stream::from(UnboundedReceiverStream::from(rx)),
                        order_id: frame.order_id,
                        service: frame.service,
                        method: frame.method,
                        metadata: header.metadata,
                    });
                }
                proto::frame::Payload::Request(request) => {
                    if let Some(tx) = self.stream_senders.get(&frame.order_id).as_ref() {
                        let _ = tx.send(request.payload);
                    }
                }
                _ => (),
            }
        }

        None
    }
}

#[async_trait]
pub trait ServerService {
    const NAME: &'static str;

    type Error: Debug;

    async fn handle(
        &self,
        request: BaseRequest<Stream<Vec<u8>>>,
    ) -> Result<Response<Stream<Vec<u8>>>, Self::Error>;
}

pub fn startup_server<T>(
    service: T,
    RpcTransport {
        receiver: mut readable_stream,
        sender: writable_stream,
    }: RpcTransport,
) where
    T: ServerService + Sync + Send + 'static,
    T::Error: Send,
{
    let service = Arc::new(service);
    let (frame_sender, mut frame_receiver) = unbounded_channel::<proto::Frame>();
    tokio::spawn(async move {
        while let Some(frame) = frame_receiver.recv().await {
            if writable_stream.send(frame).is_err() {
                break;
            }
        }
    });

    tokio::spawn(async move {
        let mut adapter = RequestFrameAdapter::default();

        while let Some(frame) = readable_stream.recv().await {
            if let Some(request) = adapter.accept(frame) {
                let service = service.clone();
                let frame_sender_ = frame_sender.clone();

                tokio::spawn(async move {
                    let mut frame = proto::Frame {
                        order_id: request.order_id.clone(),
                        service: T::NAME.to_string(),
                        method: request.method.clone(),
                        flags: 0,
                        payload: None,
                    };

                    match service.handle(request).await {
                        Ok(mut response) => {
                            {
                                frame.payload = Some(proto::frame::Payload::ResponseHeader(
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

                            while let Some(payload) = response.payload.next().await {
                                frame.payload =
                                    Some(proto::frame::Payload::Response(proto::Response {
                                        payload,
                                    }));

                                if frame_sender_.send(frame.clone()).is_err() {
                                    break;
                                }
                            }

                            frame.payload = None;
                            frame.flags = proto::FrameFlags::EndOfStream as u32
                                | proto::FrameFlags::Empty as u32;

                            let _ = frame_sender_.send(frame);
                        }
                        Err(e) => {
                            frame.payload = Some(proto::frame::Payload::ResponseHeader(
                                proto::ResponseHeader {
                                    success: false,
                                    error: Some(format!("{:?}", e)),
                                    metadata: HashMap::default(),
                                },
                            ));

                            let _ = frame_sender_.send(frame);
                        }
                    }
                });
            }
        }
    });
}
