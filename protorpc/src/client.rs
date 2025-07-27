use std::{collections::HashMap, sync::Arc, time::Duration};

use prost::Message;
use thiserror::Error;
use tokio::sync::{
    RwLock,
    mpsc::{UnboundedSender, unbounded_channel},
    oneshot,
};

use tokio_stream::{StreamExt, wrappers::UnboundedReceiverStream};

use crate::{RpcTransport, Stream, proto};

#[derive(Debug, Error)]
pub enum RequestError {
    #[error("transport terminated")]
    Terminated,
    #[error("request timeout")]
    Timeout,
    #[error("core service shutdown")]
    Shutdown,
    #[error("invalid response: {0}")]
    InvalidResponse(#[from] prost::DecodeError),
    #[error("invalid response stream")]
    InvalidStream,
    #[error("error response: {0}")]
    ErrorResponse(String),
}

// TODO: not zero copy
pub struct ClientCore {
    output_frame_sender: UnboundedSender<proto::Frame>,
    frame_handlers: Arc<RwLock<HashMap<String, UnboundedSender<proto::Frame>>>>,
}

impl ClientCore {
    pub fn new(
        RpcTransport {
            receiver: mut readable_stream,
            sender: writable_stream,
        }: RpcTransport,
    ) -> Self {
        let frame_handlers: Arc<RwLock<HashMap<String, UnboundedSender<proto::Frame>>>> =
            Default::default();

        {
            let frame_handlers_ = frame_handlers.clone();

            tokio::spawn(async move {
                while let Some(frame) = readable_stream.recv().await {
                    #[cfg(feature = "log")]
                    log::debug!(
                        "client core bus received a response frame, frame = {:?}",
                        frame
                    );

                    if let Some(handler) = frame_handlers_.read().await.get(&frame.order_id) {
                        if !handler.is_closed() {
                            let _ = handler.send(frame);
                        }
                    }
                }
            });
        }

        // A dedicated task for cleaning up dangling handles.
        {
            let frame_handlers_ = Arc::downgrade(&frame_handlers);

            tokio::spawn(async move {
                loop {
                    tokio::time::sleep(Duration::from_secs(1)).await;

                    if let Some(frame_handlers) = frame_handlers_.upgrade() {
                        frame_handlers.write().await.retain(|_, v| !v.is_closed());
                    } else {
                        break;
                    }
                }
            });
        }

        let (tx, mut rx) = unbounded_channel::<proto::Frame>();
        {
            tokio::spawn(async move {
                while let Some(frame) = rx.recv().await {
                    #[allow(unused_variables)]
                    if let Err(e) = writable_stream.send(frame) {
                        #[cfg(feature = "log")]
                        log::warn!("client core failed to send a frame, error = {}", e);

                        break;
                    }
                }
            });
        }

        Self {
            output_frame_sender: tx,
            frame_handlers,
        }
    }

    // TODO: safety shutdown & zero copy
    pub async fn stream<Q, S, A>(
        &self,
        service: &str,
        method: &str,
        timeout: Duration,
        metadata: HashMap<String, String>,
        mut request: A,
    ) -> Result<(Stream<S>, HashMap<String, String>), RequestError>
    where
        Q: Message,
        S: Message + Unpin + Default + 'static,
        A: futures_core::Stream<Item = Q> + Unpin + Send + 'static,
    {
        #[cfg(feature = "log")]
        log::debug!(
            "client core received a request, service = {}, method = {}",
            service,
            method
        );

        // The overall process actually simulates HTTP, but with some differences.

        let mut frame = proto::Frame {
            order_id: nanoid::nanoid!(),
            service: service.to_string(),
            method: method.to_string(),
            flags: 0,
            payload: None,
        };

        // All frames sent by the remote response are delivered through this channel.
        let (response_frame_sender, mut response_frame_receiver) =
            unbounded_channel::<proto::Frame>();

        // Response data stream channel.
        let (response_stream_sender, response_stream_receiver) = unbounded_channel();

        // Channel for delivering the response header.
        let (response_header_sender, response_header_receiver) =
            oneshot::channel::<Result<HashMap<String, String>, RequestError>>();

        self.frame_handlers
            .write()
            .await
            .insert(frame.order_id.clone(), response_frame_sender);

        #[cfg(feature = "log")]
        log::debug!(
            "client core registered a response frame handler, service = {}, method = {}, order id = {}",
            service,
            method,
            frame.order_id
        );

        // First send the request header, similar to an HTTP request header.
        {
            frame.payload = Some(proto::frame::Payload::RequestHeader(proto::RequestHeader {
                metadata: metadata,
            }));

            self.output_frame_sender
                .send(frame.clone())
                .map_err(|_| RequestError::Terminated)?;

            #[cfg(feature = "log")]
            log::debug!(
                "client core sent a request header, service = {}, method = {}",
                service,
                method
            );
        }

        {
            let output_frame_sender = self.output_frame_sender.clone();

            tokio::spawn(async move {
                while let Some(payload) = request.next().await {
                    #[cfg(feature = "log")]
                    log::debug!(
                        "client core received a response payload, service = {}, order id = {}",
                        frame.service,
                        frame.order_id
                    );

                    frame.payload = Some(proto::frame::Payload::Request(proto::Request {
                        payload: payload.encode_to_vec(),
                    }));

                    #[allow(unused_variables)]
                    if let Err(e) = output_frame_sender.send(frame.clone()) {
                        #[cfg(feature = "log")]
                        log::warn!(
                            "client core failed to send a response frame, service = {}, order id = {}, error = {}",
                            frame.service,
                            frame.order_id,
                            e
                        );

                        break;
                    }
                }

                // After the stream is closed, an `EndOfStream` packet needs to be sent.
                {
                    frame.payload = None;
                    frame.flags =
                        proto::FrameFlags::EndOfStream as u32 | proto::FrameFlags::Empty as u32;

                    let _ = output_frame_sender.send(frame.clone());

                    #[cfg(feature = "log")]
                    log::debug!(
                        "client core sent a response end of stream frame, service = {}, order id = {}",
                        frame.service,
                        frame.order_id
                    );
                }
            });
        }

        let mut response_header_sender = Some(response_header_sender);

        tokio::spawn(async move {
            while let Some(frame) = response_frame_receiver.recv().await {
                #[cfg(feature = "log")]
                log::debug!("client core received a response frame, frame = {:?}", frame);

                // The stream has ended, terminate the process. If the first packet is the end
                // of the stream, the stream is invalid.
                if (frame.flags & proto::FrameFlags::EndOfStream as u32) != 0 {
                    if let Some(tx) = response_header_sender.take() {
                        let _ = tx.send(Err(RequestError::InvalidStream));
                    }

                    break;
                }

                if let Some(payload) = frame.payload {
                    match payload {
                        proto::frame::Payload::Response(response) => {
                            // Received a response before receiving the response header, this is an
                            // invalid stream.
                            if let Some(tx) = response_header_sender.take() {
                                let _ = tx.send(Err(RequestError::InvalidStream));

                                break;
                            }

                            // Any error here will cause the current stream to be terminated
                            // directly.
                            if let Ok(payload) = S::decode(response.payload.as_ref()) {
                                if response_stream_sender.send(payload).is_err() {
                                    break;
                                }
                            } else {
                                break;
                            }
                        }
                        proto::frame::Payload::ResponseHeader(header) => {
                            let _ = response_header_sender.take().map(|it| {
                                it.send(if header.success {
                                    Ok(header.metadata)
                                } else {
                                    Err(RequestError::ErrorResponse(
                                        header.error.unwrap_or_else(|| "Unknown error".to_string()),
                                    ))
                                })
                            });
                        }
                        _ => (),
                    }
                }
            }
        });

        // try it the result
        let metadata = tokio::time::timeout(timeout, response_header_receiver)
            .await
            .map_err(|_| RequestError::Timeout)?
            .map_err(|_| RequestError::Shutdown)??;

        Ok((
            Stream::from(UnboundedReceiverStream::from(response_stream_receiver)),
            metadata,
        ))
    }
}
