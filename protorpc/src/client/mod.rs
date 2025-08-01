pub mod exclusive;
pub mod multiplex;

use std::{collections::HashMap, time::Duration};

use async_trait::async_trait;
use prost::Message;
use tokio::sync::{
    mpsc::{UnboundedReceiver, UnboundedSender, unbounded_channel},
    oneshot,
};

use tokio_stream::{StreamExt, wrappers::UnboundedReceiverStream};

use crate::{Error, Stream, proto};

/// Request handler trait
#[async_trait]
pub trait RequestHandler {
    /// Handle request
    ///
    /// This method is used internally and should not concern external users.
    async fn request<T, Q, S>(
        &self,
        base_request: BaseRequest<'_, T>,
    ) -> Result<(Stream<S>, HashMap<String, String>), Error>
    where
        Q: Message,
        S: Message + Unpin + Default + 'static,
        T: futures_core::Stream<Item = Q> + Unpin + Send + 'static;
}

/// Basic request information
///
/// This type is used internally and should not concern external users.
pub struct BaseRequest<'a, T> {
    pub service: &'a str,
    pub method: &'a str,
    pub timeout: Duration,
    pub metadata: HashMap<String, String>,
    pub request: T,
}

impl<'a, T> BaseRequest<'a, T> {
    async fn request<Q, S>(
        mut self,
        writable_stream: UnboundedSender<proto::Frame>,
        mut readable_stream: UnboundedReceiver<proto::Frame>,
        order_id: u128,
    ) -> Result<(Stream<S>, HashMap<String, String>), Error>
    where
        Q: Message,
        S: Message + Unpin + Default + 'static,
        T: futures_core::Stream<Item = Q> + Unpin + Send + 'static,
    {
        #[cfg(feature = "log")]
        log::debug!(
            "client core received a request, service = {}, method = {}",
            service,
            method
        );

        // The overall process actually simulates HTTP, but with some differences.

        let mut frame = proto::Frame {
            id_high: (order_id >> 64) as u64,
            id_low: (order_id & 0xFFFFFFFFFFFFFFFF) as u64,
            service: self.service.to_string(),
            method: self.method.to_string(),
            payload: None,
            flags: 0,
        };

        // Response data stream channel.
        let (response_stream_sender, response_stream_receiver) = unbounded_channel();

        // Channel for delivering the response header.
        let (response_header_sender, response_header_receiver) =
            oneshot::channel::<Result<HashMap<String, String>, Error>>();

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
                metadata: self.metadata,
            }));

            writable_stream
                .send(frame.clone())
                .map_err(|_| Error::Terminated)?;

            #[cfg(feature = "log")]
            log::debug!(
                "client core sent a request header, service = {}, method = {}",
                service,
                method
            );
        }

        {
            let output_frame_sender = writable_stream.clone();

            tokio::spawn(async move {
                while let Some(payload) = self.request.next().await {
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
            while let Some(frame) = readable_stream.recv().await {
                #[cfg(feature = "log")]
                log::debug!("client core received a response frame, frame = {:?}", frame);

                // The stream has ended, terminate the process. If the first packet is the end
                // of the stream, the stream is invalid.
                if (frame.flags & proto::FrameFlags::EndOfStream as u32) != 0 {
                    if let Some(tx) = response_header_sender.take() {
                        let _ = tx.send(Err(Error::InvalidStream));
                    }

                    break;
                }

                if let Some(payload) = frame.payload {
                    match payload {
                        proto::frame::Payload::Response(response) => {
                            // Received a response before receiving the response header, this is an
                            // invalid stream.
                            if let Some(tx) = response_header_sender.take() {
                                let _ = tx.send(Err(Error::InvalidStream));

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
                                    Err(Error::ErrorResponse(
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
        let metadata = tokio::time::timeout(self.timeout, response_header_receiver)
            .await
            .map_err(|_| Error::Timeout)?
            .map_err(|_| Error::Shutdown)??;

        Ok((
            Stream::from(UnboundedReceiverStream::from(response_stream_receiver)),
            metadata,
        ))
    }
}
