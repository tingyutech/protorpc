pub mod exclusive;
pub mod multiplex;

use std::collections::HashMap;

use async_trait::async_trait;
use prost::Message;
use tokio::{
    sync::{
        mpsc::{UnboundedReceiver, UnboundedSender, unbounded_channel},
        oneshot,
    },
    time::Duration,
};

use tokio_stream::{StreamExt, wrappers::UnboundedReceiverStream};

#[cfg(not(target_family = "wasm"))]
use tokio::time::timeout;

#[cfg(target_family = "wasm")]
use wasmtimer::tokio::timeout;

use crate::{
    OrderNumber, Stream, proto,
    result::{IoResult, RpcError},
    task::spawn,
};

/// Request handler trait
#[cfg_attr(target_family = "wasm", async_trait(?Send))]
#[cfg_attr(not(target_family = "wasm"), async_trait)]
pub trait RequestHandler {
    /// Handle request
    ///
    /// This method is used internally and should not concern external users.
    async fn request<T, Q, S>(
        &self,
        base_request: BaseRequest<'_, T>,
    ) -> Result<(Stream<Result<S, RpcError>>, HashMap<String, String>), RpcError>
    where
        Q: Message,
        S: Message + Unpin + Default + 'static,
        T: futures_core::Stream<Item = Result<Q, RpcError>> + Unpin + Send + 'static;
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

impl<'a, T> std::fmt::Debug for BaseRequest<'a, T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BaseRequest")
            .field("service", &self.service)
            .field("method", &self.method)
            .field("timeout", &self.timeout)
            .field("metadata", &self.metadata)
            .finish()
    }
}

impl<'a, T> BaseRequest<'a, T> {
    async fn request<Q, S>(
        mut self,
        writable_stream: UnboundedSender<proto::Frame>,
        mut readable_stream: UnboundedReceiver<IoResult<proto::Frame>>,
        order_number: OrderNumber,
    ) -> Result<(Stream<Result<S, RpcError>>, HashMap<String, String>), RpcError>
    where
        Q: Message,
        S: Message + Unpin + Default + 'static,
        T: futures_core::Stream<Item = Result<Q, RpcError>> + Unpin + Send + 'static,
    {
        #[cfg(feature = "log")]
        log::debug!(
            "client core send a request, service = {}, method = {}",
            self.service,
            self.method
        );

        // The overall process actually simulates HTTP, but with some differences.

        let mut frame = proto::Frame {
            service: self.service.to_string(),
            method: self.method.to_string(),
            payload: None,
            ..Default::default()
        };

        frame.set_order_number(order_number);

        // Response data stream channel.
        let (response_stream_sender, response_stream_receiver) = unbounded_channel();

        // Channel for delivering the response header.
        let (response_header_sender, response_header_receiver) =
            oneshot::channel::<Result<HashMap<String, String>, RpcError>>();

        #[cfg(feature = "log")]
        log::debug!(
            "client core registered a response frame handler, service = {}, method = {}, order id = {:?}",
            self.service,
            self.method,
            order_number
        );

        // First send the request header, similar to an HTTP request header.
        {
            frame.payload = Some(proto::frame::Payload::RequestHeader(proto::RequestHeader {
                metadata: self.metadata,
            }));

            writable_stream
                .send(frame.clone())
                .map_err(|_| RpcError::terminated())?;

            #[cfg(feature = "log")]
            log::debug!(
                "client core sent a request header, service = {}, method = {}",
                self.service,
                self.method
            );
        }

        {
            let mut response_header_sender = Some(response_header_sender);

            spawn(async move {
                while let Some(frame) = readable_stream.recv().await {
                    match frame {
                        Ok(frame) => {
                            if frame.order_number() != order_number {
                                continue;
                            }

                            #[cfg(feature = "log")]
                            log::debug!(
                                "client core received a response frame, frame = {:?}",
                                frame
                            );

                            if let Some(payload) = frame.payload {
                                match payload {
                                    proto::frame::Payload::Response(response) => {
                                        // Received a response before receiving the response header,
                                        // this is
                                        // an invalid stream.
                                        if let Some(tx) = response_header_sender.take() {
                                            let _ = tx.send(Err(RpcError::invalid_stream().into()));

                                            break;
                                        }

                                        // Any error here will cause the current stream to be
                                        // terminated
                                        // directly.
                                        if let Ok(payload) = S::decode(response.payload.as_ref()) {
                                            if response_stream_sender.send(Ok(payload)).is_err() {
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
                                                Err(header
                                                    .error
                                                    .map(|e| RpcError::from(e))
                                                    .unwrap_or_else(|| RpcError::unknown().into()))
                                            })
                                        });
                                    }
                                    proto::frame::Payload::EndOfStream(frame) => {
                                        if let Some(tx) = response_header_sender.take() {
                                            let _ = tx.send(Err(RpcError::invalid_stream().into()));
                                        } else {
                                            if let Some(e) = frame.error {
                                                let _ = response_stream_sender
                                                    .send(Err(RpcError::from(e)));
                                            }
                                        }

                                        break;
                                    }
                                    _ => (),
                                }
                            }
                        }
                        Err(e) => {
                            let err = RpcError::from(e);

                            if let Some(tx) = response_header_sender.take() {
                                let _ = tx.send(Err(err));
                            } else {
                                let _ = response_stream_sender.send(Err(err));
                            }

                            break;
                        }
                    }
                }
            });
        }

        {
            let output_frame_sender = writable_stream.clone();

            spawn(async move {
                let mut serial_number = 0;
                let mut result = None;

                while let Some(payload) = self.request.next().await {
                    #[cfg(feature = "log")]
                    log::debug!(
                        "client core send a request payload, service = {}, order id = {:?}",
                        frame.service,
                        order_number
                    );

                    let payload = match payload {
                        Ok(it) => it,
                        Err(e) => {
                            result = Some(e);

                            break;
                        }
                    };

                    frame.payload = Some(proto::frame::Payload::Request(proto::Request {
                        serial_number,
                        payload: payload.encode_to_vec(),
                    }));

                    serial_number += 1;

                    #[allow(unused_variables)]
                    if let Err(e) = output_frame_sender.send(frame.clone()) {
                        #[cfg(feature = "log")]
                        log::warn!(
                            "client core failed to send a response frame, service = {}, order id = {:?}, error = {:?}",
                            frame.service,
                            order_number,
                            e
                        );

                        break;
                    }
                }

                // After the stream is closed, an `EndOfStream` packet needs to be sent.
                {
                    frame.payload = Some(proto::frame::Payload::EndOfStream(
                        result
                            .map(|it| proto::EndOfStream {
                                success: false,
                                error: Some(RpcError::from(it).to_string()),
                            })
                            .unwrap_or_else(|| proto::EndOfStream {
                                success: true,
                                error: None,
                            }),
                    ));

                    let _ = output_frame_sender.send(frame.clone());

                    #[cfg(feature = "log")]
                    log::debug!(
                        "client core sent a request end of stream frame, service = {}, order id = {:?}",
                        frame.service,
                        order_number
                    );
                }
            });
        }

        // try it the result
        let metadata = timeout(self.timeout, response_header_receiver)
            .await
            .map_err(|_| {
                RpcError::timeout(&format!(
                    "service: {}, method: {}",
                    self.service, self.method
                ))
            })?
            .map_err(|_| RpcError::terminated())??;

        Ok((
            Stream::from(UnboundedReceiverStream::from(response_stream_receiver)),
            metadata,
        ))
    }
}
