use std::{collections::HashMap, sync::Arc};

use prost::Message;
use tokio::{
    sync::{mpsc::{unbounded_channel, UnboundedReceiver}, oneshot},
    time::Duration,
};

use tokio_stream::{StreamExt, wrappers::UnboundedReceiverStream};

#[cfg(not(target_family = "wasm"))]
use tokio::time::timeout;

use uuid::Uuid;
#[cfg(target_family = "wasm")]
use wasmtimer::tokio::timeout;

use crate::{
    OrderNumber, Stream,
    helper::{DropGuard, FrameBuilder, spawn},
    proto,
    result::RpcError,
    transport::{IOStream, Transport},
};

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

pub struct RequestHandler<F>(F);

impl<F> RequestHandler<F> {
    pub fn new(transport: F) -> Self {
        Self(transport)
    }
}

impl<F> RequestHandler<F>
where
    F: Transport,
{
    pub async fn request<T, Q, S>(
        &self,
        mut req: BaseRequest<'static, T>,
    ) -> Result<(Stream<Result<S, RpcError>>, HashMap<String, String>), RpcError>
    where
        Q: Message,
        S: Message + Unpin + Default + 'static,
        T: futures_core::Stream<Item = Result<Q, RpcError>> + Unpin + Send + 'static,
    {
        // Let the external transport layer create an independent stream for the
        // current request.
        let order_number = Uuid::new_v4().as_u128();
        let socket: Arc<IOStream> = self
            .0
            .create_stream(order_number)
            .await
            .map_err(|e| RpcError::from(e))?
            .into();

        #[cfg(feature = "log")]
        log::debug!(
            "client core send a request, service = {}, method = {}",
            req.service,
            req.method
        );

        // The overall process actually simulates HTTP, but with some differences.
        let order_number: OrderNumber = order_number.into();
        let mut frame_builder = FrameBuilder::new(
            req.service.to_string(),
            req.method.to_string(),
            order_number,
        );

        // Response data stream channel.
        let (response_stream_sender, response_stream_receiver) = unbounded_channel();

        // Channel for delivering the response header.
        let (response_header_sender, response_header_receiver) =
            oneshot::channel::<Result<HashMap<String, String>, RpcError>>();

        // Channel for delivering the close signal.
        let (close_sender, mut close_receiver) = unbounded_channel::<()>();

        #[cfg(feature = "log")]
        log::debug!(
            "client core registered a response frame handler, service = {}, method = {}, order id = {:?}",
            req.service,
            req.method,
            order_number
        );

        // First send the request header, similar to an HTTP request header.
        {
            socket
                .send(frame_builder.request_header(req.metadata))
                .map_err(|_| RpcError::terminated())?;

            #[cfg(feature = "log")]
            log::debug!(
                "client core sent a request header, service = {}, method = {}",
                req.service,
                req.method
            );
        }

        // When both the req and res are dropped, a Close packet needs to be sent
        //to notify the other side that the stream is closed.
        let drop_guard = {
            let socket = socket.clone();
            let frame = frame_builder.close();

            DropGuard::new(move || {
                log::info!(
                    "client request close, service = {}, method = {}",
                    frame.service,
                    frame.method
                );

                let _ = socket.send(frame);
            })
        };

        // A special part for handling the response data packet from the server.
        {
            let mut response_header_sender = Some(response_header_sender);
            let socket = socket.clone();
            let drop_guard = drop_guard.clone();

            spawn(async move {
                let mut endofstream = false;
                let mut current_error: Option<std::io::Error> = None;

                while let Some(frame) = socket.recv().await {
                    let frame = match frame {
                        Ok(it) => it,
                        Err(e) => {
                            current_error = Some(e);

                            break;
                        }
                    };

                    #[cfg(feature = "log")]
                    log::debug!("client core received a response frame, frame = {:?}", frame);

                    if frame.order_number() != order_number {
                        continue;
                    }

                    if let Some(payload) = frame.payload {
                        match payload {
                            proto::frame::Payload::Response(response) => {
                                // Received a response before receiving the response header,
                                // this is an invalid stream.
                                if let Some(tx) = response_header_sender.take() {
                                    let _ = tx.send(Err(RpcError::invalid_stream().into()));

                                    break;
                                }

                                // Any error here will cause the current stream to be
                                // terminated directly.
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
                                endofstream = true;

                                if let Some(tx) = response_header_sender.take() {
                                    let _ = tx.send(Err(RpcError::invalid_stream().into()));
                                } else {
                                    if let Some(e) = frame.error {
                                        let _ = response_stream_sender.send(Err(RpcError::from(e)));
                                    }
                                }

                                break;
                            }
                            proto::frame::Payload::Close(_) => {
                                let _ = close_sender.send(());

                                break;
                            }
                            _ => (),
                        }
                    }
                }

                if let Some(e) = current_error {
                    if !endofstream {
                        let err = RpcError::from(e);

                        if let Some(tx) = response_header_sender.take() {
                            let _ = tx.send(Err(err));
                        } else {
                            let _ = response_stream_sender.send(Err(err));
                        }
                    }
                }

                #[cfg(feature = "log")]
                log::info!(
                    "reponses stream closed, service = {}, method = {}, order id = {:?}",
                    req.service,
                    req.method,
                    order_number
                );

                drop_guard.drop();
            });
        }

        // A special part for sending the request data packet to the server.
        {
            spawn(async move {
                let mut serial_number = 0;
                let mut result = Ok(());

                loop {
                    tokio::select! {
                        payload = req.request.next() => {
                            #[cfg(feature = "log")]
                            log::debug!("client request next() returned: {:?}", payload.is_some());
                            
                            match payload {
                                Some(payload) => {
                                    #[cfg(feature = "log")]
                                    log::debug!(
                                        "client core send a request payload, service = {}, order id = {:?}",
                                        frame_builder.service,
                                        order_number
                                    );

                                    let payload = match payload {
                                        Ok(it) => it,
                                        Err(e) => {
                                            result = Err(e);
                                            
                                            break;
                                        }
                                    };

                                    #[allow(unused_variables)]
                                    if let Err(e) = socket.send(frame_builder.request(serial_number, payload.encode_to_vec())) {
                                        #[cfg(feature = "log")]
                                        log::warn!(
                                            "client core failed to send a response frame, service = {}, order id = {:?}, error = {:?}",
                                            frame_builder.service,
                                            order_number,
                                            e
                                        );

                                        break;
                                    }

                                    serial_number += 1;
                                }
                                None => {
                                    #[cfg(feature = "log")]
                                    log::debug!("client request stream ended, service = {}, method = {}", frame_builder.service, req.method);
                                    
                                    break;
                                }
                            }
                        }
                        Some(_) = wait_maybe_closed_channel(&mut close_receiver) => {
                            break;
                        }
                    }
                }

                #[cfg(feature = "log")]
                log::info!(
                    "requests stream closed, service = {}, method = {}, order id = {:?}",
                    frame_builder.service,
                    req.method,
                    order_number
                );

                // After the stream is closed, an `EndOfStream` packet needs to be sent.
                {
                    #[cfg(feature = "log")]
                    log::debug!("client sending EndOfStream frame, service = {}, method = {}", frame_builder.service, req.method);
                    let _ = socket.send(frame_builder.end_of_stream(&result));
                }

                drop_guard.drop();
            });
        }

        // try it the result
        let metadata = timeout(req.timeout, response_header_receiver)
            .await
            .map_err(|_| {
                RpcError::timeout(&format!("service: {}, method: {}", req.service, req.method))
            })?
            .map_err(|_| RpcError::terminated())??;

        Ok((
            Stream::from(UnboundedReceiverStream::from(response_stream_receiver)),
            metadata,
        ))
    }
}

async fn wait_maybe_closed_channel(receiver: &mut UnboundedReceiver<()>) -> Option<()> {
    if receiver.is_closed() {
        std::future::pending::<()>().await;
    }

    receiver.recv().await
}
