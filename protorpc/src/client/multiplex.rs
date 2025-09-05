use std::{collections::HashMap, sync::Arc};

use async_trait::async_trait;
use prost::Message;
use tokio::{
    sync::{
        RwLock,
        mpsc::{UnboundedSender, unbounded_channel},
    },
    time::Duration,
};

use uuid::Uuid;

use crate::{
    OrderNumber, Stream,
    client::{BaseRequest, RequestHandler},
    proto,
    result::{IoResult, RpcError},
    task::spawn,
    transport::IOStream,
};

/// Multiplexing implementation
pub struct Multiplex {
    output_frame_sender: UnboundedSender<proto::Frame>,
    frame_handlers: Arc<RwLock<HashMap<OrderNumber, UnboundedSender<IoResult<proto::Frame>>>>>,
}

impl Multiplex {
    pub fn new(
        IOStream {
            receiver: mut readable_stream,
            sender: writable_stream,
        }: IOStream,
    ) -> Self {
        let frame_handlers: Arc<
            RwLock<HashMap<OrderNumber, UnboundedSender<IoResult<proto::Frame>>>>,
        > = Default::default();

        {
            let frame_handlers_ = frame_handlers.clone();

            spawn(async move {
                while let Some(frame) = readable_stream.recv().await {
                    match frame {
                        Ok(frame) => {
                            #[cfg(feature = "log")]
                            log::debug!(
                                "client core bus received a response frame, frame = {:?}",
                                frame
                            );

                            if let Some(handler) =
                                frame_handlers_.read().await.get(&frame.order_number())
                            {
                                if !handler.is_closed() {
                                    let _ = handler.send(Ok(frame));
                                }
                            }
                        }
                        Err(e) => {
                            for (_, tx) in frame_handlers_.write().await.drain() {
                                let _ = tx.send(Err(std::io::Error::new(e.kind(), e.to_string())));
                            }

                            break;
                        }
                    }
                }
            });
        }

        // A dedicated task for cleaning up dangling handles.
        {
            let frame_handlers_ = Arc::downgrade(&frame_handlers);

            spawn(async move {
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
            spawn(async move {
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
}

#[cfg_attr(target_family = "wasm", async_trait(?Send))]
#[cfg_attr(not(target_family = "wasm"), async_trait)]
impl RequestHandler for Multiplex {
    async fn request<T, Q, S>(
        &self,
        req: BaseRequest<'_, T>,
    ) -> Result<(Stream<Result<S, RpcError>>, HashMap<String, String>), RpcError>
    where
        Q: Message,
        S: Message + Unpin + Default + 'static,
        T: futures_core::Stream<Item = Result<Q, RpcError>> + Unpin + Send + 'static,
    {
        // All frames sent by the remote response are delivered through this channel.
        let (response_frame_sender, response_frame_receiver) =
            unbounded_channel::<IoResult<proto::Frame>>();

        let order_number: OrderNumber = Uuid::new_v4().as_u128().into();
        self.frame_handlers
            .write()
            .await
            .insert(order_number, response_frame_sender);

        req.request(
            self.output_frame_sender.clone(),
            response_frame_receiver,
            order_number,
        )
        .await
    }
}
