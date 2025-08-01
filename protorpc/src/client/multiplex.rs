use std::{collections::HashMap, sync::Arc, time::Duration};

use async_trait::async_trait;
use prost::Message;
use tokio::sync::{
    RwLock,
    mpsc::{UnboundedSender, unbounded_channel},
};
use uuid::Uuid;

use crate::{
    Stream,
    client::{BaseRequest, Error, RequestHandler},
    proto,
    transport::IOStream,
};

/// Multiplexing implementation
pub struct Multiplex {
    output_frame_sender: UnboundedSender<proto::Frame>,
    frame_handlers: Arc<RwLock<HashMap<u128, UnboundedSender<proto::Frame>>>>,
}

impl Multiplex {
    pub fn new(
        IOStream {
            receiver: mut readable_stream,
            sender: writable_stream,
        }: IOStream,
    ) -> Self {
        let frame_handlers: Arc<RwLock<HashMap<u128, UnboundedSender<proto::Frame>>>> =
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

                    if let Some(handler) = frame_handlers_.read().await.get(&frame.order_number()) {
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
}

#[cfg_attr(target_family = "wasm", async_trait(?Send))]
#[cfg_attr(not(target_family = "wasm"), async_trait)]
impl RequestHandler for Multiplex {
    async fn request<T, Q, S>(
        &self,
        req: BaseRequest<'_, T>,
    ) -> Result<(Stream<S>, HashMap<String, String>), Error>
    where
        Q: Message,
        S: Message + Unpin + Default + 'static,
        T: futures_core::Stream<Item = Q> + Unpin + Send + 'static,
    {
        // All frames sent by the remote response are delivered through this channel.
        let (response_frame_sender, response_frame_receiver) = unbounded_channel::<proto::Frame>();

        let id = Uuid::new_v4().as_u128();
        self.frame_handlers
            .write()
            .await
            .insert(id, response_frame_sender);

        req.request(
            self.output_frame_sender.clone(),
            response_frame_receiver,
            id,
        )
        .await
    }
}
