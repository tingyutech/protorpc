use std::collections::HashMap;

use async_trait::async_trait;
use nanoid::nanoid;
use prost::Message;

use crate::{
    Stream,
    client::{BaseRequest, Error, RequestHandler},
    transport::{IOStream, Transport},
};

pub struct Streaming<F>(F);

impl<F> Streaming<F> {
    pub fn new(transport: F) -> Self {
        Self(transport)
    }
}

#[async_trait]
impl<F> RequestHandler for Streaming<F>
where
    F: Transport,
{
    async fn request<T, Q, S>(
        &self,
        req: BaseRequest<'_, T>,
    ) -> Result<(Stream<S>, HashMap<String, String>), Error>
    where
        Q: Message,
        S: Message + Unpin + Default + 'static,
        T: futures_core::Stream<Item = Q> + Unpin + Send + 'static,
    {
        let id = nanoid!();
        let IOStream { receiver, sender } = self
            .0
            .create_stream(&id)
            .await
            .map_err(|e| Error::Transport(format!("{:?}", e)))?
            .into();

        req.request(sender, receiver, id).await
    }
}
