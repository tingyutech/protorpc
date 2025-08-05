use std::collections::HashMap;

use async_trait::async_trait;
use prost::Message;
use uuid::Uuid;

use crate::{
    Stream,
    client::{BaseRequest, Error, RequestHandler},
    transport::{IOStream, Transport},
};

/// Exclusive stream implementation
pub struct Exclusive<F>(F);

impl<F> Exclusive<F> {
    pub fn new(transport: F) -> Self {
        Self(transport)
    }
}

#[cfg_attr(target_family = "wasm", async_trait(?Send))]
#[cfg_attr(not(target_family = "wasm"), async_trait)]
impl<F> RequestHandler for Exclusive<F>
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
        // Let the external transport layer create an independent stream for the
        // current request.
        let order_number = Uuid::new_v4().as_u128();
        let IOStream { receiver, sender } = self
            .0
            .create_stream(order_number)
            .await
            .map_err(|e| Error::Transport(format!("{:?}", e)))?;

        req.request(sender, receiver, order_number.into()).await
    }
}
