use std::{collections::HashMap, ops::Deref};

use prost::Message;
use tokio::sync::mpsc::unbounded_channel;
use tokio_stream::{StreamExt, wrappers::UnboundedReceiverStream};

use crate::Stream;

pub struct Response<T> {
    pub payload: T,
    pub metadata: HashMap<String, String>,
}

impl<T> Response<T> {
    pub fn into_inner(self) -> T {
        self.payload
    }

    pub fn set_metadata(&mut self, metadata: HashMap<String, String>) {
        self.metadata = metadata;
    }

    pub fn get_metadata(&self) -> &HashMap<String, String> {
        &self.metadata
    }
}

impl<T: Message> Response<T> {
    pub fn into_once(self) -> Response<Stream<Vec<u8>>> {
        Response {
            payload: Stream::once(self.payload.encode_to_vec()),
            metadata: self.metadata,
        }
    }
}

impl<T, S> Response<S>
where
    T: Message + Unpin + 'static,
    S: futures_core::Stream<Item = T> + Unpin + Send + 'static,
{
    pub fn into_stream(mut self) -> Response<Stream<Vec<u8>>> {
        let (tx, rx) = unbounded_channel::<Vec<u8>>();
        tokio::spawn(async move {
            while let Some(item) = self.payload.next().await {
                if tx.send(item.encode_to_vec()).is_err() {
                    break;
                }
            }
        });

        Response {
            payload: Stream::from(UnboundedReceiverStream::from(rx)),
            metadata: self.metadata,
        }
    }
}

impl<T> Deref for Response<T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        &self.payload
    }
}

impl<T> From<T> for Response<T> {
    fn from(payload: T) -> Self {
        Self {
            payload,
            metadata: Default::default(),
        }
    }
}

impl<T> AsRef<T> for Response<T> {
    fn as_ref(&self) -> &T {
        &self.payload
    }
}

impl<T> AsMut<T> for Response<T> {
    fn as_mut(&mut self) -> &mut T {
        &mut self.payload
    }
}
