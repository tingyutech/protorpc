//! Rpc Response
//!
//! For example:
//!
//! ```rust,ignore
//! mod pb {
//!     protorpc::include_proto!("helloworld");
//! }
//!
//! let response = Response::from(pb::helloworld::HelloResponse {
//!     name: "world".to_string(),
//! });
//!
//! let response_stream = Response::from(tokio_stream::iter(vec![
//!     pb::helloworld::HelloResponse {
//!         name: "world".to_string(),
//!     },
//!     pb::helloworld::HelloResponse {
//!         name: "world".to_string(),
//!     },
//! ]));
//! ```

use std::{collections::HashMap, ops::Deref};

use prost::Message;
use tokio::sync::mpsc::unbounded_channel;
use tokio_stream::{StreamExt, wrappers::UnboundedReceiverStream};

use crate::{Stream, result::RpcError, task::spawn};

/// Represents a response
pub struct Response<T> {
    /// The payload of the response
    pub payload: T,
    /// The metadata of the response
    pub metadata: HashMap<String, String>,
}

impl<T> Response<T> {
    /// Create a new response
    pub fn new(payload: T) -> Self {
        Self {
            payload,
            metadata: Default::default(),
        }
    }

    /// Get a reference to the message
    pub fn get_ref(&self) -> &T {
        &self.payload
    }

    /// Get a mutable reference to the message
    pub fn get_mut(&mut self) -> &mut T {
        &mut self.payload
    }

    /// Consumes `self`, returning the payload.
    pub fn into_inner(self) -> T {
        self.payload
    }

    /// Set the custom response metadata.
    pub fn set_metadata(&mut self, metadata: HashMap<String, String>) {
        self.metadata = metadata;
    }

    /// Get a reference to the custom response metadata.
    pub fn get_metadata(&self) -> &HashMap<String, String> {
        &self.metadata
    }

    pub fn metadata_mut(&mut self) -> &mut HashMap<String, String> {
        &mut self.metadata
    }
}

impl<T: Message> Response<T> {
    /// Convert the response to a once response.
    ///
    /// This is only used internally and should not concern external users.
    pub fn into_once(self) -> Response<Stream<Result<Vec<u8>, RpcError>>> {
        Response {
            payload: Stream::once(Ok(self.payload.encode_to_vec())),
            metadata: self.metadata,
        }
    }
}

impl<T, S> Response<S>
where
    T: Message + Unpin + 'static,
    S: futures_core::Stream<Item = Result<T, RpcError>> + Unpin + Send + 'static,
{
    /// Convert the response to a stream response.
    ///
    /// This is only used internally and should not concern external users.
    pub fn into_stream(mut self) -> Response<Stream<Result<Vec<u8>, RpcError>>> {
        let (tx, rx) = unbounded_channel::<Result<Vec<u8>, RpcError>>();
        spawn(async move {
            while let Some(item) = self.payload.next().await {
                if tx
                    .send(
                        item.map(|it| it.encode_to_vec())
                            .map_err(|e| RpcError::from(e)),
                    )
                    .is_err()
                {
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
