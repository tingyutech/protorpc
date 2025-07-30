//! Rpc Request
//!
//! For example:
//!
//! ```rust,ignore
//! mod pb {
//!     protorpc::include_proto!("helloworld");
//! }
//!
//! let request = Request::from(pb::helloworld::HelloRequest {
//!     name: "world".to_string(),
//! });
//!
//! let request_stream = Request::from(tokio_stream::iter(vec![
//!     pb::helloworld::HelloRequest {
//!         name: "world".to_string(),
//!     },
//!     pb::helloworld::HelloRequest {
//!         name: "world".to_string(),
//!     },
//! ]));
//! ```

use std::{collections::HashMap, ops::Deref, time::Duration};

/// Default timeout is 5 seconds
///
/// ```rust,ignore
/// Duration::from_secs(5)
/// ```
pub const DEFAULT_TIMEOUT: Duration = Duration::from_secs(5);

/// Represents a request
pub struct Request<T> {
    /// The payload of the request
    pub payload: T,
    /// The timeout of the request
    ///
    /// This is only meaningful on the client side, and the server ignores it.
    pub timeout: Duration,
    /// The metadata of the request
    ///
    /// Similar to http headers, can carry custom key-value pairs.
    pub metadata: HashMap<String, String>,
}

impl<T> Request<T> {
    /// Consumes `self`, returning the payload.
    pub fn into_inner(self) -> T {
        self.payload
    }

    /// Set the max duration the request is allowed to take.
    pub fn set_timeout(&mut self, timeout: Duration) {
        self.timeout = timeout;
    }

    /// Get the timeout of the request.
    pub fn get_timeout(&self) -> Duration {
        self.timeout
    }

    /// Set the custom request metadata.
    pub fn set_metadata(&mut self, metadata: HashMap<String, String>) {
        self.metadata = metadata;
    }

    /// Get a reference to the custom request metadata.
    pub fn get_metadata(&self) -> &HashMap<String, String> {
        &self.metadata
    }
}

impl<T> Deref for Request<T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        &self.payload
    }
}

impl<T> AsRef<T> for Request<T> {
    fn as_ref(&self) -> &T {
        &self.payload
    }
}

impl<T> AsMut<T> for Request<T> {
    fn as_mut(&mut self) -> &mut T {
        &mut self.payload
    }
}

impl<T> From<T> for Request<T> {
    fn from(payload: T) -> Self {
        Self {
            payload,
            timeout: DEFAULT_TIMEOUT,
            metadata: Default::default(),
        }
    }
}
