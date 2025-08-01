//! This is an RPC framework similar to tonic, but unlike tonic, it does not
//! rely on gRPC. Instead, it uses protobuf as the RPC protocol and is not tied
//! to any transport layer.
//!
//! ## Client Request Modes
//!
//! ### Exclusive Stream (Head-of-line Blocking)
//!
//! In this mode, each request exclusively occupies a stream. The client will
//! request a new stream from the external implementation via the [`Transport`]
//! trait, and this stream will be exclusively used by the current request and
//! response.
//!
//! This mode is similar to HTTP/1.1, but the difference is that it is
//! full-duplex, allowing request and response content to be sent
//! simultaneously.
//!
//! ##### sequence diagram:
//!
//! ```text
//! Client                           Server
//!   |                                 |
//!   |-------- Request Header -------->|
//!   |                                 |
//!   |                         [Server Activated]
//!   |<------- Response Header --------|
//!   |                         [Server Deactivated]
//!   |                                 |
//!   |  --- Request/Response Handshake Completed ---
//!   |                                 |
//!   |-------- Request Body ---------->|
//!   |<------- Response Body ----------|
//!   |                                 |
//!   |  ---- Request/Response Stopped --------------
//!   |                                 |
//!   |-------- EndOfStream ----------->|
//!   |<------- EndOfStream ------------|
//!   |                                 |
//! ```
//!
//! ##### For example:
//!
//! ```rust,ignore
//! mod pd {
//!     protorpc::include_proto!("helloworld");
//! }
//!
//! let client = pd::helloworld::HelloWorldClient::with_transport(transport);
//! ```
//!
//! ### Multiplexed Stream (Non-Head-of-line Blocking)
//!
//! In this mode, multiple requests can share a single stream. Within the
//! current stream, multiple requests and responses can be sent simultaneously
//! without blocking each other.
//!
//! This mode is similar to HTTP/2.0, allowing multiple requests and responses
//! to be sent concurrently over the same session.
//!
//! ##### sequence diagram:
//!
//! ```text
//! Client                           Server
//!   |                                 |
//!   |-------- Request Header -------->|
//!   |                                 |
//!   |                         ---------------- order id ------------> [ROUTES]
//!   |<------- Response Header --------|
//!   |                         <--------------- order id ------------- [ROUTES]
//!   |                                 |
//!   |  --- Request/Response Handshake Completed ---
//!   |                                 |
//!   |-------- Request Body ---------->|
//!   |                         ---------------- order id ------------> [ROUTES]
//!   |<------- Response Body ----------|
//!   |                         <--------------- order id ------------- [ROUTES]
//!   |                                 |
//!   |  ---- Request/Response Stopped --------------
//!   |                                 |
//!   |-------- EndOfStream ----------->|
//!   |<------- EndOfStream ------------|
//!   |                                 |
//! ```
//!
//! ##### For example:
//!
//! ```rust,ignore
//! mod pd {
//!     protorpc::include_proto!("helloworld");
//! }
//!
//! let client = pd::helloworld::HelloWorldClient::with_stream(stream);
//! ```
//!
//! [`Transport`]: crate::transport::Transport

pub mod request;
pub mod response;
pub mod routers;
pub mod transport;

#[cfg(not(doc))]
pub mod client;

#[cfg(not(doc))]
pub mod server;

/// see: <https://docs.rs/async-trait/latest/async_trait>
pub use async_trait::async_trait;
pub use futures_core;
pub use tokio_stream;

use thiserror::Error;

/// Include generated proto server and client items.
///
/// You must specify the gRPC package name.
///
/// ```rust,ignore
/// mod pb {
///     protorpc::include_proto!("helloworld");
/// }
/// ```
///
///
/// # Note:
/// **This only works if the protorpc-build output directory has been
/// unmodified**. The default output directory is set to the [`OUT_DIR`]
/// environment variable. If the output directory has been modified, the
/// following pattern may be used instead of this macro.
///
/// ```rust,ignore
/// mod pb {
///     include!("/relative/protobuf/directory/helloworld.rs");
/// }
/// ```
/// You can also use a custom environment variable using the following pattern.
/// ```rust,ignore
/// mod pb {
///     include!(concat!(env!("PROTOBUFS"), "/helloworld.rs"));
/// }
/// ```
///
/// [`OUT_DIR`]: https://doc.rust-lang.org/cargo/reference/environment-variables.html#environment-variables-cargo-sets-for-build-scripts
#[macro_export]
macro_rules! include_proto {
    ($package: tt) => {
        include!(concat!(env!("OUT_DIR"), concat!("/", $package, ".rs")));
    };
}

mod proto {
    include_proto!("protorpc.core");
}

impl proto::Frame {
    pub fn order_number(&self) -> u128 {
        ((self.id_high as u128) << 64) | (self.id_low as u128)
    }
}

/// Errors that occur during requests
#[derive(Debug, Error)]
pub enum Error {
    /// Transport layer has been terminated
    #[error("transport terminated")]
    Terminated,
    /// Request timeout
    #[error("request timeout")]
    Timeout,
    /// Core service has been shutdown
    #[error("core service shutdown")]
    Shutdown,
    /// Invalid response
    #[error("invalid response: {0}")]
    InvalidResponse(#[from] prost::DecodeError),
    /// Invalid request stream/response stream
    #[error("invalid response stream")]
    InvalidStream,
    /// Server error while processing request
    #[error("error response: {0}")]
    ErrorResponse(String),
    /// Transport error
    #[error("transport response: {0}")]
    Transport(String),
}

/// A unidirectional stream
pub struct Stream<T: Send>(Box<dyn futures_core::Stream<Item = T> + Send + Unpin>);

impl<T: Send + 'static> Stream<T> {
    /// Create a unidirectional stream with only one value
    ///
    /// Similar to [`tokio_stream::once`], but returns a `Stream` type.
    ///
    /// [`tokio_stream::once`]: https://docs.rs/tokio-stream/latest/tokio_stream/fn.once.html
    pub fn once(value: T) -> Self {
        Self(Box::new(tokio_stream::once(value)))
    }

    /// Create a unidirectional stream from a type that implements
    /// [`futures_core::Stream`]
    ///
    /// [`futures_core::Stream`]: https://docs.rs/futures-core/latest/futures_core/stream/trait.Stream.html
    pub fn from<S>(stream: S) -> Self
    where
        T: Send + 'static,
        S: futures_core::Stream<Item = T> + Send + Unpin + 'static + Sized,
    {
        Self(Box::new(stream))
    }
}

impl<T: Send> futures_core::Stream for Stream<T> {
    type Item = T;

    fn poll_next(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        std::pin::Pin::new(&mut self.as_mut().0).poll_next(cx)
    }
}

/// An RPC service builder
///
/// Types that implement this trait can be managed by [`Routes`].
///
/// [`Routes`]: crate::routers::Routes
///
/// ### This trait is not usually implemented by external code.
///
/// For example:
///
/// ```rust,ignore
/// mod pd {
///     protorpc::include_proto!("helloworld");
/// }
///
/// struct HelloWorldServer;
///
/// impl pd::helloworld::HelloWorldServerHandler for HelloWorldServer {
///     type Error = anyhow::Error;
///
///     // Implementation omitted...
/// }
///
/// let routes = Routes::new();
/// routes.make_service::<pd::helloworld::HelloWorldServer>(HelloWorldServer);
/// ```
///
/// # Note:
///
/// You can not only register servers, but also register clients, allowing
/// Routes to manage them uniformly.
///
/// ```rust,ignore
/// let client = routes.make_service::<pd::helloworld::HelloWorldClient>(());
/// ```
///
/// Note, however, that if you register a client into `Routes`, the router will
/// implement multiplexing and random selection of transport streams internally.
pub trait RpcServiceBuilder {
    const NAME: &'static str;

    type Context;
    type Output;

    #[cfg(not(doc))]
    fn build(ctx: Self::Context, transport: transport::IOStream) -> Self::Output;
}
