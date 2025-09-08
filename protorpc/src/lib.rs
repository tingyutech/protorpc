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
pub mod result;
pub mod routers;
pub mod transport;

#[cfg(not(doc))]
pub mod client;

#[cfg(not(doc))]
pub mod server;

/// see: <https://docs.rs/async-trait/latest/async_trait>
pub use async_trait::async_trait;
pub use futures_core;
pub use serde;
pub use tokio_stream;

#[cfg(not(doc))]
use crate::routers::MessageStream;

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

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct OrderNumber {
    id: u64,
    number: u64,
}

impl Default for OrderNumber {
    fn default() -> Self {
        Self::from(uuid::Uuid::new_v4().as_u128())
    }
}

impl From<u128> for OrderNumber {
    fn from(value: u128) -> Self {
        Self {
            id: (value >> 64) as u64,
            number: (value & 0xFFFFFFFFFFFFFFFF) as u64,
        }
    }
}

impl proto::Frame {
    pub fn order_number(&self) -> OrderNumber {
        OrderNumber {
            id: self.id,
            number: self.number,
        }
    }

    pub fn set_order_number(&mut self, value: OrderNumber) {
        self.number = value.number;
        self.id = value.id;
    }
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

pub struct NamedPayload<T> {
    pub transport: u32,
    pub payload: T,
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
    fn build(ctx: Self::Context, stream: MessageStream) -> Self::Output;
}

pub(crate) mod task {
    #[cfg(not(target_family = "wasm"))]
    pub use tokio::spawn;

    #[cfg(target_family = "wasm")]
    pub fn spawn<T>(future: T)
    where
        T: Future + 'static,
        T::Output: 'static,
    {
        wasm_bindgen_futures::spawn_local(async move {
            future.await;
        });
    }
}
