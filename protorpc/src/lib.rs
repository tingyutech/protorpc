pub mod client;
pub mod request;
pub mod response;
pub mod routers;
pub mod server;
pub mod transport;

pub use async_trait::async_trait;
pub use futures_core;
pub use tokio_stream;

#[macro_export]
macro_rules! include_proto {
    ($package: tt) => {
        include!(concat!(env!("OUT_DIR"), concat!("/", $package, ".rs")));
    };
}

mod proto {
    include_proto!("protorpc.core");
}

pub struct Stream<T: Send>(Box<dyn futures_core::Stream<Item = T> + Send + Unpin>);

impl<T: Send + 'static> Stream<T> {
    pub fn once(value: T) -> Self {
        Self(Box::new(tokio_stream::once(value)))
    }

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

pub trait RpcServiceBuilder {
    const NAME: &'static str;

    type Context;
    type Output;

    fn build(ctx: Self::Context, transport: transport::IOStream) -> Self::Output;
}
