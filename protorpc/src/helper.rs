use std::{
    collections::HashMap,
    io::Result,
    ops::{Deref, DerefMut},
    sync::Arc,
};

use tokio::io::{AsyncWrite, AsyncWriteExt};

use crate::{OrderNumber, proto, result::RpcResult};

/// Spawn a new task.
#[cfg(not(target_family = "wasm"))]
pub use tokio::spawn;

/// Spawn a new task on wasm.
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

/// Write a buffer to a socket and flush it.
///
/// If the write fails, the error is logged and returned.
pub async fn writeaf<T>(socket: &mut T, buffer: &[u8]) -> Result<()>
where
    T: AsyncWrite + Unpin + Send + 'static,
{
    #[allow(unused_variables)]
    if let Err(e) = socket.write_all(buffer).await {
        #[cfg(feature = "log")]
        log::warn!("transport write error: {:?}", e);

        Err(e)
    } else {
        socket.flush().await?;

        Ok(())
    }
}

/// A builder for creating `proto::Frame` instances.
#[derive(Clone, Debug)]
pub struct FrameBuilder(proto::Frame);

impl Deref for FrameBuilder {
    type Target = proto::Frame;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl DerefMut for FrameBuilder {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

impl FrameBuilder {
    /// Create a new `FrameBuilder` instance.
    pub fn new(service: String, method: String, order_number: OrderNumber) -> Self {
        let mut frame = proto::Frame::default();

        frame.service = service;
        frame.method = method;

        frame.set_order_number(order_number);

        Self(frame)
    }

    /// Build a request header frame.
    pub fn request_header(&mut self, metadata: HashMap<String, String>) -> proto::Frame {
        self.0.payload = Some(proto::frame::Payload::RequestHeader(proto::RequestHeader {
            metadata,
        }));

        self.0.clone()
    }

    /// Build a request frame.
    pub fn request(&mut self, serial_number: u32, payload: Vec<u8>) -> proto::Frame {
        self.0.payload = Some(proto::frame::Payload::Request(proto::Request {
            serial_number,
            payload,
        }));

        self.0.clone()
    }

    /// Build a end of stream frame.
    pub fn end_of_stream<T>(&mut self, result: &RpcResult<T>) -> proto::Frame {
        self.0.payload = Some(proto::frame::Payload::EndOfStream(proto::EndOfStream {
            success: result.is_ok(),
            error: if let Err(e) = result {
                Some(e.to_string())
            } else {
                None
            },
        }));

        self.0.clone()
    }

    /// Build a response header frame.
    pub fn response_header<T>(
        &mut self,
        metadata: HashMap<String, String>,
        result: &RpcResult<T>,
    ) -> proto::Frame {
        self.0.payload = Some(proto::frame::Payload::ResponseHeader(
            proto::ResponseHeader {
                metadata,
                success: result.is_ok(),
                error: if let Err(e) = result {
                    Some(e.to_string())
                } else {
                    None
                },
            },
        ));

        self.0.clone()
    }

    /// Build a response frame.
    pub fn response(&mut self, serial_number: u32, payload: Vec<u8>) -> proto::Frame {
        self.0.payload = Some(proto::frame::Payload::Response(proto::Response {
            serial_number,
            payload,
        }));

        self.0.clone()
    }

    /// Build a close frame.
    pub fn close(&mut self) -> proto::Frame {
        self.0.payload = Some(proto::frame::Payload::Close(proto::Close {}));

        self.0.clone()
    }
}

struct DropGuardSingle(Option<Box<dyn FnOnce() + Sync + Send>>);

impl Drop for DropGuardSingle {
    fn drop(&mut self) {
        if let Some(func) = self.0.take() {
            func();
        }
    }
}

/// A guard that ensures a function is called when it goes out of scope.
#[allow(dead_code)]
#[derive(Clone)]
pub struct DropGuard(Arc<DropGuardSingle>);

impl DropGuard {
    /// Create a new `DropGuard` instance.
    pub fn new<T: FnOnce() + Sync + Send + 'static>(func: T) -> Self {
        Self(Arc::new(DropGuardSingle(Some(Box::new(func)))))
    }

    /// Drop the `DropGuard` instance.
    pub fn drop(self) {
        drop(self);
    }
}

/// A named payload.
pub struct NamedPayload<T> {
    /// The transport of the payload.
    pub transport: u32,
    /// The payload.
    pub payload: T,
}
