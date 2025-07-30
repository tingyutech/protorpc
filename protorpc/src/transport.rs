//! Transport layer related types

use std::fmt::Debug;

use async_trait::async_trait;
use bytes::{Buf, BufMut, BytesMut};
use prost::Message;
use tokio::{
    io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt},
    sync::mpsc::{UnboundedReceiver, UnboundedSender, unbounded_channel},
};

use crate::proto;

/// A wrapper for the input/output stream
pub struct IOStream {
    pub(crate) sender: UnboundedSender<proto::Frame>,
    pub(crate) receiver: UnboundedReceiver<proto::Frame>,
}

impl<T> From<T> for IOStream
where
    T: AsyncRead + AsyncWrite + Unpin + Send + 'static,
{
    fn from(mut transport: T) -> Self {
        let (sender, mut output_receiver) = unbounded_channel::<proto::Frame>();
        let (input_sender, receiver) = unbounded_channel::<proto::Frame>();

        tokio::spawn(async move {
            let mut read_buffer = BytesMut::new();
            let mut send_buffer = BytesMut::new();

            'a: loop {
                tokio::select! {
                    Ok(size) = transport.read_buf(&mut read_buffer) => {
                        if size == 0 {
                            break;
                        }

                        loop {
                            // There are at least 4 bytes, because there must be a
                            // length field
                            if read_buffer.len() < 4 {
                                break;
                            }

                            let content_len =
                                u32::from_be_bytes(read_buffer[..4].try_into().unwrap()) as usize;

                            if content_len + 4 > read_buffer.len() {
                                break;
                            }

                            // skip len
                            read_buffer.advance(4);

                            match proto::Frame::decode(&mut read_buffer.split_to(content_len)) {
                                Ok(frame) => {
                                    if input_sender.send(frame).is_err() {
                                        break 'a;
                                    }
                                }
                                Err(_) => {
                                    break 'a;
                                }
                            }
                        }
                    }
                    Some(frame) = output_receiver.recv() => {
                        send_buffer.clear();
                        send_buffer.put_u32(0);

                        frame.encode(&mut send_buffer).unwrap();

                        {
                            let size = send_buffer.len() as u32 - 4;
                            send_buffer[..4].copy_from_slice(size.to_be_bytes().as_ref());
                        }

                        if transport.write_all(&send_buffer).await.is_err() {
                            break;
                        } else {
                            let _ = transport.flush().await;
                        }
                    }
                    else => {
                        break;
                    }
                }
            }
        });

        Self { sender, receiver }
    }
}

/// A trait for the transport layer
///
/// If the RPC service needs to create a new stream internally, it will call the
/// external implementation through this trait.
///
/// The external implementation can decide how to create the stream based on the
/// stream's ID. When returning, any type that implements `AsyncRead` and
/// `AsyncWrite` can be converted into an `IOStream`, such as `TcpStream`.
#[async_trait]
pub trait Transport: Send + Sync {
    type Error: Debug;

    async fn create_stream(&self, id: &str) -> Result<IOStream, Self::Error>;
}
