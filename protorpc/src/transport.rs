//! Transport layer related types

use async_trait::async_trait;
use bytes::{Buf, BufMut, BytesMut};
use prost::Message;
use tokio::{
    io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt},
    sync::mpsc::{UnboundedReceiver, UnboundedSender, unbounded_channel},
};

use crate::{proto, result::IoResult, task::spawn};

/// A wrapper for the input/output stream
pub struct IOStream {
    pub(crate) sender: UnboundedSender<proto::Frame>,
    pub(crate) receiver: UnboundedReceiver<IoResult<proto::Frame>>,
}

impl<T> From<T> for IOStream
where
    T: AsyncRead + AsyncWrite + Unpin + Send + 'static,
{
    fn from(mut transport: T) -> Self {
        let (sender, mut output_receiver) = unbounded_channel::<proto::Frame>();
        let (input_sender, receiver) = unbounded_channel::<IoResult<proto::Frame>>();

        spawn(async move {
            let mut read_buffer = BytesMut::new();
            let mut send_buffer = BytesMut::new();

            'a: loop {
                tokio::select! {
                    ret = transport.read_buf(&mut read_buffer) => {
                        match ret {
                            Ok(size) => {
                                if size == 0 {
                                    break;
                                }

                                loop {
                                    // There are at least 8 bytes, because there must be a
                                    // length field
                                    if read_buffer.len() < 8 {
                                        break;
                                    }

                                    let content_len =
                                        u32::from_be_bytes(read_buffer[..4].try_into().unwrap()) as usize;

                                    if content_len + 8 > read_buffer.len() {
                                        break;
                                    }

                                    // If the CRC check fails, it means this stream is already
                                    // corrupted and cannot be recovered, so just close it.
                                    if crc32fast::hash(&read_buffer[8..content_len + 8]) !=
                                        u32::from_be_bytes(read_buffer[4..8].try_into().unwrap())
                                    {
                                        break 'a;
                                    }

                                    // skip len
                                    read_buffer.advance(8);

                                    match proto::Frame::decode(&mut read_buffer.split_to(content_len)) {
                                        Ok(frame) => {
                                            if input_sender.send(Ok(frame)).is_err() {
                                                break 'a;
                                            }
                                        }
                                        Err(_) => {
                                            break 'a;
                                        }
                                    }
                                }
                            }
                            Err(e) => {
                                let _ = input_sender.send(Err(e));

                                break 'a;
                            }
                        }
                    }
                    Some(frame) = output_receiver.recv() => {
                        send_buffer.clear();
                        send_buffer.put_u64(0);

                        frame.encode(&mut send_buffer).unwrap();

                        {
                            let size = send_buffer.len() as u32 - 8;
                            send_buffer[..4].copy_from_slice(size.to_be_bytes().as_ref());

                            let checksum = crc32fast::hash(&send_buffer[8..]);
                            send_buffer[4..8].copy_from_slice(checksum.to_be_bytes().as_ref());
                        }

                        #[allow(unused_variables)]
                        if let Err(e) = transport.write_all(&send_buffer).await {
                            #[cfg(feature = "log")]
                            log::error!("transport write error: {:?}", e);

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
#[cfg_attr(target_family = "wasm", async_trait(?Send))]
#[cfg_attr(not(target_family = "wasm"), async_trait)]
pub trait Transport: Send + Sync {
    async fn create_stream(&self, id: u128) -> IoResult<IOStream>;
}
