//! Transport layer related types

use std::{
    io::{Error, ErrorKind},
    time::Duration,
};

use async_trait::async_trait;
use bytes::{Buf, BufMut, BytesMut};
use prost::Message;
use tokio::{
    io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt},
    sync::mpsc::{UnboundedReceiver, UnboundedSender, unbounded_channel},
};

use crate::{proto, result::IoResult, task::spawn};

#[cfg(not(target_family = "wasm"))]
use tokio::time::interval;

#[cfg(target_family = "wasm")]
use wasmtimer::tokio::interval;

enum Payload {
    Ping,
    Pong,
    Frame(proto::Frame),
}

impl Payload {
    fn encode(&self, buffer: &mut BytesMut) {
        buffer.clear();

        // magic token
        buffer.put("PROTORPC".as_bytes());

        match self {
            Self::Ping => {
                buffer.put_u8(1);
                buffer.put_u32(0);
            }
            Self::Pong => {
                buffer.put_u8(2);
                buffer.put_u32(0);
            }
            Self::Frame(frame) => {
                buffer.put_u8(3);
                buffer.put_u32(0);

                frame.encode(buffer).unwrap();

                let size = buffer.len() as u32 - 13;
                buffer[9..13].copy_from_slice(&size.to_be_bytes());
            }
        }
    }

    fn try_decode(buffer: &mut BytesMut) -> IoResult<Option<Self>> {
        if buffer.len() < 13 {
            return Ok(None);
        }

        if &buffer[0..8] != "PROTORPC".as_bytes() {
            return Err(Error::new(ErrorKind::InvalidData, "invalid magic token"));
        }

        match buffer[8] {
            1 => {
                buffer.advance(13);

                Ok(Some(Self::Ping))
            }
            2 => {
                buffer.advance(13);

                Ok(Some(Self::Pong))
            }
            3 => {
                let size = u32::from_be_bytes(buffer[9..13].try_into().unwrap()) as usize;
                if size + 13 > buffer.len() {
                    Ok(None)
                } else {
                    buffer.advance(13);

                    Ok(Some(Self::Frame(
                        proto::Frame::decode(&mut buffer.split_to(size))
                            .map_err(|_| Error::new(ErrorKind::InvalidData, "invalid frame"))?,
                    )))
                }
            }
            _ => Err(Error::new(ErrorKind::InvalidData, "invalid type")),
        }
    }
}

async fn writeaf<T>(socket: &mut T, buffer: &[u8]) -> Result<(), Error>
where
    T: AsyncWrite + Unpin + Send + 'static,
{
    #[allow(unused_variables)]
    if let Err(e) = socket.write_all(buffer).await {
        #[cfg(feature = "log")]
        log::warn!("transport write error: {:?}", e);

        Err(e)
    } else {
        let _ = socket.flush().await;

        Ok(())
    }
}

const KEEPALIVE_INTERVAL: usize = 2;
const KEEPALIVE_TIMEOUT: usize = 4;

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

            let mut interval = interval(Duration::from_secs(1));

            // last keepalive time
            let mut keepalive = 0;

            'a: loop {
                tokio::select! {
                    _ = interval.tick() => {
                        if keepalive >= KEEPALIVE_TIMEOUT {
                            #[cfg(feature = "log")]
                            log::warn!("transport keepalive timeout, delay= {keepalive}");

                            let _ = input_sender.send(Err(
                                Error::new(
                                    ErrorKind::TimedOut,
                                    "transport keepalive timeout"
                                )
                            ));

                            break 'a;
                        }

                        keepalive += 1;

                        if keepalive % KEEPALIVE_INTERVAL == 0 {
                            if writeaf(&mut transport, {
                                Payload::Ping.encode(&mut send_buffer);

                                &send_buffer
                            }).await.is_err() {
                                break;
                            }
                        }
                    }
                    ret = transport.read_buf(&mut read_buffer) => {
                        match ret {
                            Ok(size) => {
                                if size == 0 {
                                    break;
                                }

                                loop {
                                    match Payload::try_decode(&mut read_buffer){
                                        Ok(payload) => {
                                            if let Some(payload) = payload {
                                                match payload {
                                                    Payload::Ping => {
                                                        if writeaf(&mut transport, {
                                                            Payload::Pong.encode(&mut send_buffer);

                                                            &send_buffer
                                                        }).await.is_err() {
                                                            break;
                                                        }
                                                    }
                                                    Payload::Pong => {
                                                        keepalive = 0;
                                                    }
                                                    Payload::Frame(frame) => {
                                                        if input_sender.send(Ok(frame)).is_err() {
                                                            break 'a;
                                                        }
                                                    }
                                                }
                                            } else {
                                                break;
                                            }
                                        }
                                        Err(e) => {
                                            #[cfg(feature = "log")]
                                            log::warn!("transport decode payload error: {:?}", e);

                                            let _ = input_sender.send(Err(e));

                                            break 'a;
                                        }
                                    }
                                }
                            }
                            #[allow(unused_variables)]
                            Err(e) => {
                                #[cfg(feature = "log")]
                                log::warn!("transport read error: {:?}", e);

                                let _ = input_sender.send(Err(e));

                                break 'a;
                            }
                        }
                    }
                    Some(frame) = output_receiver.recv() => {
                        if writeaf(&mut transport, {
                            Payload::Frame(frame).encode(&mut send_buffer);

                            &send_buffer
                        }).await.is_err() {
                            break;
                        }
                    }
                    else => {
                        break;
                    }
                }
            }

            #[cfg(feature = "log")]
            log::warn!("transport closed");
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
