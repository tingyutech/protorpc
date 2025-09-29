//! Transport layer related types

use std::io::{Error, ErrorKind, Result};

use async_trait::async_trait;
use bytes::{Buf, BufMut, BytesMut};
use prost::Message;
use tokio::{
    io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt},
    sync::{
        Mutex,
        mpsc::{UnboundedReceiver, UnboundedSender, unbounded_channel},
    },
};

use crate::{proto, task::spawn};

enum Payload {
    Frame(proto::Frame),
}

impl Payload {
    fn encode(&self, buffer: &mut BytesMut) {
        buffer.clear();

        // magic token
        buffer.put("PROTORPC".as_bytes());

        match self {
            Self::Frame(frame) => {
                buffer.put_u8(3);
                buffer.put_u32(0);

                frame.encode(buffer).unwrap();

                let size = buffer.len() as u32 - 13;
                buffer[9..13].copy_from_slice(&size.to_be_bytes());
            }
        }
    }

    fn try_decode(buffer: &mut BytesMut) -> Result<Option<Self>> {
        if buffer.len() < 13 {
            return Ok(None);
        }

        if &buffer[0..8] != "PROTORPC".as_bytes() {
            return Err(Error::new(ErrorKind::InvalidData, "invalid magic token"));
        }

        match buffer[8] {
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

async fn writeaf<T>(socket: &mut T, buffer: &[u8]) -> Result<()>
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

/// A wrapper for the input/output stream
pub struct IOStream {
    sender: UnboundedSender<proto::Frame>,
    receiver: Mutex<UnboundedReceiver<Result<proto::Frame>>>,
}

impl IOStream {
    pub async fn recv(&self) -> Option<Result<proto::Frame>> {
        self.receiver.lock().await.recv().await
    }

    pub fn send(&self, frame: proto::Frame) -> Result<()> {
        self.sender
            .send(frame)
            .map_err(|e| Error::new(ErrorKind::Other, e.to_string()))
    }
}

impl<T> From<T> for IOStream
where
    T: AsyncRead + AsyncWrite + Unpin + Send + 'static,
{
    fn from(mut transport: T) -> Self {
        let (sender, mut output_receiver) = unbounded_channel::<proto::Frame>();
        let (input_sender, receiver) = unbounded_channel::<Result<proto::Frame>>();

        spawn(async move {
            let mut read_buffer = BytesMut::new();
            let mut send_buffer = BytesMut::new();

            let mut current_error: Option<std::io::Error> = None;

            'a: loop {
                tokio::select! {
                    ret = transport.read_buf(&mut read_buffer) => {
                        match ret {
                            Ok(size) => {
                                if size == 0 {
                                    #[cfg(feature = "log")]
                                    log::warn!("transport read size is 0");

                                    break;
                                }

                                loop {
                                    match Payload::try_decode(&mut read_buffer){
                                        Ok(payload) => {
                                            if let Some(payload) = payload {
                                                match payload {
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

                                            current_error = Some(e);

                                            break 'a;
                                        }
                                    }
                                }
                            }
                            #[allow(unused_variables)]
                            Err(e) => {
                                #[cfg(feature = "log")]
                                log::warn!("transport read error: {:?}", e);

                                current_error = Some(e);

                                break 'a;
                            }
                        }
                    }
                    ret = output_receiver.recv() => {
                        if let Some(frame) = ret {
                            if let Err(e) = writeaf(&mut transport, {
                                Payload::Frame(frame).encode(&mut send_buffer);

                                &send_buffer
                            }).await {
                                current_error = Some(e);

                                break 'a;
                            }
                        } else {
                            #[cfg(feature = "log")]
                            log::warn!("transport output receiver is closed");

                            break;
                        }
                    }
                }
            }

            #[cfg(feature = "log")]
            log::warn!("transport stream closed");

            if let Some(e) = current_error {
                let _ = input_sender.send(Err(e));
            }
        });

        Self {
            sender,
            receiver: receiver.into(),
        }
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
    async fn create_stream(&self, id: u128) -> Result<IOStream>;
}
