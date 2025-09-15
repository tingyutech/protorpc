use protorpc::{
    Stream,
    request::Request,
    response::Response,
    result::{Error, RpcError, RpcResult},
    tokio_stream::{self, StreamExt, wrappers::UnboundedReceiverStream},
    transport::{IOStream, Transport},
};

use serde::{Deserialize, Serialize};
use tokio::{net::TcpStream, sync::mpsc::unbounded_channel};

mod proto {
    include!(concat!(env!("OUT_DIR"), "/grpc.examples.echo.rs"));
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Eq)]
pub enum ErrorKind {
    UnaryEcho,
    BidirectionalStreamingEcho,
}

pub struct EchoService;

#[protorpc::async_trait]
impl proto::server::EchoServerHandler for EchoService {
    async fn unary_echo(
        &self,
        req: Request<proto::EchoRequest>,
    ) -> RpcResult<Response<proto::EchoResponse>> {
        if req.get_metadata().get("maybefail").is_some() {
            Err(Error::<ErrorKind>::Custom(ErrorKind::UnaryEcho).into())
        } else {
            let mut res = Response::new(proto::EchoResponse {
                message: format!("response: {}", req.payload.message),
            });

            res.set_metadata(
                req.metadata
                    .iter()
                    .map(|(k, v)| (format!("response: {}", k), format!("response: {}", v)))
                    .collect(),
            );

            Ok(res)
        }
    }

    type BidirectionalStreamingEchoStream = UnboundedReceiverStream<RpcResult<proto::EchoResponse>>;

    async fn bidirectional_streaming_echo(
        &self,
        req: Request<Stream<RpcResult<proto::EchoRequest>>>,
    ) -> RpcResult<Response<Self::BidirectionalStreamingEchoStream>> {
        let maybefail = req.get_metadata().get("maybefail").is_some();
        let (tx, rx) = unbounded_channel();

        tokio::spawn(async move {
            let mut stream = req.payload;

            let mut index = 0;
            while let Some(Ok(it)) = stream.next().await {
                if maybefail && index > 0 {
                    let _ = tx.send(Err(Error::<ErrorKind>::Custom(
                        ErrorKind::BidirectionalStreamingEcho,
                    )
                    .into()));

                    break;
                } else {
                    let _ = tx.send(Ok(proto::EchoResponse {
                        message: format!("response: {}", it.message),
                    }));
                }

                index += 1;
            }
        });

        let mut res = Response::new(UnboundedReceiverStream::from(rx));
        res.set_metadata(
            req.metadata
                .iter()
                .map(|(k, v)| (format!("response: {}", k), format!("response: {}", v)))
                .collect(),
        );

        Ok(res)
    }

    async fn client_streaming_echo(
        &self,
        req: Request<Stream<RpcResult<proto::EchoRequest>>>,
    ) -> RpcResult<Response<proto::EchoResponse>> {
        let (tx, rx) = tokio::sync::oneshot::channel();

        tokio::spawn(async move {
            let mut stream = req.payload;

            let mut messages = Vec::new();
            while let Some(Ok(it)) = stream.next().await {
                messages.push(it.message);
            }

            let _ = tx.send(proto::EchoResponse {
                message: messages.join(","),
            });
        });

        let mut res = Response::new(rx.await.map_err(|_| RpcError::invalid_stream())?);
        res.set_metadata(
            req.metadata
                .iter()
                .map(|(k, v)| (format!("response: {}", k), format!("response: {}", v)))
                .collect(),
        );

        Ok(res)
    }

    type ServerStreamingEchoStream = tokio_stream::Once<RpcResult<proto::EchoResponse>>;

    async fn server_streaming_echo(
        &self,
        req: Request<proto::EchoRequest>,
    ) -> RpcResult<Response<Self::ServerStreamingEchoStream>> {
        let mut res = Response::new(tokio_stream::once(Ok(proto::EchoResponse {
            message: format!("response: {}", req.payload.message),
        })));

        res.set_metadata(
            req.metadata
                .iter()
                .map(|(k, v)| (format!("response: {}", k), format!("response: {}", v)))
                .collect(),
        );

        Ok(res)
    }
}

pub struct Socket(u16);

#[protorpc::async_trait]
impl Transport for Socket {
    async fn create_stream(&self, _id: u128) -> Result<IOStream, std::io::Error> {
        Ok(TcpStream::connect(format!("127.0.0.1:{}", self.0))
            .await?
            .into())
    }
}

#[cfg(test)]
mod tests {
    use std::{collections::HashMap, sync::Arc};

    use log::Level;
    use protorpc::routers::Routes;
    use tokio::net::TcpListener;

    use super::*;

    #[tokio::test]
    async fn test_integration() -> anyhow::Result<()> {
        simple_logger::init_with_level(Level::Debug).unwrap();

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();

        tokio::spawn(async move {
            let routes = Arc::new(Routes::new());

            let routes_ = routes.clone();
            tokio::spawn(async move {
                routes_.start_service::<proto::server::EchoServer<EchoService>>(EchoService).await;
            });

            while let Ok((sokcet, _)) = listener.accept().await {
                routes.add_stream(sokcet.into()).await;
            }
        });

        let client = proto::client::EchoClient::with_transport(Socket(port));

        {
            let metadata = HashMap::from([("type".to_string(), "unary_echo".to_string())]);
            let mut req = Request::new(proto::EchoRequest {
                message: "unary_echo".to_string(),
            });

            req.set_metadata(metadata);

            let res = client.unary_echo(req).await?;

            assert_eq!(
                res.metadata.get("response: type").unwrap(),
                "response: unary_echo"
            );

            assert_eq!(res.payload.message, "response: unary_echo");
        }

        {
            let metadata = HashMap::from([
                ("type".to_string(), "unary_echo".to_string()),
                ("maybefail".to_string(), "".to_string()),
            ]);

            let mut req = Request::new(proto::EchoRequest {
                message: "unary_echo".to_string(),
            });

            req.set_metadata(metadata);

            let Err(err) = client.unary_echo(req).await else {
                panic!()
            };

            assert_eq!(
                err.into_error::<ErrorKind>().get_custom().unwrap(),
                &ErrorKind::UnaryEcho
            );
        }

        {
            let metadata =
                HashMap::from([("type".to_string(), "client_streaming_echo".to_string())]);
            let mut req = Request::new(tokio_stream::iter((0..5).into_iter().map(|i| {
                Ok(proto::EchoRequest {
                    message: format!("client_streaming_echo: {}", i),
                })
            })));

            req.set_metadata(metadata);

            let res = client.client_streaming_echo(req).await?;

            assert_eq!(
                res.metadata.get("response: type").unwrap(),
                "response: client_streaming_echo"
            );

            assert_eq!(
                res.payload.message,
                "client_streaming_echo: 0,client_streaming_echo: 1,client_streaming_echo: 2,client_streaming_echo: 3,client_streaming_echo: 4"
            );
        }

        {
            let metadata =
                HashMap::from([("type".to_string(), "server_streaming_echo".to_string())]);
            let mut req = Request::new(proto::EchoRequest {
                message: "server_streaming_echo".to_string(),
            });

            req.set_metadata(metadata);

            let mut res = client.server_streaming_echo(req).await?;

            assert_eq!(
                res.metadata.get("response: type").unwrap(),
                "response: server_streaming_echo"
            );

            assert_eq!(
                res.payload.next().await.unwrap()?.message,
                "response: server_streaming_echo"
            );
        }

        {
            let metadata = HashMap::from([(
                "type".to_string(),
                "bidirectional_streaming_echo".to_string(),
            )]);

            let mut req = Request::new(tokio_stream::iter((0..5).into_iter().map(|i| {
                Ok(proto::EchoRequest {
                    message: format!("bidirectional_streaming_echo: {}", i),
                })
            })));

            req.set_metadata(metadata);

            let mut res = client.bidirectional_streaming_echo(req).await?;

            assert_eq!(
                res.metadata.get("response: type").unwrap(),
                "response: bidirectional_streaming_echo"
            );

            for i in 0..5 {
                assert_eq!(
                    res.payload.next().await.unwrap()?.message,
                    format!("response: bidirectional_streaming_echo: {}", i)
                );
            }
        }

        {
            let metadata = HashMap::from([
                (
                    "type".to_string(),
                    "bidirectional_streaming_echo".to_string(),
                ),
                ("maybefail".to_string(), "".to_string()),
            ]);

            let mut req = Request::new(tokio_stream::iter((0..5).into_iter().map(|i| {
                Ok(proto::EchoRequest {
                    message: format!("bidirectional_streaming_echo: {}", i),
                })
            })));

            req.set_metadata(metadata);

            let mut res = client.bidirectional_streaming_echo(req).await?;

            assert_eq!(
                res.metadata.get("response: type").unwrap(),
                "response: bidirectional_streaming_echo"
            );

            assert_eq!(
                res.payload.next().await.unwrap()?.message,
                "response: bidirectional_streaming_echo: 0"
            );

            if let Err(e) = res.payload.next().await.unwrap() {
                assert_eq!(
                    e.into_error::<ErrorKind>().get_custom().unwrap(),
                    &ErrorKind::BidirectionalStreamingEcho
                );
            } else {
                panic!()
            }

            assert!(res.payload.next().await.is_none());
        }

        Ok(())
    }
}
