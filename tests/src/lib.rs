use protorpc::{
    Stream,
    request::Request,
    response::Response,
    tokio_stream::{self, StreamExt, wrappers::UnboundedReceiverStream},
};
use tokio::sync::mpsc::unbounded_channel;

mod proto {
    include!(concat!(env!("OUT_DIR"), "/grpc.examples.echo.rs"));
}

pub struct EchoService;

#[protorpc::async_trait]
impl proto::server::EchoServiceHandler for EchoService {
    type Error = anyhow::Error;

    async fn unary_echo(
        &self,
        req: Request<proto::EchoRequest>,
    ) -> Result<Response<proto::EchoResponse>, Self::Error> {
        let mut res = Response::from(proto::EchoResponse {
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

    type BidirectionalStreamingEchoStream = UnboundedReceiverStream<proto::EchoResponse>;

    async fn bidirectional_streaming_echo(
        &self,
        req: Request<Stream<proto::EchoRequest>>,
    ) -> Result<Response<Self::BidirectionalStreamingEchoStream>, Self::Error> {
        let (tx, rx) = unbounded_channel();

        tokio::spawn(async move {
            let mut stream = req.payload;

            while let Some(it) = stream.next().await {
                let _ = tx.send(proto::EchoResponse {
                    message: format!("response: {}", it.message),
                });
            }
        });

        let mut res = Response::from(UnboundedReceiverStream::from(rx));
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
        req: Request<Stream<proto::EchoRequest>>,
    ) -> Result<Response<proto::EchoResponse>, Self::Error> {
        let (tx, rx) = tokio::sync::oneshot::channel();

        tokio::spawn(async move {
            let mut stream = req.payload;

            let mut messages = Vec::new();
            while let Some(it) = stream.next().await {
                messages.push(it.message);
            }

            let _ = tx.send(proto::EchoResponse {
                message: messages.join(","),
            });
        });

        let mut res = Response::from(rx.await?);
        res.set_metadata(
            req.metadata
                .iter()
                .map(|(k, v)| (format!("response: {}", k), format!("response: {}", v)))
                .collect(),
        );

        Ok(res)
    }

    type ServerStreamingEchoStream = tokio_stream::Once<proto::EchoResponse>;

    async fn server_streaming_echo(
        &self,
        req: Request<proto::EchoRequest>,
    ) -> Result<Response<Self::ServerStreamingEchoStream>, Self::Error> {
        let mut res = Response::from(tokio_stream::once(proto::EchoResponse {
            message: format!("response: {}", req.payload.message),
        }));

        res.set_metadata(
            req.metadata
                .iter()
                .map(|(k, v)| (format!("response: {}", k), format!("response: {}", v)))
                .collect(),
        );

        Ok(res)
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use protorpc::routers::Routes;
    use tokio::net::{TcpListener, TcpStream};

    use super::*;

    #[tokio::test]
    async fn test_integration() -> anyhow::Result<()> {
        {
            let listener = TcpListener::bind("127.0.0.1:8088").await.unwrap();

            tokio::spawn(async move {
                let routes = Routes::new();

                routes
                    .make_service::<proto::server::EchoService<EchoService>>(EchoService)
                    .await;

                while let Ok((sokcet, _)) = listener.accept().await {
                    routes.add_transport(sokcet.into()).await;
                }
            });
        }

        let client = proto::client::EchoService::with_transport(
            TcpStream::connect("127.0.0.1:8088").await?.into(),
        );

        {
            let metadata = HashMap::from([("type".to_string(), "unary_echo".to_string())]);
            let mut req = Request::from(proto::EchoRequest {
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
            let metadata =
                HashMap::from([("type".to_string(), "client_streaming_echo".to_string())]);
            let mut req = Request::from(tokio_stream::iter((0..5).into_iter().map(|i| {
                proto::EchoRequest {
                    message: format!("client_streaming_echo: {}", i),
                }
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
            let mut req = Request::from(proto::EchoRequest {
                message: "server_streaming_echo".to_string(),
            });

            req.set_metadata(metadata);

            let mut res = client.server_streaming_echo(req).await?;

            assert_eq!(
                res.metadata.get("response: type").unwrap(),
                "response: server_streaming_echo"
            );

            assert_eq!(
                res.payload.next().await.unwrap().message,
                "response: server_streaming_echo"
            );
        }

        {
            let metadata = HashMap::from([(
                "type".to_string(),
                "bidirectional_streaming_echo".to_string(),
            )]);
            let mut req = Request::from(tokio_stream::iter((0..5).into_iter().map(|i| {
                proto::EchoRequest {
                    message: format!("bidirectional_streaming_echo: {}", i),
                }
            })));

            req.set_metadata(metadata);

            let mut res = client.bidirectional_streaming_echo(req).await?;

            assert_eq!(
                res.metadata.get("response: type").unwrap(),
                "response: bidirectional_streaming_echo"
            );

            for i in 0..5 {
                assert_eq!(
                    res.payload.next().await.unwrap().message,
                    format!("response: bidirectional_streaming_echo: {}", i)
                );
            }
        }

        Ok(())
    }
}
