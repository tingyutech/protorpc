use proc_macro2::TokenStream;
use prost_build::Service;
use quote::{format_ident, quote};

pub fn make_server(service: &Service) -> TokenStream {
    let methods = service.methods.iter().map(|method| {
        let func = format_ident!("{}", method.name);
        let request_ty = format_ident!("{}", method.input_type);
        let response_ty = format_ident!("{}", method.output_type);
        let stream_ty = format_ident!("{}Stream", method.proto_name);

        // If the response is a stream, create a stream type here. The external
        // implementation needs to implement the `tokio stream` trait.
        let reqponse_stream_ty = if method.server_streaming {
            quote! {
                type #stream_ty: protorpc::futures_core::Stream<
                    Item = Result<super::#response_ty, protorpc::result::RpcError>
                > + std::marker::Send + Unpin + 'static;
            }
        } else {
            quote! {}
        };

        // If the request is a stream, the request type is the protorpc stream type.
        let request = if method.client_streaming {
            quote! { protorpc::request::Request<protorpc::Stream<Result<super::#request_ty, protorpc::result::RpcError>>> }
        } else {
            quote! { protorpc::request::Request<super::#request_ty> }
        };

        // The response is different from the request. If the response is a stream,
        // the returned type is implemented externally.
        let response = if method.server_streaming {
            quote! { protorpc::response::Response<Self::#stream_ty> }
        } else {
            quote! { protorpc::response::Response<super::#response_ty> }
        };

        quote! {
            #reqponse_stream_ty

            async fn #func(
                &self,
                req: #request
            ) -> std::result::Result<#response, protorpc::result::RpcError>;
        }
    });

    // Generate request routing implementation
    let router = service.methods.iter().map(|method| {
        let name = method.proto_name.clone();
        let func = format_ident!("{}", method.name);

        let request = if method.client_streaming {
            quote! { request.into_stream() }
        } else {
            quote! { request.into_once().await? }
        };

        let response_into = if method.server_streaming {
            quote! { into_stream() }
        } else {
            quote! { into_once() }
        };

        quote! {
            #name => {
                Ok(
                    self.0.#func(#request).await?.#response_into
                )
            },
        }
    });

    let service_handler_name = format_ident!("{}ServerHandler", service.name);
    let service_name = format_ident!("{}Server", service.name);
    let service_attr = service.proto_name.clone();

    quote! {
        #[cfg_attr(target_family = "wasm", protorpc::async_trait(?Send))]
        #[cfg_attr(not(target_family = "wasm"), protorpc::async_trait)]
        pub trait #service_handler_name: std::marker::Send + std::marker::Sync + 'static {
            #(#methods)*
        }

        pub struct #service_name<T: #service_handler_name>(pub T);

        #[cfg_attr(target_family = "wasm", protorpc::async_trait(?Send))]
        #[cfg_attr(not(target_family = "wasm"), protorpc::async_trait)]
        impl<T: #service_handler_name> protorpc::server::ServerService for #service_name<T> {
            const NAME: &'static str = #service_attr;

            async fn handle(
                &self,
                request: protorpc::server::Session<protorpc::Stream<Result<Vec<u8>, protorpc::result::RpcError>>>,
            ) -> Result<
                protorpc::response::Response<protorpc::Stream<Result<Vec<u8>, protorpc::result::RpcError>>>,
                protorpc::result::RpcError
            > {
                match request.method.as_str() {
                    #(#router)*
                    _ => Err(protorpc::result::RpcError::not_found("Method not found in current service")),
                }
            }
        }

        impl<T: #service_handler_name> protorpc::RpcServiceBuilder for #service_name<T> {
            const NAME: &'static str = #service_attr;

            type Context = T;
            type Output = ();

            fn build(ctx: Self::Context, stream: protorpc::routers::MessageStream) {
                protorpc::server::startup_server(Self(ctx), stream)
            }
        }
    }
}
