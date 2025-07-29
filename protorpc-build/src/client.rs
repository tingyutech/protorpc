use proc_macro2::TokenStream;
use prost_build::Service;
use quote::{format_ident, quote};

pub fn make_client(service: &Service) -> TokenStream {
    let service_attr = service.proto_name.clone();

    let methods = service.methods.iter().map(|method| {
        let method_attr = method.proto_name.clone();

        let request_ty = {
            let ty = format_ident!("{}", method.input_type);
            quote! { super::#ty }
        };

        let response_ty = {
            let ty = format_ident!("{}", method.output_type);
            quote! { super::#ty }
        };

        // If the request is a stream, the request type is the protorpc stream type.
        let request = if method.client_streaming {
            quote! { protorpc::request::Request<S> }
        } else {
            quote! { protorpc::request::Request<#request_ty> }
        };

        // The response is different from the request. If the response is a stream, 
        // the returned type is implemented externally.
        let response = if method.server_streaming {
            quote! { protorpc::response::Response<protorpc::Stream<#response_ty>> }
        } else {
            quote! { protorpc::response::Response<#response_ty> }
        };

        // Different request and response types require calling different methods, 
        // so you need to select which method to call based on the method type
        let func_body = {
            let req = if method.client_streaming {
                quote! { request.payload }
            } else {
                quote! { protorpc::tokio_stream::once(request.payload) }
            };

            let res = if !method.server_streaming {
                quote! {
                    payload
                    .next()
                    .await
                    .ok_or_else(|| protorpc::client::Error::InvalidStream)?
                }
            } else {
                quote! { payload }
            };

            quote! {
                let (mut payload, metadata) = self.0.request(protorpc::client::BaseRequest {
                    service: #service_attr,
                    method: #method_attr,
                    timeout: request.timeout,
                    metadata: request.metadata,
                    request: #req,
                })
                .await?;

                Ok(protorpc::response::Response {
                    payload: #res,
                    metadata,
                })
            }
        };

        let func = format_ident!("{}", method.name);
        let generics = if method.client_streaming {
            quote! { <S: protorpc::futures_core::Stream<Item = #request_ty> + std::marker::Send + Unpin + 'static> }
        } else {
            quote! {}
        };

        quote! {
            pub async fn #func #generics(
                &self,
                request: #request
            ) -> std::result::Result<#response, protorpc::client::Error> {
                #func_body
            }
        }
    });

    let service_name = format_ident!("{}Service", service.name);

    quote! {
        pub struct #service_name<T>(T);

        impl<T: protorpc::client::RequestHandler> #service_name<T> {
            #(#methods)*
        }

        impl #service_name<protorpc::client::Multiplexing> {
            pub fn with_stream(stream: protorpc::transport::IOStream) -> Self {
                Self(protorpc::client::Multiplexing::new(stream))
            }
        }

        impl<T: protorpc::transport::Transport> #service_name<protorpc::client::Streaming<T>> {
            pub fn with_transport(transport: T) -> Self {
                Self(protorpc::client::Streaming::new(transport))
            }
        }

        impl protorpc::RpcServiceBuilder for #service_name<protorpc::client::Multiplexing> {
            const NAME: &'static str = #service_attr;

            type Context = ();
            type Output = Self;

            fn build(_: Self::Context, transport: protorpc::transport::IOStream) -> Self {
                Self::with_stream(transport)
            }
        }
    }
}
