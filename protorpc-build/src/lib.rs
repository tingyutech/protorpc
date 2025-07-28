mod client;
mod server;

use std::{fmt::Write, io::Result, path::Path};

use prost_build::{Config, Service, ServiceGenerator};
use quote::quote;

#[derive(Default)]
struct ProtorpcService {}

impl ServiceGenerator for ProtorpcService {
    fn generate(&mut self, service: Service, buf: &mut String) {
        let server = server::make_server(&service);
        let client = client::make_client(&service);

        let token = quote! {
            /// Client implementation
            #[allow(unused)]
            pub mod client {
                #[allow(unused)]
                use protorpc::tokio_stream::StreamExt;

                #client
            }

            /// Server implementation
            #[allow(unused)]
            pub mod server {
                #[allow(unused)]
                use protorpc::tokio_stream::StreamExt;

                #[allow(unused)]
                use prost::Message;

                #server
            }
        };

        // Convert the generated token stream to Rust code again.
        let ast: syn::File = syn::parse2(token).unwrap();
        buf.write_str(&prettyplease::unparse(&ast)).unwrap();
    }
}

pub fn compile_protos(protos: &[impl AsRef<Path>], includes: &[impl AsRef<Path>]) -> Result<()> {
    Config::new()
        .service_generator(Box::new(ProtorpcService::default()))
        .compile_protos(protos, includes)
}

pub fn configure() -> Config {
    let mut config = Config::new();
    config.service_generator(Box::new(ProtorpcService::default()));
    config
}
