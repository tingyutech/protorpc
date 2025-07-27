fn main() {
    protorpc_build::compile_protos(&["./proto/grpc.examples.echo.proto"], &["./proto"]).unwrap();
}
