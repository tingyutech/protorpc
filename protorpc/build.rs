fn main() {
    prost_build::compile_protos(&["./proto/rpc.proto"], &["./proto"])
        .map_err(|e| format!("Failed to compile airport own protobuf definition, this should almost never happen, internal error = {:?}", e))
        .unwrap();
}
