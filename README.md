# ProtoRPC

## Project Overview

This is an RPC framework similar to tonic, but unlike tonic, it does not rely on gRPC. Instead, it uses protobuf as the RPC protocol and is not tied to any transport layer.

## Quick Start

### Add Dependencies

Add the following to your `Cargo.toml`:

```toml
[dependencies]
prost = "0.14.1"
protorpc = { git = "https://github.com/tingyutech/protorpc", branch = "0.1.0" }

[build-dependencies]
protorpc-build = { git = "https://github.com/tingyutech/protorpc", branch = "0.1.0" }
```

For more usage examples, please refer to the `tests` directory in the project.

## Contribution & Feedback

If you have any questions or suggestions, please contact the development team through internal company channels.
