# Project Overview

This project is an internal Rust RPC (Remote Procedure Call) library developed for high-performance, easy-to-use, and transport-agnostic RPC capabilities in microservice architectures. The library supports automatic code generation for both client and server, greatly improving development efficiency.

## Key Features

-   **Transport Agnostic**: Easily adaptable to different network transport protocols.
-   **Code Generation**: Supports automatic generation of client and server code from proto files.
-   **High Performance**: Built with Rust, offering excellent performance and memory safety.
-   **Easy Integration**: Suitable for various microservice scenarios and easy to integrate with existing systems.

## Directory Structure

```
protorpc/
  ├── protorpc/           # Core RPC library
  │   ├── src/            # Core implementation (client/server/request/response)
  │   ├── proto/          # Proto file definitions
  │   └── build.rs        # Build script
  ├── protorpc-build/     # Code generation tools
  │   ├── src/            # Generator implementation
  │   └── Cargo.toml      # Dependency configuration
  ├── tests/              # Test cases and examples
  └── README.md           # Project documentation
```

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
