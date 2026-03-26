# Proxyables (Zig)

A high-performance, peer-to-peer RPC library that makes remote objects feel local. Built on top of **Yamux** multiplexing and Zig's vtable-based polymorphism, it enables seamless bi-directional interaction between processes with support for callbacks, distributed garbage collection, and stream pooling.

## Features

- **Peer-to-Peer Architecture**: No strict client/server distinction — both sides can import and export objects, enabling true bi-directional communication.
- **Vtable-Based Dispatch**: Uses `ProxyTarget` vtables for efficient, type-erased remote method dispatch without runtime reflection overhead.
- **Distributed Garbage Collection**: Automatically manages remote object lifecycles using reference counting and release instructions.
- **Multi-Threaded Accept Loop**: Spawns threads for each incoming connection, enabling concurrent request handling.
- **Stream Pooling**: Reuses streams to eliminate handshake overhead for high-frequency calls.
- **Pluggable Codec**: Abstract codec interface with a default JSON implementation — swap in MessagePack or any custom format.

## Installation

Add as a Zig package dependency:
```zig
// build.zig.zon
.dependencies = .{
    .proxyables = .{
        .url = "https://github.com/stateforward/proxyables.zig/archive/main.tar.gz",
    },
},
```

## Usage

### Basic Example

**Server (Exporting an object):**
```zig
const proxyables = @import("proxyables");

const API = struct {
    pub fn echo(self: *@This(), msg: []const u8) []const u8 {
        // return "echo " ++ msg
    }

    pub fn compute(self: *@This(), a: i64, b: i64) i64 {
        return a + b;
    }
};

var api = API{};
const exported = try proxyables.Export(.{
    .allocator = allocator,
    .session = session,
    .root = api.proxyTarget(),
});
```

**Client (Importing the object):**
```zig
const proxyables = @import("proxyables");

const cursor = try proxyables.ImportFrom(.{
    .allocator = allocator,
    .session = session,
});

// Use the proxy cursor to invoke remote methods
```

## Architecture

1. **Proxy Layer**: `ProxyCursor` provides the client-side interface for building and executing remote instruction chains.
2. **Instruction Protocol**: Operations (get, apply, etc.) are serialized into `ProxyInstruction` messages via a pluggable codec (default: JSON with length-prefixed framing).
3. **Transport**: Abstract `Session` and `Stream` interfaces allow any transport (TCP, Unix socket, stdio, etc.).
4. **Reference Management**: An `ObjectRegistry` tracks exported objects with reference counting and bidirectional ID lookup.
5. **Stream Pool**: Thread-safe pool that maintains idle streams for reuse with configurable limits.

## License

MIT
