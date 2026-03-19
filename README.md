# Quash

⚠️ **Work in progress** — Quash is an experimental gRPC-based caching and queueing service written in Go. It provides an in-memory key-value store (with TTL support) and a simple queue API via Protocol Buffers.

## Features

- **Key-Value Store**: Store, retrieve, and delete key-value pairs with optional time-to-live (TTL)
- **In-Memory Queues**: Push and pop values on named queues
- **Memory-Based Persistence (Experimental)**: In-memory data may be dumped to disk when heap usage grows (see server implementation)
- **gRPC API**: Language-agnostic API using Protocol Buffers

> **Note:** This project is in an early stage. Some APIs are stubs or partially implemented (e.g., streaming endpoints currently have minimal implementations). Use for experimentation and prototyping only.

## Architecture

- **Server** (`/server`): gRPC server listening on `:6300`.
- **Queue System** (`/server/queue`): Simple in-memory linked-list queues.
- **Garbage Collection**: Background goroutines expire TTL entries and optionally dump data to disk.
- **Client** (`/client`): A small Go client wrapper around the gRPC service.

## Project Structure

```
.
├── main.go                 # Entry point
├── go.mod                  # Go module definition
├── LICENSE                 # License file
├── admin/                  # (Empty / reserved for future admin utilities)
├── client/                 # Go client library
│   ├── client.go          # Client API implementation
│   └── client_test.go     # Client tests (experimental)
├── proto/                  # Protocol Buffer definitions
│   ├── quash_proto.proto  # Service API definition
│   ├── quash_proto.pb.go  # Generated protobuf code
│   └── quash_proto_grpc.pb.go # Generated gRPC code
└── server/                 # Server implementation
    ├── server.go          # Server startup and initialization
    ├── server/            # Server business logic
    │   └── server.go      # Core server implementation
    └── queue/             # Queue implementation
        └── queue.go       # Queue operations
```

## Installation

### Prerequisites

- Go 1.25.4 or later

### Setup

```bash
git clone https://github.com/mddfaisal/quash.git
cd quash
go mod download
```

### Build & Run

```bash
go build -o quash
./quash
```

By default, the server listens on `0.0.0.0:6300`.

## Running Tests

Run the full test suite:

```bash
go test ./...
```

> ⚠️ The current tests include a long-running loop in `client/client_test.go` and are primarily for manual experimentation.

## API Reference

The full API surface is defined in `proto/quash_proto.proto`.

### Core RPCs (Implemented)

- `SetKV` - store a key/value with a TTL
- `GetKV` - retrieve a value by key
- `DeleteKV` - delete a key/value
- `PushQueue` - push a value onto a named queue
- `QueryQueueList` - list existing queues

### Note on Unimplemented / Experimental Endpoints

The following RPCs are defined in the proto file but their server-side implementations are currently incomplete or stubbed:

- `CreateQueue`
- `DeleteQueue`
- `PopQueue` (streaming)
- `QueryQueueMetric` (streaming)

## Usage Example

```go
package main

import (
    "context"
    "fmt"
    "time"

    "github.com/mddfaisal/quash/client"
    "github.com/mddfaisal/quash/proto"
)

func main() {
    ctx := context.Background()

    // Set a key-value pair with a 5 minute TTL
    resp, err := client.SetKV(ctx, &proto.SetKVRequest{
        Key:             "user:123",
        Value:           "John Doe",
        TimeOutDuration: 300_000,
        InitTime:        time.Now().UnixMilli(),
    })
    if err != nil {
        panic(err)
    }
    fmt.Println("SetKV response:", resp)

    // Read it back
    getResp, err := client.GetKV(ctx, &proto.GetKVRequest{Key: "user:123"})
    if err != nil {
        panic(err)
    }
    fmt.Println("GetKV value:", getResp.Value)

    // Push an item into a queue
    qResp, err := client.PushQueue(ctx, &proto.PushIntoQueueRequest{
        QueueName: "tasks",
        Value:     "process_order",
    })
    if err != nil {
        panic(err)
    }
    fmt.Println("PushQueue response:", qResp)
}
```

## Building from Source

To regenerate the protobuf bindings (requires `protoc` and the Go protobuf plugin):

```bash
protoc --go_out=. --go-grpc_out=. proto/quash_proto.proto
```

## Known Limitations

- No authentication/authorization or multi-tenant support
- Single-node, in-memory storage (not durable by default)
- Queue implementation is not thread-safe and may panic on empty pops
- Some RPCs are currently unimplemented or behave as stubs

## Contributing

Contributions are welcome! Please open issues or pull requests.

## License

For license information, see the [LICENSE](LICENSE) file.

## Contact

For questions or feedback, please reach out to the maintainer.
