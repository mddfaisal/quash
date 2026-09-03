# Quash

⚠️ **Work in progress** — Quash is an experimental gRPC-based in-memory key-value store and queue service written in Go, with a live telemetry dashboard over WebSocket.

## Features

- **Key-value store** — set, get, and delete keys with a per-key TTL
- **In-memory queues** — create/delete named queues, push values, and pop them via a streaming RPC
- **Live telemetry dashboard** — an admin web page that shows queue metrics in real time over WebSocket
- **gRPC API** — language-agnostic surface defined in Protocol Buffers
- **Hot reload for development** — configured via [Air](https://github.com/air-verse/air) (`.air.toml`)

> **Note:** This is an early-stage, single-node, in-memory project meant for learning and experimentation — not production use. See [Known limitations](#known-limitations).

## Architecture

```
                        ┌─────────────────────┐
                        │        main.go       │
                        │  (signal handling,    │
                        │   graceful shutdown)  │
                        └──────────┬────────────┘
                                   │
                          go server.Serve()
                                   │
              ┌────────────────────┴────────────────────┐
              │                                          │
   gRPC server  :6300                        go admin.AdminServer()
   (server/server)                                       │
   - SetKV / GetKV / DeleteKV                   HTTP + WebSocket  :6301
   - CreateQueue / DeleteQueue                  (admin package)
   - PushQueue / PopQueue (stream)              - "/"        → dashboard HTML
   - QueryQueueList                             - "/ws/admin"→ live metrics feed
   - QueryQueueMetric (stream)                       │
              │                                       │
     server/queue (linked-list queue)     gRPC client → QueryQueueMetric stream
```

The admin dashboard doesn't read server state directly — it dials the gRPC server as a regular client and re-broadcasts `QueryQueueMetric` over a WebSocket to the browser.

## Project structure

```
.
├── main.go                     # Entry point, signal handling, graceful shutdown
├── config/
│   └── config.go                # Listen addresses (QuashDb :6300, QuashTelemetry :6301)
├── admin/
│   └── admin.go                  # Admin dashboard: HTTP + WebSocket telemetry bridge
├── client/
│   ├── client.go                 # Go client wrapper around the gRPC service
│   └── client_test.go            # Client tests
├── proto/
│   ├── quash_proto.proto         # Service and message definitions
│   ├── quash_proto.pb.go         # Generated message code
│   └── quash_proto_grpc.pb.go    # Generated gRPC code
├── server/
│   ├── server.go                 # gRPC server bootstrap, starts admin server too
│   ├── server/
│   │   └── server.go              # Core RPC handlers, KV store, TTL garbage collection
│   └── queue/
│       └── queue.go               # Singly linked-list queue implementation
├── .air.toml                     # Hot-reload config for local development
└── go.mod
```

## Installation

### Prerequisites

- Go 1.25 or later

### Setup

```bash
git clone https://github.com/mddfaisal/quash.git
cd quash
go mod download
```

### Build & run

```bash
go build -o quash
./quash
```

This starts:
- the gRPC API on `0.0.0.0:6300`
- the admin dashboard on `:6301` — open `http://localhost:6301` in a browser to see live queue metrics

### Development with hot reload

```bash
go install github.com/air-verse/air@latest
air
```

Air rebuilds and restarts the binary whenever a `.go`, `.html`, `.tpl`, or `.tmpl` file changes.

## Running tests

```bash
go test ./...
```

## API reference

Full definitions live in `proto/quash_proto.proto`.

| RPC | Type | Description |
|---|---|---|
| `SetKV` | unary | Store a key/value with a TTL (`time_out_duration`, in seconds) |
| `GetKV` | unary | Retrieve a value by key; errors if missing or expired |
| `DeleteKV` | unary | Delete a key |
| `CreateQueue` | unary | Create a named queue |
| `DeleteQueue` | unary | Delete a named queue |
| `PushQueue` | unary | Push a value onto a named queue (creates it if missing) |
| `PopQueue` | server-streaming | Stream values off a queue as they become available |
| `QueryQueueList` | unary | List all queue names |
| `QueryQueueMetric` | server-streaming | Stream `{queue name → item count}` snapshots |

## Usage example

```go
package main

import (
    "context"
    "fmt"

    "github.com/mddfaisal/quash/client"
    "github.com/mddfaisal/quash/proto"
)

func main() {
    ctx := context.Background()

    // Set a key with a 5 minute TTL (time_out_duration is in seconds)
    resp, err := client.SetKV(ctx, &proto.SetKVRequest{
        Key:             "user:123",
        Value:           "John Doe",
        TimeOutDuration: 300,
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

## Regenerating protobuf code

Requires `protoc` and the Go protobuf/gRPC plugins:

```bash
protoc --go_out=. --go-grpc_out=. proto/quash_proto.proto
```

## Known limitations

- Single-node, in-memory only — no persistence or durability across restarts
- No authentication/authorization or multi-tenancy
- `server/queue` is not thread-safe on its own; correctness currently depends on every caller holding the shared server-level lock
- No real pub/sub primitive yet — `QueryQueueMetric` streaming plus the admin WebSocket bridge is the closest thing today

## Contributing

Contributions are welcome — please open issues or pull requests.

## License

See the [LICENSE](LICENSE) file.
