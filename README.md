# Quash

⚠️ **Work in progress** — Quash is an experimental gRPC-based service written in Go that combines an in-memory key-value store with a topic-based publish/subscribe system, plus a live telemetry dashboard over WebSocket.

## Features

- **Key-value store** — set, get, and delete keys with a per-key TTL
- **Publish/Subscribe** — create named topics, add subscribers to a topic, and publish values that get broadcast to every subscriber on that topic
- **Live telemetry dashboard** — an admin web page that streams topic/subscriber metrics in real time over WebSocket
- **gRPC API** — language-agnostic surface defined in Protocol Buffers, including bidirectional streaming for `Publish` and `Subscribe`
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
   (server/quashserver)                                  │
   - SetKV / GetKV / DeleteKV                   HTTP + WebSocket  :6301
   - CreateTopic / RemoveTopic                  (admin package)
   - AddSubscriber                              - "/"        → dashboard HTML
   - Publish (bidi stream)                      - "/ws/admin"→ live topic metrics feed
   - Subscribe (bidi stream)                        │
   - QueryTopicList                                 │
   - QueryTopicMetric (stream)          gRPC client → QueryTopicMetric stream
              │
    server/queue (linked-list queue,
    one per subscriber — used as each
    subscriber's private mailbox)
```

Internally, each topic holds a map of `subscriber ID → *queue.Queue`. `Publish` pushes a value onto **every** subscriber's queue for that topic (broadcast); `Subscribe` waits for and reads off one subscriber's own queue. The admin dashboard doesn't touch server state directly — it dials the gRPC server as a regular client and re-broadcasts `QueryTopicMetric` over a WebSocket to the browser.

## Project structure

```
.
├── main.go                       # Entry point, signal handling, graceful shutdown
├── utils/
│   ├── utils.go                    # Listen addresses (QuashDb :6300, QuashTelemetry :6301), SubscriptionID()
│   └── utils_test.go
├── admin/
│   └── admin.go                    # Admin dashboard: HTTP + WebSocket telemetry bridge
├── client/
│   ├── client.go                   # Go client wrapper around the gRPC service
│   └── client_test.go
├── proto/
│   ├── quash_proto.proto           # Service and message definitions
│   ├── quash_proto.pb.go           # Generated message code
│   └── quash_proto_grpc.pb.go      # Generated gRPC code
├── server/
│   ├── server.go                   # gRPC server bootstrap, starts admin server too
│   ├── quashserver/
│   │   └── server.go                 # Core RPC handlers, KV store, topic broker, TTL garbage collection
│   └── queue/
│       └── queue.go                   # Singly linked-list queue (used as each subscriber's mailbox)
├── .air.toml                       # Hot-reload config for local development
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
- the admin dashboard on `:6301` — open `http://localhost:6301` in a browser to see live topic metrics

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
| `CreateTopic` | unary | Create a named topic |
| `RemoveTopic` | unary | Delete a topic |
| `AddSubscriber` | unary | Register a new subscriber on an existing topic; returns a `subscriber_id` |
| `Publish` | bidi-streaming | Send values to a topic; every current subscriber receives a copy |
| `Subscribe` | bidi-streaming | Wait for and pull the next value for a given `subscriber_id` on a topic |
| `QueryTopicList` | unary | List all topic names |
| `QueryTopicMetric` | server-streaming | Stream `{topic → {subscriber_count}}` snapshots |

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

    // Create a topic and add a subscriber
    if _, err := client.CreateTopic(ctx, &proto.CreateTopicRequest{TopicName: "orders"}); err != nil {
        panic(err)
    }
    sub, err := client.AddSubscriber(ctx, &proto.AddSubscriberRequest{TopicName: "orders"})
    if err != nil {
        panic(err)
    }
    fmt.Println("Subscriber ID:", sub.SubscriberId)

    // Publish a value (Publish/Subscribe use channel-driven streaming helpers)
    pubReq, pubResp := make(chan string), make(chan string)
    go client.Publish("orders", pubReq, pubResp)
    pubReq <- "process_order_42"
    fmt.Println("Publish ack:", <-pubResp)
    close(pubReq)
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
- `QueryTopicMetric` only reports subscriber counts per topic, not per-subscriber queue depth

## Contributing

Contributions are welcome — please open issues or pull requests.

## License

See the [LICENSE](LICENSE) file.
