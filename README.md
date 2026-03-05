# Quash

A high-performance gRPC-based caching and queueing service written in Go. Quash provides both key-value storage with TTL support and distributed queue operations through a simple gRPC interface.

## Features

- **Key-Value Store**: Store, retrieve, and delete key-value pairs with support for time-to-live (TTL)
- **Queue Operations**: Push, pop, and manage multiple independent queues
- **Automatic Garbage Collection**: Expired keys are automatically cleaned up
- **gRPC Interface**: Modern, language-agnostic API using Protocol Buffers
- **Thread-Safe**: Built with concurrent access patterns in mind

## Architecture

Quash consists of several components:

- **Server**: gRPC server listening on port 6300 that handles all client requests
- **Queue System**: In-memory queue management with support for multiple named queues
- **Garbage Collection**: Background process that automatically removes expired keys
- **Client**: Go client library for easy integration into other services

## Project Structure

```
.
├── main.go                 # Entry point
├── go.mod                  # Go module definition
├── LICENSE                 # License file
├── admin/                  # Admin utilities
├── client/                 # Go client library
│   ├── client.go          # Client API implementation
│   └── client_test.go     # Client tests
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
- gRPC and Protocol Buffers dependencies

### Setup

1. Clone the repository:
```bash
git clone https://github.com/mddfaisal/quash.git
cd quash
```

2. Download dependencies:
```bash
go mod download
```

3. Build the project:
```bash
go build -o quash
```

4. Run the server:
```bash
./quash
```

The server will start listening on `0.0.0.0:6300`.

## API Documentation

### Key-Value Operations

#### SetKV
Store a key-value pair with optional TTL:

```proto
message SetKVRequest {
    string key = 1;
    string value = 2;
    int64 time_out_duration = 3;  // TTL in milliseconds
    int64 init_time = 4;           // Initialization time
}

message SetKVResponse {
    string response = 1;
}
```

#### GetKV
Retrieve a value by key:

```proto
message GetKVRequest {
    string key = 1;
}

message GetKVResponse {
    string value = 1;
}
```

#### DeleteKV
Delete a key-value pair:

```proto
message DeleteKVRequest {
    string key = 1;
}

message DeleteKVResponse {
    string value = 1;
}
```

### Queue Operations

#### PushIntoQueue
Add an item to a queue:

```proto
message PushIntoQueueRequest {
    string queue_name = 1;
    string value = 2;
}

message PushIntoQueueResponse {
    string response = 1;
}
```

#### PopFromQueue
Remove and return an item from a queue:

```proto
message PopFromQueueRequest {
    string queue_name = 1;
}

message PopFromQueueResponse {
    string response = 1;
}
```

#### QueryQueueList
List all available queues:

```proto
message QueryQueueListRequest {}

message QueueListResponse {
    repeated string queueList = 1;
}
```

#### QueryQueueMetrics
Get metrics for all queues:

```proto
message QueueMetricResponse {
    map<string, int64> queue_matric = 1;  // Queue name to size mapping
}
```

## Usage Example

Using the provided Go client:

```go
package main

import (
    "context"
    "github.com/mddfaisal/quash/client"
    "github.com/mddfaisal/quash/proto"
)

func main() {
    ctx := context.Background()
    
    // Set a key-value pair with 5 minute TTL
    resp, err := client.SetKV(ctx, &proto.SetKVRequest{
        Key:                  "user:123",
        Value:                "John Doe",
        TimeOutDuration:      300000, // 5 minutes in milliseconds
        InitTime:             time.Now().UnixMilli(),
    })
    
    // Push to queue
    qResp, err := client.PushIntoQueue(ctx, &proto.PushIntoQueueRequest{
        QueueName: "tasks",
        Value:     "process_order",
    })
}
```

## Building from Source

To rebuild the protocol buffer files:

```bash
protoc --go_out=. --go-grpc_out=. proto/quash_proto.proto
```

## Performance Characteristics

- **Key-Value Lookups**: O(1) average case
- **Queue Operations**: O(1) for push/pop operations
- **Garbage Collection**: Runs in the background without blocking client operations

## Contributing

Contributions are welcome! Please feel free to submit issues and pull requests.

## License

For license information, see the [LICENSE](LICENSE) file.

## Contact

For questions or feedback, please reach out to the maintainer.
