# DistributedLog 🚧

> **Work in Progress**: Building a distributed event-log service.

## What We've Built So Far

### Core Storage Engine

**Store Layer**: Low-level file operations with buffered writes for performance

- Record format: 8-byte length header + variable-length data
- Thread-safe operations with mutex protection
- Efficient append and read operations

**Index Layer**: Memory-mapped file indexing for fast record lookups

- Maps record offsets to file positions
- Fixed-size entries (12 bytes: 4-byte offset + 8-byte position)
- Pre-allocated file space with memory mapping for performance

### Key Features Implemented

- **Binary Encoding**: Big-endian format for cross-platform compatibility
- **Buffered I/O**: Reduces system calls for better performance with frequent writes
- **Memory Mapping**: Fast index access using mmap for efficient lookups
- **Thread Safety**: Mutex-protected operations for concurrent access

## Quick Start

```bash
git clone https://github.com/srivastavcodes/distributed-event-logger.git
cd distributed-event-logger
go run .
```

Server starts on `http://localhost:8080`

## Testing

```bash
# Produce
curl -X POST http://localhost:8080/log \
  -H "Content-Type: application/json" \
  -d '{"record": {"value": "SGVsbG8gV29ybGQ="}}'

# Consume  
curl "http://localhost:8080/log?offset=0"
```

## Structure

```
├── cmd/server/          # Entry point
├── internal/log/        # Storage (log, segments, index, store)
└── proto/v1/            # Future gRPC definitions
```

## Goal

Building a distributed commit log (like Kafka) with clustering, replication, service discovery, and production deployment.
