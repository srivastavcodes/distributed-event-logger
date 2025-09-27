# DistributedLog 🚧

> **Work in Progress**: Building a distributed event-log service.

## Progress

### ☑️ Done
- [x] Protobufs

### 🚧 Current
- [x] Log package

### 📅 Future Impl
- gRPC impl, TLS security, observability, clustering with Raft, service discovery, Kubernetes deployment

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
└── api/v1/              # Future gRPC definitions
```

## Goal

Building a distributed commit log (like Kafka) with clustering, replication, service discovery, and production deployment.
